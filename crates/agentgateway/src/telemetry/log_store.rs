use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Instant;
use std::{env, thread};

use chrono::{DateTime, Duration, Utc};
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::{apply, schema};

mod postgres;
mod sqlite;

static REQUEST_LOG_STORE: OnceLock<RequestLogStore> = OnceLock::new();

/// Request log database metrics. They are process-wide, like the store itself.
pub(crate) static METRICS: LazyLock<LogStoreMetrics> = LazyLock::new(LogStoreMetrics::default);

#[derive(Default)]
pub(crate) struct LogStoreMetrics {
	/// 1 when the last connect or write succeeded, 0 when it failed or no connection exists yet.
	pub up: Gauge,
	pub queued_records: Gauge,
	pub dropped_records: Family<DropLabels, Counter>,
}

impl LogStoreMetrics {
	fn dropped(&self, reason: DropReason, count: usize) {
		self
			.dropped_records
			.get_or_create(&DropLabels { reason })
			.inc_by(count as u64);
	}
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum DropReason {
	/// The queue already held `maxQueuedRecords` records.
	QueueFull,
	/// The database rejected the batch.
	WriteFailed,
	/// The store shut down before the database became available.
	Shutdown,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct DropLabels {
	pub reason: DropReason,
}

#[apply(schema!)]
#[derive(Eq, PartialEq)]
pub struct Config {
	/// Connection URL for the request log database. A postgres:// or postgresql:// URL uses Postgres; any other value is treated as a SQLite database.
	pub url: String,
	/// Maximum number of connections to open in this database's connection pool. Defaults to 5.
	/// When the request log and config stores have matching database settings, they share one pool
	/// with this limit.
	#[serde(default)]
	#[cfg_attr(feature = "schema", schemars(range(min = 1)))]
	pub max_connections: Option<u32>,
	/// Whether the request log database must be available at startup. Defaults to true: startup
	/// fails if the database cannot be connected to or its schema cannot be created.
	/// When false, the gateway starts serving immediately and connects in the background, retrying
	/// with exponential backoff. Until then records are queued in memory (see `maxQueuedRecords`)
	/// and log queries fail. Only supported on `config.logging.database`: the primary database
	/// backs budgets and the config store and is always required, so a request log that shares it
	/// still needs it at startup.
	#[serde(default)]
	pub required: Option<bool>,
	/// Maximum number of request log records held in memory waiting to be written to the
	/// database. When the queue is full, new records are dropped and counted in
	/// `agentgateway_log_store_dropped_records_total`. Unbounded when unset.
	#[serde(default)]
	#[cfg_attr(feature = "schema", schemars(range(min = 1)))]
	pub max_queued_records: Option<usize>,
}

impl Config {
	/// Whether startup fails when the database is unavailable.
	pub fn is_required(&self) -> bool {
		self.required.unwrap_or(true)
	}

	/// Whether both configs describe the same connection pool.
	pub fn same_pool(&self, other: &Config) -> bool {
		self.url == other.url && self.max_connections == other.max_connections
	}
}

#[derive(Clone)]
pub struct RequestLogStore {
	tx: Sender<LogStoreMsg>,
	queue: Arc<QueueState>,
}

impl RequestLogStore {
	pub(super) fn emit(&self, record: PendingRequestLog) {
		if !self.queue.try_enqueue() {
			return;
		}
		if let Err(err) = self.tx.send(LogStoreMsg::Record(record)) {
			self.queue.dequeue(1);
			warn!(target: "request", ?err, "failed to enqueue request log database record");
		}
	}

	async fn request<T: Send + 'static>(
		&self,
		msg: impl FnOnce(QueryResponse<T>) -> LogStoreMsg,
	) -> anyhow::Result<T> {
		let (tx, rx) = oneshot::channel();
		self
			.tx
			.send(msg(tx))
			.map_err(|_| anyhow::anyhow!("request log database worker is stopped"))?;
		rx.await
			.map_err(|_| anyhow::anyhow!("request log database worker is stopped"))?
	}
}

pub async fn setup(cfg: &Config) -> anyhow::Result<RequestLogStoreGuard> {
	setup_with_pool(cfg, None).await
}

pub async fn setup_with_pool(
	cfg: &Config,
	pool: Option<crate::database::DatabasePool>,
) -> anyhow::Result<RequestLogStoreGuard> {
	let (store, guard) = start(cfg, pool, DEFAULT_RETRY).await?;
	let _ = REQUEST_LOG_STORE.set(store);
	Ok(guard)
}

async fn start(
	cfg: &Config,
	pool: Option<crate::database::DatabasePool>,
	retry: RetryBackoff,
) -> anyhow::Result<(RequestLogStore, RequestLogStoreGuard)> {
	let (tx, rx) = crossbeam::channel::unbounded();
	let (ready_tx, ready_rx) = oneshot::channel();
	let queue = Arc::new(QueueState::new(cfg.max_queued_records));
	let writer = LogStoreWorker::new(rx, cfg.clone(), pool, queue.clone(), retry, ready_tx)
		.worker_thread("request-log-db-writer".to_string())?;
	ready_rx
		.await
		.map_err(|_| anyhow::anyhow!("request log database worker stopped during startup"))??;
	let store = RequestLogStore {
		tx: tx.clone(),
		queue,
	};
	let guard = RequestLogStoreGuard {
		tx,
		writer: Some(writer),
	};
	Ok((store, guard))
}

/// Counts records that have been enqueued but not yet written (or dropped), and enforces
/// `maxQueuedRecords`.
struct QueueState {
	max: Option<usize>,
	queued: AtomicUsize,
	full: AtomicBool,
}

impl QueueState {
	fn new(max: Option<usize>) -> Self {
		Self {
			max,
			queued: AtomicUsize::new(0),
			full: AtomicBool::new(false),
		}
	}

	/// Reserves a queue slot for one record. Returns false, counting the record as dropped, when
	/// the queue is full.
	fn try_enqueue(&self) -> bool {
		let queued = self.queued.fetch_add(1, Ordering::Relaxed);
		if self.max.is_some_and(|max| queued >= max) {
			self.queued.fetch_sub(1, Ordering::Relaxed);
			METRICS.dropped(DropReason::QueueFull, 1);
			if !self.full.swap(true, Ordering::Relaxed) {
				warn!(
					target: "request",
					max_queued_records = self.max,
					"request log queue is full; dropping records until the database catches up"
				);
			}
			return false;
		}
		if self.full.load(Ordering::Relaxed) && self.full.swap(false, Ordering::Relaxed) {
			info!(target: "request", "request log queue has room again");
		}
		METRICS.queued_records.inc();
		true
	}

	/// Releases `count` queue slots and returns how many records remain queued.
	fn dequeue(&self, count: usize) -> usize {
		METRICS.queued_records.dec_by(count as i64);
		self.queued.fetch_sub(count, Ordering::Relaxed) - count
	}
}

pub(super) fn emit(record: PendingRequestLog) {
	if let Some(store) = REQUEST_LOG_STORE.get() {
		store.emit(record);
	}
}

pub fn enabled() -> bool {
	REQUEST_LOG_STORE.get().is_some()
}

pub async fn search(request: SearchRequest) -> anyhow::Result<SearchResponse> {
	let store = REQUEST_LOG_STORE
		.get()
		.ok_or_else(|| anyhow::anyhow!("request log database is not configured"))?;
	store
		.request(|tx| LogStoreMsg::Search { request, tx })
		.await
}

pub async fn get(request: GetRequest) -> anyhow::Result<GetResponse> {
	let store = REQUEST_LOG_STORE
		.get()
		.ok_or_else(|| anyhow::anyhow!("request log database is not configured"))?;
	store.request(|tx| LogStoreMsg::Get { request, tx }).await
}

pub async fn analytics_summary(
	request: AnalyticsSummaryRequest,
) -> anyhow::Result<AnalyticsSummaryResponse> {
	let store = REQUEST_LOG_STORE
		.get()
		.ok_or_else(|| anyhow::anyhow!("request log database is not configured"))?;
	store
		.request(|tx| LogStoreMsg::AnalyticsSummary { request, tx })
		.await
}

pub async fn tail(request: TailRequest) -> anyhow::Result<TailResponse> {
	let store = REQUEST_LOG_STORE
		.get()
		.ok_or_else(|| anyhow::anyhow!("request log database is not configured"))?;
	store.request(|tx| LogStoreMsg::Tail { request, tx }).await
}

pub struct RequestLogStoreGuard {
	tx: Sender<LogStoreMsg>,
	writer: Option<thread::JoinHandle<()>>,
}

impl RequestLogStoreGuard {
	pub async fn shutdown_and_wait(mut self) {
		let _ = self.tx.send(LogStoreMsg::Shutdown);
		if let Some(writer) = self.writer.take() {
			match tokio::task::spawn_blocking(move || writer.join()).await {
				Ok(Ok(())) => {},
				Ok(Err(_)) => {
					warn!(target: "request", "request log database writer panicked");
				},
				Err(err) => {
					warn!(target: "request", ?err, "failed to join request log database writer");
				},
			};
		}
	}
}

impl Drop for RequestLogStoreGuard {
	fn drop(&mut self) {
		let _ = self.tx.send(LogStoreMsg::Shutdown);
	}
}

// Testing shows this to achieve a pretty good throughput for both postgres and sqlite
const DEFAULT_LOG_STORE_BATCH_SIZE: usize = 16384;
const LOG_STORE_BATCH_SIZE_ENV: &str = "REQUEST_LOG_STORE_BATCH_SIZE";

/// Delay between connection attempts when the database is not required at startup.
#[derive(Clone, Copy, Debug)]
struct RetryBackoff {
	initial: std::time::Duration,
	max: std::time::Duration,
}

const DEFAULT_RETRY: RetryBackoff = RetryBackoff {
	initial: std::time::Duration::from_secs(1),
	max: std::time::Duration::from_secs(30),
};

/// How often the worker handles queued messages while it waits for the database.
const DISCONNECTED_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

#[allow(clippy::large_enum_variant)] // The StoredRequestLog, which is used 99.9% of the time, is the large one
enum LogStoreMsg {
	Record(PendingRequestLog),
	Search {
		request: SearchRequest,
		tx: QueryResponse<SearchResponse>,
	},
	Get {
		request: GetRequest,
		tx: QueryResponse<GetResponse>,
	},
	AnalyticsSummary {
		request: AnalyticsSummaryRequest,
		tx: QueryResponse<AnalyticsSummaryResponse>,
	},
	Tail {
		request: TailRequest,
		tx: QueryResponse<TailResponse>,
	},
	Shutdown,
}

type QueryResponse<T> = oneshot::Sender<anyhow::Result<T>>;

struct LogStoreWorker {
	receiver: Receiver<LogStoreMsg>,
	cfg: Config,
	pool: Option<crate::database::DatabasePool>,
	queue: Arc<QueueState>,
	retry: RetryBackoff,
	ready_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
}

impl LogStoreWorker {
	fn new(
		receiver: Receiver<LogStoreMsg>,
		cfg: Config,
		pool: Option<crate::database::DatabasePool>,
		queue: Arc<QueueState>,
		retry: RetryBackoff,
		ready_tx: oneshot::Sender<anyhow::Result<()>>,
	) -> Self {
		Self {
			receiver,
			cfg,
			pool,
			queue,
			retry,
			ready_tx: Some(ready_tx),
		}
	}

	fn worker_thread(self, name: String) -> anyhow::Result<thread::JoinHandle<()>> {
		thread::Builder::new()
			.name(name)
			.spawn(move || self.work())
			.map_err(Into::into)
	}

	fn work(mut self) {
		let runtime = match tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
		{
			Ok(runtime) => runtime,
			Err(err) => {
				warn!(target: "request", ?err, "failed to start request log database worker runtime");
				self.notify_ready(Err(anyhow::Error::new(err)));
				return;
			},
		};
		runtime.block_on(self.work_async());
		debug!(target: "request", "request log database writer stopped");
	}

	fn notify_ready(&mut self, result: anyhow::Result<()>) {
		if let Some(tx) = self.ready_tx.take() {
			let _ = tx.send(result);
		}
	}

	async fn work_async(mut self) {
		let batch_size = match log_store_batch_size() {
			Ok(batch_size) => batch_size,
			Err(err) => {
				self.notify_ready(Err(err));
				return;
			},
		};
		let backend = if self.cfg.is_required() {
			match Backend::connect(&self.cfg, self.pool.take()).await {
				Ok(backend) => backend,
				Err(err) => {
					self.notify_ready(Err(err));
					return;
				},
			}
		} else {
			// Do not hold up startup on the database; queue records until it is reachable.
			self.notify_ready(Ok(()));
			let Some((backend, pending)) = self.connect_in_background().await else {
				return;
			};
			for chunk in pending.chunks(batch_size) {
				write_records(&backend, &self.queue, chunk).await;
			}
			backend
		};
		METRICS.up.set(1);
		self.notify_ready(Ok(()));
		let mut batch = Vec::with_capacity(batch_size);
		loop {
			match self.receiver.recv() {
				Ok(msg) => {
					if process_log_store_msg(&backend, &self.queue, &mut batch, msg).await {
						self.drain_available_messages(&backend, &mut batch).await;
						break;
					}
				},
				Err(_) => {
					self.drain_available_messages(&backend, &mut batch).await;
					break;
				},
			}

			let mut shutdown = false;
			while batch.len() < batch.capacity() {
				match self.receiver.try_recv() {
					Ok(msg) => {
						if process_log_store_msg(&backend, &self.queue, &mut batch, msg).await {
							shutdown = true;
							break;
						}
					},
					Err(TryRecvError::Disconnected) => {
						shutdown = true;
						break;
					},
					Err(TryRecvError::Empty) => break,
				}
			}
			flush_log_store_batch(&backend, &self.queue, &mut batch).await;
			if shutdown {
				self.drain_available_messages(&backend, &mut batch).await;
				break;
			}
		}
	}

	/// Connects to the database without holding up startup, retrying with exponential backoff.
	/// Meanwhile records are buffered and queries fail. Returns the backend and the buffered
	/// records, or None if the store shut down before the database became available.
	async fn connect_in_background(&mut self) -> Option<(Backend, Vec<StoredRequestLog>)> {
		let pool = self.pool.take();
		let mut pending = Vec::new();
		let mut backoff = self.retry.initial;
		let mut attempt: u32 = 0;
		loop {
			attempt += 1;
			let connect = Backend::connect(&self.cfg, pool.clone());
			tokio::pin!(connect);
			let result = loop {
				tokio::select! {
					result = &mut connect => break result,
					_ = tokio::time::sleep(DISCONNECTED_POLL_INTERVAL) => {
						if !self.poll_while_disconnected(&mut pending) {
							return None;
						}
					},
				}
			};
			match result {
				Ok(backend) => {
					if attempt > 1 {
						info!(
							target: "request",
							attempt,
							queued = pending.len(),
							"connected to request log database"
						);
					}
					return Some((backend, pending));
				},
				Err(err) => {
					METRICS.up.set(0);
					warn!(
						target: "request",
						?err,
						attempt,
						retry_in = ?backoff,
						"request log database is unavailable; queueing records and retrying"
					);
				},
			}
			let retry_at = Instant::now() + backoff;
			while let Some(wait) = retry_at.checked_duration_since(Instant::now()) {
				tokio::time::sleep(wait.min(DISCONNECTED_POLL_INTERVAL)).await;
				if !self.poll_while_disconnected(&mut pending) {
					return None;
				}
			}
			backoff = (backoff * 2).min(self.retry.max);
		}
	}

	/// Handles the messages queued while the database is unavailable: records are buffered and
	/// queries fail. Returns false once the store is shut down, after dropping buffered records.
	fn poll_while_disconnected(&self, pending: &mut Vec<StoredRequestLog>) -> bool {
		loop {
			match self.receiver.try_recv() {
				Ok(LogStoreMsg::Record(record)) => pending.push(prepare_record(record)),
				Ok(LogStoreMsg::Shutdown) | Err(TryRecvError::Disconnected) => break,
				Ok(msg) => reply_unavailable(msg),
				Err(TryRecvError::Empty) => return true,
			}
		}
		let mut dropped = pending.len();
		pending.clear();
		while let Ok(msg) = self.receiver.try_recv() {
			match msg {
				LogStoreMsg::Record(_) => dropped += 1,
				msg => reply_unavailable(msg),
			}
		}
		if dropped > 0 {
			warn!(
				target: "request",
				dropped,
				"request log database never became available; dropping queued records"
			);
			METRICS.dropped(DropReason::Shutdown, dropped);
			self.queue.dequeue(dropped);
		}
		false
	}

	async fn drain_available_messages(&self, backend: &Backend, batch: &mut Vec<StoredRequestLog>) {
		loop {
			match self.receiver.try_recv() {
				Ok(msg) => {
					let _ = process_log_store_msg(backend, &self.queue, batch, msg).await;
				},
				Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
			}
			if batch.len() == batch.capacity() {
				flush_log_store_batch(backend, &self.queue, batch).await;
			}
		}
		flush_log_store_batch(backend, &self.queue, batch).await;
	}
}

fn reply_unavailable(msg: LogStoreMsg) {
	let err = || anyhow::anyhow!("request log database is unavailable");
	match msg {
		LogStoreMsg::Search { tx, .. } => {
			let _ = tx.send(Err(err()));
		},
		LogStoreMsg::Get { tx, .. } => {
			let _ = tx.send(Err(err()));
		},
		LogStoreMsg::AnalyticsSummary { tx, .. } => {
			let _ = tx.send(Err(err()));
		},
		LogStoreMsg::Tail { tx, .. } => {
			let _ = tx.send(Err(err()));
		},
		LogStoreMsg::Record(_) | LogStoreMsg::Shutdown => {},
	}
}

fn log_store_batch_size() -> anyhow::Result<usize> {
	let Ok(value) = env::var(LOG_STORE_BATCH_SIZE_ENV) else {
		return Ok(DEFAULT_LOG_STORE_BATCH_SIZE);
	};
	let batch_size = value.parse::<usize>().map_err(|err| {
		anyhow::anyhow!(
			"invalid env var {LOG_STORE_BATCH_SIZE_ENV}={value} ({})",
			err
		)
	})?;
	if batch_size == 0 {
		anyhow::bail!("invalid env var {LOG_STORE_BATCH_SIZE_ENV}=0 (must be greater than 0)");
	}
	Ok(batch_size)
}

fn prepare_record(pending: PendingRequestLog) -> StoredRequestLog {
	let mut record = pending.record;
	record.payload = super::log::database_llm_payload(
		pending.llm_mode,
		pending.input_messages.as_deref().map(Vec::as_slice),
		pending.llm_response.as_ref(),
	);
	record.has_payload = record.payload.is_some();
	record
}

async fn process_log_store_msg(
	backend: &Backend,
	queue: &QueueState,
	batch: &mut Vec<StoredRequestLog>,
	msg: LogStoreMsg,
) -> bool {
	match msg {
		LogStoreMsg::Record(pending) => {
			batch.push(prepare_record(pending));
			false
		},
		LogStoreMsg::Search { request, tx } => {
			flush_log_store_batch(backend, queue, batch).await;
			let _ = tx.send(backend.search(request).await);
			false
		},
		LogStoreMsg::Get { request, tx } => {
			flush_log_store_batch(backend, queue, batch).await;
			let _ = tx.send(backend.get(request).await);
			false
		},
		LogStoreMsg::AnalyticsSummary { request, tx } => {
			flush_log_store_batch(backend, queue, batch).await;
			let _ = tx.send(backend.analytics_summary(request).await);
			false
		},
		LogStoreMsg::Tail { request, tx } => {
			flush_log_store_batch(backend, queue, batch).await;
			let _ = tx.send(backend.tail(request).await);
			false
		},
		LogStoreMsg::Shutdown => true,
	}
}

async fn flush_log_store_batch(
	backend: &Backend,
	queue: &QueueState,
	batch: &mut Vec<StoredRequestLog>,
) {
	write_records(backend, queue, batch).await;
	batch.clear();
}

async fn write_records(backend: &Backend, queue: &QueueState, records: &[StoredRequestLog]) {
	if records.is_empty() {
		return;
	}
	let t0 = Instant::now();
	let count = records.len();
	match backend.insert_batch(records).await {
		Ok(()) => METRICS.up.set(1),
		Err(err) => {
			METRICS.up.set(0);
			METRICS.dropped(DropReason::WriteFailed, count);
			warn!(target: "request", ?err, count, "failed to persist request log batch");
		},
	}
	let backlog = queue.dequeue(count);
	let latency = t0.elapsed();
	let throughput = count as f64 / latency.as_secs_f64();
	debug!(
		count,
		backlog,
		?latency,
		throughput,
		"flushed request log database batch"
	);
}

// Keep captured content in its original form until it reaches the database worker.
// Normalization and JSON serialization can be expensive for large prompts and completions.
pub(super) struct PendingRequestLog {
	pub record: StoredRequestLog,
	pub llm_mode: Option<crate::types::frontend::DatabaseLlmMode>,
	pub input_messages: Option<Arc<Vec<agent_llm::types::NormalizedMessage>>>,
	pub llm_response: Option<crate::cel::LLMContext>,
}

#[derive(Clone, Debug)]
pub struct StoredRequestLog {
	pub id: String,
	pub started_at: DateTime<Utc>,
	pub completed_at: DateTime<Utc>,
	pub duration_ms: i64,
	pub trace_id: Option<String>,
	pub span_id: Option<String>,
	pub http_status: Option<i64>,
	pub error: Option<String>,
	pub gen_ai_operation_name: Option<String>,
	pub gen_ai_provider_name: Option<String>,
	pub gen_ai_request_model: Option<String>,
	pub gen_ai_response_model: Option<String>,
	pub input_tokens: Option<i64>,
	pub output_tokens: Option<i64>,
	pub total_tokens: Option<i64>,
	pub cost: Option<f64>,
	pub agentgateway_user: Option<String>,
	pub agentgateway_group: Option<String>,
	pub user_agent_name: Option<String>,
	pub has_payload: bool,
	pub attributes_json: Box<str>,
	pub payload: Option<StoredRequestLogPayload>,
}

#[derive(Clone, Debug)]
pub struct StoredRequestLogPayload {
	pub request_prompt_json: Option<Value>,
	pub response_completion_json: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimeRange {
	pub from: Option<DateTime<Utc>>,
	pub to: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogFilters {
	#[serde(default)]
	pub http_status: Vec<i64>,
	#[serde(default)]
	pub provider: Vec<String>,
	#[serde(default)]
	pub request_model: Vec<String>,
	#[serde(default)]
	pub response_model: Vec<String>,
	#[serde(default)]
	pub trace_id: Option<String>,
	#[serde(default)]
	pub has_payload: Option<bool>,
	#[serde(default)]
	pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchRequest {
	#[serde(default)]
	pub limit: Option<i64>,
	#[serde(default)]
	pub cursor: Option<String>,
	#[serde(default)]
	pub time_range: Option<TimeRange>,
	#[serde(default)]
	pub filters: LogFilters,
	#[serde(default)]
	pub include_attributes: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetRequest {
	pub id: String,
	#[serde(default)]
	pub include_payload: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticsSummaryRequest {
	#[serde(default)]
	pub time_range: Option<TimeRange>,
	#[serde(default)]
	pub filters: LogFilters,
	#[serde(default)]
	pub group_by: Vec<GroupBy>,
	#[serde(default)]
	pub bucket_count: Option<i64>,
	#[serde(default)]
	pub bucket_seconds: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailRequest {
	#[serde(default)]
	pub limit: Option<i64>,
	#[serde(default)]
	pub cursor: Option<String>,
	#[serde(default)]
	pub filters: LogFilters,
	#[serde(default)]
	pub include_attributes: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupBy {
	pub field: GroupByField,
	#[serde(default)]
	pub key: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum GroupByField {
	Provider,
	RequestModel,
	ResponseModel,
	HttpStatus,
	Attributes,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResponse {
	pub logs: Vec<LogEntry>,
	pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetResponse {
	pub log: Option<LogEntry>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsSummaryResponse {
	pub time_range: TimeRange,
	pub bucket_seconds: i64,
	pub buckets: Vec<AnalyticsTimeBucket>,
	pub groups: Vec<AnalyticsGroup>,
	pub filter_options: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TailResponse {
	pub logs: Vec<LogEntry>,
	pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TailEvent {
	pub entry: LogEntry,
	pub cursor: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsGroup {
	pub group: BTreeMap<String, Value>,
	pub requests: i64,
	pub total_tokens: i64,
	pub cost: f64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsTimeBucket {
	pub start: DateTime<Utc>,
	pub group: BTreeMap<String, Value>,
	pub requests: i64,
	pub total_tokens: i64,
	pub cost: f64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
	pub id: String,
	pub started_at: DateTime<Utc>,
	pub completed_at: DateTime<Utc>,
	pub duration_ms: i64,
	pub trace_id: Option<String>,
	pub span_id: Option<String>,
	pub http_status: Option<i64>,
	pub error: Option<String>,
	pub prompt_preview: Option<String>,
	pub turn: TurnEntry,
	pub gen_ai: GenAiEntry,
	pub usage: UsageEntry,
	pub cost: Option<f64>,
	pub has_payload: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub attributes: Option<Value>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub payload: Option<PayloadEntry>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnEntry {
	pub input: Option<TurnKind>,
	pub output: Option<TurnKind>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnKind {
	User,
	Assistant,
	ToolCall,
	ToolResult,
}

fn prompt_preview(prompt: Option<&Value>) -> Option<String> {
	let messages = prompt?.as_array()?;
	messages.iter().rev().find_map(|message| {
		if message.get("role").and_then(Value::as_str) != Some("user") {
			return None;
		}
		let text = message
			.get("parts")?
			.as_array()?
			.iter()
			.filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
			.filter_map(|part| part.get("text").and_then(Value::as_str))
			.collect::<Vec<_>>()
			.join("\n");
		let text = text.trim();
		if text.is_empty() {
			None
		} else if text.chars().nth(180).is_some() {
			Some(format!("{}...", text.chars().take(177).collect::<String>()))
		} else {
			Some(text.to_string())
		}
	})
}

fn turn_kind(messages: Option<&Value>) -> Option<TurnKind> {
	// Classify the final meaningful normalized part; reasoning and system text are not turns.
	messages?.as_array()?.iter().rev().find_map(|message| {
		let role = message.get("role").and_then(Value::as_str)?;
		message
			.get("parts")?
			.as_array()?
			.iter()
			.rev()
			.find_map(|part| match part.get("type").and_then(Value::as_str)? {
				"toolCall" => Some(TurnKind::ToolCall),
				"toolResult" => Some(TurnKind::ToolResult),
				"text"
					if part
						.get("text")
						.and_then(Value::as_str)
						.is_some_and(|text| !text.trim().is_empty()) =>
				{
					match role {
						"user" => Some(TurnKind::User),
						"assistant" => Some(TurnKind::Assistant),
						_ => None,
					}
				},
				_ => None,
			})
	})
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenAiEntry {
	pub operation_name: Option<String>,
	pub provider_name: Option<String>,
	pub request_model: Option<String>,
	pub response_model: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageEntry {
	pub input_tokens: Option<i64>,
	pub output_tokens: Option<i64>,
	pub total_tokens: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadEntry {
	pub request_prompt: Option<Value>,
	pub response_completion: Option<Value>,
}

pub(crate) fn limit(limit: Option<i64>) -> i64 {
	limit.unwrap_or(100).clamp(1, 500)
}

pub(crate) fn encode_cursor(completed_at: DateTime<Utc>, id: &str) -> String {
	format!("{}|{}", completed_at.to_rfc3339(), id)
}

pub(crate) fn decode_cursor(cursor: &str) -> anyhow::Result<(DateTime<Utc>, String)> {
	let (completed_at, id) = cursor
		.split_once('|')
		.ok_or_else(|| anyhow::anyhow!("invalid cursor"))?;
	Ok((completed_at.parse::<DateTime<Utc>>()?, id.to_string()))
}

pub(crate) fn attr_value(value: &Value) -> Option<String> {
	match value {
		Value::Null => None,
		Value::Bool(value) => Some(value.to_string()),
		Value::Number(value) => Some(value.to_string()),
		Value::String(value) => Some(value.clone()),
		Value::Array(_) | Value::Object(_) => None,
	}
}

pub(crate) fn attr_filter_values(value: &Value) -> Option<Vec<String>> {
	match value {
		Value::Array(values) => values.iter().map(attr_value).collect(),
		_ => attr_value(value).map(|value| vec![value]),
	}
}

pub(crate) fn promoted_attribute_column(key: &str) -> Option<&'static str> {
	match key {
		"agentgateway.user" => Some("agentgateway_user"),
		"agentgateway.group" => Some("agentgateway_group"),
		"user_agent.name" => Some("user_agent_name"),
		_ => None,
	}
}

pub(crate) fn analytics_window(
	time_range: Option<TimeRange>,
	bucket_count: Option<i64>,
	bucket_seconds: Option<i64>,
) -> (TimeRange, DateTime<Utc>, DateTime<Utc>, i64) {
	let now = Utc::now();
	let from = time_range.as_ref().and_then(|range| range.from);
	let to = time_range
		.as_ref()
		.and_then(|range| range.to)
		.unwrap_or(now);
	let from = from.unwrap_or_else(|| to - Duration::hours(24));
	let to = if to > from {
		to
	} else {
		from + Duration::hours(24)
	};
	let span_seconds = (to - from).num_seconds().max(1);
	let bucket_seconds = bucket_seconds
		.map(|seconds| seconds.clamp(1, span_seconds))
		.unwrap_or_else(|| {
			let bucket_count = bucket_count.unwrap_or(96).clamp(1, 500);
			((span_seconds + bucket_count - 1) / bucket_count).max(1)
		});
	(
		TimeRange {
			from: Some(from),
			to: Some(to),
		},
		from,
		to,
		bucket_seconds,
	)
}

enum Backend {
	Sqlite(sqlite::SqliteLogStore),
	Postgres(postgres::PostgresLogStore),
}

impl Backend {
	async fn connect(
		cfg: &Config,
		pool: Option<crate::database::DatabasePool>,
	) -> anyhow::Result<Self> {
		let pool = match pool {
			Some(pool) => pool,
			None => {
				crate::database::DatabasePool::connect_with_max_connections(&cfg.url, cfg.max_connections)
					.await?
			},
		};
		match pool {
			crate::database::DatabasePool::Sqlite(pool) => {
				Ok(Self::Sqlite(sqlite::SqliteLogStore::from_pool(pool).await?))
			},
			crate::database::DatabasePool::Postgres(pool) => Ok(Self::Postgres(
				postgres::PostgresLogStore::from_pool(pool).await?,
			)),
		}
	}

	async fn insert_batch(&self, records: &[StoredRequestLog]) -> anyhow::Result<()> {
		match self {
			Self::Sqlite(store) => store.insert_batch(records).await,
			Self::Postgres(store) => store.insert_batch(records).await,
		}
	}

	async fn search(&self, request: SearchRequest) -> anyhow::Result<SearchResponse> {
		match self {
			Self::Sqlite(store) => store.search(request).await,
			Self::Postgres(store) => store.search(request).await,
		}
	}

	async fn get(&self, request: GetRequest) -> anyhow::Result<GetResponse> {
		match self {
			Self::Sqlite(store) => store.get(request).await,
			Self::Postgres(store) => store.get(request).await,
		}
	}

	async fn analytics_summary(
		&self,
		request: AnalyticsSummaryRequest,
	) -> anyhow::Result<AnalyticsSummaryResponse> {
		match self {
			Self::Sqlite(store) => store.analytics_summary(request).await,
			Self::Postgres(store) => store.analytics_summary(request).await,
		}
	}

	async fn tail(&self, request: TailRequest) -> anyhow::Result<TailResponse> {
		match self {
			Self::Sqlite(store) => store.tail(request).await,
			Self::Postgres(store) => store.tail(request).await,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const FAST_RETRY: RetryBackoff = RetryBackoff {
		initial: std::time::Duration::from_millis(10),
		max: std::time::Duration::from_millis(50),
	};

	/// A SQLite database whose directory may not exist yet, which makes connecting fail.
	fn sqlite_config(
		path: &std::path::Path,
		required: Option<bool>,
		max_queued_records: Option<usize>,
	) -> Config {
		Config {
			url: format!("sqlite://{}", path.display()),
			max_connections: Some(1),
			required,
			max_queued_records,
		}
	}

	fn record(id: &str) -> PendingRequestLog {
		let now = Utc::now();
		PendingRequestLog {
			record: StoredRequestLog {
				id: id.to_string(),
				started_at: now,
				completed_at: now,
				duration_ms: 0,
				trace_id: None,
				span_id: None,
				http_status: Some(200),
				error: None,
				gen_ai_operation_name: None,
				gen_ai_provider_name: None,
				gen_ai_request_model: None,
				gen_ai_response_model: None,
				input_tokens: None,
				output_tokens: None,
				total_tokens: None,
				cost: None,
				agentgateway_user: None,
				agentgateway_group: None,
				user_agent_name: None,
				has_payload: false,
				attributes_json: "{}".into(),
				payload: None,
			},
			llm_mode: Some(crate::types::frontend::DatabaseLlmMode::Metadata),
			input_messages: None,
			llm_response: None,
		}
	}

	async fn stored_ids(store: &RequestLogStore) -> anyhow::Result<Vec<String>> {
		let response = store
			.request(|tx| LogStoreMsg::Search {
				request: SearchRequest {
					limit: Some(500),
					cursor: None,
					time_range: None,
					filters: LogFilters::default(),
					include_attributes: false,
				},
				tx,
			})
			.await?;
		let mut ids = response
			.logs
			.into_iter()
			.map(|log| log.id)
			.collect::<Vec<_>>();
		ids.sort();
		Ok(ids)
	}

	async fn wait_for_ids(store: &RequestLogStore, expected: &[&str]) {
		let deadline = Instant::now() + std::time::Duration::from_secs(60);
		loop {
			let result = stored_ids(store).await;
			if result.as_ref().is_ok_and(|ids| ids == expected) {
				return;
			}
			assert!(
				Instant::now() < deadline,
				"timed out waiting for {expected:?}; last result: {result:?}"
			);
			tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		}
	}

	#[tokio::test]
	async fn required_database_fails_startup() {
		let dir = tempfile::tempdir().unwrap();
		let cfg = sqlite_config(&dir.path().join("missing").join("logs.db"), None, None);
		assert!(cfg.is_required());
		assert!(
			start(&cfg, None, FAST_RETRY).await.is_err(),
			"an unreachable required database must fail startup"
		);
	}

	#[tokio::test]
	async fn optional_database_connects_in_background() {
		let dir = tempfile::tempdir().unwrap();
		let db_dir = dir.path().join("created-later");
		let cfg = sqlite_config(&db_dir.join("logs.db"), Some(false), Some(2));
		let (store, guard) = start(&cfg, None, FAST_RETRY)
			.await
			.expect("startup must not wait for an optional database");

		let err = stored_ids(&store)
			.await
			.expect_err("queries fail while the database is unavailable");
		assert!(
			err.to_string().contains("unavailable"),
			"unexpected error: {err}"
		);

		// Two records fit in the queue; the third is dropped.
		for id in ["a", "b", "c"] {
			store.emit(record(id));
		}
		assert_eq!(store.queue.queued.load(Ordering::Relaxed), 2);

		// The database becomes reachable: queued records are written and writes resume.
		std::fs::create_dir(&db_dir).unwrap();
		wait_for_ids(&store, &["a", "b"]).await;
		assert_eq!(store.queue.queued.load(Ordering::Relaxed), 0);
		store.emit(record("d"));
		wait_for_ids(&store, &["a", "b", "d"]).await;

		guard.shutdown_and_wait().await;
	}

	#[tokio::test]
	async fn optional_database_shuts_down_while_unavailable() {
		let dir = tempfile::tempdir().unwrap();
		let cfg = sqlite_config(
			&dir.path().join("missing").join("logs.db"),
			Some(false),
			None,
		);
		let (store, guard) = start(&cfg, None, FAST_RETRY)
			.await
			.expect("startup must not wait for an optional database");
		store.emit(record("a"));

		tokio::time::timeout(
			std::time::Duration::from_secs(10),
			guard.shutdown_and_wait(),
		)
		.await
		.expect("shutdown must not wait for the database");
		assert_eq!(store.queue.queued.load(Ordering::Relaxed), 0);
	}
}

//! Provider-quota-aware selection of LLM providers.
//!
//! A provider can declare the limits its upstream enforces (`capacity`: requests, input tokens and
//! output tokens per minute). Usage is tracked in a sliding one-minute window and turned into a
//! headroom fraction (`1 - used / limit`, minimum over the declared dimensions). Provider selection
//! prefers providers with more headroom, skips providers that have none while another provider in
//! the same priority group has some, and falls through to the next priority group when a whole
//! group is out. Low-priority requests can additionally be shed when every reachable provider is
//! below a reserve kept for high-priority traffic.
//!
//! Requests are charged when a provider is selected; tokens are charged when the response
//! completes, and in the meantime each in-flight request is assumed to cost the provider's recent
//! average. Rate-limit headers reported by the provider (OpenAI, Azure OpenAI and Anthropic send
//! them) correct the estimate, which also accounts for traffic the gateway cannot see (other
//! clients of the same key).
//!
//! Usage counters live in a process-wide [`CapacityRegistry`] keyed by backend and provider name,
//! so they survive config reloads. When `config.database` is set, every replica writes its own
//! per-slot usage to the `capacity_usage` table and reads the other replicas' usage back every
//! [`SYNC_INTERVAL`] (see `capacity_store.rs`); headroom then counts all replicas. Without a
//! database, or while it is unreachable, each replica counts only its own traffic against
//! `limit / expectedReplicas`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ::http::HeaderMap;
use agent_core::strng::RichStrng;
use parking_lot::Mutex;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;

use super::NamedAIProvider;
use crate::types::loadbalancer::{EndpointInfo, EndpointSet};
use crate::*;

/// Length of the sliding window that provider limits are expressed in.
const WINDOW: Duration = Duration::from_secs(60);
/// Granularity of the sliding window.
const SLOT: Duration = Duration::from_secs(5);
const SLOT_MS: u64 = SLOT.as_millis() as u64;
/// Slots fully inside the window; one more slot is counted in part.
const SLOTS_IN_WINDOW: u64 = WINDOW.as_secs() / SLOT.as_secs();
/// How often a replica writes its usage to the database and reads the other replicas' usage.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Usage stops being shared when the last successful sync is older than this; each replica then
/// falls back to its own traffic against `limit / expectedReplicas`.
const SYNC_STALE_AFTER: Duration = Duration::from_secs(15);
const RING: usize = SLOTS_IN_WINDOW as usize + 1;
/// Weight of the newest sample in the per-request token averages.
const AVG_ALPHA: f64 = 0.2;

const REQUESTS: usize = 0;
const INPUT: usize = 1;
const OUTPUT: usize = 2;

/// Limits enforced by the upstream provider for this provider entry, per minute.
/// Unset dimensions are not limited.
#[apply(schema!)]
pub struct ProviderCapacity {
	/// Requests per minute.
	pub rpm: Option<u64>,
	/// Input (prompt) tokens per minute.
	pub input_tpm: Option<u64>,
	/// Output (completion) tokens per minute.
	pub output_tpm: Option<u64>,
	/// Number of gateway replicas sending traffic against these limits, used while usage is not
	/// shared through `config.database` (no database configured, or no successful sync in the
	/// last 15 seconds). Each replica then allows itself `1 / expectedReplicas` of every limit.
	/// While usage is shared, replicas count each other's traffic and the full limits apply.
	/// Defaults to 1.
	pub expected_replicas: Option<u32>,
}

impl ProviderCapacity {
	fn limits(&self) -> [Option<u64>; 3] {
		[self.rpm, self.input_tpm, self.output_tpm]
	}
}

/// Whether a request may use capacity reserved for high-priority traffic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Priority {
	#[default]
	High,
	Low,
}

/// Returned when a low-priority request finds no provider above the reserve.
#[derive(Debug, thiserror::Error)]
#[error("provider capacity is reserved for high-priority requests")]
pub struct CapacityExhausted {
	/// Seconds until the usage window next drops.
	pub retry_after: u64,
}

/// A selected provider and its load-balancing state.
type Candidate = (Arc<NamedAIProvider>, Arc<EndpointInfo>);

/// Selection settings of an AI backend whose providers declare capacity.
#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapacityPolicy {
	/// CEL expression classifying a request; `"low"` marks it low priority.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub priority: Option<Arc<cel::Expression>>,
	/// Per priority group: the headroom fraction a low-priority request leaves untouched.
	pub reserve_for_high_priority: Vec<Option<f64>>,
}

impl CapacityPolicy {
	/// Validates the shedding settings of one backend.
	pub fn new(
		priority: Option<Arc<cel::Expression>>,
		reserve_for_high_priority: Vec<Option<f64>>,
	) -> anyhow::Result<Self> {
		for reserve in reserve_for_high_priority.iter().flatten() {
			anyhow::ensure!(
				(0.0..1.0).contains(reserve),
				"reserveForHighPriority must be at least 0 and less than 1, got {reserve}"
			);
		}
		let has_reserve = reserve_for_high_priority.iter().any(Option::is_some);
		anyhow::ensure!(
			has_reserve == priority.is_some(),
			"priority and reserveForHighPriority must be set together"
		);
		Ok(Self {
			priority,
			reserve_for_high_priority,
		})
	}

	fn is_configured(&self) -> bool {
		self.priority.is_some()
	}

	pub fn priority(&self, req: &crate::http::Request) -> Priority {
		let Some(expr) = &self.priority else {
			return Priority::High;
		};
		let exec = cel::Executor::new_request(req);
		match exec.eval(expr) {
			Ok(cel::Value::String(s)) if s.trim().eq_ignore_ascii_case("low") => Priority::Low,
			_ => Priority::High,
		}
	}

	fn reserve(&self, group: usize) -> f64 {
		self
			.reserve_for_high_priority
			.get(group)
			.copied()
			.flatten()
			.unwrap_or(0.0)
	}

	/// Picks a provider with headroom: the first priority group with an active provider above its
	/// floor wins, and within it P2C weighs the usual score by headroom. The floor is zero for
	/// high-priority requests and the group's reserve for low-priority ones.
	///
	/// `Ok(None)` means no provider has headroom and the caller should fall back to ordinary
	/// selection: the estimate can be wrong, and the provider's own limit has the final say.
	pub(crate) fn select(
		&self,
		providers: &EndpointSet<NamedAIProvider>,
		affinity_key: Option<u64>,
		priority: Priority,
		now: Instant,
	) -> Result<Option<Candidate>, CapacityExhausted> {
		let shed = priority == Priority::Low && self.is_configured();
		let selected = providers.select_eligible(affinity_key, |group, p| {
			let headroom = p.capacity.as_ref().map_or(1.0, |c| c.headroom_at(now));
			let floor = if shed { self.reserve(group) } else { 0.0 };
			(headroom > floor).then_some(headroom)
		});
		if selected.is_some() || !shed {
			return Ok(selected);
		}
		let mut retry_after: Option<Duration> = None;
		providers.any(|p| {
			if let Some(c) = &p.capacity {
				let r = c.retry_after_at(now);
				retry_after = Some(retry_after.map_or(r, |cur| cur.min(r)));
			}
			false
		});
		let retry_after = retry_after.unwrap_or(SLOT);
		Err(CapacityExhausted {
			retry_after: retry_after
				.as_secs()
				.saturating_add(u64::from(retry_after.subsec_nanos() != 0))
				.max(1),
		})
	}
}

#[derive(Debug, Default, Clone, Copy)]
struct Slot {
	index: u64,
	used: [u64; 3],
	/// What the database holds in this replica's row for the slot.
	flushed: [u64; 3],
}

/// The other replicas' usage in one slot, as last read from the database.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RemoteSlot {
	index: u64,
	used: [u64; 3],
}

#[derive(Debug, Default, Clone, Copy)]
struct Correction {
	/// Used fraction reported by the provider minus the estimated used fraction.
	fraction: f64,
	at: Option<Instant>,
}

#[derive(Debug, Default)]
struct State {
	/// This replica's usage.
	slots: [Slot; RING],
	/// The other replicas' usage, replaced on every successful sync.
	remote: Vec<RemoteSlot>,
	in_flight: u64,
	/// Recent average tokens per completed request, for input and output.
	avg_tokens: [Option<f64>; 3],
	/// Difference between what the provider reported and the estimate, per dimension.
	corrections: [Correction; 3],
}

/// Maps `Instant`s onto wall-clock slots so that replicas agree on slot boundaries. The wall clock
/// is read once per process; later adjustments to it do not move the slots.
#[derive(Debug, Clone, Copy)]
struct Clock {
	origin: Instant,
	origin_ms: u64,
}

impl Clock {
	fn system() -> Self {
		let origin_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(0, |d| d.as_millis() as u64);
		Self {
			origin: Instant::now(),
			origin_ms,
		}
	}

	/// A clock whose origin is a slot boundary, for counters that are never shared.
	fn aligned() -> Self {
		let clock = Self::system();
		Self {
			origin_ms: clock.origin_ms - clock.origin_ms % SLOT_MS,
			..clock
		}
	}

	fn unix_ms(&self, now: Instant) -> u64 {
		self.origin_ms + now.saturating_duration_since(self.origin).as_millis() as u64
	}

	/// Index of the slot containing `now`, and the fraction of that slot already elapsed.
	fn slot_at(&self, now: Instant) -> (u64, f64) {
		let ms = self.unix_ms(now);
		(ms / SLOT_MS, (ms % SLOT_MS) as f64 / SLOT_MS as f64)
	}
}

/// Whether usage is currently shared with the other replicas.
#[derive(Debug)]
struct SyncState {
	clock: Clock,
	/// Wall-clock milliseconds of the last successful sync; 0 if there was none.
	last_success_ms: AtomicU64,
}

impl SyncState {
	fn new(clock: Clock) -> Self {
		Self {
			clock,
			last_success_ms: AtomicU64::new(0),
		}
	}

	fn shared_at(&self, now: Instant) -> bool {
		let last = self.last_success_ms.load(Ordering::Relaxed);
		last != 0 && self.clock.unix_ms(now).saturating_sub(last) <= SYNC_STALE_AFTER.as_millis() as u64
	}
}

/// Usage counters of one provider entry. They outlive config reloads: every tracker for the same
/// backend and provider shares them.
#[derive(Debug)]
struct Usage {
	backend: Strng,
	provider: Strng,
	sync: Arc<SyncState>,
	labels: crate::telemetry::metrics::ProviderCapacityLabels,
	gauge: OnceLock<Gauge<f64, AtomicU64>>,
	state: Mutex<State>,
}

impl Usage {
	fn new(backend: &str, provider: &str, sync: Arc<SyncState>) -> Self {
		Self {
			backend: strng::new(backend),
			provider: strng::new(provider),
			sync,
			labels: crate::telemetry::metrics::ProviderCapacityLabels {
				backend: RichStrng::from(backend).into(),
				provider: RichStrng::from(provider).into(),
			},
			gauge: OnceLock::new(),
			state: Mutex::new(State::default()),
		}
	}

	/// Nothing in flight and no usage left in the window.
	fn is_idle(&self, now: Instant) -> bool {
		let (cur, _) = self.sync.clock.slot_at(now);
		let st = self.state.lock();
		st.in_flight == 0
			&& st
				.slots
				.iter()
				.all(|s| s.used == [0; 3] || cur.saturating_sub(s.index) > SLOTS_IN_WINDOW)
	}
}

/// Process-wide usage counters, keyed by backend and provider name. Config translation looks the
/// counters up here, so a reload keeps them instead of starting from zero, and the database sync
/// (`capacity_store.rs`) shares them with the other replicas.
#[derive(Debug)]
pub struct CapacityRegistry {
	/// Random per process; identifies this replica's rows in the database.
	replica_id: String,
	/// Stored beside the replica id, for debugging.
	hostname: String,
	sync: Arc<SyncState>,
	usage: Mutex<HashMap<(Strng, Strng), Arc<Usage>>>,
	database: OnceLock<crate::database::DatabasePool>,
}

static REGISTRY: LazyLock<Arc<CapacityRegistry>> =
	LazyLock::new(|| Arc::new(CapacityRegistry::new(Clock::system())));

/// The registry that config translation registers providers with.
pub fn registry() -> &'static Arc<CapacityRegistry> {
	&REGISTRY
}

/// Metrics of the capacity usage sync, registered with the other metrics.
#[derive(Debug, Default)]
pub struct SyncMetrics {
	/// 1 while usage is shared through the database, 0 otherwise.
	pub shared: Gauge,
	/// Seconds since the last successful sync.
	pub sync_age: Gauge<f64, AtomicU64>,
	pub sync_failures: Counter,
}

pub static SYNC_METRICS: LazyLock<SyncMetrics> = LazyLock::new(SyncMetrics::default);

impl CapacityRegistry {
	fn new(clock: Clock) -> Self {
		Self {
			replica_id: uuid::Uuid::new_v4().simple().to_string(),
			hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
			sync: Arc::new(SyncState::new(clock)),
			usage: Default::default(),
			database: OnceLock::new(),
		}
	}

	/// Returns a tracker for these limits. It shares its usage counters with every other tracker
	/// for the same backend and provider, including those of earlier configs.
	pub fn tracker(
		&self,
		config: ProviderCapacity,
		backend: &str,
		provider: &str,
	) -> anyhow::Result<CapacityTracker> {
		CapacityTracker::validate(&config, provider)?;
		let now = Instant::now();
		let mut usage = self.usage.lock();
		// Forget counters that no tracker uses any more once their usage has left the window.
		usage.retain(|_, u| Arc::strong_count(u) > 1 || !u.is_idle(now));
		let usage = usage
			.entry((strng::new(backend), strng::new(provider)))
			.or_insert_with(|| Arc::new(Usage::new(backend, provider, self.sync.clone())))
			.clone();
		Ok(CapacityTracker::with_usage(config, usage))
	}

	/// Whether usage is currently shared with the other replicas through the database.
	pub fn is_shared(&self) -> bool {
		self.sync.shared_at(Instant::now())
	}

	fn usages(&self) -> Vec<Arc<Usage>> {
		self.usage.lock().values().cloned().collect()
	}
}

/// The limits of one provider entry and the usage tracked against them. Every backend that routes
/// to the same provider entry shares it (for example a model and the virtual models that target
/// it).
#[derive(Debug)]
pub struct CapacityTracker {
	config: ProviderCapacity,
	limits: [Option<u64>; 3],
	/// Fraction of every limit this replica allows itself while usage is not shared.
	local_share: f64,
	usage: Arc<Usage>,
}

impl serde::Serialize for CapacityTracker {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		#[derive(serde::Serialize)]
		#[serde(rename_all = "camelCase")]
		struct Dump<'a> {
			#[serde(flatten)]
			limits: &'a ProviderCapacity,
			headroom: f64,
			shared: bool,
		}
		Dump {
			limits: &self.config,
			headroom: self.headroom(),
			shared: self.usage.sync.shared_at(Instant::now()),
		}
		.serialize(serializer)
	}
}

impl CapacityTracker {
	/// A tracker with counters of its own that are never shared.
	pub fn new(config: ProviderCapacity, backend: &str, provider: &str) -> anyhow::Result<Self> {
		Self::validate(&config, provider)?;
		let sync = Arc::new(SyncState::new(Clock::aligned()));
		Ok(Self::with_usage(
			config,
			Arc::new(Usage::new(backend, provider, sync)),
		))
	}

	fn validate(config: &ProviderCapacity, provider: &str) -> anyhow::Result<()> {
		let limits = config.limits();
		anyhow::ensure!(
			limits.iter().any(Option::is_some),
			"capacity for provider {provider} must set at least one of rpm, inputTpm, outputTpm"
		);
		anyhow::ensure!(
			limits.iter().flatten().all(|l| *l > 0),
			"capacity limits for provider {provider} must be greater than zero"
		);
		anyhow::ensure!(
			config.expected_replicas != Some(0),
			"capacity expectedReplicas for provider {provider} must be greater than zero"
		);
		Ok(())
	}

	fn with_usage(config: ProviderCapacity, usage: Arc<Usage>) -> Self {
		let limits = config.limits();
		let local_share = 1.0 / f64::from(config.expected_replicas.unwrap_or(1).max(1));
		Self {
			config,
			limits,
			local_share,
			usage,
		}
	}

	#[cfg(test)]
	pub(crate) fn origin(&self) -> Instant {
		self.usage.sync.clock.origin
	}

	fn clock(&self) -> &Clock {
		&self.usage.sync.clock
	}

	/// The limits that apply now: the full limits while usage is shared, this replica's share of
	/// them otherwise.
	fn effective_limits(&self, shared: bool) -> [Option<f64>; 3] {
		let share = if shared { 1.0 } else { self.local_share };
		self.limits.map(|l| l.map(|l| l as f64 * share))
	}

	/// Usage charged in the last minute, by this replica and, while shared, by the others. The slot
	/// that is partly outside the window is counted in proportion to its overlap, assuming its usage
	/// was spread evenly.
	fn windowed(&self, st: &State, now: Instant, shared: bool) -> [f64; 3] {
		let (cur, frac) = self.clock().slot_at(now);
		let weight = |index: u64| match cur.checked_sub(index) {
			// Another replica's clock can run slightly ahead of ours.
			None => 1.0,
			Some(a) if a < SLOTS_IN_WINDOW => 1.0,
			Some(a) if a == SLOTS_IN_WINDOW => 1.0 - frac,
			Some(_) => 0.0,
		};
		let local = st.slots.iter().map(|s| (s.index, s.used));
		let remote = st
			.remote
			.iter()
			.filter(|_| shared)
			.map(|s| (s.index, s.used));
		let mut used = [0.0; 3];
		for (index, slot) in local.chain(remote) {
			let w = weight(index);
			if w == 0.0 {
				continue;
			}
			for (u, s) in used.iter_mut().zip(slot) {
				*u += s as f64 * w;
			}
		}
		used
	}

	/// The window plus the expected tokens of this replica's requests still in flight.
	fn projected(&self, st: &State, now: Instant, shared: bool) -> [f64; 3] {
		let mut used = self.windowed(st, now, shared);
		for dim in [INPUT, OUTPUT] {
			used[dim] += st.in_flight as f64 * st.avg_tokens[dim].unwrap_or(0.0);
		}
		used
	}

	/// The estimate corrected by the latest provider report. A correction fades out over one
	/// window, as the usage it describes leaves the provider's window too.
	fn effective(
		&self,
		st: &State,
		now: Instant,
		limits: &[Option<f64>; 3],
		shared: bool,
	) -> [f64; 3] {
		let mut used = self.projected(st, now, shared);
		for ((u, c), limit) in used.iter_mut().zip(st.corrections).zip(limits) {
			let (Some(at), Some(limit)) = (c.at, limit) else {
				continue;
			};
			let age = now.saturating_duration_since(at).as_secs_f64();
			let fade = (1.0 - age / WINDOW.as_secs_f64()).max(0.0);
			*u = (*u + c.fraction * limit * fade).max(0.0);
		}
		used
	}

	/// Remaining capacity as a fraction in [0, 1]: the minimum over the limited dimensions.
	pub fn headroom(&self) -> f64 {
		self.headroom_at(Instant::now())
	}

	pub(crate) fn headroom_at(&self, now: Instant) -> f64 {
		let shared = self.usage.sync.shared_at(now);
		let limits = self.effective_limits(shared);
		let st = self.usage.state.lock();
		let used = self.effective(&st, now, &limits, shared);
		limits
			.iter()
			.zip(used)
			.filter_map(|(limit, used)| limit.map(|l| 1.0 - used / l))
			.fold(1.0_f64, f64::min)
			.clamp(0.0, 1.0)
	}

	/// Time until the oldest usage in the window starts to leave it.
	fn retry_after_at(&self, now: Instant) -> Duration {
		let shared = self.usage.sync.shared_at(now);
		let st = self.usage.state.lock();
		let clock = self.clock();
		let (cur, _) = clock.slot_at(now);
		let local = st.slots.iter().map(|s| (s.index, s.used));
		let remote = st
			.remote
			.iter()
			.filter(|_| shared)
			.map(|s| (s.index, s.used));
		let oldest = local
			.chain(remote)
			.filter(|(index, used)| {
				used.iter().any(|u| *u > 0) && cur.checked_sub(*index).is_none_or(|a| a <= SLOTS_IN_WINDOW)
			})
			.map(|(index, _)| index)
			.min();
		match oldest {
			Some(index) => {
				let leaves_ms = (index + SLOTS_IN_WINDOW) * SLOT_MS;
				Duration::from_millis(leaves_ms.saturating_sub(clock.unix_ms(now)))
					.max(Duration::from_secs(1))
			},
			None => SLOT,
		}
	}

	fn charge(&self, st: &mut State, now: Instant, dim: usize, amount: u64) {
		let (cur, _) = self.clock().slot_at(now);
		let slot = &mut st.slots[(cur % RING as u64) as usize];
		if slot.index != cur {
			*slot = Slot {
				index: cur,
				..Default::default()
			};
		}
		slot.used[dim] = slot.used[dim].saturating_add(amount);
	}

	/// Charges one request and holds an in-flight slot until the permit is settled or dropped.
	pub fn admit(self: &Arc<Self>) -> CapacityPermit {
		self.admit_at(Instant::now())
	}

	pub(crate) fn admit_at(self: &Arc<Self>, now: Instant) -> CapacityPermit {
		let mut st = self.usage.state.lock();
		self.charge(&mut st, now, REQUESTS, 1);
		st.in_flight += 1;
		CapacityPermit {
			tracker: self.clone(),
			observation: None,
			settled: false,
		}
	}

	fn settle_at(
		&self,
		now: Instant,
		input: Option<u64>,
		output: Option<u64>,
		observation: Option<RateLimitObservation>,
	) {
		let shared = self.usage.sync.shared_at(now);
		let mut st = self.usage.state.lock();
		st.in_flight = st.in_flight.saturating_sub(1);
		for (dim, tokens) in [(INPUT, input), (OUTPUT, output)] {
			let Some(tokens) = tokens else { continue };
			self.charge(&mut st, now, dim, tokens);
			let avg = &mut st.avg_tokens[dim];
			*avg = Some(match *avg {
				Some(a) => AVG_ALPHA * tokens as f64 + (1.0 - AVG_ALPHA) * a,
				None => tokens as f64,
			});
		}
		if let Some(observation) = observation {
			let limits = self.effective_limits(shared);
			let estimate = self.projected(&st, now, shared);
			for (dim, reported) in observation.0.iter().enumerate() {
				let (Some(configured), Some(limit), Some(reported)) =
					(self.limits[dim], limits[dim], reported)
				else {
					continue;
				};
				// The provider reports usage of the whole limit, from all replicas.
				let used_fraction = match reported.limit {
					Some(provider_limit) if provider_limit > 0.0 => 1.0 - reported.remaining / provider_limit,
					_ => 1.0 - reported.remaining / configured as f64,
				}
				.clamp(0.0, 1.0);
				st.corrections[dim] = Correction {
					fraction: used_fraction - estimate[dim] / limit,
					at: Some(now),
				};
			}
		}
	}

	fn release(&self) {
		let mut st = self.usage.state.lock();
		st.in_flight = st.in_flight.saturating_sub(1);
	}

	/// Publishes the current headroom on the `gen_ai_provider_capacity_headroom` gauge.
	pub fn export(&self, metrics: &crate::telemetry::metrics::Metrics) {
		let gauge = self.usage.gauge.get_or_init(|| {
			metrics
				.gen_ai_provider_capacity_headroom
				.get_or_create(&self.usage.labels)
				.clone()
		});
		gauge.set(self.headroom());
	}
}

/// Accounting for one attempt against a provider with a declared capacity. Settling it charges the
/// response's tokens and applies any rate-limit headers seen; dropping it only ends the in-flight
/// estimate.
#[derive(Debug)]
pub struct CapacityPermit {
	tracker: Arc<CapacityTracker>,
	observation: Option<RateLimitObservation>,
	settled: bool,
}

impl CapacityPermit {
	/// Records the provider's rate-limit headers, applied when the permit is settled.
	pub fn observe_headers(&mut self, headers: &HeaderMap) {
		if let Some(observation) = RateLimitObservation::parse(headers) {
			self.observation = Some(observation);
		}
	}

	pub fn settle(self, response: Option<&cel::LLMContext>) {
		self.settle_at(
			Instant::now(),
			response.and_then(|r| r.input_tokens),
			response.and_then(|r| r.output_tokens),
		)
	}

	pub(crate) fn settle_at(mut self, now: Instant, input: Option<u64>, output: Option<u64>) {
		self.settled = true;
		let observation = self.observation.take();
		self.tracker.settle_at(now, input, output, observation);
	}
}

impl Drop for CapacityPermit {
	fn drop(&mut self) {
		if !self.settled {
			self.tracker.release();
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Reported {
	remaining: f64,
	limit: Option<f64>,
}

/// Remaining capacity reported by the provider, per dimension (requests, input, output).
#[derive(Debug, Clone, Copy, PartialEq)]
struct RateLimitObservation([Option<Reported>; 3]);

impl RateLimitObservation {
	/// Reads the OpenAI / Azure OpenAI (`x-ratelimit-*-requests`, `x-ratelimit-*-tokens`) and
	/// Anthropic (`anthropic-ratelimit-*`) headers. A combined token limit applies to both token
	/// dimensions unless a dimension-specific header is present.
	fn parse(h: &HeaderMap) -> Option<Self> {
		let read = |remaining: &[&str], limit: &[&str]| {
			let first = |names: &[&str]| {
				names.iter().find_map(|n| {
					h.get(*n)
						.and_then(|v| v.to_str().ok())
						.and_then(|v| v.trim().parse::<f64>().ok())
						.filter(|v| v.is_finite() && *v >= 0.0)
				})
			};
			first(remaining).map(|remaining| Reported {
				remaining,
				limit: first(limit),
			})
		};
		let requests = read(
			&[
				"x-ratelimit-remaining-requests",
				"anthropic-ratelimit-requests-remaining",
			],
			&[
				"x-ratelimit-limit-requests",
				"anthropic-ratelimit-requests-limit",
			],
		);
		let tokens = read(
			&[
				"x-ratelimit-remaining-tokens",
				"anthropic-ratelimit-tokens-remaining",
			],
			&[
				"x-ratelimit-limit-tokens",
				"anthropic-ratelimit-tokens-limit",
			],
		);
		let input = read(
			&["anthropic-ratelimit-input-tokens-remaining"],
			&["anthropic-ratelimit-input-tokens-limit"],
		)
		.or(tokens);
		let output = read(
			&["anthropic-ratelimit-output-tokens-remaining"],
			&["anthropic-ratelimit-output-tokens-limit"],
		)
		.or(tokens);
		let observation = [requests, input, output];
		observation
			.iter()
			.any(Option::is_some)
			.then_some(Self(observation))
	}
}

#[path = "capacity_store.rs"]
mod store;

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;

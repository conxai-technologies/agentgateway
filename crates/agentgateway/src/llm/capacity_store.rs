//! Shares provider capacity usage across replicas through `config.database`.
//!
//! Every replica owns its rows of `capacity_usage`: one per provider entry and 5 s slot, holding
//! the replica's total usage in that slot (only that replica writes them, so a write is an
//! idempotent upsert of the total, not an increment). Every [`SYNC_INTERVAL`] a replica writes
//! the slots that changed since its last write, then reads the sum of every other replica's rows
//! in the window. A replica that stops or crashes stops writing: its rows leave the window within
//! a minute, so they never hold capacity longer than the provider itself would, and they are
//! deleted after two minutes.
//!
//! Lag: usage charged by another replica becomes visible here after that replica's next sync and
//! then ours, so at most 2 × `SYNC_INTERVAL` (10 s) plus database latency. A database that is
//! unreachable never fails startup or requests: replicas fall back to their local estimate once
//! the last successful sync is older than `SYNC_STALE_AFTER`.

use anyhow::Context;

use super::*;
use crate::database::DatabasePool;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS capacity_usage (
    backend TEXT NOT NULL,
    provider TEXT NOT NULL,
    replica_id TEXT NOT NULL,
    slot_start BIGINT NOT NULL,
    hostname TEXT NOT NULL,
    requests BIGINT NOT NULL DEFAULT 0,
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (backend, provider, replica_id, slot_start)
);
CREATE INDEX IF NOT EXISTS capacity_usage_slot_start ON capacity_usage (slot_start);
"#;

const SQLITE_UPSERT: &str = r#"
INSERT INTO capacity_usage
    (backend, provider, replica_id, slot_start, hostname, requests, input_tokens, output_tokens, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (backend, provider, replica_id, slot_start) DO UPDATE SET
    requests = MAX(capacity_usage.requests, excluded.requests),
    input_tokens = MAX(capacity_usage.input_tokens, excluded.input_tokens),
    output_tokens = MAX(capacity_usage.output_tokens, excluded.output_tokens),
    updated_at = excluded.updated_at
"#;

const POSTGRES_UPSERT: &str = r#"
INSERT INTO capacity_usage
    (backend, provider, replica_id, slot_start, hostname, requests, input_tokens, output_tokens, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (backend, provider, replica_id, slot_start) DO UPDATE SET
    requests = GREATEST(capacity_usage.requests, excluded.requests),
    input_tokens = GREATEST(capacity_usage.input_tokens, excluded.input_tokens),
    output_tokens = GREATEST(capacity_usage.output_tokens, excluded.output_tokens),
    updated_at = excluded.updated_at
"#;

const SQLITE_READ: &str = r#"
SELECT backend, provider, slot_start,
    CAST(SUM(requests) AS BIGINT), CAST(SUM(input_tokens) AS BIGINT), CAST(SUM(output_tokens) AS BIGINT)
FROM capacity_usage
WHERE replica_id <> ? AND slot_start >= ? AND slot_start <= ?
GROUP BY backend, provider, slot_start
"#;

const POSTGRES_READ: &str = r#"
SELECT backend, provider, slot_start,
    CAST(SUM(requests) AS BIGINT), CAST(SUM(input_tokens) AS BIGINT), CAST(SUM(output_tokens) AS BIGINT)
FROM capacity_usage
WHERE replica_id <> $1 AND slot_start >= $2 AND slot_start <= $3
GROUP BY backend, provider, slot_start
"#;

const SQLITE_PRUNE: &str = "DELETE FROM capacity_usage WHERE slot_start < ?";
const POSTGRES_PRUNE: &str = "DELETE FROM capacity_usage WHERE slot_start < $1";

/// Rows whose slot started longer ago than this are deleted; two windows, so that clock skew
/// between replicas never deletes a row that is still in someone's window.
const RETENTION: Duration = Duration::from_secs(2 * WINDOW.as_secs());

type UsageRow = (String, String, i64, i64, i64, i64);

struct PendingSlot {
	usage: Arc<Usage>,
	index: u64,
	used: [u64; 3],
}

fn to_db(v: u64) -> i64 {
	i64::try_from(v).unwrap_or(i64::MAX)
}

fn from_db(v: i64) -> u64 {
	u64::try_from(v).unwrap_or(0)
}

/// How long Postgres schema creation waits for a lock. Shorter than [`SYNC_INTERVAL`], which
/// bounds the whole sync: a stuck lock holder fails this attempt with an error, and the next tick
/// retries.
const SCHEMA_LOCK_TIMEOUT: Duration = Duration::from_secs(2);
const _: () = assert!(SCHEMA_LOCK_TIMEOUT.as_millis() < SYNC_INTERVAL.as_millis());

/// Creates the `capacity_usage` table and its index if they do not exist.
///
/// On Postgres this runs under the shared schema advisory lock
/// ([`crate::database::begin_postgres_schema_init`]): `CREATE TABLE IF NOT EXISTS` and `CREATE
/// INDEX IF NOT EXISTS` are not safe against the same statement from another replica, and the
/// loser of that race fails on a catalog unique index (`pg_type_typname_nsp_index`).
pub(super) async fn ensure_schema(pool: &DatabasePool) -> anyhow::Result<()> {
	match pool {
		DatabasePool::Sqlite(pool) => sqlx::raw_sql(SCHEMA)
			.execute(pool)
			.await
			.map(|_| ())
			.map_err(anyhow::Error::from),
		DatabasePool::Postgres(pool) => ensure_postgres_schema(pool).await,
	}
	.context("failed to initialize the capacity usage table")
}

async fn ensure_postgres_schema(pool: &sqlx::PgPool) -> anyhow::Result<()> {
	let mut tx = crate::database::begin_postgres_schema_init_within(
		pool,
		crate::database::CAPACITY_SCHEMA_LOCK,
		SCHEMA_LOCK_TIMEOUT,
	)
	.await?;
	sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
	tx.commit().await?;
	Ok(())
}

impl CapacityRegistry {
	/// Starts sharing usage through the database, in the background. This never fails: until the
	/// table exists and a sync succeeds, and whenever the database is unreachable, each replica
	/// uses its local estimate.
	pub fn attach(self: &Arc<Self>, pool: DatabasePool) {
		if self.database.set(pool.clone()).is_err() {
			return;
		}
		let registry = Arc::downgrade(self);
		tokio::spawn(async move {
			let started = Instant::now();
			let mut schema_ready = false;
			let mut failing = false;
			let mut interval = tokio::time::interval(SYNC_INTERVAL);
			interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				interval.tick().await;
				let Some(registry) = registry.upgrade() else {
					return;
				};
				let result =
					tokio::time::timeout(SYNC_INTERVAL, registry.sync_once(&pool, &mut schema_ready))
						.await
						.unwrap_or_else(|_| Err(anyhow::anyhow!("timed out after {SYNC_INTERVAL:?}")));
				match result {
					Ok(()) => {
						if failing {
							tracing::info!(target: "capacity", "sharing provider capacity usage again");
						}
						failing = false;
					},
					Err(err) => {
						SYNC_METRICS.sync_failures.inc();
						if failing {
							tracing::debug!(target: "capacity", ?err, "failed to share provider capacity usage");
						} else {
							tracing::warn!(
								target: "capacity",
								?err,
								"failed to share provider capacity usage; replicas use local estimates until the database is reachable"
							);
						}
						failing = true;
					},
				}
				registry.export_sync_metrics(Instant::now(), started);
			}
		});
	}

	async fn sync_once(&self, pool: &DatabasePool, schema_ready: &mut bool) -> anyhow::Result<()> {
		if !*schema_ready {
			ensure_schema(pool).await?;
			*schema_ready = true;
		}
		self.sync_at(pool, Instant::now()).await
	}

	/// Writes this replica's latest usage, so that the other replicas see its last seconds too.
	/// Called on shutdown.
	pub async fn flush(&self) -> anyhow::Result<()> {
		let Some(pool) = self.database.get() else {
			return Ok(());
		};
		tokio::time::timeout(SYNC_INTERVAL, self.sync_at(pool, Instant::now()))
			.await
			.unwrap_or_else(|_| Err(anyhow::anyhow!("timed out after {SYNC_INTERVAL:?}")))
	}

	fn export_sync_metrics(&self, now: Instant, started: Instant) {
		SYNC_METRICS.shared.set(i64::from(self.sync.shared_at(now)));
		let last = self.sync.last_success_ms.load(Ordering::Relaxed);
		let age = if last == 0 {
			now.saturating_duration_since(started).as_secs_f64()
		} else {
			self.sync.clock.unix_ms(now).saturating_sub(last) as f64 / 1000.0
		};
		SYNC_METRICS.sync_age.set(age);
	}

	/// One sync: writes this replica's changed slots, reads the other replicas' usage in the window,
	/// and deletes rows that are past retention.
	pub(super) async fn sync_at(&self, pool: &DatabasePool, now: Instant) -> anyhow::Result<()> {
		let clock = self.sync.clock;
		let now_ms = clock.unix_ms(now);
		let (cur, _) = clock.slot_at(now);
		let usages = self.usages();

		let pending: Vec<PendingSlot> = usages
			.iter()
			.flat_map(|usage| {
				let st = usage.state.lock();
				st.slots
					.iter()
					.filter(|s| s.used != s.flushed && cur.saturating_sub(s.index) <= SLOTS_IN_WINDOW)
					.map(|s| PendingSlot {
						usage: usage.clone(),
						index: s.index,
						used: s.used,
					})
					.collect::<Vec<_>>()
			})
			.collect();
		if !pending.is_empty() {
			match pool {
				DatabasePool::Sqlite(pool) => {
					let mut transaction = pool.begin().await?;
					for p in &pending {
						sqlx::query(SQLITE_UPSERT)
							.bind(p.usage.backend.as_str())
							.bind(p.usage.provider.as_str())
							.bind(&self.replica_id)
							.bind(to_db(p.index * SLOT_MS))
							.bind(&self.hostname)
							.bind(to_db(p.used[REQUESTS]))
							.bind(to_db(p.used[INPUT]))
							.bind(to_db(p.used[OUTPUT]))
							.bind(to_db(now_ms))
							.execute(&mut *transaction)
							.await?;
					}
					transaction.commit().await?;
				},
				DatabasePool::Postgres(pool) => {
					let mut transaction = pool.begin().await?;
					for p in &pending {
						sqlx::query(POSTGRES_UPSERT)
							.bind(p.usage.backend.as_str())
							.bind(p.usage.provider.as_str())
							.bind(&self.replica_id)
							.bind(to_db(p.index * SLOT_MS))
							.bind(&self.hostname)
							.bind(to_db(p.used[REQUESTS]))
							.bind(to_db(p.used[INPUT]))
							.bind(to_db(p.used[OUTPUT]))
							.bind(to_db(now_ms))
							.execute(&mut *transaction)
							.await?;
					}
					transaction.commit().await?;
				},
			}
			for p in &pending {
				let mut st = p.usage.state.lock();
				let slot = &mut st.slots[(p.index % RING as u64) as usize];
				if slot.index == p.index {
					slot.flushed = p.used;
				}
			}
		}

		// One slot ahead of ours, for replicas whose clock runs slightly ahead.
		let from = to_db(cur.saturating_sub(SLOTS_IN_WINDOW) * SLOT_MS);
		let to = to_db((cur + 1) * SLOT_MS);
		let rows: Vec<UsageRow> = match pool {
			DatabasePool::Sqlite(pool) => {
				sqlx::query_as(SQLITE_READ)
					.bind(&self.replica_id)
					.bind(from)
					.bind(to)
					.fetch_all(pool)
					.await?
			},
			DatabasePool::Postgres(pool) => {
				sqlx::query_as(POSTGRES_READ)
					.bind(&self.replica_id)
					.bind(from)
					.bind(to)
					.fetch_all(pool)
					.await?
			},
		};
		let mut remote: HashMap<(String, String), Vec<RemoteSlot>> = HashMap::new();
		for (backend, provider, slot_start, requests, input, output) in rows {
			remote
				.entry((backend, provider))
				.or_default()
				.push(RemoteSlot {
					index: from_db(slot_start) / SLOT_MS,
					used: [from_db(requests), from_db(input), from_db(output)],
				});
		}
		for usage in &usages {
			let slots = remote
				.remove(&(usage.backend.to_string(), usage.provider.to_string()))
				.unwrap_or_default();
			usage.state.lock().remote = slots;
		}

		let cutoff = to_db(now_ms.saturating_sub(RETENTION.as_millis() as u64));
		let pruned = match pool {
			DatabasePool::Sqlite(pool) => sqlx::query(SQLITE_PRUNE)
				.bind(cutoff)
				.execute(pool)
				.await
				.map(|_| ()),
			DatabasePool::Postgres(pool) => sqlx::query(POSTGRES_PRUNE)
				.bind(cutoff)
				.execute(pool)
				.await
				.map(|_| ()),
		};
		if let Err(err) = pruned {
			tracing::debug!(target: "capacity", ?err, "failed to delete expired capacity usage");
		}

		self
			.sync
			.last_success_ms
			.fetch_max(now_ms, Ordering::Relaxed);
		Ok(())
	}
}

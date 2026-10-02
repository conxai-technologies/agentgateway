use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{PgPool, Postgres, SqlitePool, Transaction};
use tracing::info;

use crate::{apply, schema};

mod rds_iam;

#[derive(Clone, Debug)]
pub enum DatabasePool {
	Sqlite(SqlitePool),
	Postgres(PgPool),
}

/// Default pool size when `maxConnections` is not set in config.
const DEFAULT_MAX_CONNECTIONS: u32 = 5;

/// How the gateway authenticates to a PostgreSQL database. When unset, the credentials in
/// `url` are used.
#[apply(schema!)]
#[derive(Eq, PartialEq)]
pub enum DatabaseAuth {
	/// Amazon RDS / Aurora IAM database authentication. The password is a short-lived token
	/// signed with the AWS default credential chain (for example EKS Pod Identity or IRSA). The
	/// token is renewed in the background; it is only checked when a connection is opened, so
	/// open connections are not affected when it expires. `url` must name the database user,
	/// must not contain a password, and must use `sslmode=require`, `verify-ca` or `verify-full`.
	/// Pass the RDS CA bundle with `sslrootcert=<path>` in `url` or `PGSSLROOTCERT`.
	#[serde(rename_all = "camelCase")]
	AwsRdsIam {
		/// AWS region of the database. Defaults to the region of the AWS SDK environment
		/// (`AWS_REGION` or the active profile).
		region: Option<String>,
	},
}

/// Checks that `auth` can be used with `url`, without connecting.
pub fn validate_auth(url: &str, auth: &DatabaseAuth) -> anyhow::Result<()> {
	match auth {
		DatabaseAuth::AwsRdsIam { .. } => {
			anyhow::ensure!(
				is_postgres_url(url),
				"awsRdsIam authentication requires a postgres:// or postgresql:// URL"
			);
			rds_iam::connect_options(url).map(|_| ())
		},
	}
}

fn is_postgres_url(url: &str) -> bool {
	url.starts_with("postgres://") || url.starts_with("postgresql://")
}

impl DatabasePool {
	pub async fn connect(url: &str) -> anyhow::Result<Self> {
		Self::connect_with_max_connections(url, None).await
	}

	/// Connects using every setting of a database config block.
	pub async fn connect_config(cfg: &crate::telemetry::log_store::Config) -> anyhow::Result<Self> {
		Self::connect_with_auth(&cfg.url, cfg.max_connections, cfg.auth.as_ref()).await
	}

	pub async fn connect_with_max_connections(
		url: &str,
		max_connections: Option<u32>,
	) -> anyhow::Result<Self> {
		Self::connect_with_auth(url, max_connections, None).await
	}

	async fn connect_with_auth(
		url: &str,
		max_connections: Option<u32>,
		auth: Option<&DatabaseAuth>,
	) -> anyhow::Result<Self> {
		let max_connections = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
		anyhow::ensure!(
			max_connections > 0,
			"database maxConnections must be greater than zero"
		);
		if let Some(auth) = auth {
			validate_auth(url, auth)?;
		}
		if is_postgres_url(url) {
			let pool_options = PgPoolOptions::new().max_connections(max_connections);
			let pool = match auth {
				Some(DatabaseAuth::AwsRdsIam { region }) => {
					rds_iam::connect(url, region.clone(), pool_options).await
				},
				None => pool_options.connect(url).await.map_err(Into::into),
			}
			.context("failed to connect postgres database")?;
			return Ok(Self::Postgres(pool));
		}

		let options = url
			.parse::<SqliteConnectOptions>()
			.context("failed to parse sqlite database URL")?
			.create_if_missing(true)
			.journal_mode(SqliteJournalMode::Wal)
			.synchronous(SqliteSynchronous::Normal)
			.busy_timeout(Duration::from_secs(5));
		let pool = SqlitePoolOptions::new()
			.max_connections(max_connections)
			.connect_with(options)
			.await
			.context("failed to connect sqlite database")?;
		Ok(Self::Sqlite(pool))
	}
}

/// How long schema initialization waits for a lock (the schema advisory lock, or a table lock the
/// DDL needs) before failing. Matches the request log store: a stuck holder becomes a startup
/// error instead of a silent hang that only a liveness probe ends.
const SCHEMA_LOCK_TIMEOUT: &str = "10s";

/// First key of every schema advisory lock, so that the per-store second key only has to be
/// unique among agentgateway stores. The two-key form never collides with single-key advisory
/// locks, such as the one SQLx migrations take.
const SCHEMA_LOCK_NAMESPACE: i32 = i32::from_be_bytes(*b"agwy");

/// Scoped to the transaction, so it cannot leak into the pool.
const SET_SCHEMA_LOCK_TIMEOUT: &str = "SELECT set_config('lock_timeout', $1, true)";

/// Released at commit or rollback, or when the connection drops, so an error or a crash in the
/// middle of schema initialization cannot leave it held.
const TAKE_SCHEMA_LOCK: &str = "SELECT pg_advisory_xact_lock($1, $2)";

/// Second key of the schema advisory lock for `store` (FNV-1a), stable across builds and replicas.
const fn schema_lock_key(store: &str) -> i32 {
	let bytes = store.as_bytes();
	let mut hash: u32 = 0x811c_9dc5;
	let mut i = 0;
	while i < bytes.len() {
		hash ^= bytes[i] as u32;
		hash = hash.wrapping_mul(0x0100_0193);
		i += 1;
	}
	hash as i32
}

/// Begins a transaction for creating or migrating the Postgres schema of `store`, holding an
/// advisory lock that serializes it across every replica sharing the database.
///
/// `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` are not safe against a
/// concurrent identical statement: both sessions can pass the existence check, and the loser
/// fails on a catalog unique index (for a table, `pg_type_typname_nsp_index` on its row type).
/// Under this lock a second replica waits for the first to commit, then finds everything in place.
///
/// Run the schema statements on the returned transaction and commit it.
pub async fn begin_postgres_schema_init(
	pool: &PgPool,
	store: &'static str,
) -> anyhow::Result<Transaction<'static, Postgres>> {
	info!(
		store,
		lock_timeout = SCHEMA_LOCK_TIMEOUT,
		"initializing database schema"
	);
	let mut tx = pool
		.begin()
		.await
		.with_context(|| format!("failed to begin {store} schema transaction"))?;
	sqlx::query(SET_SCHEMA_LOCK_TIMEOUT)
		.bind(SCHEMA_LOCK_TIMEOUT)
		.execute(&mut *tx)
		.await
		.with_context(|| format!("failed to configure {store} schema lock timeout"))?;
	sqlx::query(TAKE_SCHEMA_LOCK)
		.bind(SCHEMA_LOCK_NAMESPACE)
		.bind(schema_lock_key(store))
		.execute(&mut *tx)
		.await
		.with_context(|| format!("failed to lock {store} schema"))?;
	Ok(tx)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Every store that initializes a Postgres schema through [`begin_postgres_schema_init`].
	const STORES: &[&str] = &[
		crate::config_store::SCHEMA_LOCK_STORE,
		crate::http::budget::database::SCHEMA_LOCK_STORE,
	];

	#[test]
	fn schema_lock_is_transaction_scoped() {
		// A session-level lock or lock_timeout would outlive the transaction and leak into the pool.
		assert!(TAKE_SCHEMA_LOCK.contains("pg_advisory_xact_lock("));
		assert!(SET_SCHEMA_LOCK_TIMEOUT.ends_with(", true)"));
	}

	#[test]
	fn schema_lock_keys_are_stable_and_distinct() {
		// The key is shared by replicas running different builds, so it must never change.
		assert_eq!(SCHEMA_LOCK_NAMESPACE, 0x6167_7779);
		assert_eq!(schema_lock_key(""), 0x811c_9dc5_u32 as i32);
		assert_eq!(schema_lock_key("a"), 0xe40c_292c_u32 as i32);
		let mut keys: Vec<_> = STORES.iter().map(|&store| schema_lock_key(store)).collect();
		keys.sort_unstable();
		keys.dedup();
		assert_eq!(keys.len(), STORES.len(), "schema lock keys collide");
	}

	/// Starts several stores at once against one fresh Postgres schema, as replicas do on a rollout.
	/// Without the schema lock this fails intermittently with a duplicate key error.
	///
	/// CI has no Postgres server; run it with
	/// `AGW_TEST_POSTGRES_URL=postgres://... cargo test -p agentgateway concurrent_postgres_schema_init -- --ignored`.
	#[tokio::test]
	#[ignore = "needs a Postgres server in AGW_TEST_POSTGRES_URL"]
	async fn concurrent_postgres_schema_init() {
		use std::str::FromStr;
		use std::sync::Arc;

		let url = std::env::var("AGW_TEST_POSTGRES_URL").expect("AGW_TEST_POSTGRES_URL is not set");
		let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
		let schema = format!("agw_schema_lock_{}", uuid::Uuid::new_v4().simple());
		let admin = PgPool::connect_with(options.clone()).await.unwrap();
		sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
			.execute(&admin)
			.await
			.unwrap();
		let pool = PgPoolOptions::new()
			.max_connections(32)
			.connect_with(options.options([("search_path", schema.as_str())]))
			.await
			.unwrap();

		let mut tasks = tokio::task::JoinSet::new();
		for _ in 0..8 {
			let budget_pool = DatabasePool::Postgres(pool.clone());
			tasks.spawn(async move {
				Arc::new(crate::http::budget::BudgetPolicy::default())
					.initialize(budget_pool)
					.await
			});
			let config_pool = DatabasePool::Postgres(pool.clone());
			tasks.spawn(async move {
				crate::config_store::ConfigResourceStore::from_pool(config_pool)
					.await
					.map(|_| ())
			});
		}
		let results = tasks.join_all().await;

		// Not pool.close(): each config store's change listener keeps a pooled connection.
		drop(pool);
		sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
			.execute(&admin)
			.await
			.unwrap();
		for result in results {
			result.unwrap();
		}
	}
}

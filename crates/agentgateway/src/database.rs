use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{PgPool, SqlitePool};

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

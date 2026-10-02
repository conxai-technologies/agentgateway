//! Amazon RDS IAM database authentication for PostgreSQL pools.
//!
//! RDS accepts a SigV4-presigned `rds-db:connect` URL as the password of a database user that
//! has the `rds_iam` role. The token is valid for 15 minutes and is checked only when a
//! connection authenticates, so connections that are already open keep working after it
//! expires.
//!
//! sqlx 0.9 has no hook that runs before each new connection, but `Pool::set_connect_options`
//! replaces the options used for every connection opened afterwards. A dedicated thread mints
//! a fresh token every [`REFRESH_INTERVAL`] and installs it, so a new connection always uses a
//! token that has at least five minutes left. The thread owns its own single-threaded runtime
//! and AWS SDK config: the request-log writer blocks its runtime on a synchronous channel, so a
//! task spawned there would never run, and AWS HTTP connections must not be driven by a runtime
//! that can block.

use std::borrow::Cow;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{
	SignableBody, SignableRequest, SignatureLocation, SigningParams, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use aws_types::region::Region;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use tokio::sync::oneshot;
use tracing::{debug, warn};
use url::form_urlencoded;

/// SigV4 signing name for RDS IAM authentication.
const SIGNING_NAME: &str = "rds-db";
/// Lifetime of an auth token. RDS rejects anything longer than 15 minutes.
const TOKEN_LIFETIME: Duration = Duration::from_secs(15 * 60);
/// How often a new token is minted. Leaves five minutes of validity as headroom.
const REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Retry interval after a failed refresh (for example a credential provider outage).
const RETRY_INTERVAL: Duration = Duration::from_secs(15);
/// Upper bound on one credential fetch from the AWS default chain.
const CREDENTIALS_TIMEOUT: Duration = Duration::from_secs(10);

/// Parses `url` and checks that it can carry an IAM auth token.
pub(super) fn connect_options(url: &str) -> anyhow::Result<PgConnectOptions> {
	let parsed = url::Url::parse(url).context("invalid database URL")?;
	let has_query = |name: &str| parsed.query_pairs().any(|(key, _)| key == name);
	anyhow::ensure!(
		parsed.password().is_none() && !has_query("password"),
		"awsRdsIam authentication uses an IAM auth token as the password; remove the password from the database URL"
	);
	anyhow::ensure!(
		!parsed.username().is_empty() || has_query("user"),
		"awsRdsIam authentication requires the database user in the URL, for example postgres://<user>@<host>:5432/<database>"
	);
	let options: PgConnectOptions = url.parse().context("invalid postgres database URL")?;
	let ssl_mode = options.get_ssl_mode();
	anyhow::ensure!(
		matches!(
			ssl_mode,
			PgSslMode::Require | PgSslMode::VerifyCa | PgSslMode::VerifyFull
		),
		"awsRdsIam authentication sends the auth token as a password and requires TLS; set sslmode=verify-full (recommended), verify-ca or require in the database URL (found {ssl_mode:?})"
	);
	anyhow::ensure!(
		!options.get_host().starts_with('/'),
		"awsRdsIam authentication requires a TCP host, not a Unix socket"
	);
	Ok(options)
}

/// The database endpoint and user a token is signed for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Target {
	host: String,
	port: u16,
	user: String,
}

impl Target {
	fn new(options: &PgConnectOptions) -> Self {
		Self {
			host: options.get_host().to_string(),
			port: options.get_port(),
			user: options.get_username().to_string(),
		}
	}

	fn authority(&self) -> String {
		if self.host.contains(':') {
			format!("[{}]:{}", self.host, self.port)
		} else {
			format!("{}:{}", self.host, self.port)
		}
	}
}

/// Builds an RDS IAM auth token: a presigned `GET https://<host>:<port>/?Action=connect&DBUser=<user>`
/// URL without its scheme, matching what the AWS SDKs and `aws rds generate-db-auth-token`
/// produce.
pub(super) fn generate_token(
	target: &Target,
	region: &str,
	credentials: &Credentials,
	now: SystemTime,
) -> anyhow::Result<String> {
	let identity = credentials.clone().into();
	let mut settings = SigningSettings::default();
	settings.signature_location = SignatureLocation::QueryParams;
	settings.expires_in = Some(TOKEN_LIFETIME);
	let params: SigningParams<'_> = v4::SigningParams::builder()
		.identity(&identity)
		.region(region)
		.name(SIGNING_NAME)
		.time(now)
		.settings(settings)
		.build()
		.context("failed to build RDS auth token signing parameters")?
		.into();

	let authority = target.authority();
	let query = form_urlencoded::Serializer::new(String::new())
		.append_pair("Action", "connect")
		.append_pair("DBUser", &target.user)
		.finish();
	let request = SignableRequest::new(
		"GET",
		Cow::Owned(format!("https://{authority}/?{query}")),
		std::iter::empty(),
		SignableBody::Bytes(&[]),
	)
	.context("failed to build RDS auth token request")?;
	let (instructions, _signature) = sign(request, &params)
		.context("failed to sign RDS auth token")?
		.into_parts();
	let (_headers, signed_params) = instructions.into_parts();

	let mut query = form_urlencoded::Serializer::new(query);
	for (name, value) in &signed_params {
		query.append_pair(name, value);
	}
	Ok(format!("{authority}/?{}", query.finish()))
}

/// Mints tokens for one database target from the AWS default credential chain.
struct Signer {
	target: Target,
	region: String,
	credentials: SharedCredentialsProvider,
}

impl Signer {
	async fn load(target: Target, region: Option<String>) -> anyhow::Result<Self> {
		let mut loader = aws_config::defaults(BehaviorVersion::v2026_01_12());
		if let Some(region) = region {
			loader = loader.region(Region::new(region));
		}
		let sdk = loader.load().await;
		let region = sdk
			.region()
			.map(|region| region.as_ref().to_string())
			.context(
				"no AWS region for awsRdsIam authentication; set auth.awsRdsIam.region or AWS_REGION",
			)?;
		let credentials = sdk
			.credentials_provider()
			.context("no AWS credentials provider found for awsRdsIam authentication")?;
		Ok(Self {
			target,
			region,
			credentials,
		})
	}

	async fn token(&self) -> anyhow::Result<String> {
		let credentials =
			tokio::time::timeout(CREDENTIALS_TIMEOUT, self.credentials.provide_credentials())
				.await
				.context("AWS credential fetch timed out")?
				.context("failed to load AWS credentials")?;
		generate_token(&self.target, &self.region, &credentials, SystemTime::now())
	}
}

/// Opens a pool whose connections authenticate with RDS IAM auth tokens, and starts the thread
/// that keeps the pool's token fresh.
pub(super) async fn connect(
	url: &str,
	region: Option<String>,
	pool_options: PgPoolOptions,
) -> anyhow::Result<PgPool> {
	let options = connect_options(url)?;
	let target = Target::new(&options);
	let (token_tx, token_rx) = oneshot::channel();
	let (pool_tx, pool_rx) = oneshot::channel();
	let refresh_options = options.clone();
	std::thread::Builder::new()
		.name("rds-iam-token".to_string())
		.spawn(move || {
			let runtime = match tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
			{
				Ok(runtime) => runtime,
				Err(err) => {
					let _ = token_tx.send(Err(anyhow::Error::new(err)));
					return;
				},
			};
			runtime.block_on(refresh_tokens(
				target,
				region,
				refresh_options,
				token_tx,
				pool_rx,
			));
		})
		.context("failed to start the RDS IAM token thread")?;

	let token = token_rx
		.await
		.map_err(|_| anyhow::anyhow!("RDS IAM token thread stopped during startup"))?
		.context("failed to create an RDS IAM auth token")?;
	let pool = pool_options.connect_with(options.password(&token)).await?;
	// If the connect failed, dropping `pool_tx` stops the thread.
	let _ = pool_tx.send(pool.clone());
	Ok(pool)
}

async fn refresh_tokens(
	target: Target,
	region: Option<String>,
	options: PgConnectOptions,
	token_tx: oneshot::Sender<anyhow::Result<String>>,
	pool_rx: oneshot::Receiver<PgPool>,
) {
	let signer = match Signer::load(target, region).await {
		Ok(signer) => signer,
		Err(err) => {
			let _ = token_tx.send(Err(err));
			return;
		},
	};
	let first = signer.token().await;
	let failed = first.is_err();
	if token_tx.send(first).is_err() || failed {
		return;
	}
	let Ok(pool) = pool_rx.await else {
		return;
	};
	let mut wait = REFRESH_INTERVAL;
	loop {
		tokio::select! {
			_ = pool.close_event() => return,
			_ = tokio::time::sleep(wait) => {},
		}
		match signer.token().await {
			Ok(token) => {
				pool.set_connect_options(options.clone().password(&token));
				wait = REFRESH_INTERVAL;
				debug!("refreshed RDS IAM auth token");
			},
			Err(err) => {
				warn!(
					?err,
					"failed to refresh RDS IAM auth token; open connections are unaffected, new connections fail once the current token expires"
				);
				wait = RETRY_INTERVAL;
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;

	const HOST: &str = "llm-gateway.cluster-abc123.eu-central-1.rds.amazonaws.com";
	const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
	/// 2026-01-02T03:04:05Z
	const NOW: u64 = 1_767_323_045;

	fn target() -> Target {
		Target {
			host: HOST.to_string(),
			port: 5432,
			user: "llm_gateway".to_string(),
		}
	}

	fn now() -> SystemTime {
		SystemTime::UNIX_EPOCH + Duration::from_secs(NOW)
	}

	fn query(token: &str) -> BTreeMap<String, String> {
		let (_, query) = token.split_once("/?").expect("token has a query");
		form_urlencoded::parse(query.as_bytes())
			.into_owned()
			.collect()
	}

	// Expected values come from botocore 1.43 `generate_db_auth_token` with the same
	// credentials, endpoint and a clock frozen at NOW.
	#[test]
	fn token_matches_aws_sdk() {
		let credentials = Credentials::new("AKIDEXAMPLE", SECRET, None, None, "test");
		let token = generate_token(&target(), "eu-central-1", &credentials, now()).unwrap();
		assert_eq!(
			token,
			"llm-gateway.cluster-abc123.eu-central-1.rds.amazonaws.com:5432/?Action=connect&DBUser=llm_gateway\
			&X-Amz-Algorithm=AWS4-HMAC-SHA256\
			&X-Amz-Credential=AKIDEXAMPLE%2F20260102%2Feu-central-1%2Frds-db%2Faws4_request\
			&X-Amz-Date=20260102T030405Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host\
			&X-Amz-Signature=d3ac02d36d7144a4bc2deb6140aa47ac6c00ce8041f1f19b8b515baa1d54ee3d"
		);
	}

	#[test]
	fn token_signs_session_token() {
		let credentials = Credentials::new(
			"AKIDEXAMPLE",
			SECRET,
			Some("SESSIONTOKEN/with+chars=".to_string()),
			None,
			"test",
		);
		let token = generate_token(&target(), "eu-central-1", &credentials, now()).unwrap();
		assert!(
			token.starts_with(&format!("{HOST}:5432/?Action=connect&DBUser=llm_gateway&")),
			"{token}"
		);
		let params = query(&token);
		assert_eq!(
			params.get("X-Amz-Security-Token").map(String::as_str),
			Some("SESSIONTOKEN/with+chars=")
		);
		assert_eq!(
			params.get("X-Amz-Signature").map(String::as_str),
			Some("ed2f2370a4da84eb4bf8c2d99598201fc529fcb5453cb3455efe59cd6cc0da58")
		);
		assert_eq!(params.len(), 9, "{params:?}");
	}

	#[test]
	fn token_brackets_ipv6_hosts() {
		let target = Target {
			host: "fd00::1".to_string(),
			port: 5432,
			user: "llm_gateway".to_string(),
		};
		let credentials = Credentials::new("AKIDEXAMPLE", SECRET, None, None, "test");
		let token = generate_token(&target, "eu-central-1", &credentials, now()).unwrap();
		assert!(
			token.starts_with("[fd00::1]:5432/?Action=connect&"),
			"{token}"
		);
	}

	#[test]
	fn connect_options_reads_target_from_url() {
		let options = connect_options(&format!(
			"postgres://llm_gateway@{HOST}:5432/llm_gateway?sslmode=verify-full&sslrootcert=/etc/rds/global-bundle.pem"
		))
		.unwrap();
		assert_eq!(Target::new(&options), target());
		assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
	}

	#[test]
	fn connect_options_requires_tls() {
		for mode in ["disable", "allow", "prefer"] {
			let err = connect_options(&format!(
				"postgres://llm_gateway@{HOST}:5432/llm_gateway?sslmode={mode}"
			))
			.unwrap_err();
			assert!(err.to_string().contains("requires TLS"), "{mode}: {err}");
		}
		for mode in ["require", "verify-ca", "verify-full"] {
			connect_options(&format!(
				"postgres://llm_gateway@{HOST}:5432/llm_gateway?sslmode={mode}"
			))
			.unwrap_or_else(|err| panic!("{mode}: {err}"));
		}
	}

	#[test]
	fn connect_options_rejects_password_and_missing_user() {
		for url in [
			format!("postgres://llm_gateway:secret@{HOST}/llm_gateway?sslmode=require"),
			format!("postgres://llm_gateway@{HOST}/llm_gateway?sslmode=require&password=secret"),
		] {
			let err = connect_options(&url).unwrap_err();
			assert!(
				err.to_string().contains("remove the password"),
				"{url}: {err}"
			);
		}
		let err =
			connect_options(&format!("postgres://{HOST}/llm_gateway?sslmode=require")).unwrap_err();
		assert!(err.to_string().contains("database user"), "{err}");
	}
}

use ::http::HeaderMap;
use itertools::Itertools;

use crate::cel::{Executor, Expression};
use crate::http::localratelimit::{self, RateLimitType};
use crate::http::remoteratelimit::proto::rate_limit_descriptor::Entry;
use crate::http::remoteratelimit::proto::rate_limit_service_client::RateLimitServiceClient;
use crate::http::remoteratelimit::proto::{RateLimitDescriptor, RateLimitRequest};
use crate::http::{PolicyResponse, Request, envoy_proto_common};
use crate::proxy::ProxyError;
use crate::proxy::httpproxy::PolicyClient;
use crate::telemetry::metrics::{OutboundCallKind, OutboundCallSubtype};
use crate::types::agent::SimpleBackendReferenceWithPolicies;
use crate::*;

#[cfg(test)]
#[path = "remoteratelimit_tests.rs"]
mod tests;

#[allow(warnings)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod proto {
	pub use protos::envoy::service::common::v3::HeaderValue;
	pub use protos::envoy::service::ratelimit::v3::*;
}

/// Defines how the proxy behaves when the remote rate limit service is
/// unavailable or returns an error.
///
/// Defaults to `FailClosed`. When failing closed, a 500 Internal Server Error
/// is returned when the service is unavailable. When failing open, requests are
/// allowed through despite the service failure.
///
/// # Configuration
///
/// Both camelCase (`failOpen`, `failClosed`) and PascalCase (`FailOpen`,
/// `FailClosed`) are accepted in configuration files
#[apply(schema!)]
#[cfg_attr(feature = "schema", schemars(rename = "RemoteRateLimitFailureMode"))]
#[derive(Default, Copy, PartialEq, Eq)]
pub enum FailureMode {
	/// Deny the request with a 500 status when the rate limit service is unavailable (default).
	#[default]
	#[serde(rename = "failClosed", alias = "FailClosed")]
	FailClosed,
	/// Allow the request through when the rate limit service is unavailable.
	#[serde(rename = "failOpen", alias = "FailOpen")]
	FailOpen,
}

/// Defines what happens to a descriptor when one of its expressions (an entry value, `cost`, or
/// `limitOverride`) fails to evaluate or returns a value of the wrong type, for example because
/// the request lacks a header the descriptor reads.
#[apply(schema!)]
#[cfg_attr(feature = "schema", schemars(rename = "RemoteRateLimitOnError"))]
#[derive(Default, Copy, PartialEq, Eq)]
pub enum OnError {
	/// Drop the whole descriptor and send the remaining ones (default). If no descriptor remains,
	/// the rate limit service is not called and the request is allowed.
	#[default]
	Drop,
	/// Ignore only the expression that failed: a failed entry is left out of the descriptor, a
	/// failed `cost` falls back to the default cost, and a failed `limitOverride` is not sent.
	/// A descriptor left without entries is dropped.
	Skip,
	/// Reject the request with a 500 status, as when the rate limit service fails under
	/// `failClosed`. Use this when the descriptor enforces a limit that must not be bypassed.
	/// `tokens` costs evaluated after the response can no longer reject the request; there the
	/// descriptor is dropped.
	Error,
}

#[apply(schema!)]
pub struct RemoteRateLimit {
	/// Rate limit domain sent to the remote rate limit service.
	pub domain: String,
	/// Backend that receives rate limit checks and policies used when connecting to it.
	#[serde(flatten)]
	pub target: SimpleBackendReferenceWithPolicies,
	/// Descriptors sent to the remote rate limit service.
	pub descriptors: Arc<DescriptorSet>,
	/// Behavior when the remote rate limit service is unavailable or returns an error.
	/// Defaults to failClosed, denying requests with a 500 status on service failure.
	#[serde(default)]
	pub failure_mode: FailureMode,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor(pub String, pub cel::Expression);

#[apply(schema!)]
pub struct DescriptorSet(pub Vec<DescriptorEntry>);

#[apply(schema!)]
pub struct DescriptorEntry {
	/// Descriptor key/value entries. Values are CEL expressions evaluated from the request.
	#[serde(deserialize_with = "de_descriptors")]
	#[cfg_attr(feature = "schema", schemars(with = "Vec<KV>"))]
	pub entries: Arc<Vec<Descriptor>>,
	/// Whether this descriptor limits requests or LLM tokens.
	#[serde(default)]
	#[serde(rename = "type")]
	pub limit_type: RateLimitType,
	/// cost determines the optional expression to determine the cost of the request.
	/// If unset, type `requests` defaults to `1`, and type `tokens` defaults to `llm.totalTokens`.
	/// If the expression fails to evaluate, `onError` decides what happens.
	/// Costs for type `requests` are evaluated during request processing. Costs for type `tokens`
	/// are evaluated upon request completion.
	pub cost: Option<Arc<cel::Expression>>,
	/// limitOverride determines the optional expression to determine the limit of the request.
	/// This tells the remote server what limit to apply to the request.
	/// Note: this does not specify the *cost* of the request, which is done by the `cost` field.
	/// The expression must evaluate to a map with `unit` and `requestsPerUnit` keys. For example:
	/// `{"unit":"second","requestsPerUnit":100}`.
	/// Valid units: second, minute, hour, day, month, year
	/// If the expression fails to evaluate, `onError` decides what happens.
	pub limit_override: Option<Arc<cel::Expression>>,
	/// What to do when an expression of this descriptor fails to evaluate.
	/// Defaults to `drop`, which leaves the descriptor out of the check. Use `error` to reject
	/// the request instead, so a missing attribute cannot bypass the limit.
	#[serde(default)]
	pub on_error: OnError,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DescriptorLimitOverride {
	unit: String,
	#[serde(alias = "requests_per_unit")]
	requests_per_unit: u32,
}

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
struct KV {
	/// Descriptor entry key sent to the remote rate limit service.
	key: String,
	/// CEL expression used to compute the descriptor entry value.
	value: String,
}

fn de_descriptors<'de: 'a, 'a, D>(deserializer: D) -> Result<Arc<Vec<Descriptor>>, D::Error>
where
	D: Deserializer<'de>,
{
	let raw = Vec::<KV>::deserialize(deserializer)?;
	let parsed: Vec<_> = raw
		.into_iter()
		.map(|i| cel::Expression::new_strict(i.value).map(|v| Descriptor(i.key, v)))
		.collect::<Result<_, _>>()
		.map_err(|e| serde::de::Error::custom(e.to_string()))?;
	Ok(Arc::new(parsed))
}

#[derive(Debug)]
pub struct LLMResponseAmend {
	base: RemoteRateLimit,
	client: PolicyClient,
	request: proto::RateLimitRequest,
	descriptor_costs: Vec<DescriptorCost>,
}

/// The response-side cost of a descriptor sent on the request path, with what to do if it fails.
type DescriptorCost = (Option<Arc<Expression>>, OnError);

impl LLMResponseAmend {
	pub fn amend_tokens(mut self, default_tokens: i64, exec: &Executor) {
		Self::apply_token_amend(
			&mut self.request,
			&self.descriptor_costs,
			default_tokens,
			exec,
		);
		if self.request.descriptors.is_empty() {
			debug!(
				domain = %self.request.domain,
				"skipping remote rate limit token amendment because no descriptors remain"
			);
			return;
		}
		tokio::task::spawn(async move {
			let _ = self.base.check_internal(self.client, self.request).await;
		});
	}

	fn apply_token_amend(
		request: &mut proto::RateLimitRequest,
		descriptor_costs: &[DescriptorCost],
		default_tokens: i64,
		exec: &Executor,
	) {
		let domain = request.domain.clone();
		let descriptors = std::mem::take(&mut request.descriptors);
		request.descriptors = descriptors
			.into_iter()
			.zip(descriptor_costs.iter())
			.filter_map(|(mut d, (cost, on_error))| {
				// if there is a cost expression, run it.
				let cost = match eval_cost(exec, cost.as_deref(), None) {
					Ok(cost) => cost,
					Err(error) if *on_error == OnError::Skip => {
						debug!(
							domain = %domain,
							%error,
							"remote rate limit token cost expression failed; using the default cost"
						);
						None
					},
					Err(error) => {
						// The request has completed, so even `onError: error` can only drop the descriptor.
						debug!(
							domain = %domain,
							%error,
							"remote rate limit token cost expression failed; skipping descriptor"
						);
						return None;
					},
				};
				d.hits_addend = match cost {
					Some(cost) => Some(cost),
					// We cannot currently do negative amendments, so if its negative just skip
					// The input is not the cost, but the delta, so if we get -5 we should have a cost of 5
					None => Some(default_tokens.try_into().ok()?),
				};
				Some(d)
			})
			.collect();
	}
}

impl RemoteRateLimit {
	/// Build a rate-limit request by evaluating all descriptor entries of the
	/// given `limit_type` against the incoming HTTP request.
	///
	/// A descriptor whose CEL expressions fail to evaluate is handled according to its
	/// `on_error`: by default it is dropped (matching Envoy's per-descriptor "all-or-nothing"
	/// semantics), with `Skip` only the failed expression is ignored, and with `Error` the
	/// request is rejected. Returns `None` only when **no** descriptor could be
	/// successfully resolved, so the gRPC call is skipped entirely.
	fn build_request(
		&self,
		req: &http::Request,
		limit_type: RateLimitType,
		default_cost: Option<u64>,
	) -> Result<Option<(RateLimitRequest, Vec<DescriptorCost>)>, ProxyError> {
		let mut descriptors = Vec::with_capacity(self.descriptors.0.len());
		let exec = Executor::new_request(req);
		let candidate_count = self
			.descriptors
			.0
			.iter()
			.filter(|e| e.limit_type == limit_type)
			.count();
		trace!(
			"ratelimit build_request start: domain={}, type={:?}, cost={:?}, candidates={}",
			self.domain, limit_type, default_cost, candidate_count
		);

		let mut descriptor_costs = vec![];
		for desc_entry in self
			.descriptors
			.0
			.iter()
			.filter(|e| e.limit_type == limit_type)
		{
			match self.eval_descriptor_entry(&exec, desc_entry, limit_type, default_cost) {
				Ok(Some(descriptor)) => {
					descriptors.push(descriptor);
					descriptor_costs.push((desc_entry.cost.clone(), desc_entry.on_error));
				},
				Ok(None) => {},
				Err(error) if desc_entry.on_error == OnError::Error => {
					debug!(
						domain = %self.domain,
						?limit_type,
						error = %format_args!("{error:#}"),
						"remote rate limit descriptor failed to evaluate; rejecting request (onError: error)"
					);
					return Err(ProxyError::RateLimitFailed);
				},
				Err(error) => {
					trace!(
						"ratelimit descriptor evaluation failed for domain={}, type={:?}, skipping descriptor: {}: {}",
						self.domain,
						limit_type,
						desc_entry
							.entries
							.iter()
							.map(|d| format!("{}={:?}", d.0, d.1))
							.join(", "),
						format_args!("{error:#}")
					);
				},
			}
		}

		if descriptors.is_empty() {
			trace!(
				"ratelimit all descriptors failed evaluation for domain={}, type={:?}, skipping rate-limit call",
				self.domain, limit_type,
			);
			return Ok(None);
		}

		trace!(
			"ratelimit built request descriptors (domain: {}, type: {:?}): count={}",
			self.domain,
			limit_type,
			descriptors.len()
		);

		Ok(Some((
			proto::RateLimitRequest {
				domain: self.domain.clone(),
				descriptors,
				// Ignored; we always set the per-descriptor one which allows distinguishing empty vs 0
				hits_addend: 0,
			},
			descriptor_costs,
		)))
	}

	/// Evaluate one descriptor. Returns `Ok(None)` when it has no entries to send, and an error
	/// when one of its expressions failed and `on_error` is not `Skip`.
	fn eval_descriptor_entry(
		&self,
		exec: &cel::Executor<'_>,
		desc_entry: &DescriptorEntry,
		limit_type: RateLimitType,
		default_cost: Option<u64>,
	) -> anyhow::Result<Option<RateLimitDescriptor>> {
		let skip = desc_entry.on_error == OnError::Skip;
		let rl_entries = Self::eval_descriptor(exec, &desc_entry.entries, skip)?;
		// Rate limit servers require each descriptor to have at least one entry.
		if rl_entries.is_empty() {
			trace!(
				"ratelimit skipping descriptor with no entries for domain={}, type={:?}",
				self.domain, limit_type,
			);
			return Ok(None);
		}
		// Trace evaluated descriptor key/value pairs for visibility
		let kv_pairs: Vec<String> = rl_entries
			.iter()
			.map(|e| format!("{}={}", e.key, e.value))
			.collect();
		trace!(
			"ratelimit evaluated descriptors (domain: {}, type: {:?}): {}",
			self.domain,
			limit_type,
			kv_pairs.join(", ")
		);
		let hits_addend = if desc_entry.cost.is_some() && limit_type == RateLimitType::Tokens {
			// Skip sending anything on the target request side; the cost computation is specified to be on the response (amend) side
			Some(0)
		} else {
			match eval_cost(exec, desc_entry.cost.as_deref(), default_cost) {
				Ok(hits_addend) => hits_addend,
				Err(e) if skip => {
					trace!(
						"ratelimit cost evaluation failed for domain={}, type={:?}, expr={:?}, error={}; using the default cost",
						self.domain, limit_type, desc_entry.cost, e
					);
					default_cost
				},
				Err(e) => return Err(e.context("cost")),
			}
		};

		let limit = match Self::eval_limit_override(exec, desc_entry.limit_override.as_deref()) {
			Ok(limit) => limit,
			Err(e) if skip => {
				trace!(
					"ratelimit limit override evaluation failed for domain={}, type={:?}, expr={:?}, error={}; sending no override",
					self.domain, limit_type, desc_entry.limit_override, e
				);
				None
			},
			Err(e) => return Err(e.context("limitOverride")),
		};
		Ok(Some(RateLimitDescriptor {
			entries: rl_entries,
			limit,
			hits_addend,
		}))
	}

	pub async fn check_llm(
		&self,
		client: PolicyClient,
		req: &mut Request,
		default_cost: u64,
	) -> Result<(PolicyResponse, Option<LLMResponseAmend>), ProxyError> {
		if !self
			.descriptors
			.0
			.iter()
			.any(|d| d.limit_type == RateLimitType::Tokens)
		{
			// Nothing to do
			trace!(
				"ratelimit: no token descriptors configured for domain={}, skipping",
				self.domain
			);
			return Ok((PolicyResponse::default(), None));
		}
		// We usually don't have any information at this point.
		// If they have an explicit `cost` expression, it is specified to be on output; send only a '0' cost here.
		// If they have tokenization enabled, we have an explicit cost to send, so we can send it.
		// Else send '0'.
		let Some((request, descriptor_costs)) =
			self.build_request(req, RateLimitType::Tokens, Some(default_cost))?
		else {
			return Ok((PolicyResponse::default(), None));
		};
		let cr = self.check_internal(client.clone(), request.clone()).await;
		let r = LLMResponseAmend {
			base: self.clone(),
			client,
			request,
			descriptor_costs,
		};

		match cr {
			Ok(resp) => Self::apply(req, resp).map(|x| (x, Some(r))),
			Err(e) => {
				if self.failure_mode == FailureMode::FailOpen {
					Ok((PolicyResponse::default(), Some(r)))
				} else {
					Err(e)
				}
			},
		}
	}

	pub async fn check(
		&self,
		client: PolicyClient,
		req: &mut Request,
	) -> Result<PolicyResponse, ProxyError> {
		// This is on the request path
		if !self
			.descriptors
			.0
			.iter()
			.any(|d| d.limit_type == RateLimitType::Requests)
		{
			// Nothing to do
			trace!(
				"ratelimit: no request descriptors configured for domain={}, skipping",
				self.domain
			);
			return Ok(PolicyResponse::default());
		}
		let Some((request, _)) = self.build_request(req, RateLimitType::Requests, None)? else {
			return Ok(PolicyResponse::default());
		};
		match self.check_internal(client, request).await {
			Ok(cr) => Self::apply(req, cr),
			Err(e) => {
				if self.failure_mode == FailureMode::FailOpen {
					Ok(PolicyResponse::default())
				} else {
					Err(e)
				}
			},
		}
	}

	async fn check_internal(
		&self,
		client: PolicyClient,
		request: proto::RateLimitRequest,
	) -> Result<proto::RateLimitResponse, ProxyError> {
		trace!("connecting to {:?}", self.target.target);
		trace!(
			"ratelimit request summary (domain: {}): descriptors={} {}",
			request.domain,
			request.descriptors.len(),
			request
				.descriptors
				.iter()
				.map(|d| {
					let kvs: Vec<String> = d
						.entries
						.iter()
						.map(|e| format!("{}={}", e.key, e.value))
						.collect();
					format!("[hits_addend={:?}; {}]", d.hits_addend, kvs.join(", "))
				})
				.join(" | ")
		);
		let policy_client =
			client.with_outbound(OutboundCallKind::Policy, OutboundCallSubtype::RateLimit);
		let chan = self.target.grpc_channel(policy_client.clone());
		let mut client = RateLimitServiceClient::new(chan);
		let mut request = tonic::Request::new(request);
		let mut span = policy_client.start_grpc_span(
			&mut request,
			self.target.target.as_ref(),
			"/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
		);
		let resp = client.should_rate_limit(request).await;
		if let Some(span) = span.as_deref_mut() {
			span.record_grpc_result(&resp);
		}
		trace!("check response: {:?}", resp);
		if let Err(ref error) = resp {
			let ignore = self.failure_mode == FailureMode::FailOpen;
			warn!(
				"ratelimit service failed (domain: {}): {:?}; {}",
				self.domain,
				error,
				if ignore {
					"failure will be ignored (failure_mode: failOpen)"
				} else {
					"denying request (failure_mode: failClosed)"
				}
			);
		}
		let cr = resp.map_err(|_| ProxyError::RateLimitFailed)?;

		let cr = cr.into_inner();
		Ok(cr)
	}

	fn apply(req: &mut Request, cr: proto::RateLimitResponse) -> Result<PolicyResponse, ProxyError> {
		let proto::RateLimitResponse {
			overall_code,
			statuses,
			response_headers_to_add,
			request_headers_to_add,
			raw_body,
			..
		} = cr;
		let mut res = PolicyResponse::default();
		// if not OK, deny; ProxyError::into_response_with_grpc builds the 429 from this error
		if overall_code != (proto::rate_limit_response::Code::Ok as i32) {
			let mut hm = HeaderMap::new();
			process_headers(&mut hm, response_headers_to_add);
			process_ratelimit_status_headers(&mut hm, &statuses, true);
			return Err(ProxyError::RemoteRateLimitExceeded {
				status: ratelimit_status(&statuses),
				raw_body,
				response_headers: Box::new(hm),
			});
		}

		process_headers(req.headers_mut(), request_headers_to_add);
		// Surface the standard x-ratelimit-* headers on allowed responses so clients can self-throttle.
		let mut hm = HeaderMap::new();
		process_headers(&mut hm, response_headers_to_add);
		process_ratelimit_status_headers(&mut hm, &statuses, false);
		if !hm.is_empty() {
			res.response_headers = Some(hm);
		}
		Ok(res)
	}

	fn eval_limit_override(
		exec: &cel::Executor<'_>,
		limit_override: Option<&Expression>,
	) -> anyhow::Result<Option<proto::rate_limit_descriptor::RateLimitOverride>> {
		let Some(expr) = limit_override else {
			return Ok(None);
		};

		let raw = exec
			.eval(expr)?
			.json()
			.map_err(|_| cel::Error::JsonConvert)?;
		let override_config: DescriptorLimitOverride = serde_json::from_value(raw)?;
		let unit = match override_config.unit.to_ascii_lowercase().as_str() {
			"second" => proto::RateLimitUnit::Second,
			"minute" => proto::RateLimitUnit::Minute,
			"hour" => proto::RateLimitUnit::Hour,
			"day" => proto::RateLimitUnit::Day,
			"month" => proto::RateLimitUnit::Month,
			"year" => proto::RateLimitUnit::Year,
			unit => anyhow::bail!("invalid limit override unit: {unit}"),
		};
		Ok(Some(proto::rate_limit_descriptor::RateLimitOverride {
			requests_per_unit: override_config.requests_per_unit,
			unit: unit as i32,
		}))
	}

	/// Evaluate the entries of a descriptor. A failed entry fails the whole descriptor, unless
	/// `skip` is set, in which case the entry is left out.
	fn eval_descriptor(
		exec: &cel::Executor<'_>,
		entries: &Vec<Descriptor>,
		skip: bool,
	) -> anyhow::Result<Vec<Entry>> {
		let mut rl_entries = Vec::with_capacity(entries.len());
		for Descriptor(k, lookup) in entries {
			let value = exec
				.eval(lookup)
				.map_err(anyhow::Error::from)
				.and_then(|value| {
					value
						.as_string()
						.map_err(|_| anyhow::anyhow!("value is not convertible to a string"))
				});
			match value {
				Ok(value) => rl_entries.push(Entry {
					key: k.clone(),
					value,
				}),
				// Emit trace to aid debugging
				Err(e) if skip => {
					trace!(
						"ratelimit failed to evaluate expression, skipping entry: key={}, expr={:?}, error={}",
						k, lookup, e
					);
				},
				Err(e) => {
					trace!(
						"ratelimit failed to evaluate expression: key={}, expr={:?}, error={}",
						k, lookup, e
					);
					// We drop the entire set if we cannot eval one
					return Err(e.context(format!("entry {k}")));
				},
			}
		}
		Ok(rl_entries)
	}

	pub fn expressions(&self) -> impl Iterator<Item = &Expression> {
		self.descriptors.0.iter().flat_map(|v| {
			v.entries
				.iter()
				.map(|entry| &entry.1)
				.chain(v.cost.iter().map(|expr| expr.as_ref()))
				.chain(v.limit_override.iter().map(|expr| expr.as_ref()))
		})
	}
}

impl crate::store::RequestPolicyTrait for RemoteRateLimit {
	async fn apply(
		&self,
		client: &PolicyClient,
		_log: &mut crate::telemetry::log::RequestLog,
		req: &mut Request,
	) -> Result<PolicyResponse, crate::proxy::ProxyResponse> {
		Ok(self.check(client.clone(), req).await?)
	}

	fn expressions(&self) -> impl Iterator<Item = &Expression> {
		RemoteRateLimit::expressions(self)
	}
}

fn process_headers(hm: &mut HeaderMap, headers: Vec<proto::HeaderValue>) {
	for h in headers {
		let _ = envoy_proto_common::apply_header_value(hm, &h);
	}
}

/// When multiple descriptors apply, the most-constrained one (fewest requests remaining)
/// is reported, matching the limit a client is most likely to hit next.
fn ratelimit_status(
	statuses: &[proto::rate_limit_response::DescriptorStatus],
) -> Option<localratelimit::RateLimitStatus> {
	let best = statuses
		.iter()
		.filter(|status| status.current_limit.is_some())
		.min_by_key(|status| status.limit_remaining)?;
	let limit = best.current_limit.as_ref()?;
	let reset_seconds = best
		.duration_until_reset
		.as_ref()
		.map(|d| d.seconds.max(0) as u64)
		.unwrap_or(0);
	Some(localratelimit::RateLimitStatus {
		limit: limit.requests_per_unit as u64,
		remaining: best.limit_remaining as u64,
		reset_seconds,
	})
}

fn process_ratelimit_status_headers(
	hm: &mut HeaderMap,
	statuses: &[proto::rate_limit_response::DescriptorStatus],
	denied: bool,
) {
	if let Some(status) = ratelimit_status(statuses) {
		http::x_headers::set_ratelimit_headers(
			hm,
			status.limit,
			status.remaining,
			status.reset_seconds,
		);
	}
	// Only denials need Retry-After; allowed responses still get advisory quota headers.
	// Like Envoy, derive the longest denied reset separately: current_limit may be
	// absent, and advisory x-ratelimit headers may describe a different descriptor.
	if denied
		&& let Some(reset) = statuses
			.iter()
			.filter(|s| s.code == proto::rate_limit_response::Code::OverLimit as i32)
			.filter_map(|s| s.duration_until_reset.as_ref())
			.map(|d| (d.seconds.max(0) as u64 + u64::from(d.nanos > 0)).max(1))
			.max()
	{
		hm.entry(::http::header::RETRY_AFTER)
			.or_insert(reset.into());
	}
}

fn eval_cost(
	exec: &cel::Executor<'_>,
	cost: Option<&Expression>,
	default_cost: Option<u64>,
) -> anyhow::Result<Option<u64>> {
	match cost {
		Some(expr) => Ok(Some(exec.eval(expr)?.as_unsigned()?.try_into()?)),
		None => Ok(default_cost),
	}
}

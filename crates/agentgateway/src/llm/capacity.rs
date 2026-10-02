//! Provider-quota-aware selection of LLM providers.
//!
//! A provider can declare the limits its upstream enforces (`capacity`: requests, input tokens and
//! output tokens per minute). Each replica tracks what it has sent in a sliding one-minute window
//! and turns that into a headroom fraction (`1 - used / limit`, minimum over the declared
//! dimensions). Provider selection prefers providers with more headroom, skips providers that
//! have none while another provider in the same priority group has some, and falls through to the
//! next priority group when a whole group is out. Low-priority requests can additionally be shed
//! when every reachable provider is below a reserve kept for high-priority traffic.
//!
//! Accounting is local to the replica. Requests are charged when a provider is selected; tokens
//! are charged when the response completes, and in the meantime each in-flight request is assumed
//! to cost the provider's recent average. Rate-limit headers reported by the provider (OpenAI,
//! Azure OpenAI and Anthropic send them) correct the estimate, which also accounts for traffic the
//! replica cannot see (other replicas, other clients of the same key).

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ::http::HeaderMap;
use agent_core::strng::RichStrng;
use parking_lot::Mutex;
use prometheus_client::metrics::gauge::Gauge;

use super::NamedAIProvider;
use crate::types::loadbalancer::{EndpointInfo, EndpointSet};
use crate::*;

/// Length of the sliding window that provider limits are expressed in.
const WINDOW: Duration = Duration::from_secs(60);
/// Granularity of the sliding window.
const SLOT: Duration = Duration::from_secs(5);
/// Slots fully inside the window; one more slot is counted in part.
const SLOTS_IN_WINDOW: u64 = WINDOW.as_secs() / SLOT.as_secs();
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
}

#[derive(Debug, Default, Clone, Copy)]
struct Correction {
	offset: f64,
	at: Option<Instant>,
}

#[derive(Debug, Default)]
struct State {
	slots: [Slot; RING],
	in_flight: u64,
	/// Recent average tokens per completed request, for input and output.
	avg_tokens: [Option<f64>; 3],
	/// Difference between what the provider reported and the local estimate, per dimension.
	corrections: [Correction; 3],
}

/// Runtime usage tracking for one provider with a declared capacity. Shared by every backend that
/// routes to the same provider entry (for example a model and the virtual models that target it).
#[derive(Debug)]
pub struct CapacityTracker {
	config: ProviderCapacity,
	limits: [Option<u64>; 3],
	origin: Instant,
	labels: crate::telemetry::metrics::ProviderCapacityLabels,
	gauge: OnceLock<Gauge<f64, AtomicU64>>,
	state: Mutex<State>,
}

impl serde::Serialize for CapacityTracker {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		#[derive(serde::Serialize)]
		#[serde(rename_all = "camelCase")]
		struct Dump<'a> {
			#[serde(flatten)]
			limits: &'a ProviderCapacity,
			headroom: f64,
		}
		Dump {
			limits: &self.config,
			headroom: self.headroom(),
		}
		.serialize(serializer)
	}
}

impl CapacityTracker {
	pub fn new(config: ProviderCapacity, backend: &str, provider: &str) -> anyhow::Result<Self> {
		let limits = config.limits();
		anyhow::ensure!(
			limits.iter().any(Option::is_some),
			"capacity for provider {provider} must set at least one of rpm, inputTpm, outputTpm"
		);
		anyhow::ensure!(
			limits.iter().flatten().all(|l| *l > 0),
			"capacity limits for provider {provider} must be greater than zero"
		);
		Ok(Self {
			config,
			limits,
			origin: Instant::now(),
			labels: crate::telemetry::metrics::ProviderCapacityLabels {
				backend: RichStrng::from(backend).into(),
				provider: RichStrng::from(provider).into(),
			},
			gauge: OnceLock::new(),
			state: Mutex::new(State::default()),
		})
	}

	/// Index of the slot containing `now`, and the fraction of that slot already elapsed.
	fn slot_at(&self, now: Instant) -> (u64, f64) {
		let elapsed = now.saturating_duration_since(self.origin);
		let index = elapsed.as_secs() / SLOT.as_secs();
		let into_slot = elapsed - SLOT * index as u32;
		(index, into_slot.as_secs_f64() / SLOT.as_secs_f64())
	}

	/// Usage charged in the last minute. The slot that is partly outside the window is counted in
	/// proportion to its overlap, assuming its usage was spread evenly.
	fn windowed(&self, st: &State, now: Instant) -> [f64; 3] {
		let (cur, frac) = self.slot_at(now);
		let mut used = [0.0; 3];
		for slot in &st.slots {
			let Some(age) = cur.checked_sub(slot.index) else {
				continue;
			};
			let weight = match age {
				a if a < SLOTS_IN_WINDOW => 1.0,
				a if a == SLOTS_IN_WINDOW => 1.0 - frac,
				_ => continue,
			};
			for (u, s) in used.iter_mut().zip(slot.used) {
				*u += s as f64 * weight;
			}
		}
		used
	}

	/// Local estimate: the window plus the expected tokens of requests still in flight.
	fn projected(&self, st: &State, now: Instant) -> [f64; 3] {
		let mut used = self.windowed(st, now);
		for dim in [INPUT, OUTPUT] {
			used[dim] += st.in_flight as f64 * st.avg_tokens[dim].unwrap_or(0.0);
		}
		used
	}

	/// The local estimate corrected by the latest provider report. A correction fades out over one
	/// window, as the usage it describes leaves the provider's window too.
	fn effective(&self, st: &State, now: Instant) -> [f64; 3] {
		let mut used = self.projected(st, now);
		for (u, c) in used.iter_mut().zip(st.corrections) {
			let Some(at) = c.at else { continue };
			let age = now.saturating_duration_since(at).as_secs_f64();
			let fade = (1.0 - age / WINDOW.as_secs_f64()).max(0.0);
			*u = (*u + c.offset * fade).max(0.0);
		}
		used
	}

	/// Remaining capacity as a fraction in [0, 1]: the minimum over the limited dimensions.
	pub fn headroom(&self) -> f64 {
		self.headroom_at(Instant::now())
	}

	pub(crate) fn headroom_at(&self, now: Instant) -> f64 {
		let st = self.state.lock();
		let used = self.effective(&st, now);
		self
			.limits
			.iter()
			.zip(used)
			.filter_map(|(limit, used)| limit.map(|l| 1.0 - used / l as f64))
			.fold(1.0_f64, f64::min)
			.clamp(0.0, 1.0)
	}

	/// Time until the oldest usage in the window starts to leave it.
	fn retry_after_at(&self, now: Instant) -> Duration {
		let st = self.state.lock();
		let (cur, _) = self.slot_at(now);
		let oldest = st
			.slots
			.iter()
			.filter(|s| s.used.iter().any(|u| *u > 0))
			.filter(|s| {
				cur
					.checked_sub(s.index)
					.is_some_and(|a| a <= SLOTS_IN_WINDOW)
			})
			.map(|s| s.index)
			.min();
		match oldest {
			Some(index) => {
				let leaves_at = self.origin + SLOT * (index + SLOTS_IN_WINDOW) as u32;
				leaves_at
					.saturating_duration_since(now)
					.max(Duration::from_secs(1))
			},
			None => SLOT,
		}
	}

	fn charge(&self, st: &mut State, now: Instant, dim: usize, amount: u64) {
		let (cur, _) = self.slot_at(now);
		let slot = &mut st.slots[(cur % RING as u64) as usize];
		if slot.index != cur {
			*slot = Slot {
				index: cur,
				used: [0; 3],
			};
		}
		slot.used[dim] = slot.used[dim].saturating_add(amount);
	}

	/// Charges one request and holds an in-flight slot until the permit is settled or dropped.
	pub fn admit(self: &Arc<Self>) -> CapacityPermit {
		self.admit_at(Instant::now())
	}

	pub(crate) fn admit_at(self: &Arc<Self>, now: Instant) -> CapacityPermit {
		let mut st = self.state.lock();
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
		let mut st = self.state.lock();
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
			let local = self.projected(&st, now);
			for (dim, reported) in observation.0.iter().enumerate() {
				let (Some(limit), Some(reported)) = (self.limits[dim], reported) else {
					continue;
				};
				let limit = limit as f64;
				let used_fraction = match reported.limit {
					Some(provider_limit) if provider_limit > 0.0 => 1.0 - reported.remaining / provider_limit,
					_ => 1.0 - reported.remaining / limit,
				}
				.clamp(0.0, 1.0);
				st.corrections[dim] = Correction {
					offset: used_fraction * limit - local[dim],
					at: Some(now),
				};
			}
		}
	}

	fn release(&self) {
		let mut st = self.state.lock();
		st.in_flight = st.in_flight.saturating_sub(1);
	}

	/// Publishes the current headroom on the `gen_ai_provider_capacity_headroom` gauge.
	pub fn export(&self, metrics: &crate::telemetry::metrics::Metrics) {
		let gauge = self.gauge.get_or_init(|| {
			metrics
				.gen_ai_provider_capacity_headroom
				.get_or_create(&self.labels)
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

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;

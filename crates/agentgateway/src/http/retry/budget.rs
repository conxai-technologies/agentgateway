use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::*;

/// Limits how many retries may be in flight at once, relative to the number of requests in
/// flight, so that a failing backend does not see its load multiplied by retries.
///
/// A retry is admitted only while the active retries are below
/// `max(budgetPercent% of active requests, minRetryConcurrency)`. Counts are kept per retry
/// policy and per gateway replica. A request counts as active from its first attempt until its
/// response body has been fully sent (or dropped), so long-lived streaming responses keep
/// counting. A request counts as one active retry from the moment a retry is admitted
/// (including the backoff wait) until it completes; retrying it again competes for the budget
/// anew. Failover across providers is performed by retries, so it is subject to the same budget.
#[apply(schema!)]
#[cfg_attr(feature = "schema", schemars(rename = "RetryBudget"))]
pub struct Budget {
	/// Maximum active retries, as a percentage (0-100) of active requests. Defaults to 20.
	#[serde(
		default = "default_budget_percent",
		deserialize_with = "de_budget_percent"
	)]
	#[cfg_attr(feature = "schema", schemars(with = "f64"))]
	pub budget_percent: f64,
	/// Number of concurrent retries always allowed, regardless of `budgetPercent`, so that
	/// low-traffic routes can still retry. Defaults to 3.
	#[serde(default = "default_min_retry_concurrency")]
	pub min_retry_concurrency: u32,
	#[serde(skip)]
	#[cfg_attr(feature = "schema", schemars(skip))]
	state: Arc<State>,
}

/// In-flight counters, shared by every clone of the policy.
#[derive(Debug, Default)]
struct State {
	requests: AtomicUsize,
	retries: AtomicUsize,
}

impl Budget {
	pub fn new(budget_percent: f64, min_retry_concurrency: u32) -> Self {
		Self {
			budget_percent,
			min_retry_concurrency,
			state: Default::default(),
		}
	}

	/// Counts a request as active until the returned permit is dropped.
	pub fn track_request(&self) -> Permit {
		self.state.requests.fetch_add(1, Ordering::Relaxed);
		Permit {
			state: self.state.clone(),
			kind: Kind::Request,
		}
	}

	/// Admits one more active retry if the budget allows it. The retry counts as active until
	/// the returned permit is dropped.
	pub fn try_acquire_retry(&self) -> Option<Permit> {
		let limit = self.max_retries(self.state.requests.load(Ordering::Relaxed));
		self
			.state
			.retries
			.try_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
				(active < limit).then_some(active + 1)
			})
			.ok()?;
		Some(Permit {
			state: self.state.clone(),
			kind: Kind::Retry,
		})
	}

	fn max_retries(&self, active_requests: usize) -> usize {
		let budget = (active_requests as f64 * self.budget_percent / 100.0) as usize;
		budget.max(self.min_retry_concurrency as usize)
	}

	#[cfg(test)]
	fn active(&self) -> (usize, usize) {
		(
			self.state.requests.load(Ordering::Relaxed),
			self.state.retries.load(Ordering::Relaxed),
		)
	}
}

#[derive(Debug, Clone, Copy)]
enum Kind {
	Request,
	Retry,
}

/// Holds a request or a retry as active against a [`Budget`] until dropped.
#[must_use]
#[derive(Debug)]
pub struct Permit {
	state: Arc<State>,
	kind: Kind,
}

impl Drop for Permit {
	fn drop(&mut self) {
		let counter = match self.kind {
			Kind::Request => &self.state.requests,
			Kind::Retry => &self.state.retries,
		};
		counter.fetch_sub(1, Ordering::Relaxed);
	}
}

/// Keeps budget permits held until the response body completes, so that streaming responses
/// keep counting as in flight. Without a budget (no request permit) the response is unchanged.
pub fn hold_until_complete<E>(
	res: Result<crate::http::Response, E>,
	request: Option<Permit>,
	retry: Option<Permit>,
) -> Result<crate::http::Response, E> {
	let Some(request) = request else {
		return res;
	};
	res.map(|resp| resp.map(|body| body.with_drop_guard((request, retry))))
}

fn default_budget_percent() -> f64 {
	20.0
}

fn default_min_retry_concurrency() -> u32 {
	3
}

fn de_budget_percent<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
	D: Deserializer<'de>,
{
	let percent = f64::deserialize(deserializer)?;
	if !(0.0..=100.0).contains(&percent) {
		return Err(serde::de::Error::custom(format!(
			"budgetPercent must be between 0 and 100, got {percent}"
		)));
	}
	Ok(percent)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_defaults() {
		let budget: Budget = serde_json::from_value(serde_json::json!({})).unwrap();
		assert_eq!(budget.budget_percent, 20.0);
		assert_eq!(budget.min_retry_concurrency, 3);
	}

	#[test]
	fn rejects_out_of_range_percent() {
		for percent in [-1.0, 100.5] {
			let err = serde_json::from_value::<Budget>(serde_json::json!({ "budgetPercent": percent }))
				.unwrap_err();
			assert!(err.to_string().contains("budgetPercent"), "{err}");
		}
	}

	#[test]
	fn retries_are_capped_by_active_requests() {
		let budget = Budget::new(20.0, 0);
		let requests: Vec<_> = (0..10).map(|_| budget.track_request()).collect();
		let retries: Vec<_> = (0..10).filter_map(|_| budget.try_acquire_retry()).collect();
		// 20% of 10 active requests.
		assert_eq!(retries.len(), 2);
		assert_eq!(budget.active(), (10, 2));

		assert!(budget.try_acquire_retry().is_none());

		// A released retry frees a slot for the next one.
		let mut retries = retries;
		retries.pop();
		retries.push(budget.try_acquire_retry().expect("released slot"));
		assert!(budget.try_acquire_retry().is_none());

		drop(retries);
		drop(requests);
		assert_eq!(budget.active(), (0, 0));
	}

	#[test]
	fn min_retry_concurrency_applies_at_low_traffic() {
		let budget = Budget::new(20.0, 3);
		let _request = budget.track_request();
		let retries: Vec<_> = (0..10).filter_map(|_| budget.try_acquire_retry()).collect();
		assert_eq!(retries.len(), 3);
	}

	#[test]
	fn zero_budget_disables_retries() {
		let budget = Budget::new(0.0, 0);
		let _requests: Vec<_> = (0..100).map(|_| budget.track_request()).collect();
		assert!(budget.try_acquire_retry().is_none());
	}

	#[test]
	fn clones_share_counters() {
		let budget = Budget::new(0.0, 1);
		let clone = budget.clone();
		let _retry = budget.try_acquire_retry().unwrap();
		assert!(clone.try_acquire_retry().is_none());
	}

	#[test]
	fn concurrent_acquisition_never_exceeds_limit() {
		let budget = Arc::new(Budget::new(0.0, 4));
		let barrier = Arc::new(std::sync::Barrier::new(33));
		let tasks: Vec<_> = (0..32)
			.map(|_| {
				let budget = budget.clone();
				let barrier = barrier.clone();
				std::thread::spawn(move || {
					barrier.wait();
					budget.try_acquire_retry()
				})
			})
			.collect();
		barrier.wait();
		let permits: Vec<_> = tasks
			.into_iter()
			.filter_map(|task| task.join().unwrap())
			.collect();
		assert_eq!(permits.len(), 4);
		drop(permits);
		assert_eq!(budget.active(), (0, 0));
	}

	#[tokio::test]
	async fn permits_are_held_until_the_response_body_completes() {
		use http_body_util::BodyExt;

		let budget = Budget::new(20.0, 3);
		let (tx, rx) = futures::channel::mpsc::unbounded::<Result<bytes::Bytes, std::io::Error>>();
		let res: Result<_, ()> = Ok(::http::Response::new(crate::http::Body::from_stream(rx)));
		let res = hold_until_complete(
			res,
			Some(budget.track_request()),
			budget.try_acquire_retry(),
		)
		.unwrap();
		// Headers have been returned, but the streaming body is still in flight.
		assert_eq!(budget.active(), (1, 1));

		tx.unbounded_send(Ok(bytes::Bytes::from_static(b"chunk")))
			.unwrap();
		drop(tx);
		let body = res.into_body().collect().await.unwrap().to_bytes();
		assert_eq!(&body[..], b"chunk");
		assert_eq!(budget.active(), (0, 0));
	}
}

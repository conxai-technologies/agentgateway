use ::http::{HeaderMap, HeaderValue};

use super::*;
use crate::llm::{AIBackend, AIProvider, openai};
use crate::types::loadbalancer::EndpointSet;

fn limits(rpm: Option<u64>, input_tpm: Option<u64>, output_tpm: Option<u64>) -> ProviderCapacity {
	ProviderCapacity {
		rpm,
		input_tpm,
		output_tpm,
	}
}

fn tracker(capacity: ProviderCapacity) -> Arc<CapacityTracker> {
	Arc::new(CapacityTracker::new(capacity, "backend", "provider").unwrap())
}

fn provider(name: &str, capacity: Option<ProviderCapacity>) -> (Strng, NamedAIProvider) {
	let capacity = capacity.map(|c| Arc::new(CapacityTracker::new(c, "backend", name).unwrap()));
	(
		strng::new(name),
		NamedAIProvider {
			name: strng::new(name),
			provider: AIProvider::OpenAI(openai::Provider {
				model_override: None,
				moderation: None,
			}),
			provider_backend: None,
			host_override: None,
			path_override: None,
			path_prefix: None,
			tokenize: false,
			inline_policies: vec![],
			capacity,
		},
	)
}

fn rpm(n: u64) -> Option<ProviderCapacity> {
	Some(limits(Some(n), None, None))
}

fn backend(groups: Vec<Vec<(Strng, NamedAIProvider)>>) -> AIBackend {
	AIBackend::new(EndpointSet::new(groups))
}

fn tracker_of(backend: &AIBackend, name: &str) -> Arc<CapacityTracker> {
	let mut found = None;
	backend.providers.any(|p| {
		if p.name.as_str() == name {
			found = p.capacity.clone();
		}
		false
	});
	found.expect("provider has a capacity")
}

/// Charges `n` requests that have already completed.
fn charge_requests(t: &Arc<CapacityTracker>, n: usize, now: Instant) {
	for _ in 0..n {
		t.admit_at(now).settle_at(now, None, None);
	}
}

fn request(priority: &str) -> crate::http::Request {
	::http::Request::builder()
		.uri("http://example.com/v1/chat/completions")
		.header("x-priority", priority)
		.body(crate::http::Body::empty())
		.unwrap()
}

fn assert_close(actual: f64, expected: f64) {
	assert!(
		(actual - expected).abs() < 1e-6,
		"expected {expected}, got {actual}"
	);
}

#[test]
fn rejects_empty_or_zero_capacity() {
	assert!(CapacityTracker::new(limits(None, None, None), "b", "p").is_err());
	assert!(CapacityTracker::new(limits(Some(0), None, None), "b", "p").is_err());
	assert!(CapacityTracker::new(limits(None, Some(10), Some(0)), "b", "p").is_err());
}

#[test]
fn sliding_window_charges_requests_and_expires_them() {
	let t = tracker(limits(Some(10), None, None));
	let t0 = t.origin;
	charge_requests(&t, 5, t0);
	assert_close(t.headroom_at(t0), 0.5);
	assert_close(t.headroom_at(t0 + Duration::from_secs(30)), 0.5);
	// The first slot is half outside the window.
	assert_close(t.headroom_at(t0 + Duration::from_millis(62_500)), 0.75);
	assert_close(t.headroom_at(t0 + Duration::from_secs(65)), 1.0);
}

#[test]
fn headroom_is_the_minimum_over_dimensions_and_clamped() {
	let t = tracker(limits(Some(100), Some(1000), Some(100)));
	let t0 = t.origin;
	t.admit_at(t0).settle_at(t0, Some(200), Some(90));
	// requests 0.99, input 0.8, output 0.1
	assert_close(t.headroom_at(t0), 0.1);
	t.admit_at(t0).settle_at(t0, Some(0), Some(500));
	assert_close(t.headroom_at(t0), 0.0);
}

#[test]
fn tokens_are_charged_on_settle_and_projected_while_in_flight() {
	let t = tracker(limits(None, Some(1000), None));
	let t0 = t.origin;
	let first = t.admit_at(t0);
	// No usage known yet: an in-flight request costs nothing.
	assert_close(t.headroom_at(t0), 1.0);
	first.settle_at(t0, Some(400), Some(10));
	assert_close(t.headroom_at(t0), 0.6);
	// A request in flight is expected to cost the recent average.
	let second = t.admit_at(t0);
	assert_close(t.headroom_at(t0), 0.2);
	drop(second);
	assert_close(t.headroom_at(t0), 0.6);
}

#[test]
fn openai_headers_correct_the_estimate_and_fade() {
	let t = tracker(limits(Some(100), None, None));
	let t0 = t.origin;
	let mut permit = t.admit_at(t0);
	let mut headers = HeaderMap::new();
	headers.insert(
		"x-ratelimit-limit-requests",
		HeaderValue::from_static("100"),
	);
	headers.insert(
		"x-ratelimit-remaining-requests",
		HeaderValue::from_static("10"),
	);
	permit.observe_headers(&headers);
	permit.settle_at(t0, None, None);
	// One request seen locally, ninety reported by the provider.
	assert_close(t.headroom_at(t0), 0.1);
	// Half a window later, half of the 89-request correction remains.
	assert_close(
		t.headroom_at(t0 + Duration::from_secs(30)),
		1.0 - 45.5 / 100.0,
	);
}

#[test]
fn headers_can_lower_the_estimate() {
	let t = tracker(limits(Some(100), None, None));
	let t0 = t.origin;
	charge_requests(&t, 49, t0);
	let mut permit = t.admit_at(t0);
	let mut headers = HeaderMap::new();
	// No limit header: the configured limit is used.
	headers.insert(
		"x-ratelimit-remaining-requests",
		HeaderValue::from_static("90"),
	);
	permit.observe_headers(&headers);
	permit.settle_at(t0, None, None);
	assert_close(t.headroom_at(t0), 0.9);
}

#[test]
fn anthropic_headers_correct_token_dimensions() {
	let t = tracker(limits(None, Some(10_000), Some(1_000)));
	let t0 = t.origin;
	let mut permit = t.admit_at(t0);
	let mut headers = HeaderMap::new();
	headers.insert(
		"anthropic-ratelimit-input-tokens-limit",
		HeaderValue::from_static("20000"),
	);
	headers.insert(
		"anthropic-ratelimit-input-tokens-remaining",
		HeaderValue::from_static("15000"),
	);
	permit.observe_headers(&headers);
	permit.settle_at(t0, Some(100), Some(10));
	// Input: the provider reports 25% used; output: 1% used locally.
	assert_close(t.headroom_at(t0), 0.75);
}

#[test]
fn parses_rate_limit_headers() {
	assert_eq!(RateLimitObservation::parse(&HeaderMap::new()), None);
	let mut headers = HeaderMap::new();
	headers.insert("x-ratelimit-limit-tokens", HeaderValue::from_static("1000"));
	headers.insert(
		"x-ratelimit-remaining-tokens",
		HeaderValue::from_static("250"),
	);
	headers.insert(
		"anthropic-ratelimit-output-tokens-remaining",
		HeaderValue::from_static("7"),
	);
	let RateLimitObservation([requests, input, output]) =
		RateLimitObservation::parse(&headers).unwrap();
	assert_eq!(requests, None);
	// The combined token limit applies to input; output has its own header.
	assert_eq!(
		input,
		Some(Reported {
			remaining: 250.0,
			limit: Some(1000.0)
		})
	);
	assert_eq!(
		output,
		Some(Reported {
			remaining: 7.0,
			limit: None
		})
	);
	let mut invalid = HeaderMap::new();
	invalid.insert(
		"x-ratelimit-remaining-requests",
		HeaderValue::from_static("lots"),
	);
	assert_eq!(RateLimitObservation::parse(&invalid), None);
}

#[test]
fn prefers_the_provider_with_more_headroom() {
	let backend = backend(vec![vec![provider("a", rpm(100)), provider("b", rpm(100))]]);
	let policy = backend.capacity.clone().expect("capacity policy");
	let now = Instant::now();
	charge_requests(&tracker_of(&backend, "a"), 90, now);
	let mut a = 0;
	for _ in 0..2000 {
		let (p, _) = policy
			.select(&backend.providers, None, Priority::High, now)
			.unwrap()
			.unwrap();
		if p.name.as_str() == "a" {
			a += 1;
		}
	}
	// With equal scores otherwise, P2C only picks `a` when both samples are `a` (25%).
	assert!((300..700).contains(&a), "a selected {a} of 2000 times");
}

#[test]
fn skips_a_provider_at_its_limit() {
	let backend = backend(vec![vec![provider("a", rpm(2)), provider("b", rpm(100))]]);
	let policy = backend.capacity.clone().expect("capacity policy");
	let now = Instant::now();
	charge_requests(&tracker_of(&backend, "a"), 2, now);
	for _ in 0..200 {
		let (p, _) = policy
			.select(&backend.providers, None, Priority::High, now)
			.unwrap()
			.unwrap();
		assert_eq!(p.name.as_str(), "b");
	}
}

#[test]
fn providers_without_capacity_are_always_eligible() {
	let backend = backend(vec![vec![provider("a", rpm(1)), provider("b", None)]]);
	let policy = backend.capacity.clone().expect("capacity policy");
	let now = Instant::now();
	charge_requests(&tracker_of(&backend, "a"), 1, now);
	let (p, _) = policy
		.select(&backend.providers, None, Priority::High, now)
		.unwrap()
		.unwrap();
	assert_eq!(p.name.as_str(), "b");
}

#[test]
fn affinity_keeps_its_choice_until_the_provider_is_exhausted() {
	let backend = backend(vec![vec![provider("a", rpm(1)), provider("b", rpm(1))]]);
	let policy = backend.capacity.clone().expect("capacity policy");
	let now = Instant::now();
	let (first, _) = policy
		.select(&backend.providers, Some(42), Priority::High, now)
		.unwrap()
		.unwrap();
	let (again, _) = policy
		.select(&backend.providers, Some(42), Priority::High, now)
		.unwrap()
		.unwrap();
	assert_eq!(first.name.as_str(), again.name.as_str());
	charge_requests(&tracker_of(&backend, &first.name), 1, now);
	let (moved, _) = policy
		.select(&backend.providers, Some(42), Priority::High, now)
		.unwrap()
		.unwrap();
	assert_ne!(moved.name.as_str(), first.name.as_str());
}

#[test]
fn falls_through_to_the_next_group_and_back_to_ordinary_selection() {
	let backend = backend(vec![
		vec![provider("a", rpm(1))],
		vec![provider("b", rpm(1))],
	]);
	let req = request("interactive");
	let first = backend.select_provider_for(None, &req).unwrap().unwrap();
	assert_eq!(first.provider.name.as_str(), "a");
	assert!(first.permit.is_some());
	drop(first);
	// `a` has used its one request; the next group takes over.
	let second = backend.select_provider_for(None, &req).unwrap().unwrap();
	assert_eq!(second.provider.name.as_str(), "b");
	drop(second);
	// Every group is out: selection falls back to the first group, and is still charged.
	let third = backend.select_provider_for(None, &req).unwrap().unwrap();
	assert_eq!(third.provider.name.as_str(), "a");
	assert!(third.permit.is_some());
	assert_close(tracker_of(&backend, "a").headroom(), 0.0);
}

#[test]
fn sheds_low_priority_requests_below_the_reserve() {
	let priority =
		cel::Expression::new_strict(r#"request.headers["x-priority"] == "batch" ? "low" : "high""#)
			.unwrap();
	let backend = backend(vec![vec![provider("a", rpm(10))]])
		.with_capacity_policy(CapacityPolicy::new(Some(Arc::new(priority)), vec![Some(0.4)]).unwrap());
	let batch = request("batch");
	let interactive = request("interactive");
	let policy = backend.capacity.clone().unwrap();
	assert_eq!(policy.priority(&batch), Priority::Low);
	assert_eq!(policy.priority(&interactive), Priority::High);

	charge_requests(&tracker_of(&backend, "a"), 5, Instant::now());
	// 50% headroom is above the 40% reserve.
	assert!(backend.select_provider_for(None, &batch).unwrap().is_some());
	// That request left exactly 40%: low priority is refused, high priority is not.
	let err = backend.select_provider_for(None, &batch).unwrap_err();
	assert!(
		(55..=60).contains(&err.retry_after),
		"retry after {}",
		err.retry_after
	);
	let selected = backend.select_provider_for(None, &interactive).unwrap();
	assert_eq!(selected.unwrap().provider.name.as_str(), "a");
}

#[test]
fn capacity_policy_validation() {
	let expr = || Some(Arc::new(cel::Expression::new_strict(r#""low""#).unwrap()));
	assert!(CapacityPolicy::new(expr(), vec![Some(0.4), None]).is_ok());
	assert!(CapacityPolicy::new(None, vec![None]).is_ok());
	assert!(CapacityPolicy::new(expr(), vec![Some(1.0)]).is_err());
	assert!(CapacityPolicy::new(expr(), vec![Some(-0.1)]).is_err());
	assert!(CapacityPolicy::new(expr(), vec![None]).is_err());
	assert!(CapacityPolicy::new(None, vec![Some(0.4)]).is_err());
}

#[tokio::test]
async fn parses_capacity_config() {
	let fetcher = crate::resource_manager::ResourceFetcher::files_only();
	let parse = |v: serde_json::Value| {
		serde_json::from_value::<crate::types::local::LocalAIBackend>(v).map_err(|e| e.to_string())
	};
	let openai = serde_json::json!({"openAI": {}});
	let config = parse(serde_json::json!({
		"groups": [
			{
				"providers": [
					{"name": "a", "provider": openai, "capacity": {"rpm": 10, "inputTpm": 1000}},
					{"name": "b", "provider": openai},
				],
				"reserveForHighPriority": 0.4,
			},
			{"providers": [{"name": "c", "provider": openai, "capacity": {"outputTpm": 5}}]},
		],
		"priority": "apiKey.class == 'batch' ? 'low' : 'high'",
	}))
	.unwrap();
	let backend = config.translate("my-backend", &fetcher).await.unwrap();
	let policy = backend.capacity.clone().expect("capacity policy");
	assert!(policy.priority.is_some());
	assert_eq!(policy.reserve_for_high_priority, vec![Some(0.4), None]);
	let a = tracker_of(&backend, "a");
	assert_eq!(a.limits, [Some(10), Some(1000), None]);

	// A provider without capacity and no shedding keeps the plain selection path.
	let plain = parse(serde_json::json!({"name": "a", "provider": openai}))
		.unwrap()
		.translate("plain", &fetcher)
		.await
		.unwrap();
	assert!(plain.capacity.is_none());

	let invalid = parse(serde_json::json!({
		"groups": [{"providers": [{"name": "a", "provider": openai}], "reserveForHighPriority": 0.4}],
	}))
	.unwrap()
	.translate("invalid", &fetcher)
	.await;
	assert!(invalid.is_err());
	let invalid = parse(serde_json::json!({
		"name": "a", "provider": openai, "capacity": {},
	}))
	.unwrap()
	.translate("invalid", &fetcher)
	.await;
	assert!(invalid.is_err());
	assert!(
		parse(serde_json::json!({
			"groups": [{"providers": [{"name": "a", "provider": openai}]}],
			"unknown": true,
		}))
		.is_err()
	);
}

#[test]
fn shed_response_is_a_429_with_retry_after() {
	let resp = crate::proxy::ProxyError::from(CapacityExhausted { retry_after: 7 })
		.into_response_with_grpc(false);
	assert_eq!(resp.status(), ::http::StatusCode::TOO_MANY_REQUESTS);
	assert_eq!(resp.headers()[::http::header::RETRY_AFTER], "7");
}

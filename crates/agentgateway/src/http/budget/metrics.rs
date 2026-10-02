use std::sync::{Arc, OnceLock};

use chrono::Utc;
use prometheus_client::encoding::{EncodeMetric, MetricEncoder};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::{BudgetCounter, BudgetPolicy, UnixDate, budget_window};

/// Prometheus view of the in-memory budget counters.
///
/// Values are read when metrics are scraped rather than pushed on every change, so they always
/// reflect the latest settlement or database reconciliation, budgets removed from configuration
/// disappear, and an expired window reports zero usage before a request advances it. Metrics are
/// registered before the process-wide policy exists, so the policy is attached afterwards and
/// nothing is encoded until then.
#[derive(Debug, Clone, Default)]
pub struct BudgetMetrics(Arc<OnceLock<BudgetPolicy>>);

impl BudgetMetrics {
	/// Exposes `policy`'s counters. Only the first attached policy is used.
	pub fn attach(&self, policy: &BudgetPolicy) {
		let _ = self.0.set(policy.clone());
	}

	pub(crate) fn metric(&self, value: BudgetMetricValue) -> BudgetMetric {
		BudgetMetric {
			source: self.clone(),
			value,
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BudgetMetricValue {
	Limit,
	Used,
	Utilization,
	WindowStart,
	WindowEnd,
	ExceededRequests,
}

/// One budget metric family, encoded from the attached policy at scrape time.
#[derive(Debug)]
pub struct BudgetMetric {
	source: BudgetMetrics,
	value: BudgetMetricValue,
}

impl EncodeMetric for BudgetMetric {
	fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
		let Some(policy) = self.source.0.get() else {
			return Ok(());
		};
		let now = Utc::now();
		for counter in policy.counters.iter() {
			// Preloaded rows without a configured budget have no labels to report.
			let Some(definition) = counter.definition.as_ref() else {
				continue;
			};
			let budget = &definition.budget;
			// The API key display name identifies the key; the key and its hash are never exported.
			let api_key = ("api_key", definition.api_key.as_str());
			let name = ("budget", budget.name.as_str());
			let unit = ("unit", budget.limit.unit.as_str());
			if let BudgetMetricValue::ExceededRequests = self.value {
				let action = ("action", budget.on_budget_exceeded.as_str());
				ConstCounter::new(counter.exceeded_requests)
					.encode(encoder.encode_family(&[api_key, name, unit, action])?)?;
			} else {
				let value = counter.observe(budget.limit.amount.decimal(), self.value, now);
				ConstGauge::new(value).encode(encoder.encode_family(&[api_key, name, unit])?)?;
			}
		}
		Ok(())
	}

	fn metric_type(&self) -> MetricType {
		match self.value {
			BudgetMetricValue::ExceededRequests => MetricType::Counter,
			_ => MetricType::Gauge,
		}
	}
}

impl BudgetCounter {
	/// Returns a gauge value for the window containing `now`, matching the status API: an expired
	/// counter reports zero usage and the window it will advance to.
	fn observe(&self, limit: Decimal, value: BudgetMetricValue, now: UnixDate) -> f64 {
		let expired = now >= self.window_end;
		let used = if expired { Decimal::ZERO } else { self.amount };
		let (window_start, window_end) = if expired {
			budget_window(now, &self.window).unwrap_or((self.window_start, self.window_end))
		} else {
			(self.window_start, self.window_end)
		};
		let seconds = |time: UnixDate| time.timestamp_millis() as f64 / 1000.0;
		match value {
			BudgetMetricValue::Limit => limit.to_f64().unwrap_or(f64::NAN),
			BudgetMetricValue::Used => used.to_f64().unwrap_or(f64::NAN),
			// A zero limit is exceeded by any usage, including none, so it is always exhausted.
			BudgetMetricValue::Utilization => used
				.checked_div(limit)
				.unwrap_or(Decimal::ONE)
				.to_f64()
				.unwrap_or(f64::NAN),
			BudgetMetricValue::WindowStart => seconds(window_start),
			BudgetMetricValue::WindowEnd => seconds(window_end),
			BudgetMetricValue::ExceededRequests => self.exceeded_requests as f64,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use prometheus_client::registry::Registry;

	use super::*;
	use crate::cel::LLMContext;
	use crate::http::budget::{
		BudgetLimitUnit, BudgetRegistration, MatchedBudgets, PersistedBudgetUsage,
	};
	use crate::llm;
	use crate::telemetry::metrics::Metrics;

	const LABELS: &str = r#"{api_key="budgeted-key",budget="tokens",unit="Tokens"}"#;

	fn setup() -> (Registry, BudgetPolicy, MatchedBudgets) {
		let keys: crate::http::apikey::LocalAPIKeys = serde_json::from_value(serde_json::json!({
			"keys": [{
				"key": "sk-budget",
				"metadata": {"name": "budgeted-key"},
				"budgets": [{
					"name": "tokens",
					"limit": {"unit": "Tokens", "amount": 40},
					"window": {"rolling": "1h"},
					"onBudgetExceeded": "Block"
				}]
			}]
		}))
		.unwrap();
		let authentication = keys.compile().unwrap();
		let matched = authentication
			.users
			.values()
			.find_map(|policy| policy.budgets.clone())
			.unwrap();
		let policy = BudgetPolicy::default();
		policy.register(&authentication, true).unwrap();

		let mut registry = Registry::default();
		let metrics = Metrics::new(
			&mut registry,
			Default::default(),
			crate::HistogramMode::Classic,
		);
		metrics.budgets.attach(&policy);
		(registry, policy, matched)
	}

	fn response(total_tokens: u64) -> LLMContext {
		let mut context = LLMContext::from(llm::LLMRequest {
			input_tokens: None,
			input_format: llm::InputFormat::Responses,
			cache_convention: Default::default(),
			request_model: "test-model".into(),
			provider: "test-provider".into(),
			streaming: false,
			params: Default::default(),
			prompt: None,
			provider_state: None,
		});
		context.total_tokens = Some(total_tokens);
		context
	}

	/// Returns the value of the sample `name` + `labels`, or `None` if it is not exported.
	fn sample(registry: &Registry, name: &str, labels: &str) -> Option<f64> {
		let mut text = String::new();
		prometheus_client::encoding::text::encode(&mut text, registry).unwrap();
		let prefix = format!("{name}{labels} ");
		text
			.lines()
			.find_map(|line| line.strip_prefix(&prefix))
			.map(|value| value.parse().unwrap())
	}

	#[test]
	fn gauges_follow_settlement_and_reconciliation() {
		let (registry, policy, matched) = setup();
		assert_eq!(sample(&registry, "budget_limit", LABELS), Some(40.0));
		assert_eq!(sample(&registry, "budget_used", LABELS), Some(0.0));
		assert_eq!(
			sample(&registry, "budget_utilization_ratio", LABELS),
			Some(0.0)
		);

		let (window_start, window_end) = {
			let counter = policy.counters.iter().next().unwrap();
			(counter.window_start, counter.window_end)
		};
		assert_eq!(
			sample(&registry, "budget_window_start_timestamp_seconds", LABELS),
			Some(window_start.timestamp() as f64)
		);
		assert_eq!(
			sample(&registry, "budget_window_end_timestamp_seconds", LABELS),
			Some(window_end.timestamp() as f64)
		);

		policy.settle(&matched, &response(10));
		assert_eq!(sample(&registry, "budget_used", LABELS), Some(10.0));
		assert_eq!(
			sample(&registry, "budget_utilization_ratio", LABELS),
			Some(0.25)
		);

		// Usage written by other replicas arrives through database reconciliation.
		let budget_id = policy.counters.iter().next().unwrap().key().clone();
		policy.reconcile(
			HashMap::from([(
				budget_id,
				PersistedBudgetUsage {
					window_start,
					window_end,
					unit: Some(BudgetLimitUnit::Tokens),
					used_amount: 22,
					updated_at: Utc::now(),
				},
			)]),
			Utc::now(),
		);
		// 22 persisted plus 10 not yet flushed.
		assert_eq!(sample(&registry, "budget_used", LABELS), Some(32.0));
		assert_eq!(
			sample(&registry, "budget_utilization_ratio", LABELS),
			Some(0.8)
		);
	}

	#[test]
	fn exceeded_requests_count_checks_against_an_exhausted_budget() {
		let (registry, policy, matched) = setup();
		let labels = r#"{api_key="budgeted-key",budget="tokens",unit="Tokens",action="Block"}"#;
		assert!(policy.check(&matched).unwrap().is_none());
		assert_eq!(
			sample(&registry, "budget_exceeded_requests_total", labels),
			Some(0.0)
		);

		policy.settle(&matched, &response(40));
		assert!(policy.check(&matched).unwrap().is_some());
		assert!(policy.check(&matched).unwrap().is_some());
		assert_eq!(
			sample(&registry, "budget_exceeded_requests_total", labels),
			Some(2.0)
		);
		assert_eq!(
			sample(&registry, "budget_utilization_ratio", LABELS),
			Some(1.0)
		);
	}

	#[test]
	fn expired_and_removed_budgets() {
		let (registry, policy, matched) = setup();
		policy.settle(&matched, &response(30));
		{
			let mut counter = policy.counters.iter_mut().next().unwrap();
			counter.window_start = UnixDate::from_timestamp_millis(0).unwrap();
			counter.window_end = UnixDate::from_timestamp_millis(3_600_000).unwrap();
		}
		// An expired window reports no usage and the current window, without waiting for a request.
		assert_eq!(sample(&registry, "budget_used", LABELS), Some(0.0));
		let start = sample(&registry, "budget_window_start_timestamp_seconds", LABELS).unwrap();
		assert!(start > 3_600.0);

		policy
			.apply_registration(BudgetRegistration::default())
			.unwrap();
		assert_eq!(sample(&registry, "budget_limit", LABELS), None);
	}
}

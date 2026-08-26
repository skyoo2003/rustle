use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
pub const SCHEMA_VERSION: u32 = 1;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub schema_version: u32,
    pub market: String,
    pub exchange_ts: DateTime<Utc>,
    pub receive_ts: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub meta: Meta,
    pub price: f64,
    pub volume: f64,
    pub side: Side,
    pub sequential_id: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub ask_price: f64,
    pub bid_price: f64,
    pub ask_size: f64,
    pub bid_size: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orderbook {
    pub meta: Meta,
    pub total_ask_size: f64,
    pub total_bid_size: f64,
    pub levels: Vec<Level>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionEvent {
    pub meta: Meta,
    pub state: String,
    pub detail: String,
    pub gap_ms: Option<i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseSnapshot {
    pub schema_version: u32,
    pub refreshed_at: DateTime<Utc>,
    pub markets: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub meta: Meta,
    pub signal_type: String,
    pub direction: Side,
    pub feature_value: f64,
    pub baseline: f64,
    pub rationale: String,
    pub market_snapshot: serde_json::Value,
    pub rule_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperTrade {
    pub signal: Signal,
    pub entry_price: f64,
    pub exit_price: f64,
    pub gross_pnl_pct: f64,
    /// Gross less one round trip of `[paper] fee_bps + slippage_bps`.
    pub net_pnl_pct: f64,
}

/// The replayable result of one candidate signal.  `complete` is deliberately
/// separate from `hit`: incomplete horizons must never become negative data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalOutcome {
    pub signal: Signal,
    pub rule_key: String,
    pub entry_price: Option<f64>,
    pub horizon_price: Option<f64>,
    pub reached_target: Option<bool>,
    pub complete: bool,
}

/// The single versioned gate consumed by live alerting and paper trading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualifiedRuleSet {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub tuning_start: chrono::NaiveDate,
    pub tuning_end: chrono::NaiveDate,
    pub validation_start: chrono::NaiveDate,
    pub validation_end: chrono::NaiveDate,
    pub bootstrap_seed: u64,
    pub rules: Vec<String>,
    /// Missing fields on older files deliberately deserialize as stale.
    #[serde(default)]
    pub audit: Option<crate::analysis::EvaluationAudit>,
    #[serde(default)]
    pub config_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub emitted_at: DateTime<Utc>,
    pub rule_key: String,
    pub signal: Signal,
    #[serde(default)]
    pub validation: Option<crate::analysis::RuleResult>,
}

/// The headline of one paper study.  Everything after `win_rate` exists so the headline
/// can be disbelieved: the window it covers, how much of the universe it spans, what it
/// declined to trade, how far underwater it went, and what simply holding would have paid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperSummary {
    pub generated_at: DateTime<Utc>,
    pub trade_count: usize,
    /// Equal-weighted, compounded return of the whole universe, in percent.
    pub cumulative_net_pnl_pct: f64,
    pub win_rate: f64,
    /// Missing fields on older files deliberately deserialize as unknown or zero.
    #[serde(default)]
    pub window_start: Option<chrono::NaiveDate>,
    #[serde(default)]
    pub window_end: Option<chrono::NaiveDate>,
    #[serde(default)]
    pub market_count: usize,
    /// Qualified in-window signals declined because that market was already in a position.
    #[serde(default)]
    pub skipped_overlapping: usize,
    /// Qualified in-window signals with no executable entry or no trade at the horizon.
    #[serde(default)]
    pub incomplete_horizon: usize,
    /// Deepest peak-to-trough move of the equal-weighted equity curve, negative or zero.
    #[serde(default)]
    pub max_drawdown_pct: f64,
    /// Equal-weighted buy-at-window-open / sell-at-window-close, less one round trip.
    #[serde(default)]
    pub hodl_pnl_pct: f64,
    #[serde(default)]
    pub excess_pnl_pct: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_alert_event_json_defaults_validation_to_none() {
        let json = serde_json::json!({
            "emitted_at": "2025-01-01T00:00:00Z",
            "rule_key": "synthetic:test",
            "signal": {
                "meta": {
                    "schema_version": 1,
                    "market": "KRW-TEST",
                    "exchange_ts": "2025-01-01T00:00:00Z",
                    "receive_ts": "2025-01-01T00:00:00Z"
                },
                "signal_type": "synthetic",
                "direction": "buy",
                "feature_value": 1.0,
                "baseline": 0.0,
                "rationale": "test",
                "market_snapshot": {},
                "rule_id": "test"
            }
        });

        let event: AlertEvent = serde_json::from_value(json).unwrap();
        assert!(event.validation.is_none());
    }

    #[test]
    fn old_paper_summary_json_defaults_every_milestone_four_field() {
        let json = serde_json::json!({
            "generated_at": "2025-01-29T00:00:00Z",
            "trade_count": 3,
            "cumulative_net_pnl_pct": 1.5,
            "win_rate": 0.5
        });

        let summary: PaperSummary = serde_json::from_value(json).unwrap();

        assert_eq!(summary.trade_count, 3);
        assert!(summary.window_start.is_none());
        assert!(summary.window_end.is_none());
        assert_eq!(summary.market_count, 0);
        assert_eq!(summary.skipped_overlapping, 0);
        assert_eq!(summary.incomplete_horizon, 0);
        assert_eq!(summary.max_drawdown_pct, 0.0);
        assert_eq!(summary.hodl_pnl_pct, 0.0);
        assert_eq!(summary.excess_pnl_pct, 0.0);
    }
}

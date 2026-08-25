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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
    pub net_pnl_pct: f64,
    pub benchmark_pnl_pct: f64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub emitted_at: DateTime<Utc>,
    pub rule_key: String,
    pub signal: Signal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperSummary {
    pub generated_at: DateTime<Utc>,
    pub trade_count: usize,
    pub cumulative_net_pnl_pct: f64,
    pub win_rate: f64,
    pub hodl_benchmark_pnl_pct: f64,
}

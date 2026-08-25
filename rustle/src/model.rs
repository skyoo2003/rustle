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

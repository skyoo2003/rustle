use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub version: u32,
    pub data_root: String,
    pub top_market_count: usize,
    pub daily_refresh_utc_hour: u8,
    pub imbalance_window_seconds: i64,
    pub trade_rate_window_seconds: i64,
    pub wall_min_krw: f64,
    pub candidate: CandidateConfig,
    pub validation: ValidationConfig,
    pub paper: PaperConfig,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateConfig {
    pub imbalance_thresholds: Vec<f64>,
    pub large_trade_multiples: Vec<f64>,
    pub trade_rate_multiples: Vec<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Number of complete UTC collection days used to choose one rule per signal type.
    #[serde(default = "default_tuning_days")]
    pub tuning_days: usize,
    /// Number of subsequent untouched UTC collection days used for the gate.
    #[serde(default = "default_validation_days")]
    pub validation_days: usize,
    #[serde(default = "default_min_validation_signals")]
    pub min_validation_signals: usize,
    pub horizon_minutes: i64,
    /// Maximum delay after a signal at which the first trade is executable.
    #[serde(default = "default_entry_max_lag_seconds")]
    pub entry_max_lag_seconds: i64,
    pub hit_threshold_pct: f64,
    pub bootstrap_iterations: usize,
    pub bootstrap_seed: u64,
}
fn default_tuning_days() -> usize {
    14
}
fn default_validation_days() -> usize {
    14
}
fn default_min_validation_signals() -> usize {
    50
}
fn default_entry_max_lag_seconds() -> i64 {
    60
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperConfig {
    pub fee_bps: f64,
    pub slippage_bps: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            data_root: "./data".into(),
            top_market_count: 20,
            daily_refresh_utc_hour: 0,
            imbalance_window_seconds: 60,
            trade_rate_window_seconds: 60,
            wall_min_krw: 10_000_000.0,
            candidate: CandidateConfig {
                imbalance_thresholds: vec![0.25, 0.4],
                large_trade_multiples: vec![3.0, 5.0],
                trade_rate_multiples: vec![2.0, 3.0],
            },
            validation: ValidationConfig {
                tuning_days: 14,
                validation_days: 14,
                min_validation_signals: 50,
                horizon_minutes: 15,
                entry_max_lag_seconds: 60,
                hit_threshold_pct: 0.3,
                bootstrap_iterations: 1000,
                bootstrap_seed: 7,
            },
            paper: PaperConfig {
                fee_bps: 5.0,
                slippage_bps: 3.0,
            },
        }
    }
}
impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(p) if p.exists() => Ok(toml::from_str(&fs::read_to_string(p)?)?),
            _ => Ok(Self::default()),
        }
    }
    pub fn template() -> Result<String> {
        Ok(toml::to_string_pretty(&Self::default())?)
    }
}

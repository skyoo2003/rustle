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
    #[serde(default = "default_stall_timeout_seconds")]
    pub stall_timeout_seconds: i64,
    #[serde(default = "default_flush_interval_seconds")]
    pub flush_interval_seconds: i64,
    /// zstd level for written Parquet, 1..=22.
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
    pub imbalance_window_seconds: i64,
    pub trade_rate_window_seconds: i64,
    pub wall_min_krw: f64,
    pub candidate: CandidateConfig,
    pub validation: ValidationConfig,
    #[serde(default)]
    pub alert: AlertConfig,
    pub paper: PaperConfig,
}
fn default_stall_timeout_seconds() -> i64 {
    90
}
fn default_flush_interval_seconds() -> i64 {
    30
}
fn default_compression_level() -> i32 {
    3
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
    /// Apply a Bonferroni correction across selected signal families.
    #[serde(default = "default_family_wise_correction")]
    pub family_wise_correction: bool,
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
fn default_family_wise_correction() -> bool {
    true
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperConfig {
    pub fee_bps: f64,
    pub slippage_bps: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    #[serde(default = "default_alert_cooldown_seconds")]
    pub cooldown_seconds: i64,
}
fn default_alert_cooldown_seconds() -> i64 {
    900
}
impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            cooldown_seconds: default_alert_cooldown_seconds(),
        }
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            data_root: "./data".into(),
            top_market_count: 20,
            daily_refresh_utc_hour: 0,
            stall_timeout_seconds: default_stall_timeout_seconds(),
            flush_interval_seconds: default_flush_interval_seconds(),
            compression_level: default_compression_level(),
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
                family_wise_correction: true,
                bootstrap_iterations: 10_000,
                bootstrap_seed: 7,
            },
            alert: AlertConfig::default(),
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

    pub fn validate_collection_intervals(&self) -> Result<()> {
        if self.stall_timeout_seconds <= 0 {
            anyhow::bail!("stall_timeout_seconds must be greater than zero");
        }
        if self.flush_interval_seconds <= 0 {
            anyhow::bail!("flush_interval_seconds must be greater than zero");
        }
        if self.validation.entry_max_lag_seconds < 0 {
            anyhow::bail!("entry_max_lag_seconds must be non-negative");
        }
        if self.alert.cooldown_seconds < 0 {
            anyhow::bail!("alert.cooldown_seconds must be non-negative");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_toml_uses_collection_interval_defaults() {
        let mut value = toml::Value::try_from(Config::default()).unwrap();
        let table = value.as_table_mut().unwrap();
        table.remove("stall_timeout_seconds");
        table.remove("flush_interval_seconds");
        table.remove("compression_level");
        table.remove("alert");
        table
            .get_mut("validation")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .remove("family_wise_correction");

        let loaded: Config = value.try_into().unwrap();

        assert_eq!(loaded.stall_timeout_seconds, 90);
        assert_eq!(loaded.flush_interval_seconds, 300);
        assert_eq!(loaded.compression_level, 3);
        assert_eq!(loaded.alert.cooldown_seconds, 900);
        assert!(loaded.validation.family_wise_correction);
    }

    #[test]
    fn flush_interval_defaults_to_five_minutes() {
        // At the measured 65 orderbooks/s across 20 markets, a 30s cadence with a
        // 100-record trigger wrote ~15.6 files/s — 37.7M files over the 28-day gate window.
        // File count must track flush cadence, not record volume.
        assert_eq!(Config::default().flush_interval_seconds, 300);
    }

    #[test]
    fn compression_level_defaults_into_zstds_accepted_range() {
        let cfg = Config::default();
        assert_eq!(cfg.compression_level, 3);
        cfg.validate_collection_intervals().unwrap();
    }

    #[test]
    fn compression_level_outside_zstd_range_is_rejected_before_collection_starts() {
        for bad in [0, 23, -1] {
            let cfg = Config {
                compression_level: bad,
                ..Config::default()
            };
            assert!(
                cfg.validate_collection_intervals()
                    .unwrap_err()
                    .to_string()
                    .contains("compression_level"),
                "level {bad} must be rejected"
            );
        }
        for ok in [1, 22] {
            Config {
                compression_level: ok,
                ..Config::default()
            }
            .validate_collection_intervals()
            .unwrap();
        }
    }

    #[test]
    fn collection_intervals_must_be_positive() {
        for (stall, flush, expected) in [
            (0, 30, "stall_timeout_seconds"),
            (-1, 30, "stall_timeout_seconds"),
            (90, 0, "flush_interval_seconds"),
            (90, -1, "flush_interval_seconds"),
        ] {
            let cfg = Config {
                stall_timeout_seconds: stall,
                flush_interval_seconds: flush,
                ..Config::default()
            };
            assert!(cfg
                .validate_collection_intervals()
                .unwrap_err()
                .to_string()
                .contains(expected));
        }
    }

    #[test]
    fn entry_lag_must_be_non_negative() {
        let mut cfg = Config::default();
        cfg.validation.entry_max_lag_seconds = -1;
        assert!(cfg
            .validate_collection_intervals()
            .unwrap_err()
            .to_string()
            .contains("entry_max_lag_seconds"));
    }

    #[test]
    fn alert_cooldown_must_be_non_negative_but_zero_disables_it() {
        let mut cfg = Config::default();
        cfg.alert.cooldown_seconds = -1;
        assert!(cfg
            .validate_collection_intervals()
            .unwrap_err()
            .to_string()
            .contains("alert.cooldown_seconds"));

        cfg.alert.cooldown_seconds = 0;
        cfg.validate_collection_intervals().unwrap();
    }
}

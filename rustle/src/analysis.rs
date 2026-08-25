use crate::{
    config::Config,
    model::{Meta, Orderbook, Side, Signal, Trade},
};
use anyhow::{bail, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
// rand 0.10 split the convenience methods (`random_range`) out of `Rng` — which is
// now just the core trait re-exported from rand_core — into `RngExt`.
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleResult {
    pub rule_id: String,
    pub signal_type: String,
    pub train_count: usize,
    pub train_hit_rate: f64,
    pub validation_count: usize,
    pub validation_hit_rate: f64,
    pub random_hit_rate: f64,
    pub lift: f64,
    pub ci_low: f64,
    pub ci_high: f64,
    pub passed: bool,
    pub tuning_start: NaiveDate,
    pub tuning_end: NaiveDate,
    pub validation_start: NaiveDate,
    pub validation_end: NaiveDate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationAudit {
    pub input_start: NaiveDate,
    pub input_end: NaiveDate,
    pub collection_dates: Vec<NaiveDate>,
    pub validation_config: crate::config::ValidationConfig,
    pub bootstrap_seed: u64,
    pub results: Vec<RuleResult>,
}
#[derive(Default)]
struct State {
    trades: VecDeque<(DateTime<Utc>, f64)>,
    trade_notional_sum: f64,
    last_wall: Option<(f64, Side)>,
    active_rules: BTreeSet<String>,
}
pub struct SignalDetector {
    cfg: Config,
    states: HashMap<String, State>,
}
impl SignalDetector {
    pub fn new(cfg: &Config) -> Self {
        Self {
            cfg: cfg.clone(),
            states: HashMap::new(),
        }
    }

    pub fn on_trade(&mut self, t: &Trade) -> Vec<Signal> {
        let mut out = vec![];
        let s = self.states.entry(t.meta.market.clone()).or_default();
        while s.trades.front().is_some_and(|x| {
            x.0 < t.meta.exchange_ts - Duration::seconds(self.cfg.trade_rate_window_seconds)
        }) {
            let (_, notional) = s.trades.pop_front().expect("front was checked");
            s.trade_notional_sum -= notional;
        }
        let baseline = s.trade_notional_sum / (s.trades.len().max(1) as f64);
        let value = t.price * t.volume;
        for &m in &self.cfg.candidate.large_trade_multiples {
            let rule = format!("large-{m}");
            let active = s.trades.len() >= 5 && value > baseline * m;
            if active && s.active_rules.insert(rule.clone()) {
                out.push(signal(
                    t.meta.clone(),
                    "large_aggressive_trade",
                    t.side,
                    value,
                    baseline,
                    format!(
                        "aggressive notional {:.0} is {:.1}x rolling mean",
                        value,
                        value / baseline
                    ),
                    rule,
                ));
            } else if !active {
                s.active_rules.remove(&rule);
            }
        }
        let prior = s.trades.len();
        let rate = (prior as f64) / (self.cfg.trade_rate_window_seconds as f64);
        let base = 5.0 / (self.cfg.trade_rate_window_seconds as f64);
        for &m in &self.cfg.candidate.trade_rate_multiples {
            let rule = format!("rate-{m}");
            let active = prior >= 5 && rate > base * m;
            if active && s.active_rules.insert(rule.clone()) {
                out.push(signal(
                    t.meta.clone(),
                    "trade_rate_acceleration",
                    t.side,
                    rate,
                    base,
                    format!("{} trades in rolling window", prior),
                    rule,
                ));
            } else if !active {
                s.active_rules.remove(&rule);
            }
        }
        s.trades.push_back((t.meta.exchange_ts, value));
        s.trade_notional_sum += value;
        out
    }

    pub fn on_orderbook(&mut self, b: &Orderbook) -> Vec<Signal> {
        let mut out = vec![];
        let s = self.states.entry(b.meta.market.clone()).or_default();
        let denom = b.total_bid_size + b.total_ask_size;
        let im = (denom > 0.0).then(|| (b.total_bid_size - b.total_ask_size) / denom);
        for &th in &self.cfg.candidate.imbalance_thresholds {
            let rule = format!("imbalance-{th}");
            let active = im.is_some_and(|value| value.abs() >= th);
            if active && s.active_rules.insert(rule.clone()) {
                let value = im.expect("active imbalance has a value");
                let direction = if value > 0.0 { Side::Buy } else { Side::Sell };
                out.push(signal(
                    b.meta.clone(),
                    "orderbook_imbalance",
                    direction,
                    value,
                    0.0,
                    format!("bid/ask size imbalance {:.3}", value),
                    rule,
                ));
            } else if !active {
                s.active_rules.remove(&rule);
            }
        }
        if b.levels.is_empty() {
            s.last_wall = None;
            return out;
        }
        let bid_wall = b
            .levels
            .iter()
            .map(|level| level.bid_price * level.bid_size)
            .fold(0.0_f64, f64::max);
        let ask_wall = b
            .levels
            .iter()
            .map(|level| level.ask_price * level.ask_size)
            .fold(0.0_f64, f64::max);
        let (wall, side) = if ask_wall > bid_wall {
            (ask_wall, Side::Sell)
        } else {
            (bid_wall, Side::Buy)
        };
        if let Some((old, oldside)) = s.last_wall {
            if old >= self.cfg.wall_min_krw && wall < old * 0.2 {
                out.push(signal(
                    b.meta.clone(),
                    "wall_disappearance",
                    oldside,
                    wall,
                    old,
                    format!("previous {:.0} KRW {:?} wall disappeared", old, oldside),
                    "wall-drop".into(),
                ));
            }
        }
        s.last_wall = Some((wall, side));
        out
    }

    /// Drop rolling observations for markets which are no longer in the live universe.
    pub fn retain_markets(&mut self, markets: &[String]) {
        let markets: BTreeSet<&str> = markets.iter().map(String::as_str).collect();
        self.states
            .retain(|market, _| markets.contains(market.as_str()));
    }
}
struct SelectedCandidate<'a> {
    id: String,
    signals: Vec<&'a Signal>,
    train_hits: Vec<bool>,
    train_controls: Vec<bool>,
}
pub fn build_signals(
    mut trades: Vec<Trade>,
    mut books: Vec<Orderbook>,
    cfg: &Config,
) -> Vec<Signal> {
    trades.sort_by_key(|trade| serde_json::to_string(trade).expect("trade serializes"));
    books.sort_by_key(|book| serde_json::to_string(book).expect("orderbook serializes"));
    let mut events: Vec<(DateTime<Utc>, bool, usize)> = trades
        .iter()
        .enumerate()
        .map(|(i, t)| (t.meta.exchange_ts, true, i))
        .chain(
            books
                .iter()
                .enumerate()
                .map(|(i, b)| (b.meta.exchange_ts, false, i)),
        )
        .collect();
    events.sort_by_key(|event| (event.0, event.1, event.2));
    let mut detector = SignalDetector::new(cfg);
    let mut out = vec![];
    for (_, is_trade, i) in events {
        if is_trade {
            out.extend(detector.on_trade(&trades[i]));
        } else {
            out.extend(detector.on_orderbook(&books[i]));
        }
    }
    out
}
fn signal(
    meta: Meta,
    ty: &str,
    direction: Side,
    value: f64,
    baseline: f64,
    rationale: String,
    rule: String,
) -> Signal {
    Signal {
        market_snapshot: serde_json::json!({"market":meta.market,"exchange_ts":meta.exchange_ts,"feature":value,"baseline":baseline}),
        meta,
        signal_type: ty.into(),
        direction,
        feature_value: value,
        baseline,
        rationale,
        rule_id: rule,
    }
}
fn is_hit(s: &Signal, trades: &[Trade], cfg: &Config) -> bool {
    let entry = trades
        .iter()
        .find(|t| t.meta.market == s.meta.market && t.meta.exchange_ts >= s.meta.exchange_ts)
        .map(|t| t.price);
    let Some(entry) = entry else { return false };
    let end = s.meta.exchange_ts + Duration::minutes(cfg.validation.horizon_minutes);
    trades
        .iter()
        .filter(|t| {
            t.meta.market == s.meta.market
                && t.meta.exchange_ts >= s.meta.exchange_ts
                && t.meta.exchange_ts <= end
        })
        .any(|t| match s.direction {
            Side::Buy => (t.price / entry - 1.0) * 100.0 >= cfg.validation.hit_threshold_pct,
            Side::Sell => (entry / t.price - 1.0) * 100.0 >= cfg.validation.hit_threshold_pct,
        })
}
fn complete_horizon(s: &Signal, trades: &[Trade], cfg: &Config) -> bool {
    let end = s.meta.exchange_ts + Duration::minutes(cfg.validation.horizon_minutes);
    trades
        .iter()
        .any(|t| t.meta.market == s.meta.market && t.meta.exchange_ts >= s.meta.exchange_ts)
        && trades
            .iter()
            .any(|t| t.meta.market == s.meta.market && t.meta.exchange_ts >= end)
}

fn rate(values: &[bool]) -> f64 {
    values.iter().filter(|&&x| x).count() as f64 / values.len().max(1) as f64
}

fn seeded(seed: u64, label: &str) -> StdRng {
    // Stable FNV-1a mixing avoids HashMap iteration order affecting reproducibility.
    let hash = label.bytes().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100000001b3)
    });
    StdRng::seed_from_u64(seed ^ hash)
}

fn paired_outcomes(
    signals: &[&Signal],
    trades: &[Trade],
    cfg: &Config,
    label: &str,
) -> (Vec<bool>, Vec<bool>) {
    let mut rng = seeded(cfg.validation.bootstrap_seed, label);
    let mut hits = Vec::new();
    let mut controls = Vec::new();
    for signal in signals
        .iter()
        .copied()
        .filter(|s| complete_horizon(s, trades, cfg))
    {
        // The pool is matched on exactly the signal's market and UTC date.
        let pool: Vec<&Trade> = trades
            .iter()
            .filter(|t| {
                t.meta.market == signal.meta.market
                    && t.meta.exchange_ts.date_naive() == signal.meta.exchange_ts.date_naive()
            })
            .filter(|t| {
                let mut control = signal.clone();
                control.meta.exchange_ts = t.meta.exchange_ts;
                control.meta.receive_ts = t.meta.receive_ts;
                complete_horizon(&control, trades, cfg)
            })
            .collect();
        if let Some(t) = (!pool.is_empty()).then(|| pool[rng.random_range(0..pool.len())]) {
            let mut control = signal.clone();
            control.meta.exchange_ts = t.meta.exchange_ts;
            control.meta.receive_ts = t.meta.receive_ts;
            hits.push(is_hit(signal, trades, cfg));
            controls.push(is_hit(&control, trades, cfg));
        }
    }
    (hits, controls)
}

fn confidence_interval(hits: &[bool], controls: &[bool], cfg: &Config, label: &str) -> (f64, f64) {
    if hits.is_empty() {
        return (0.0, 0.0);
    }
    let mut rng = seeded(cfg.validation.bootstrap_seed, &format!("bootstrap:{label}"));
    let n = hits.len();
    let mut samples = Vec::with_capacity(cfg.validation.bootstrap_iterations);
    for _ in 0..cfg.validation.bootstrap_iterations {
        let mut signal_hits = 0usize;
        let mut random_hits = 0usize;
        for _ in 0..n {
            let i = rng.random_range(0..n); // paired resampling preserves each signal/control pair
            signal_hits += hits[i] as usize;
            random_hits += controls[i] as usize;
        }
        samples.push(signal_hits as f64 / n as f64 - random_hits as f64 / n as f64);
    }
    samples.sort_by(|a, b| a.total_cmp(b));
    let last = samples.len() - 1;
    (samples[(last * 25) / 1000], samples[(last * 975) / 1000])
}

pub fn evaluate_with_audit(
    signals: &[Signal],
    trades: &[Trade],
    books: &[Orderbook],
    cfg: &Config,
) -> Result<EvaluationAudit> {
    let required = cfg.validation.tuning_days + cfg.validation.validation_days;
    if cfg.validation.tuning_days == 0 || cfg.validation.validation_days == 0 {
        bail!("tuning_days and validation_days must both be greater than zero");
    }
    if cfg.validation.bootstrap_iterations == 0 {
        bail!("bootstrap_iterations must be greater than zero");
    }
    let dates: BTreeSet<NaiveDate> = trades
        .iter()
        .map(|t| t.meta.exchange_ts.date_naive())
        .chain(books.iter().map(|b| b.meta.exchange_ts.date_naive()))
        .collect();
    if dates.is_empty() {
        bail!("need {required} UTC collection dates, found none");
    }
    let end = *dates.iter().next_back().expect("checked non-empty");
    let start = end - Duration::days((required - 1) as i64);
    let expected: Vec<_> = (0..required)
        .map(|i| start + Duration::days(i as i64))
        .collect();
    let missing: Vec<_> = expected.iter().filter(|d| !dates.contains(d)).collect();
    if !missing.is_empty() {
        bail!(
            "missing UTC collection dates: {}",
            missing
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if dates.len() < required {
        bail!(
            "need {required} UTC collection dates, found {} ({})",
            dates.len(),
            dates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let tuning_end = start + Duration::days((cfg.validation.tuning_days - 1) as i64);
    let validation_start = tuning_end + Duration::days(1);
    let mut candidates: BTreeMap<String, Vec<&Signal>> = BTreeMap::new();
    for signal in signals.iter().filter(|s| {
        s.meta.exchange_ts.date_naive() >= start && s.meta.exchange_ts.date_naive() <= end
    }) {
        candidates
            .entry(format!("{}:{}", signal.signal_type, signal.rule_id))
            .or_default()
            .push(signal);
    }
    let mut selected: BTreeMap<String, SelectedCandidate<'_>> = BTreeMap::new();
    for (id, candidate) in candidates {
        let train: Vec<_> = candidate
            .iter()
            .copied()
            .filter(|s| s.meta.exchange_ts.date_naive() <= tuning_end)
            .collect();
        let (hits, controls) = paired_outcomes(&train, trades, cfg, &format!("train:{id}"));
        let key = candidate.first().expect("non-empty").signal_type.clone();
        let replace = selected
            .get(&key)
            .map(|best| {
                let lift = rate(&hits) - rate(&controls);
                let best_lift = rate(&best.train_hits) - rate(&best.train_controls);
                lift > best_lift || (lift == best_lift && hits.len() > best.train_hits.len())
            })
            .unwrap_or(true);
        if replace {
            selected.insert(
                key,
                SelectedCandidate {
                    id,
                    signals: candidate,
                    train_hits: hits,
                    train_controls: controls,
                },
            );
        }
    }
    let results = selected
        .into_iter()
        .map(|(signal_type, selected)| {
            let validation: Vec<_> = selected
                .signals
                .iter()
                .copied()
                .filter(|s| s.meta.exchange_ts.date_naive() >= validation_start)
                .collect();
            let (hits, controls) = paired_outcomes(
                &validation,
                trades,
                cfg,
                &format!("validation:{}", selected.id),
            );
            let (ci_low, ci_high) = confidence_interval(&hits, &controls, cfg, &selected.id);
            let validation_hit_rate = rate(&hits);
            let random_hit_rate = rate(&controls);
            RuleResult {
                rule_id: selected.id,
                signal_type,
                train_count: selected.train_hits.len(),
                train_hit_rate: rate(&selected.train_hits),
                validation_count: hits.len(),
                validation_hit_rate,
                random_hit_rate,
                lift: validation_hit_rate - random_hit_rate,
                ci_low,
                ci_high,
                passed: hits.len() >= cfg.validation.min_validation_signals && ci_low > 0.0,
                tuning_start: start,
                tuning_end,
                validation_start,
                validation_end: end,
            }
        })
        .collect();
    Ok(EvaluationAudit {
        input_start: start,
        input_end: end,
        collection_dates: expected,
        validation_config: cfg.validation.clone(),
        bootstrap_seed: cfg.validation.bootstrap_seed,
        results,
    })
}

pub fn evaluate(
    signals: &[Signal],
    trades: &[Trade],
    books: &[Orderbook],
    cfg: &Config,
) -> Result<Vec<RuleResult>> {
    Ok(evaluate_with_audit(signals, trades, books, cfg)?.results)
}
pub fn paper(
    signals: &[Signal],
    trades: &[Trade],
    passed: &[String],
    cfg: &Config,
) -> Vec<crate::model::PaperTrade> {
    signals
        .iter()
        .filter(|s| passed.contains(&format!("{}:{}", s.signal_type, s.rule_id)))
        .filter_map(|s| {
            let e = trades.iter().find(|t| {
                t.meta.market == s.meta.market && t.meta.exchange_ts >= s.meta.exchange_ts
            })?;
            let x = trades.iter().find(|t| {
                t.meta.market == s.meta.market
                    && t.meta.exchange_ts >= s.meta.exchange_ts + Duration::minutes(15)
            })?;
            let g = match s.direction {
                Side::Buy => (x.price / e.price - 1.0) * 100.0,
                Side::Sell => (e.price / x.price - 1.0) * 100.0,
            };
            Some(crate::model::PaperTrade {
                signal: s.clone(),
                entry_price: e.price,
                exit_price: x.price,
                gross_pnl_pct: g,
                net_pnl_pct: g - 2.0 * (cfg.paper.fee_bps + cfg.paper.slippage_bps) / 100.0,
                benchmark_pnl_pct: (x.price / e.price - 1.0) * 100.0,
            })
        })
        .collect()
}

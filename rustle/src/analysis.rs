use crate::{
    config::Config,
    model::{AlertEvent, ConnectionEvent, Meta, Orderbook, Side, Signal, SignalOutcome, Trade},
};
use anyhow::{bail, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
// rand 0.10 split the convenience methods (`random_range`) out of `Rng` — which is
// now just the core trait re-exported from rand_core — into `RngExt`.
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt::Write,
};

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
    #[serde(default)]
    pub retention: f64,
    pub passed: bool,
    pub tuning_start: NaiveDate,
    pub tuning_end: NaiveDate,
    pub validation_start: NaiveDate,
    pub validation_end: NaiveDate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateSummary {
    pub rule_id: String,
    pub signal_type: String,
    pub train_count: usize,
    pub train_hit_rate: f64,
    pub train_random_hit_rate: f64,
    pub selected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationAudit {
    pub input_start: NaiveDate,
    pub input_end: NaiveDate,
    pub collection_dates: Vec<NaiveDate>,
    pub validation_config: crate::config::ValidationConfig,
    pub bootstrap_seed: u64,
    #[serde(default)]
    pub candidates: Vec<CandidateSummary>,
    #[serde(default)]
    pub family_size: usize,
    #[serde(default)]
    pub effective_tail_alpha: f64,
    pub results: Vec<RuleResult>,
}
/// A deterministic fingerprint of every configuration value that can affect collection,
/// detection, replay, or validation.
pub fn config_fingerprint(cfg: &Config) -> String {
    let mut value = serde_json::to_value(cfg).expect("config serializes");
    value
        .as_object_mut()
        .expect("config serializes as an object")
        .remove("alert");
    let bytes = serde_json::to_vec(&value).expect("config value serializes");
    let hash = bytes.into_iter().fold(0xcbf29ce484222325u64, |h, byte| {
        (h ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("fnv1a64:{hash:016x}")
}

pub fn current_collection_dates(
    trades: &[Trade],
    orderbook_dates: &BTreeSet<NaiveDate>,
    cfg: &Config,
) -> Result<Vec<NaiveDate>> {
    let status = collection_date_status(trades, orderbook_dates, cfg)?;
    let required = status.required;
    if status.end.is_none() {
        bail!("need {required} UTC collection dates, found none");
    }
    if !status.missing_dates.is_empty() {
        bail!(
            "missing UTC collection dates: {}",
            status
                .missing_dates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(status.required_dates)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionDateStatus {
    pub required: usize,
    pub end: Option<NaiveDate>,
    pub required_dates: Vec<NaiveDate>,
    pub present_count: usize,
    pub missing_dates: Vec<NaiveDate>,
}

pub fn collection_date_status(
    trades: &[Trade],
    orderbook_dates: &BTreeSet<NaiveDate>,
    cfg: &Config,
) -> Result<CollectionDateStatus> {
    let required = cfg.validation.tuning_days + cfg.validation.validation_days;
    if cfg.validation.tuning_days == 0 || cfg.validation.validation_days == 0 {
        bail!("tuning_days and validation_days must both be greater than zero");
    }
    let dates: BTreeSet<NaiveDate> = trades
        .iter()
        .map(|t| t.meta.exchange_ts.date_naive())
        .chain(orderbook_dates.iter().copied())
        .collect();
    if dates.is_empty() {
        return Ok(CollectionDateStatus {
            required,
            end: None,
            required_dates: vec![],
            present_count: 0,
            missing_dates: vec![],
        });
    }
    let end = *dates.iter().next_back().expect("checked non-empty");
    let start = end - Duration::days((required - 1) as i64);
    let expected: Vec<_> = (0..required)
        .map(|i| start + Duration::days(i as i64))
        .collect();
    let missing_dates: Vec<_> = expected
        .iter()
        .filter(|date| !dates.contains(date))
        .copied()
        .collect();
    Ok(CollectionDateStatus {
        required,
        end: Some(end),
        present_count: required - missing_dates.len(),
        required_dates: expected,
        missing_dates,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoverageDay {
    pub date: NaiveDate,
    pub trade_count: usize,
    pub orderbook_count: usize,
    pub live_signal_count: usize,
    pub market_count: usize,
    pub disconnected_count: usize,
    pub stalled_count: usize,
    pub total_gap_ms: i64,
    pub longest_gap_ms: i64,
}

pub fn collection_coverage(
    trades: &[Trade],
    books: &[Orderbook],
    live_signals: &[Signal],
    connection_events: &[ConnectionEvent],
) -> Vec<CoverageDay> {
    #[derive(Default)]
    struct Counts {
        day: CoverageDay,
        markets: BTreeSet<String>,
    }
    let mut days: BTreeMap<NaiveDate, Counts> = BTreeMap::new();
    for trade in trades {
        let entry = days.entry(trade.meta.exchange_ts.date_naive()).or_default();
        entry.day.trade_count += 1;
        entry.markets.insert(trade.meta.market.clone());
    }
    for book in books {
        let entry = days.entry(book.meta.exchange_ts.date_naive()).or_default();
        entry.day.orderbook_count += 1;
        entry.markets.insert(book.meta.market.clone());
    }
    for signal in live_signals {
        days.entry(signal.meta.exchange_ts.date_naive())
            .or_default()
            .day
            .live_signal_count += 1;
    }
    for event in connection_events {
        let entry = &mut days
            .entry(event.meta.exchange_ts.date_naive())
            .or_default()
            .day;
        match event.state.as_str() {
            "disconnected" => entry.disconnected_count += 1,
            "stalled" => entry.stalled_count += 1,
            "connected" => {
                if let Some(gap_ms) = event.gap_ms {
                    entry.total_gap_ms += gap_ms;
                    entry.longest_gap_ms = entry.longest_gap_ms.max(gap_ms);
                }
            }
            _ => {}
        }
    }
    days.into_iter()
        .map(|(date, mut counts)| {
            counts.day.date = date;
            counts.day.market_count = counts.markets.len();
            counts.day
        })
        .collect()
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
                        "aggressive notional {:.0} is {:.1}x rolling mean (threshold {:.1}x over {} trades)",
                        value,
                        value / baseline,
                        m,
                        s.trades.len()
                    ),
                    rule,
                    serde_json::json!({
                        "source":"trade", "trigger": t,
                        "rolling_trade_count": s.trades.len(),
                        "rolling_notional_sum": s.trade_notional_sum,
                        "rolling_notional_mean": baseline,
                    }),
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
                    format!(
                        "{} trades in {}s window = {:.1}x the 5-trade baseline (threshold {:.1}x)",
                        prior,
                        self.cfg.trade_rate_window_seconds,
                        rate / base,
                        m
                    ),
                    rule,
                    serde_json::json!({
                        "source":"trade", "trigger": t,
                        "rolling_trade_count": prior,
                        "window_seconds": self.cfg.trade_rate_window_seconds,
                        "rolling_rate_per_second": rate,
                        "baseline_rate_per_second": base,
                    }),
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
                    format!(
                        "bid/ask size imbalance {:.3} (threshold {:.2}, {}-heavy)",
                        value,
                        th,
                        if value > 0.0 { "bid" } else { "ask" }
                    ),
                    rule,
                    serde_json::json!({
                        "source":"orderbook", "total_bid_size": b.total_bid_size,
                        "total_ask_size": b.total_ask_size, "levels": b.levels,
                        "imbalance": value,
                    }),
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
                    format!(
                        "{:.0} KRW {:?} wall fell to {:.0} ({:.0}% drop, qualifying floor {:.0} KRW)",
                        old,
                        oldside,
                        wall,
                        (1.0 - wall / old) * 100.0,
                        self.cfg.wall_min_krw
                    ),
                    "wall-drop".into(),
                    serde_json::json!({
                        "source":"orderbook", "total_bid_size": b.total_bid_size,
                        "total_ask_size": b.total_ask_size, "levels": b.levels,
                        "previous_wall_krw": old, "previous_wall_side": oldside,
                        "current_largest_wall_krw": wall, "current_largest_wall_side": side,
                    }),
                ));
            }
        }
        s.last_wall = Some((wall, side));
        out
    }

    /// Drop rolling observations for markets which are no longer in the live universe.
    /// A reconnect is a discontinuity: never carry a rolling baseline across it.
    pub fn reset(&mut self) {
        self.states.clear();
    }
}
struct SelectedCandidate<'a> {
    id: String,
    signals: Vec<&'a Signal>,
    train_hits: Vec<bool>,
    train_controls: Vec<bool>,
}
pub fn build_signals(trades: &[Trade], books: &[Orderbook], cfg: &Config) -> Vec<Signal> {
    let mut trades: Vec<&Trade> = trades.iter().collect();
    let mut books: Vec<&Orderbook> = books.iter().collect();
    trades.sort_by(|a, b| trade_cmp(a, b));
    books.sort_by(|a, b| book_cmp(a, b));
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
            out.extend(detector.on_trade(trades[i]));
        } else {
            out.extend(detector.on_orderbook(books[i]));
        }
    }
    out
}
fn trade_cmp(a: &Trade, b: &Trade) -> Ordering {
    a.meta
        .exchange_ts
        .cmp(&b.meta.exchange_ts)
        .then_with(|| a.meta.receive_ts.cmp(&b.meta.receive_ts))
        .then_with(|| a.meta.market.cmp(&b.meta.market))
        .then_with(|| a.sequential_id.cmp(&b.sequential_id))
        .then_with(|| a.price.total_cmp(&b.price))
        .then_with(|| a.volume.total_cmp(&b.volume))
        .then_with(|| (a.side as u8).cmp(&(b.side as u8)))
}
fn book_cmp(a: &Orderbook, b: &Orderbook) -> Ordering {
    a.meta
        .exchange_ts
        .cmp(&b.meta.exchange_ts)
        .then_with(|| a.meta.receive_ts.cmp(&b.meta.receive_ts))
        .then_with(|| a.meta.market.cmp(&b.meta.market))
        .then_with(|| a.total_ask_size.total_cmp(&b.total_ask_size))
        .then_with(|| a.total_bid_size.total_cmp(&b.total_bid_size))
        .then_with(|| {
            a.levels
                .iter()
                .zip(&b.levels)
                .map(|(left, right)| {
                    left.ask_price
                        .total_cmp(&right.ask_price)
                        .then_with(|| left.bid_price.total_cmp(&right.bid_price))
                        .then_with(|| left.ask_size.total_cmp(&right.ask_size))
                        .then_with(|| left.bid_size.total_cmp(&right.bid_size))
                })
                .find(|order| *order != Ordering::Equal)
                .unwrap_or_else(|| a.levels.len().cmp(&b.levels.len()))
        })
}
#[allow(clippy::too_many_arguments)] // inputs mirror the persisted Signal fields plus provenance
fn signal(
    meta: Meta,
    ty: &str,
    direction: Side,
    value: f64,
    baseline: f64,
    rationale: String,
    rule: String,
    evidence: serde_json::Value,
) -> Signal {
    assert!(
        !rationale.trim().is_empty(),
        "signal rationale must not be empty"
    );
    assert!(!evidence.is_null(), "signal evidence must not be null");
    Signal {
        market_snapshot: serde_json::json!({"market":meta.market,"exchange_ts":meta.exchange_ts,"feature":value,"baseline":baseline,"evidence":evidence}),
        meta,
        signal_type: ty.into(),
        direction,
        feature_value: value,
        baseline,
        rationale,
        rule_id: rule,
    }
}
pub fn rule_key(signal: &Signal) -> String {
    format!("{}:{}", signal.signal_type, signal.rule_id)
}

struct TradeIndex<'a> {
    by_market: HashMap<&'a str, Vec<&'a Trade>>,
}
impl<'a> TradeIndex<'a> {
    fn new(trades: &'a [Trade]) -> Self {
        let mut by_market: HashMap<&str, Vec<&Trade>> = HashMap::new();
        for trade in trades {
            by_market.entry(&trade.meta.market).or_default().push(trade);
        }
        for values in by_market.values_mut() {
            values.sort_by(|a, b| trade_cmp(a, b));
        }
        Self { by_market }
    }
    fn at_or_after(&self, market: &str, time: DateTime<Utc>) -> Option<&'a Trade> {
        let values = self.by_market.get(market)?;
        let pos = values.partition_point(|trade| trade.meta.exchange_ts < time);
        values.get(pos).copied()
    }
    fn on_date(&self, market: &str, date: NaiveDate) -> &[&'a Trade] {
        let Some(values) = self.by_market.get(market) else {
            return &[];
        };
        let start = date.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
        let end = start + Duration::days(1);
        let first = values.partition_point(|trade| trade.meta.exchange_ts < start);
        let last = values.partition_point(|trade| trade.meta.exchange_ts < end);
        &values[first..last]
    }
    fn last_ts(&self, market: &str) -> Option<DateTime<Utc>> {
        self.by_market
            .get(market)
            .and_then(|values| values.last())
            .map(|trade| trade.meta.exchange_ts)
    }
    fn complete_prefix_len(&self, market: &str, date: NaiveDate, horizon_minutes: i64) -> usize {
        let on_date = self.on_date(market, date);
        self.last_ts(market)
            .map(|last| last - Duration::minutes(horizon_minutes))
            .map_or(0, |cutoff| {
                on_date.partition_point(|trade| trade.meta.exchange_ts <= cutoff)
            })
    }
    fn between(&self, market: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> &[&'a Trade] {
        let Some(values) = self.by_market.get(market) else {
            return &[];
        };
        let first = values.partition_point(|trade| trade.meta.exchange_ts < start);
        let last = values.partition_point(|trade| trade.meta.exchange_ts <= end);
        &values[first..last]
    }
}
#[derive(Clone, Copy)]
struct OutcomeValues {
    entry: Option<f64>,
    horizon: Option<f64>,
    hit: Option<bool>,
    complete: bool,
}
fn outcome_at(
    signal: &Signal,
    at: DateTime<Utc>,
    index: &TradeIndex<'_>,
    cfg: &Config,
) -> OutcomeValues {
    let entry = index.at_or_after(&signal.meta.market, at).filter(|trade| {
        trade.meta.exchange_ts <= at + Duration::seconds(cfg.validation.entry_max_lag_seconds)
    });
    let end = at + Duration::minutes(cfg.validation.horizon_minutes);
    let horizon = index.at_or_after(&signal.meta.market, end);
    let complete = entry.is_some() && horizon.is_some();
    let hit = complete.then(|| {
        let entry_trade = entry.expect("complete has entry");
        let entry = entry_trade.price;
        index
            .between(&signal.meta.market, entry_trade.meta.exchange_ts, end)
            .iter()
            .any(|t| match signal.direction {
                Side::Buy => (t.price / entry - 1.0) * 100.0 >= cfg.validation.hit_threshold_pct,
                Side::Sell => (entry / t.price - 1.0) * 100.0 >= cfg.validation.hit_threshold_pct,
            })
    });
    OutcomeValues {
        entry: entry.map(|t| t.price),
        horizon: horizon.map(|t| t.price),
        hit,
        complete,
    }
}
pub fn outcome(signal: &Signal, trades: &[Trade], cfg: &Config) -> SignalOutcome {
    let index = TradeIndex::new(trades);
    let values = outcome_at(signal, signal.meta.exchange_ts, &index, cfg);
    SignalOutcome {
        signal: signal.clone(),
        rule_key: rule_key(signal),
        entry_price: values.entry,
        horizon_price: values.horizon,
        reached_target: values.hit,
        complete: values.complete,
    }
}

pub fn build_outcomes(signals: &[Signal], trades: &[Trade], cfg: &Config) -> Vec<SignalOutcome> {
    let index = TradeIndex::new(trades);
    signals
        .iter()
        .map(|signal| {
            let values = outcome_at(signal, signal.meta.exchange_ts, &index, cfg);
            SignalOutcome {
                signal: signal.clone(),
                rule_key: rule_key(signal),
                entry_price: values.entry,
                horizon_price: values.horizon,
                reached_target: values.hit,
                complete: values.complete,
            }
        })
        .collect()
}

fn rate(values: &[bool]) -> f64 {
    values.iter().filter(|&&x| x).count() as f64 / values.len().max(1) as f64
}

fn retention(train_hit_rate: f64, validation_hit_rate: f64) -> f64 {
    if train_hit_rate == 0.0 {
        0.0
    } else {
        validation_hit_rate / train_hit_rate
    }
}

fn passes_gate(validation_count: usize, ci_low: f64, retention: f64, cfg: &Config) -> bool {
    validation_count >= cfg.validation.min_validation_signals && ci_low > 0.0 && retention >= 0.80
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
    index: &TradeIndex<'_>,
    cfg: &Config,
    label: &str,
) -> (Vec<bool>, Vec<bool>) {
    let mut rng = seeded(cfg.validation.bootstrap_seed, label);
    let mut hits = Vec::new();
    let mut controls = Vec::new();
    for signal in signals.iter().copied() {
        let signal_outcome = outcome_at(signal, signal.meta.exchange_ts, index, cfg);
        if !signal_outcome.complete {
            continue;
        }
        // The pool is matched on exactly the signal's market and UTC date.
        let on_date = index.on_date(&signal.meta.market, signal.meta.exchange_ts.date_naive());
        let pool_len = index.complete_prefix_len(
            &signal.meta.market,
            signal.meta.exchange_ts.date_naive(),
            cfg.validation.horizon_minutes,
        );
        if let Some(t) = (pool_len > 0).then(|| on_date[rng.random_range(0..pool_len)]) {
            hits.push(signal_outcome.hit.unwrap_or(false));
            controls.push(
                outcome_at(signal, t.meta.exchange_ts, index, cfg)
                    .hit
                    .unwrap_or(false),
            );
        }
    }
    (hits, controls)
}

fn confidence_interval(
    hits: &[bool],
    controls: &[bool],
    cfg: &Config,
    label: &str,
    tail_alpha: f64,
) -> (f64, f64) {
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
    let low = ((last as f64) * tail_alpha).floor() as usize;
    let high = ((last as f64) * (1.0 - tail_alpha)).floor() as usize;
    (samples[low], samples[high])
}

pub fn evaluate_with_audit(
    signals: &[Signal],
    trades: &[Trade],
    orderbook_dates: &BTreeSet<NaiveDate>,
    cfg: &Config,
) -> Result<EvaluationAudit> {
    if cfg.validation.bootstrap_iterations == 0 {
        bail!("bootstrap_iterations must be greater than zero");
    }
    if cfg.validation.entry_max_lag_seconds < 0 {
        bail!("entry_max_lag_seconds must be non-negative");
    }
    let expected = current_collection_dates(trades, orderbook_dates, cfg)?;
    let start = *expected.first().expect("validated non-empty");
    let end = *expected.last().expect("validated non-empty");
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
    let index = TradeIndex::new(trades);
    let mut selected: BTreeMap<String, SelectedCandidate<'_>> = BTreeMap::new();
    let mut candidate_summaries = Vec::new();
    for (id, candidate) in candidates {
        let train: Vec<_> = candidate
            .iter()
            .copied()
            .filter(|s| s.meta.exchange_ts.date_naive() <= tuning_end)
            .collect();
        let (hits, controls) = paired_outcomes(&train, &index, cfg, &format!("train:{id}"));
        let key = candidate.first().expect("non-empty").signal_type.clone();
        candidate_summaries.push(CandidateSummary {
            rule_id: id.clone(),
            signal_type: key.clone(),
            train_count: hits.len(),
            train_hit_rate: rate(&hits),
            train_random_hit_rate: rate(&controls),
            selected: false,
        });
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
    for summary in &mut candidate_summaries {
        summary.selected = selected
            .get(&summary.signal_type)
            .is_some_and(|candidate| candidate.id == summary.rule_id);
    }
    struct ValidationCandidate<'a> {
        signal_type: String,
        selected: SelectedCandidate<'a>,
        hits: Vec<bool>,
        controls: Vec<bool>,
    }
    let validation_candidates: Vec<_> = selected
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
                &index,
                cfg,
                &format!("validation:{}", selected.id),
            );
            ValidationCandidate {
                signal_type,
                selected,
                hits,
                controls,
            }
        })
        .collect();
    let family_size = validation_candidates
        .iter()
        .filter(|candidate| !candidate.hits.is_empty())
        .count();
    let effective_tail_alpha = if cfg.validation.family_wise_correction && family_size > 0 {
        0.025 / family_size as f64
    } else {
        0.025
    };
    let results = validation_candidates
        .into_iter()
        .map(|candidate| {
            let (ci_low, ci_high) = confidence_interval(
                &candidate.hits,
                &candidate.controls,
                cfg,
                &candidate.selected.id,
                effective_tail_alpha,
            );
            let hits = candidate.hits;
            let controls = candidate.controls;
            let validation_hit_rate = rate(&hits);
            let random_hit_rate = rate(&controls);
            let train_hit_rate = rate(&candidate.selected.train_hits);
            let retention = retention(train_hit_rate, validation_hit_rate);
            RuleResult {
                rule_id: candidate.selected.id,
                signal_type: candidate.signal_type,
                train_count: candidate.selected.train_hits.len(),
                train_hit_rate,
                validation_count: hits.len(),
                validation_hit_rate,
                random_hit_rate,
                lift: validation_hit_rate - random_hit_rate,
                ci_low,
                ci_high,
                retention,
                passed: passes_gate(hits.len(), ci_low, retention, cfg),
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
        candidates: candidate_summaries,
        family_size,
        effective_tail_alpha,
        results,
    })
}

pub fn evaluate(
    signals: &[Signal],
    trades: &[Trade],
    orderbook_dates: &BTreeSet<NaiveDate>,
    cfg: &Config,
) -> Result<Vec<RuleResult>> {
    Ok(evaluate_with_audit(signals, trades, orderbook_dates, cfg)?.results)
}

pub fn render_csv_report(audit: &EvaluationAudit) -> String {
    let mut output = String::from(
        "signal_type,rule_id,selected,tuning_start,tuning_end,validation_start,validation_end,train_count,train_hit_rate,train_random_hit_rate,validation_count,validation_hit_rate,random_hit_rate,lift,ci_low,ci_high,retention,passed\n",
    );
    for candidate in &audit.candidates {
        let result = audit
            .results
            .iter()
            .find(|result| candidate.selected && result.rule_id == candidate.rule_id);
        let (tuning_start, tuning_end, validation_start, validation_end) = audit
            .results
            .first()
            .map(|result| {
                (
                    result.tuning_start,
                    result.tuning_end,
                    result.validation_start,
                    result.validation_end,
                )
            })
            .unwrap_or((
                audit.input_start,
                audit.input_start
                    + Duration::days(
                        (audit.validation_config.tuning_days.saturating_sub(1)) as i64,
                    ),
                audit.input_start + Duration::days(audit.validation_config.tuning_days as i64),
                audit.input_end,
            ));
        write!(
            output,
            "{},{},{},{},{},{},{},{},{:.4},{:.4}",
            candidate.signal_type,
            candidate.rule_id,
            candidate.selected,
            tuning_start,
            tuning_end,
            validation_start,
            validation_end,
            candidate.train_count,
            candidate.train_hit_rate,
            candidate.train_random_hit_rate,
        )
        .expect("writing to String cannot fail");
        if let Some(result) = result {
            writeln!(
                output,
                ",{},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{}",
                result.validation_count,
                result.validation_hit_rate,
                result.random_hit_rate,
                result.lift,
                result.ci_low,
                result.ci_high,
                result.retention,
                result.passed,
            )
            .expect("writing to String cannot fail");
        } else {
            output.push_str(",,,,,,,,\n");
        }
    }
    output
}

pub fn render_alert(event: &AlertEvent) -> String {
    let direction = match event.signal.direction {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let mut output = format!(
        "ALERT {}  {}  {}  {}\n  why:   {}\n  rule:  {}\n",
        event.signal.meta.exchange_ts.to_rfc3339(),
        event.signal.meta.market,
        direction,
        event.signal.signal_type,
        event.signal.rationale,
        event.rule_key,
    );
    if let Some(result) = &event.validation {
        writeln!(
            output,
            "  track: validation n={}, hit {:.1}% vs {:.1}% matched-random, lift {:+.1}%,\n         corrected CI [{:+.1}%, {:+.1}%], retention {:.1}%\n         tuned {}–{}, validated {}–{}",
            result.validation_count,
            result.validation_hit_rate * 100.0,
            result.random_hit_rate * 100.0,
            result.lift * 100.0,
            result.ci_low * 100.0,
            result.ci_high * 100.0,
            result.retention * 100.0,
            result.tuning_start,
            result.tuning_end,
            result.validation_start,
            result.validation_end,
        )
        .expect("writing to String cannot fail");
    } else {
        output.push_str("  track: validation record unavailable\n");
    }
    output
}

pub fn render_markdown_report(audit: &EvaluationAudit) -> Result<String> {
    let mut output = String::from(
        "# Rustle validation report\n\n## Selected rules\n\n| Signal type | Selected rule | Tuning | Validation | Train n / hit | Validation n / hit | Matched random | Lift | Corrected CI | Retention | Pass |\n|---|---|---|---|---|---|---:|---:|---|---:|---|\n",
    );
    for result in &audit.results {
        writeln!(
            output,
            "| {} | {} | {}–{} | {}–{} | {} / {:.1}% | {} / {:.1}% | {:.1}% | {:.1}% | [{:.1}%, {:.1}%] | {:.1}% | {} |",
            result.signal_type,
            result.rule_id,
            result.tuning_start,
            result.tuning_end,
            result.validation_start,
            result.validation_end,
            result.train_count,
            result.train_hit_rate * 100.0,
            result.validation_count,
            result.validation_hit_rate * 100.0,
            result.random_hit_rate * 100.0,
            result.lift * 100.0,
            result.ci_low * 100.0,
            result.ci_high * 100.0,
            result.retention * 100.0,
            if result.passed { "yes" } else { "no" },
        )
        .expect("writing to String cannot fail");
    }
    writeln!(
        output,
        "\nFamily size: {}; effective tail alpha: {:.6} (family-wise correction: {}).",
        audit.family_size,
        audit.effective_tail_alpha,
        if audit.validation_config.family_wise_correction {
            "enabled"
        } else {
            "disabled"
        },
    )
    .expect("writing to String cannot fail");
    output.push_str(
        "\n## All tuning candidates\n\n| Signal type | Rule | Train n | Train hit | Matched random | Selected |\n|---|---|---:|---:|---:|---|\n",
    );
    for candidate in &audit.candidates {
        writeln!(
            output,
            "| {} | {} | {} | {:.1}% | {:.1}% | {} |",
            candidate.signal_type,
            candidate.rule_id,
            candidate.train_count,
            candidate.train_hit_rate * 100.0,
            candidate.train_random_hit_rate * 100.0,
            if candidate.selected { "yes" } else { "no" },
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("\n```json\n");
    output.push_str(&serde_json::to_string_pretty(audit)?);
    output.push_str("\n```\n\n");

    let passed: Vec<_> = audit
        .results
        .iter()
        .filter(|result| result.passed)
        .map(|result| result.rule_id.as_str())
        .collect();
    if !passed.is_empty() {
        writeln!(output, "GATE: PASS — {}", passed.join(", "))
            .expect("writing to String cannot fail");
    } else if audit.results.is_empty() {
        output.push_str("GATE: FAIL — no selected rules\n");
    } else {
        let reasons: Vec<_> = audit
            .results
            .iter()
            .map(|result| {
                let mut failures = Vec::new();
                if result.validation_count < audit.validation_config.min_validation_signals {
                    failures.push(format!(
                        "insufficient validation sample ({}/{})",
                        result.validation_count, audit.validation_config.min_validation_signals
                    ));
                }
                if result.ci_low <= 0.0 {
                    failures.push("CI lower bound is non-positive".to_string());
                }
                if result.retention < 0.80 {
                    failures.push(format!(
                        "retention below 80% ({:.1}%)",
                        result.retention * 100.0
                    ));
                }
                format!("{}: {}", result.rule_id, failures.join(", "))
            })
            .collect();
        writeln!(output, "GATE: FAIL — {}", reasons.join("; "))
            .expect("writing to String cannot fail");
    }
    Ok(output)
}

pub fn paper(
    signals: &[Signal],
    trades: &[Trade],
    passed: &[String],
    cfg: &Config,
) -> Vec<crate::model::PaperTrade> {
    let index = TradeIndex::new(trades);
    signals
        .iter()
        .filter(|s| passed.contains(&format!("{}:{}", s.signal_type, s.rule_id)))
        .filter_map(|s| {
            let e = index.at_or_after(&s.meta.market, s.meta.exchange_ts)?;
            if e.meta.exchange_ts
                > s.meta.exchange_ts + Duration::seconds(cfg.validation.entry_max_lag_seconds)
            {
                return None;
            }
            let x =
                index.at_or_after(&s.meta.market, s.meta.exchange_ts + Duration::minutes(15))?;
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
                long_only_benchmark_pnl_pct: (x.price / e.price - 1.0) * 100.0,
            })
        })
        .collect()
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::model::{ConnectionEvent, Level, SCHEMA_VERSION};
    use chrono::TimeZone;

    fn meta(day: u32, market: &str) -> Meta {
        let timestamp = Utc.with_ymd_and_hms(2025, 1, day, 12, 0, 0).unwrap();
        Meta {
            schema_version: SCHEMA_VERSION,
            market: market.into(),
            exchange_ts: timestamp,
            receive_ts: timestamp,
        }
    }

    fn trade(day: u32, market: &str) -> Trade {
        Trade {
            meta: meta(day, market),
            price: 1.,
            volume: 1.,
            side: Side::Buy,
            sequential_id: None,
        }
    }

    fn book(day: u32, market: &str) -> Orderbook {
        Orderbook {
            meta: meta(day, market),
            total_ask_size: 1.,
            total_bid_size: 1.,
            levels: Vec::<Level>::new(),
        }
    }

    fn event(day: u32, state: &str, gap_ms: Option<i64>) -> ConnectionEvent {
        ConnectionEvent {
            meta: meta(day, "ALL"),
            state: state.into(),
            detail: "test".into(),
            gap_ms,
        }
    }

    #[test]
    fn coverage_aggregates_daily_counts_markets_and_connected_gaps() {
        let trades = vec![trade(1, "KRW-A"), trade(1, "KRW-B"), trade(3, "KRW-A")];
        let books = vec![book(1, "KRW-A"), book(2, "KRW-C")];
        let signals = vec![Signal {
            meta: meta(2, "KRW-C"),
            signal_type: "test".into(),
            direction: Side::Buy,
            feature_value: 1.,
            baseline: 0.,
            rationale: "test".into(),
            market_snapshot: serde_json::json!({}),
            rule_id: "test".into(),
        }];
        let events = vec![
            event(1, "disconnected", Some(999)),
            event(1, "stalled", None),
            event(1, "connected", Some(100)),
            event(1, "connected", Some(250)),
            event(2, "reconnect", Some(10_000)),
        ];

        let rows = collection_coverage(&trades, &books, &signals, &events);

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].trade_count, 2);
        assert_eq!(rows[0].orderbook_count, 1);
        assert_eq!(rows[0].market_count, 2);
        assert_eq!(rows[0].disconnected_count, 1);
        assert_eq!(rows[0].stalled_count, 1);
        assert_eq!(rows[0].total_gap_ms, 350);
        assert_eq!(rows[0].longest_gap_ms, 250);
        assert_eq!(rows[1].live_signal_count, 1);
        assert_eq!(rows[1].total_gap_ms, 0);
    }

    #[test]
    fn collection_status_reports_the_trailing_window_and_missing_dates() {
        let cfg = Config {
            validation: crate::config::ValidationConfig {
                tuning_days: 1,
                validation_days: 2,
                ..Config::default().validation
            },
            ..Config::default()
        };
        let trades = vec![trade(1, "KRW-A"), trade(3, "KRW-A")];
        let status = collection_date_status(&trades, &BTreeSet::new(), &cfg).unwrap();

        assert_eq!(status.required, 3);
        assert_eq!(
            status.end,
            Some(NaiveDate::from_ymd_opt(2025, 1, 3).unwrap())
        );
        assert_eq!(status.present_count, 2);
        assert_eq!(
            status.missing_dates,
            vec![NaiveDate::from_ymd_opt(2025, 1, 2).unwrap()]
        );
        assert!(current_collection_dates(&trades, &BTreeSet::new(), &cfg)
            .unwrap_err()
            .to_string()
            .contains("2025-01-02"));
    }

    #[test]
    fn empty_data_has_no_coverage_rows_or_required_dates_present() {
        let cfg = Config::default();
        assert!(collection_coverage(&[], &[], &[], &[]).is_empty());
        let status = collection_date_status(&[], &BTreeSet::new(), &cfg).unwrap();
        assert_eq!(status.required, 28);
        assert_eq!(status.present_count, 0);
        assert!(status.required_dates.is_empty());
        assert!(status.missing_dates.is_empty());
    }
}

#[cfg(test)]
mod milestone_two_tests {
    use super::*;
    use crate::model::SCHEMA_VERSION;
    use chrono::TimeZone;

    fn trade(day: u32, minute: i64, price: f64) -> Trade {
        let timestamp =
            Utc.with_ymd_and_hms(2025, 1, day, 0, 0, 0).unwrap() + Duration::minutes(minute);
        Trade {
            meta: Meta {
                schema_version: SCHEMA_VERSION,
                market: "KRW-TEST".into(),
                exchange_ts: timestamp,
                receive_ts: timestamp,
            },
            price,
            volume: 1.0,
            side: Side::Buy,
            sequential_id: None,
        }
    }

    fn signal(at: DateTime<Utc>) -> Signal {
        Signal {
            meta: Meta {
                schema_version: SCHEMA_VERSION,
                market: "KRW-TEST".into(),
                exchange_ts: at,
                receive_ts: at,
            },
            signal_type: "synthetic".into(),
            direction: Side::Buy,
            feature_value: 1.0,
            baseline: 0.0,
            rationale: "test".into(),
            market_snapshot: serde_json::json!({}),
            rule_id: "candidate".into(),
        }
    }

    #[test]
    fn complete_prefix_matches_exhaustive_pool_across_gaps_ties_and_final_horizon() {
        let trades = vec![
            trade(1, 0, 100.0),
            trade(1, 0, 101.0),
            trade(1, 10, 102.0),
            trade(3, 0, 103.0),
            trade(3, 15, 104.0),
        ];
        let cfg = Config::default();
        let index = TradeIndex::new(&trades);
        for day in [1, 2, 3] {
            let date = NaiveDate::from_ymd_opt(2025, 1, day).unwrap();
            let on_date = index.on_date("KRW-TEST", date);
            let probe = signal(date.and_hms_opt(0, 0, 0).expect("valid midnight").and_utc());
            let exhaustive: Vec<_> = on_date
                .iter()
                .copied()
                .filter(|candidate| {
                    outcome_at(&probe, candidate.meta.exchange_ts, &index, &cfg).complete
                })
                .map(|candidate| (candidate.meta.exchange_ts, candidate.price))
                .collect();
            let prefix_len =
                index.complete_prefix_len("KRW-TEST", date, cfg.validation.horizon_minutes);
            let optimized: Vec<_> = on_date[..prefix_len]
                .iter()
                .map(|candidate| (candidate.meta.exchange_ts, candidate.price))
                .collect();
            assert_eq!(optimized, exhaustive);
        }
        assert_eq!(
            index
                .complete_prefix_len("KRW-TEST", NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(), 15,),
            1,
            "a trade exactly one horizon before the last trade remains eligible"
        );
    }

    #[test]
    fn correction_can_make_a_nominally_positive_interval_non_positive() {
        let mut cfg = Config::default();
        cfg.validation.bootstrap_iterations = 20_000;
        let mut hits = vec![true; 30];
        let mut controls = vec![false; 30];
        hits.extend(vec![false; 15]);
        controls.extend(vec![true; 15]);
        hits.extend(vec![false; 55]);
        controls.extend(vec![false; 55]);

        let nominal = confidence_interval(&hits, &controls, &cfg, "family-fixture", 0.025);
        let corrected = confidence_interval(&hits, &controls, &cfg, "family-fixture", 0.025 / 4.0);

        assert!(nominal.0 > 0.0, "nominal interval was {nominal:?}");
        assert!(corrected.0 <= 0.0, "corrected interval was {corrected:?}");
    }

    #[test]
    fn retention_zero_is_finite_and_is_a_hard_gate() {
        let mut cfg = Config::default();
        cfg.validation.min_validation_signals = 1;
        assert_eq!(retention(0.0, 1.0), 0.0);
        assert!(!passes_gate(10, 0.1, 0.79, &cfg));
        assert!(passes_gate(10, 0.1, 0.80, &cfg));
    }

    #[test]
    fn evaluation_rejects_negative_entry_lag_before_readiness_checks() {
        let mut cfg = Config::default();
        cfg.validation.entry_max_lag_seconds = -1;
        let error = evaluate_with_audit(&[], &[], &BTreeSet::new(), &cfg).unwrap_err();
        assert!(error.to_string().contains("entry_max_lag_seconds"));
    }

    #[test]
    fn evaluation_uses_complete_validation_families_for_effective_alpha() {
        let mut cfg = Config::default();
        cfg.validation.tuning_days = 1;
        cfg.validation.validation_days = 1;
        cfg.validation.bootstrap_iterations = 100;
        let trades = vec![
            trade(1, 0, 100.0),
            trade(1, 15, 101.0),
            trade(2, 0, 100.0),
            trade(2, 15, 101.0),
        ];
        let mut signals = Vec::new();
        for family in 0..4 {
            for day in [1, 2] {
                let mut candidate = signal(trade(day, 0, 100.0).meta.exchange_ts);
                candidate.signal_type = format!("family-{family}");
                candidate.rule_id = format!("rule-{family}");
                signals.push(candidate);
            }
        }

        let corrected = evaluate_with_audit(&signals, &trades, &BTreeSet::new(), &cfg).unwrap();
        assert_eq!(corrected.family_size, 4);
        assert_eq!(corrected.effective_tail_alpha, 0.025 / 4.0);

        cfg.validation.family_wise_correction = false;
        let nominal = evaluate_with_audit(&signals, &trades, &BTreeSet::new(), &cfg).unwrap();
        assert_eq!(nominal.family_size, 4);
        assert_eq!(nominal.effective_tail_alpha, 0.025);
    }

    fn audit_fixture() -> EvaluationAudit {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 28).unwrap();
        let cfg = Config::default();
        EvaluationAudit {
            input_start: start,
            input_end: end,
            collection_dates: vec![start, end],
            validation_config: cfg.validation,
            bootstrap_seed: 7,
            candidates: vec![
                CandidateSummary {
                    rule_id: "synthetic:a".into(),
                    signal_type: "synthetic".into(),
                    train_count: 60,
                    train_hit_rate: 0.75,
                    train_random_hit_rate: 0.50,
                    selected: true,
                },
                CandidateSummary {
                    rule_id: "synthetic:b".into(),
                    signal_type: "synthetic".into(),
                    train_count: 40,
                    train_hit_rate: 0.60,
                    train_random_hit_rate: 0.55,
                    selected: false,
                },
            ],
            family_size: 2,
            effective_tail_alpha: 0.0125,
            results: vec![RuleResult {
                rule_id: "synthetic:a".into(),
                signal_type: "synthetic".into(),
                train_count: 60,
                train_hit_rate: 0.75,
                validation_count: 20,
                validation_hit_rate: 0.50,
                random_hit_rate: 0.45,
                lift: 0.05,
                ci_low: 0.0,
                ci_high: 0.10,
                retention: 2.0 / 3.0,
                passed: false,
                tuning_start: start,
                tuning_end: start + Duration::days(13),
                validation_start: start + Duration::days(14),
                validation_end: end,
            }],
        }
    }

    #[test]
    fn reports_render_all_candidates_blank_unselected_fields_and_explicit_verdict() {
        let audit = audit_fixture();
        let csv = render_csv_report(&audit);
        assert_eq!(csv.lines().count(), 3);
        assert_eq!(csv.lines().next().unwrap(), "signal_type,rule_id,selected,tuning_start,tuning_end,validation_start,validation_end,train_count,train_hit_rate,train_random_hit_rate,validation_count,validation_hit_rate,random_hit_rate,lift,ci_low,ci_high,retention,passed");
        let unselected = csv.lines().nth(2).unwrap();
        assert!(unselected.starts_with("synthetic,synthetic:b,false,"));
        assert!(unselected.ends_with(",,,,,,,,"));

        let markdown = render_markdown_report(&audit).unwrap();
        assert!(markdown.contains("## All tuning candidates"));
        assert!(markdown.contains("effective tail alpha: 0.012500"));
        assert!(markdown.contains("retention below 80%"));
        assert!(markdown
            .trim_end()
            .ends_with("CI lower bound is non-positive, retention below 80% (66.7%)"));
        assert!(markdown.contains("GATE: FAIL"));

        let mut passing = audit;
        passing.results[0].passed = true;
        let markdown = render_markdown_report(&passing).unwrap();
        assert_eq!(markdown.lines().last(), Some("GATE: PASS — synthetic:a"));
    }

    #[test]
    fn alert_renderer_includes_reason_and_validation_track_record() {
        let mut audit = audit_fixture();
        audit.results[0].passed = true;
        let at = Utc.with_ymd_and_hms(2025, 1, 29, 12, 0, 0).unwrap();
        let event = AlertEvent {
            emitted_at: at,
            rule_key: "synthetic:a".into(),
            signal: signal(at),
            validation: Some(audit.results[0].clone()),
        };

        let rendered = render_alert(&event);
        assert!(rendered.contains("ALERT 2025-01-29T12:00:00+00:00  KRW-TEST  BUY  synthetic"));
        assert!(rendered.contains("why:   test"));
        assert!(rendered.contains("validation n=20, hit 50.0% vs 45.0% matched-random"));
        assert!(rendered.contains("corrected CI [+0.0%, +10.0%]"));
        assert!(rendered.contains("tuned 2025-01-01–2025-01-14"));

        let json = serde_json::to_string(&event).unwrap();
        let round_trip: AlertEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(round_trip.validation.unwrap().rule_id, "synthetic:a");
    }

    #[test]
    fn alert_renderer_plainly_marks_missing_validation() {
        let at = Utc.with_ymd_and_hms(2025, 1, 29, 12, 0, 0).unwrap();
        let event = AlertEvent {
            emitted_at: at,
            rule_key: "synthetic:candidate".into(),
            signal: signal(at),
            validation: None,
        };
        assert!(render_alert(&event).contains("validation record unavailable"));
    }

    #[test]
    fn alert_config_does_not_change_validation_fingerprint() {
        let cfg = Config::default();
        let original = config_fingerprint(&cfg);
        let mut changed_alert = cfg.clone();
        changed_alert.alert.cooldown_seconds = 0;
        assert_eq!(config_fingerprint(&changed_alert), original);

        let mut changed_validation = cfg;
        changed_validation.validation.horizon_minutes += 1;
        assert_ne!(config_fingerprint(&changed_validation), original);
    }

    #[test]
    #[should_panic(expected = "signal rationale must not be empty")]
    fn signal_constructor_rejects_empty_rationale() {
        let at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let meta = trade(1, 0, 1.0).meta;
        super::signal(
            meta,
            "synthetic",
            Side::Buy,
            1.0,
            0.0,
            "  ".into(),
            "candidate".into(),
            serde_json::json!({"at": at}),
        );
    }

    #[test]
    #[should_panic(expected = "signal evidence must not be null")]
    fn signal_constructor_rejects_null_evidence() {
        let meta = trade(1, 0, 1.0).meta;
        super::signal(
            meta,
            "synthetic",
            Side::Buy,
            1.0,
            0.0,
            "test".into(),
            "candidate".into(),
            serde_json::Value::Null,
        );
    }

    #[test]
    fn old_audit_json_uses_defaults_for_new_fields() {
        let mut value = serde_json::to_value(audit_fixture()).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("candidates");
        object.remove("family_size");
        object.remove("effective_tail_alpha");
        object.get_mut("results").unwrap().as_array_mut().unwrap()[0]
            .as_object_mut()
            .unwrap()
            .remove("retention");

        let old: EvaluationAudit = serde_json::from_value(value).unwrap();
        assert!(old.candidates.is_empty());
        assert_eq!(old.family_size, 0);
        assert_eq!(old.effective_tail_alpha, 0.0);
        assert_eq!(old.results[0].retention, 0.0);
    }
}

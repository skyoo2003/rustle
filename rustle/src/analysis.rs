use crate::{
    config::Config,
    model::{ConnectionEvent, Meta, Orderbook, Side, Signal, SignalOutcome, Trade},
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
/// A deterministic fingerprint of every configuration value that can affect collection,
/// detection, replay, or validation.
pub fn config_fingerprint(cfg: &Config) -> String {
    let bytes = serde_json::to_vec(cfg).expect("config serializes");
    let hash = bytes.into_iter().fold(0xcbf29ce484222325u64, |h, byte| {
        (h ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("fnv1a64:{hash:016x}")
}

pub fn current_collection_dates(
    trades: &[Trade],
    books: &[Orderbook],
    cfg: &Config,
) -> Result<Vec<NaiveDate>> {
    let status = collection_date_status(trades, books, cfg)?;
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
    books: &[Orderbook],
    cfg: &Config,
) -> Result<CollectionDateStatus> {
    let required = cfg.validation.tuning_days + cfg.validation.validation_days;
    if cfg.validation.tuning_days == 0 || cfg.validation.validation_days == 0 {
        bail!("tuning_days and validation_days must both be greater than zero");
    }
    let dates: BTreeSet<NaiveDate> = trades
        .iter()
        .map(|t| t.meta.exchange_ts.date_naive())
        .chain(books.iter().map(|b| b.meta.exchange_ts.date_naive()))
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
                        "aggressive notional {:.0} is {:.1}x rolling mean",
                        value,
                        value / baseline
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
                    format!("{} trades in rolling window", prior),
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
                    format!("bid/ask size imbalance {:.3}", value),
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
                    format!("previous {:.0} KRW {:?} wall disappeared", old, oldside),
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
pub fn build_signals(
    mut trades: Vec<Trade>,
    mut books: Vec<Orderbook>,
    cfg: &Config,
) -> Vec<Signal> {
    trades.sort_by(trade_cmp);
    books.sort_by(book_cmp);
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
        let pool: Vec<&Trade> = index
            .on_date(&signal.meta.market, signal.meta.exchange_ts.date_naive())
            .iter()
            .copied()
            .filter(|t| outcome_at(signal, t.meta.exchange_ts, index, cfg).complete)
            .collect();
        if let Some(t) = (!pool.is_empty()).then(|| pool[rng.random_range(0..pool.len())]) {
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
    if cfg.validation.bootstrap_iterations == 0 {
        bail!("bootstrap_iterations must be greater than zero");
    }
    let expected = current_collection_dates(trades, books, cfg)?;
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
    let mut selected: BTreeMap<String, SelectedCandidate<'_>> = BTreeMap::new();
    for (id, candidate) in candidates {
        let train: Vec<_> = candidate
            .iter()
            .copied()
            .filter(|s| s.meta.exchange_ts.date_naive() <= tuning_end)
            .collect();
        let index = TradeIndex::new(trades);
        let (hits, controls) = paired_outcomes(&train, &index, cfg, &format!("train:{id}"));
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
                &TradeIndex::new(trades),
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
        let status = collection_date_status(&trades, &[], &cfg).unwrap();

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
        assert!(current_collection_dates(&trades, &[], &cfg)
            .unwrap_err()
            .to_string()
            .contains("2025-01-02"));
    }

    #[test]
    fn empty_data_has_no_coverage_rows_or_required_dates_present() {
        let cfg = Config::default();
        assert!(collection_coverage(&[], &[], &[], &[]).is_empty());
        let status = collection_date_status(&[], &[], &cfg).unwrap();
        assert_eq!(status.required, 28);
        assert_eq!(status.present_count, 0);
        assert!(status.required_dates.is_empty());
        assert!(status.missing_dates.is_empty());
    }
}

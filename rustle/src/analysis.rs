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

/// Seconds that actually hold data, counted as distinct one-second buckets.
///
/// Not first-to-last span: two short sittings a day apart are three minutes of collection,
/// not twenty-two hours, and a 28-day run accumulates gaps from disconnects and restarts.
/// Counting buckets ignores idle time, and where a market is quiet it undercounts — which
/// pushes the projection up rather than down, the safe direction for "do I have room".
pub fn observed_collection_seconds(trades: &[Trade], books: &[Orderbook]) -> i64 {
    let seconds: BTreeSet<i64> = trades
        .iter()
        .map(|t| t.meta.exchange_ts.timestamp())
        .chain(books.iter().map(|b| b.meta.exchange_ts.timestamp()))
        .collect();
    seconds.len() as i64
}

/// What the collected dates so far imply for the full gate window.
///
/// The 2026-08-26 sample was 26 MB and 1,810 files for 116 seconds of one date. Nothing in
/// `coverage` said where that was heading, so it stayed invisible until someone multiplied
/// it out by hand. This line does the multiplying.
pub fn render_footprint_projection(
    bytes: u64,
    files: usize,
    observed_seconds: i64,
    required_dates: usize,
) -> String {
    if observed_seconds <= 0 {
        return format!(
            "Not enough collected data to project a footprint; \
             {required_dates} complete UTC dates required before analyze can run.\n"
        );
    }
    // Scale by elapsed collection time, never by date count. A date holding 90 seconds of
    // data is still one present date, and scaling by dates would report a 542 GB trajectory
    // as 0.7 GB — the precise blindness that let this go unnoticed until day 28.
    let scale = (required_dates as f64 * 86_400.0) / observed_seconds as f64;
    // Decimal GB, matching what `df -H` and the disk vendor report — this number exists to
    // answer "do I have room", not to match `du -h`.
    let gb = bytes as f64 * scale / 1_000_000_000.0;
    let projected_files = (files as f64 * scale).round() as u64;
    format!(
        "Footprint: {:.1} GB in {} files over {} of collection \
         → {:.1} GB and {} files projected for {} dates.\n",
        bytes as f64 / 1_000_000_000.0,
        thousands(files as u64),
        humanize_seconds(observed_seconds),
        gb,
        thousands(projected_files),
        required_dates,
    )
}

fn humanize_seconds(seconds: i64) -> String {
    match seconds {
        s if s < 120 => format!("{s}s"),
        s if s < 7_200 => format!("{}m", s / 60),
        s if s < 172_800 => format!("{:.1}h", s as f64 / 3_600.0),
        s => format!("{:.1}d", s as f64 / 86_400.0),
    }
}

fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
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
    feed_signals(&mut SignalDetector::new(cfg), trades, books)
}

/// Drive one chunk of already-collected data through a detector the caller owns.
///
/// `analyze` reads the archive one UTC partition at a time, so the detector must outlive
/// the chunk: rolling windows and the `active_rules` edge-tracking that decides whether a
/// signal fires at all do not reset at midnight during live collection, and must not reset
/// here either. Passing a fresh detector per chunk re-arms every active rule on every
/// market at every boundary.
pub fn feed_signals(
    detector: &mut SignalDetector,
    trades: &[Trade],
    books: &[Orderbook],
) -> Vec<Signal> {
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
    /// Every market that traded inside the inclusive UTC date window, with its first and
    /// last in-window price.  Sorted, so both the equal weighting and the report are stable.
    fn window_universe(&self, start: NaiveDate, end: NaiveDate) -> Vec<(&'a str, f64, f64)> {
        let from = start.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
        let to = (end + Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .expect("midnight")
            .and_utc();
        let mut universe: Vec<_> = self
            .by_market
            .iter()
            .filter_map(|(market, values)| {
                let first = values.partition_point(|trade| trade.meta.exchange_ts < from);
                let last = values.partition_point(|trade| trade.meta.exchange_ts < to);
                let in_window = values.get(first..last)?;
                Some((*market, in_window.first()?.price, in_window.last()?.price))
            })
            .collect();
        universe.sort_by_key(|(market, ..)| *market);
        universe
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

#[derive(Debug, Clone)]
pub struct PaperRuleRow {
    pub rule_key: String,
    pub trade_count: usize,
    pub skipped_overlapping: usize,
    pub incomplete_horizon: usize,
    pub win_rate: f64,
    /// Compounded return of a sleeve that took only this rule's trades.
    pub net_pnl_pct: f64,
    pub mean_pnl_pct: f64,
}

#[derive(Debug, Clone)]
pub struct PaperMarketRow {
    pub market: String,
    pub trade_count: usize,
    pub skipped_overlapping: usize,
    pub incomplete_horizon: usize,
    pub win_rate: f64,
    /// Compounded return of this market's equally weighted sleeve.
    pub net_pnl_pct: f64,
    pub mean_pnl_pct: f64,
    pub hodl_pnl_pct: f64,
}

/// Everything `paper` produced, ready to persist or render.  The window and costs travel
/// with it so a renderer never has to reach back into the config to label its own output.
#[derive(Debug, Clone)]
pub struct PaperReport {
    pub summary: crate::model::PaperSummary,
    pub trades: Vec<crate::model::PaperTrade>,
    pub rules: Vec<PaperRuleRow>,
    pub markets: Vec<PaperMarketRow>,
    pub window: (NaiveDate, NaiveDate),
    pub horizon_minutes: i64,
    pub fee_bps: f64,
    pub slippage_bps: f64,
}

/// Running counts for one slice of the study (a rule, a market, or the whole thing).
#[derive(Debug, Clone)]
struct Tally {
    trade_count: usize,
    skipped_overlapping: usize,
    incomplete_horizon: usize,
    wins: usize,
    net_sum: f64,
    /// Compounding multiplier, so two 10% winners make 21%, not 20%.
    sleeve: f64,
}

impl Tally {
    fn new() -> Self {
        Self {
            trade_count: 0,
            skipped_overlapping: 0,
            incomplete_horizon: 0,
            wins: 0,
            net_sum: 0.0,
            sleeve: 1.0,
        }
    }

    fn fill(&mut self, net_pnl_pct: f64) {
        self.trade_count += 1;
        self.wins += usize::from(net_pnl_pct > 0.0);
        self.net_sum += net_pnl_pct;
        self.sleeve *= 1.0 + net_pnl_pct / 100.0;
    }

    fn win_rate(&self) -> f64 {
        self.wins as f64 / self.trade_count.max(1) as f64
    }

    fn net_pnl_pct(&self) -> f64 {
        (self.sleeve - 1.0) * 100.0
    }

    fn mean_pnl_pct(&self) -> f64 {
        self.net_sum / self.trade_count.max(1) as f64
    }
}

/// Every count lands in three places: its rule, its market, and the study total. Markets
/// outside the universe have no tally, so their signals still reach the rule and the total.
fn bump(
    key: &str,
    market: &str,
    per_rule: &mut BTreeMap<String, Tally>,
    per_market: &mut BTreeMap<String, Tally>,
    total: &mut Tally,
    apply: impl Fn(&mut Tally),
) {
    apply(per_rule.get_mut(key).expect("rule tally is inserted first"));
    if let Some(tally) = per_market.get_mut(market) {
        apply(tally);
    }
    apply(total);
}

/// Signals are replayed in a total order so the simulation is reproducible whatever
/// order detection produced them in.
fn signal_cmp(a: &Signal, b: &Signal) -> Ordering {
    a.meta
        .exchange_ts
        .cmp(&b.meta.exchange_ts)
        .then_with(|| a.meta.market.cmp(&b.meta.market))
        .then_with(|| a.signal_type.cmp(&b.signal_type))
        .then_with(|| a.rule_id.cmp(&b.rule_id))
}

/// Replay the qualified rules over the validation window as one equally weighted account.
///
/// Capital is split evenly across every market with a trade in the window; each sleeve
/// compounds independently and holds at most one position at a time.  The result is a
/// return an actual account could have produced, which is what makes the hold benchmark
/// on the same universe a comparison rather than two unrelated numbers.
pub fn paper(
    signals: &[Signal],
    trades: &[Trade],
    passed: &[String],
    window: (NaiveDate, NaiveDate),
    generated_at: DateTime<Utc>,
    cfg: &Config,
) -> PaperReport {
    let (window_start, window_end) = window;
    let index = TradeIndex::new(trades);
    let qualified: std::collections::HashSet<&String> = passed.iter().collect();
    let round_trip_pct = 2.0 * (cfg.paper.fee_bps + cfg.paper.slippage_bps) / 100.0;

    // The universe is every market that traded inside the window.  The strategy and the
    // hold benchmark are weighted over this same set, so their difference means something.
    let universe = index.window_universe(window_start, window_end);
    let market_count = universe.len();
    let mut per_market: BTreeMap<String, Tally> = universe
        .iter()
        .map(|(market, ..)| ((*market).to_owned(), Tally::new()))
        .collect();
    let mut per_rule: BTreeMap<String, Tally> = BTreeMap::new();
    let mut total = Tally::new();

    let mut ordered: Vec<&Signal> = signals
        .iter()
        .filter(|signal| qualified.contains(&rule_key(signal)))
        .filter(|signal| {
            let date = signal.meta.exchange_ts.date_naive();
            date >= window_start && date <= window_end
        })
        .collect();
    ordered.sort_by(|a, b| signal_cmp(a, b));

    let mut busy_until: HashMap<String, DateTime<Utc>> = HashMap::new();
    let mut closes: Vec<(DateTime<Utc>, String, f64)> = Vec::new();
    let mut paper_trades = Vec::new();

    for signal in ordered {
        let market = signal.meta.market.clone();
        let key = rule_key(signal);
        per_rule.entry(key.clone()).or_insert_with(Tally::new);
        let at = signal.meta.exchange_ts;

        if busy_until.get(&market).is_some_and(|until| at < *until) {
            bump(
                &key,
                &market,
                &mut per_rule,
                &mut per_market,
                &mut total,
                |tally| tally.skipped_overlapping += 1,
            );
            continue;
        }

        // A market outside the universe has neither a sleeve to trade nor a hold benchmark
        // to be judged against, so its signals are reported rather than silently dropped.
        let filled = per_market
            .contains_key(&market)
            .then(|| {
                let entry = index.at_or_after(&market, at).filter(|trade| {
                    trade.meta.exchange_ts
                        <= at + Duration::seconds(cfg.validation.entry_max_lag_seconds)
                })?;
                let exit = index.at_or_after(
                    &market,
                    at + Duration::minutes(cfg.validation.horizon_minutes),
                )?;
                Some((entry, exit))
            })
            .flatten();

        let Some((entry, exit)) = filled else {
            bump(
                &key,
                &market,
                &mut per_rule,
                &mut per_market,
                &mut total,
                |tally| tally.incomplete_horizon += 1,
            );
            continue;
        };

        let gross = match signal.direction {
            Side::Buy => (exit.price / entry.price - 1.0) * 100.0,
            Side::Sell => (entry.price / exit.price - 1.0) * 100.0,
        };
        let net = gross - round_trip_pct;
        bump(
            &key,
            &market,
            &mut per_rule,
            &mut per_market,
            &mut total,
            |tally| tally.fill(net),
        );
        busy_until.insert(market.clone(), exit.meta.exchange_ts);
        closes.push((
            exit.meta.exchange_ts,
            market.clone(),
            per_market[&market].sleeve,
        ));
        paper_trades.push(crate::model::PaperTrade {
            signal: signal.clone(),
            entry_price: entry.price,
            exit_price: exit.price,
            gross_pnl_pct: gross,
            net_pnl_pct: net,
        });
    }

    let markets: Vec<PaperMarketRow> = universe
        .iter()
        .map(|(market, first, last)| {
            let tally = &per_market[*market];
            PaperMarketRow {
                market: (*market).to_owned(),
                trade_count: tally.trade_count,
                skipped_overlapping: tally.skipped_overlapping,
                incomplete_horizon: tally.incomplete_horizon,
                win_rate: tally.win_rate(),
                net_pnl_pct: tally.net_pnl_pct(),
                mean_pnl_pct: tally.mean_pnl_pct(),
                hodl_pnl_pct: (last / first - 1.0) * 100.0 - round_trip_pct,
            }
        })
        .collect();
    let rules: Vec<PaperRuleRow> = per_rule
        .iter()
        .map(|(rule_key, tally)| PaperRuleRow {
            rule_key: rule_key.clone(),
            trade_count: tally.trade_count,
            skipped_overlapping: tally.skipped_overlapping,
            incomplete_horizon: tally.incomplete_horizon,
            win_rate: tally.win_rate(),
            net_pnl_pct: tally.net_pnl_pct(),
            mean_pnl_pct: tally.mean_pnl_pct(),
        })
        .collect();
    // An empty universe sums to zero over a denominator of one, which is the honest answer:
    // nothing was traded and nothing was held.
    let equal_weight = |value: fn(&PaperMarketRow) -> f64| {
        markets.iter().map(value).sum::<f64>() / market_count.max(1) as f64
    };
    let cumulative_net_pnl_pct = equal_weight(|row| row.net_pnl_pct);
    let hodl_pnl_pct = equal_weight(|row| row.hodl_pnl_pct);

    PaperReport {
        summary: crate::model::PaperSummary {
            generated_at,
            trade_count: total.trade_count,
            cumulative_net_pnl_pct,
            win_rate: total.win_rate(),
            window_start: Some(window_start),
            window_end: Some(window_end),
            market_count,
            skipped_overlapping: total.skipped_overlapping,
            incomplete_horizon: total.incomplete_horizon,
            max_drawdown_pct: max_drawdown_pct(&closes, &universe),
            hodl_pnl_pct,
            excess_pnl_pct: cumulative_net_pnl_pct - hodl_pnl_pct,
        },
        trades: paper_trades,
        rules,
        markets,
        window,
        horizon_minutes: cfg.validation.horizon_minutes,
        fee_bps: cfg.paper.fee_bps,
        slippage_bps: cfg.paper.slippage_bps,
    }
}

/// Deepest peak-to-trough move of the equally weighted equity curve, in percent and
/// negative.  The curve is marked at position closes only: open positions are not
/// marked to market, so this is a floor on the real drawdown, never an overstatement.
fn max_drawdown_pct(closes: &[(DateTime<Utc>, String, f64)], universe: &[(&str, f64, f64)]) -> f64 {
    if universe.is_empty() {
        return 0.0;
    }
    let mut sleeves: BTreeMap<&str, f64> = universe.iter().map(|(m, ..)| (*m, 1.0)).collect();
    let mut ordered: Vec<&(DateTime<Utc>, String, f64)> = closes.iter().collect();
    ordered.sort_by_key(|(at, ..)| *at);
    let mut peak = 1.0_f64;
    let mut worst = 0.0_f64;
    for (_, market, sleeve) in ordered {
        if let Some(slot) = sleeves.get_mut(market.as_str()) {
            *slot = *sleeve;
        }
        let value = sleeves.values().sum::<f64>() / universe.len() as f64;
        peak = peak.max(value);
        worst = worst.min((value - peak) / peak * 100.0);
    }
    worst
}

/// The universe refreshes daily to the top markets by volume, so a market that fell out
/// is absent from later dates.  That biases the hold benchmark upward, which makes the
/// verdict conservative in the strategy's favour — and belongs in the report, not a footnote
/// in a document nobody opens.
const SURVIVORSHIP_CAVEAT: &str = "The market universe refreshes daily to the top markets by volume, so markets that fell\nout of it are absent from later dates. That survivorship bias favours hold: beating this\nbenchmark is a stronger result than the raw gap suggests, and losing to it a weaker one.";

/// The milestone's single number, stated so the reader does not have to do the subtraction.
fn paper_verdict(report: &PaperReport) -> String {
    let (start, end) = report.window;
    let excess = report.summary.excess_pnl_pct;
    match excess.partial_cmp(&0.0) {
        Some(Ordering::Greater) => {
            format!("VERDICT: STRATEGY WINS by {excess:.3}pp over {start}–{end}")
        }
        Some(Ordering::Less) => {
            format!("VERDICT: HOLD WINS by {:.3}pp over {start}–{end}", -excess)
        }
        _ => format!("VERDICT: TIE at 0.000pp over {start}–{end}"),
    }
}

/// Trade-weighted mean across rules, which is total net divided by total trades without
/// carrying a second copy of either.
fn mean_across_rules(report: &PaperReport) -> f64 {
    let trades: usize = report.rules.iter().map(|rule| rule.trade_count).sum();
    let total: f64 = report
        .rules
        .iter()
        .map(|rule| rule.mean_pnl_pct * rule.trade_count as f64)
        .sum();
    total / trades.max(1) as f64
}

pub fn render_paper_markdown(report: &PaperReport) -> Result<String> {
    let summary = &report.summary;
    let (start, end) = report.window;
    let markets = format!(
        "{} market{}",
        summary.market_count,
        if summary.market_count == 1 { "" } else { "s" }
    );
    let mut output = String::from("# Rustle paper study\n\n");
    writeln!(
        output,
        "Window: {}–{} (validation only) · {} · horizon {}m · fees {}bps, slippage {}bps\n",
        start, end, markets, report.horizon_minutes, report.fee_bps, report.slippage_bps,
    )
    .expect("writing to String cannot fail");

    output.push_str("| Rule | Trades | Skipped (overlap) | Incomplete | Win rate | Net P&L | Mean / trade |\n|---|---:|---:|---:|---:|---:|---:|\n");
    for rule in &report.rules {
        writeln!(
            output,
            "| {} | {} | {} | {} | {:.1}% | {:+.3}% | {:+.3}% |",
            rule.rule_key,
            rule.trade_count,
            rule.skipped_overlapping,
            rule.incomplete_horizon,
            rule.win_rate * 100.0,
            rule.net_pnl_pct,
            rule.mean_pnl_pct,
        )
        .expect("writing to String cannot fail");
    }

    output.push_str("\n| Market | Trades | Net P&L | Hold P&L |\n|---|---:|---:|---:|\n");
    for market in &report.markets {
        writeln!(
            output,
            "| {} | {} | {:+.3}% | {:+.3}% |",
            market.market, market.trade_count, market.net_pnl_pct, market.hodl_pnl_pct,
        )
        .expect("writing to String cannot fail");
    }

    if summary.trade_count == 0 {
        output.push_str("\nNo qualified signal produced a paper trade in this window.\n");
    }
    writeln!(
        output,
        "\nStrategy: {:+.3}% net, max drawdown {:.3}%, {} trades, {} skipped for overlap, {} incomplete.",
        summary.cumulative_net_pnl_pct,
        summary.max_drawdown_pct,
        summary.trade_count,
        summary.skipped_overlapping,
        summary.incomplete_horizon,
    )
    .expect("writing to String cannot fail");
    writeln!(
        output,
        "Hold:     {:+.3}% (equal weight over the same {}, one round trip).\n",
        summary.hodl_pnl_pct, markets,
    )
    .expect("writing to String cannot fail");
    output.push_str(SURVIVORSHIP_CAVEAT);
    writeln!(output, "\n\n{}", paper_verdict(report)).expect("writing to String cannot fail");
    Ok(output)
}

pub fn render_paper_csv(report: &PaperReport) -> String {
    let summary = &report.summary;
    let mut output = String::from(
        "section,key,trades,skipped_overlapping,incomplete_horizon,win_rate,net_pnl_pct,mean_pnl_pct,max_drawdown_pct,hodl_pnl_pct\n",
    );
    for rule in &report.rules {
        // A rule has no hold benchmark: you cannot hold a rule, only the markets it traded.
        writeln!(
            output,
            "rule,{},{},{},{},{:.4},{:.4},{:.4},,",
            rule.rule_key,
            rule.trade_count,
            rule.skipped_overlapping,
            rule.incomplete_horizon,
            rule.win_rate,
            rule.net_pnl_pct,
            rule.mean_pnl_pct,
        )
        .expect("writing to String cannot fail");
    }
    for market in &report.markets {
        writeln!(
            output,
            "market,{},{},{},{},{:.4},{:.4},{:.4},,{:.4}",
            market.market,
            market.trade_count,
            market.skipped_overlapping,
            market.incomplete_horizon,
            market.win_rate,
            market.net_pnl_pct,
            market.mean_pnl_pct,
            market.hodl_pnl_pct,
        )
        .expect("writing to String cannot fail");
    }
    writeln!(
        output,
        "total,ALL,{},{},{},{:.4},{:.4},{:.4},{:.4},{:.4}",
        summary.trade_count,
        summary.skipped_overlapping,
        summary.incomplete_horizon,
        summary.win_rate,
        summary.cumulative_net_pnl_pct,
        mean_across_rules(report),
        summary.max_drawdown_pct,
        summary.hodl_pnl_pct,
    )
    .expect("writing to String cannot fail");
    // The verdict is the deliverable, so it trails the rows here exactly as it ends the
    // Markdown report. Consumers already have to read the `section` column.
    writeln!(output, "{}", paper_verdict(report)).expect("writing to String cannot fail");
    output
}

#[cfg(test)]
mod milestone_four_tests {
    use super::*;
    use chrono::TimeZone;

    fn paper_fixture() -> PaperReport {
        let start = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 28).unwrap();
        PaperReport {
            summary: crate::model::PaperSummary {
                generated_at: Utc.with_ymd_and_hms(2025, 1, 29, 0, 0, 0).unwrap(),
                trade_count: 3,
                cumulative_net_pnl_pct: 0.75,
                win_rate: 2.0 / 3.0,
                window_start: Some(start),
                window_end: Some(end),
                market_count: 2,
                skipped_overlapping: 1,
                incomplete_horizon: 2,
                max_drawdown_pct: -1.2,
                hodl_pnl_pct: 2.0,
                excess_pnl_pct: -1.25,
            },
            trades: vec![],
            rules: vec![PaperRuleRow {
                rule_key: "synthetic:a".into(),
                trade_count: 3,
                skipped_overlapping: 1,
                incomplete_horizon: 2,
                win_rate: 2.0 / 3.0,
                net_pnl_pct: 1.5,
                mean_pnl_pct: 0.5,
            }],
            markets: vec![
                PaperMarketRow {
                    market: "KRW-A".into(),
                    trade_count: 2,
                    skipped_overlapping: 0,
                    incomplete_horizon: 1,
                    win_rate: 1.0,
                    net_pnl_pct: 1.0,
                    mean_pnl_pct: 0.5,
                    hodl_pnl_pct: 3.0,
                },
                PaperMarketRow {
                    market: "KRW-B".into(),
                    trade_count: 1,
                    skipped_overlapping: 1,
                    incomplete_horizon: 1,
                    win_rate: 0.0,
                    net_pnl_pct: 0.5,
                    mean_pnl_pct: 0.5,
                    hodl_pnl_pct: 1.0,
                },
            ],
            window: (start, end),
            horizon_minutes: 15,
            fee_bps: 5.0,
            slippage_bps: 3.0,
        }
    }

    #[test]
    fn paper_report_renders_both_tables_and_ends_in_an_explicit_verdict() {
        let report = paper_fixture();

        let markdown = render_paper_markdown(&report).unwrap();
        assert!(markdown.contains(
            "Window: 2025-01-15–2025-01-28 (validation only) · 2 markets · horizon 15m · fees 5bps, slippage 3bps"
        ));
        assert!(markdown.contains("| synthetic:a | 3 | 1 | 2 | 66.7% | +1.500% | +0.500% |"));
        assert!(markdown.contains("| KRW-A | 2 | +1.000% | +3.000% |"));
        assert!(markdown.contains(
            "Strategy: +0.750% net, max drawdown -1.200%, 3 trades, 1 skipped for overlap, 2 incomplete."
        ));
        assert!(markdown.contains("Hold:     +2.000%"));
        assert!(
            markdown.contains("survivorship"),
            "the hold benchmark's bias belongs in the report, not only in the ADR"
        );
        assert_eq!(
            markdown.lines().last(),
            Some("VERDICT: HOLD WINS by 1.250pp over 2025-01-15–2025-01-28")
        );

        let csv = render_paper_csv(&report);
        let mut lines = csv.lines();
        assert_eq!(
            lines.next(),
            Some("section,key,trades,skipped_overlapping,incomplete_horizon,win_rate,net_pnl_pct,mean_pnl_pct,max_drawdown_pct,hodl_pnl_pct")
        );
        assert_eq!(
            lines.next(),
            Some("rule,synthetic:a,3,1,2,0.6667,1.5000,0.5000,,")
        );
        assert_eq!(
            lines.next(),
            Some("market,KRW-A,2,0,1,1.0000,1.0000,0.5000,,3.0000")
        );
        assert_eq!(
            lines.next(),
            Some("market,KRW-B,1,1,1,0.0000,0.5000,0.5000,,1.0000")
        );
        assert_eq!(
            lines.next(),
            Some("total,ALL,3,1,2,0.6667,0.7500,0.5000,-1.2000,2.0000")
        );
        assert_eq!(
            csv.lines().last(),
            Some("VERDICT: HOLD WINS by 1.250pp over 2025-01-15–2025-01-28")
        );
    }

    #[test]
    fn the_verdict_names_the_winner_in_both_directions_and_calls_a_tie_a_tie() {
        let mut report = paper_fixture();
        report.summary.cumulative_net_pnl_pct = 3.5;
        report.summary.excess_pnl_pct = 1.5;

        let winner = Some("VERDICT: STRATEGY WINS by 1.500pp over 2025-01-15–2025-01-28");
        assert_eq!(
            render_paper_markdown(&report).unwrap().lines().last(),
            winner
        );
        assert_eq!(render_paper_csv(&report).lines().last(), winner);

        report.summary.excess_pnl_pct = 0.0;
        assert_eq!(
            render_paper_markdown(&report).unwrap().lines().last(),
            Some("VERDICT: TIE at 0.000pp over 2025-01-15–2025-01-28")
        );
    }

    #[test]
    fn a_paper_study_with_no_trades_renders_its_tables_and_says_so() {
        let mut report = paper_fixture();
        report.summary.trade_count = 0;
        report.summary.win_rate = 0.0;
        report.summary.cumulative_net_pnl_pct = 0.0;
        report.summary.max_drawdown_pct = 0.0;
        report.summary.excess_pnl_pct = -report.summary.hodl_pnl_pct;
        report.rules.clear();
        for market in &mut report.markets {
            market.trade_count = 0;
            market.win_rate = 0.0;
            market.net_pnl_pct = 0.0;
            market.mean_pnl_pct = 0.0;
        }

        let markdown = render_paper_markdown(&report).unwrap();
        assert!(markdown.contains("| Rule | Trades |"));
        assert!(markdown.contains("| Market | Trades |"));
        assert!(markdown.contains("No qualified signal produced a paper trade in this window."));
        assert!(!markdown.contains("NaN"), "{markdown}");
        assert_eq!(
            markdown.lines().last(),
            Some("VERDICT: HOLD WINS by 2.000pp over 2025-01-15–2025-01-28")
        );

        let csv = render_paper_csv(&report);
        assert!(!csv.contains("NaN"), "{csv}");
        assert_eq!(
            csv.lines().filter(|line| line.starts_with("rule,")).count(),
            0
        );
        assert_eq!(
            csv.lines().last(),
            Some("VERDICT: HOLD WINS by 2.000pp over 2025-01-15–2025-01-28")
        );
    }
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

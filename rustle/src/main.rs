use anyhow::{bail, Result};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use clap::{Parser, Subcommand};
use rustle::{
    analysis,
    config::Config,
    model::{
        AlertEvent, ConnectionEvent, Meta, QualifiedRuleSet, Signal, SignalOutcome,
        UniverseSnapshot, SCHEMA_VERSION,
    },
    storage,
    upbit::{self, Incoming},
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Parser)]
#[command(
    name = "rustle",
    about = "Public-data-only Upbit signal study; never submits orders"
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    InitConfig {
        #[arg(default_value = "rustle.toml")]
        path: PathBuf,
    },
    Collect {
        #[arg(long)]
        once: bool,
        /// Emit console and JSON Lines alerts only for the active qualified ruleset.
        #[arg(long)]
        emit_alerts: bool,
    },
    Analyze,
    Report {
        #[arg(long)]
        csv: bool,
    },
    Coverage {
        #[arg(long)]
        csv: bool,
    },
    /// Print explainable alerts only for rules that passed validation.
    Alert {
        /// Print each complete AlertEvent as JSON Lines instead of the human-readable block.
        #[arg(long)]
        json: bool,
    },
    /// Replay the qualified ruleset over its validation window against an equal-weight hold.
    Paper {
        #[arg(long)]
        csv: bool,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::InitConfig { path } => {
            std::fs::write(&path, Config::template()?)?;
            println!("wrote {}", path.display());
            Ok(())
        }
        cmd => {
            let cfg = Config::load(cli.config.as_deref())?;
            match cmd {
                Command::Collect { once, emit_alerts } => collect(&cfg, once, emit_alerts).await,
                Command::Analyze => analyze(&cfg),
                Command::Report { csv } => report(&cfg, csv),
                Command::Coverage { csv } => coverage(&cfg, csv),
                Command::Alert { json } => alert(&cfg, json),
                Command::Paper { csv } => paper(&cfg, csv),
                _ => unreachable!(),
            }
        }
    }
}
async fn collect(cfg: &Config, once: bool, emit_alerts: bool) -> Result<()> {
    cfg.validate_collection_intervals()?;
    let root = PathBuf::from(&cfg.data_root);
    let mut markets = upbit::top_krw_markets(cfg.top_market_count).await?;
    let now = Utc::now();
    let mut universe_day = now.date_naive();
    storage::write(
        &root,
        "universes",
        "KRW",
        now,
        &[UniverseSnapshot {
            schema_version: SCHEMA_VERSION,
            refreshed_at: now,
            markets: markets.clone(),
        }],
    )?;
    eprintln!(
        "collecting {} markets; Ctrl-C gracefully stops and flushes batches",
        markets.len()
    );
    let mut delay = 1u64;
    let mut disconnected_at = None;
    let mut detector = analysis::SignalDetector::new(cfg);
    let active_rules = load_fresh_ruleset(&root, cfg)
        .and_then(|set| qualified_rule_results(&set))
        .unwrap_or_default();
    let mut alert_sink = AlertSink::new(
        root.clone(),
        emit_alerts,
        active_rules,
        cfg.alert.cooldown_seconds,
    );
    if emit_alerts && alert_sink.rules.is_empty() {
        eprintln!("alerts blocked: no active validation-qualified ruleset; collection continues");
    }
    loop {
        // A reconnect after the configured UTC refresh hour adopts a persisted new universe.
        let refresh_now = Utc::now();
        if refresh_now.date_naive() != universe_day
            && refresh_now.hour() >= cfg.daily_refresh_utc_hour.into()
        {
            markets = upbit::top_krw_markets(cfg.top_market_count).await?;
            universe_day = refresh_now.date_naive();
            storage::write(
                &root,
                "universes",
                "KRW",
                refresh_now,
                &[UniverseSnapshot {
                    schema_version: SCHEMA_VERSION,
                    refreshed_at: refresh_now,
                    markets: markets.clone(),
                }],
            )?;
            detector.reset();
            record_connection(
                &root,
                "universe_changed",
                "universe refreshed; rolling state reset",
                None,
            )?;
        }
        let cmeta = Meta {
            schema_version: SCHEMA_VERSION,
            market: "ALL".into(),
            exchange_ts: Utc::now(),
            receive_ts: Utc::now(),
        };
        storage::write(
            &root,
            "connection_events",
            "ALL",
            Utc::now(),
            &[ConnectionEvent {
                meta: cmeta,
                state: "connecting".into(),
                detail: "public websocket".into(),
                gap_ms: None,
            }],
        )?;
        match upbit::connect(&markets).await {
            Ok(mut ws) => {
                delay = 1;
                detector.reset();
                let mut sequences = HashMap::new();
                let gap = disconnected_at
                    .take()
                    .map(|then: chrono::DateTime<Utc>| (Utc::now() - then).num_milliseconds());
                record_connection(
                    &root,
                    "connected",
                    "websocket connected; rolling and sequence state reset",
                    gap,
                )?;
                let mut ts = Vec::new();
                let mut bs = Vec::new();
                let mut ss = Vec::new();
                let flush_period = Duration::from_secs(cfg.flush_interval_seconds as u64);
                let stall_period = Duration::from_secs(cfg.stall_timeout_seconds as u64);
                let mut flush_ticker = tokio::time::interval_at(
                    tokio::time::Instant::now() + flush_period,
                    flush_period,
                );
                let stall_deadline = tokio::time::sleep(stall_period);
                tokio::pin!(stall_deadline);
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => { flush(&root, &mut ts, &mut bs, &mut ss)?; return Ok(()); }
                        maintenance = next_maintenance(&mut flush_ticker, stall_deadline.as_mut()) => match maintenance {
                            Maintenance::Flush => {
                                flush(&root, &mut ts, &mut bs, &mut ss)?;
                                let refresh_now = Utc::now();
                                if universe_refresh_due(universe_day, refresh_now, cfg.daily_refresh_utc_hour) {
                                    disconnected_at = Some(refresh_now);
                                    record_connection(
                                        &root,
                                        "universe_refresh_due",
                                        "daily universe refresh due; reconnecting",
                                        None,
                                    )?;
                                    break;
                                }
                            }
                            Maintenance::Stalled => {
                                handle_stall(&root, &mut ts, &mut bs, &mut ss)?;
                                disconnected_at = Some(Utc::now());
                                eprintln!("connection stalled: no frame within stall timeout");
                                break;
                            }
                        },
                        got = upbit::next(&mut ws) => match got {
                            Ok(Some(Incoming::Trade(t))) => {
                                stall_deadline.as_mut().reset(tokio::time::Instant::now() + stall_period);
                                // Raw capture is authoritative: sequence checks only suppress detector state.
                                ts.push(t.clone());
                                let handled: Result<()> = (|| {
                                    if sequence_ok(&mut sequences, &t, &root)? {
                                        let new = detector.on_trade(&t);
                                        retain_and_emit_alerts(&root, &mut alert_sink, &new, &mut ss)?;
                                    }
                                    if ts.len() >= MAX_BUFFERED_RECORDS { flush(&root, &mut ts, &mut bs, &mut ss)?; }
                                    Ok(())
                                })();
                                if let Err(error) = handled { flush(&root, &mut ts, &mut bs, &mut ss)?; return Err(error); }
                            }
                            Ok(Some(Incoming::Orderbook(b))) => {
                                stall_deadline.as_mut().reset(tokio::time::Instant::now() + stall_period);
                                bs.push(b.clone());
                                let handled: Result<()> = (|| {
                                    let new = detector.on_orderbook(&b);
                                    retain_and_emit_alerts(&root, &mut alert_sink, &new, &mut ss)?;
                                    if bs.len() >= MAX_BUFFERED_RECORDS { flush(&root, &mut ts, &mut bs, &mut ss)?; }
                                    Ok(())
                                })();
                                if let Err(error) = handled { flush(&root, &mut ts, &mut bs, &mut ss)?; return Err(error); }
                            }
                            Ok(None) => {
                                stall_deadline.as_mut().reset(tokio::time::Instant::now() + stall_period);
                            }
                            Err(e) => {
                                flush(&root, &mut ts, &mut bs, &mut ss)?;
                                disconnected_at = Some(Utc::now());
                                record_connection(&root, "disconnected", &e.to_string(), None)?;
                                eprintln!("connection ended: {e}");
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                disconnected_at.get_or_insert_with(Utc::now);
                record_connection(&root, "connect_failed", &e.to_string(), None)?;
                eprintln!("connect failed: {e}")
            }
        };
        if once {
            return Ok(());
        }
        let start = Utc::now();
        tokio::time::sleep(Duration::from_secs(delay)).await;
        let meta = Meta {
            schema_version: SCHEMA_VERSION,
            market: "ALL".into(),
            exchange_ts: Utc::now(),
            receive_ts: Utc::now(),
        };
        storage::write(
            &root,
            "connection_events",
            "ALL",
            Utc::now(),
            &[ConnectionEvent {
                meta,
                state: "reconnect".into(),
                detail: "backoff reconnect".into(),
                gap_ms: Some((Utc::now() - start).num_milliseconds()),
            }],
        )?;
        delay = (delay * 2).min(60);
    }
}

/// Memory backstop only. Normal flushing is the `flush_interval_seconds` ticker; this
/// exists so a starved ticker cannot grow a buffer without bound.
const MAX_BUFFERED_RECORDS: usize = 50_000;

#[derive(Debug, PartialEq, Eq)]
enum Maintenance {
    Flush,
    Stalled,
}

async fn next_maintenance(
    flush_ticker: &mut tokio::time::Interval,
    mut stall_deadline: std::pin::Pin<&mut tokio::time::Sleep>,
) -> Maintenance {
    tokio::select! {
        biased;
        _ = &mut stall_deadline => Maintenance::Stalled,
        _ = flush_ticker.tick() => Maintenance::Flush,
    }
}

fn universe_refresh_due(
    universe_day: NaiveDate,
    now: chrono::DateTime<Utc>,
    refresh_hour: u8,
) -> bool {
    now.date_naive() != universe_day && now.hour() >= u32::from(refresh_hour)
}

fn handle_stall(
    root: &Path,
    trades: &mut Vec<rustle::model::Trade>,
    books: &mut Vec<rustle::model::Orderbook>,
    signals: &mut Vec<rustle::model::Signal>,
) -> Result<()> {
    flush(root, trades, books, signals)?;
    record_connection(root, "stalled", "no frame within stall timeout", None)
}
fn record_connection(root: &Path, state: &str, detail: &str, gap_ms: Option<i64>) -> Result<()> {
    let now = Utc::now();
    storage::write(
        root,
        "connection_events",
        "ALL",
        now,
        &[ConnectionEvent {
            meta: Meta {
                schema_version: SCHEMA_VERSION,
                market: "ALL".into(),
                exchange_ts: now,
                receive_ts: now,
            },
            state: state.into(),
            detail: detail.into(),
            gap_ms,
        }],
    )?;
    Ok(())
}
fn sequence_ok(
    sequences: &mut HashMap<String, u64>,
    trade: &rustle::model::Trade,
    root: &Path,
) -> Result<bool> {
    let Some(id) = trade.sequential_id else {
        return Ok(true);
    };
    let market = trade.meta.market.clone();
    match sequences.get(&market).copied() {
        Some(last) if id == last => {
            record_connection(
                root,
                "integrity_duplicate",
                &format!("{market} sequential_id={id}"),
                None,
            )?;
            Ok(false)
        }
        Some(last) if id < last => {
            record_connection(
                root,
                "integrity_out_of_order",
                &format!("{market} sequential_id={id} after {last}"),
                None,
            )?;
            Ok(false)
        }
        _ => {
            sequences.insert(market, id);
            Ok(true)
        }
    }
}
fn load_fresh_ruleset(root: &Path, cfg: &Config) -> Result<QualifiedRuleSet> {
    let mut sets: Vec<QualifiedRuleSet> = storage::read_all(root, "active_rule_sets")?;
    sets.sort_by_key(|set| set.created_at);
    let set = sets
        .pop()
        .ok_or_else(|| anyhow::anyhow!("no persisted ruleset; run analyze"))?;
    let audit = set
        .audit
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ruleset is from an older format; run analyze"))?;
    if set.config_fingerprint.as_deref() != Some(&analysis::config_fingerprint(cfg)) {
        bail!("ruleset is stale because configuration changed; run analyze");
    }
    let trades = storage::read_all(root, "trades")?;
    let books: Vec<rustle::model::Orderbook> = storage::read_all(root, "orderbooks")?;
    let orderbook_dates = books
        .iter()
        .map(|book| book.meta.exchange_ts.date_naive())
        .collect();
    if audit.collection_dates != analysis::current_collection_dates(&trades, &orderbook_dates, cfg)?
    {
        bail!("ruleset is stale because its collection window changed; run analyze");
    }
    Ok(set)
}
fn qualified_rule_results(set: &QualifiedRuleSet) -> Result<HashMap<String, analysis::RuleResult>> {
    let audit = set
        .audit
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ruleset is from an older format; run analyze"))?;
    let mut results = HashMap::new();
    for rule in &set.rules {
        let result = audit
            .results
            .iter()
            .find(|result| result.passed && result.rule_id == *rule)
            .ok_or_else(|| {
                anyhow::anyhow!("qualified rule {rule} has no passed validation result")
            })?;
        results.insert(rule.clone(), result.clone());
    }
    Ok(results)
}

struct AlertCooldown {
    seconds: i64,
    last_emitted: HashMap<(String, String), DateTime<Utc>>,
}

impl AlertCooldown {
    fn new(seconds: i64) -> Self {
        Self {
            seconds,
            last_emitted: HashMap::new(),
        }
    }

    fn is_suppressed(&self, signal: &Signal, rule_key: &str) -> bool {
        if self.seconds == 0 {
            return false;
        }
        let key = (signal.meta.market.clone(), rule_key.to_owned());
        self.last_emitted.get(&key).is_some_and(|last| {
            signal.meta.exchange_ts < *last + chrono::Duration::seconds(self.seconds)
        })
    }

    fn record(&mut self, signal: &Signal, rule_key: &str) {
        self.last_emitted.insert(
            (signal.meta.market.clone(), rule_key.to_owned()),
            signal.meta.exchange_ts,
        );
    }
}

struct AlertSink {
    enabled: bool,
    rules: HashMap<String, analysis::RuleResult>,
    root: PathBuf,
    cooldown: AlertCooldown,
}

impl AlertSink {
    fn new(
        root: PathBuf,
        enabled: bool,
        rules: HashMap<String, analysis::RuleResult>,
        cooldown_seconds: i64,
    ) -> Self {
        Self {
            enabled,
            rules,
            root,
            cooldown: AlertCooldown::new(cooldown_seconds),
        }
    }

    fn emit(&mut self, signals: &[Signal]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        for signal in signals {
            let rule_key = analysis::rule_key(signal);
            let Some(validation) = self.rules.get(&rule_key).cloned() else {
                continue;
            };
            if self.cooldown.is_suppressed(signal, &rule_key) {
                continue;
            }
            let event = AlertEvent {
                emitted_at: Utc::now(),
                rule_key: rule_key.clone(),
                signal: signal.clone(),
                validation: Some(validation),
            };
            storage::append_jsonl(&self.root, "alerts", event.emitted_at, &event)?;
            self.cooldown.record(signal, &rule_key);
            print!("{}", analysis::render_alert(&event));
        }
        Ok(())
    }
}

fn retain_and_emit_alerts(
    root: &Path,
    sink: &mut AlertSink,
    new: &[Signal],
    live_signals: &mut Vec<Signal>,
) -> Result<()> {
    live_signals.extend_from_slice(new);
    if let Err(error) = sink.emit(new) {
        eprintln!("alert emission failed; collection continues: {error}");
        record_connection(root, "alert_error", &error.to_string(), None)?;
    }
    Ok(())
}
fn flush(
    root: &Path,
    trades: &mut Vec<rustle::model::Trade>,
    books: &mut Vec<rustle::model::Orderbook>,
    signals: &mut Vec<rustle::model::Signal>,
) -> Result<()> {
    write_partitioned(root, "trades", trades.drain(..), |trade| &trade.meta)?;
    write_partitioned(root, "orderbooks", books.drain(..), |book| &book.meta)?;
    write_partitioned(root, "live_signals", signals.drain(..), |signal| {
        &signal.meta
    })?;
    Ok(())
}

fn write_partitioned<T, I, F>(root: &Path, dataset: &str, records: I, meta: F) -> Result<()>
where
    T: serde::Serialize,
    I: IntoIterator<Item = T>,
    F: Fn(&T) -> &Meta,
{
    let mut groups: BTreeMap<(String, NaiveDate), Vec<T>> = BTreeMap::new();
    for record in records {
        let record_meta = meta(&record);
        groups
            .entry((
                record_meta.market.clone(),
                record_meta.exchange_ts.date_naive(),
            ))
            .or_default()
            .push(record);
    }
    for ((market, _), records) in groups {
        storage::write(
            root,
            dataset,
            &market,
            meta(&records[0]).exchange_ts,
            &records,
        )?;
    }
    Ok(())
}
fn analyze(cfg: &Config) -> Result<()> {
    cfg.validate_collection_intervals()?;
    let root = PathBuf::from(&cfg.data_root);
    // Orderbooks are the bulk of the archive — ~157M records over a 28-day window at the
    // measured rate — and nothing after signal building reads them again. So they are read
    // one UTC partition at a time and dropped, while trades accumulate: outcome matching and
    // validation both need the whole trade series. The detector is deliberately created once
    // and held across every chunk; see `analysis::feed_signals`.
    let mut dates: BTreeSet<NaiveDate> = storage::dataset_dates(&root, "trades")?
        .into_iter()
        .collect();
    dates.extend(storage::dataset_dates(&root, "orderbooks")?);
    let mut detector = analysis::SignalDetector::new(cfg);
    let mut t: Vec<rustle::model::Trade> = vec![];
    let mut s: Vec<Signal> = vec![];
    let mut orderbook_dates: BTreeSet<NaiveDate> = BTreeSet::new();
    for date in dates {
        let day_trades: Vec<rustle::model::Trade> = storage::read_date(&root, "trades", date)?;
        let day_books: Vec<rustle::model::Orderbook> =
            storage::read_date(&root, "orderbooks", date)?;
        orderbook_dates.extend(day_books.iter().map(|b| b.meta.exchange_ts.date_naive()));
        s.extend(analysis::feed_signals(
            &mut detector,
            &day_trades,
            &day_books,
        ));
        t.extend(day_trades);
    }
    // Signals are derived artefacts. Replace them so a repeated analyze is deterministic.
    storage::clear_dataset(&root, "signals")?;
    let audit = analysis::evaluate_with_audit(&s, &t, &orderbook_dates, cfg)?;
    let outcomes = analysis::build_outcomes(&s, &t, cfg);
    let signal_count = s.len();
    write_partitioned(&root, "signals", s, |signal| &signal.meta)?;
    storage::clear_dataset(&root, "signal_outcomes")?;
    write_partitioned(
        &root,
        "signal_outcomes",
        outcomes,
        |outcome: &SignalOutcome| &outcome.signal.meta,
    )?;
    storage::clear_dataset(&root, "evaluation_results")?;
    storage::write(
        &root,
        "evaluation_results",
        "ALL",
        Utc::now(),
        std::slice::from_ref(&audit),
    )?;
    storage::clear_dataset(&root, "active_rule_sets")?;
    let rules: Vec<String> = audit
        .results
        .iter()
        .filter(|result| result.passed)
        .map(|result| result.rule_id.clone())
        .collect();
    storage::write(
        &root,
        "active_rule_sets",
        "ALL",
        Utc::now(),
        &[QualifiedRuleSet {
            version: 2,
            created_at: Utc::now(),
            tuning_start: audit.input_start,
            tuning_end: audit.input_start
                + chrono::Duration::days((cfg.validation.tuning_days - 1) as i64),
            validation_start: audit.input_start
                + chrono::Duration::days(cfg.validation.tuning_days as i64),
            validation_end: audit.input_end,
            bootstrap_seed: audit.bootstrap_seed,
            rules,
            audit: Some(audit.clone()),
            config_fingerprint: Some(analysis::config_fingerprint(cfg)),
        }],
    )?;
    println!(
        "generated {} candidate signals; selected {} rules for untouched validation",
        signal_count,
        audit.results.len()
    );
    Ok(())
}
fn report(cfg: &Config, csv: bool) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let audit = load_fresh_ruleset(&root, cfg)?
        .audit
        .expect("fresh loader requires audit");
    if csv {
        print!("{}", analysis::render_csv_report(&audit));
    } else {
        print!("{}", analysis::render_markdown_report(&audit)?);
    }
    Ok(())
}

fn coverage(cfg: &Config, csv: bool) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let trades: Vec<rustle::model::Trade> = storage::read_all(&root, "trades")?;
    let books: Vec<rustle::model::Orderbook> = storage::read_all(&root, "orderbooks")?;
    let live_signals: Vec<rustle::model::Signal> = storage::read_all(&root, "live_signals")?;
    let events: Vec<ConnectionEvent> = storage::read_all(&root, "connection_events")?;
    let rows = analysis::collection_coverage(&trades, &books, &live_signals, &events);
    let orderbook_dates = books
        .iter()
        .map(|book| book.meta.exchange_ts.date_naive())
        .collect();
    let status = analysis::collection_date_status(&trades, &orderbook_dates, cfg)?;

    if csv {
        println!("date,trades,orderbooks,live_signals,markets,disconnected,stalled,total_gap_ms,longest_gap_ms");
        for row in &rows {
            println!(
                "{},{},{},{},{},{},{},{},{}",
                row.date,
                row.trade_count,
                row.orderbook_count,
                row.live_signal_count,
                row.market_count,
                row.disconnected_count,
                row.stalled_count,
                row.total_gap_ms,
                row.longest_gap_ms,
            );
        }
    } else {
        println!("# Rustle collection coverage\n");
        println!("| UTC date | Trades | Orderbooks | Live signals | Markets | Disconnected | Stalled | Total gap ms | Longest gap ms |");
        println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        for row in &rows {
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                row.date,
                row.trade_count,
                row.orderbook_count,
                row.live_signal_count,
                row.market_count,
                row.disconnected_count,
                row.stalled_count,
                row.total_gap_ms,
                row.longest_gap_ms,
            );
        }
        println!();
    }
    let summary = format!(
        "{} of {} required contiguous UTC dates present",
        status.present_count, status.required
    );
    if csv {
        eprintln!("{summary}");
        if !status.missing_dates.is_empty() {
            eprintln!(
                "Missing UTC dates: {}",
                status
                    .missing_dates
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    } else {
        println!("{summary}");
        if !status.missing_dates.is_empty() {
            println!(
                "Missing UTC dates: {}",
                status
                    .missing_dates
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    Ok(())
}

fn paper(cfg: &Config, csv: bool) -> Result<()> {
    cfg.validate_collection_intervals()?;
    let root = PathBuf::from(&cfg.data_root);
    let set = load_fresh_ruleset(&root, cfg)?;
    if set.rules.is_empty() {
        storage::clear_dataset(&root, "paper_trades")?;
        storage::clear_dataset(&root, "paper_summaries")?;
        println!("paper blocked: no validation-qualified rules");
        return Ok(());
    }
    let trades = storage::read_all(&root, "trades")?;
    let books = storage::read_all(&root, "orderbooks")?;
    let signals = analysis::build_signals(&trades, &books, cfg);
    // Days 1-14 are the days each rule was chosen on; simulating them would restate the
    // tuning result as a finding. The gate authorized the validation window, so that is
    // the window that gets traded.
    let mut report = analysis::paper(
        &signals,
        &trades,
        &set.rules,
        (set.validation_start, set.validation_end),
        Utc::now(),
        cfg,
    );
    let paper_trades = std::mem::take(&mut report.trades);
    storage::clear_dataset(&root, "paper_trades")?;
    write_partitioned(&root, "paper_trades", paper_trades, |paper_trade| {
        &paper_trade.signal.meta
    })?;
    storage::clear_dataset(&root, "paper_summaries")?;
    storage::write(
        &root,
        "paper_summaries",
        "ALL",
        report.summary.generated_at,
        std::slice::from_ref(&report.summary),
    )?;
    if csv {
        print!("{}", analysis::render_paper_csv(&report));
    } else {
        print!("{}", analysis::render_paper_markdown(&report)?);
    }
    Ok(())
}

fn alert(cfg: &Config, json: bool) -> Result<()> {
    cfg.validate_collection_intervals()?;
    let root = PathBuf::from(&cfg.data_root);
    let passed = match load_fresh_ruleset(&root, cfg).and_then(|set| qualified_rule_results(&set)) {
        Ok(passed) => passed,
        Err(error) => {
            eprintln!("alerts blocked: {error}");
            return Ok(());
        }
    };
    let trades = storage::read_all(&root, "trades")?;
    let books = storage::read_all(&root, "orderbooks")?;
    let signals = analysis::build_signals(&trades, &books, cfg);
    let mut cooldown = AlertCooldown::new(cfg.alert.cooldown_seconds);
    for signal in &signals {
        let rule_key = analysis::rule_key(signal);
        let Some(validation) = passed.get(&rule_key).cloned() else {
            continue;
        };
        if cooldown.is_suppressed(signal, &rule_key) {
            continue;
        }
        let event = AlertEvent {
            emitted_at: Utc::now(),
            rule_key: rule_key.clone(),
            signal: signal.clone(),
            validation: Some(validation),
        };
        cooldown.record(signal, &rule_key);
        if json {
            println!("{}", serde_json::to_string(&event)?);
        } else {
            print!("{}", analysis::render_alert(&event));
        }
    }
    if passed.is_empty() {
        eprintln!("alerts blocked: no validation-qualified rules");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use rustle::model::{Side, Signal, Trade, SCHEMA_VERSION};

    fn meta(minutes: i64) -> Meta {
        let exchange_ts =
            Utc.with_ymd_and_hms(2025, 1, 1, 23, 59, 0).unwrap() + Duration::minutes(minutes);
        Meta {
            schema_version: SCHEMA_VERSION,
            market: "KRW-TEST".into(),
            exchange_ts,
            receive_ts: exchange_ts,
        }
    }

    fn signal_at(minutes: i64, market: &str, rule_id: &str) -> Signal {
        let mut meta = meta(minutes);
        meta.market = market.into();
        Signal {
            meta,
            signal_type: "synthetic".into(),
            direction: Side::Buy,
            feature_value: 2.0,
            baseline: 1.0,
            rationale: "observed 2.0 (threshold 1.0)".into(),
            market_snapshot: serde_json::json!({"evidence": {"source": "test"}}),
            rule_id: rule_id.into(),
        }
    }

    fn rule_result(rule_key: &str) -> analysis::RuleResult {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        analysis::RuleResult {
            rule_id: rule_key.into(),
            signal_type: "synthetic".into(),
            train_count: 100,
            train_hit_rate: 0.6,
            validation_count: 50,
            validation_hit_rate: 0.58,
            random_hit_rate: 0.49,
            lift: 0.09,
            ci_low: 0.03,
            ci_high: 0.15,
            retention: 0.96,
            passed: true,
            tuning_start: start,
            tuning_end: start + Duration::days(13),
            validation_start: start + Duration::days(14),
            validation_end: start + Duration::days(27),
        }
    }

    #[test]
    fn cooldown_is_scoped_by_market_and_rule_and_zero_disables_it() {
        let first = signal_at(0, "KRW-A", "candidate");
        let inside = signal_at(1, "KRW-A", "candidate");
        let boundary = signal_at(15, "KRW-A", "candidate");
        let other_market = signal_at(1, "KRW-B", "candidate");
        let other_rule = signal_at(1, "KRW-A", "other");
        let key = analysis::rule_key(&first);
        let mut cooldown = AlertCooldown::new(900);

        assert!(!cooldown.is_suppressed(&first, &key));
        cooldown.record(&first, &key);
        assert!(cooldown.is_suppressed(&inside, &key));
        assert!(!cooldown.is_suppressed(&boundary, &key));
        assert!(!cooldown.is_suppressed(&other_market, &key));
        assert!(!cooldown.is_suppressed(&other_rule, "synthetic:other"));

        let mut disabled = AlertCooldown::new(0);
        disabled.record(&first, &key);
        assert!(!disabled.is_suppressed(&inside, &key));
    }

    #[test]
    fn alert_sink_attaches_validation_and_suppresses_repeats() {
        let dir = tempfile::tempdir().unwrap();
        let key = "synthetic:candidate";
        let mut sink = AlertSink::new(
            dir.path().to_path_buf(),
            true,
            HashMap::from([(key.into(), rule_result(key))]),
            900,
        );
        sink.emit(&[
            signal_at(0, "KRW-A", "candidate"),
            signal_at(1, "KRW-A", "candidate"),
            signal_at(1, "KRW-B", "candidate"),
        ])
        .unwrap();

        let date_dir = std::fs::read_dir(dir.path().join("alerts"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let contents = std::fs::read_to_string(date_dir.join("events.jsonl")).unwrap();
        let events: Vec<AlertEvent> = contents
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].validation.as_ref().unwrap().validation_count, 50);
        assert_eq!(events[1].signal.meta.market, "KRW-B");
    }

    #[test]
    fn cooldown_suppression_does_not_remove_live_signals() {
        let dir = tempfile::tempdir().unwrap();
        let key = "synthetic:candidate";
        let mut sink = AlertSink::new(
            dir.path().to_path_buf(),
            true,
            HashMap::from([(key.into(), rule_result(key))]),
            900,
        );
        let detected = vec![
            signal_at(0, "KRW-A", "candidate"),
            signal_at(1, "KRW-A", "candidate"),
        ];
        let mut buffered = vec![];

        retain_and_emit_alerts(dir.path(), &mut sink, &detected, &mut buffered).unwrap();

        assert_eq!(buffered.len(), 2);
        let date_dir = std::fs::read_dir(dir.path().join("alerts"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let contents = std::fs::read_to_string(date_dir.join("events.jsonl")).unwrap();
        assert_eq!(contents.lines().count(), 1);
    }

    #[test]
    fn alert_write_failure_keeps_live_signal_and_collection_can_continue() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("alerts"), "blocks alert directory").unwrap();
        let key = "synthetic:candidate";
        let mut sink = AlertSink::new(
            dir.path().to_path_buf(),
            true,
            HashMap::from([(key.into(), rule_result(key))]),
            900,
        );
        let detected = vec![signal_at(0, "KRW-A", "candidate")];
        let mut buffered = vec![];

        retain_and_emit_alerts(dir.path(), &mut sink, &detected, &mut buffered).unwrap();

        assert_eq!(buffered.len(), 1);
        let events: Vec<ConnectionEvent> =
            storage::read_all(dir.path(), "connection_events").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, "alert_error");
    }

    #[test]
    fn alert_json_flag_parses() {
        let cli = Cli::try_parse_from(["rustle", "alert", "--json"]).unwrap();
        assert!(matches!(cli.command, Command::Alert { json: true }));
    }

    #[test]
    fn paper_csv_flag_parses_like_its_sibling_commands() {
        let cli = Cli::try_parse_from(["rustle", "paper", "--csv"]).unwrap();
        assert!(matches!(cli.command, Command::Paper { csv: true }));
        let plain = Cli::try_parse_from(["rustle", "paper"]).unwrap();
        assert!(matches!(plain.command, Command::Paper { csv: false }));
    }

    #[test]
    fn flush_keeps_live_signals_separate_and_partitions_by_utc_date() {
        let dir = tempfile::tempdir().unwrap();
        let mut trades = vec![
            Trade {
                meta: meta(0),
                price: 10.,
                volume: 1.,
                side: Side::Buy,
                sequential_id: None,
            },
            Trade {
                meta: meta(2),
                price: 11.,
                volume: 1.,
                side: Side::Buy,
                sequential_id: None,
            },
        ];
        let mut books = vec![];
        let mut live_signals = vec![Signal {
            meta: meta(2),
            signal_type: "test".into(),
            direction: Side::Buy,
            feature_value: 1.,
            baseline: 0.,
            rationale: "test".into(),
            market_snapshot: serde_json::json!({}),
            rule_id: "test".into(),
        }];

        flush(dir.path(), &mut trades, &mut books, &mut live_signals).unwrap();

        let live: Vec<Signal> = storage::read_all(dir.path(), "live_signals").unwrap();
        let derived: Vec<Signal> = storage::read_all(dir.path(), "signals").unwrap();
        assert_eq!(live.len(), 1);
        assert!(derived.is_empty());
        assert!(dir.path().join("trades/date=2025-01-01").exists());
        assert!(dir.path().join("trades/date=2025-01-02").exists());
    }

    #[test]
    fn sequence_tracker_records_duplicate_and_reverse_events_but_not_id_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let mut tracker = HashMap::new();
        let trade = |id| Trade {
            meta: meta(id as i64),
            price: 1.0,
            volume: 1.0,
            side: Side::Buy,
            sequential_id: Some(id),
        };
        assert!(sequence_ok(&mut tracker, &trade(10), dir.path()).unwrap());
        assert!(!sequence_ok(&mut tracker, &trade(10), dir.path()).unwrap());
        assert!(!sequence_ok(&mut tracker, &trade(9), dir.path()).unwrap());
        assert!(sequence_ok(&mut tracker, &trade(12), dir.path()).unwrap());
        let events: Vec<ConnectionEvent> =
            storage::read_all(dir.path(), "connection_events").unwrap();
        let states: std::collections::BTreeSet<_> =
            events.iter().map(|event| event.state.as_str()).collect();
        assert_eq!(
            states,
            ["integrity_duplicate", "integrity_out_of_order"].into()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn flush_ticks_do_not_extend_the_stall_deadline() {
        let start = tokio::time::Instant::now();
        let mut ticker = tokio::time::interval_at(
            start + Duration::seconds(30).to_std().unwrap(),
            Duration::seconds(30).to_std().unwrap(),
        );
        let deadline = tokio::time::sleep_until(start + Duration::seconds(90).to_std().unwrap());
        tokio::pin!(deadline);

        tokio::time::advance(Duration::seconds(30).to_std().unwrap()).await;
        assert_eq!(
            next_maintenance(&mut ticker, deadline.as_mut()).await,
            Maintenance::Flush
        );
        tokio::time::advance(Duration::seconds(30).to_std().unwrap()).await;
        assert_eq!(
            next_maintenance(&mut ticker, deadline.as_mut()).await,
            Maintenance::Flush
        );
        tokio::time::advance(Duration::seconds(30).to_std().unwrap()).await;
        assert_eq!(
            next_maintenance(&mut ticker, deadline.as_mut()).await,
            Maintenance::Stalled
        );
    }

    #[tokio::test]
    async fn invalid_intervals_are_rejected_before_collection_connects() {
        let cfg = Config {
            stall_timeout_seconds: 0,
            ..Config::default()
        };
        let error = collect(&cfg, true, false).await.unwrap_err();
        assert!(error.to_string().contains("stall_timeout_seconds"));
    }

    #[test]
    fn stall_flushes_buffers_and_records_the_event() {
        let dir = tempfile::tempdir().unwrap();
        let mut trades = vec![Trade {
            meta: meta(0),
            price: 10.,
            volume: 1.,
            side: Side::Buy,
            sequential_id: None,
        }];
        let mut books = vec![];
        let mut signals = vec![];

        handle_stall(dir.path(), &mut trades, &mut books, &mut signals).unwrap();

        assert!(trades.is_empty());
        assert_eq!(
            storage::read_all::<Trade>(dir.path(), "trades")
                .unwrap()
                .len(),
            1
        );
        let events: Vec<ConnectionEvent> =
            storage::read_all(dir.path(), "connection_events").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, "stalled");
        assert_eq!(events[0].detail, "no frame within stall timeout");
    }

    #[test]
    fn buffer_backstop_sits_far_above_one_flush_interval_of_observed_traffic() {
        // Measured 2026-08-26: 7,516 orderbooks in 116s across 20 markets = ~65/s.
        // The backstop exists to bound memory if the flush ticker is starved, not to
        // drive normal flushing — if it trips on ordinary traffic we are back to
        // writing a file per market every few seconds.
        let per_interval = 65 * Config::default().flush_interval_seconds as usize;
        assert!(
            MAX_BUFFERED_RECORDS > per_interval * 2,
            "backstop {MAX_BUFFERED_RECORDS} must clear {per_interval} buffered records with margin"
        );
    }

    #[test]
    fn one_flush_writes_one_file_per_market_day_regardless_of_record_count() {
        let dir = tempfile::tempdir().unwrap();
        let mut trades: Vec<Trade> = (0..600)
            .map(|i| {
                let mut m = meta(0);
                m.market = if i % 2 == 0 { "KRW-A" } else { "KRW-B" }.into();
                Trade {
                    meta: m,
                    price: 10.,
                    volume: 1.,
                    side: Side::Buy,
                    sequential_id: None,
                }
            })
            .collect();

        flush(dir.path(), &mut trades, &mut vec![], &mut vec![]).unwrap();

        let files = std::fs::read_dir(dir.path().join("trades/date=2025-01-01"))
            .unwrap()
            .flat_map(|market| std::fs::read_dir(market.unwrap().path()).unwrap())
            .count();
        assert_eq!(files, 2, "600 records across 2 markets is 2 files, not 12");
    }

    #[test]
    fn empty_flush_does_not_create_dataset_files() {
        let dir = tempfile::tempdir().unwrap();
        flush(dir.path(), &mut vec![], &mut vec![], &mut vec![]).unwrap();
        assert!(!dir.path().join("trades").exists());
        assert!(!dir.path().join("orderbooks").exists());
        assert!(!dir.path().join("live_signals").exists());
    }

    #[test]
    fn universe_refresh_waits_for_a_new_utc_date_and_configured_hour() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let at = |date: NaiveDate, hour: u32| {
            Utc.from_utc_datetime(&date.and_hms_opt(hour, 0, 0).unwrap())
        };
        assert!(!universe_refresh_due(day, at(day, 23), 3));
        let next = day.succ_opt().unwrap();
        assert!(!universe_refresh_due(day, at(next, 2), 3));
        assert!(universe_refresh_due(day, at(next, 3), 3));
        assert!(universe_refresh_due(day, at(next, 4), 3));
    }
}

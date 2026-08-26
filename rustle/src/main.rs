use anyhow::{bail, Result};
use chrono::{NaiveDate, Timelike, Utc};
use clap::{Parser, Subcommand};
use rustle::{
    analysis,
    config::Config,
    model::{
        AlertEvent, ConnectionEvent, Meta, PaperSummary, QualifiedRuleSet, SignalOutcome,
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
    Alert,
    Paper,
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
                Command::Alert => alert(&cfg),
                Command::Paper => paper(&cfg),
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
    let active_rules = load_fresh_ruleset(&root, cfg).ok().map(|set| set.rules);
    if emit_alerts && active_rules.as_ref().is_none_or(|rules| rules.is_empty()) {
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
                                        emit_live_alerts(&root, active_rules.as_deref(), emit_alerts, &new)?;
                                        ss.extend(new);
                                    }
                                    let should_flush = ts.len() >= 100;
                                    if should_flush { flush(&root, &mut ts, &mut bs, &mut ss)?; }
                                    Ok(())
                                })();
                                if let Err(error) = handled { flush(&root, &mut ts, &mut bs, &mut ss)?; return Err(error); }
                            }
                            Ok(Some(Incoming::Orderbook(b))) => {
                                stall_deadline.as_mut().reset(tokio::time::Instant::now() + stall_period);
                                bs.push(b.clone());
                                let handled: Result<()> = (|| {
                                    let new = detector.on_orderbook(&b);
                                    emit_live_alerts(&root, active_rules.as_deref(), emit_alerts, &new)?;
                                    ss.extend(new);
                                    if bs.len() >= 100 { flush(&root, &mut ts, &mut bs, &mut ss)?; }
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
fn emit_live_alerts(
    root: &Path,
    rules: Option<&[String]>,
    enabled: bool,
    signals: &[rustle::model::Signal],
) -> Result<()> {
    if !enabled {
        return Ok(());
    }
    let Some(rules) = rules else { return Ok(()) };
    for signal in signals
        .iter()
        .filter(|signal| rules.contains(&analysis::rule_key(signal)))
    {
        let event = AlertEvent {
            emitted_at: Utc::now(),
            rule_key: analysis::rule_key(signal),
            signal: signal.clone(),
        };
        println!(
            "ALERT {} {} {:?}: {}",
            event.signal.meta.exchange_ts,
            event.signal.meta.market,
            event.signal.direction,
            event.signal.rationale
        );
        storage::append_jsonl(root, "alerts", event.emitted_at, &event)?;
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
    let t = storage::read_all(&root, "trades")?;
    let b = storage::read_all(&root, "orderbooks")?;
    let orderbook_dates: BTreeSet<_> = b
        .iter()
        .map(|book: &rustle::model::Orderbook| book.meta.exchange_ts.date_naive())
        .collect();
    let s = analysis::build_signals(&t, &b, cfg);
    drop(b);
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

fn paper(cfg: &Config) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let ids = load_fresh_ruleset(&root, cfg)?.rules;
    if ids.is_empty() {
        storage::clear_dataset(&root, "paper_trades")?;
        storage::clear_dataset(&root, "paper_summaries")?;
        println!("paper blocked: no validation-qualified rules");
        return Ok(());
    }
    let trades = storage::read_all(&root, "trades")?;
    let books = storage::read_all(&root, "orderbooks")?;
    let signals = analysis::build_signals(&trades, &books, cfg);
    let p = analysis::paper(&signals, &trades, &ids, cfg);
    let paper_count = p.len();
    let summary = PaperSummary {
        generated_at: Utc::now(),
        trade_count: paper_count,
        cumulative_net_pnl_pct: p.iter().map(|trade| trade.net_pnl_pct).sum(),
        win_rate: p.iter().filter(|trade| trade.net_pnl_pct > 0.0).count() as f64
            / paper_count.max(1) as f64,
        long_only_benchmark_pnl_pct: p
            .iter()
            .map(|trade| trade.long_only_benchmark_pnl_pct)
            .sum(),
    };
    storage::clear_dataset(&root, "paper_trades")?;
    write_partitioned(&root, "paper_trades", p, |paper_trade| {
        &paper_trade.signal.meta
    })?;
    storage::clear_dataset(&root, "paper_summaries")?;
    storage::write(
        &root,
        "paper_summaries",
        "ALL",
        summary.generated_at,
        std::slice::from_ref(&summary),
    )?;
    println!(
        "simulated {} paper trades; net {:.3}%, win {:.1}%, long-only benchmark {:.3}%",
        summary.trade_count,
        summary.cumulative_net_pnl_pct,
        summary.win_rate * 100.0,
        summary.long_only_benchmark_pnl_pct
    );
    Ok(())
}

fn alert(cfg: &Config) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let passed = load_fresh_ruleset(&root, cfg)?.rules;
    let trades = storage::read_all(&root, "trades")?;
    let books = storage::read_all(&root, "orderbooks")?;
    let signals = analysis::build_signals(&trades, &books, cfg);
    for signal in signals
        .iter()
        .filter(|s| passed.contains(&format!("{}:{}", s.signal_type, s.rule_id)))
    {
        println!(
            "ALERT {} {} {:?}: {}\n{}",
            signal.meta.exchange_ts,
            signal.meta.market,
            signal.direction,
            signal.rationale,
            serde_json::to_string(signal)?
        );
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

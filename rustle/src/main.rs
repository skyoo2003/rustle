use anyhow::Result;
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
    collections::{BTreeMap, HashMap},
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
                Command::Alert => alert(&cfg),
                Command::Paper => paper(&cfg),
                _ => unreachable!(),
            }
        }
    }
}
async fn collect(cfg: &Config, once: bool, emit_alerts: bool) -> Result<()> {
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
    let active_rules = active_ruleset(&root)?;
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
            detector.retain_markets(&markets);
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
                loop {
                    tokio::select! { _=tokio::signal::ctrl_c()=>{flush(&root,&mut ts,&mut bs,&mut ss)?;return Ok(())}, got=upbit::next(&mut ws)=>match got {Ok(Some(Incoming::Trade(t)))=>{
                        if sequence_ok(&mut sequences, &t, &root)? { let new = detector.on_trade(&t); emit_live_alerts(&root, active_rules.as_deref(), emit_alerts, &new)?; ss.extend(new); ts.push(t); }
                        if ts.len()>=100{flush(&root,&mut ts,&mut bs,&mut ss)?}
                    },Ok(Some(Incoming::Orderbook(b)))=>{let new=detector.on_orderbook(&b);emit_live_alerts(&root, active_rules.as_deref(), emit_alerts, &new)?;ss.extend(new);bs.push(b);if bs.len()>=100{flush(&root,&mut ts,&mut bs,&mut ss)?}},Ok(None)=>{},Err(e)=>{flush(&root,&mut ts,&mut bs,&mut ss)?;disconnected_at=Some(Utc::now());record_connection(&root,"disconnected",&e.to_string(),None)?;eprintln!("connection ended: {e}");break}} }
                }
            }
            Err(e) => {
                disconnected_at.get_or_insert_with(Utc::now);
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
        Some(last) if id > last + 1 => {
            record_connection(
                root,
                "integrity_gap",
                &format!("{market} missing {} sequential ids", id - last - 1),
                None,
            )?;
            sequences.insert(market, id);
            Ok(true)
        }
        _ => {
            sequences.insert(market, id);
            Ok(true)
        }
    }
}
fn active_ruleset(root: &Path) -> Result<Option<Vec<String>>> {
    let mut sets: Vec<QualifiedRuleSet> = storage::read_all(root, "active_rule_sets")?;
    sets.sort_by_key(|set| set.created_at);
    Ok(sets.pop().map(|set| set.rules))
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
    let root = PathBuf::from(&cfg.data_root);
    let t = storage::read_all(&root, "trades")?;
    let b = storage::read_all(&root, "orderbooks")?;
    let s = analysis::build_signals(t.clone(), b.clone(), cfg);
    // Signals are derived artefacts. Replace them so a repeated analyze is deterministic.
    storage::clear_dataset(&root, "signals")?;
    let audit = analysis::evaluate_with_audit(&s, &t, &b, cfg)?;
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
            version: 1,
            created_at: Utc::now(),
            tuning_start: audit.input_start,
            tuning_end: audit
                .results
                .first()
                .map(|r| r.tuning_end)
                .unwrap_or(audit.input_start),
            validation_start: audit
                .results
                .first()
                .map(|r| r.validation_start)
                .unwrap_or(audit.input_end),
            validation_end: audit.input_end,
            bootstrap_seed: audit.bootstrap_seed,
            rules,
        }],
    )?;
    println!(
        "generated {} candidate signals; selected {} rules for untouched validation",
        signal_count,
        audit.results.len()
    );
    Ok(())
}
fn results(cfg: &Config) -> Result<Vec<analysis::RuleResult>> {
    let root = PathBuf::from(&cfg.data_root);
    let trades = storage::read_all(&root, "trades")?;
    let books = storage::read_all(&root, "orderbooks")?;
    // Do not trust accumulated signal files: always regenerate candidates from raw input.
    let signals = analysis::build_signals(trades.clone(), books.clone(), cfg);
    analysis::evaluate(&signals, &trades, &books, cfg)
}
fn report(cfg: &Config, csv: bool) -> Result<()> {
    let r = results(cfg)?;
    if csv {
        println!(
            "signal_type,rule_id,tuning_start,tuning_end,validation_start,validation_end,train_count,train_hit_rate,validation_count,validation_hit_rate,random_hit_rate,lift,ci_low,ci_high,passed"
        );
        for x in &r {
            println!(
                "{},{},{},{},{},{},{},{:.4},{},{:.4},{:.4},{:.4},{:.4},{:.4},{}",
                x.signal_type,
                x.rule_id,
                x.tuning_start,
                x.tuning_end,
                x.validation_start,
                x.validation_end,
                x.train_count,
                x.train_hit_rate,
                x.validation_count,
                x.validation_hit_rate,
                x.random_hit_rate,
                x.lift,
                x.ci_low,
                x.ci_high,
                x.passed
            )
        }
    } else {
        println!("# Rustle validation report\n\n| Signal type | Selected rule | Tuning | Validation | Train n / hit | Validation n / hit | Matched random | Lift | 95% CI | Pass |\n|---|---|---|---|---|---|---:|---:|---|---|");
        for x in &r {
            println!(
                "| {} | {} | {}–{} | {}–{} | {} / {:.1}% | {} / {:.1}% | {:.1}% | {:.1}% | [{:.1}%, {:.1}%] | {} |",
                x.signal_type,
                x.rule_id,
                x.tuning_start,
                x.tuning_end,
                x.validation_start,
                x.validation_end,
                x.train_count,
                x.train_hit_rate * 100.,
                x.validation_count,
                x.validation_hit_rate * 100.,
                x.random_hit_rate * 100.,
                x.lift * 100.,
                x.ci_low * 100.,
                x.ci_high * 100.,
                if x.passed { "yes" } else { "no" }
            )
        }
        if r.is_empty() || !r.iter().any(|x| x.passed) {
            println!("\n**Milestones 3–4 blocked:** no candidate has passed untouched validation.")
        };
        println!("\n```json\n{}\n```", serde_json::to_string_pretty(&r)?)
    }
    Ok(())
}
fn paper(cfg: &Config) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let ids = active_ruleset(&root)?.unwrap_or_default();
    if ids.is_empty() {
        storage::clear_dataset(&root, "paper_trades")?;
        storage::clear_dataset(&root, "paper_summaries")?;
        println!("paper blocked: no validation-qualified rules");
        return Ok(());
    }
    let p = analysis::paper(
        &analysis::build_signals(
            storage::read_all(&root, "trades")?,
            storage::read_all(&root, "orderbooks")?,
            cfg,
        ),
        &storage::read_all(&root, "trades")?,
        &ids,
        cfg,
    );
    let paper_count = p.len();
    let summary = PaperSummary {
        generated_at: Utc::now(),
        trade_count: paper_count,
        cumulative_net_pnl_pct: p.iter().map(|trade| trade.net_pnl_pct).sum(),
        win_rate: p.iter().filter(|trade| trade.net_pnl_pct > 0.0).count() as f64
            / paper_count.max(1) as f64,
        hodl_benchmark_pnl_pct: p.iter().map(|trade| trade.benchmark_pnl_pct).sum(),
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
        "simulated {} paper trades; net {:.3}%, win {:.1}%, HODL {:.3}%",
        summary.trade_count,
        summary.cumulative_net_pnl_pct,
        summary.win_rate * 100.0,
        summary.hodl_benchmark_pnl_pct
    );
    Ok(())
}

fn alert(cfg: &Config) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let passed = active_ruleset(&root)?.unwrap_or_default();
    let signals = analysis::build_signals(
        storage::read_all(&root, "trades")?,
        storage::read_all(&root, "orderbooks")?,
        cfg,
    );
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
    fn sequence_tracker_records_duplicate_reverse_and_gap_events() {
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
            [
                "integrity_duplicate",
                "integrity_gap",
                "integrity_out_of_order"
            ]
            .into()
        );
    }
}

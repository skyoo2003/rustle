use anyhow::Result;
use chrono::{Timelike, Utc};
use clap::{Parser, Subcommand};
use rustle::{
    analysis,
    config::Config,
    model::{ConnectionEvent, Meta, UniverseSnapshot, SCHEMA_VERSION},
    storage,
    upbit::{self, Incoming},
};
use std::{
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
                Command::Collect { once } => collect(&cfg, once).await,
                Command::Analyze => analyze(&cfg),
                Command::Report { csv } => report(&cfg, csv),
                Command::Alert => alert(&cfg),
                Command::Paper => paper(&cfg),
                _ => unreachable!(),
            }
        }
    }
}
async fn collect(cfg: &Config, once: bool) -> Result<()> {
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
                let mut ts = Vec::new();
                let mut bs = Vec::new();
                loop {
                    tokio::select! { _=tokio::signal::ctrl_c()=>{flush(&root,&mut ts,&mut bs)?;return Ok(())}, got=upbit::next(&mut ws)=>match got {Ok(Some(Incoming::Trade(t)))=>{ts.push(t);if ts.len()>=100{flush(&root,&mut ts,&mut bs)?}},Ok(Some(Incoming::Orderbook(b)))=>{bs.push(b);if bs.len()>=100{flush(&root,&mut ts,&mut bs)?}},Ok(None)=>{},Err(e)=>{flush(&root,&mut ts,&mut bs)?;eprintln!("connection ended: {e}");break}} }
                }
            }
            Err(e) => eprintln!("connect failed: {e}"),
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
fn flush(
    root: &Path,
    trades: &mut Vec<rustle::model::Trade>,
    books: &mut Vec<rustle::model::Orderbook>,
) -> Result<()> {
    use std::collections::HashMap;
    let mut tg: HashMap<String, Vec<rustle::model::Trade>> = HashMap::new();
    for t in trades.drain(..) {
        tg.entry(t.meta.market.clone()).or_default().push(t)
    }
    for (m, x) in tg {
        storage::write(root, "trades", &m, x[0].meta.exchange_ts, &x)?;
    }
    let mut bg: HashMap<String, Vec<rustle::model::Orderbook>> = HashMap::new();
    for b in books.drain(..) {
        bg.entry(b.meta.market.clone()).or_default().push(b)
    }
    for (m, x) in bg {
        storage::write(root, "orderbooks", &m, x[0].meta.exchange_ts, &x)?;
    }
    Ok(())
}
fn analyze(cfg: &Config) -> Result<()> {
    use std::collections::HashMap;
    let root = PathBuf::from(&cfg.data_root);
    let t = storage::read_all(&root, "trades")?;
    let b = storage::read_all(&root, "orderbooks")?;
    let s = analysis::build_signals(t.clone(), b.clone(), cfg);
    // Signals are derived artefacts. Replace them so a repeated analyze is deterministic.
    storage::clear_dataset(&root, "signals")?;
    let mut groups: HashMap<String, Vec<rustle::model::Signal>> = HashMap::new();
    for x in s.iter().cloned() {
        groups.entry(x.meta.market.clone()).or_default().push(x)
    }
    for (m, x) in groups {
        storage::write(&root, "signals", &m, x[0].meta.exchange_ts, &x)?;
    }
    let audit = analysis::evaluate_with_audit(&s, &t, &b, cfg)?;
    storage::clear_dataset(&root, "evaluation_results")?;
    storage::write(
        &root,
        "evaluation_results",
        "ALL",
        Utc::now(),
        std::slice::from_ref(&audit),
    )?;
    println!(
        "generated {} candidate signals; selected {} rules for untouched validation",
        s.len(),
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
    use std::collections::HashMap;
    let root = PathBuf::from(&cfg.data_root);
    let r = results(cfg)?;
    let ids: Vec<String> = r
        .into_iter()
        .filter(|x| x.passed)
        .filter_map(|x| x.rule_id.split(':').nth(1).map(str::to_owned))
        .collect();
    if ids.is_empty() {
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
    let mut groups: HashMap<String, Vec<rustle::model::PaperTrade>> = HashMap::new();
    for x in p.iter().cloned() {
        groups
            .entry(x.signal.meta.market.clone())
            .or_default()
            .push(x)
    }
    for (m, x) in groups {
        storage::write(&root, "paper_trades", &m, x[0].signal.meta.exchange_ts, &x)?;
    }
    println!("simulated {} paper trades", p.len());
    Ok(())
}

fn alert(cfg: &Config) -> Result<()> {
    let root = PathBuf::from(&cfg.data_root);
    let passed: Vec<String> = results(cfg)?
        .into_iter()
        .filter(|r| r.passed)
        .filter_map(|r| r.rule_id.split(':').nth(1).map(str::to_owned))
        .collect();
    let signals = analysis::build_signals(
        storage::read_all(&root, "trades")?,
        storage::read_all(&root, "orderbooks")?,
        cfg,
    );
    for signal in signals.iter().filter(|s| passed.contains(&s.rule_id)) {
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

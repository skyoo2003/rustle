use chrono::{Duration, TimeZone, Utc};
use rustle::{
    analysis::{self, SignalDetector},
    config::Config,
    model::{Level, Meta, Orderbook, Side, Trade, SCHEMA_VERSION},
    storage, upbit,
};
use std::process::Command;

fn meta(ms: i64) -> Meta {
    Meta {
        schema_version: SCHEMA_VERSION,
        market: "KRW-TEST".into(),
        exchange_ts: Utc.timestamp_millis_opt(ms).unwrap(),
        receive_ts: Utc.timestamp_millis_opt(ms + 1).unwrap(),
    }
}
#[test]
fn subscription_contains_both_public_streams() {
    let s = upbit::subscription(&["KRW-BTC".into()]);
    assert!(s.contains("orderbook") && s.contains("trade") && s.contains("KRW-BTC"));
}
#[test]
fn normalization_preserves_exchange_and_receive_times() {
    let receive = Utc::now();
    let x = serde_json::json!({"type":"trade","code":"KRW-BTC","timestamp":1000,"trade_price":1.0,"trade_volume":2.0,"ask_bid":"BID","sequential_id":3});
    match upbit::normalize(x, receive).unwrap() {
        upbit::Incoming::Trade(t) => {
            assert_eq!(t.meta.exchange_ts.timestamp_millis(), 1000);
            assert_eq!(t.meta.receive_ts, receive);
            assert_eq!(t.side, Side::Buy)
        }
        _ => panic!(),
    }
}
#[test]
fn parquet_round_trip_retains_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let t = Trade {
        meta: meta(1_000),
        price: 10.,
        volume: 2.,
        side: Side::Buy,
        sequential_id: Some(1),
    };
    storage::write(
        dir.path(),
        "trades",
        "KRW-TEST",
        t.meta.exchange_ts,
        std::slice::from_ref(&t),
    )
    .unwrap();
    let read: Vec<Trade> = storage::read_all(dir.path(), "trades").unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].meta.schema_version, SCHEMA_VERSION);
    assert_eq!(read[0].price, 10.);
}

#[test]
fn empty_coverage_cli_prints_a_stable_table_and_zero_progress() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("rustle.toml");
    let cfg = Config {
        data_root: dir.path().join("data").display().to_string(),
        ..Config::default()
    };
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rustle"))
        .args(["coverage", "--config"])
        .arg(&config_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("| UTC date | Trades | Orderbooks |"));
    assert!(stdout.contains("0 of 28 required contiguous UTC dates present"));

    let csv = Command::new(env!("CARGO_BIN_EXE_rustle"))
        .args(["coverage", "--csv", "--config"])
        .arg(&config_path)
        .output()
        .unwrap();
    assert!(csv.status.success());
    assert_eq!(
        String::from_utf8(csv.stdout).unwrap(),
        "date,trades,orderbooks,live_signals,markets,disconnected,stalled,total_gap_ms,longest_gap_ms\n"
    );
    assert!(String::from_utf8(csv.stderr)
        .unwrap()
        .contains("0 of 28 required contiguous UTC dates present"));
}
#[test]
fn wall_cancel_signal_has_evidence() {
    let cfg = Config {
        wall_min_krw: 100.,
        ..Default::default()
    };
    let a = Orderbook {
        meta: meta(1_000),
        total_ask_size: 10.,
        total_bid_size: 10.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 20.,
            ask_price: 11.,
            ask_size: 1.,
        }],
    };
    let b = Orderbook {
        meta: meta(2_000),
        total_ask_size: 1.,
        total_bid_size: 1.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 1.,
            ask_price: 11.,
            ask_size: 1.,
        }],
    };
    let s = analysis::build_signals(&[], &[a, b], &cfg);
    let w = s
        .iter()
        .find(|x| x.signal_type == "wall_disappearance")
        .unwrap();
    assert!(!w.rationale.is_empty() && w.market_snapshot.get("market").is_some());
    assert_eq!(w.market_snapshot["evidence"]["source"], "orderbook");
    assert!(w.market_snapshot["evidence"]["levels"].is_array());
}

#[test]
fn trade_snapshot_includes_trigger_and_rolling_baseline() {
    let cfg = Config {
        candidate: rustle::config::CandidateConfig {
            imbalance_thresholds: vec![],
            large_trade_multiples: vec![3.],
            trade_rate_multiples: vec![],
        },
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    for second in 0..5 {
        detector.on_trade(&Trade {
            meta: meta(second * 1_000),
            price: 10.,
            volume: 1.,
            side: Side::Buy,
            sequential_id: Some(second as u64),
        });
    }
    let signal = detector
        .on_trade(&Trade {
            meta: meta(5_000),
            price: 40.,
            volume: 1.,
            side: Side::Buy,
            sequential_id: Some(5),
        })
        .pop()
        .unwrap();
    assert_eq!(signal.market_snapshot["evidence"]["source"], "trade");
    assert_eq!(
        signal.market_snapshot["evidence"]["rolling_notional_mean"],
        10.0
    );
    assert_eq!(signal.market_snapshot["evidence"]["trigger"]["price"], 40.0);
}

#[test]
fn outcome_marks_incomplete_horizons_without_counting_a_hit() {
    let cfg = Config::default();
    let signal = candidate(0, 0, "only");
    let only_entry = vec![dated_trade(0, 0, 100.)];
    let outcome = analysis::outcome(&signal, &only_entry, &cfg);
    assert!(!outcome.complete);
    assert_eq!(outcome.entry_price, Some(100.));
    assert_eq!(outcome.horizon_price, None);
    assert_eq!(outcome.reached_target, None);
    assert_eq!(outcome.rule_key, "synthetic:only");
}

#[test]
fn streaming_detector_emits_a_snapshot_bearing_signal_when_an_orderbook_arrives() {
    let cfg = Config::default();
    let mut detector = SignalDetector::new(&cfg);
    let book = Orderbook {
        meta: meta(1_000),
        total_ask_size: 1.0,
        total_bid_size: 9.0,
        levels: vec![],
    };

    let signals = detector.on_orderbook(&book);

    assert!(signals.iter().any(|signal| {
        signal.signal_type == "orderbook_imbalance"
            && signal.meta.exchange_ts.timestamp_millis() == 1_000
            && signal.market_snapshot["market"] == "KRW-TEST"
    }));
}

#[test]
fn detector_uses_the_larger_ask_wall() {
    let cfg = Config {
        wall_min_krw: 100.,
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    let high_ask = Orderbook {
        meta: meta(1_000),
        total_ask_size: 10.,
        total_bid_size: 10.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 10.,
            ask_price: 20.,
            ask_size: 10.,
        }],
    };
    let low = Orderbook {
        meta: meta(2_000),
        total_ask_size: 1.,
        total_bid_size: 1.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 1.,
            ask_price: 20.,
            ask_size: 1.,
        }],
    };

    detector.on_orderbook(&high_ask);
    let signals = detector.on_orderbook(&low);

    let wall = signals
        .iter()
        .find(|signal| signal.signal_type == "wall_disappearance")
        .unwrap();
    assert_eq!(wall.direction, Side::Sell);
    assert_eq!(wall.baseline, 200.);
}

#[test]
fn empty_orderbook_resets_wall_without_emitting_a_disappearance() {
    let cfg = Config {
        wall_min_krw: 100.,
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    let wall = Orderbook {
        meta: meta(1_000),
        total_ask_size: 10.,
        total_bid_size: 10.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 20.,
            ask_price: 11.,
            ask_size: 1.,
        }],
    };
    let empty = Orderbook {
        meta: meta(2_000),
        total_ask_size: 0.,
        total_bid_size: 0.,
        levels: vec![],
    };
    let low = Orderbook {
        meta: meta(3_000),
        total_ask_size: 1.,
        total_bid_size: 1.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 1.,
            ask_price: 11.,
            ask_size: 1.,
        }],
    };

    detector.on_orderbook(&wall);
    assert!(detector.on_orderbook(&empty).is_empty());
    assert!(detector.on_orderbook(&low).is_empty());
}

#[test]
fn threshold_signals_fire_on_crossing_and_rearm_after_reset() {
    let cfg = Config::default();
    let mut detector = SignalDetector::new(&cfg);
    let high = Orderbook {
        meta: meta(1_000),
        total_ask_size: 1.,
        total_bid_size: 9.,
        levels: vec![],
    };
    let still_high = Orderbook {
        meta: meta(2_000),
        ..high.clone()
    };
    let neutral = Orderbook {
        meta: meta(3_000),
        total_ask_size: 5.,
        total_bid_size: 5.,
        levels: vec![],
    };
    let high_again = Orderbook {
        meta: meta(4_000),
        ..high.clone()
    };

    assert_eq!(detector.on_orderbook(&high).len(), 2);
    assert!(detector.on_orderbook(&still_high).is_empty());
    assert!(detector.on_orderbook(&neutral).is_empty());
    assert_eq!(detector.on_orderbook(&high_again).len(), 2);
}

#[test]
fn trade_baseline_evicts_expired_notionals_and_rearms_signal() {
    let cfg = Config {
        trade_rate_window_seconds: 10,
        candidate: rustle::config::CandidateConfig {
            imbalance_thresholds: vec![],
            large_trade_multiples: vec![3.],
            trade_rate_multiples: vec![],
        },
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    let trade = |second: i64, value: f64| Trade {
        meta: meta(second * 1_000),
        price: value,
        volume: 1.,
        side: Side::Buy,
        sequential_id: None,
    };

    for second in 0..5 {
        detector.on_trade(&trade(second, 10.));
    }
    let first = detector.on_trade(&trade(5, 40.));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].baseline, 10.);
    detector.on_trade(&trade(20, 100.));
    for second in 21..25 {
        detector.on_trade(&trade(second, 100.));
    }
    let second = detector.on_trade(&trade(25, 400.));
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].baseline, 100.);
}

#[test]
fn streaming_trade_signal_has_a_snapshot_and_is_suppressed_until_reset() {
    let cfg = Config {
        candidate: rustle::config::CandidateConfig {
            imbalance_thresholds: vec![],
            large_trade_multiples: vec![3.],
            trade_rate_multiples: vec![],
        },
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    let trade = |second: i64, value: f64| Trade {
        meta: meta(second * 1_000),
        price: value,
        volume: 1.,
        side: Side::Buy,
        sequential_id: None,
    };
    for second in 0..5 {
        detector.on_trade(&trade(second, 10.));
    }

    let first = detector.on_trade(&trade(5, 40.));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].market_snapshot["market"], "KRW-TEST");
    assert_eq!(first[0].meta.exchange_ts.timestamp_millis(), 5_000);
    assert!(detector.on_trade(&trade(6, 50.)).is_empty());
    assert!(detector.on_trade(&trade(7, 10.)).is_empty());
    assert_eq!(detector.on_trade(&trade(8, 100.)).len(), 1);
}

#[test]
fn trade_rate_signal_fires_on_crossing_and_rearms_after_eviction() {
    let cfg = Config {
        candidate: rustle::config::CandidateConfig {
            imbalance_thresholds: vec![],
            large_trade_multiples: vec![],
            trade_rate_multiples: vec![0.5],
        },
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    let trade = |second: i64| Trade {
        meta: meta(second * 1_000),
        price: 10.,
        volume: 1.,
        side: Side::Buy,
        sequential_id: None,
    };
    for second in 0..5 {
        assert!(detector.on_trade(&trade(second)).is_empty());
    }
    assert_eq!(detector.on_trade(&trade(5)).len(), 1);
    assert!(detector.on_trade(&trade(6)).is_empty());
    assert!(detector.on_trade(&trade(70)).is_empty());
    for second in 71..75 {
        assert!(detector.on_trade(&trade(second)).is_empty());
    }
    assert_eq!(detector.on_trade(&trade(75)).len(), 1);
}

#[test]
fn reset_drops_removed_market_state() {
    let cfg = Config {
        wall_min_krw: 100.,
        ..Default::default()
    };
    let mut detector = SignalDetector::new(&cfg);
    let wall = Orderbook {
        meta: meta(1_000),
        total_ask_size: 10.,
        total_bid_size: 10.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 20.,
            ask_price: 11.,
            ask_size: 1.,
        }],
    };
    detector.on_orderbook(&wall);
    detector.reset();
    let low = Orderbook {
        meta: meta(2_000),
        total_ask_size: 1.,
        total_bid_size: 1.,
        levels: vec![Level {
            bid_price: 10.,
            bid_size: 1.,
            ask_price: 11.,
            ask_size: 1.,
        }],
    };
    assert!(detector.on_orderbook(&low).is_empty());
}

#[test]
fn build_signals_is_deterministic_when_inputs_are_reordered() {
    let cfg = Config::default();
    let trades = vec![
        dated_trade(0, 1, 10.),
        dated_trade(0, 0, 10.),
        dated_trade(0, 2, 10.),
        dated_trade(0, 3, 10.),
        dated_trade(0, 4, 10.),
        dated_trade(0, 5, 100.),
    ];
    let books = vec![
        Orderbook {
            meta: dated_meta(0, 1),
            total_ask_size: 1.,
            total_bid_size: 9.,
            levels: vec![],
        },
        Orderbook {
            meta: dated_meta(0, 0),
            total_ask_size: 5.,
            total_bid_size: 5.,
            levels: vec![],
        },
    ];
    let mut reverse_trades = trades.clone();
    let mut reverse_books = books.clone();
    reverse_trades.reverse();
    reverse_books.reverse();

    assert_eq!(
        serde_json::to_string(&analysis::build_signals(&trades, &books, &cfg)).unwrap(),
        serde_json::to_string(&analysis::build_signals(
            &reverse_trades,
            &reverse_books,
            &cfg
        ))
        .unwrap()
    );
}
#[test]
fn paper_exit_is_first_trade_at_or_after_fifteen_minutes() {
    let cfg = Config::default();
    let signal = rustle::model::Signal {
        meta: meta(0),
        signal_type: "x".into(),
        direction: Side::Buy,
        feature_value: 1.,
        baseline: 0.,
        rationale: "x".into(),
        market_snapshot: serde_json::json!({}),
        rule_id: "ok".into(),
    };
    let tr = vec![
        Trade {
            meta: meta(0),
            price: 100.,
            volume: 1.,
            side: Side::Buy,
            sequential_id: None,
        },
        Trade {
            meta: meta(Duration::minutes(15).num_milliseconds()),
            price: 110.,
            volume: 1.,
            side: Side::Buy,
            sequential_id: None,
        },
    ];
    let p = analysis::paper(&[signal], &tr, &["x:ok".into()], &cfg);
    assert_eq!(p[0].exit_price, 110.);
}

#[test]
fn paper_only_uses_the_validation_qualified_signal_type_and_rule() {
    let cfg = Config::default();
    let mut qualified = candidate(0, 0, "shared");
    qualified.signal_type = "qualified".into();
    let mut unqualified = qualified.clone();
    unqualified.signal_type = "unqualified".into();
    let trades = vec![dated_trade(0, 0, 100.0), dated_trade(0, 15, 101.0)];

    let paper = analysis::paper(
        &[qualified, unqualified],
        &trades,
        &["qualified:shared".into()],
        &cfg,
    );

    assert_eq!(paper.len(), 1);
    assert_eq!(paper[0].signal.signal_type, "qualified");
}

#[test]
fn entry_after_deadline_is_incomplete_even_if_horizon_exists() {
    let cfg = Config::default();
    let signal = candidate(0, 0, "deadline");
    let trades = vec![dated_trade(0, 2, 100.), dated_trade(0, 15, 101.)];
    let outcome = analysis::outcome(&signal, &trades, &cfg);
    assert!(!outcome.complete);
    assert_eq!(outcome.entry_price, None);
    assert_eq!(outcome.reached_target, None);
}

#[test]
fn exact_timestamp_entries_use_the_explicit_sequence_tiebreaker() {
    let cfg = Config::default();
    let signal = candidate(0, 0, "tie");
    let mut first = dated_trade(0, 0, 100.);
    first.sequential_id = Some(1);
    let mut second = dated_trade(0, 0, 200.);
    second.sequential_id = Some(2);
    let horizon = dated_trade(0, 15, 110.);
    let outcome = analysis::outcome(&signal, &[second, horizon, first], &cfg);
    assert_eq!(outcome.entry_price, Some(100.));
}

#[test]
fn sell_paper_trade_uses_a_long_only_benchmark() {
    let cfg = Config::default();
    let mut signal = candidate(0, 0, "sell");
    signal.direction = Side::Sell;
    let trades = vec![dated_trade(0, 0, 100.), dated_trade(0, 15, 90.)];
    let paper = analysis::paper(&[signal], &trades, &["synthetic:sell".into()], &cfg);
    assert!((paper[0].long_only_benchmark_pnl_pct + 10.).abs() < 1e-9);
    assert!(paper[0].gross_pnl_pct > 0.);
}

fn dated_meta(day: i64, minute: i64) -> Meta {
    let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
        + Duration::days(day)
        + Duration::minutes(minute);
    Meta {
        schema_version: SCHEMA_VERSION,
        market: "KRW-TEST".into(),
        exchange_ts: ts,
        receive_ts: ts,
    }
}

fn dated_trade(day: i64, minute: i64, price: f64) -> Trade {
    Trade {
        meta: dated_meta(day, minute),
        price,
        volume: 1.0,
        side: Side::Buy,
        sequential_id: None,
    }
}

fn candidate(day: i64, minute: i64, rule_id: &str) -> rustle::model::Signal {
    rustle::model::Signal {
        meta: dated_meta(day, minute),
        signal_type: "synthetic".into(),
        direction: Side::Buy,
        feature_value: 1.0,
        baseline: 0.0,
        rationale: "test candidate".into(),
        market_snapshot: serde_json::json!({}),
        rule_id: rule_id.into(),
    }
}

fn twenty_eight_days_of_trades() -> Vec<Trade> {
    (0..28)
        .flat_map(|day| {
            [
                dated_trade(day, 0, 100.0),
                dated_trade(day, 15, 101.0),
                dated_trade(day, 30, 101.0),
            ]
        })
        .collect()
}

#[test]
fn evaluation_requires_contiguous_utc_collection_dates() {
    let mut trades = twenty_eight_days_of_trades();
    trades.retain(|t| t.meta.exchange_ts.date_naive() != dated_meta(8, 0).exchange_ts.date_naive());
    let err = analysis::evaluate_with_audit(
        &[],
        &trades,
        &std::collections::BTreeSet::new(),
        &Config::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("missing UTC collection dates"));
}

#[test]
fn tuning_selects_one_rule_without_using_validation_and_is_reproducible() {
    let trades = twenty_eight_days_of_trades();
    let mut signals = Vec::new();
    // Rule a fires at a time which cannot hit; rule b fires at the profitable time.
    for day in 0..14 {
        signals.push(candidate(day, 15, "a"));
        signals.push(candidate(day, 0, "b"));
    }
    for day in 14..28 {
        signals.push(candidate(day, 0, "b"));
    }
    // This is after the final observed horizon and must not count as validation evidence.
    signals.push(candidate(27, 30, "b"));
    let cfg = Config::default();
    let first =
        analysis::evaluate_with_audit(&signals, &trades, &std::collections::BTreeSet::new(), &cfg)
            .unwrap();
    let second =
        analysis::evaluate_with_audit(&signals, &trades, &std::collections::BTreeSet::new(), &cfg)
            .unwrap();
    let mut uncorrected_cfg = cfg.clone();
    uncorrected_cfg.validation.family_wise_correction = false;
    let uncorrected = analysis::evaluate_with_audit(
        &signals,
        &trades,
        &std::collections::BTreeSet::new(),
        &uncorrected_cfg,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
    assert_eq!(
        serde_json::to_string(&first.results).unwrap(),
        serde_json::to_string(&uncorrected.results).unwrap(),
        "one-family results retain the uncorrected interval"
    );
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.candidates.len(), 2);
    assert_eq!(
        first
            .candidates
            .iter()
            .filter(|candidate| candidate.selected)
            .count(),
        1
    );
    assert!(first
        .candidates
        .iter()
        .any(|candidate| candidate.rule_id == "synthetic:a" && !candidate.selected));
    let selected = &first.results[0];
    assert_eq!(selected.rule_id, "synthetic:b");
    assert_eq!(selected.validation_count, 14);
    assert!(!selected.passed, "the default 50-observation gate applies");
}

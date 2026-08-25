use chrono::{Duration, TimeZone, Utc};
use rustle::{
    analysis::{self, SignalDetector},
    config::Config,
    model::{Level, Meta, Orderbook, Side, Trade, SCHEMA_VERSION},
    storage, upbit,
};

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
    let s = analysis::build_signals(vec![], vec![a, b], &cfg);
    let w = s
        .iter()
        .find(|x| x.signal_type == "wall_disappearance")
        .unwrap();
    assert!(!w.rationale.is_empty() && w.market_snapshot.get("market").is_some());
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

    let signals = detector.on_orderbook(book);

    assert!(signals.iter().any(|signal| {
        signal.signal_type == "orderbook_imbalance"
            && signal.meta.exchange_ts.timestamp_millis() == 1_000
            && signal.market_snapshot["market"] == "KRW-TEST"
    }));
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
    let err = analysis::evaluate_with_audit(&[], &trades, &[], &Config::default()).unwrap_err();
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
    let first = analysis::evaluate_with_audit(&signals, &trades, &[], &cfg).unwrap();
    let second = analysis::evaluate_with_audit(&signals, &trades, &[], &cfg).unwrap();
    assert_eq!(
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
    assert_eq!(first.results.len(), 1);
    let selected = &first.results[0];
    assert_eq!(selected.rule_id, "synthetic:b");
    assert_eq!(selected.validation_count, 14);
    assert!(!selected.passed, "the default 50-observation gate applies");
}

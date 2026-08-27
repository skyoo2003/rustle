use chrono::{Duration, NaiveDate, TimeZone, Utc};
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
/// A trade on `date` for `market`, priced so signal fixtures can drive thresholds.
fn trade_on(date: NaiveDate, hour: u32, market: &str, price: f64) -> Trade {
    let exchange_ts = Utc.from_utc_datetime(&date.and_hms_opt(hour, 0, 0).unwrap());
    Trade {
        meta: Meta {
            schema_version: SCHEMA_VERSION,
            market: market.into(),
            exchange_ts,
            receive_ts: exchange_ts,
        },
        price,
        volume: 1.0,
        side: Side::Buy,
        sequential_id: None,
    }
}

fn book_on(date: NaiveDate, hour: u32, market: &str, bid: f64, ask: f64) -> Orderbook {
    let exchange_ts = Utc.from_utc_datetime(&date.and_hms_opt(hour, 0, 0).unwrap());
    Orderbook {
        meta: Meta {
            schema_version: SCHEMA_VERSION,
            market: market.into(),
            exchange_ts,
            receive_ts: exchange_ts,
        },
        total_bid_size: bid,
        total_ask_size: ask,
        levels: vec![Level {
            ask_price: 100.0,
            bid_price: 99.0,
            ask_size: ask,
            bid_size: bid,
        }],
    }
}

#[test]
fn footprint_counts_every_parquet_file_and_its_bytes() {
    let dir = tempfile::tempdir().unwrap();
    for day in [1, 2] {
        let date = NaiveDate::from_ymd_opt(2025, 1, day).unwrap();
        let t = trade_on(date, 12, "KRW-A", 10.0);
        storage::write(dir.path(), "trades", "KRW-A", t.meta.exchange_ts, &[t]).unwrap();
    }

    let (bytes, files) = storage::footprint(dir.path()).unwrap();

    assert_eq!(files, 2);
    assert!(bytes > 0, "two parquet files cannot occupy zero bytes");
    assert_eq!(
        storage::footprint(&dir.path().join("absent")).unwrap(),
        (0, 0)
    );
}

#[test]
fn the_projection_extrapolates_observed_collection_time_to_the_full_gate_window() {
    // The instrument that would have caught a 542 GB collection on day one instead of day 28.
    // Two complete UTC dates = 172,800 seconds; 2 GB and 40k files over that extrapolates
    // to 28 GB and 560k files across the 28-date window.
    let projected = analysis::render_footprint_projection(2_000_000_000, 40_000, 172_800, 28);

    assert!(
        projected.contains("28"),
        "must name the required window: {projected}"
    );
    assert!(
        projected.contains("28.0 GB"),
        "must project total bytes: {projected}"
    );
    assert!(
        projected.contains("560,000"),
        "must project total files: {projected}"
    );

    let nothing_yet = analysis::render_footprint_projection(0, 0, 0, 28);
    assert!(
        !nothing_yet.contains("NaN") && !nothing_yet.contains("inf"),
        "zero observed seconds must not divide by zero: {nothing_yet}"
    );
}

#[test]
fn observed_time_counts_seconds_that_hold_data_and_skips_the_gaps() {
    // Two short sittings a day apart is ~3 minutes of collection, not 22.7 hours.
    // Measuring first-to-last span counts the idle gap as collection and understates
    // the projected footprint by orders of magnitude -- and a real 28-day run has gaps.
    let day = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let next = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let trades = vec![
        trade_on(day, 0, "KRW-A", 10.0),
        trade_on(day, 0, "KRW-A", 11.0),
        trade_on(next, 22, "KRW-A", 12.0),
    ];
    let books = vec![book_on(day, 0, "KRW-A", 900.0, 100.0)];

    let observed = analysis::observed_collection_seconds(&trades, &books);

    // Three records share one second on day one, one record on the next day: 2 seconds held
    // data. The ~46h between them is idle and must not count.
    assert_eq!(observed, 2);
}

#[test]
fn a_partial_date_projects_by_elapsed_time_not_as_a_complete_date() {
    // The real 2026-08-26 partition held 26 MB and 1,810 files across 116 SECONDS.
    // Scaling by "1 of 28 dates" reports 0.7 GB and hides the problem completely;
    // scaling by elapsed collection time reports the ~542 GB that is actually coming.
    let projected = analysis::render_footprint_projection(26_000_000, 1_810, 116, 28);

    assert!(
        projected.contains("542") && projected.contains("GB"),
        "116s of 26 MB must project ~542 GB, got: {projected}"
    );
    assert!(
        projected.contains("37,7"),
        "must project ~37.7M files, got: {projected}"
    );
}

#[test]
fn dataset_dates_lists_partitions_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let days = [
        NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
    ];
    for day in days {
        let t = trade_on(day, 12, "KRW-A", 10.0);
        storage::write(dir.path(), "trades", "KRW-A", t.meta.exchange_ts, &[t]).unwrap();
    }

    let listed = storage::dataset_dates(dir.path(), "trades").unwrap();

    let mut expected = days;
    expected.sort();
    assert_eq!(listed, expected.to_vec());
    assert!(storage::dataset_dates(dir.path(), "absent")
        .unwrap()
        .is_empty());
}

#[test]
fn read_date_returns_only_that_partition() {
    let dir = tempfile::tempdir().unwrap();
    let first = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let second = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    for (day, price) in [(first, 10.0), (second, 20.0)] {
        let t = trade_on(day, 12, "KRW-A", price);
        storage::write(dir.path(), "trades", "KRW-A", t.meta.exchange_ts, &[t]).unwrap();
    }

    let only_second: Vec<Trade> = storage::read_date(dir.path(), "trades", second).unwrap();

    assert_eq!(only_second.len(), 1);
    assert_eq!(only_second[0].price, 20.0);
}

#[test]
fn a_malformed_partition_name_is_an_error_not_a_silent_skip() {
    // Silently skipping an unreadable partition would drop a collection day from the
    // gate window without saying so, which is the one failure mode the gate cannot survive.
    let dir = tempfile::tempdir().unwrap();
    let t = trade_on(
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        12,
        "KRW-A",
        10.0,
    );
    storage::write(dir.path(), "trades", "KRW-A", t.meta.exchange_ts, &[t]).unwrap();
    std::fs::create_dir_all(dir.path().join("trades/date=not-a-date")).unwrap();

    let error = storage::dataset_dates(dir.path(), "trades").unwrap_err();

    assert!(
        error.to_string().contains("not-a-date"),
        "error must name the bad partition, got: {error}"
    );
}

#[test]
fn chunked_and_whole_archive_signal_streams_are_identical() {
    // This is the load-bearing assertion of the whole milestone: reading date-by-date
    // must not change what the gate sees.
    let cfg = Config::default();
    let days: Vec<NaiveDate> = (1..=3)
        .map(|d| NaiveDate::from_ymd_opt(2025, 1, d).unwrap())
        .collect();
    let mut all_trades = vec![];
    let mut all_books = vec![];
    for (i, day) in days.iter().enumerate() {
        for hour in 0..6u32 {
            let swing = if (hour as usize + i).is_multiple_of(2) {
                900.0
            } else {
                100.0
            };
            all_books.push(book_on(*day, hour, "KRW-A", swing, 1000.0 - swing));
            all_trades.push(trade_on(*day, hour, "KRW-A", 100.0 + hour as f64));
        }
    }

    let whole = analysis::build_signals(&all_trades, &all_books, &cfg);

    let mut detector = SignalDetector::new(&cfg);
    let mut chunked = vec![];
    for day in &days {
        let trades: Vec<Trade> = all_trades
            .iter()
            .filter(|t| t.meta.exchange_ts.date_naive() == *day)
            .cloned()
            .collect();
        let books: Vec<Orderbook> = all_books
            .iter()
            .filter(|b| b.meta.exchange_ts.date_naive() == *day)
            .cloned()
            .collect();
        chunked.extend(analysis::feed_signals(&mut detector, &trades, &books));
    }

    assert!(!whole.is_empty(), "fixture must actually produce signals");
    let key = |s: &rustle::model::Signal| {
        (
            s.meta.exchange_ts,
            s.signal_type.clone(),
            s.rule_id.clone(),
            s.rationale.clone(),
        )
    };
    assert_eq!(
        whole.iter().map(key).collect::<Vec<_>>(),
        chunked.iter().map(key).collect::<Vec<_>>()
    );
}

#[test]
fn a_rule_active_at_the_end_of_one_date_does_not_refire_at_the_start_of_the_next() {
    // Resetting the detector per chunk would re-fire every active rule on every market
    // at every midnight -- 28 days of phantom signals straight into the gate.
    let cfg = Config::default();
    let first = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let second = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let mut detector = SignalDetector::new(&cfg);

    let day_one = analysis::feed_signals(
        &mut detector,
        &[],
        &[book_on(first, 23, "KRW-A", 950.0, 50.0)],
    );
    let day_two = analysis::feed_signals(
        &mut detector,
        &[],
        &[book_on(second, 0, "KRW-A", 950.0, 50.0)],
    );

    assert!(
        day_one
            .iter()
            .any(|s| s.signal_type == "orderbook_imbalance"),
        "the imbalance rule must fire on first crossing"
    );
    assert!(
        !day_two
            .iter()
            .any(|s| s.signal_type == "orderbook_imbalance"),
        "a still-active rule must not re-arm across the chunk boundary"
    );
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
    assert!(w.rationale.contains("200 KRW Buy wall fell to 11"));
    assert!(w.rationale.contains("qualifying floor 100 KRW"));
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
    assert!(signal.rationale.contains("aggressive notional 40"));
    assert!(signal.rationale.contains("threshold 3.0x over 5 trades"));
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

    let first = detector.on_orderbook(&high);
    assert_eq!(first.len(), 2);
    assert!(first
        .iter()
        .any(|signal| signal.rationale.contains("threshold 0.40, bid-heavy")));
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
    let first = detector.on_trade(&trade(5));
    assert_eq!(first.len(), 1);
    assert!(first[0].rationale.contains("5 trades in 60s window"));
    assert!(first[0].rationale.contains("threshold 0.5x"));
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
fn paper_exit_is_the_first_trade_at_or_after_the_validated_horizon() {
    let mut cfg = Config::default();
    cfg.validation.horizon_minutes = 30;
    let trades = vec![
        dated_trade(14, 0, 100.0),
        dated_trade(14, 15, 110.0),
        dated_trade(14, 30, 120.0),
    ];

    let report = paper_study(
        &[candidate(14, 0, "ok")],
        &trades,
        &["synthetic:ok".into()],
        &cfg,
    );

    assert_eq!(
        report.trades[0].exit_price, 120.0,
        "paper must hold for validation.horizon_minutes, not a hardcoded 15"
    );
}

#[test]
fn paper_only_uses_the_validation_qualified_signal_type_and_rule() {
    let cfg = Config::default();
    let mut qualified = candidate(14, 0, "shared");
    qualified.signal_type = "qualified".into();
    let mut unqualified = qualified.clone();
    unqualified.signal_type = "unqualified".into();
    let trades = vec![dated_trade(14, 0, 100.0), dated_trade(14, 15, 101.0)];

    let report = paper_study(
        &[qualified, unqualified],
        &trades,
        &["qualified:shared".into()],
        &cfg,
    );

    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].signal.signal_type, "qualified");
}

#[test]
fn paper_simulates_validation_days_only_and_ignores_the_tuning_days() {
    let cfg = Config::default();
    let trades = vec![
        // Tuning days. A rule was *chosen* on these, so its P&L here restates the selection.
        dated_trade(0, 0, 500.0),
        dated_trade(0, 15, 550.0),
        dated_trade(14, 0, 100.0),
        dated_trade(14, 15, 110.0),
    ];
    let signals = vec![candidate(0, 0, "r"), candidate(14, 0, "r")];

    let report = paper_study(&signals, &trades, &["synthetic:r".into()], &cfg);

    assert_eq!(
        report.trades.len(),
        1,
        "the tuning-day signal must not produce a paper trade"
    );
    assert_eq!(
        report.trades[0].signal.meta.exchange_ts.date_naive(),
        NaiveDate::from_ymd_opt(2025, 1, 15).unwrap()
    );
    assert_eq!(report.summary.window_start, Some(validation_window().0));
    assert_eq!(report.summary.window_end, Some(validation_window().1));
    assert!(
        (report.summary.hodl_pnl_pct - (10.0 - ROUND_TRIP_PCT)).abs() < 1e-9,
        "hold must open at the first in-window price, not the first price ever collected"
    );
}

#[test]
fn a_market_holds_one_position_at_a_time_and_counts_the_overlapping_signal() {
    let cfg = Config::default();
    let trades = vec![
        dated_trade(14, 0, 100.0),
        dated_trade(14, 5, 105.0),
        dated_trade(14, 15, 110.0),
        dated_trade(14, 20, 115.0),
    ];
    let signals = vec![candidate(14, 0, "r"), candidate(14, 5, "r")];

    let report = paper_study(&signals, &trades, &["synthetic:r".into()], &cfg);

    assert_eq!(report.trades.len(), 1);
    assert_eq!(report.trades[0].exit_price, 110.0);
    assert_eq!(report.summary.skipped_overlapping, 1);
    assert_eq!(report.summary.trade_count, 1);
}

#[test]
fn sequential_wins_on_one_market_compound_instead_of_summing() {
    let cfg = Config::default();
    let trades = vec![
        dated_trade(14, 0, 100.0),
        dated_trade(14, 15, 110.0),
        dated_trade(14, 30, 121.0),
    ];
    let signals = vec![candidate(14, 0, "r"), candidate(14, 15, "r")];

    let report = paper_study(&signals, &trades, &["synthetic:r".into()], &cfg);

    assert_eq!(report.trades.len(), 2);
    assert_eq!(report.summary.skipped_overlapping, 0);
    let net = 10.0 - ROUND_TRIP_PCT;
    let compounded = ((1.0 + net / 100.0).powi(2) - 1.0) * 100.0;
    assert!(
        (report.summary.cumulative_net_pnl_pct - compounded).abs() < 1e-9,
        "expected {compounded:.6}, got {:.6}",
        report.summary.cumulative_net_pnl_pct
    );
    assert!(
        (report.summary.cumulative_net_pnl_pct - 2.0 * net).abs() > 0.9,
        "a sum of per-trade percentages would have reported {:.4}",
        2.0 * net
    );
}

#[test]
fn markets_are_equally_weighted_whatever_each_sleeve_returned() {
    let cfg = Config::default();
    let trades = vec![
        market_trade("KRW-A", 14, 0, 100.0),
        market_trade("KRW-A", 14, 15, 110.0),
        market_trade("KRW-B", 14, 0, 100.0),
        market_trade("KRW-B", 14, 15, 90.0),
    ];
    let signals = vec![
        market_candidate("KRW-A", 14, 0, "r"),
        market_candidate("KRW-B", 14, 0, "r"),
    ];

    let report = paper_study(&signals, &trades, &["synthetic:r".into()], &cfg);

    assert_eq!(report.summary.market_count, 2);
    // +10% and -10% gross cancel, so what is left is one round trip on each sleeve.
    assert!((report.summary.cumulative_net_pnl_pct + ROUND_TRIP_PCT).abs() < 1e-9);
    assert_eq!(report.markets.len(), 2);
    assert_eq!(report.markets[0].market, "KRW-A");
    assert!((report.markets[0].net_pnl_pct - (10.0 - ROUND_TRIP_PCT)).abs() < 1e-9);
    assert_eq!(report.markets[1].market, "KRW-B");
    assert!((report.markets[1].net_pnl_pct - (-10.0 - ROUND_TRIP_PCT)).abs() < 1e-9);
}

#[test]
fn hold_benchmark_buys_at_window_open_sells_at_window_close_and_pays_one_round_trip() {
    let cfg = Config::default();
    let trades = vec![
        // Outside the validation window; must not anchor the hold benchmark.
        market_trade("KRW-A", 0, 0, 1_000.0),
        market_trade("KRW-B", 0, 0, 2_000.0),
        market_trade("KRW-A", 14, 0, 100.0),
        market_trade("KRW-A", 14, 15, 110.0),
        market_trade("KRW-A", 27, 0, 110.0),
        market_trade("KRW-B", 14, 0, 100.0),
        market_trade("KRW-B", 27, 0, 105.0),
    ];

    let report = paper_study(
        &[market_candidate("KRW-A", 14, 0, "r")],
        &trades,
        &["synthetic:r".into()],
        &cfg,
    );

    let a_hold = 10.0 - ROUND_TRIP_PCT;
    let b_hold = 5.0 - ROUND_TRIP_PCT;
    assert!((report.summary.hodl_pnl_pct - (a_hold + b_hold) / 2.0).abs() < 1e-9);
    assert!((report.markets[0].hodl_pnl_pct - a_hold).abs() < 1e-9);
    assert!((report.markets[1].hodl_pnl_pct - b_hold).abs() < 1e-9);

    // The strategy pays a round trip per trade; hold pays exactly one for the whole window.
    assert_eq!(report.trades.len(), 1);
    assert!((report.trades[0].gross_pnl_pct - 10.0).abs() < 1e-9);
    assert!((report.trades[0].net_pnl_pct - (10.0 - ROUND_TRIP_PCT)).abs() < 1e-9);
    // KRW-B fired no signal, so its equally weighted sleeve sat in cash at 0%.
    assert!((report.summary.cumulative_net_pnl_pct - (10.0 - ROUND_TRIP_PCT) / 2.0).abs() < 1e-9);
    assert!(
        (report.summary.excess_pnl_pct
            - (report.summary.cumulative_net_pnl_pct - report.summary.hodl_pnl_pct))
            .abs()
            < 1e-12
    );
    assert!(
        report.summary.excess_pnl_pct < 0.0,
        "hold wins this fixture"
    );
}

#[test]
fn a_signal_whose_horizon_runs_past_the_last_trade_is_counted_not_traded() {
    let cfg = Config::default();
    let trades = vec![dated_trade(14, 0, 100.0)];

    let report = paper_study(
        &[candidate(14, 0, "r")],
        &trades,
        &["synthetic:r".into()],
        &cfg,
    );

    assert!(report.trades.is_empty());
    assert_eq!(report.summary.incomplete_horizon, 1);
    assert_eq!(report.summary.trade_count, 0);
    assert_eq!(report.summary.win_rate, 0.0);
    assert_eq!(report.summary.cumulative_net_pnl_pct, 0.0);
}

/// Four contiguous UTC dates on one market. Three times a day the book goes bid-heavy and
/// the price steps up 1% fifteen minutes later, then sits flat; every other moment is quiet,
/// so a matched-random entry almost never hits and one rule can clear the gate.
fn gated_fixture(root: &std::path::Path) {
    let market = "KRW-A";
    for day in 0..4 {
        let mut trades = Vec::new();
        let mut books = Vec::new();
        for event in 0..3 {
            let at = event * 120;
            let book = |minute: i64, bid: f64, ask: f64| Orderbook {
                meta: market_meta(market, day, minute),
                total_ask_size: ask,
                total_bid_size: bid,
                levels: vec![],
            };
            books.push(book(at, 9.0, 1.0));
            books.push(book(at + 1, 5.0, 5.0));
            trades.push(market_trade(market, day, at, 100.0));
            trades.push(market_trade(market, day, at + 15, 101.0));
            for flat in (20..120).step_by(5) {
                trades.push(market_trade(market, day, at + flat as i64, 101.0));
            }
        }
        let at = market_meta(market, day, 0).exchange_ts;
        storage::write(root, "trades", market, at, &trades).unwrap();
        storage::write(root, "orderbooks", market, at, &books).unwrap();
    }
}

fn gated_config(data_root: &std::path::Path) -> Config {
    let mut cfg = Config {
        data_root: data_root.display().to_string(),
        ..Config::default()
    };
    // One candidate rule keeps the fixture's family size at one; the other detectors are
    // switched off so the only signals are the book imbalances the fixture stages.
    cfg.candidate.imbalance_thresholds = vec![0.4];
    cfg.candidate.large_trade_multiples = vec![];
    cfg.candidate.trade_rate_multiples = vec![];
    cfg.validation.tuning_days = 2;
    cfg.validation.validation_days = 2;
    cfg.validation.min_validation_signals = 1;
    cfg.validation.bootstrap_iterations = 200;
    cfg
}

fn run_cli(config_path: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustle"))
        .args(args)
        .args(["--config"])
        .arg(config_path)
        .output()
        .unwrap()
}

#[test]
fn paper_output_stays_blocked_until_a_qualified_ruleset_exists() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("rustle.toml");
    let cfg = Config {
        data_root: dir.path().join("data").display().to_string(),
        ..Config::default()
    };
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();

    for args in [&["paper"][..], &["paper", "--csv"][..]] {
        let output = run_cli(&config_path, args);
        assert!(!output.status.success(), "{args:?} must not report a P&L");
        assert!(String::from_utf8(output.stdout).unwrap().is_empty());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("no persisted ruleset"));
    }
}

#[test]
fn a_passed_gate_yields_a_windowed_paper_report_ending_in_a_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("rustle.toml");
    let cfg = gated_config(&dir.path().join("data"));
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
    gated_fixture(&dir.path().join("data"));

    let analyze = run_cli(&config_path, &["analyze"]);
    assert!(analyze.status.success(), "{:?}", analyze);
    let report = run_cli(&config_path, &["report"]);
    assert!(
        String::from_utf8(report.stdout)
            .unwrap()
            .contains("GATE: PASS"),
        "the fixture must clear the gate for this test to mean anything"
    );

    let markdown = run_cli(&config_path, &["paper"]);
    assert!(markdown.status.success(), "{markdown:?}");
    let markdown = String::from_utf8(markdown.stdout).unwrap();
    // Days 1-2 tuned the rule; only days 3-4 are simulated.
    assert!(
        markdown.contains("Window: 2025-01-03–2025-01-04 (validation only) · 1 market ·"),
        "{markdown}"
    );
    assert!(markdown.contains("| KRW-A |"), "{markdown}");
    let verdict = markdown.lines().last().unwrap().to_owned();
    assert!(
        verdict.starts_with("VERDICT: STRATEGY WINS by "),
        "six compounded +0.84% trades must beat a +0.84% hold: {verdict}"
    );
    assert!(
        verdict.ends_with(" over 2025-01-03–2025-01-04"),
        "{verdict}"
    );

    let csv = run_cli(&config_path, &["paper", "--csv"]);
    assert!(csv.status.success(), "{csv:?}");
    let csv = String::from_utf8(csv.stdout).unwrap();
    assert!(csv.starts_with("section,key,trades,"), "{csv}");
    assert!(csv.lines().any(|line| line.starts_with("market,KRW-A,")));
    assert_eq!(csv.lines().last(), Some(verdict.as_str()));

    let summaries: Vec<rustle::model::PaperSummary> =
        storage::read_all(&dir.path().join("data"), "paper_summaries").unwrap();
    assert_eq!(summaries.len(), 1, "the derived dataset is cleared per run");
    assert_eq!(
        summaries[0].window_start,
        Some(NaiveDate::from_ymd_opt(2025, 1, 3).unwrap())
    );
    assert_eq!(summaries[0].market_count, 1);
    assert!(summaries[0].excess_pnl_pct > 0.0);
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
fn a_sell_signal_profits_when_price_falls_and_still_pays_the_round_trip() {
    let cfg = Config::default();
    let mut signal = candidate(14, 0, "sell");
    signal.direction = Side::Sell;
    let trades = vec![dated_trade(14, 0, 100.), dated_trade(14, 15, 90.)];

    let report = paper_study(&[signal], &trades, &["synthetic:sell".into()], &cfg);

    let gross = (100.0 / 90.0 - 1.0) * 100.0;
    assert!((report.trades[0].gross_pnl_pct - gross).abs() < 1e-9);
    assert!((report.trades[0].net_pnl_pct - (gross - ROUND_TRIP_PCT)).abs() < 1e-9);
    // Holding this market over the same window lost money; following the side did not.
    assert!(report.summary.hodl_pnl_pct < 0.0);
    assert!(report.summary.cumulative_net_pnl_pct > 0.0);
    assert!(report.summary.excess_pnl_pct > 0.0);
}

fn market_meta(market: &str, day: i64, minute: i64) -> Meta {
    let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
        + Duration::days(day)
        + Duration::minutes(minute);
    Meta {
        schema_version: SCHEMA_VERSION,
        market: market.into(),
        exchange_ts: ts,
        receive_ts: ts,
    }
}

fn dated_meta(day: i64, minute: i64) -> Meta {
    market_meta("KRW-TEST", day, minute)
}

fn market_trade(market: &str, day: i64, minute: i64, price: f64) -> Trade {
    Trade {
        meta: market_meta(market, day, minute),
        price,
        volume: 1.0,
        side: Side::Buy,
        sequential_id: None,
    }
}

fn dated_trade(day: i64, minute: i64, price: f64) -> Trade {
    market_trade("KRW-TEST", day, minute, price)
}

fn market_candidate(market: &str, day: i64, minute: i64, rule_id: &str) -> rustle::model::Signal {
    rustle::model::Signal {
        meta: market_meta(market, day, minute),
        signal_type: "synthetic".into(),
        direction: Side::Buy,
        feature_value: 1.0,
        baseline: 0.0,
        rationale: "test candidate".into(),
        market_snapshot: serde_json::json!({}),
        rule_id: rule_id.into(),
    }
}

fn candidate(day: i64, minute: i64, rule_id: &str) -> rustle::model::Signal {
    market_candidate("KRW-TEST", day, minute, rule_id)
}

/// Default `[paper]` costs: one round trip of 2 x (5 fee bps + 3 slippage bps).
const ROUND_TRIP_PCT: f64 = 0.16;

/// Days 15-28 of the fixture calendar, i.e. `dated_*` days 14 through 27.
fn validation_window() -> (NaiveDate, NaiveDate) {
    (
        NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2025, 1, 28).unwrap(),
    )
}

fn paper_study(
    signals: &[rustle::model::Signal],
    trades: &[Trade],
    passed: &[String],
    cfg: &Config,
) -> analysis::PaperReport {
    analysis::paper(
        signals,
        trades,
        passed,
        validation_window(),
        Utc.with_ymd_and_hms(2025, 1, 29, 0, 0, 0).unwrap(),
        cfg,
    )
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

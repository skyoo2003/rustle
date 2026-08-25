# Rustle

[![CI](https://github.com/skyoo2003/rustle/actions/workflows/ci.yml/badge.svg)](https://github.com/skyoo2003/rustle/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust 1.98+](https://img.shields.io/badge/rust-1.98%2B-b7410e.svg)](rust-toolchain.toml)

> *rustle*: the sound of leaves. The noise the order book makes before it shows up on a candle.

Project page: <https://skyoo2003.github.io/rustle/>

**Goal: a rigorously validated public-data signal study for Upbit.** Rustle records the order book
as it moves and tests whether explainable signals earn the right to be alerted on or paper traded.

Getting there honestly means earning the right to trade first. So Rustle is built bottom-up: today
it is a local, **public-data-only** CLI that records Upbit `trade` and `orderbook` streams, detects
microstructure signals, and measures whether those signals actually predict anything. Live execution
is the destination; a signal that survives out-of-sample validation is the ticket.

## Roadmap

| Phase | What it means | State |
| --- | --- | --- |
| 1. Collect | Continuous, gap-tolerant capture of public Upbit streams to Parquet | Working |
| 2. Detect | Explainable microstructure rules, each carrying its own evidence | Working |
| 3. Validate | Out-of-sample proof that a rule beats a random baseline | **Current — kill gate** |
| 4. Alert and paper | Qualified live alerts and simulated fills | Gated on phase 3 |
| 5. Live execution | Credentialed order submission | Out of scope |

Phases 4 and 5 do not open until phase 3 passes. That ordering is the design, not caution for its
own sake: an automated trader built on an unvalidated signal is an expensive way to pay fees.

## Disclaimer

**This is not investment advice.** Rustle is, today, a research tool for studying market
microstructure. It publishes no recommendations, makes no performance claims, and its signals are
unvalidated by design — measuring whether they have any predictive power at all is the current work.

- It holds **no API credentials** and contains **no order-submission code**. It reads public market
  data only. That changes only after phase 3, and only behind explicit risk limits and a kill switch.
- Paper-trading output is an upper bound, not a forecast: it ignores slippage, partial fills, and fees.
- Anything you do with this software is at your own risk. See [LICENSE](LICENSE) — provided "AS IS",
  without warranties or conditions of any kind.

## Quickstart

```sh
cargo run -p rustle -- init-config
cargo run -p rustle -- collect --config rustle.toml
# Stop with Ctrl-C; batches are flushed before exit. Restart the same command to resume.
cargo run -p rustle -- analyze --config rustle.toml
cargo run -p rustle -- report  --config rustle.toml
cargo run -p rustle -- paper   --config rustle.toml
```

Requires a recent stable Rust toolchain (edition 2021).

## Data layout

Parquet files are partitioned as `data/<dataset>/date=YYYY-MM-DD/market=KRW-X/`. Each payload carries
a schema version, exchange/receive timestamps, and market. Keep raw `trades` and `orderbooks`: they are
the source for retrospective rule changes.

The MVP procedure is a hard gate:

1. Collect continuously for 28 consecutive UTC dates. Raw `trades` and `orderbooks` are never replaced; sequence anomalies and connection gaps are recorded separately.
2. Run `analyze`. It deterministically regenerates candidate signals and 15-minute outcomes from raw data, uses days 1–14 to tune one rule per signal type, and leaves days 15–28 untouched for validation.
3. A rule passes only with the minimum validation sample and a wholly positive paired-bootstrap CI. Passing rules are persisted as the versioned active ruleset.
4. Only then use `collect --emit-alerts` and `paper`. Alerts are console plus `data/alerts/.../events.jsonl`; paper output replaces prior derived trades and includes fees, slippage, win rate, cumulative net P&L, and HODL comparison.

If no rule passes, alerting and paper trading remain blocked: revise the rules or stop the project. `analyze` replaces its derived `signals`, `signal_outcomes`, evaluation, and active-ruleset files so reruns cannot accumulate stale results.

## Status

Phase 3 of 4 — pre-validation, at the kill gate. If detected signals show no edge over a random
baseline, the signal definitions change or the project stops. Nothing downstream of that gate —
alerting or paper P&L interpretation — means anything until it passes.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first — the project's stage narrows what's useful. Some
categories of change (order submission, credentials, extra exchanges) are on the roadmap but closed
until phase 3 clears, and one (ML-first signals) is out of scope by design.
Participation is under the [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

No credentials are handled. To report a vulnerability, see [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).

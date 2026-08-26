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
is out of scope; a signal that survives out-of-sample validation is the ticket for gated alert and paper-study tools.

## Roadmap

| Phase | What it means | State |
| --- | --- | --- |
| 1. Collect | Continuous, gap-tolerant capture of public Upbit streams to Parquet | Working |
| 2. Detect | Explainable microstructure rules, each carrying its own evidence | Working |
| 3. Validate | Out-of-sample proof that a rule beats a random baseline | **Current — kill gate** |
| 4. Alert and paper | Qualified live alerts and simulated fills | Gated on phase 3 |
| 5. Live execution | Credentialed order submission | Out of scope |

Phases 4 and 5 do not open until phase 3 passes. That ordering is the design: an automated system
built on an unvalidated signal is an expensive way to pay fees.

## Disclaimer

**This is not investment advice.** Rustle is, today, a research tool for studying market
microstructure. It publishes no recommendations and makes no performance claims. Alerts and paper
simulation are available only from a current, persisted validation-qualified ruleset.

- It holds **no API credentials** and contains **no order-submission code**. It reads public market
  data only. That changes only after phase 3, and only behind explicit risk limits and a kill switch.
- Paper-trading output is not a forecast: it applies configured fees and slippage but cannot model
  queue position, partial fills, or real execution.
- Anything you do with this software is at your own risk. See [LICENSE](LICENSE) — provided "AS IS",
  without warranties or conditions of any kind.

## Quickstart

```sh
cargo run -p rustle -- init-config
cargo run -p rustle -- collect --config rustle.toml
cargo run -p rustle -- coverage --config rustle.toml
# Keep collect running for 28 UTC dates; stop with Ctrl-C to flush, then restart to resume.
cargo run -p rustle -- analyze --config rustle.toml
cargo run -p rustle -- report  --config rustle.toml
cargo run -p rustle -- paper   --config rustle.toml
```

Requires a recent stable Rust toolchain (edition 2021).

Collection flushes buffered trades, orderbooks, and live signals every
`flush_interval_seconds` (30 seconds by default), in addition to size-based flushes. If no
WebSocket frame arrives within `stall_timeout_seconds` (90 seconds by default), Rustle flushes the
buffers, records a `stalled` connection event, and reconnects automatically. Both values must be
positive. Run `collect --config rustle.toml` continuously in a terminal or your existing process
manager; this repository intentionally does not install a background service. Run `coverage`
periodically (or `coverage --csv`) to inspect daily counts, markets, disconnects, stalls, connection
gaps, and progress toward the required 28 contiguous UTC dates.

## Data layout

Parquet files are partitioned as `data/<dataset>/date=YYYY-MM-DD/market=KRW-X/`. Each payload carries
a schema version, exchange/receive timestamps, and market. Keep raw `trades` and `orderbooks`: they are
the source for retrospective rule changes.

The MVP procedure is a hard gate:

1. Collect continuously for 28 consecutive UTC dates. Raw `trades` and `orderbooks` are never replaced; sequence anomalies and connection gaps are recorded separately.
2. Run `analyze`. It deterministically regenerates candidate signals and 15-minute outcomes from raw data, uses days 1–14 to tune one rule per signal type, and leaves days 15–28 untouched for validation.
3. A rule passes only when it has the minimum validation sample, the lower bound of its family-wise-corrected paired-bootstrap CI is positive, and its out-of-sample hit rate retains at least 80% of its tuning hit rate. Bonferroni family-wise correction is enabled by default across selected signal types. The complete audit—including every tuning candidate, its matched-random result, selection status, effective alpha, and retention—and a full configuration fingerprint are persisted with the versioned active ruleset.
4. Only then use `collect --emit-alerts`, `alert`, `report`, and `paper`. Each consumes that same persisted audit/ruleset and blocks when its exact configuration fingerprint or validated collection window is stale. Paper entries must occur within 60 seconds by default; output includes fees, slippage, win rate, cumulative net P&L, and a long-only benchmark (buy at each simulated entry and sell at its exit regardless of signal side).

If no rule passes, alerting and paper trading remain blocked: revise the rules or stop the project. `analyze` replaces its derived `signals`, `signal_outcomes`, evaluation, and active-ruleset files so reruns cannot accumulate stale results.

The generated config uses 10,000 bootstrap iterations and enables
`validation.family_wise_correction` by default; set it to `false` only when an explicitly
uncorrected analysis is intended. `validation.entry_max_lag_seconds` must be non-negative.
The Markdown report includes both the selected-rule validation table and the complete tuning
candidate table, and always ends with an explicit `GATE: PASS` or `GATE: FAIL` verdict and reasons.
`report --csv` emits one row per candidate with the fixed schema
`signal_type,rule_id,selected,tuning_start,tuning_end,validation_start,validation_end,train_count,train_hit_rate,train_random_hit_rate,validation_count,validation_hit_rate,random_hit_rate,lift,ci_low,ci_high,retention,passed`;
validation fields are blank for candidates that were not selected.

## Status

Validation is the live kill gate. Alerts and paper simulation work only after `analyze` persists a
current qualified ruleset; a config or collection-window change requires another analysis.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first — the project's stage narrows what's useful. Some
categories of change (order submission, credentials, extra exchanges) are on the roadmap but closed
until phase 3 clears, and one (ML-first signals) is out of scope by design.
Participation is under the [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

No credentials are handled. To report a vulnerability, see [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).

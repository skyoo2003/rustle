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
`flush_interval_seconds` (300 seconds by default). If no WebSocket frame arrives within
`stall_timeout_seconds` (90 seconds by default), Rustle flushes the buffers, records a `stalled`
connection event, and reconnects automatically. Both values must be positive. Run
`collect --config rustle.toml` continuously in a terminal or your existing process manager; this
repository intentionally does not install a background service.

**What the 28-day run costs.** One flush writes one Parquet file per market per dataset, so file
count tracks the flush cadence and not how busy the market is: roughly 300 files per flush interval
across a 20-market universe, or ~500K files over the full window. Parquet is written ZSTD-compressed,
which is worth about 34x on this data — a JSON payload column with a repeated field-name skeleton.
Budget tens of GB, not hundreds. Both numbers scale with `top_market_count`.

There is a five-minute buffer in front of that. Ctrl-C, a stall, a disconnect, and any write error
all flush first, so the exposure is one `flush_interval_seconds` window on an *ungraceful* death
only — SIGKILL, panic, power loss. Lower `flush_interval_seconds` if you would rather trade files
for durability; the cost is roughly linear.

Run `coverage` periodically (or `coverage --csv`) to inspect daily counts, markets, disconnects,
stalls, connection gaps, and progress toward the required 28 contiguous UTC dates. It ends with a
projected footprint:

```text
1 of 28 required contiguous UTC dates present
Footprint: 2.1 GB in 74,318 files over 18.4h of collection → 79.2 GB and 2,834,901 files projected for 28 dates.
```

That projection scales by **elapsed collection time**, not by how many dates have been touched — a
date holding ninety seconds of data is still one present date, and scaling by dates would report a
runaway trajectory as a rounding error. Check it on day one. If the projected figures do not fit the
disk you have, stop and fix that before spending 28 days finding out.

## Data layout

Parquet files are partitioned as `data/<dataset>/date=YYYY-MM-DD/market=KRW-X/`. Each payload carries
a schema version, exchange/receive timestamps, and market. Keep raw `trades` and `orderbooks`: they are
the source for retrospective rule changes.

The MVP procedure is a hard gate:

1. Collect continuously for 28 consecutive UTC dates. Raw `trades` and `orderbooks` are never replaced; sequence anomalies and connection gaps are recorded separately.
2. Run `analyze`. It deterministically regenerates candidate signals and 15-minute outcomes from raw data, uses days 1–14 to tune one rule per signal type, and leaves days 15–28 untouched for validation.
3. A rule passes only when it has the minimum validation sample, the lower bound of its family-wise-corrected paired-bootstrap CI is positive, and its out-of-sample hit rate retains at least 80% of its tuning hit rate. Bonferroni family-wise correction is enabled by default across selected signal types. The complete audit—including every tuning candidate, its matched-random result, selection status, effective alpha, and retention—and a full configuration fingerprint are persisted with the versioned active ruleset.
4. Only then use `collect --emit-alerts`, `alert`, `report`, and `paper`. Each consumes that same persisted audit/ruleset and blocks when its exact configuration fingerprint or validated collection window is stale. Paper entries must occur within 60 seconds by default; see [Paper study](#paper-study) for what the P&L number is and what it is compared against.

If no rule passes, alerting and paper trading remain blocked: revise the rules or stop the project. `analyze` replaces its derived `signals`, `signal_outcomes`, evaluation, and active-ruleset files so reruns cannot accumulate stale results.

## Alerts

Every qualified alert explains the observed condition and threshold, identifies the exact rule, and
includes that rule's out-of-sample validation count, hit rate, matched-random rate, lift, corrected
confidence interval, retention, and tuning/validation dates. Human-readable output is the default;
use `alert --json` for one complete `AlertEvent` JSON object per line.

Live alerts from `collect --emit-alerts` are printed to stdout and appended immediately to
`data/alerts/date=YYYY-MM-DD/events.jsonl`. They remain local by design and can be watched with:

```sh
tail -f data/alerts/date=*/events.jsonl
```

`[alert] cooldown_seconds` defaults to 900. It suppresses repeat delivery for the same market and
rule until the cooldown expires; set it to `0` to disable suppression. Cooldown never suppresses
detection: every detected signal is still persisted in `live_signals`, including signals for which
alert delivery was suppressed or failed.

The generated config uses 10,000 bootstrap iterations and enables
`validation.family_wise_correction` by default; set it to `false` only when an explicitly
uncorrected analysis is intended. `validation.entry_max_lag_seconds` must be non-negative.
The Markdown report includes both the selected-rule validation table and the complete tuning
candidate table, and always ends with an explicit `GATE: PASS` or `GATE: FAIL` verdict and reasons.
`report --csv` emits one row per candidate with the fixed schema
`signal_type,rule_id,selected,tuning_start,tuning_end,validation_start,validation_end,train_count,train_hit_rate,train_random_hit_rate,validation_count,validation_hit_rate,random_hit_rate,lift,ci_low,ci_high,retention,passed`;
validation fields are blank for candidates that were not selected.

## Paper study

`paper` replays the qualified ruleset over **the validation window only** — the untouched
days 15–28. Days 1–14 are the days each rule was *chosen* on, so a profitable P&L there is
the selection procedure working, not the rule.

The capital model is one rule: capital is split equally across every market that traded in
the window, and each market's sleeve compounds independently while holding **at most one
position at a time**. A qualified signal that arrives while its market is already in a
position is skipped and counted, not stacked — summing overlapping percentage returns would
quietly assume you had capital for all of them at once. Positions exit at
`validation.horizon_minutes`, the same horizon the gate validated.

The benchmark is a real hold: buy every market in that same universe at its first in-window
price, sell at its last, equal weight, less **one** round trip of `fee_bps + slippage_bps`.
The strategy pays that round trip on *every* trade. That asymmetry is the comparison.

**Caveat, printed in the report and not only here:** the universe refreshes daily to the top
20 markets by volume, so markets that fell out are absent from later dates. That survivorship
bias favours hold — beating this benchmark is a stronger result than the raw gap suggests,
and losing to it a weaker one.

Both outputs end in an explicit verdict line naming the winner and the gap:

```text
VERDICT: HOLD WINS by 0.790pp over 2025-01-15–2025-01-28
```

`paper --csv` emits one row per rule and per market plus a `total` row, with the fixed schema
`section,key,trades,skipped_overlapping,incomplete_horizon,win_rate,net_pnl_pct,mean_pnl_pct,max_drawdown_pct,hodl_pnl_pct`;
`hodl_pnl_pct` is blank for rule rows because you cannot hold a rule, and the same `VERDICT:`
line trails the rows. The persisted summary also records the window, market count, skipped and
incomplete counts, max drawdown, and the excess over hold, so the headline can be argued with.

Deliberately not modelled — see `docs/adr/0005-paper-capital-model.md`: partial fills, queue
position, cross-market reallocation, confidence-weighted sizing, and stop-losses.

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

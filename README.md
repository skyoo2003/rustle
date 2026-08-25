# Rustle

> *rustle*: the sound of leaves. The noise the order book makes before it shows up on a candle.

A local, **public-data-only** CLI that records Upbit `trade` and `orderbook` streams, detects
microstructure signals, and measures whether those signals actually predict anything.

## Disclaimer

**This is not investment advice.** Rustle is a research tool for studying market microstructure.
It publishes no recommendations, makes no performance claims, and its signals are unvalidated by
design — measuring whether they have any predictive power at all is the entire point of the project.

- It holds **no API credentials** and contains **no order-submission code**. It reads public market data only.
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

`report` deterministically regenerates candidates from raw data, requires 28 consecutive UTC collection
dates, uses days 1–14 for selection and days 15–28 exclusively for validation, and only passes a rule
when it has at least 50 validation signals and a wholly-positive paired-bootstrap CI. `analyze` replaces
its derived signal files and saves an evaluation audit snapshot under `evaluation_results`.

## Status

Pre-validation, and there is a kill gate: if detected signals show no edge over a random baseline,
the signal definitions change or the project stops. Nothing downstream of that — alerting, paper
P&L interpretation — means anything until it passes.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first — the project's stage narrows what's useful, and some
categories of change (order submission, credentials, extra exchanges) are deliberately out of scope.
Participation is under the [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

No credentials are handled. To report a vulnerability, see [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).

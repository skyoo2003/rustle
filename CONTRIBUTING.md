# Contributing

Thanks for looking. Read this first — the project's stage changes what's useful to contribute.

## Project stage

Rustle is **pre-validation**. Whether the signals it detects have any predictive power at all is an
open question, and answering it is the current work. Until that's settled, contributions that add
surface area (new exchanges, new signal families, UI, live order submission) are unlikely to be
merged — not because they're bad, but because they'd be built on an unproven premise.

Most useful right now:

- Reproducibility and correctness fixes in `analyze`/`report` (the evaluation path)
- Bugs in Upbit stream handling: reconnects, gaps, out-of-order or duplicate messages
- Anything that makes the tuning/validation split harder to accidentally violate

## Ground rules

- This is a research tool. It holds no credentials and submits no orders. **Pull requests that add
  order submission, credential handling, or anything that turns output into a recommendation will be
  closed.** See the disclaimer in [README.md](README.md).
- Don't weaken the evaluation gates (28 contiguous UTC days, days 1–14 selection / 15–28 validation,
  minimum 50 validation signals, wholly-positive bootstrap CI) to make a rule pass. Loosening a gate
  is a design change that needs its own discussion, not a line in a feature PR.
- Please don't open issues asking which signals or parameters are profitable. Nobody here knows yet.

## Development

```sh
cargo build
cargo test --all
```

The toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml). If you use rustup, the right
version installs itself; a Homebrew or distro `rustc` ignores the pin, so your local results may
differ slightly from CI.

Before opening a PR, run what CI runs:

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo deny check          # cargo install cargo-deny --locked
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs those on every push and pull
request, on Linux and macOS. `clippy` is `-D warnings`, so a new lint fails the build. Two more jobs
run alongside: `cargo-deny` (advisories, licenses, source trust — see [`deny.toml`](deny.toml)) and a
`beta` job that is allowed to fail and exists only to warn about the next stable release.

`unsafe` is forbidden crate-wide via `[lints.rust]` in `rustle/Cargo.toml`. That is a compile error,
not a guideline.

## Pull requests

- One concern per PR. A formatting sweep mixed into a behavior change is hard to review.
- Behavior changes need a test. `rustle/tests/mvp.rs` shows the style: small fixtures, explicit
  assertions on the property that matters.
- Explain *why* in the PR description, not just what. See
  [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md).
- Reversing a stated design decision — explainable rules before ML, no credentials, no order
  submission — needs its own issue and explicit agreement first, not a quiet code change.

## Expectations

Single maintainer, side project. Review may take a while, and some PRs will be declined on scope
grounds even when the code is good. Opening an issue to discuss before writing a large change will
save you time.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md). Contributions are licensed
under [Apache-2.0](LICENSE), per section 5 of that license.

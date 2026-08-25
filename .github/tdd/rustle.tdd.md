# Rustle TDD Evidence

## Source and scope

Source: [`.claude/prds/rustle.prd.md`](../../.claude/prds/rustle.prd.md), supplied as requirements input on 2026-08-26. It is a PRD rather than an implementation plan. Its collection and measurement requirements were treated as product intent, not authorization to connect to Upbit or run network installers.

The PRD's existing implementation first passed `cargo test --workspace` (7 tests). This cycle closes one safety gap in the Milestone 2 → 4 gate: a qualified rule is identified by both signal type and rule ID, preventing an unqualified signal from becoming eligible by sharing an ID.

## User journeys

1. As an Upbit trader, I want timestamped public market data, so that later signals are auditable.
2. As an analyst, I want tuning and untouched validation against matched random controls, so that the hypothesis is falsifiable.
3. As a trader, I want rationale-bearing alerts only for validation-qualified rules, so that signals are explainable and gated.
4. As an analyst, I want paper trades only from validation-qualified signals, so that simulations exclude unvalidated candidates.

## Task report

| Behavior | Test target | RED evidence | GREEN evidence | Guarantee |
|---|---|---|---|---|
| Exact rule qualification controls paper trading | `rustle/tests/mvp.rs:paper_only_uses_the_validation_qualified_signal_type_and_rule` | `rtk cargo test --workspace paper_` failed: expected 1 paper trade, got 0. Old code compared bare IDs while validation uses `signal_type:rule_id`. | `rtk cargo test --workspace paper_`: 2 passed; `rtk cargo test --workspace`: 8 passed. | `qualified:shared` includes only that type, excluding `unqualified:shared`. |

Checkpoint commits reachable from `main`:

1. `135b391 test: qualify paper rules by signal type` — RED test added and executed.
2. `84280ee fix: preserve signal type in validation gate` — minimal production fix; relevant and full suites GREEN.

## Test specification

| # | Guarantee | Test file | Type | Result | Evidence |
|---|---|---|---|---|---|
| 1 | Paper exits at the first trade at or after 15 minutes. | `rustle/tests/mvp.rs:paper_exit_is_first_trade_at_or_after_fifteen_minutes` | integration | PASS | `rtk cargo test --workspace paper_` |
| 2 | Paper trading requires exact `signal_type:rule_id` qualification. | `rustle/tests/mvp.rs:paper_only_uses_the_validation_qualified_signal_type_and_rule` | integration | PASS | `rtk cargo test --workspace paper_` |

## Coverage and known gaps

`cargo llvm-cov --workspace --summary-only` could not run because `cargo-llvm-cov` is absent. It was not installed because the PRD does not authorize a network installer. CI installs it and currently enforces 50% lines; its documented 50.76% baseline is below this workflow's 80% target. No 80% coverage claim is made.

`rtk cargo fmt --check` and `rtk cargo clippy --workspace --all-targets -- -D warnings` passed.

The PRD's 2+ week live collection and out-of-sample metric remain unverified; its milestones remain pending.

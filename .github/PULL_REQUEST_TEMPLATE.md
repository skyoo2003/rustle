## What

<!-- One or two sentences. What changes? -->

## Why

<!-- The reason this is worth doing. If it fixes a bug, what was the wrong behavior? -->

## Evidence

<!--
For evaluation-path changes (analyze/report/paper), show the before/after numbers, not just "it works".
For stream-handling changes, say how you reproduced the failure.
-->

## Checklist

- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --all-targets -- -D warnings` clean
- [ ] `cargo test --all` passes
- [ ] Behavior change is covered by a test
- [ ] No credentials, no order submission, no output framed as a recommendation (still gated on phase 3 — see the README roadmap)
- [ ] Evaluation gates unchanged (or the change is justified above)

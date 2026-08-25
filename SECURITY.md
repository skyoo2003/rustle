# Security Policy

## Scope

Rustle handles **no exchange credentials and submits no orders**. It reads public Upbit market data
and writes Parquet files to a local directory. There is no server, no network listener, and no
authenticated API surface.

Relevant risks are therefore limited to: dependency vulnerabilities, unsafe handling of untrusted
WebSocket/REST payloads, and local file writes outside the configured data directory.

## Supported versions

Only the latest commit on `main` is supported. There are no maintained release branches.

## Reporting a vulnerability

Open a [private security advisory](https://github.com/skyoo2003/rustle/security/advisories/new).
Please do not open a public issue for an exploitable finding.

Expect an initial response within 7 days. This is a single-maintainer project, so there is no
guaranteed remediation timeline.

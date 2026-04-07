# 0001 — Dagster runs in Docker always

**Status:** Accepted (2026-03-xx)

## Context

Running `dagster dev` directly on macOS triggered repeated OOM crashes at ~60 GB RAM. Root cause: Python's default multiprocessing start method on macOS is `spawn`, which duplicates parent memory per worker. The asset graph has 300+ definitions and Dagster holds them all in each subprocess.

## Decision

**Dagster always runs inside Docker**, which uses Linux's `forkserver` start method. Forked workers share memory via copy-on-write, cutting peak usage by ~75%.

Concretely:
- Local dev: `docker compose up -d` starts `dagster-code`, `dagster-webserver`, `dagster-daemon`
- CLI: `docker compose exec dagster-code dagster asset materialize ...`
- Never run `dagster dev` on the host

## Consequences

- **+** No more OOM crashes
- **+** Local env matches production exactly
- **−** Slightly slower iteration (rebuild on code changes)
- **−** Agents must remember to `docker compose exec` instead of running things directly

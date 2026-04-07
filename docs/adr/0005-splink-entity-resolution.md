# 0005 — Splink 4.0 for probabilistic entity resolution

**Status:** Accepted (2026-03-17, v1.0 shipped)

## Context

55M+ records across NYC open data contain the same people and companies under different spellings, addresses, and abbreviations. Deterministic join keys fail. Options considered:

- Hand-rolled fuzzy matching (rapidfuzz + custom blocking) — brittle, no probabilistic framework
- Dedupe.io — Python-native but slow on 55M records
- **Splink 4.0** — probabilistic ER with a DuckDB backend, scales to 100M+ via blocking and EM training

## Decision

Use **Splink 4.0** with the DuckDB backend. Training model persisted at `models/splink_model_v2.json`. Clustering runs as a Dagster asset (`resolved_entities_asset.py`) and feeds `entity_master_asset.py`.

## Consequences

- **+** Probabilistic scores + explainable match weights
- **+** Runs entirely in DuckDB, no extra infra
- **+** Scales — v1.0 shipped 55M records in 8 phases
- **−** EM training is slow; model is retrained infrequently
- **−** Requires careful blocking rules to avoid O(n²) comparisons

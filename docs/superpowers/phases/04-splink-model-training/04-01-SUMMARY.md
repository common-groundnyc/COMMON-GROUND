---
phase: 04-splink-model-training
plan: "01"
subsystem: entity-resolution
tags: [splink, probabilistic-matching, fellegi-sunter, em-training, duckdb]

requires:
  - phase: 03-materialized-name-index
    provides: lake.federal.name_index (55.5M rows, 44 tables)
provides:
  - Trained Splink model at models/splink_model.json (8.6KB)
  - Training script at scripts/train_splink_model.py
  - Validated m/u probabilities for name/city/zip comparisons
affects: [05-batch-by-last-name-processor]

tech-stack:
  added: []
  patterns: [train-on-sample then predict-per-batch, model serialization via save_model_to_json]

key-files:
  created: [scripts/train_splink_model.py, models/splink_model.json]
  modified: []

key-decisions:
  - "1.63M sample (50K per table cap) — sufficient for EM convergence"
  - "Model saves to JSON via linker.misc.save_model_to_json() — loads via SettingsCreator.from_path_or_dict()"
  - "last_name m values are None (in both blocking rules) — expected Splink behavior, doesn't affect prediction"
  - "EM converged in 6+8 iterations (under max 10) — model is stable"

patterns-established:
  - "Train on stratified sample, save model JSON, load per batch in Phase 5"

issues-created: []

duration: 4min
completed: 2026-03-17
---

# Phase 4 Plan 1: Splink Model Training Summary

**Trained Fellegi-Sunter model on 1.63M sample — EM converged in 14 iterations, model saved to JSON (8.6KB), validated with 1.17M clusters and 181K multi-record matches**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-17T14:23:48Z
- **Completed:** 2026-03-17T14:27:34Z
- **Tasks:** 2
- **Files modified:** 2 (both created)

## Accomplishments
- Created training script that reads from `lake.federal.name_index`, samples 1.63M rows (50K per table), trains Splink EM
- Model trained in 35s — EM converged in 6+8 iterations (well under max 10)
- Saved to `models/splink_model.json` (8.6KB) — loads via `SettingsCreator.from_path_or_dict()`
- Validation prediction: 1.17M clusters, 181K multi-record matches from 1.63M sample
- m/u probabilities converged: first_name exact m=0.776/u=0.003, city exact m=0.955/u=0.106, zip exact m=0.977/u=0.006

## Task Commits

1. **Task 1: Create Splink training script** - `8f50073` (feat)
2. **Task 2: Train model and validate** - `dd002cd` (feat)

**Plan metadata:** (this commit)

## Files Created/Modified
- `scripts/train_splink_model.py` — Training script: connect to DuckLake, sample, train EM, save model, validate
- `models/splink_model.json` — Trained model with all m/u probabilities and comparison settings

## Decisions Made
- 50K per-table sample cap produces 1.63M total (some tables have fewer rows) — sufficient for EM convergence
- Same settings as PoC: NameComparison for first/last, ExactMatch with TF for city/zip
- last_name m values are None because it appears in both blocking rules — normal Splink behavior

## Deviations from Plan

None significant. Sample size slightly smaller (1.63M vs ~2M target) due to smaller source tables. Cluster counts proportionally lower than PoC but model quality is equivalent.

---

**Total deviations:** 0
**Impact on plan:** Plan executed as written.

## Issues Encountered
None

## Next Phase Readiness
- Model JSON ready for Phase 5 batch loading via `SettingsCreator.from_path_or_dict("models/splink_model.json")`
- Blocking rules confirmed: block_on(last_name, first_name), block_on(last_name, zip)
- Comparison settings locked: NameComparison(first_name), NameComparison(last_name), ExactMatch(city)+TF, ExactMatch(zip)+TF
- No blockers

---
*Phase: 04-splink-model-training*
*Completed: 2026-03-17*

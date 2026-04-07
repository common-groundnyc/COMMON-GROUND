# Phase 1 Plan 1: Schema Introspection Summary

**246 tables classified across 12 schemas — 162 (66%) contain entity columns, 588 entity column occurrences across 13 entity types.**

## Accomplishments

- Queried all 12 production schemas via `information_schema.columns` (3 parallel batches)
- Collected row counts for all tables via `list_tables` per schema
- Classified every column against 13 entity types by name pattern matching
- Identified 11 high-value tables missed by query truncation and patched with manual inspection
- Produced comprehensive entity_registry.md (844 lines)

## Key Findings

### Entity Type Scale
| Entity Type | Tables | Total Rows |
|:---|---:|---:|
| zip | 126 | ~294M |
| address | 91 | ~308M |
| bbl | 107 | ~202M |
| person_name | 63 | ~185M |
| bin | 103 | ~176M |
| org_name | 59 | ~116M |
| document_id | 7 | ~104M |
| dos_id | 6 | ~74M |
| complaint_id | 9 | ~70M |
| precinct | 18 | ~55M |
| registration_id | 6 | ~14M |
| license_id | 20 | ~11M |
| camis | 1 | ~297K |

### Top Cross-Reference Tables (most entity types per table)
1. **oath_hearings** — 7 types, 21.6M rows (NOT in any graph!)
2. **n311_service_requests** — 7 types, 21M rows (NOT in any graph!)
3. **dob_permit_issuance** — 7 types, 4M rows (partially in contractor graph)
4. **nys_corporations** — 6 types, 16.7M rows (in corporate web graph at 5% coverage)
5. **hpd_complaints** — 5 types, 16M rows (NOT in ownership graph)
6. **pluto** — 5 types, 859K rows (used for matching only, not as graph vertex table)

### Biggest Graph Expansion Opportunities
- oath_hearings (21.6M) + n311_service_requests (21M) = 42.6M rows of cross-domain entity data not in any graph
- citywide_payroll (6.8M) links government employees by name — connects to campaign donations
- marriage/death/birth certificates (15.8M combined) — massive person name index, partially in resolved_entities

## Files Created/Modified
- `.planning/phases/01-data-audit/entity_registry.md` — complete entity map

## Decisions Made
- 13 entity types defined (person_name, org_name, bbl, address, bin, zip, license_id, precinct, complaint_id, document_id, dos_id, registration_id, camis)
- Skip _dlt_*, v_*, coordinate/list subtables, footnotes tables
- Corrections section added for 11 tables missed by query truncation

## Issues Encountered
- `information_schema.columns` query hit row limits when querying all schemas at once — split into 3 batches by schema group
- 11 large tables (oath_hearings, pluto, citywide_payroll, etc.) were not in the column results — manually inspected and patched

## Commit
- `6a67da2` — feat(01-01): entity registry — 246 tables classified, 588 entity columns

## Next Step
Ready for 01-02-PLAN.md (graph expansion plan — cross-reference registry against current graph coverage)

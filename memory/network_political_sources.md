# `network(type="political")` data sources

`_political_core()` in `infra/duckdb-server/tools/network.py` reads from:

| Source | Lake table | Constant |
|---|---|---|
| NYS BOE campaign finance | `lake.federal.nys_campaign_finance` | `MONEY_NYS_DONATIONS_SQL` |
| FEC contributions | `lake.federal.fec_contributions` | `MONEY_FEC_SQL` |
| NYC campaign finance | `lake.city_government.campaign_contributions` | `MONEY_NYC_DONATIONS_SQL` |
| NYC city contracts | `lake.city_government.contract_awards` | `MONEY_CONTRACTS_SQL` |
| NYS state procurement | `lake.financial.nys_procurement_state` | `MONEY_PROCUREMENT_SQL` |
| Federal awards (USAspending) | `lake.federal.usaspending_contracts` + `_grants` | `MONEY_USASPENDING_SQL` |
| Power map (LittleSis) | `lake.federal.littlesis_entities` + `_relationships` | `MONEY_LITTLESIS_SQL` |
| Pay-to-play graph | `main.graph_pol_entities` (DuckPGQ) | inline in `_pay_to_play_core` |

When adding a new source: write the SQL constant, add a `safe_query` call,
add a render block after the others, add a key under `data["money_trail"]`,
update the `political` description in the `network()` docstring,
and update this memory file.

# Super Tools — MCP Server Consolidation

63 tools → 8. Zero functionality loss. ~2,000 tokens. Most queries: 1 roundtrip.

Inspired by the [metafunctor MCP pattern](https://metafunctor.com/post/2026-03-20-the-mcp-pattern/):
fat tools with smart defaults, schema resource for raw SQL, no discovery roundtrips.

---

## 1. `building(address_or_bbl, view?)`

The place.

**Default** returns profile + violations + landlord info + enforcement combined.
Accepts address OR BBL — auto-resolves addresses via PLUTO.

| view | What it does | Replaces |
|------|-------------|----------|
| `"full"` (default) | Profile + violations + enforcement + landlord portfolio | building_profile, landlord_watchdog, owner_violations |
| `"story"` | Narrative history: era, complaints, milestones | building_story, building_context |
| `"block"` | All buildings on the block, trends over time | block_timeline |
| `"similar"` | Twin/similar buildings via hnsw_acorn vectors | nyc_twins, similar_buildings |
| `"enforcement"` | Multi-agency enforcement: HPD, DOB, FDNY, OATH, facades, boilers | enforcement_web |
| `"history"` | ACRIS transaction chain since 1966 | property_history |
| `"flippers"` | Buy-and-flip detection on this block/area | flipper_detector |

**Absorbs 11 tools**: building_profile, address_lookup, building_story, building_context, block_timeline, nyc_twins, similar_buildings, owner_violations, enforcement_web, property_history, flipper_detector

**Internal queries**: 6-15 depending on view (profile path runs 6-9, enforcement runs 8-10)

---

## 2. `entity(name, role?)`

The person or company. hnsw_acorn name index resolves identity across all 44 tables.

**Default** returns full X-ray: every dataset hit, corporate ties, property, donations.
`role` param narrows to specialized dossiers.

| role | What it does | Replaces |
|------|-------------|----------|
| `"auto"` (default) | Full X-ray across all datasets + Splink cross-ref | entity_xray, person_crossref, fuzzy_entity_search, name_variants |
| `"background"` | Professional/financial: attorney, broker, tax warrants, contractor | due_diligence |
| `"cop"` | CCRB complaints, penalties, federal lawsuits, settlements | cop_sheet |
| `"judge"` | Career history, education, financial disclosures, investments | judge_profile |
| `"vitals"` | Death/marriage/birth records 1855-2017 | vital_records, marriage_search |
| `"top"` | Most cross-referenced people in the lake | top_crossrefs |

**Absorbs 9 tools**: entity_xray, person_crossref, fuzzy_entity_search, name_variants, due_diligence, cop_sheet, judge_profile, vital_records, marriage_search

**Internal queries**: 2-15 depending on role (auto runs 10-15, cop runs 4-5)

---

## 3. `neighborhood(zip_or_coords, view?)`

The area.

**Default** returns portrait + safety + climate combined.
Accepts ZIP code or lat/lng coordinates.

| view | What it does | Replaces |
|------|-------------|----------|
| `"full"` (default) | Portrait + safety + climate + complaints | neighborhood_portrait, safety_report, climate_risk, complaints_by_zip |
| `"compare"` | Side-by-side ZIP comparison (pass multiple zips) | neighborhood_compare |
| `"gentrification"` | Quarterly displacement signals + forecasts | gentrification_tracker |
| `"environment"` | Pollution burden, air quality, EJ analysis + score | environmental_justice |
| `"hotspot"` | H3 hex heatmap around a point | hotspot_map |
| `"area"` | Spatial radius query by lat/lng | area_snapshot |

**Extra params**: `zip_codes` (list, for compare/gentrification), `lat`/`lng`/`radius_m` (for area/hotspot), `category` (for hotspot)

**Absorbs 9 tools**: neighborhood_portrait, neighborhood_compare, area_snapshot, complaints_by_zip, gentrification_tracker, hotspot_map, environmental_justice, climate_risk, safety_report

**Internal queries**: 6-13 depending on view (full runs ~20 across portrait+safety+climate)

---

## 4. `network(name, type?)`

The graph traversal. All 15 current graph tools start from a name and traverse connections.

**Default** infers the network type from the name (landlord if BBL given, corporate if LLC, etc).

| type | What it does | Replaces |
|------|-------------|----------|
| `"landlord"` | Ownership portfolio, violations, slumlord score, graph traversal | landlord_watchdog, landlord_network, ownership_graph |
| `"corporate"` | Shell companies, shared officers, LLC piercing | corporate_web, llc_piercer, shell_detector |
| `"political"` | Campaign donations + lobbying + contracts graph | pay_to_play, money_trail |
| `"property"` | ACRIS transaction network, co-transactors | transaction_network |
| `"contractor"` | DOB permits, co-workers, buildings worked on | contractor_network |
| `"tradewaste"` | BIC hauler connections, shared buildings | tradewaste_network |
| `"officer"` | Shared-command network, misconduct patterns | officer_network |
| `"clusters"` | WCC across all buildings: hidden ownership empires | ownership_clusters |
| `"cliques"` | Local clustering coefficient: tight-knit ownership | ownership_cliques |
| `"worst"` | Ranked slumlord score across all portfolios | worst_landlords |

**Extra params**: `bbl` (for landlord modes), `borough` (for filtering), `depth` (1-6, for graph), `top_n`, `min_buildings`, `min_corps`

**Absorbs 15 tools**: landlord_watchdog, landlord_network, ownership_graph, ownership_clusters, ownership_cliques, worst_landlords, llc_piercer, corporate_web, shell_detector, transaction_network, pay_to_play, money_trail, contractor_network, tradewaste_network, officer_network

**Internal queries**: 1-10 depending on type (watchdog runs 8-10, WCC runs 1)

---

## 5. `school(query, view?)`

The education system. Auto-detects input type.

| input | behavior | Replaces |
|-------|----------|----------|
| School name or ZIP | Search for matching schools | school_search |
| DBN (e.g. "02M475") | Full school report | school_report |
| District number (e.g. "2") | District aggregate report | district_report |
| Multiple DBNs | Side-by-side comparison | school_compare |

**Absorbs 4 tools**: school_search, school_report, school_compare, district_report

**Internal queries**: 1-14 depending on mode (report runs 10-14)

---

## 6. `query(sql)`

Raw data access with annotated schema in the docstring.

The docstring contains:
- All 12 schemas with table names
- Key column mappings (BBL construction, JOIN keys)
- 20+ example queries covering every domain
- Common gotchas (column naming quirks, date formats)

Also handles:
- `DESCRIBE lake.housing.hpd_violations` — column inspection
- `SELECT * FROM duckdb_tables()` — schema discovery
- Keyword detection: "schemas" → list_schemas, "tables in housing" → list_tables

**Absorbs 6 tools**: sql_query, list_schemas, list_tables, describe_table, data_catalog, lake_health

**Internal queries**: 1

---

## 7. `search(text, domain?)`

Unified search across all corpora. Auto-routes by query type.

| domain | What it does | Replaces |
|--------|-------------|----------|
| `"auto"` (default) | Routes to best search method based on query | — |
| `"semantic"` | Vector similarity on complaint/violation descriptions | semantic_search |
| `"text"` | Keyword ILIKE on CFPB complaints + restaurant violations | text_search |
| `"entity"` | Vector similarity on entity names | fuzzy_entity_search |
| `"restaurant"` | Restaurant lookup by name | restaurant_lookup |
| `"resources"` | Food pantries, shelters, legal aid near a ZIP | resource_finder |
| `"commercial"` | Business licenses, permits, storefronts in a ZIP | commercial_vitality |
| `"explore"` | Pre-computed interesting findings | suggest_explorations |

**Extra params**: `zipcode` (for resources/commercial/restaurant), `need` (for resources), `corpus` (for text), `borough` (for restaurant), `limit`

**Absorbs 7 tools**: text_search, semantic_search, suggest_explorations, restaurant_lookup, resource_finder, commercial_vitality, fuzzy_entity_search

**Internal queries**: 1-10 depending on domain

---

## 8. `export(sql, format?)`

Download data as branded spreadsheet.

| format | Output |
|--------|--------|
| `"xlsx"` (default) | Branded XLSX with hyperlinks, percentile color scales |
| `"csv"` | Plain CSV |

**Extra params**: `name` (filename), `sources` (source descriptions for header)

**Absorbs 1 tool**: export_data

---

## Routing Table (for server instructions)

```
BUILDING by address or BBL    → building()
PERSON/COMPANY by name        → entity()
COP by name                   → entity(role="cop")
JUDGE by name                 → entity(role="judge")
NEIGHBORHOOD/ZIP              → neighborhood()
COMPARE neighborhoods         → neighborhood(view="compare")
LANDLORD/OWNER network        → network(type="landlord")
CORPORATE/LLC network         → network(type="corporate")
POLITICAL MONEY               → network(type="political")
SCHOOL by name/ZIP/DBN        → school()
RAW SQL or SCHEMA questions   → query()
SEARCH text/concepts          → search()
EXPLORE what's interesting    → search(domain="explore")
DOWNLOAD/EXPORT               → export()
```

## Token Budget

8 tools x ~250 tokens each = ~2,000 tokens (down from ~15,000)

All tools always visible. No BM25 search needed. No discovery roundtrips.

## Migration Path

1. Build super tools as wrappers calling existing tool functions internally
2. Old tool functions become private `_building_profile()`, `_landlord_watchdog()`, etc.
3. Super tools compose the private functions based on view/role/type params
4. Test with Claude Desktop — verify routing accuracy
5. Remove BM25SearchTransform — all 8 tools fit in context
6. Deploy, monitor PostHog for tool call patterns

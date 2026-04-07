# Super Tools — MCP Server Consolidation Design

**Date**: 2026-03-31
**Status**: Approved
**Scope**: Rewrite 63 MCP tools into 14 modular super tools with full 294-table coverage

---

## Problem

The Common Ground MCP server has 63 `@mcp.tool` functions in a single 13,576-line file. This causes:

- ~15,000 tokens of tool descriptions per session (requires BM25SearchTransform to manage)
- ~150 of 294 lake tables unreachable except via raw SQL
- Two entire schemas (recreation, transportation) with zero tool coverage
- LLM routing errors when selecting from 63 similar tools
- Unmaintainable monolith — every change touches one massive file

## Solution

14 domain-anchored super tools, each in its own module. Full coverage of all 294 tables. ~3,500 tokens total. No BM25SearchTransform needed.

## Design Principles

1. **Domain-first, not operation-first.** Tools map to how users think (buildings, people, neighborhoods), not how the database is organized (schemas, tables).
2. **Smart defaults eliminate round-trips.** Every tool returns the richest view by default. The LLM never needs a discovery call before the real call.
3. **Auto-detection over explicit params.** `building()` accepts address or BBL. `school()` auto-detects DBN vs ZIP vs name. Fewer LLM decisions = fewer errors.
4. **`Literal` enums with per-value descriptions.** Validated by Microsoft Research (BiasBusters, ICLR 2026) as the highest-accuracy schema pattern. Each value explained in prose with meaning, not just restated.
5. **Plaintext descriptions, 3-5 sentences.** Per Anthropic official guidance: no markdown, no formatting. Cover WHAT / WHEN / RETURNS / WHEN-NOT / DEFAULT. Parameter descriptions: 1 sentence + inline example.
6. **`Field(examples=[...])`** on all input params. Pydantic renders to JSON Schema `"examples"`. Low token cost, potential accuracy boost.
7. **Clean break.** Old tool names cease to exist. No aliases, no backwards compatibility.

---

## The 14 Tools

### 1. `building(identifier, view?)`

The place. Any NYC building by address or BBL.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | Profile + violations + enforcement + landlord portfolio | building_profile, address_lookup, owner_violations |
| `"story"` | Narrative history — construction era, complaints, milestones | building_story, building_context |
| `"block"` | All buildings on the tax block with comparative trends | block_timeline |
| `"similar"` | Twin buildings via Lance vector similarity | nyc_twins, similar_buildings |
| `"enforcement"` | Multi-agency timeline — HPD, DOB, FDNY, OATH, facades, boilers | enforcement_web |
| `"history"` | ACRIS transaction chain since 1966 | property_history |
| `"flippers"` | Buy-and-flip detection on this block/area | flipper_detector |

**Input auto-detection**: Address string → resolved to BBL via PLUTO. 10-digit numeric string → used as BBL directly.

**Tables covered**: pluto, hpd_violations, hpd_complaints, hpd_jurisdiction, hpd_registration_contacts, hpd_litigations, dob_ecb_violations, dob_complaints, dob_permit_issuance, dob_violations, dob_safety_violations, dob_safety_facades, dob_safety_boiler, dob_now_build_filings, dob_application_owners, acris_master, acris_legals, acris_parties, acris_pp_parties, acris_pp_legals, acris_pp_master, aep_buildings, tax_lien_sales, tax_exemptions, property_valuation, designated_buildings, sro_buildings, conh_pilot, underlying_conditions, fdny_violations, fdny_vacate_list, dwelling_registrations, hpd_repair_vacate, j51_historical, property_abatement_detail, property_assessment, property_charges, nyc_water_charges, rpie_noncompliance, rolling_sales, ll44_income_rent, n311_service_requests (building-filtered), restaurant_inspections (building-filtered).

**Internal queries**: 6-15 depending on view.

---

### 2. `entity(name, role?)`

The person or company. Lance vector index resolves identity across 44 tables.

| role | What it returns | Absorbs |
|------|----------------|---------|
| `"auto"` (default) | Full X-ray across all datasets + Splink cross-ref | entity_xray, person_crossref, fuzzy_entity_search, name_variants |
| `"background"` | Professional/financial — attorney, broker, tax warrants, contractor | due_diligence |
| `"cop"` | CCRB complaints, penalties, federal lawsuits, settlements | cop_sheet |
| `"judge"` | Career history, education, financial disclosures, investments | judge_profile |
| `"vitals"` | Death/marriage/birth records 1855-2017 | vital_records, marriage_search |
| `"top"` | Most cross-referenced people in the lake (ignores name param) | top_crossrefs |

**Tables covered**: All tables in the entity NAME_TABLES registry (44 tables across all schemas), plus: nys_attorney_registrations, nys_re_brokers, nys_tax_warrants, nys_child_support_warrants, nys_notaries, nys_contractor_registry, nys_non_responsible, ccrb_complaints, ccrb_allegations, ccrb_officers, ccrb_penalties, nypd_officer_profile, nypd_ccrb_officers_current, police_settlements_538, cl_nypd_cases_sdny, cl_nypd_cases_edny, cl_judges, cl_judges__positions, cl_judges__educations, cl_financial_disclosures, cl_financial_disclosures__investments, cl_financial_disclosures__gifts, nys_death_index, nyc_marriage_index, marriage_certificates_1866_1937, marriage_licenses_1950_2017, birth_certificates_1855_1909, death_certificates_1862_1948, resolved_entities, nys_re_appraisers, citywide_payroll, civil_service_active, doing_business_people, doing_business_entities, nys_corporations, nys_entity_addresses, propublica_nonprofits, littlesis_entities.

**Internal queries**: 2-15 depending on role.

---

### 3. `neighborhood(location, view?)`

The area. Demographics, environment, displacement, and local context.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | Portrait + environment + complaints combined | neighborhood_portrait |
| `"compare"` | Side-by-side ZIP comparison (pass multiple ZIPs) | neighborhood_compare |
| `"gentrification"` | Quarterly displacement signals + forecasts | gentrification_tracker |
| `"environment"` | Pollution burden, air quality, environmental justice score | environmental_justice |
| `"hotspot"` | H3 hex heatmap around a point (requires lat,lng) | hotspot_map |
| `"area"` | Spatial radius query — buildings, 311, restaurants nearby | area_snapshot |
| `"restaurants"` | Restaurant search by name in area + inspection history | restaurant_lookup |

**Input**: ZIP code, or `"lat,lng"` for spatial views, or multiple ZIPs comma-separated for compare/gentrification.

**Tables covered**: acs_zcta_demographics, pluto, n311_service_requests, affordable_housing, evictions, restaurant_inspections, air_quality, waste_transfer, e_designations, street_trees, flood_vulnerability, heat_vulnerability, dsny_tonnage, lead_service_lines, ll84_energy_2023, ll84_energy_monthly, oer_cleanup, hyperlocal_temp, nys_solar, recycling_rates, lep_population, broadband_adoption, h3_index, subway_stops, spatial views.

**Internal queries**: 6-20 depending on view.

---

### 4. `network(name, type?)`

The graph traversal. Traces connections across all relationship types. The `type` parameter is a **filter**, not a router — default behavior fans out across all edge types and returns everything found. Specify `type` only to narrow results.

| type | What it filters to | Absorbs |
|------|-------------------|---------|
| `"all"` (default) | All connections found across all edge types | — |
| `"ownership"` | Property ownership, landlord portfolio, slumlord score | landlord_watchdog, landlord_network, ownership_graph |
| `"corporate"` | Shell companies, shared officers, LLC piercing | corporate_web, llc_piercer, shell_detector |
| `"political"` | Campaign donations + lobbying + contracts | pay_to_play, money_trail |
| `"property"` | ACRIS transaction co-transactors | transaction_network |
| `"contractor"` | DOB permits, co-workers, buildings worked on | contractor_network |
| `"tradewaste"` | BIC hauler connections, shared buildings | tradewaste_network |
| `"officer"` | Shared-command network, misconduct patterns | officer_network |
| `"clusters"` | WCC — hidden ownership empires | ownership_clusters |
| `"cliques"` | Local clustering coefficient — tight-knit ownership | ownership_cliques |
| `"worst"` | Ranked slumlord score across all portfolios | worst_landlords |

**Extra params**: `depth` (1-6), `borough`, `top_n`, `min_buildings`, `min_corps`.

**Tables covered**: All 44 graph_* tables in main schema, plus source lake tables they're built from. Also: nys_campaign_contributions, nys_campaign_expenditures, nys_campaign_filer_ref, nys_lobbyist_disclosures_pre2019, nys_lobbyist_public_monies, cfb_intermediaries, cfb_enforcement_penalties, cfb_enforcement_audits, cfb_offyear_contributions, doing_business_contributions, lobbyist_fundraising, fec_contributions, nys_campaign_finance, nys_corp_all_filings, nys_corp_constituents, nys_corp_name_history, nys_daily_corp_filings, bic_denied_companies, bic_wholesale_markets, usaspending_contracts, usaspending_grants.

**Internal queries**: 1-10 depending on type.

---

### 5. `school(query)`

Education. Auto-detects input type — no view parameter needed.

| Input | Behavior | Absorbs |
|-------|----------|---------|
| School name or ZIP | Search for matching schools | school_search |
| DBN (e.g. "02M475") | Full school report | school_report |
| District number (e.g. "2") | District aggregate report | district_report |
| Multiple DBNs comma-separated | Side-by-side comparison | school_compare |

**Tables covered**: ela_results, math_results, school_safety, quality_reports, demographics_2020, chronic_absenteeism, class_size, survey_parents, survey_teachers, survey_students, regents_results, specialized_hs_tests, discharge_reporting, attendance_2015, attendance_2018, demographics_2013, demographics_2018, demographics_2019, quality_early_childhood, quality_elem_middle, quality_high_schools, school_programs, specialized_hs_summary, capacity_projects, school_cafeteria_inspections, urban_school_directory, college_scorecard.

**Internal queries**: 1-14 depending on mode.

---

### 6. `semantic_search(query, domain?)`

Concept search. Lance-powered hybrid search (vector + FTS) across all corpora.

| domain | What it searches | Absorbs |
|--------|-----------------|---------|
| `"auto"` (default) | Routes to best corpus based on query content | — |
| `"complaints"` | 311 service requests + CFPB financial complaints | text_search (financial corpus) |
| `"violations"` | HPD violations, restaurant inspections, OATH hearings | text_search (restaurant corpus) |
| `"entities"` | Fuzzy name matching across all entity tables via Lance | fuzzy_entity_search |
| `"explore"` | Pre-computed interesting findings from the lake | suggest_explorations |

**Lance integration**: Uses DuckDB 1.5.1 Lance extension for `lance_hybrid_search()` — combines vector similarity with full-text search in a single SQL query. Results can JOIN directly with DuckLake tables.

**Tables covered**: n311_service_requests (descriptions), cfpb_complaints_ny (narratives), hpd_violations (descriptions), restaurant_inspections (violations), oath_hearings (charge descriptions), plus Lance datasets: entity_name_embeddings, description_embeddings, catalog_embeddings.

**Internal queries**: 1-5 depending on domain.

---

### 7. `query(input, mode?, format?)`

Direct data access. SQL execution, schema discovery, catalog search, and data export.

| mode | What it does | Absorbs |
|------|-------------|---------|
| `"sql"` (default) | Execute read-only SQL against the lake | sql_query |
| `"catalog"` | Search table names/descriptions by keyword | data_catalog |
| `"schemas"` | List all schemas (input ignored) | list_schemas |
| `"tables"` | List tables in a schema (input = schema name) | list_tables |
| `"describe"` | Show columns and types (input = "schema.table") | describe_table |
| `"health"` | Lake freshness and row counts (input ignored) | lake_health |
| `"admin"` | DDL: CREATE OR REPLACE VIEW only | sql_admin |

| format | Output |
|--------|--------|
| `"text"` (default) | Formatted results for conversation |
| `"xlsx"` | Branded Excel with hyperlinks + percentile heatmaps — returns download URL |
| `"csv"` | Plain CSV — returns download URL |

**Tables covered**: All 294 tables via SQL. The escape hatch for custom analysis.

**Internal queries**: 1.

---

### 8. `safety(location, view?)`

Crime, crashes, and public safety data.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | Precinct report — crimes, arrests, trends, year-over-year | safety_report |
| `"crashes"` | Motor vehicle collisions — injuries, fatalities, contributing factors | — (new) |
| `"shootings"` | Shooting incidents — location, time, demographics | — (new) |
| `"force"` | Use of force incidents — subjects, officers, type of force | — (new) |
| `"hate"` | Hate crime incidents — bias type, offense, trends | — (new) |
| `"summons"` | Criminal court summons — charges, demographics | — (new) |

**Input**: Precinct number, ZIP code, or `"lat,lng"` coordinates.

**Tables covered**: nypd_complaints_historic, nypd_complaints_ytd, nypd_arrests_historic, nypd_arrests_ytd, shootings, criminal_court_summons, hate_crimes, motor_vehicle_collisions, nys_crashes, nys_crash_vehicles, use_of_force_incidents, use_of_force_subjects, use_of_force_officers, discipline_charges, discipline_summary, mv_crime_precinct.

**Internal queries**: 4-12 depending on view.

---

### 9. `health(location, view?)`

Public health data — disease rates, inspections, facilities, environmental health.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | Community health profile — CDC PLACES metrics + key indicators | — (new) |
| `"covid"` | COVID cases, outcomes, wastewater surveillance by ZIP | — (new) |
| `"facilities"` | Hospitals, clinics, medicaid providers near a location | — (new) |
| `"inspections"` | Cooling towers, drinking water tanks, rodent inspections | — (new) |
| `"environmental"` | Lead exposure, asthma, air quality health impacts | — (new) |

**Input**: ZIP code, neighborhood name, or `"lat,lng"` coordinates.

**Tables covered**: cdc_places, covid_by_zip, covid_outcomes, wastewater_sarscov2, health_facilities, medicaid_providers, cooling_tower_inspections, drinking_water_tanks, drinking_water_tank_inspections, rodent_inspections, ed_flu_visits, hiv_aids_annual, hiv_aids_by_neighborhood, leading_causes_of_death, sparcs_discharges_2020-2024, asthma_ed, lead_children, community_health_survey, beach_water_samples, pregnancy_mortality, epa_echo_facilities (all borough tables).

**Internal queries**: 3-10 depending on view.

---

### 10. `legal(query, view?)`

Courts, litigation, settlements, and hearings.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | All legal proceedings matching a name or entity | — (new) |
| `"litigation"` | Civil lawsuits — parties, claims, outcomes | — (new) |
| `"settlements"` | Settlement payments by the city — amounts, agencies, reasons | — (new) |
| `"hearings"` | OATH administrative hearings and trials — charges, penalties | — (new) |
| `"inmates"` | Daily inmate population — facility, demographics | — (new) |
| `"claims"` | Claims against NYC — injuries, property damage, amounts | — (new) |

**Input**: Person name, agency name, or keyword.

**Tables covered**: civil_litigation, settlement_payments, oath_hearings, oath_trials, daily_inmates, nyc_claims_report, nys_coelig_enforcement, ccrb_penalties, vacate_relocation, emergency_repair_hwo, emergency_repair_omo.

**Internal queries**: 2-8 depending on view.

---

### 11. `civic(query, view?)`

City government operations — contracts, permits, budget, jobs, events.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"contracts"` (default) | City and federal contracts — vendors, amounts, agencies | — (new) |
| `"permits"` | Film permits, liquor licenses, event permits | — (new) |
| `"jobs"` | City employment — payroll, civil service titles, job postings | — (new) |
| `"budget"` | Revenue budget, IDA projects, procurement | — (new) |
| `"events"` | Permitted events, city record notices | — (new) |

**Input**: Company name, agency name, keyword, or location.

**Tables covered**: contract_awards, ddc_vendor_payments, covid_emergency_contracts, usaspending_contracts, usaspending_grants, film_permits, permitted_events, nys_liquor_authority, citywide_payroll, civil_service_active, civil_list, civil_service_titles, nyc_jobs, revenue_budget, nys_ida_projects, nys_procurement_local, nys_procurement_state, nys_cosmetology_licenses, nys_elevator_licenses, city_record, laus, oews, qcew_annual, occupational_projections, sustainability_compliance, nys_dos_nonprofits, equity_nyc, license_applications, passport_vendors, idnyc_applications.

**Internal queries**: 2-8 depending on view.

---

### 12. `transit(location, view?)`

Transportation — parking, transit ridership, traffic, infrastructure.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | Transportation overview for an area | — (new) |
| `"parking"` | Parking violations — plates, locations, fines | — (new) |
| `"ridership"` | MTA subway + bus + ferry ridership trends | — (new) |
| `"traffic"` | Traffic volume, speed data, collision hotspots | — (new) |
| `"infrastructure"` | Potholes, pavement quality, pedestrian ramps | — (new) |

**Input**: ZIP code, address, station name, license plate, or `"lat,lng"` coordinates.

**Tables covered**: parking_violations, mta_daily_ridership, mta_entrances, traffic_volume, pothole_orders, pavement_rating, pedestrian_ramps, ferry_ridership, nrel_alt_fuel_stations.

**Internal queries**: 3-10 depending on view.

---

### 13. `services(location, view?)`

Social services — childcare, shelters, food, benefits, community resources.

| view | What it returns | Absorbs |
|------|----------------|---------|
| `"full"` (default) | All available services near a location | resource_finder |
| `"childcare"` | Licensed childcare programs + inspection results | — (new) |
| `"food"` | Food pantries, farmers markets, SNAP centers, Shop Healthy stores | — (new) |
| `"shelter"` | DHS shelters, census, daily population | — (new) |
| `"benefits"` | Benefits centers, Access NYC programs, IDNYC | — (new) |
| `"legal_aid"` | Legal aid, family justice centers, know-your-rights | — (new) |
| `"community"` | Community gardens, community orgs, literacy programs, wifi | — (new) |

**Input**: ZIP code, address, or `"lat,lng"` coordinates. Optional `need` parameter for natural language filtering.

**Tables covered**: dycd_program_sites, snap_centers, benefits_centers, family_justice_centers, nys_child_care, childcare_inspections, childcare_inspections_current, childcare_programs, community_gardens, community_orgs, farmers_markets, literacy_programs, dhs_daily_report, dhs_shelter_census, access_nyc, shop_healthy, snap_access_index, know_your_rights, language_secret_shopper, moia_interpretation, hud_public_housing_developments, hud_public_housing_buildings, housing_connect_buildings, housing_connect_lotteries.

**Internal queries**: 3-10 depending on view.

---

### 14. `suggest(topic?)`

Onboarding and discovery. Guides users and LLMs to the right tool.

| Input | What it returns |
|-------|----------------|
| No params | Lake overview + 5 curated interesting findings + example tool calls |
| Topic string | Relevant tools + example calls for that topic |
| Ambiguous question | Tool recommendation with ready-to-use call syntax |

**Examples**:
- `suggest()` → "This lake has 294 tables covering NYC housing, safety, education, health... Try: `building('350 5th Ave')` or `entity('Steven Croman')`"
- `suggest("corruption")` → "Try `network(type='political')` for campaign money trails, `entity(role='cop')` for misconduct, `legal()` for settlements against the city"
- `suggest("I want to know about rent")` → "For a specific building: `building('address')`. For displacement trends: `neighborhood('10003', view='gentrification')`. For affordable housing: `services(view='community')`"

**Never returns raw data.** Always returns tool recommendations with example calls. Uses Lance catalog embeddings for semantic topic matching.

---

## Server Instructions (Routing Table)

```
Common Ground -- NYC open data lake. 294 tables, 60M+ rows, 14 schemas.

ROUTING -- pick the FIRST match:
* Address, BBL, or "this building"       -> building()
* Person or company name                  -> entity()
* Cop by name                             -> entity(role="cop")
* Judge by name                           -> entity(role="judge")
* Birth/death/marriage records            -> entity(role="vitals")
* ZIP code or "this neighborhood"         -> neighborhood()
* Compare ZIPs                            -> neighborhood(view="compare")
* Restaurants in an area                  -> neighborhood(view="restaurants")
* Landlord portfolio or slumlord score    -> network(type="landlord")
* LLC piercing or shell companies         -> network(type="corporate")
* Campaign donations or lobbying          -> network(type="political")
* Worst landlords ranking                 -> network(type="worst")
* School by name, DBN, or ZIP            -> school()
* "Find complaints about X" or concepts  -> semantic_search()
* Crime, crashes, shootings               -> safety()
* Health data, COVID, hospitals           -> health()
* Court cases, settlements, hearings      -> legal()
* City contracts, permits, jobs, budget   -> civic()
* Parking tickets, MTA, traffic           -> transit()
* Childcare, shelters, food pantries      -> services()
* "What can I explore?" or unsure         -> suggest()
* Custom SQL or "what tables have X"      -> query()
* Download or export data                 -> query(format="xlsx")

WORKFLOWS -- chain tools for deep investigations:
* Landlord investigation: building() -> entity() -> network(type="landlord")
* Follow the money: entity() -> network(type="political") -> civic(view="contracts")
* School comparison: school("02M475,02M001")
* Health equity: health("10456") -> services("10456") -> neighborhood("10456")

This is NYC-only data. Do not query for national/federal statistics.
```

---

## File Structure

```
infra/duckdb-server/
+-- mcp_server.py              # ~250 lines: FastMCP init, lifespan, middleware, registration
+-- tools/
|   +-- __init__.py            # Exports all 14 tool functions
|   +-- building.py            # ~800 lines
|   +-- entity.py              # ~800 lines
|   +-- neighborhood.py        # ~600 lines
|   +-- network.py             # ~800 lines
|   +-- school.py              # ~400 lines
|   +-- semantic_search.py     # ~300 lines
|   +-- query.py               # ~400 lines
|   +-- safety.py              # ~500 lines
|   +-- health.py              # ~400 lines (new domain)
|   +-- legal.py               # ~350 lines (new domain)
|   +-- civic.py               # ~400 lines (new domain)
|   +-- transit.py             # ~400 lines (new domain)
|   +-- services.py            # ~400 lines (new domain)
|   +-- suggest.py             # ~200 lines (new)
+-- shared/
|   +-- types.py               # BBL, ZIP, NAME annotated types + Literal enums
|   +-- db.py                  # CursorPool, execute, safe_query, reconnect
|   +-- formatting.py          # make_result, format_text_table
|   +-- lance.py               # vector_expand_names, lance_route_entity, hybrid_search
|   +-- graph.py               # require_graph, graph cache load/save/freshness
|   +-- validation.py          # validate_sql, UNSAFE_SQL, UNSAFE_FUNCTIONS
+-- middleware/
|   +-- response_middleware.py
|   +-- citation_middleware.py
|   +-- freshness_middleware.py
|   +-- percentile_middleware.py
|   +-- posthog_middleware.py
+-- spatial.py                 # Existing H3 helpers
+-- entity.py                  # Existing fuzzy SQL helpers (rename to shared/ in migration)
+-- formatters.py              # Existing
+-- percentiles.py             # Existing
+-- embedder.py                # Existing
+-- xlsx_export.py             # Existing
+-- csv_export.py              # Existing
+-- constants.py               # Existing
```

---

## Migration Strategy

### Phase 1: Scaffold (no behavior change)
Create `tools/`, `shared/`, `middleware/` directories. Move existing utility code into `shared/` with zero logic changes. Middleware files into `middleware/`. Verify server still starts and all 63 tools work.

### Phase 2: Extract tools (one at a time, test after each)
Order: least dependencies first.

1. `school` — 4 tools, self-contained, no Lance/graph deps
2. `query` — routing to existing functions, absorbs export
3. `suggest` — new tool, no migration needed, build from scratch
4. `semantic_search` — depends on Lance, isolated
5. `building` — 11 tools, depends on shared/db and address resolution
6. `neighborhood` — absorbs search children (restaurants, resources → services)
7. `entity` — depends on Lance routing + graph
8. `safety` — new domain, queries existing public_safety tables
9. `health` — new domain, queries existing health tables + orphans
10. `legal` — new domain, queries city_government + public_safety legal tables
11. `civic` — new domain, queries city_government operational tables
12. `transit` — new domain, queries transportation tables
13. `services` — new domain, queries social_services + housing_connect tables
14. `network` — 15 tools, depends on graph tables, most complex, last

For each extraction:
1. Create `tools/toolname.py` with the super tool function
2. Move relevant private helpers from monolith
3. Replace direct duckdb access with shared/db imports
4. Add view/role/type dispatch logic
5. Wire up in mcp_server.py
6. Delete old functions from monolith
7. Test against known queries

### Phase 3: Slim mcp_server.py
After all 14 tools extracted, delete everything except lifespan, registration, middleware, prompts, and custom routes. Target: ~250 lines.

### Phase 4: DuckDB 1.5.1 upgrade
- Switch Lance Python client to DuckDB Lance extension for semantic_search
- Enable `lance_hybrid_search()` for combined vector + FTS
- Benefit from non-blocking checkpointing, curl HTTP backend
- Test `operator_memory_limit` if available

### Phase 5: DuckPGQ pilot (optional)
- Test `INSTALL duckpgq FROM community` with DuckLake tables
- If it works, rewrite network() graph traversals with SQL/PGQ syntax
- Keep CTE implementations as fallback

---

## Tool Schema Pattern

Based on Anthropic official guidance, Microsoft Research BiasBusters (ICLR 2026), and Pydantic v2 JSON Schema rendering.

### Rules (Anthropic official)

1. **All descriptions are plaintext.** No markdown, no formatting, no newlines.
2. **Parameter descriptions: 1 sentence + inline example.** 10-25 words. e.g. `"Street address or 10-digit BBL, e.g. '350 5th Ave' or '1000670001'"`
3. **Enum values: restate in prose WITH meaning.** Not just the values but what each one does.
4. **Use `Field(examples=[...])`.** Pydantic renders this to JSON Schema `"examples": [...]`. Low cost, potential accuracy boost.
5. **No `oneOf` for mutually exclusive params.** Use optional params + instruction in tool description, or a single param with format auto-detection.
6. **Tool descriptions: 3-5 sentences plaintext.** Cover WHAT / WHEN / RETURNS / WHEN-NOT / DEFAULT.
7. **Prompt-engineer descriptions like onboarding a new hire.** Make specialized query formats, niche terminology, and resource relationships explicit.

### Canonical tool pattern

```python
@mcp.tool(annotations=READONLY)
def building(
    identifier: Annotated[str, Field(
        description="Street address or 10-digit BBL, e.g. '350 5th Ave, Manhattan' or '1000670001'",
        examples=["350 5th Ave, Manhattan", "1000670001", "123 Main St, Brooklyn"],
    )],
    view: Annotated[
        Literal["full", "story", "block", "similar", "enforcement", "history", "flippers"],
        Field(
            default="full",
            description="'full' returns profile + violations + enforcement + landlord. 'story' returns narrative history with complaint arcs. 'block' returns all buildings on the tax block. 'similar' finds twin buildings via vectors. 'enforcement' returns multi-agency timeline. 'history' returns ACRIS transactions since 1966. 'flippers' detects buy-and-flip activity.",
        )
    ] = "full",
    ctx: Context = None,
) -> ToolResult:
    """Look up any NYC building by address or BBL. Returns violations, enforcement actions, landlord info, and property history. Use for any question about a specific building, address, or property. Do NOT use for person lookups (use entity), neighborhood questions without a specific address (use neighborhood), or ownership network traversal (use network). Default returns the full profile with violations and landlord portfolio."""
```

### Canonical enum description format

Each enum value gets: `'value' does/returns X.` Single sentence, action verb, no redundant restating of the value name.

```python
# GOOD: explains what each value DOES
"'landlord' traces ownership portfolio and slumlord score. 'corporate' pierces LLCs and finds shared officers. 'political' maps campaign donations and lobbying connections."

# BAD: just restates values
"'landlord' for landlord networks. 'corporate' for corporate networks. 'political' for political networks."
```

### Canonical multi-format input

Single param with inline examples for each format. Auto-detection happens server-side.

```python
# GOOD: one param, multiple formats, examples
query: Annotated[str, Field(
    description="School name, DBN, ZIP, or district number, e.g. 'Stuyvesant', '02M475', '10003', or '2'",
    examples=["Stuyvesant", "02M475", "10003", "district 2", "02M475,02M001"],
)]

# BAD: separate params for each format
name: str | None = None
dbn: str | None = None
zip_code: str | None = None
```

### Canonical error response

Errors suggest next steps so the LLM can self-correct without a round-trip.

```python
# GOOD
raise ToolError("No building found for '123 Fake St'. Try a full NYC address with borough, e.g. '350 5th Ave, Manhattan', or a 10-digit BBL.")

# BAD
raise ToolError("Address not found")
```

### What the LLM sees (JSON Schema output)

A `Literal["full", "story", "block"]` with `Field(description=..., examples=[...])` renders to:

```json
{
  "view": {
    "type": "string",
    "enum": ["full", "story", "block"],
    "default": "full",
    "description": "'full' returns profile + violations + enforcement + landlord. 'story' returns narrative history with complaint arcs. 'block' returns all buildings on the tax block.",
    "examples": ["full", "enforcement"]
  }
}
```

---

## Complete Tool Descriptions & Examples

Exact docstrings and Field descriptions for all 14 tools. Following Anthropic rules: plaintext only, 3-5 sentences for tool description, 1 sentence + inline example for parameter descriptions, `Field(examples=[...])` on all inputs.

### 1. building

```python
def building(
    identifier: Annotated[str, Field(
        description="Street address or 10-digit BBL, e.g. '350 5th Ave, Manhattan' or '1000670001'",
        examples=["350 5th Ave, Manhattan", "1000670001", "123 Main St, Brooklyn", "2039720033"],
    )],
    view: Annotated[
        Literal["full", "story", "block", "similar", "enforcement", "history", "flippers"],
        Field(
            default="full",
            description="'full' returns profile + violations + enforcement + landlord. 'story' returns narrative history with complaint arcs. 'block' returns all buildings on the tax block. 'similar' finds twin buildings via vectors. 'enforcement' returns multi-agency timeline. 'history' returns ACRIS transactions since 1966. 'flippers' detects buy-and-flip activity.",
        )
    ] = "full",
) -> ToolResult:
    """Look up any NYC building by address or BBL. Returns violations, enforcement actions, landlord info, and property history. Use for any question about a specific building, address, or property. Do NOT use for person lookups (use entity), neighborhood questions without a specific address (use neighborhood), or ownership network traversal (use network). Default returns the full profile with violations and landlord portfolio."""
```

### 2. entity

```python
def entity(
    name: Annotated[str, Field(
        description="Person or company name, fuzzy matched. Partial names and misspellings OK, e.g. 'Steven Croman', 'BLACKSTONE', 'J Smith'",
        examples=["Steven Croman", "Barton Perlbinder", "BLACKSTONE GROUP", "Jane Smith"],
    )],
    role: Annotated[
        Literal["auto", "background", "cop", "judge", "vitals", "top"],
        Field(
            default="auto",
            description="'auto' returns full X-ray across all datasets. 'background' returns professional and financial records. 'cop' returns CCRB complaints, penalties, and federal lawsuits. 'judge' returns career history and financial disclosures. 'vitals' returns death, marriage, and birth records 1855-2017. 'top' returns most cross-referenced people in the lake.",
        )
    ] = "auto",
) -> ToolResult:
    """Look up any person or company across all NYC datasets. Lance vector index resolves identity across 44 tables. Returns every dataset hit, corporate ties, property records, and donations. Use for any question about a specific person, company, landlord, or organization. Do NOT use for building lookups by address (use building) or relationship traversal (use network). Default returns the full cross-reference across all datasets."""
```

### 3. neighborhood

```python
def neighborhood(
    location: Annotated[str, Field(
        description="NYC ZIP code, lat/lng coordinates, or multiple ZIPs comma-separated, e.g. '10003', '40.7128,-74.0060', '10003,11201,11215'",
        examples=["10003", "11201", "40.7128,-74.0060", "10003,11201,11215"],
    )],
    view: Annotated[
        Literal["full", "compare", "gentrification", "environment", "hotspot", "area", "restaurants"],
        Field(
            default="full",
            description="'full' returns demographics + environment + complaints combined. 'compare' compares multiple ZIPs side-by-side. 'gentrification' shows quarterly displacement signals. 'environment' shows pollution burden and air quality. 'hotspot' shows H3 hex heatmap around coordinates. 'area' shows everything within a radius of coordinates. 'restaurants' searches restaurants by name in the area.",
        )
    ] = "full",
    name: Annotated[str, Field(
        description="Restaurant or resource name filter, only used with 'restaurants' view, e.g. 'Wo Hop', 'joes pizza'",
        examples=["Wo Hop", "joes pizza", "halal cart"],
    )] = "",
    radius_m: Annotated[int, Field(
        description="Search radius in meters for 'area' and 'hotspot' views, 50-2000, e.g. 500",
        ge=50, le=2000,
    )] = 500,
) -> ToolResult:
    """Explore any NYC neighborhood by ZIP code or coordinates. Returns demographics, safety, climate risk, complaints, and local services. Use for any area-level question about quality of life, comparisons, gentrification, or restaurant searches. Do NOT use for specific building questions (use building) or person lookups (use entity). Default returns the full neighborhood portrait with demographics and environment."""
```

### 4. network

```python
def network(
    name: Annotated[str, Field(
        description="Person name, company name, or BBL to start traversal, e.g. 'Steven Croman', 'BLACKSTONE', '1000670001'",
        examples=["Steven Croman", "BLACKSTONE GROUP", "1000670001", "NACHMAN PLUMBING"],
    )],
    type: Annotated[
        Literal["all", "ownership", "corporate", "political", "property", "contractor",
                "tradewaste", "officer", "clusters", "cliques", "worst"],
        Field(
            default="all",
            description="Filter which relationship types to include. 'all' returns every connection found. 'ownership' limits to property and landlord edges. 'corporate' limits to LLC and shared-officer edges. 'political' limits to donations, lobbying, and contracts. 'property' limits to ACRIS transaction co-parties. 'contractor' limits to DOB permit networks. 'tradewaste' limits to BIC hauler connections. 'officer' limits to shared-command misconduct. 'clusters' finds hidden ownership empires via WCC. 'cliques' finds tight-knit ownership groups. 'worst' ranks slumlord scores.",
        )
    ] = "all",
    depth: Annotated[int, Field(
        description="Graph traversal hops 1-6, higher means wider network but slower, e.g. 2",
        ge=1, le=6,
    )] = 2,
    borough: Annotated[str, Field(
        description="Borough filter, empty for all boroughs, e.g. 'Brooklyn', 'Manhattan'",
        examples=["Brooklyn", "Manhattan", "Bronx"],
    )] = "",
    top_n: Annotated[int, Field(
        description="Number of results for ranked views like worst and clusters, 1-100, e.g. 25",
        ge=1, le=100,
    )] = 25,
) -> ToolResult:
    """Trace connections across ownership, corporate, and influence networks in NYC. Fans out across all relationship types by default and returns every connection found. Use when asking how entities are connected: landlord portfolios, shell company webs, political money trails, contractor networks. Do NOT use for simple entity lookup (use entity) or building profile (use building). Default returns all connections found for the name across all edge types."""
```

### 5. school

```python
def school(
    query: Annotated[str, Field(
        description="School name, DBN, ZIP, or district number. Auto-detected, e.g. 'Stuyvesant', '02M475', '10003', '2', '02M475,02M001'",
        examples=["Stuyvesant", "02M475", "10003", "district 2", "02M475,02M001"],
    )],
) -> ToolResult:
    """Look up NYC public schools by name, DBN, ZIP code, or district number. Returns test scores, chronic absenteeism, class size, and survey results. Auto-detects input type: DBN gives a full report, ZIP searches nearby schools, district number gives an aggregate, multiple DBNs compare side-by-side. Do NOT use for neighborhood-level analysis (use neighborhood) or person lookups (use entity)."""
```

### 6. semantic_search

```python
def semantic_search(
    query: Annotated[str, Field(
        description="Natural language search query, finds similar content even with different words, e.g. 'apartments with mold problems', 'noise complaints late night'",
        examples=["apartments with mold problems", "noise complaints late night", "identity theft credit report", "rat infestation kitchen"],
    )],
    domain: Annotated[
        Literal["auto", "complaints", "violations", "entities", "explore"],
        Field(
            default="auto",
            description="'auto' routes to the best corpus based on query content. 'complaints' searches 311 and CFPB complaints. 'violations' searches HPD, restaurant, and OATH violations. 'entities' does fuzzy name matching across all tables. 'explore' returns pre-computed interesting findings.",
        )
    ] = "auto",
    limit: Annotated[int, Field(
        description="Maximum results 1-100, e.g. 20",
        ge=1, le=100,
    )] = 20,
) -> ToolResult:
    """Search across all NYC data using natural language, powered by Lance vector index and DuckDB hybrid search. Finds similar content even when exact keywords do not match. Use when looking for complaints, violations, or entities by concept rather than exact lookup. Do NOT use for specific building or person lookups (use building or entity) or structured SQL queries (use query). Default auto-routes to the best corpus based on the query."""
```

### 7. query

```python
def query(
    input: Annotated[str, Field(
        description="SQL query, table name, schema name, or search keyword depending on mode, e.g. 'SELECT * FROM lake.housing.hpd_violations LIMIT 10' or 'eviction' or 'housing'",
        examples=[
            "SELECT borough, COUNT(*) FROM lake.housing.hpd_violations GROUP BY borough",
            "SELECT contributor, amount FROM lake.city_government.campaign_contributions WHERE contributor ILIKE '%blackstone%'",
            "SELECT zipcode, COUNT(*) as cnt FROM lake.social_services.n311_service_requests WHERE complaint_type ILIKE '%noise%' GROUP BY zipcode ORDER BY cnt DESC",
            "SELECT * FROM lake.public_safety.motor_vehicle_collisions WHERE crash_date > '2025-01-01' LIMIT 20",
            "eviction",
            "housing",
        ],
    )],
    mode: Annotated[
        Literal["sql", "catalog", "schemas", "tables", "describe", "health", "admin"],
        Field(
            default="sql",
            description="'sql' executes read-only SQL against the lake. 'catalog' searches table names and descriptions by keyword. 'schemas' lists all schemas. 'tables' lists tables in a schema. 'describe' shows columns and types for a table. 'health' shows lake freshness and row counts. 'admin' executes DDL (CREATE OR REPLACE VIEW only).",
        )
    ] = "sql",
    format: Annotated[
        Literal["text", "xlsx", "csv"],
        Field(
            default="text",
            description="'text' returns formatted results for conversation. 'xlsx' returns a branded Excel download URL with hyperlinks and heatmaps. 'csv' returns a plain CSV download URL.",
        )
    ] = "text",
) -> ToolResult:
    """Execute SQL queries, discover schemas, search the catalog, and export data from the NYC data lake. 294 tables across 14 schemas in the 'lake' database using lake.schema.table format. Common schemas: housing (violations, permits, ACRIS sales), public_safety (arrests, complaints), education (test scores), health (inspections, COVID), city_government (contracts, payroll). Start with limit=20 for exploration. Do NOT use for building, entity, or neighborhood lookups since the domain tools are faster and richer. Use query for custom analysis the domain tools do not cover."""
```

### 8. safety

```python
def safety(
    location: Annotated[str, Field(
        description="Precinct number, ZIP code, or lat/lng coordinates, e.g. '75', '10003', '40.7128,-74.0060'",
        examples=["75", "10003", "40.6782,-73.9442", "14"],
    )],
    view: Annotated[
        Literal["full", "crashes", "shootings", "force", "hate", "summons"],
        Field(
            default="full",
            description="'full' returns precinct crime report with arrests and trends. 'crashes' returns motor vehicle collisions with injuries and contributing factors. 'shootings' returns shooting incidents by location and time. 'force' returns use of force incidents. 'hate' returns hate crime incidents by bias type. 'summons' returns criminal court summons by charge type.",
        )
    ] = "full",
) -> ToolResult:
    """Look up crime, crash, and public safety data for any NYC location. Returns arrest trends, shooting incidents, collision data, and use-of-force reports. Use for any question about crime, safety, crashes, or policing in an area. Do NOT use for individual officer misconduct (use entity with role='cop') or building-specific complaints (use building). Default returns the full precinct report with year-over-year trends."""
```

### 9. health

```python
def health(
    location: Annotated[str, Field(
        description="ZIP code, neighborhood name, or lat/lng coordinates, e.g. '10003', 'East Harlem', '40.7128,-74.0060'",
        examples=["10003", "10456", "East Harlem", "40.7128,-74.0060"],
    )],
    view: Annotated[
        Literal["full", "covid", "facilities", "inspections", "environmental"],
        Field(
            default="full",
            description="'full' returns community health profile with CDC PLACES metrics. 'covid' returns COVID cases, outcomes, and wastewater surveillance. 'facilities' returns hospitals, clinics, and medicaid providers nearby. 'inspections' returns cooling tower, drinking water, and rodent inspection results. 'environmental' returns lead exposure, asthma rates, and air quality impacts.",
        )
    ] = "full",
) -> ToolResult:
    """Look up public health data for any NYC location. Returns disease rates, hospital discharge data, facility locations, and environmental health indicators. Use for any question about health conditions, COVID data, health services, or environmental health risks in an area. Do NOT use for social services like childcare or food pantries (use services) or restaurant inspections (use neighborhood with view='restaurants'). Default returns the full community health profile."""
```

### 10. legal

```python
def legal(
    query: Annotated[str, Field(
        description="Person name, agency name, or keyword to search legal proceedings, e.g. 'NYPD', 'Steven Croman', 'lead paint'",
        examples=["NYPD", "Steven Croman", "lead paint", "DOE", "excessive force"],
    )],
    view: Annotated[
        Literal["full", "litigation", "settlements", "hearings", "inmates", "claims"],
        Field(
            default="full",
            description="'full' returns all legal proceedings matching the query. 'litigation' returns civil lawsuits with parties and outcomes. 'settlements' returns settlement payments by the city with amounts and reasons. 'hearings' returns OATH administrative hearings and trials. 'inmates' returns daily inmate population data. 'claims' returns claims filed against NYC.",
        )
    ] = "full",
) -> ToolResult:
    """Search NYC legal proceedings, court records, and city settlements. Returns litigation, OATH hearings, settlement payments, and claims against the city. Use for any question about lawsuits, court cases, administrative hearings, or city payouts. Do NOT use for individual person background checks (use entity with role='background') or police misconduct records (use entity with role='cop'). Default returns all legal proceedings matching the query."""
```

### 11. civic

```python
def civic(
    query: Annotated[str, Field(
        description="Company name, agency name, keyword, or location, e.g. 'AECOM', 'DOT', 'film permit Brooklyn'",
        examples=["AECOM", "DOT", "film permit Brooklyn", "sanitation", "lifeguard"],
    )],
    view: Annotated[
        Literal["contracts", "permits", "jobs", "budget", "events"],
        Field(
            default="contracts",
            description="'contracts' returns city and federal contracts with vendors and amounts. 'permits' returns film permits, liquor licenses, and event permits. 'jobs' returns city employment data, payroll, and civil service titles. 'budget' returns revenue budget, IDA projects, and procurement data. 'events' returns permitted events and city record notices.",
        )
    ] = "contracts",
) -> ToolResult:
    """Look up NYC city government operations including contracts, permits, employment, budget, and events. Use for any question about city spending, vendor contracts, film permits, city jobs, or public events. Do NOT use for political donation networks (use network with type='political') or building permits (use building with view='enforcement'). Default returns contract data matching the query."""
```

### 12. transit

```python
def transit(
    location: Annotated[str, Field(
        description="ZIP code, address, station name, license plate, or lat/lng coordinates, e.g. '10003', 'Times Square', 'ABC1234', '40.7128,-74.0060'",
        examples=["10003", "Times Square", "ABC1234", "Grand Central", "40.7128,-74.0060"],
    )],
    view: Annotated[
        Literal["full", "parking", "ridership", "traffic", "infrastructure"],
        Field(
            default="full",
            description="'full' returns transportation overview for the area. 'parking' returns parking violations by plate or location. 'ridership' returns MTA subway, bus, and ferry ridership trends. 'traffic' returns traffic volume and speed data. 'infrastructure' returns pothole orders, pavement quality, and pedestrian ramp data.",
        )
    ] = "full",
) -> ToolResult:
    """Look up transportation data for any NYC location. Returns parking violations, MTA ridership, traffic volumes, and infrastructure condition. Use for any question about parking tickets, subway ridership, traffic patterns, or street conditions. Do NOT use for motor vehicle crashes (use safety with view='crashes') or building-level data (use building). Default returns the full transportation overview for the area."""
```

### 13. services

```python
def services(
    location: Annotated[str, Field(
        description="ZIP code, address, or lat/lng coordinates, e.g. '10003', '123 Main St, Brooklyn', '40.7128,-74.0060'",
        examples=["10003", "10456", "123 Main St, Brooklyn", "40.7128,-74.0060"],
    )],
    view: Annotated[
        Literal["full", "childcare", "food", "shelter", "benefits", "legal_aid", "community"],
        Field(
            default="full",
            description="'full' returns all available services near the location. 'childcare' returns licensed childcare programs and inspection results. 'food' returns food pantries, farmers markets, and SNAP centers. 'shelter' returns DHS shelters and daily population. 'benefits' returns benefits centers and Access NYC programs. 'legal_aid' returns legal aid and family justice centers. 'community' returns community gardens, organizations, and literacy programs.",
        )
    ] = "full",
    need: Annotated[str, Field(
        description="Natural language filter for what you need, e.g. 'halal food', 'infant daycare', 'free wifi'",
        examples=["halal food", "infant daycare", "free wifi", "immigration legal help"],
    )] = "",
) -> ToolResult:
    """Find social services near any NYC location including childcare, food assistance, shelters, benefits, legal aid, and community resources. Use for any question about where to get help, find services, or access community resources. Do NOT use for health facilities (use health with view='facilities') or school programs (use school). Default returns all available services near the location."""
```

### 14. suggest

```python
def suggest(
    topic: Annotated[str, Field(
        description="Topic to explore, or empty for a general overview, e.g. 'corruption', 'rent stabilization', 'environmental justice'",
        examples=["corruption", "rent stabilization", "environmental justice", "worst landlords", "school quality"],
    )] = "",
) -> ToolResult:
    """Discover what the NYC data lake can tell you and get guided to the right tools. Returns curated findings, tool recommendations, and ready-to-use example calls. Use when unsure which tool fits, when exploring a new topic, or when onboarding a new user. This tool never returns raw data, only recommendations. For actual data retrieval, use the recommended tool instead."""
```

---

## Testing Strategy

### Per-tool smoke tests
Each tool module gets `tests/test_toolname.py` with:
- Known-good inputs returning results
- Every enum value exercised once
- Edge cases: malformed input, unknown identifiers, empty results
- Error messages include next-step suggestions

### Integration tests
Hit the running server via MCP client:
- One call per tool with default params
- Verify ToolResult has content + structured_content
- Verify middleware fires (citations, percentiles in output)

### Routing accuracy test
Feed 30 natural language queries to Claude with the 14 tools. Verify correct tool selection. Tests descriptions and routing table, not SQL.

### Coverage verification
Query every schema in the lake. Verify each table is reachable through at least one super tool (not just query).

---

## Token Budget

14 tools x ~250 tokens each = ~3,500 tokens.

Down from ~15,000 (63 tools). 77% reduction.

All 14 tools always visible. No BM25SearchTransform. No CodeMode. No discovery round-trips.

Well under the 20-25 tool degradation threshold identified by benchmarks.

---

## Research Sources

### Tool design and routing accuracy
- BiasBusters (Microsoft Research, ICLR 2026): Description semantic alignment is the strongest predictor of tool selection
- Anthropic Advanced Tool Use: "Consolidate related operations into fewer tools"
- Anthropic Define Tools docs: Plaintext descriptions, 3-4 sentences minimum, inline examples canonical
- Anthropic `input_examples`: 72% to 90% accuracy improvement on complex parameter handling
- Google MCP Toolbox Style Guide: 5-8 tools per toolset, 40 max
- Pamela Fox (Microsoft): Literal/Enum + heuristic descriptions = highest routing accuracy

### Tool count benchmarks
- Boundary Benchmark (March 2026): Degradation starts at 25-50 tools, cliff at ~107
- GitHub Copilot: 40 -> 13 tools, +2-5pp benchmark improvement
- Layered.dev: 106 tools = 54,600 tokens consumed per session

### Schema and description patterns
- Anthropic official: All descriptions plaintext, no markdown
- Anthropic official: Parameter descriptions 1 sentence + inline example, 10-25 words
- Anthropic official: Enum values restated in prose WITH meaning in description
- Pydantic v2: `Field(examples=[...])` renders to JSON Schema `"examples": [...]`
- Anthropic official: `oneOf` not recommended, use auto-detection instead
- Anthropic engineering blog: "Prompt-engineer descriptions like onboarding a new hire"
- MCP Bundles: Actionable error messages with next-step suggestions

### Architecture patterns
- PostHog MCP v2: "Wrapping your entire API as MCP tools is really bad"
- Metafunctor pattern: "Ten example queries teach more than a schema diagram"
- FastMCP 3.1 CodeMode: Validated but unnecessary at 14 tools
- Redpanda Connect: Start descriptions with action verb, state defaults explicitly

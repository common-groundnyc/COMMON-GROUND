# Graph Expansion Plan — DuckPGQ Rebuild

*Produced from entity_registry.md + live audit data (2026-03-25 session)*

## Current Graph Coverage vs Available Data

### Graph 1: Housing (nyc_housing, nyc_building_network)

**Current sources**: hpd_jurisdiction (377K), hpd_registration_contacts (804K)
**Current graph**: 194K owners, 348K buildings, 10.8M violations, **489 shared edges**

| Gap | Table | Rows | Entity Types | New Nodes | Priority |
|:---|:---|---:|:---|---:|:---|
| Buildings not in graph | pluto | 859K | bbl, address, ownername, zip | +511K BBLs | **P0** |
| Owner names from ACRIS | acris_parties (grantee) | 46.2M | person_name, address | +millions unique | P1 |
| DOB application owners | dob_application_owners | 2.7M | person_name, org_name, bbl | +200K names | P1 |
| Evictions (petitioners) | evictions | 374K | bbl, address, person_name | +100K BBLs | P1 |
| Rolling sales (buyers) | rolling_sales | 79K | bbl, person_name, address | +50K names | P2 |
| FDNY violations | fdny_violations | 52K | bbl, bin, address | already in enforcement_web | P2 |

**Critical fix**: graph_shared_owner uses registrationid (489 edges) instead of owner name (31K+ edges). Name-based rebuild = **63x more connectivity**.

**PLUTO expansion**: 859K lots vs HPD's 348K = **+511K buildings**. PLUTO also has `ownername` — adds commercial/vacant lot owners not in HPD.

**Memory**: Current housing graph CSR ~170MB. With PLUTO expansion: ~300MB. Well within 28GB.

---

### Graph 2: Corporate Web (nyc_corporate_web)

**Current sources**: nys_corporations filtered to HPD/PLUTO name matches (417K, 104K unique)
**Current graph**: 104K unique corps, 82K people, 145K officer edges, 185K shared officer edges

| Gap | Table | Rows | Entity Types | New Nodes | Priority |
|:---|:---|---:|:---|---:|:---|
| Duplicate vertex PKs | graph_corps | 312K dupes | dos_id | FIX: deduplicate | **P0** |
| ACRIS-referenced corps | acris_parties | 46.2M | org_name (LLC/CORP/INC) | +100K+ corps | P1 |
| Campaign employer corps | campaign_contributions | 1.7M | org_name (empname) | +50K employers | P1 |
| OATH respondent corps | oath_hearings | 21.6M | org_name (respondent) | +200K respondents | P1 |
| DOB permit owner corps | dob_application_owners | 2.7M | org_name (business_name) | +100K businesses | P1 |
| Doing Business entities | doing_business_entities | 11K | org_name | +10K entities | P2 |
| SBS M/WBE certified | sbs_certified | 23K | org_name | +20K businesses | P2 |

**Current coverage**: 104K unique corps out of 8.3M NYC-relevant = **1.3%**. Target: 500K+ corps (6%+) by adding ACRIS + OATH + DOB + campaign matches.

**Memory**: Current corp graph CSR ~5MB. Expanded to 500K corps: ~50MB. No concern.

---

### Graph 3: Transaction Network (nyc_transaction_network)

**Current sources**: acris_parties + acris_master (real property only)
**Current graph**: 1.96M tx_entities, 8.9M tx_edges, 3.96M shared edges

| Gap | Table | Rows | Entity Types | New Nodes | Priority |
|:---|:---|---:|:---|---:|:---|
| Personal property (UCC) | acris_pp_parties | 11M (business) | person_name, org_name, address | +500K entities | P1 |
| Personal property master | acris_pp_master | 4.5M | document_id | +4.5M transactions | P1 |
| Personal property legals | acris_pp_legals | 3.9M | document_id | context for pp | P2 |

**Current coverage**: 1.96M entities from 46.2M real property parties (4.2%). Personal property adds 11M more party records.

**Memory**: Current tx graph CSR ~140MB. Adding PP tables: ~250MB. Fine.

---

### Graph 4: Influence Network (nyc_influence_network)

**Current sources**: campaign_contributions (1.7M) + campaign_expenditures (516K)
**Current graph**: 311K pol_entities, 700K pol_donations

| Gap | Table | Rows | Entity Types | New Nodes | Priority |
|:---|:---|---:|:---|---:|:---|
| Off-year contributions | cfb_offyear_contributions | 109K | person_name, org_name | +30K donors | P1 |
| Doing business contribs | doing_business_contributions | 22K | person_name, org_name | +10K donors | P1 |
| NYS campaign finance | nys_campaign_finance (federal) | 69M | person_name, org_name | +millions | P1 |
| FEC contributions | fec_contributions | 44M | person_name, org_name | +millions (federal) | P1 |
| Lobbyist registration | nys_lobbyist_registration | 17.3M | person_name, org_name | +100K lobbyists | P1 |
| Lobbyist disclosures | nys_lobbyist_disclosures_pre2019 | 193K | person_name, org_name | +50K | P2 |
| COIB donations | coib_agency_donations + coib_elected_nfp | 13.6K | person_name, org_name | +5K | P2 |
| Contract awards | contract_awards | 52K | org_name, address | +30K vendors | P1 |
| NYS procurement | nys_procurement_state + local | 341K | org_name | +100K vendors | P2 |

**Current coverage**: 311K entities. With NYS + FEC + lobbying: **potentially 2M+ political entities**. This makes the influence graph the second-largest after transactions.

**Memory**: With 2M entities and ~5M edges: ~80MB. Manageable.

---

### Graph 5: Contractor Network (nyc_contractor_network)

**Current sources**: dob_permit_issuance (4M)
**Current graph**: 58K contractors, 1.46M permit edges

| Gap | Table | Rows | Entity Types | New Nodes | Priority |
|:---|:---|---:|:---|---:|:---|
| NYS contractor registry | nys_contractor_registry | 13K | org_name, license_id, address | +5K contractors | P2 |
| DOB safety violations | dob_safety_violations | 1.1M | license_id, bbl | +enforcement data | P2 |

**Coverage is actually good** — the DOB permit data already captures the main contractor network. Low-priority expansion.

---

## Tables NOT in Any Graph (High Value)

| Schema | Table | Rows | Entity Types | Recommended Graph | Priority |
|:---|:---|---:|:---|:---|:---|
| city_government | oath_hearings | 21.6M | person, org, bbl, bin, address, zip (7) | **NEW: Enforcement** | **P0** |
| social_services | n311_service_requests | 21M | address, zip, bbl (3+) | **NEW: Civic** | P1 |
| city_government | citywide_payroll | 6.8M | person, org | Influence (employee→agency) | P1 |
| city_government | marriage_licenses_1950_2017 | 4.8M | person (4 name cols) | Cross-domain (person links) | P2 |
| city_government | civil_list | 3.2M | person, org | Influence (employee→agency) | P2 |
| city_government | nys_campaign_expenditures | 4.2M | person, org, address | Influence (spending) | P1 |
| financial | nys_attorney_registrations | 1.7M | person, org, address | Cross-domain (professional) | P2 |
| financial | nys_re_brokers | 591K | person, address | Cross-domain (professional) | P2 |
| federal | nys_death_index | 38.6M | person | Cross-domain (person links) | P2 |
| public_safety | nypd_misconduct_named | 303K | person (officer names) | Officer network (existing) | P1 |
| public_safety | ccrb_officers | 193K | person (officer names) | Officer network (existing) | P1 |

---

## New Graph Candidates

### Enforcement Graph (NEW — Phase 3 or 4)
Connect all enforcement/violation data through BBL:
- **Vertices**: oath_hearings respondents, FDNY violations, DOB violations, HPD violations
- **Edges**: shared BBL, shared respondent name
- **Scale**: ~55M violation records, ~1M unique BBLs, ~500K unique respondent names
- **Value**: "Show me every enforcement action against this building/person across all agencies"
- **Priority**: **P0** — oath_hearings is the biggest ungraphed table

### Civic Graph (NEW — Phase 6)
Connect 311 complaints to buildings and neighborhoods:
- **Vertices**: 311 complaints (by unique_key), buildings (by BBL), addresses
- **Edges**: complaint→building (via BBL or address)
- **Scale**: 21M complaints, ~200K unique BBLs
- **Value**: neighborhood complaint patterns, slumlord complaint chains
- **Priority**: P1

---

## Memory Budget (28GB Container)

| Graph | Current CSR | Expanded CSR | Notes |
|:---|---:|---:|:---|
| Housing | 170MB | 300MB | +PLUTO buildings |
| Corporate Web | 5MB | 50MB | +ACRIS/OATH/DOB corps |
| Transaction | 140MB | 250MB | +personal property |
| Influence | 10MB | 80MB | +NYS/FEC/lobbying |
| Contractor | 25MB | 30MB | minimal expansion |
| Enforcement (NEW) | 0 | 200MB | oath+fdny+dob violations |
| Civic (NEW) | 0 | 150MB | 311 complaints |
| **TOTAL** | **350MB** | **~1.1GB** | **4% of 28GB** |

All graphs fit comfortably. Even with the unified cross-domain graph (Phase 7), we're well under the 28GB limit.

---

## Phase Mapping

### Phase 2: Bug Fixes
- Deduplicate graph_corps (312K dupes)
- Fix property_history date sort
- Fix graph_has_violation orphans (13,701)
- **FIX S3 CREDENTIALS** — DuckLake can't reach MinIO after restart (CREATE SECRET + SET s3_* not working for DuckLake's internal S3 client)
- **FIX duckpgq loading** — extension installed but not loaded after container rebuild

### Phase 3: Ownership Rebuild
- Add PLUTO as building source (+511K BBLs)
- Rebuild graph_shared_owner on owner name (+63x edges)
- Add DOB application owners, evictions as owner sources
- Add ACRIS grantee names as property owners

### Phase 4: Corporate Web Rebuild
- Deduplicate graph_corps (move from Phase 2 if needed)
- Expand corp matching: ACRIS parties, OATH respondents, campaign employers, DOB business names
- Target: 104K → 500K+ unique corps

### Phase 5: Transaction Expansion
- Add personal property tables (acris_pp_parties 11M, acris_pp_master 4.5M)
- Rebuild shared edges with expanded entity set

### Phase 6: Influence Expansion
- Add NYS campaign finance (69M), FEC contributions (44M)
- Add lobbyist registrations (17.3M)
- Add contract awards (52K), doing business (22K), COIB (13.6K)
- Add citywide payroll as employee→agency edges
- Target: 311K → 2M+ political entities

### Phase 7: Cross-Domain Unified Graph
- Bridge through resolved_entities cluster_ids (55.5M records, 33.1M clusters)
- Person in housing graph + same person in influence graph + same person in enforcement = one traversable path
- New enforcement graph as bridge between housing (BBL) and corporate (respondent name)

---

## resolved_entities Integration

The Splink v1.0 resolved_entities table (55.5M rows, 33.1M clusters) is the backbone for Phase 7:

| Graph | Entity Type | Join Column | Cluster Coverage |
|:---|:---|:---|:---|
| Housing → Corporate | owner_name → corp_name | cluster_id on last_name+first_name | ~44 tables covered |
| Housing → Influence | owner_name → donor_name | cluster_id | High overlap (landlord donors) |
| Corporate → Influence | corp officer → donor | cluster_id | Medium overlap |
| Enforcement → Housing | respondent → owner | cluster_id + BBL | Very high overlap |
| All graphs | Any person → Any person | cluster_id | Universal bridge |

**Key insight**: resolved_entities already links names across 44 tables. Phase 7 uses cluster_id as the edge weight/join key for the unified graph — no new entity resolution needed.

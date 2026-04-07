# Entity X-Ray Ingestion Plan
**Created:** 2026-03-15
**Status:** In progress

## Overview

Adding 30+ new datasets to the dlt pipeline to power the entity X-ray tool.
All Socrata datasets are zero-effort (just add tuples to `datasets.py`).
Federal APIs (FEC, ProPublica, LittleSis, USAspending) get custom dlt sources.

## Socrata Additions (30 datasets)

### city_government (+8)
| Table | Dataset ID | Domain | Rows | Value |
|-------|-----------|--------|------|-------|
| elobbyist | fmf3-knd8 | nyc | 74k | WHO lobbies WHOM for which developer/LLC |
| contract_awards | qyyg-4tf5 | nyc | 51k | City contracts → vendor names → campaign donors |
| lobbyist_fundraising | 7arw-dbem | nyc | 440 | Lobbyist-candidate political consulting |
| coib_enforcement | p39r-nm7f | nyc | 1.4k | Conflicts of Interest Board fines |
| oath_trials | y3hw-z6bm | nyc | 3.2k | OATH Trials Division cases |
| nys_campaign_finance | e9ss-239a | nys | 17.8M | State-level campaign contributions & expenditures |
| nys_lobbyist_registration | se5j-cmbb | nys | 17.3M | State lobbying registrations |
| nys_coelig_enforcement | vsmx-hgi8 | nys | 286 | Lobbying law violation enforcements |

### business (+4)
| Table | Dataset ID | Domain | Rows | Value |
|-------|-----------|--------|------|-------|
| acris_pp_parties | nbbg-wtuz | nyc | 11M | UCC filings, chattel mortgages — entity names + addresses |
| nys_liquor_authority | 9s3h-dpkz | nys | 58k | LLC names operating liquor-licensed businesses |
| nys_contractor_registry | i4jv-zkey | nys | 12.7k | Debarment flags, wage violation flags |
| nys_non_responsible | jhxt-dfv6 | nys | 27 | State-debarred contractors (any match = red flag) |

### financial (+8, NEW schema)
| Table | Dataset ID | Domain | Rows | Value |
|-------|-----------|--------|------|-------|
| nys_tax_warrants | v7ua-z23v | nys | 275k | Name → tax debt (red flag signal) |
| nys_child_support_warrants | 8pp4-isha | nys | 49k | Supplementary red-flag data |
| nys_attorney_registrations | eqw2-r5nb | nys | 429k | Every attorney in NYS — firm, status, admission |
| nys_re_brokers | yg7h-zjbf | nys | 148k | RE broker names → property deal cross-ref |
| nys_notaries | rwbv-mz6z | nys | 238k | Notaries on property documents |
| nys_procurement_state | ehig-g5x3 | nys | 276k | State authority vendor contracts |
| nys_procurement_local | 8w5p-k45m | nys | 65k | Local authority vendor contracts |
| nys_ida_projects | 9rtk-3fkw | nys | 34k | Tax exemption/PILOT deals with developers |

### housing (+2)
| Table | Dataset ID | Domain | Rows | Value |
|-------|-----------|--------|------|-------|
| fdny_violations | bi53-yph3 | nyc | 52k | Fire code violations with building owner names |
| fdny_vacate_list | n5xc-7jfa | nyc | 357 | Vacate orders (serious safety signal) |

### city_government (+2 more, via separate extractor)
| Table | Dataset ID | Domain | Rows | Value |
|-------|-----------|--------|------|-------|
| city_record | dg92-zbpx | nyc | 1M | ALL city govt actions — contracts, personnel, property |
| passport_vendors | irs3-wn2g | nyc | 1.6M | NYC vendor registrations by commodity |

## Federal API Sources (4 new dlt sources)

### 1. FEC (Federal Election Commission)
- **Source:** `sources/fec.py`
- **Method:** Bulk CSV download (2-4 GB/cycle) → PyArrow read → filter NYC ZIPs → dlt
- **URL:** `https://www.fec.gov/files/bulk-downloads/{year}/indiv{yy}.zip`
- **Cycles:** 2020, 2022, 2024 (configurable)
- **Data:** ~40-60M rows/cycle total, ~2-5M NYC rows after ZIP filter
- **Fields:** name, employer, occupation, transaction_amt, zip_code, cmte_id, sub_id
- **Auth:** None for bulk download (no API key needed)
- **Pipeline:** Download ZIP → extract pipe-delimited TXT → PyArrow read_csv → filter ZIP prefix → yield Arrow table to dlt
- **Note:** `parallelized=True` + `dlt.defer()` processes all 3 cycles concurrently

### 2. ProPublica Nonprofit Explorer
- **Source:** `sources/propublica.py`
- **Method:** REST API (no auth)
- **Data:** 1.8M tax-exempt orgs with EIN, revenue, assets, officer compensation
- **Strategy:** Search by org name → fetch detail by EIN

### 3. LittleSis Power Mapping
- **Source:** `sources/littlesis.py`
- **Method:** REST API (no auth, CC BY-SA 4.0)
- **Data:** 500k entities, millions of relationships (boards, donations, lobbying)
- **Strategy:** Entity search → relationship crawl

### 4. USAspending Federal Contracts
- **Source:** `sources/usaspending.py`
- **Method:** POST-based REST API (no auth)
- **Data:** 100M+ federal awards — contracts, grants
- **Strategy:** Recipient autocomplete → detail → award search

## Deferred (too large for initial load)
| Dataset | ID | Rows | Reason |
|---------|------|------|--------|
| NYS Lobbyist Bi-Monthly | t9kf-dqbc | 260M | ~3.4 hours at 21k/sec |
| NYS Client Semi-Annual | qym9-xzj6 | 65M | ~52 min at 21k/sec |

Enable after initial load succeeds. Add to datasets.py when ready.

## Entity X-Ray Wire-Up

After ingestion, add queries for new tables to `entity_xray()` in mcp_server.py:
- eLobbyist → "who lobbies on behalf of this entity"
- Contract awards → "what city contracts has this entity won"
- NYS campaign finance → "state-level political donations"
- Tax warrants → "outstanding tax debts" (red flag)
- Attorney registrations → "is this person a lawyer"
- Contractor registry → "debarment/wage violation flags"
- FDNY violations → "fire safety record"
- FEC contributions → "federal political donations"

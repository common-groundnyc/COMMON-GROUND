# Mirror: Open Data 2.0

**Date:** 2026-04-13
**Status:** Design concept — derived from UoT analysis + graphify landscape scan

## One sentence

Open data is not a collection of spreadsheets. It's a navigable model of the city built from four primitives: Entity, Observation, Link, Context.

## The problem with open data

Open data is stuck in the file era. Government agencies publish CSV tables. Every consumer — journalist, tenant organizer, civic hacker, researcher — downloads them, figures out the columns, writes queries, cross-references manually, hopes the data is fresh. 80% of the effort is plumbing. The actual insight takes 20%.

Common Ground v1 solved the access problem. You can ask "is my landlord a slumlord?" in English and get an answer. DuckDB + MCP handles that.

But underneath, it's still 294 tables, 400M rows, flat files. The relationships between buildings, people, companies, violations, donations, and contracts exist only because we wrote SQL joins to create them. The data itself doesn't know it's connected.

## What open data should be

A navigable model of the city. Not tables to query — a space to explore.

You don't download datasets. You don't write queries. You say "take me to 123 Main Street" and you see everything: who owns it, what violations it has, what happened there over time, how it compares to its neighbors, whether the owner has a pattern. Every fact has a source. Every number has context. Every entity tells its own story.

## Four primitives

Everything in civic data maps to exactly four things:

### 1. Entity

A permanent thing with a canonical identity.

```
Entity:
  id: "building:bbl-3012340001"     # permanent, never changes
  type: Building
  canonical_name: "123 Main St, Brooklyn, NY 11201"
  created_at: 2024-03-15            # when we first observed it
  source_ids:                       # how different agencies know it
    pluto: "3012340001"
    hpd: "REG-123456"
    dob: "BIN-3387654"
    acris: "BBL-3012340001"
```

Entity types: Building, Person, Corporation, Agency, Violation, Complaint, Permit, Transaction, Donation, Contract, School, Officer.

The entity is the anchor. It exists independently of any dataset. Agencies don't publish entities — they publish observations ABOUT entities. Entity resolution (Splink) creates the canonical identity.

### 2. Observation

An immutable fact from a source at a time. This is what agencies actually publish — reframed.

```
Observation:
  id: "obs:hpd-viol-12345-2026-01-15"
  entity: "building:bbl-3012340001"
  source: "NYC HPD"
  observed_at: 2026-01-15
  type: violation_filed
  attributes:
    class: "A"                     # immediately hazardous
    description: "Lead-based paint"
    status: "OPEN"
    apartment: "4B"
  raw_source:
    dataset: "HPD Violations"
    row_id: "12345"
    fetched_at: 2026-01-16T03:00:00Z
```

Observations are append-only. They never change. If HPD closes the violation, that's a NEW observation:

```
Observation:
  id: "obs:hpd-viol-12345-closed-2026-03-20"
  entity: "building:bbl-3012340001"
  source: "NYC HPD"
  observed_at: 2026-03-20
  type: violation_closed
  attributes:
    class: "A"
    original_observation: "obs:hpd-viol-12345-2026-01-15"
```

Current state is always derivable from the observation stream. But history is never lost.

### 3. Link

A typed, sourced relationship between two entities.

```
Link:
  id: "link:owns-smithllc-3012340001"
  source_entity: "corp:john-smith-llc"
  target_entity: "building:bbl-3012340001"
  type: OWNS
  source: "NYC ACRIS"
  established_at: 2019-06-15       # when the deed was recorded
  evidence:                         # what observations support this
    - "obs:acris-deed-2019-abc123"
  confidence: 0.95                  # from entity resolution
```

Link types: OWNS, CONTROLS, DONATED_TO, EMPLOYED_BY, PERMITTED_AT, LOCATED_IN, NEAR, SAME_AS, PREVIOUSLY_OWNED_BY.

Links are the relationships that today require manual SQL joins. In this model they're first-class, sourced, and time-aware. "Who owned this before?" is a link with a `dissolved_at` timestamp.

### 4. Context

Computed statistical significance. Not opinion — computation. Every fact carries its own "so what."

```
Context:
  entity: "building:bbl-3012340001"
  computed_at: 2026-04-13T07:00:00Z
  metrics:
    violation_count: 47
    violation_percentile_borough: 0.99   # worse than 99% in Brooklyn
    violation_percentile_city: 0.97
    violation_trend_2yr: 3.2             # 3.2x increase
    avg_violations_same_owner: 23        # owner's portfolio average
    owner_portfolio_size: 12             # how many buildings the owner has
    owner_portfolio_violation_avg: 23
    comparable_buildings:                # similar age, size, zip
      avg_violations: 8
      this_vs_comparable: 5.9x          # 5.9x worse than comparable
```

Context is recomputed daily. It turns "47 violations" into "47 violations, which is 5.9x worse than comparable buildings and increasing 3x in 2 years, and the owner's 11 other buildings average 23 each."

A 66-year-old doesn't need to know what a percentile is. The system says: "This building has significantly more problems than similar buildings, and it's getting worse."

## How it works

### For the LLM (MCP interface)

The LLM navigates the entity space. It doesn't write SQL.

```
User: "What's going on at 123 Main St?"

LLM reads: entity page for building:bbl-3012340001
  → sees 47 violations (99th percentile, 3x trend)
  → follows OWNS link to corp:john-smith-llc
  → sees owner has 12 buildings, portfolio avg 23 violations
  → follows DONATED_TO link to entity:council-member-x ($45K)
  → follows CONTRACTED_WITH link to entity:city-contract-abc ($2M)

LLM answers: "123 Main St has 47 open violations — that's worse than
99% of buildings in Brooklyn and getting worse fast. The owner, John
Smith LLC, owns 11 other buildings with similar problems. They also
donated $45,000 to Council Member X, who awarded them a $2M city
contract. Sources: HPD (violations, updated Jan 2026), ACRIS
(ownership, recorded Jun 2019), CFB (donations, filed 2025)."
```

One traversal. No SQL. Every claim sourced.

### For the data (storage)

Under the hood, it's still DuckDB + DuckLake. The four primitives map to four tables:

```sql
CREATE TABLE lake.mirror.entities (
    entity_id VARCHAR PRIMARY KEY,
    entity_type VARCHAR,
    canonical_name VARCHAR,
    source_ids MAP(VARCHAR, VARCHAR),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE lake.mirror.observations (
    observation_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR,
    source VARCHAR,
    observed_at TIMESTAMP,
    observation_type VARCHAR,
    attributes JSON,
    raw_source JSON,
    ingested_at TIMESTAMP
);

CREATE TABLE lake.mirror.links (
    link_id VARCHAR PRIMARY KEY,
    source_entity VARCHAR,
    target_entity VARCHAR,
    link_type VARCHAR,
    source VARCHAR,
    established_at TIMESTAMP,
    dissolved_at TIMESTAMP,
    evidence JSON,
    confidence FLOAT
);

CREATE TABLE lake.mirror.context (
    entity_id VARCHAR,
    computed_at TIMESTAMP,
    metrics JSON,
    PRIMARY KEY (entity_id, computed_at)
);
```

Four tables. That's the entire data model. Every dataset in the lake gets ingested as observations about entities with links between them. The 294 existing tables become the source layer. The four mirror tables become the serving layer.

### For the user

They don't know any of this. They ask a question. They get a story. Every fact is sourced. Every number has meaning.

## What makes this different

| Current open data | Mirror |
|---|---|
| Data is tables | Data is entities with observations |
| Relationships are joins | Relationships are first-class links |
| Numbers are raw | Numbers carry their significance |
| History is lost (snapshots) | History is native (event stream) |
| Consumer figures out meaning | Meaning is computed and included |
| Each dataset is independent | Everything is pre-linked |
| You query it | You navigate it |
| For programmers | For anyone |

## What we already have

Most of this is built:

| Component | Status | What it becomes |
|---|---|---|
| DuckDB + DuckLake | Production | Storage for the four tables |
| Splink entity resolution | Production | Creates Entity records |
| 294 Socrata/federal datasets | Production | Source of Observations |
| DuckPGQ property graphs | Production | Navigable Link structure |
| Percentile middleware | Production | Computes Context |
| FastMCP server | Production | Interface for navigation |
| 15 MCP super tools | Production | Navigation actions |

## What's new

1. **Mirror tables** — the four-primitive schema (entities, observations, links, context)
2. **Ingestion transform** — Dagster assets that convert source tables into mirror format
3. **Context computation** — daily Dagster asset that computes significance metrics
4. **Entity navigation MCP tools** — new tools that traverse the entity graph instead of querying tables

## Why this wins

**For a 66-year-old:** "What about my building?" → a complete, sourced, contextualized story. No tables, no downloads, no jargon.

**For a journalist:** follow-the-money trails are pre-built. Owner → LLC → donations → contracts. Every hop is sourced with agency and date.

**For a civic hacker:** no more data integration. Entities are pre-linked. Build your app on top of resolved, contextualized entities instead of raw tables.

**For open data itself:** this is what CSV was to paper records. A fundamental format upgrade that makes everything downstream easier.

## Derived from

- concept-hop cross-domain scan (287 packages across 14 platforms)
- graphify landscape vault (78 repos, 481 nodes, 35 communities)
- Graph path tracing across communities 9, 10, 13, 20
- Universe of Thoughts analysis (Combinational + Exploratory + Transformative)
- Key inspirations: DataScript (triples), CR-SQLite (CRDT sync), D2TS (incremental computation), Cognee (knowledge engine), Wikipedia (linked knowledge), Google Maps (navigable space)

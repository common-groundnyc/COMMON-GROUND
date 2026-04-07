# CG `/explore` Dashboard — Design Spec

> **Date:** 2026-04-07
> **Status:** APPROVED — ready for implementation plan
> **Scope:** First user-visible feature of the Common Ground platform expansion. A neighborhood-overview dashboard with an NYC ZIP map, cross-filtered charts, and a worst-buildings table, all powered by the existing MCP server's `neighborhood()` tool and Mosaic for auto cross-filtering.

---

## 1. Goals & Non-Goals

### Goals

1. **Discovery surface for Common Ground** — the page someone arrives at, types a ZIP, and immediately understands what CG is.
2. **Cross-filtered exploration** — a single user gesture (click a ZIP, brush a date range, click a category) refilters every panel on the page.
3. **Source attribution everywhere** — every data block carries a chip showing which lake tables fed it. "Show the receipts."
4. **Zero accounts** — anonymous, public, no friction.
5. **Cheap to operate** — runs on existing infrastructure, no new containers, no third-party SaaS.
6. **Inherits the CG website aesthetic** — same tokens, same typography, same Civic Punk discipline.

### Non-Goals

- Mobile-first design. Desktop is primary; mobile gets a stripped variant in v1.1.
- Authentication. Anonymous only.
- Subscription/notification UI. That ships in a separate phase.
- Building-detail page. Linked from the worst-buildings table but built separately.
- Comparison of 3+ ZIPs. v1 supports 2.
- Gentrification, environmental justice, hotspot heatmap views. Deferred to v1.1+.

---

## 2. Architecture

### High-level

```
┌──────────────────────────────────────────────────────────────┐
│                        BROWSER                                │
│                                                                │
│   Next.js 16 /explore page                                    │
│   ┌──────────────────────────────────────────────────────┐   │
│   │ MapLibre GL choropleth (deck.gl layer)                │   │
│   │ vgplot bar/line charts                                │   │
│   │ Stat cards (server-rendered initial state)            │   │
│   │ Worst buildings table                                 │   │
│   │                                                       │   │
│   │ All driven by Mosaic Coordinator selections           │   │
│   │ URL state via nuqs (?zip=11201&compare=11206...)     │   │
│   └──────────────────────────────────────────────────────┘   │
└──────────┬─────────────────────────────────┬─────────────────┘
           │                                 │
           │ REST (initial render +          │ SQL over HTTP
           │  ZIP search autocomplete)       │ (Mosaic protocol)
           ▼                                 ▼
┌──────────────────────────────────────────────────────────────┐
│         duckdb-server container (existing, on Hetzner)        │
│                                                                │
│  FastMCP 3.x server                                           │
│  ├─ /mcp                  ← existing MCP protocol             │
│  ├─ /api/neighborhood/{zip}     (custom_route)                │
│  ├─ /api/zips/search?q=         (custom_route)                │
│  ├─ /api/buildings/worst?zip=   (custom_route)                │
│  └─ /mosaic/query               (custom_route, mosaic-server) │
│                                                                │
│  CursorPool → DuckDB (read-only, schema allowlist)            │
└──────────┬───────────────────────────────────────────────────┘
           │
           ▼
   DuckLake (lake.duckdb on local NVMe — 195 GB)
   • housing.hpd_violations, hpd_complaints, dob_*
   • social_services.n311_service_requests
   • public_safety.nypd_complaints
   • health.restaurant_inspections
   • foundation.* (materialized views)
```

### Server-side: extend the existing MCP container

The existing `infra/duckdb-server/` container already runs FastMCP 3.x. We add four `@mcp.custom_route` handlers to `mcp_server.py` (or a new `routes/` module imported by it). No new container, no new service.

```python
# infra/duckdb-server/routes/explore.py
from starlette.responses import JSONResponse
from starlette.requests import Request
from tools.neighborhood import neighborhood
from tools.building import building

@mcp.custom_route("/api/neighborhood/{zip}", methods=["GET"])
async def neighborhood_rest(request: Request) -> JSONResponse:
    zip_code = request.path_params["zip"]
    view = request.query_params.get("view", "full")
    timeframe = request.query_params.get("timeframe", "12mo")
    result = await neighborhood.fn(location=zip_code, view=view, timeframe=timeframe)
    return JSONResponse(result, headers={"Cache-Control": "public, max-age=300"})
```

**Why custom_route over a separate FastAPI:**
- Tool functions are reused as-is — no logic duplication
- One container, one deploy story
- `@mcp.custom_route` bypasses MCP auth middleware, so no token negotiation for public dashboard data
- Cloudflare can cache the JSON at the edge via the `Cache-Control` header

### Server-side: Mosaic data layer

Mosaic's `mosaic-server` Python package implements a JSON-RPC over HTTP protocol that the `@uwdata/mosaic-core` client uses. The browser sends SQL queries; the server runs them against DuckDB and returns Apache Arrow buffers.

We mount it as a custom route inside the same container:

```python
# infra/duckdb-server/routes/mosaic_route.py
from mosaic_server import MosaicServer
from shared.db import get_readonly_connection

mosaic_server = MosaicServer(
    connection=get_readonly_connection(),
    allowed_schemas={"housing", "public_safety", "social_services", "health", "foundation"},
    max_query_duration_ms=5_000,
)

@mcp.custom_route("/mosaic/query", methods=["POST"])
async def mosaic_query(request: Request):
    body = await request.json()
    return await mosaic_server.handle(body)
```

**Safety guardrails:**
- Read-only DuckDB connection
- Schema allowlist (no `_pipeline_state`, no `_subscriptions`)
- 5-second query timeout
- Query length cap (10 KB SQL)
- Per-IP rate limit at the Cloudflare edge (100 req/min)

### Client-side: Next.js page

The `/explore` route is a client component (Mosaic and MapLibre both need a browser environment). The first render is a Server Component that fetches initial stats via `/api/neighborhood/{zip}` so the page paints fast even before Mosaic boots.

```
src/app/explore/page.tsx          ← Server Component, SSR initial state
src/components/explore/
├── ExploreShell.tsx               ← layout, theme provider, Mosaic Coordinator
├── ExploreSearchBar.tsx           ← ZIP search, timeframe, topic, compare
├── ExploreMap.tsx                 ← MapLibre + deck.gl + Mosaic selection
├── StatCards.tsx                  ← initial values from SSR, refresh on selection change
├── ViolationsChart.tsx            ← vgplot bar chart, time series
├── CategoriesChart.tsx            ← vgplot bar chart, top categories
├── WorstBuildings.tsx             ← table, click → /building/{bbl}
├── theme/cgTheme.ts               ← vgplot theme matching CG tokens
└── lib/
    ├── mosaicClient.ts            ← Mosaic Coordinator + Connector setup
    ├── api.ts                     ← REST helpers for /api/neighborhood etc.
    └── selections.ts              ← named selections (zipSelection, dateSelection)
```

---

## 3. Data Sources & Tables

The dashboard reads from these DuckLake tables (all already exist):

| Panel | Source tables |
|-------|---------------|
| Map choropleth | `foundation.geo_zip_boundaries` (new, see §6), `housing.hpd_violations` (rolled up by ZIP) |
| Stat: Population, Rent | `foundation.mv_zip_stats` (existing materialized view) |
| Stat: HPD Violations | `housing.hpd_violations` |
| Stat: 311 Complaints | `social_services.n311_service_requests` |
| Stat: Crimes | `public_safety.nypd_complaints` |
| Stat: Restaurants A-grade % | `health.restaurant_inspections` |
| Violations/month chart | `housing.hpd_violations` grouped by `inspection_date` month |
| Top categories chart | `housing.hpd_violations` grouped by `class` + `novdescription` |
| Worst buildings table | `foundation.mv_building_hub` filtered to ZIP, sorted by violation count |

The `neighborhood()` MCP tool already produces a `full` view that returns the stat-card data in one call. The dashboard's stat cards reuse this directly.

Mosaic charts query the raw tables via the `/mosaic/query` endpoint with vgplot specs. They subscribe to a shared `zipSelection` so a click on the map filters every chart.

---

## 4. Cross-Filtering Behavior (Mosaic)

Mosaic's coordinator pattern: each panel registers as a client of one or more named selections. When the selection changes, all subscribers re-query in parallel.

```typescript
// src/components/explore/lib/selections.ts
import { Selection } from "@uwdata/mosaic-core";

export const zipSelection = Selection.single({ field: "zip" });
export const dateSelection = Selection.intersect({ cross: true });
export const categorySelection = Selection.single({ field: "category" });
```

**Interactions:**

| User action | Selection change | What refilters |
|-------------|------------------|----------------|
| Click ZIP on map | `zipSelection` ← `zip = '11201'` | All charts, stat cards, buildings table |
| Brush date range on chart | `dateSelection` ← `date BETWEEN x AND y` | Map (color intensity), other charts, buildings |
| Click bar in categories chart | `categorySelection` ← `category = 'Heat'` | Buildings table only |
| Change ZIP in search bar | URL update + `zipSelection` ← new ZIP | Everything (also recenters map) |

URL state via nuqs: `?zip=11201&topic=housing&timeframe=12mo&compare=11206`. Selection changes write to URL; URL changes restore selections on page load.

---

## 5. Comparison Mode

Triggered by `[+ COMPARE]` button. Adds a second ZIP picker. The URL becomes `?zip=11201&compare=11206`.

**Hybrid layout for comparison:**
- **Stat cards & worst-buildings table:** side-by-side columns (ZIP A on left, ZIP B on right)
- **Time-series & category charts:** overlay (two colored series — `truth` teal vs `lilac`, never gray)

The map shows both ZIPs highlighted. Map click in compare mode swaps the active ZIP (the one being edited).

---

## 6. NYC ZIP Boundaries (one-time data prep)

Mosaic + deck.gl needs the NYC ZIP polygons. We add them as a foundation table:

**Source:** NYC Department of City Planning's Modified ZIP Code Tabulation Areas (MODZCTA), free download from `https://data.cityofnewyork.us/Health/Modified-Zip-Code-Tabulation-Areas-MODZCTA-/pri4-ifjk`.

**Process:**
1. Download the GeoJSON (~1 MB, 178 ZIPs)
2. Convert to a DuckDB table with WKB geometry column via the `spatial` extension
3. Materialize as `foundation.geo_zip_boundaries` with columns `(zip, modzcta, label, geom_wkb, centroid_lon, centroid_lat)`
4. New Dagster asset: `geo_zip_boundaries` — runs once, refresh annually

**Map tiles:** Protomaps `nyc.pmtiles` (~50 MB, single static file). Built from OpenStreetMap, served from Cloudflare R2 or directly from the duckdb-server container's static files. No tile server needed.

---

## 7. Aesthetic — Inherit From CG Website

The dashboard uses the **exact same tokens, typography, and visual conventions** as `common-ground-website/CLAUDE.md`:

### Tokens

| Token | Hex | Role in dashboard |
|-------|-----|-------------------|
| `void` | `#0A0A0A` | Background, map basemap |
| `warm-white` | `#F3F1EB` | Primary text, chart axes |
| `truth` | `#2DD4BF` | Verified stat values, primary chart series, active ZIP outline |
| `cta` | `#6B9C8A` | Buttons (compare, search), interactive icons |
| `lilac` | `#C4A6E8` | Section labels, table column headers, secondary chart series, structure lines |
| `muted` | `#A7ADB7` | Secondary text, axis labels |
| `muted-dim` | `#758494` | Source attribution chips, tertiary text |

### Typography

- **Matter** — headlines, ZIP names, body
- **JetBrains Mono** — stat numbers, deltas, source chips, axis labels (uppercase, 0.05em tracked)
- **Space Grotesk** — entity names (building owners, council members)

### Conventions

- **Source chips on every panel** — `HPD · DOB · NYPD · 311` in muted-dim mono below the data block
- **Stat numbers in mono with delta arrows** — `1,847 ↑ 12%` (teal for verified, lilac for relative)
- **Section labels in lilac uppercase mono** — `HOUSING ENFORCEMENT`, `WORST BUILDINGS`
- **No rounded corners beyond 2 px**, no gradients, no shadows
- **Hairline dividers** between table rows (`1px solid muted-dim`)
- **Density first** — no decorative whitespace, no card padding beyond 16 px
- **Accent colors are information, not decoration**

### vgplot Theme

A `cgTheme.ts` file wraps vgplot's default styling with the CG tokens:

```typescript
// src/components/explore/theme/cgTheme.ts
export const cgTheme = {
  background: "#0A0A0A",
  fontFamily: "JetBrains Mono, monospace",
  fontSize: 11,
  color: "#F3F1EB",
  axisColor: "#758494",
  gridColor: "#1a1a1a",
  primarySeries: "#2DD4BF",     // truth
  secondarySeries: "#C4A6E8",   // lilac
  tickFormat: { number: ",", date: "%b %y" },
  marginLeft: 56,
  marginBottom: 32,
};
```

### Map Style

MapLibre style JSON with void background, warm-white labels, lilac outlines for ZIP polygons, teal fill on hover/selection. No satellite imagery, no terrain. The map is a flat data surface, not a basemap.

---

## 8. Page Layout (desktop)

```
┌───────────────────────────────────────────────────────────────────┐
│ COMMON GROUND  /  EXPLORE                            [≡] [search] │  ← header
├───────────────────────────────────────────────────────────────────┤
│ ZIP / NEIGHBORHOOD                                                 │
│ ┌────────────────────────────┐                                     │
│ │ 11201  ▾   Brooklyn — DUMBO│  [+ COMPARE]                        │
│ └────────────────────────────┘                                     │
│ TIMEFRAME: [LAST 12 MO ▾]   TOPIC: [HOUSING ▾]                     │
├───────────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────┬─────────────────────────────┐ │
│ │                                 │ POPULATION    54,300         │ │
│ │                                 │ MEDIAN RENT   $3,200         │ │
│ │       MAPLIBRE CHOROPLETH       │ HPD VIOLAT.   1,847   ↑ 12%  │ │
│ │       (NYC ZIPs, click =        │ 311 CMPLNT.   4,210   ↓ 3%   │ │
│ │        crossfilter selection)   │ CRIMES        892     ↓ 8%   │ │
│ │                                 │ RESTAURANTS   340     A: 78% │ │
│ │                                 │                               │ │
│ │                                 │ HPD · DOB · NYPD · 311 ·DOHMH │ │
│ └─────────────────────────────────┴─────────────────────────────┘ │
│ ┌──────────────────────────────┬──────────────────────────────┐  │
│ │ VIOLATIONS / MONTH           │ TOP CATEGORIES               │  │
│ │ ▁▂▃▅▆▇█▇▆▅▃▂  (vgplot bar)  │ Heat/Hot Water  ▆▆▆ 412      │  │
│ │ HPD                          │ Pests           ▆▆ 318       │  │
│ │                              │ Mold            ▆ 209        │  │
│ │                              │ Lead Paint      ▌ 47         │  │
│ │                              │ HPD                          │  │
│ └──────────────────────────────┴──────────────────────────────┘  │
│ ┌─────────────────────────────────────────────────────────────┐  │
│ │ TOP 10 WORST BUILDINGS — 11201 — last 12 months             │  │
│ │ # │ ADDRESS              │ OWNER             │ VIOL │ ↦      │  │
│ │ 1 │ 305 Linden Blvd      │ ABC Realty LLC    │  47  │ →      │  │
│ │ 2 │ 142 Adams St          │ DUMBO Hldgs       │  31  │ →      │  │
│ │ ...                                                           │  │
│ │ HPD · DOB                                                    │  │
│ └─────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

**Grid:** 12-column at 1280 px, 16 px gutter, dense Civic Punk spacing.
**Heights:** map row 480 px, charts row 280 px, buildings table 320 px (scrollable).

---

## 9. Implementation Plan (high-level — full plan in next phase)

### Server (Week 1, ~5 days)

1. **Day 1:** Add `@mcp.custom_route` adapters for `/api/neighborhood/{zip}`, `/api/zips/search`, `/api/buildings/worst` to `mcp_server.py`. Test from curl.
2. **Day 2:** Install `mosaic-server` Python package, mount `/mosaic/query` route, add schema allowlist, test with a hand-written vgplot spec.
3. **Day 3:** Build `geo_zip_boundaries` Dagster asset (download MODZCTA GeoJSON → DuckLake table). Generate `nyc.pmtiles` from OSM extract.
4. **Day 4:** CORS config for `*.common-ground.nyc`, Cloudflare cache rules for `/api/*`, rate limiting for `/mosaic/query`.
5. **Day 5:** Integration tests, deploy to Hetzner.

### Frontend (Week 2, ~5 days)

1. **Day 1:** Scaffold `/explore` route, install deps (`@uwdata/mosaic-core`, `@uwdata/vgplot`, `maplibre-gl`, `deck.gl`, `pmtiles`). Set up `cgTheme.ts`.
2. **Day 2:** Search bar + URL state via nuqs. Initial server-rendered stat cards via `/api/neighborhood`.
3. **Day 3:** MapLibre + Protomaps + ZIP polygons + Mosaic selection wiring.
4. **Day 4:** vgplot violations chart + categories chart, subscribed to selections.
5. **Day 5:** Worst buildings table, comparison mode toggle, polish, source chips, deploy.

**Total:** ~10 working days. The Rich scope is achievable because the server-side `neighborhood()` tool already implements the heavy lifting.

---

## 10. Testing Strategy

### Server
- pytest tests for each `custom_route` (input validation, response shape, error handling)
- A live integration test against a snapshot of the lake (skipped if no DuckLake connection)
- Schema allowlist enforcement test for `/mosaic/query` (rejects `_subscriptions`, `_pipeline_state`)
- Rate-limit test (10 rapid requests → 11th is 429)

### Frontend
- Component tests with Vitest + React Testing Library (search bar, stat card, table)
- A Playwright e2e test that loads `/explore?zip=11201`, asserts the page renders, clicks a ZIP on the map, asserts charts refilter
- Visual regression test for the layout (Playwright screenshot diff)

### Coverage target
- Server adapters: 90% (they're thin)
- Frontend components: 80%
- e2e flows: the happy path (search → click ZIP → see charts) + comparison mode

---

## 11. Risks & Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| `mosaic-server` Python package is academic-grade, may have bugs at scale | Medium | Wrap in our own try/except, fallback to `/api/neighborhood` REST when Mosaic fails. Cap query duration at 5s. |
| Mosaic + deck.gl bundle is heavy (~800 KB minified) | Medium | Code-split the `/explore` route. Lazy-load vgplot and deck.gl after initial paint. SSR stat cards so first paint doesn't block. |
| MapLibre + Protomaps tiles need correct CORS and content-type headers | Low | Serve via the existing Cloudflare-fronted nginx; include sample config in spec. |
| DuckDB single-writer contention if Dagster materializes during a Mosaic query burst | Medium | Use a separate read-only connection for Mosaic. Connection pool already exists. |
| Cross-filtering UX confuses users who don't expect every click to refilter everything | Medium | Subtle "filtered by 11201" pill in the header with a clear button. Animation on refilter shows what changed. |
| Stat card SSR creates two renders (server then Mosaic) that look different briefly | Low | Use the same SQL on server and client; render skeleton while Mosaic boots. |
| Cloudflare caches stale data | Low | 5-min cache on `/api/neighborhood`, no cache on `/mosaic/query`, `Cache-Control: no-store` on hot tables. |

---

## 12. Out of Scope (Deferred)

- **Mobile layout** — desktop-first, mobile in v1.1
- **Subscription/notification UI** — separate phase
- **Building detail page** — linked from worst-buildings table, built separately
- **Gentrification, environmental justice, hotspot heatmap views** — additional `view=` modes in v1.1+
- **3+ ZIP comparison** — v1 supports 2
- **Account/login** — anonymous only
- **Spanish/multi-language** — English only for v1, i18n added in P5 per amended roadmap
- **Telegram Mini App version** — separate spec
- **`/explore/table?table=...` raw browser** — keep the existing server-rendered table browser at this URL

---

## 13. Open Questions (resolve during implementation plan)

1. **mosaic-server Python install** — pin version, check Python 3.13 compatibility on the duckdb-server container
2. **Protomaps `nyc.pmtiles` build** — use the official Protomaps build pipeline or fetch a pre-built NYC extract
3. **Worst buildings query speed** — `mv_building_hub` might need a ZIP index
4. **Mosaic Connector for our endpoint** — does the official `socketConnector` work, or do we need a custom HTTP connector?
5. **Cloudflare cache TTL** — 5 min is the starting point; tune based on data freshness vs cache hit rate

---

## 14. References

- `cg-platform-roadmap.md` — original roadmap (this spec is the first feature in the amended roadmap)
- `review-synthesis.md` — review amendments (server-rendered, channel-agnostic, dashboard-first)
- `cg-sqlrooms-dashboard-plan.md` — original SQLRooms plan (now superseded; we kept the data model and visual ideas, dropped DuckDB-WASM)
- `common-ground-website/CLAUDE.md` — design language (Civic Punk tokens, typography, conventions)
- FastMCP custom routes: https://gofastmcp.com/deployment/http
- Mosaic: https://idl.uw.edu/mosaic/
- MapLibre + Protomaps civic tech examples

# Common Ground SQLRooms Dashboard Plan

**Date**: 2026-04-03
**Status**: DRAFT
**Targets**: CG website `/explore` page + Telegram Mini App

---

## 1. Architecture Overview

### One App, Two Shells

A single React client app powers both the CG website and Telegram Mini App. The core is a SQLRooms "Room" — a Zustand store composed from slices — that manages DuckDB-WASM, map state, chart state, and cross-filtering. Two thin wrappers adapt it to each environment:

```
                    ┌──────────────────────┐
                    │   SQLRooms Room Core  │
                    │  (DuckDB + Kepler +   │
                    │   Mosaic + Recharts)  │
                    └──────┬───────┬────────┘
                           │       │
              ┌────────────┘       └────────────┐
              │                                 │
    ┌─────────▼──────────┐           ┌──────────▼──────────┐
    │  Next.js /explore  │           │  Telegram Mini App  │
    │  (full desktop UI) │           │  (compact mobile)   │
    │  localStorage/CRDT │           │  CloudStorage API   │
    │  URL deep links    │           │  startapp deep links│
    └────────────────────┘           └─────────────────────┘
```

### Package Selection (SQLRooms v0.28)

| Package | Role |
|---------|------|
| `@sqlrooms/duckdb` | DuckDB-WASM wrapper, `useSql` hook, remote Parquet via httpfs |
| `@sqlrooms/room-shell` | Store orchestration, layout panels, Zustand slices |
| `@sqlrooms/kepler` | Kepler.gl map with NYC boundary overlays |
| `@sqlrooms/mosaic` | Cross-filtering coordinator between map and charts |
| `@sqlrooms/recharts` | Bar/line/area charts with CG theme |
| `@sqlrooms/crdt` | Loro CRDT for localStorage persistence (website) |
| `@sqlrooms/ai` | "Ask a question" natural language query panel |
| `@sqlrooms/ui` | Base UI components, shadcn/ui foundation |

---

## 2. Data Layer Design

### 2.1 Current State

The existing `/explore` page is server-dependent: it fetches from `https://mcp.common-ground.nyc/api/query` (the Hetzner MCP server) for every filter/sort/page operation. This means:
- Every interaction requires a round-trip to Hetzner
- No offline capability
- No cross-filtering between views
- Limited to single-table browsing

### 2.2 Target: Client-Side DuckDB-WASM + Remote Parquet

DuckDB-WASM's httpfs extension supports HTTP range requests on Parquet files. This means the browser downloads only the column chunks and row groups needed for the current query — not entire files.

**MinIO must expose Parquet files via HTTP with:**
- `Accept-Ranges: bytes` header (MinIO does this by default)
- CORS headers for `common-ground.nyc` origin
- Public read access on the `ducklake` bucket (or a dedicated `public-agg` bucket)

### 2.3 Pre-Computed Aggregate Parquet Files

Raw lake tables (294 tables, 400M+ rows) are too large for browser DuckDB. We need a set of **pre-computed aggregate Parquet files** optimized for the dashboard views.

#### Required Aggregates

| File | Description | Est. Size | Grain |
|------|-------------|-----------|-------|
| `agg_zip_overview.parquet` | Demographics, income, rent, complaints, violations per ZIP | ~2 MB | 1 row per ZIP (~200 ZIPs) |
| `agg_zip_timeseries.parquet` | Monthly complaint/violation/crime counts per ZIP | ~10 MB | ZIP x month x category |
| `agg_building_summary.parquet` | Per-BBL violation count, complaint count, owner, address | ~50 MB | 1 row per BBL (~1M buildings) |
| `agg_entity_network.parquet` | Top entities with connection counts, roles, portfolios | ~5 MB | 1 row per entity |
| `agg_election_results.parquet` | Election results by district/race/year | ~3 MB | district x race x year |
| `geo_zip_boundaries.parquet` | NYC ZIP code polygons as WKB | ~2 MB | 1 row per ZIP |
| `geo_council_districts.parquet` | Council district polygons as WKB | ~1 MB | 1 row per district |
| `geo_precincts.parquet` | Police precinct polygons as WKB | ~1 MB | 1 row per precinct |
| `geo_community_districts.parquet` | Community district polygons as WKB | ~1 MB | 1 row per CD |

**Total estimated browser download: ~75 MB maximum** (but with range requests, initial load is ~5-10 MB for the default ZIP overview view).

#### GeoJSON Boundaries

NYC boundary files are publicly available from NYC Planning (bytes.nyc). Convert to Parquet with WKB geometry columns for DuckDB spatial queries. The `geo_*.parquet` files are loaded once and cached in DuckDB-WASM's OPFS.

### 2.4 CORS Configuration on MinIO

MinIO needs a bucket policy + CORS config for browser access:

```json
{
  "CORSRules": [
    {
      "AllowedOrigins": ["https://common-ground.nyc", "https://www.common-ground.nyc"],
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedHeaders": ["Range", "If-None-Match"],
      "ExposeHeaders": ["Content-Range", "Accept-Ranges", "Content-Length", "ETag"],
      "MaxAgeSeconds": 86400
    }
  ]
}
```

Option A: Add CORS to existing `ducklake` bucket (public-read on `data/agg_*` prefix only).
Option B: Create a separate `public-agg` bucket with public read + CORS. **Recommended** — isolates public data from the private lake.

### 2.5 Dagster Asset for Aggregates

A new Dagster asset (`aggregate_parquet_views`) runs after the main ingestion, queries DuckLake, and writes the aggregate Parquet files to MinIO. Scheduled daily after the main load completes.

---

## 3. SQLRooms Store Composition

### 3.1 Room Store Setup

```typescript
// src/explore/store.ts
import { createRoomStore, type RoomShellSliceState } from '@sqlrooms/room-shell';
import { createKeplerSlice, type KeplerSliceState } from '@sqlrooms/kepler';
import { createMosaicSlice, type MosaicSliceState } from '@sqlrooms/mosaic';
import { createCrdtSlice, type CrdtSliceState } from '@sqlrooms/crdt';

type CGRoomState = RoomShellSliceState
  & KeplerSliceState
  & MosaicSliceState
  & CrdtSliceState
  & CGDashboardSlice;

// Custom slice for CG-specific state
interface CGDashboardSlice {
  activeZip: string | null;
  activeView: 'neighborhood' | 'building' | 'entity' | 'election';
  setActiveZip: (zip: string | null) => void;
  setActiveView: (view: CGDashboardSlice['activeView']) => void;
}

export const { roomStore, useRoomStore } = createRoomStore<CGRoomState>(
  (set, get, store) => ({
    ...createRoomShellSlice({
      config: {
        dataSources: [
          { type: 'url', url: `${PARQUET_BASE}/agg_zip_overview.parquet`, tableName: 'zip_overview' },
          { type: 'url', url: `${PARQUET_BASE}/geo_zip_boundaries.parquet`, tableName: 'zip_boundaries' },
          // Additional sources loaded on-demand per view
        ],
      },
    })(set, get, store),
    ...createKeplerSlice({
      basicKeplerProps: {
        mapboxApiAccessToken: process.env.NEXT_PUBLIC_MAPBOX_TOKEN,
      },
    })(set, get, store),
    ...createMosaicSlice()(set, get, store),
    ...createCrdtSlice({
      persistence: 'localStorage', // Overridden in Telegram to CloudStorage
    })(set, get, store),

    // CG-specific state
    activeZip: null,
    activeView: 'neighborhood',
    setActiveZip: (zip) => set({ activeZip: zip }),
    setActiveView: (view) => set({ activeView: view }),
  }),
);
```

### 3.2 On-Demand Data Loading

Not all Parquet files load at startup. The store loads data based on the active view:

- **Neighborhood view** (default): `agg_zip_overview` + `geo_zip_boundaries` (~4 MB)
- **Building detail**: loads `agg_building_summary` on ZIP click (~50 MB, range-requested)
- **Entity network**: loads `agg_entity_network` on entity click (~5 MB)
- **Election view**: loads `agg_election_results` on navigation (~3 MB)

---

## 4. Dashboard Layout Design

### 4.1 Desktop Layout (CG Website `/explore`)

```
┌──────────────────────────────────────────────────────────┐
│ FILTER BAR: [ZIP ▼] [Date Range] [Topic ▼] [Search...]  │
├──────────────────────────┬───────────────────────────────┤
│                          │  STAT CARDS                   │
│                          │  ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│    KEPLER MAP            │  │Pop │ │Rent│ │Viol│ │311 │ │
│    (NYC choropleth       │  └────┘ └────┘ └────┘ └────┘ │
│     by ZIP/district)     ├───────────────────────────────┤
│                          │  CHARTS (cross-filtered)      │
│    Click ZIP = filter    │  ┌─────────────┐ ┌──────────┐│
│    all panels            │  │Complaints/mo│ │Violations││
│                          │  │(line chart) │ │(bar chart)││
│                          │  └─────────────┘ └──────────┘│
├──────────────────────────┴───────────────────────────────┤
│ DATA TABLE (current selection, sortable, downloadable)   │
│ Provenance bar: source, row count, last ingested         │
└──────────────────────────────────────────────────────────┘
```

### 4.2 Mobile Layout (Telegram Mini App)

```
┌─────────────────────────┐
│ [ZIP ▼] [Topic ▼]       │ <- Compact filter row
├─────────────────────────┤
│ ┌────┐ ┌────┐ ┌────┐   │ <- Scrollable stat cards
│ │Pop │ │Rent│ │Viol│   │
│ └────┘ └────┘ └────┘   │
├─────────────────────────┤
│                         │
│   MAP (half height)     │ <- Kepler map, simplified
│                         │
├─────────────────────────┤
│ ┌─────────────────────┐ │
│ │ Complaints / month  │ │ <- Single chart, swipeable
│ │ (area chart)        │ │
│ └─────────────────────┘ │
├─────────────────────────┤
│  Top 5 buildings table  │ <- Condensed table
│  (tap to expand)        │
└─────────────────────────┘
```

### 4.3 Key Views

**Neighborhood Overview** (default)
- Map: choropleth by selected metric (complaints, violations, rent, etc.)
- Charts: time series + category breakdown, cross-filtered with map
- Stats: aggregate numbers for selected ZIP(s)

**Building Detail** (click building or address search)
- Map: zoomed to building, nearby buildings highlighted
- Charts: violation history, complaint timeline
- Table: all violations/complaints for this BBL
- Falls back to MCP server API for full detail (too much data for WASM)

**Entity Network** (click owner name or search)
- Graph visualization of entity connections (properties owned, LLCs, donors)
- Uses existing `entity_xray` MCP tool for deep data; WASM for aggregate view
- Charts: portfolio size, violation rates across portfolio

**Election Results** (toggle to elections view)
- Map: results by council district / assembly district
- Charts: vote totals, party breakdown
- Filter: year, race type

---

## 5. Cross-Filtering with Mosaic

Mosaic's coordinator connects all views through DuckDB SQL predicates:

```typescript
// When user clicks ZIP 11201 on the map:
// 1. Mosaic creates SQL predicate: WHERE zip = '11201'
// 2. All charts re-query through DuckDB-WASM with this filter
// 3. Data table filters to ZIP 11201
// 4. Stat cards update to ZIP 11201 aggregates

const brush = useMemo(() => {
  const state = roomStore.getState();
  return state.mosaic.getSelection('zipFilter');
}, []);

// Map selection drives all charts
const mapSpec = vg.plot(
  vg.geo(vg.from('zip_boundaries', { filterBy: brush }), {
    fill: 'complaint_rate',
  }),
  vg.interactiveGeo({ as: brush, field: 'zip' }),
);

// Chart responds to map selection
const chartSpec = vg.plot(
  vg.lineY(vg.from('zip_timeseries', { filterBy: brush }), {
    x: 'month',
    y: 'complaint_count',
  }),
);
```

---

## 6. Telegram Mini App Integration

### 6.1 Environment Detection

```typescript
// src/explore/env.ts
export function isTelegram(): boolean {
  return typeof window !== 'undefined' && window.Telegram?.WebApp != null;
}

export function getTelegramStartParam(): string | null {
  if (!isTelegram()) return null;
  return window.Telegram.WebApp.initDataUnsafe?.start_param ?? null;
}
```

### 6.2 Telegram-Specific Adaptations

| Aspect | Website | Telegram Mini App |
|--------|---------|-------------------|
| **Viewport** | Full browser | Constrained WebView, use `var(--tg-viewport-stable-height)` |
| **Theme** | CG dark theme (void + warm-white) | Inherit `var(--tg-theme-*)` CSS variables, map to CG tokens |
| **State persistence** | localStorage via CRDT slice | `Telegram.WebApp.CloudStorage` (1024 items, 4KB each) |
| **Deep links** | URL params: `/explore?zip=11201&view=neighborhood` | `https://t.me/CommonGroundNYCBot/explore?startapp=zip_11201` |
| **Layout** | Multi-panel desktop grid | Single-column stack, swipeable cards |
| **Map interaction** | Click + hover tooltips | Tap only, no hover, larger touch targets |
| **Data table** | Full paginated table | Condensed top-5 with "View all" link to website |
| **Downloads** | CSV/XLSX export button | Share via `Telegram.WebApp.shareMessage()` |
| **Keyboard** | Standard | Use `Telegram.WebApp.hideKeyboard()` after search |

### 6.3 Deep Link Format

```
# Open to ZIP 11201
https://t.me/CommonGroundNYCBot/explore?startapp=zip_11201

# Open to building by BBL
https://t.me/CommonGroundNYCBot/explore?startapp=bbl_3012340001

# Open to entity
https://t.me/CommonGroundNYCBot/explore?startapp=entity_john-doe

# Open in compact mode (half-screen)
https://t.me/CommonGroundNYCBot/explore?startapp=zip_11201&mode=compact
```

Parse format: `startapp` value is `{type}_{id}` where type is `zip`, `bbl`, `entity`, or `district`.

### 6.4 CloudStorage Adapter

```typescript
// src/explore/persistence.ts
interface PersistenceAdapter {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
  removeItem(key: string): Promise<void>;
}

function createTelegramStorage(): PersistenceAdapter {
  const cs = window.Telegram.WebApp.CloudStorage;
  return {
    getItem: (key) => new Promise((resolve) => cs.getItem(key, (err, val) => resolve(val ?? null))),
    setItem: (key, value) => new Promise((resolve) => cs.setItem(key, value, () => resolve())),
    removeItem: (key) => new Promise((resolve) => cs.removeItem(key, () => resolve())),
  };
}

function createLocalStorage(): PersistenceAdapter {
  return {
    getItem: async (key) => localStorage.getItem(key),
    setItem: async (key, value) => localStorage.setItem(key, value),
    removeItem: async (key) => localStorage.removeItem(key),
  };
}

export function createPersistence(): PersistenceAdapter {
  return isTelegram() ? createTelegramStorage() : createLocalStorage();
}
```

### 6.5 Theme Bridge

```typescript
// Map Telegram theme vars to CG design tokens
function applyTelegramTheme() {
  if (!isTelegram()) return;
  const root = document.documentElement;
  // Use CG's dark theme as base, override selectively
  root.style.setProperty('--void', 'var(--tg-theme-bg-color, #0A0A0A)');
  root.style.setProperty('--warm-white', 'var(--tg-theme-text-color, #F3F1EB)');
  // Keep CG accent colors (teal, lilac, sage) — they work on most Telegram themes
}
```

---

## 7. Next.js Integration

### 7.1 Client Component Boundary

SQLRooms is entirely client-side (DuckDB-WASM runs in the browser). The `/explore` page uses a client component boundary:

```typescript
// src/app/explore/page.tsx (Server Component — metadata only)
export const metadata = { title: 'Explore NYC Data — Common Ground' };

export default function ExplorePage() {
  return (
    <Suspense fallback={<ExploreLoadingSkeleton />}>
      <ExploreDashboard />
    </Suspense>
  );
}
```

```typescript
// src/components/explore/explore-dashboard.tsx
'use client';

import { RoomProvider } from './room-provider';
import { DashboardShell } from './dashboard-shell';

export function ExploreDashboard() {
  return (
    <RoomProvider>
      <DashboardShell />
    </RoomProvider>
  );
}
```

### 7.2 Build Considerations

- DuckDB-WASM requires `SharedArrayBuffer` which needs `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp` headers
- Cloudflare Workers (via opennextjs-cloudflare) needs these headers in `next.config.ts`
- Mapbox GL JS needs to be excluded from SSR (`dynamic(() => import(...), { ssr: false })`)
- WASM files (~30 MB) should be served from CDN, not bundled

### 7.3 Coexistence with Existing Explorer

The current server-backed `ExploreView` (table browser) remains useful for raw table access. The new SQLRooms dashboard becomes the default `/explore` view, with the table browser accessible via `/explore/table?table=schema.name`.

---

## 8. Shared App Architecture

### 8.1 Package Structure

```
src/
  components/
    explore/
      room-provider.tsx        # SQLRooms store initialization
      dashboard-shell.tsx      # Layout switcher (desktop vs mobile)
      desktop-layout.tsx       # Multi-panel grid for website
      mobile-layout.tsx        # Single-column stack for Telegram
      map-panel.tsx            # Kepler map with NYC boundaries
      chart-panel.tsx          # Recharts + Mosaic cross-filtering
      stat-cards.tsx           # KPI cards (population, rent, etc.)
      filter-bar.tsx           # ZIP selector, date range, topic
      data-table-panel.tsx     # Filtered data table with download
      env.ts                   # Environment detection (web vs Telegram)
      persistence.ts           # Storage adapter (localStorage vs CloudStorage)
      theme-bridge.ts          # CG theme + Telegram theme mapping
      deep-links.ts            # URL params <-> startapp parser
```

### 8.2 Telegram Mini App Deployment

The Telegram Mini App is a separate build target pointing to the same source:
- Built as a standalone Vite app (no Next.js SSR needed)
- Deployed to a static host (Cloudflare Pages or Netlify)
- URL: `https://app.common-ground.nyc/explore`
- Registered with BotFather as a Mini App for `@CommonGroundNYCBot`

Alternatively, it can be served from the same Next.js app with a `/tg/explore` route that has the appropriate CSP headers for Telegram WebView.

---

## 9. AI Integration ("Ask a Question")

The `@sqlrooms/ai` package can power a natural language query interface:

- User types "What ZIP code has the most rat complaints?"
- AI generates SQL against the loaded Parquet tables
- Results render as a chart or table

**Implementation**: Use the AI slice with the CG MCP server as backend. The AI doesn't run SQL locally — it calls the MCP `query()` tool for complex questions that exceed the browser's aggregate data, falling back to local DuckDB for simple aggregate queries.

This is a **Phase 2** feature. The initial release focuses on the visual dashboard with cross-filtering.

---

## 10. Implementation Phases

### Phase 1: Foundation (Week 1-2)

- [ ] Install SQLRooms packages in CG website
- [ ] Create Room store with DuckDB + Kepler slices
- [ ] Build Dagster asset for aggregate Parquet generation
- [ ] Configure MinIO CORS + public bucket
- [ ] Build neighborhood overview: map + stat cards + 2 charts
- [ ] Wire cross-filtering via Mosaic coordinator
- [ ] Deploy and validate Parquet range requests work from browser

### Phase 2: Views + Polish (Week 3-4)

- [ ] Building detail view (hybrid: local aggregates + MCP API for full data)
- [ ] Entity network view (aggregate graph + MCP entity_xray fallback)
- [ ] Election results view
- [ ] CRDT persistence (remember last viewed ZIP, filters)
- [ ] Responsive design: desktop + tablet breakpoints
- [ ] Loading skeletons, error states, empty states

### Phase 3: Telegram Mini App (Week 5-6)

- [ ] Mobile layout components (single-column stack)
- [ ] Telegram environment detection + theme bridge
- [ ] CloudStorage adapter for state persistence
- [ ] Deep link parsing (startapp parameter)
- [ ] Register Mini App with BotFather
- [ ] Performance optimization for mobile WebView (lazy load, reduce Parquet sizes)
- [ ] Touch interaction adjustments (larger targets, no hover states)

### Phase 4: AI + Advanced (Week 7+)

- [ ] "Ask a question" panel with @sqlrooms/ai
- [ ] Natural language to SQL via MCP backend
- [ ] Shareable dashboard states (URL encoding of full filter state)
- [ ] OPFS caching for offline-capable repeat visits
- [ ] Telegram shareMessage() integration for sharing findings

---

## 11. Technical Risks

| Risk | Mitigation |
|------|------------|
| DuckDB-WASM memory on mobile | Keep aggregate files small (<10 MB per view). Lazy-load per view. |
| SharedArrayBuffer requires COOP/COEP headers | Configure in Cloudflare Workers. May break third-party embeds. |
| Kepler.gl bundle size (~2 MB) | Tree-shake. Consider deck.gl directly for lighter alternative. |
| MinIO public access security | Dedicated read-only bucket. Only aggregates exposed, never raw PII. |
| Telegram WebView inconsistencies | Test on iOS + Android Telegram. Use `viewportStableHeight`. |
| Parquet file staleness | Daily Dagster rebuild. ETag-based browser caching. |

---

## 12. Open Questions

1. **deck.gl vs Kepler.gl**: Kepler is higher-level but heavier. The deck.gl + Mosaic example from SQLRooms may be lighter and more customizable. Need to benchmark bundle size.
2. **Mapbox vs MapLibre**: Kepler uses Mapbox which requires a token. MapLibre is free and open source. SQLRooms Kepler slice requires Mapbox token.
3. **Telegram bot integration**: Should the Mini App be accessible from the existing CG Telegram bot, or a dedicated bot? Affects BotFather setup.
4. **Building-level data**: 1M BBL rows is ~50 MB. Should this be loaded into WASM or always fetched from MCP server API? Hybrid approach recommended.

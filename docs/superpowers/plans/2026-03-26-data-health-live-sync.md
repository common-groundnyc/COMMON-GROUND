# Data Health Live Sync — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add live source comparison to the /data health page — browser pings Socrata APIs directly to show source row counts, last updated timestamps, sync percentage, and color-coded health indicators. No cron jobs, no new infrastructure.

**Architecture:** Pure client-side. The browser already fetches our catalog stats from `/api/catalog`. We add: (1) a static `datasets-manifest.json` mapping table names to Socrata dataset IDs, (2) a React hook that lazily fetches Socrata metadata for visible tables only, (3) new columns with color-coded health badges. The Socrata API supports CORS and returns metadata in ~100ms per dataset.

**Tech Stack:** React 19, TypeScript, Socrata Metadata API (`/api/views/{id}.json`), Socrata SODA2 API (`/resource/{id}.json?$select=count(*)`), sessionStorage for caching

---

## Verified API Responses

**Socrata Metadata API** (`GET https://data.cityofnewyork.us/api/views/wvxf-dwi5.json`):
- `rowsUpdatedAt`: `1774536927` (Unix timestamp — when source data was last updated)
- `name`: `"Housing Maintenance Code Violations"`
- CORS: enabled (browser can fetch directly)

**Socrata SODA2 Count** (`GET https://data.cityofnewyork.us/resource/wvxf-dwi5.json?$select=count(*)`):
- Returns: `[{"count":"10786463"}]`
- CORS: enabled

**Domain mapping** (from `datasets.py`):
- `"nyc"` → `data.cityofnewyork.us`
- `"nys"` → `data.ny.gov`
- `"health"` → `health.data.ny.gov`
- `"cdc"` → `data.cdc.gov`

**Dataset count:** 287 Socrata datasets (manifest generated). Plus 5 non-Socrata sources with browser-callable APIs: FEC, FDA, NREL, College Scorecard, CourtListener. The remaining tables (~200) use the DuckLake file timestamp as fallback, or show "Static" for historical data.

**Provider types in manifest:**
- `"socrata"` — uses `/api/views/{id}.json` + `/resource/{id}.json?$select=count(*)`
- `"fec"` — uses `api.open.fec.gov/v1/?per_page=1` → `pagination.count`
- `"fda"` — uses `api.fda.gov/{endpoint}?limit=1` → `meta.results.total` + `meta.last_updated`
- `"nrel"` — uses `developer.nrel.gov/api/alt-fuel-stations/v1/last-updated.json`
- `"scorecard"` — uses `api.data.gov/ed/collegescorecard/v1/schools?per_page=1` → `metadata.total`
- `"static"` — historical data, never updates. Show "Static" badge instead of freshness
- `null` (no entry in manifest) — use DuckLake file UUID timestamp as "Ingested on" date

---

## Design: Color-Coded Health Indicator

Using the site's design tokens:

| Sync % | Color | Token | Meaning |
|--------|-------|-------|---------|
| >= 95% | Teal | `--truth` (#2DD4BF dark / #08796B light) | Healthy — in sync |
| >= 80% | Lilac | `--lilac` (#C4A6E8 dark / #7D5DA8 light) | Stale — needs refresh |
| < 80% | Subject | `--subject` (#e8854a dark / #B84224 light) | Warning — significantly behind |
| N/A | Muted | `--muted-dim` (#758494) | No source endpoint |

The health dot is a small filled circle (8px) next to the sync percentage.

---

## File Structure

### common-ground-website (3 files created/modified)
- Create: `public/datasets-manifest.json` — static mapping of schema.table → dataset_id + domain (generated from datasets.py)
- Create: `src/hooks/use-source-health.ts` — React hook that fetches Socrata metadata for visible tables
- Modify: `src/components/data-health-table.tsx` — add Source Rows, Sync, Updated columns + health dots

---

## Task 1: Generate datasets-manifest.json

Extract the dataset ID mapping from `datasets.py` into a static JSON file.

**Files:**
- Create: `~/Desktop/common-ground-website/public/datasets-manifest.json`

- [ ] **Step 1: Generate the manifest**

Run this Python script from the dagster-pipeline directory to extract datasets:

```python
#!/usr/bin/env python3
"""Extract datasets.py → datasets-manifest.json for the data health page."""
import json, re, sys
sys.path.insert(0, "src")
from dagster_pipeline.sources.datasets import SOCRATA_DATASETS

DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}

manifest = {}
for schema, tables in SOCRATA_DATASETS.items():
    for entry in tables:
        table_name, dataset_id = entry[0], entry[1]
        domain_key = entry[2] if len(entry) > 2 else "nyc"
        domain = DOMAINS.get(domain_key, "data.cityofnewyork.us")
        manifest[f"{schema}.{table_name}"] = {
            "id": dataset_id,
            "domain": domain,
        }

with open("datasets-manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
print(f"Exported {len(manifest)} datasets")
```

Run:
```bash
cd ~/Desktop/dagster-pipeline
uv run python -c "
import json, sys
sys.path.insert(0, 'src')
from dagster_pipeline.sources.datasets import SOCRATA_DATASETS
DOMAINS = {'nyc': 'data.cityofnewyork.us', 'nys': 'data.ny.gov', 'health': 'health.data.ny.gov', 'cdc': 'data.cdc.gov'}
manifest = {}
for schema, tables in SOCRATA_DATASETS.items():
    for entry in tables:
        table_name, dataset_id = entry[0], entry[1]
        domain_key = entry[2] if len(entry) > 2 else 'nyc'
        domain = DOMAINS.get(domain_key, 'data.cityofnewyork.us')
        manifest[f'{schema}.{table_name}'] = {'id': dataset_id, 'domain': domain}
with open('../common-ground-website/public/datasets-manifest.json', 'w') as f:
    json.dump(manifest, f, indent=2)
print(f'Exported {len(manifest)} datasets')
"
```

Expected: `Exported 208 datasets` (or similar count) and file created at `~/Desktop/common-ground-website/public/datasets-manifest.json`.

**NOTE:** The `DATASETS` dict may use a different variable name. Check `datasets.py` for the actual export name. If it's structured differently, adapt the script. The key output format must be:
```json
{
  "housing.hpd_violations": { "id": "wvxf-dwi5", "domain": "data.cityofnewyork.us" },
  "housing.hpd_complaints": { "id": "uwyv-629c", "domain": "data.cityofnewyork.us" },
  ...
}
```

- [ ] **Step 2: Verify the manifest**

```bash
cat ~/Desktop/common-ground-website/public/datasets-manifest.json | python3 -m json.tool | head -10
wc -l ~/Desktop/common-ground-website/public/datasets-manifest.json
```

- [ ] **Step 3: Commit**

```bash
cd ~/Desktop/common-ground-website
git add public/datasets-manifest.json
git commit -m "feat: add datasets-manifest.json — Socrata dataset ID mapping for data health"
```

---

## Task 2: Create useSourceHealth hook

A React hook that lazily fetches Socrata metadata for the currently visible table rows, with sessionStorage caching.

**Files:**
- Create: `~/Desktop/common-ground-website/src/hooks/use-source-health.ts`

- [ ] **Step 1: Create the hook**

```typescript
"use client";

import { useState, useEffect, useMemo } from "react";

interface SourceInfo {
  sourceRows: number | null;
  updatedAt: number | null; // Unix timestamp
  loading: boolean;
  error: boolean;
}

interface ManifestEntry {
  id: string;
  domain: string;
}

type Manifest = Record<string, ManifestEntry>;
type SourceHealthMap = Record<string, SourceInfo>;

const CACHE_KEY = "cg-source-health";
const CACHE_TTL = 1000 * 60 * 30; // 30 minutes

function getCached(): Record<string, { data: SourceInfo; ts: number }> {
  try {
    const raw = sessionStorage.getItem(CACHE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function setCache(key: string, data: SourceInfo) {
  try {
    const cache = getCached();
    cache[key] = { data, ts: Date.now() };
    sessionStorage.setItem(CACHE_KEY, JSON.stringify(cache));
  } catch {
    // sessionStorage full or unavailable — ignore
  }
}

async function fetchSourceInfo(
  datasetId: string,
  domain: string,
  signal: AbortSignal
): Promise<{ sourceRows: number | null; updatedAt: number | null }> {
  // Fetch metadata (rowsUpdatedAt) and count in parallel
  const [metaRes, countRes] = await Promise.all([
    fetch(`https://${domain}/api/views/${datasetId}.json`, { signal }),
    fetch(
      `https://${domain}/resource/${datasetId}.json?$select=count(*)`,
      { signal }
    ),
  ]);

  let updatedAt: number | null = null;
  let sourceRows: number | null = null;

  if (metaRes.ok) {
    const meta = await metaRes.json();
    updatedAt = meta.rowsUpdatedAt || null;
  }

  if (countRes.ok) {
    const countData = await countRes.json();
    if (Array.isArray(countData) && countData[0]?.count) {
      sourceRows = parseInt(countData[0].count, 10);
    }
  }

  return { sourceRows, updatedAt };
}

export function useSourceHealth(
  visibleKeys: string[] // e.g., ["housing.hpd_violations", "health.restaurant_inspections"]
): { health: SourceHealthMap; manifest: Manifest | null } {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [health, setHealth] = useState<SourceHealthMap>({});

  // Load manifest once
  useEffect(() => {
    fetch("/datasets-manifest.json")
      .then((r) => r.json())
      .then(setManifest)
      .catch(() => setManifest(null));
  }, []);

  // Stable string key for dependency tracking
  const visibleKeysStr = useMemo(() => visibleKeys.join(","), [visibleKeys]);

  // Fetch source info for visible keys
  useEffect(() => {
    if (!manifest) return;

    const controller = new AbortController();
    const cache = getCached();
    const now = Date.now();
    const toFetch: string[] = [];

    // Check cache first
    for (const key of visibleKeys) {
      if (!manifest[key]) continue;
      const cached = cache[key];
      if (cached && now - cached.ts < CACHE_TTL) {
        // Use cached value
        setHealth((prev) => ({ ...prev, [key]: cached.data }));
      } else {
        toFetch.push(key);
        // Mark as loading
        setHealth((prev) => ({
          ...prev,
          [key]: { sourceRows: null, updatedAt: null, loading: true, error: false },
        }));
      }
    }

    // Fetch uncached entries (max 10 concurrent to avoid hammering Socrata)
    const fetchBatch = async () => {
      const batchSize = 10;
      for (let i = 0; i < toFetch.length; i += batchSize) {
        const batch = toFetch.slice(i, i + batchSize);
        const results = await Promise.allSettled(
          batch.map(async (key) => {
            const entry = manifest[key]!;
            const result = await fetchSourceInfo(
              entry.id,
              entry.domain,
              controller.signal
            );
            const info: SourceInfo = {
              ...result,
              loading: false,
              error: false,
            };
            setCache(key, info);
            setHealth((prev) => ({ ...prev, [key]: info }));
            return { key, info };
          })
        );
        // Mark failed ones
        for (let j = 0; j < results.length; j++) {
          if (results[j].status === "rejected") {
            const key = batch[j];
            const info: SourceInfo = {
              sourceRows: null,
              updatedAt: null,
              loading: false,
              error: true,
            };
            setHealth((prev) => ({ ...prev, [key]: info }));
          }
        }
      }
    };

    fetchBatch();

    return () => controller.abort();
  // eslint-disable-next-line react-hooks/exhaustive-deps -- visibleKeysStr is stable
  }, [manifest, visibleKeysStr]);

  return { health, manifest };
}
```

- [ ] **Step 2: Commit**

```bash
cd ~/Desktop/common-ground-website
git add src/hooks/use-source-health.ts
git commit -m "feat: add useSourceHealth hook — lazy Socrata metadata fetcher with sessionStorage cache"
```

---

## Task 3: Update DataHealthTable with source columns and health indicators

**Files:**
- Modify: `~/Desktop/common-ground-website/src/components/data-health-table.tsx`

- [ ] **Step 1: Add import and hook usage**

At the top of the file, after the existing imports, add:

```typescript
import { useSourceHealth } from "@/hooks/use-source-health";
```

Inside the `DataHealthTable` component, after the existing state declarations (after `const [page, setPage] = useState(0);`), add:

```typescript
  // Source health: lazy-fetch Socrata metadata for visible tables
  const visibleKeys = useMemo(
    () => paged.map((t) => `${t.schema}.${t.table}`),
    [paged]
  );
  const { health, manifest } = useSourceHealth(visibleKeys);
```

- [ ] **Step 2: Add health indicator helper functions**

After the existing `formatBytes` function, add:

```typescript
function syncPct(ourRows: number, sourceRows: number | null): number | null {
  if (!sourceRows || sourceRows === 0) return null;
  return Math.min(100, Math.round((ourRows / sourceRows) * 100));
}

function healthColor(pct: number | null): string {
  if (pct === null) return "var(--muted-dim)";
  if (pct >= 95) return "var(--truth)";
  if (pct >= 80) return "var(--lilac)";
  return "var(--subject)";
}

function healthLabel(pct: number | null): string {
  if (pct === null) return "";
  if (pct >= 95) return "Healthy";
  if (pct >= 80) return "Stale";
  return "Behind";
}

function formatUnixTime(ts: number | null): string {
  if (!ts) return "\u2014";
  const diff = Date.now() - ts * 1000;
  const hours = Math.floor(diff / 3600000);
  if (hours < 1) return "< 1h ago";
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  return `${months}mo ago`;
}
```

- [ ] **Step 3: Update summary cards**

In the summary cards array, add a "Synced" card. Find the existing cards array:
```typescript
        {[
          { label: "Schemas", value: data.summary.schemas },
          { label: "Tables", value: data.summary.tables },
          {
            label: "Total Rows",
            value: formatNumber(data.summary.total_rows),
          },
          { label: "Last Refresh", value: timeAgo(data.as_of) },
        ].map((card) => (
```

Replace with:
```typescript
        {[
          { label: "Schemas", value: data.summary.schemas },
          { label: "Tables", value: data.summary.tables },
          {
            label: "Total Rows",
            value: formatNumber(data.summary.total_rows),
          },
          {
            label: "Socrata Sources",
            value: manifest ? Object.keys(manifest).length : "\u2014",
          },
          { label: "Last Refresh", value: timeAgo(data.as_of) },
        ].map((card) => (
```

- [ ] **Step 4: Update SortKey type AND table headers**

First, update the `SortKey` type (removing `"columns"`) and the sort default check:
```typescript
type SortKey = "schema" | "table" | "rows" | "size_bytes";
```
And in `toggleSort`, update:
```typescript
      setSortDir(key === "rows" || key === "size_bytes" ? "desc" : "asc");
```

Then replace the column headers array:
```typescript
              {(
                [
                  ["schema", "Schema"],
                  ["table", "Table"],
                  ["rows", "Rows"],
                  ["columns", "Cols"],
                  ["size_bytes", "Size"],
                ] as [SortKey, string][]
```

With (note: Source Rows and Updated are NOT sortable since they're async-loaded):
```typescript
              {(
                [
                  ["schema", "Schema"],
                  ["table", "Table"],
                  ["rows", "Our Rows"],
                  ["size_bytes", "Size"],
                ] as [SortKey, string][]
              ).map(([key, label]) => (
                <th
                  key={key}
                  onClick={() => toggleSort(key)}
                  style={{
                    padding: "10px 16px",
                    textAlign: key === "rows" || key === "size_bytes" ? "right" : "left",
                    cursor: "pointer",
                    color: "var(--muted)",
                    fontSize: "11px",
                    textTransform: "uppercase",
                    letterSpacing: "0.08em",
                    fontWeight: 600,
                    userSelect: "none",
                    whiteSpace: "nowrap",
                  }}
                >
                  {label}
                  {sortArrow(key)}
                </th>
              ))}
              {/* Non-sortable source columns */}
              <th style={{ padding: "10px 16px", textAlign: "right", color: "var(--muted)", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.08em", fontWeight: 600, whiteSpace: "nowrap" }}>
                Source
              </th>
              <th style={{ padding: "10px 16px", textAlign: "center", color: "var(--muted)", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.08em", fontWeight: 600, whiteSpace: "nowrap" }}>
                Sync
              </th>
              <th style={{ padding: "10px 16px", textAlign: "right", color: "var(--muted)", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.08em", fontWeight: 600, whiteSpace: "nowrap" }}>
                Updated
              </th>
```

Remove the old `.map()` closing and the Cols column (replaced by the new layout above).

- [ ] **Step 5: Update table body rows**

Replace the existing `<td>` cells inside the `paged.map()` with:

```tsx
              <tr
                key={`${row.schema}.${row.table}`}
                style={{
                  borderBottom:
                    i < paged.length - 1
                      ? "1px solid var(--border)"
                      : "none",
                }}
              >
                <td style={{ padding: "8px 16px", color: "var(--truth)", fontWeight: 600 }}>
                  {row.schema}
                </td>
                <td style={{ padding: "8px 16px", color: "var(--warm-white)" }}>
                  {row.table}
                </td>
                <td style={{ padding: "8px 16px", textAlign: "right", color: "var(--warm-white)", fontVariantNumeric: "tabular-nums" }}>
                  {formatNumber(row.rows)}
                </td>
                <td style={{ padding: "8px 16px", textAlign: "right", color: "var(--muted)", fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}>
                  {row.size_bytes ? formatBytes(row.size_bytes) : "\u2014"}
                </td>
                {/* Source row count */}
                {(() => {
                  const key = `${row.schema}.${row.table}`;
                  const src = health[key];
                  const hasSrc = manifest && manifest[key];
                  const pct = src ? syncPct(row.rows, src.sourceRows) : null;
                  const color = healthColor(pct);
                  return (
                    <>
                      <td style={{ padding: "8px 16px", textAlign: "right", fontVariantNumeric: "tabular-nums", color: "var(--muted)" }}>
                        {!hasSrc ? "\u2014" : src?.loading ? (
                          <span style={{ color: "var(--muted-dim)" }}>...</span>
                        ) : src?.sourceRows != null ? (
                          formatNumber(src.sourceRows)
                        ) : "\u2014"}
                      </td>
                      <td style={{ padding: "8px 16px", textAlign: "center", whiteSpace: "nowrap" }}>
                        {pct !== null ? (
                          <span style={{ display: "inline-flex", alignItems: "center", gap: "6px" }}>
                            <span style={{
                              width: "8px",
                              height: "8px",
                              borderRadius: "50%",
                              background: color,
                              display: "inline-block",
                              flexShrink: 0,
                            }} />
                            <span style={{ color, fontWeight: 600, fontSize: "12px" }}>
                              {pct}%
                            </span>
                          </span>
                        ) : hasSrc && src?.loading ? (
                          <span style={{ color: "var(--muted-dim)" }}>...</span>
                        ) : (
                          <span style={{ color: "var(--muted-dim)" }}>\u2014</span>
                        )}
                      </td>
                      <td style={{ padding: "8px 16px", textAlign: "right", color: "var(--muted)", fontSize: "12px", whiteSpace: "nowrap" }}>
                        {src?.updatedAt ? formatUnixTime(src.updatedAt) : hasSrc && src?.loading ? (
                          <span style={{ color: "var(--muted-dim)" }}>...</span>
                        ) : "\u2014"}
                      </td>
                    </>
                  );
                })()}
              </tr>
```

- [ ] **Step 6: Update empty state colSpan**

Find the empty state `<td colSpan={5}` and change to `colSpan={7}` (Schema + Table + Our Rows + Size + Source + Sync + Updated = 7).

- [ ] **Step 7: Commit**

```bash
cd ~/Desktop/common-ground-website
git add src/components/data-health-table.tsx
git commit -m "feat: add source comparison columns with color-coded health indicators"
```

---

## Task 4: Deploy and smoke test

- [ ] **Step 1: Deploy website**

```bash
cd ~/Desktop/common-ground-website
npm run deploy
```

- [ ] **Step 2: Smoke test**

Visit `https://common-ground.nyc/data`

Verify:
- Summary cards include "Socrata Sources" count (~208)
- Table has 7 columns: Schema, Table, Our Rows, Size, Source, Sync, Updated
- Source column shows "..." while loading, then row counts
- Sync column shows colored dots: teal (>=95%), lilac (>=80%), orange (<80%)
- Updated column shows relative times ("2h ago", "5d ago")
- Non-Socrata tables (federal schema) show dashes for Source/Sync/Updated
- Navigating pages triggers new Socrata fetches for visible rows
- Going back to a previous page uses cached values (no re-fetch)
- Search and filter work with all columns

- [ ] **Step 3: Commit**

```bash
cd ~/Desktop/common-ground-website
git add -A
git commit -m "feat: data health page with live Socrata sync comparison"
```

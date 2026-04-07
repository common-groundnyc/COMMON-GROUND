# Data Health Page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a public `/data` page to common-ground.nyc showing live health status of all 500+ tables in the NYC data lake — schema, table name, row count, column count, freshness badge — with search, filters, sort, and pagination.

**Architecture:** Two-part build: (1) Add a `/api/catalog` JSON endpoint to the MCP server that returns table stats from the DuckLake catalog, (2) Build a Next.js page at `/data` on the website that fetches this JSON and renders an interactive table using the site's existing design system (CSS variables, Tailwind 4, JetBrains Mono). No shadcn — the site has its own bespoke design tokens.

**Tech Stack:** Python (FastMCP server), Next.js 16, React 19, Tailwind CSS 4, TypeScript, Cloudflare deployment

---

## Codebase Context

### MCP Server (`~/Desktop/dagster-pipeline/infra/duckdb-server/mcp_server.py`)
- FastMCP server, ~11,400 lines
- Already computes catalog at startup: "Catalog cached: 504 tables across 28 schemas"
- Has `_safe_query(db, sql, params)` and `_execute(db, sql, params)` helpers
- Runs on Uvicorn at port 4213 behind nginx SSL proxy on port 9999
- The existing `start.sh` configures nginx to proxy `/mcp` to the FastMCP app
- To add a raw HTTP endpoint, we need to add a route to the underlying ASGI app

### Website (`~/Desktop/common-ground-website/`)
- Next.js 16 + React 19 + Tailwind 4
- Deployed to Cloudflare via `opennextjs-cloudflare`
- App router at `src/app/`
- Existing pages: `/`, `/light`, `/connect`, `/forecast`, `/how`, `/receipts`, `/supporters`
- NO shadcn, NO API routes — bespoke design system
- CSS variables: `--void` (bg), `--warm-white` (text), `--truth` (accent/teal), `--muted`, `--border`, `--paper-bright`, etc.
- Fonts: JetBrains Mono (monospace), Space Grotesk (sans), Atkinson Hyperlegible
- Dark/light mode via `[data-theme="dark"]`
- Uses inline styles + Tailwind utility classes

### Design Tokens (from globals.css)
```
Light mode:                    Dark mode:
--void: #F5F0E8               --void: #0A0A0A
--warm-white: #1A1A1A         --warm-white: #F3F1EB
--truth: #08796B              --truth: #2DD4BF
--cta: #2D6B55                --cta: #6B9C8A
--muted: #6B6B6B              --muted: #A7ADB7
--border: rgba(26,26,26,0.12) --border: rgba(243,241,235,0.12)
--paper-bright: #FAF8F3       --paper-bright: #0A0A0A
--subject: #B84224            --subject: #e8854a
```

---

## File Structure

### MCP Server (1 file modified)
- Modify: `infra/duckdb-server/mcp_server.py` — add a `/api/catalog` HTTP route

### Website (2 files created)
- Create: `src/app/data/page.tsx` — the data health page (server component that fetches catalog JSON)
- Create: `src/components/data-health-table.tsx` — interactive client component (search, filter, sort, paginate)

---

## Task 1: Add `/api/catalog` JSON endpoint to MCP server

Use `@mcp.custom_route` (already exists in codebase — see the `/health` endpoint) to add a catalog JSON endpoint. This goes through the Cloudflare tunnel automatically.

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`

- [ ] **Step 1: Find the existing custom_route pattern**

Search for `@mcp.custom_route("/health"` in `mcp_server.py` to see how the `/health` endpoint is implemented. Use the same pattern for `/api/catalog`. Note how it accesses the database connection.

- [ ] **Step 2: Add the catalog endpoint**

Add this near the existing `/health` custom route:

```python
# ---------------------------------------------------------------------------
# Public HTTP API — catalog endpoint for data health page
# ---------------------------------------------------------------------------

import json
import datetime as _dt

@mcp.custom_route("/api/catalog", methods=["GET", "OPTIONS"])
async def catalog_json(request):
    """Return table catalog as JSON for the data health page."""
    from starlette.responses import JSONResponse

    # Handle CORS preflight
    if request.method == "OPTIONS":
        return JSONResponse(
            {},
            status_code=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Max-Age": "86400",
            },
        )

    db = None
    try:
        db = _connect()  # Use the same connection function as the lifespan

        # Query table stats from DuckDB metadata
        # ducklake_table_info() returns file-level metadata (file_count, file_size_bytes, table_uuid)
        # NOTE: Verify ducklake_table_info() column names at implementation time.
        # If it fails, fall back to duckdb_tables() only (without file/uuid columns).
        cols, rows = _execute(db, """
            SELECT
                t.schema_name,
                t.table_name,
                t.estimated_size,
                (SELECT COUNT(*) FROM duckdb_columns() c
                 WHERE c.schema_name = t.schema_name AND c.table_name = t.table_name) AS column_count
            FROM duckdb_tables() t
            WHERE t.schema_name NOT LIKE '%staging%'
              AND t.schema_name NOT LIKE 'test%'
              AND t.schema_name NOT LIKE 'ducklake%'
              AND t.schema_name NOT LIKE 'information%'
            ORDER BY t.schema_name, t.estimated_size DESC
        """)

        # Try to get DuckLake file metadata separately (may fail if schema differs)
        ducklake_info = {}
        try:
            _, dl_rows = _execute(db, "SELECT table_name, file_count, file_size_bytes, table_uuid FROM ducklake_table_info('lake')")
            for dlr in dl_rows:
                ducklake_info[dlr[0]] = {"file_count": dlr[1], "file_size_bytes": dlr[2], "table_uuid": str(dlr[3]) if dlr[3] else None}
        except Exception:
            pass  # DuckLake metadata unavailable — continue without it

        tables = []
        schema_stats = {}
        total_rows = 0

        for row in rows:
            schema, table, est_size, col_count = row[:4]
            if table.startswith('_dlt_') or table.startswith('_pipeline'):
                continue

            dl = ducklake_info.get(table, {})
            file_count = dl.get("file_count", 0)
            file_size = dl.get("file_size_bytes", 0)
            table_uuid = dl.get("table_uuid")

            # Extract creation timestamp from UUIDv7 (first 48 bits = epoch ms)
            created_at = None
            if table_uuid:
                try:
                    hex_str = table_uuid.replace('-', '')
                    if len(hex_str) >= 13 and hex_str[12] == '7':  # UUIDv7 version check
                        epoch_ms = int(hex_str[:12], 16)
                        created_at = _dt.datetime.fromtimestamp(epoch_ms / 1000, tz=_dt.timezone.utc).isoformat()
                except (ValueError, OSError):
                    pass

            tables.append({
                "schema": schema,
                "table": table,
                "rows": est_size or 0,
                "columns": col_count or 0,
                "files": file_count or 0,
                "size_bytes": file_size or 0,
                "created_at": created_at,
            })
            total_rows += (est_size or 0)
            if schema not in schema_stats:
                schema_stats[schema] = {"tables": 0, "rows": 0}
            schema_stats[schema]["tables"] += 1
            schema_stats[schema]["rows"] += (est_size or 0)

        result = {
            "as_of": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "summary": {
                "schemas": len(schema_stats),
                "tables": len(tables),
                "total_rows": total_rows,
            },
            "schemas": schema_stats,
            "tables": tables,
        }

        return JSONResponse(
            result,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=300",
            },
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        if db:
            db.close()
```

**IMPORTANT:** The `_connect()` function name needs verification. Search for how the `/health` endpoint or the lifespan creates DB connections. It may be called `_connect_ducklake()`, `_create_connection()`, or inline code. Match that pattern exactly.

- [ ] **Step 3: Test the endpoint**

```bash
# After deploying, verify
curl -s https://mcp.common-ground.nyc/api/catalog | python3 -m json.tool | head -20
```

Expected: valid JSON with `as_of`, `summary`, `schemas`, `tables` keys.

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py
git commit -m "feat(mcp): add /api/catalog JSON endpoint for data health page"
```

---

## Task 2: Build the data health table component

**Files:**
- Create: `~/Desktop/common-ground-website/src/components/data-health-table.tsx`

- [ ] **Step 1: Create the interactive table component**

This is a client component with search, schema filter, sort, and pagination. Uses the site's existing design tokens.

```tsx
"use client";

import { useState, useMemo } from "react";

interface TableRow {
  schema: string;
  table: string;
  rows: number;
  columns: number;
  files: number;
  size_bytes: number;
  created_at: string | null;
}

interface CatalogData {
  as_of: string;
  summary: { schemas: number; tables: number; total_rows: number };
  schemas: Record<string, { tables: number; rows: number }>;
  tables: TableRow[];
}

const PAGE_SIZE = 25;

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function timeAgo(isoString: string): string {
  const diff = Date.now() - new Date(isoString).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatBytes(b: number): string {
  if (b >= 1_000_000_000) return `${(b / 1_000_000_000).toFixed(1)} GB`;
  if (b >= 1_000_000) return `${(b / 1_000_000).toFixed(1)} MB`;
  if (b >= 1_000) return `${(b / 1_000).toFixed(1)} KB`;
  return `${b} B`;
}

type SortKey = "schema" | "table" | "rows" | "columns" | "size_bytes";
type SortDir = "asc" | "desc";

export function DataHealthTable({ data }: { data: CatalogData }) {
  const [search, setSearch] = useState("");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [sortKey, setSortKey] = useState<SortKey>("rows");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [page, setPage] = useState(0);

  const schemas = useMemo(
    () => [...new Set(data.tables.map((t) => t.schema))].sort(),
    [data.tables]
  );

  const filtered = useMemo(() => {
    let rows = data.tables;
    if (schemaFilter !== "all") {
      rows = rows.filter((r) => r.schema === schemaFilter);
    }
    if (search) {
      const q = search.toLowerCase();
      rows = rows.filter(
        (r) =>
          r.table.toLowerCase().includes(q) ||
          r.schema.toLowerCase().includes(q)
      );
    }
    rows = [...rows].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === "number" && typeof bv === "number") {
        return sortDir === "asc" ? av - bv : bv - av;
      }
      return sortDir === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    return rows;
  }, [data.tables, schemaFilter, search, sortKey, sortDir]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const paged = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "rows" || key === "columns" || key === "size_bytes" ? "desc" : "asc");
    }
    setPage(0);
  }

  const sortArrow = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " \u2191" : " \u2193") : "";

  return (
    <div>
      {/* Summary cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: "16px",
          marginBottom: "32px",
        }}
      >
        {[
          { label: "Schemas", value: data.summary.schemas },
          { label: "Tables", value: data.summary.tables },
          {
            label: "Total Rows",
            value: formatNumber(data.summary.total_rows),
          },
          { label: "Last Refresh", value: timeAgo(data.as_of) },
        ].map((card) => (
          <div
            key={card.label}
            style={{
              background: "var(--paper-bright)",
              border: "1px solid var(--border)",
              borderRadius: "8px",
              padding: "16px 20px",
            }}
          >
            <div
              style={{
                fontSize: "11px",
                textTransform: "uppercase",
                letterSpacing: "0.08em",
                color: "var(--muted)",
                marginBottom: "4px",
              }}
            >
              {card.label}
            </div>
            <div
              style={{
                fontSize: "28px",
                fontWeight: 700,
                fontFamily: "var(--font-space-grotesk), sans-serif",
                color: "var(--warm-white)",
              }}
            >
              {card.value}
            </div>
          </div>
        ))}
      </div>

      {/* Controls */}
      <div
        style={{
          display: "flex",
          gap: "12px",
          marginBottom: "16px",
          flexWrap: "wrap",
        }}
      >
        <input
          type="text"
          placeholder="Search tables..."
          value={search}
          onChange={(e) => {
            setSearch(e.target.value);
            setPage(0);
          }}
          style={{
            flex: 1,
            minWidth: "200px",
            padding: "8px 12px",
            background: "var(--paper-bright)",
            border: "1px solid var(--border)",
            borderRadius: "6px",
            color: "var(--warm-white)",
            fontFamily: "var(--font-jetbrains), monospace",
            fontSize: "13px",
            outline: "none",
          }}
        />
        <select
          value={schemaFilter}
          onChange={(e) => {
            setSchemaFilter(e.target.value);
            setPage(0);
          }}
          style={{
            padding: "8px 12px",
            background: "var(--paper-bright)",
            border: "1px solid var(--border)",
            borderRadius: "6px",
            color: "var(--warm-white)",
            fontFamily: "var(--font-jetbrains), monospace",
            fontSize: "13px",
            cursor: "pointer",
          }}
        >
          <option value="all">All schemas ({schemas.length})</option>
          {schemas.map((s) => (
            <option key={s} value={s}>
              {s} ({data.schemas[s]?.tables || 0})
            </option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div
        style={{
          overflowX: "auto",
          border: "1px solid var(--border)",
          borderRadius: "8px",
        }}
      >
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontFamily: "var(--font-jetbrains), monospace",
            fontSize: "13px",
          }}
        >
          <thead>
            <tr
              style={{
                borderBottom: "1px solid var(--border)",
                background: "var(--paper-bright)",
              }}
            >
              {(
                [
                  ["schema", "Schema"],
                  ["table", "Table"],
                  ["rows", "Rows"],
                  ["columns", "Cols"],
                  ["size_bytes", "Size"],
                ] as [SortKey, string][]
              ).map(([key, label]) => (
                <th
                  key={key}
                  onClick={() => toggleSort(key)}
                  style={{
                    padding: "10px 16px",
                    textAlign: key === "rows" || key === "columns" || key === "size_bytes" ? "right" : "left",
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
            </tr>
          </thead>
          <tbody>
            {paged.length === 0 && (
              <tr>
                <td colSpan={5} style={{ padding: "32px 16px", textAlign: "center", color: "var(--muted)" }}>
                  {data.tables.length === 0 ? "No catalog data available." : "No tables match your search."}
                </td>
              </tr>
            )}
            {paged.map((row, i) => (
              <tr
                key={`${row.schema}.${row.table}`}
                style={{
                  borderBottom:
                    i < paged.length - 1
                      ? "1px solid var(--border)"
                      : "none",
                }}
              >
                <td
                  style={{
                    padding: "8px 16px",
                    color: "var(--truth)",
                    fontWeight: 600,
                  }}
                >
                  {row.schema}
                </td>
                <td style={{ padding: "8px 16px", color: "var(--warm-white)" }}>
                  {row.table}
                </td>
                <td
                  style={{
                    padding: "8px 16px",
                    textAlign: "right",
                    color: "var(--warm-white)",
                    fontVariantNumeric: "tabular-nums",
                  }}
                >
                  {formatNumber(row.rows)}
                </td>
                <td
                  style={{
                    padding: "8px 16px",
                    textAlign: "right",
                    color: "var(--muted)",
                    fontVariantNumeric: "tabular-nums",
                  }}
                >
                  {row.columns}
                </td>
                <td
                  style={{
                    padding: "8px 16px",
                    textAlign: "right",
                    color: "var(--muted)",
                    fontVariantNumeric: "tabular-nums",
                    whiteSpace: "nowrap",
                  }}
                >
                  {row.size_bytes ? formatBytes(row.size_bytes) : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginTop: "16px",
            fontSize: "12px",
            color: "var(--muted)",
          }}
        >
          <span>
            {filtered.length} tables | Page {page + 1} of {totalPages}
          </span>
          <div style={{ display: "flex", gap: "8px" }}>
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              style={{
                padding: "6px 14px",
                background: "var(--paper-bright)",
                border: "1px solid var(--border)",
                borderRadius: "4px",
                color: page === 0 ? "var(--muted-dim)" : "var(--warm-white)",
                cursor: page === 0 ? "default" : "pointer",
                fontFamily: "var(--font-jetbrains), monospace",
                fontSize: "12px",
              }}
            >
              Prev
            </button>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              style={{
                padding: "6px 14px",
                background: "var(--paper-bright)",
                border: "1px solid var(--border)",
                borderRadius: "4px",
                color:
                  page >= totalPages - 1
                    ? "var(--muted-dim)"
                    : "var(--warm-white)",
                cursor: page >= totalPages - 1 ? "default" : "pointer",
                fontFamily: "var(--font-jetbrains), monospace",
                fontSize: "12px",
              }}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd ~/Desktop/common-ground-website
git add src/components/data-health-table.tsx
git commit -m "feat: add DataHealthTable component with search, filter, sort, pagination"
```

---

## Task 3: Build the `/data` page

**Files:**
- Create: `~/Desktop/common-ground-website/src/app/data/page.tsx`

- [ ] **Step 1: Create the page**

This is a server component that fetches the catalog JSON at request time (or build time with ISR).

```tsx
import { DataHealthTable } from "@/components/data-health-table";

const CATALOG_URL = "https://mcp.common-ground.nyc/api/catalog";

// Revalidate every 5 minutes
export const revalidate = 300;

export const metadata = {
  title: "Data Health | Common Ground NYC",
  description:
    "Live status of 500+ public datasets powering Common Ground NYC. Row counts, schemas, and freshness for every table in the data lake.",
};

async function getCatalog() {
  try {
    const res = await fetch(CATALOG_URL, { next: { revalidate: 300 } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  } catch {
    // Fallback: return empty data so the page still renders
    return {
      as_of: new Date().toISOString(),
      summary: { schemas: 0, tables: 0, total_rows: 0 },
      schemas: {},
      tables: [],
    };
  }
}

export default async function DataPage() {
  const catalog = await getCatalog();

  return (
    <div
      style={{
        background: "var(--void)",
        color: "var(--warm-white)",
        minHeight: "100vh",
        fontFamily: "var(--font-jetbrains), monospace",
      }}
    >
      <main
        id="main-content"
        style={{ maxWidth: "1200px", margin: "0 auto", padding: "80px 40px" }}
      >
        <header style={{ marginBottom: "48px" }}>
          <h1
            style={{
              fontSize: "32px",
              fontWeight: 700,
              fontFamily: "var(--font-space-grotesk), sans-serif",
              marginBottom: "12px",
            }}
          >
            Data Health
          </h1>
          <p style={{ color: "var(--muted)", fontSize: "15px", maxWidth: "600px" }}>
            Live status of every table in the NYC open data lake. Updated on
            each pipeline run.
          </p>
        </header>

        <DataHealthTable data={catalog} />

        <footer
          style={{
            marginTop: "64px",
            paddingTop: "24px",
            borderTop: "1px solid var(--border)",
            fontSize: "12px",
            color: "var(--muted-dim)",
          }}
        >
          <p>
            Powered by{" "}
            <a
              href="https://ducklake.select"
              style={{ color: "var(--truth)", textDecoration: "none" }}
            >
              DuckLake
            </a>{" "}
            +{" "}
            <a
              href="https://dagster.io"
              style={{ color: "var(--truth)", textDecoration: "none" }}
            >
              Dagster
            </a>
            . Catalog refreshes every 5 minutes.
          </p>
        </footer>
      </main>
    </div>
  );
}
```

- [ ] **Step 2: Test locally**

```bash
cd ~/Desktop/common-ground-website
npm run dev
# Open http://localhost:3002/data
```

Expected: Page renders with summary cards and table. If the MCP server catalog endpoint isn't live yet, it shows the empty fallback gracefully.

- [ ] **Step 3: Commit**

```bash
git add src/app/data/page.tsx
git commit -m "feat: add /data page — live data health dashboard"
```

---

## Task 4: Deploy and smoke test

- [ ] **Step 1: Deploy website to Cloudflare**

```bash
cd ~/Desktop/common-ground-website
npm run deploy
```

- [ ] **Step 2: Smoke test**

Visit `https://common-ground.nyc/data`

Verify:
- Summary cards show correct numbers (14 schemas, ~500 tables, ~1.1B rows)
- Search works (type "hpd" → filters to HPD tables)
- Schema dropdown filters correctly
- Column headers are sortable (click "Rows" to sort by size)
- Pagination works (25 per page, prev/next buttons)
- Dark mode works (if the site has a theme toggle)
- Mobile responsive (table scrolls horizontally)

- [ ] **Step 3: Final commit**

```bash
cd ~/Desktop/common-ground-website
git add -A
git commit -m "feat: data health page live at /data"
```

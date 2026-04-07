# Branded XLSX Export with Hyperlinks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace CSV export with branded XLSX files featuring Common Ground styling, frozen headers, percentile color scales, and clickable hyperlinks to the original source record (Socrata, ACRIS, DOB BIS, WhoOwnsWhat).

**Architecture:** A new `xlsx_export.py` module generates Excel files using openpyxl with branded header rows, hyperlinked data cells, and conditional formatting for percentile columns. A `source_links.py` module maps table names to Socrata dataset IDs and builds row-level deep-link URLs. The existing `export_csv` MCP tool is upgraded to `export_data` which generates XLSX by default (CSV as fallback). Each task starts with Exa research.

**Tech Stack:** openpyxl (pure Python XLSX), FastMCP 3.1.1, DuckDB, Python 3.12

---

## What the User Gets

An XLSX file that opens in Excel/Sheets/Numbers with:

```
┌─────────────────────────────────────────────────────────┐
│  COMMON GROUND — NYC Open Data Export                   │  ← Row 1: dark blue, white text, merged
│  common-ground.nyc | Generated 2026-03-27 | 100 rows   │  ← Row 2: metadata
├────┬────────────┬────────┬─────────┬──────┬─────────────┤
│ #  │ BBL        │ Viol.  │ Open    │ Pctl │ Source      │  ← Row 3: frozen header, bold
├────┼────────────┼────────┼─────────┼──────┼─────────────┤
│ 1  │ 3042719001 │ 10,396 │  2,642  │ 100  │ View →      │  ← Hyperlinked to Socrata/WhoOwnsWhat
│ 2  │ 4159260001 │  9,322 │    603  │ 100  │ View →      │
│ ...│            │        │         │      │             │
├────┴────────────┴────────┴─────────┴──────┴─────────────┤
│  Data: NYC public records | Powered by DuckDB + DuckLake│  ← Footer
│  common-ground.nyc                                      │
└─────────────────────────────────────────────────────────┘
```

- **Percentile columns** get green→yellow→red color scale
- **BBL columns** hyperlink to WhoOwnsWhat
- **Source column** added as last column — hyperlinks to original Socrata record
- **Header frozen** so it stays visible while scrolling

---

## File Structure

### New files

| File | Responsibility |
|------|----------------|
| `infra/duckdb-server/source_links.py` | `DATASET_URLS` map (table→Socrata ID+domain), `build_source_url()` for row-level links, `build_bbl_urls()` for building links |
| `infra/duckdb-server/xlsx_export.py` | `generate_branded_xlsx()` — builds openpyxl Workbook with branding, hyperlinks, color scales |
| `infra/duckdb-server/tests/test_source_links.py` | Tests for URL generation |
| `infra/duckdb-server/tests/test_xlsx_export.py` | Tests for XLSX generation |

### Modified files

| File | Change |
|------|--------|
| `infra/duckdb-server/mcp_server.py` | Rename `export_csv` → `export_data`, generate XLSX instead of CSV |
| `infra/duckdb-server/Dockerfile` | Add `openpyxl` to pip install |

---

## Task 1: Build source link URL generator

**Files:**
- Create: `infra/duckdb-server/source_links.py`
- Create: `infra/duckdb-server/tests/test_source_links.py`

- [ ] **Step 1: Research — Socrata filtered view URLs**

Search with Exa:
1. `Socrata "$where" filter URL browseable "data.cityofnewyork.us"` — confirm filter URL format
2. `"whoownswhat.justfix.org" URL format BBL integer block lot` — WhoOwnsWhat URL with integer block/lot

- [ ] **Step 2: Write failing tests**

```python
# tests/test_source_links.py
import pytest
from source_links import build_source_url, build_bbl_urls, parse_bbl, DATASET_URLS

class TestDatasetUrls:
    def test_has_hpd_violations(self):
        assert "hpd_violations" in DATASET_URLS
        assert DATASET_URLS["hpd_violations"]["id"] == "wvxf-dwi5"

    def test_has_restaurant_inspections(self):
        assert "restaurant_inspections" in DATASET_URLS

    def test_has_n311_service_requests(self):
        assert "n311_service_requests" in DATASET_URLS

class TestBuildSourceUrl:
    def test_dataset_page_fallback(self):
        url = build_source_url("hpd_violations", {})
        assert "data.cityofnewyork.us/d/wvxf-dwi5" in url

    def test_row_filter_by_key(self):
        url = build_source_url("hpd_violations", {"violationid": "12345"})
        assert "violationid" in url
        assert "12345" in url

    def test_restaurant_by_camis(self):
        url = build_source_url("restaurant_inspections", {"camis": "50089474"})
        assert "camis" in url
        assert "50089474" in url

    def test_unknown_table_returns_none(self):
        assert build_source_url("fake_table", {}) is None

    def test_nys_domain(self):
        url = build_source_url("nys_corporations", {})
        assert "data.ny.gov" in url

class TestParseBbl:
    def test_parse_10_digit(self):
        boro, block, lot = parse_bbl("3012340056")
        assert boro == "3"
        assert block == "01234"
        assert lot == "0056"

    def test_returns_none_for_invalid(self):
        assert parse_bbl("bad") is None
        assert parse_bbl("") is None

class TestBuildBblUrls:
    def test_whoownswhat(self):
        urls = build_bbl_urls("3012340056")
        assert any("whoownswhat" in u["url"] for u in urls)
        wow = next(u for u in urls if "whoownswhat" in u["url"])
        assert "/3/1234/56" in wow["url"]  # integer block/lot, no leading zeros

    def test_dob_bis(self):
        urls = build_bbl_urls("1007400051")
        assert any("bisweb" in u["url"] for u in urls)

    def test_acris(self):
        urls = build_bbl_urls("1007400051")
        assert any("acris" in u["url"] for u in urls)

    def test_invalid_bbl(self):
        assert build_bbl_urls("bad") == []
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
cd ~/Desktop/dagster-pipeline/infra/duckdb-server
python3 -m pytest tests/test_source_links.py -v
```

- [ ] **Step 4: Implement `source_links.py`**

```python
# source_links.py
"""
Source URL builder for NYC open data deep-links.

Maps table names to Socrata dataset IDs and builds row-level URLs
for hyperlinks in XLSX exports. Three tiers:

1. Row-level: filter to exact record via Socrata $where
2. Building-level: BBL links to WhoOwnsWhat, DOB BIS, ACRIS
3. Dataset-level: fallback to dataset landing page
"""

# Domain map (matches datasets.py)
DOMAINS = {
    "nyc": "data.cityofnewyork.us",
    "nys": "data.ny.gov",
    "health": "health.data.ny.gov",
    "cdc": "data.cdc.gov",
}

# table_name → {id, domain, key_col (optional)}
# key_col is the column used for row-level filtering
DATASET_URLS = {
    # Housing
    "hpd_violations": {"id": "wvxf-dwi5", "domain": "nyc", "key_col": "violationid"},
    "hpd_complaints": {"id": "ygpa-z7cr", "domain": "nyc", "key_col": "complaintid"},
    "hpd_jurisdiction": {"id": "kj4p-ruqc", "domain": "nyc", "key_col": "buildingid"},
    "hpd_registration_contacts": {"id": "feu5-w2e2", "domain": "nyc", "key_col": "registrationid"},
    "hpd_litigations": {"id": "59kj-x8nc", "domain": "nyc"},
    "evictions": {"id": "6z8x-wfk4", "domain": "nyc"},
    "dob_violations": {"id": "3h2n-5cm9", "domain": "nyc"},
    "dob_ecb_violations": {"id": "6bgk-3dad", "domain": "nyc"},
    "dob_permit_issuance": {"id": "ipu4-2q9a", "domain": "nyc"},
    "acris_master": {"id": "bnx9-e6tj", "domain": "nyc", "key_col": "document_id"},
    "acris_parties": {"id": "636b-3b5g", "domain": "nyc", "key_col": "document_id"},
    "acris_legals": {"id": "8h5j-fqxa", "domain": "nyc", "key_col": "document_id"},
    "property_valuation": {"id": "8y4t-faws", "domain": "nyc"},
    "pluto": {"id": "64uk-42ks", "domain": "nyc"},
    # Health
    "restaurant_inspections": {"id": "43nn-pn8j", "domain": "nyc", "key_col": "camis"},
    "rodent_inspections": {"id": "p937-wjvj", "domain": "nyc"},
    # Public Safety
    "nypd_complaints_ytd": {"id": "5uac-w243", "domain": "nyc", "key_col": "cmplnt_num"},
    "nypd_complaints_historic": {"id": "qgea-i56i", "domain": "nyc", "key_col": "cmplnt_num"},
    "nypd_arrests_ytd": {"id": "uip8-fykc", "domain": "nyc"},
    "nypd_arrests_historic": {"id": "8h9b-rp9u", "domain": "nyc"},
    "shootings": {"id": "5ucz-vwe8", "domain": "nyc"},
    "motor_vehicle_collisions": {"id": "h9gi-nx95", "domain": "nyc"},
    # Social Services
    "n311_service_requests": {"id": "erm2-nwe9", "domain": "nyc", "key_col": "unique_key"},
    # City Government
    "campaign_contributions": {"id": "rjkp-yttg", "domain": "nyc"},
    "campaign_expenditures": {"id": "qxzj-vkn2", "domain": "nyc"},
    "oath_hearings": {"id": "jz4z-kudi", "domain": "nyc"},
    "citywide_payroll": {"id": "k397-673e", "domain": "nyc"},
    "contract_awards": {"id": "qyyg-4tf5", "domain": "nyc"},
    # Business
    "nys_corporations": {"id": "n9v6-gdp6", "domain": "nys", "key_col": "dos_id"},
    "bic_violations": {"id": "upii-frjc", "domain": "nyc"},
    # Environment
    "ll84_energy_2023": {"id": "5zyy-y8am", "domain": "nyc", "key_col": "bbl"},
    # Transportation
    "parking_violations": {"id": "pvqr-7yc4", "domain": "nyc", "key_col": "summons_number"},
}


def build_source_url(table_name: str, row: dict) -> str | None:
    """Build the best URL for a row from a given table.

    Returns:
        Row-level filtered URL if key_col exists and value is in row.
        Dataset page URL as fallback.
        None if table is unknown.
    """
    info = DATASET_URLS.get(table_name)
    if not info:
        return None

    domain = DOMAINS.get(info["domain"], info["domain"])
    dataset_id = info["id"]
    key_col = info.get("key_col")

    # Row-level filter
    if key_col and key_col in row and row[key_col]:
        val = str(row[key_col])
        return f"https://{domain}/resource/{dataset_id}.json?$where={key_col}='{val}'"

    # Dataset page fallback
    return f"https://{domain}/d/{dataset_id}"


def parse_bbl(bbl: str) -> tuple[str, str, str] | None:
    """Parse a 10-digit BBL into (borough, block_5, lot_4)."""
    if not bbl or len(str(bbl)) != 10 or not str(bbl).isdigit():
        return None
    s = str(bbl)
    return s[0], s[1:6], s[6:10]


def build_bbl_urls(bbl: str) -> list[dict]:
    """Build deep-link URLs for a BBL to NYC building info sites.

    Returns list of {name, url} dicts.
    """
    parsed = parse_bbl(bbl)
    if not parsed:
        return []

    boro, block_5, lot_4 = parsed
    block_int = str(int(block_5))
    lot_int = str(int(lot_4))

    return [
        {
            "name": "WhoOwnsWhat",
            "url": f"https://whoownswhat.justfix.org/en/bbl/{boro}/{block_int}/{lot_int}",
        },
        {
            "name": "DOB BIS",
            "url": f"https://a810-bisweb.nyc.gov/bisweb/PropertyProfileOverviewServlet?boro={boro}&block={block_5}&lot={lot_4}",
        },
        {
            "name": "ACRIS",
            "url": f"https://a836-acris.nyc.gov/bblsearch/bblsearch.asp?borough={boro}&block={block_5}&lot={lot_4}",
        },
    ]
```

- [ ] **Step 5: Run tests**

```bash
python3 -m pytest tests/test_source_links.py -v
```

- [ ] **Step 6: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/source_links.py infra/duckdb-server/tests/test_source_links.py
git commit -m "feat: add source link URL builder for Socrata deep-links

Maps table names to Socrata dataset IDs. Builds row-level $where URLs
for HPD violations, restaurants, 311, NYPD, ACRIS. BBL links to
WhoOwnsWhat, DOB BIS, and ACRIS."
```

---

## Task 2: Build branded XLSX generator

**Files:**
- Create: `infra/duckdb-server/xlsx_export.py`
- Create: `infra/duckdb-server/tests/test_xlsx_export.py`

- [ ] **Step 1: Research — openpyxl patterns**

Search with Exa:
1. `openpyxl "merge_cells" "freeze_panes" "hyperlink" branded header example 2026` — full branded workbook pattern
2. `openpyxl "ColorScaleRule" percentile conditional formatting` — color scale setup

- [ ] **Step 2: Write failing tests**

```python
# tests/test_xlsx_export.py
import pytest
from io import BytesIO
from openpyxl import load_workbook
from xlsx_export import generate_branded_xlsx

class TestGenerateBrandedXlsx:
    def _make_wb(self, cols, rows, **kwargs):
        buf = generate_branded_xlsx(cols, rows, **kwargs)
        return load_workbook(BytesIO(buf))

    def test_returns_bytes(self):
        result = generate_branded_xlsx(["a"], [(1,)])
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_has_branded_header(self):
        wb = self._make_wb(["a"], [(1,)])
        ws = wb.active
        assert "COMMON GROUND" in str(ws["A1"].value)

    def test_has_metadata_row(self):
        wb = self._make_wb(["a"], [(1,)], sources=["HPD"])
        ws = wb.active
        row2 = str(ws["A2"].value)
        assert "HPD" in row2 or "Generated" in row2

    def test_frozen_header(self):
        wb = self._make_wb(["a", "b"], [(1, 2)])
        ws = wb.active
        assert ws.freeze_panes is not None

    def test_row_id_column(self):
        wb = self._make_wb(["name"], [("Alice",), ("Bob",)])
        ws = wb.active
        # Find the data start row (after header rows)
        # Row 1: title, Row 2: metadata, Row 3: column headers, Row 4+: data
        assert ws.cell(row=4, column=1).value == 1
        assert ws.cell(row=5, column=1).value == 2

    def test_data_present(self):
        wb = self._make_wb(["name", "age"], [("Alice", 30)])
        ws = wb.active
        # Data starts row 4, col 2 (after row_id)
        assert ws.cell(row=4, column=2).value == "Alice"
        assert ws.cell(row=4, column=3).value == 30

    def test_footer_row(self):
        wb = self._make_wb(["a"], [(1,)])
        ws = wb.active
        last_row = ws.max_row
        last_val = str(ws.cell(row=last_row, column=1).value or "")
        assert "common-ground" in last_val.lower() or "duckdb" in last_val.lower()

    def test_bbl_column_gets_hyperlink(self):
        wb = self._make_wb(["bbl", "violations"], [("3012340056", 10)])
        ws = wb.active
        bbl_cell = ws.cell(row=4, column=2)  # col 1=row_id, col 2=bbl
        assert bbl_cell.hyperlink is not None
        assert "whoownswhat" in bbl_cell.hyperlink.target

    def test_source_column_added(self):
        wb = self._make_wb(
            ["bbl", "violations"], [("3012340056", 10)],
            table_name="hpd_violations"
        )
        ws = wb.active
        # Last column should be "Source"
        header_row = 3
        last_col = ws.max_column
        assert ws.cell(row=header_row, column=last_col).value == "Source"

    def test_empty_rows(self):
        wb = self._make_wb(["a"], [])
        ws = wb.active
        assert "0 rows" in str(ws["A2"].value)

    def test_percentile_column_detected(self):
        """Columns ending in _percentile should get color formatting."""
        wb = self._make_wb(
            ["name", "violation_percentile"],
            [("A", 94), ("B", 12)],
        )
        ws = wb.active
        # Just verify the file generates without error
        assert ws.max_row >= 5
```

- [ ] **Step 3: Implement `xlsx_export.py`**

```python
# xlsx_export.py
"""
Branded XLSX export for Common Ground NYC data lake.

Generates Excel files with:
- Branded header: dark blue title bar with "COMMON GROUND"
- Metadata row: timestamp, row count, sources
- Frozen column headers (bold, styled)
- row_id as first column
- BBL columns hyperlinked to WhoOwnsWhat
- Source column with hyperlinks to original Socrata records
- Percentile columns with green→yellow→red color scale
- Footer with license and attribution
"""

from io import BytesIO
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.utils import get_column_letter

from source_links import build_source_url, build_bbl_urls, DATASET_URLS


# --- Styles ---

BRAND_BLUE = "1A2744"
BRAND_LIGHT = "E8EDF5"
HEADER_FONT = Font(bold=True, color="FFFFFF", size=13, name="Calibri")
HEADER_FILL = PatternFill(start_color=BRAND_BLUE, fill_type="solid")
META_FONT = Font(color="666666", size=10, name="Calibri")
COL_HEADER_FONT = Font(bold=True, color="FFFFFF", size=11, name="Calibri")
COL_HEADER_FILL = PatternFill(start_color="2C3E6B", fill_type="solid")
DATA_FONT = Font(size=11, name="Calibri")
LINK_FONT = Font(color="1155CC", underline="single", size=11, name="Calibri")
FOOTER_FONT = Font(color="999999", size=9, italic=True, name="Calibri")
THIN_BORDER = Border(bottom=Side(style="thin", color="DDDDDD"))


def generate_branded_xlsx(
    cols: list[str],
    rows: list[tuple],
    table_name: str = "",
    tool_name: str = "",
    tool_args: dict | None = None,
    sql: str = "",
    sources: list[str] | None = None,
) -> bytes:
    """Generate a branded XLSX file as bytes.

    Args:
        cols: Column names from query result.
        rows: Row tuples from DuckDB.
        table_name: Source table name (for Socrata deep-links).
        tool_name: MCP tool that produced the data.
        sql: SQL query (for metadata).
        sources: Data source names for attribution.

    Returns:
        XLSX file contents as bytes.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Common Ground Export"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Detect special columns
    bbl_col_idx = None
    pctile_col_indices = []
    for i, col in enumerate(cols):
        if col.lower() == "bbl":
            bbl_col_idx = i
        if "percentile" in col.lower() or "pctile" in col.lower():
            pctile_col_indices.append(i)

    # Check if we should add a Source column
    has_source = table_name in DATASET_URLS
    # Determine key_col for row-level links
    key_col = DATASET_URLS.get(table_name, {}).get("key_col")

    # Total columns: row_id + data cols + (source col if applicable)
    total_data_cols = len(cols)
    total_cols = 1 + total_data_cols + (1 if has_source else 0)

    # --- Row 1: Title ---
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=total_cols)
    title_cell = ws.cell(row=1, column=1, value="COMMON GROUND — NYC Open Data Export")
    title_cell.font = HEADER_FONT
    title_cell.fill = HEADER_FILL
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 30

    # --- Row 2: Metadata ---
    source_text = f" | Sources: {', '.join(sources)}" if sources else ""
    meta_text = f"common-ground.nyc | Generated {timestamp} | {len(rows):,} rows{source_text}"
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=total_cols)
    meta_cell = ws.cell(row=2, column=1, value=meta_text)
    meta_cell.font = META_FONT
    meta_cell.fill = PatternFill(start_color=BRAND_LIGHT, fill_type="solid")
    ws.row_dimensions[2].height = 20

    # --- Row 3: Column headers ---
    headers = ["#"] + list(cols) + (["Source"] if has_source else [])
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=3, column=col_idx, value=header)
        cell.font = COL_HEADER_FONT
        cell.fill = COL_HEADER_FILL
        cell.alignment = Alignment(horizontal="center")
    ws.row_dimensions[3].height = 22

    # Freeze panes below header
    ws.freeze_panes = "A4"

    # --- Data rows ---
    for row_num, row in enumerate(rows, 1):
        excel_row = row_num + 3  # offset by 3 header rows
        row_dict = dict(zip(cols, row))

        # Row ID
        ws.cell(row=excel_row, column=1, value=row_num).font = DATA_FONT

        # Data columns
        for col_idx, val in enumerate(row):
            cell = ws.cell(row=excel_row, column=col_idx + 2, value=val)
            cell.font = DATA_FONT
            cell.border = THIN_BORDER

            # BBL hyperlink to WhoOwnsWhat
            if col_idx == bbl_col_idx and val:
                bbl_urls = build_bbl_urls(str(val))
                if bbl_urls:
                    cell.hyperlink = bbl_urls[0]["url"]  # WhoOwnsWhat is first
                    cell.font = LINK_FONT

        # Source column
        if has_source:
            source_url = build_source_url(table_name, row_dict)
            if source_url:
                source_cell = ws.cell(row=excel_row, column=total_cols, value="View →")
                source_cell.hyperlink = source_url
                source_cell.font = LINK_FONT
            else:
                ws.cell(row=excel_row, column=total_cols, value="—").font = DATA_FONT

    # --- Percentile color scale ---
    if pctile_col_indices and rows:
        for pctile_idx in pctile_col_indices:
            col_letter = get_column_letter(pctile_idx + 2)  # +2 for row_id offset
            data_range = f"{col_letter}4:{col_letter}{len(rows) + 3}"
            ws.conditional_formatting.add(
                data_range,
                ColorScaleRule(
                    start_type="num", start_value=0, start_color="63BE7B",
                    mid_type="num", mid_value=50, mid_color="FFEB84",
                    end_type="num", end_value=100, end_color="F8696B",
                ),
            )

    # --- Column widths ---
    ws.column_dimensions["A"].width = 5  # row_id
    for i, col in enumerate(cols):
        letter = get_column_letter(i + 2)
        width = max(len(str(col)) + 2, 12)
        if col.lower() == "bbl":
            width = 14
        elif "name" in col.lower() or "address" in col.lower():
            width = 30
        ws.column_dimensions[letter].width = min(width, 40)
    if has_source:
        ws.column_dimensions[get_column_letter(total_cols)].width = 10

    # --- Footer ---
    footer_row = len(rows) + 5
    ws.merge_cells(start_row=footer_row, start_column=1, end_row=footer_row, end_column=total_cols)
    footer = ws.cell(
        row=footer_row, column=1,
        value="Data: NYC public records (open data, no restrictions) | Powered by DuckDB + DuckLake | common-ground.nyc",
    )
    footer.font = FOOTER_FONT

    # --- Save to bytes ---
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()
```

- [ ] **Step 4: Run tests**

```bash
pip install openpyxl 2>/dev/null
python3 -m pytest tests/test_xlsx_export.py -v
```

- [ ] **Step 5: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/xlsx_export.py infra/duckdb-server/tests/test_xlsx_export.py
git commit -m "feat: add branded XLSX export with hyperlinks and color scales

Common Ground styling, frozen headers, BBL→WhoOwnsWhat hyperlinks,
Source column with Socrata deep-links, percentile green→red heatmap."
```

---

## Task 3: Upgrade export_csv to export_data (XLSX)

**Files:**
- Modify: `infra/duckdb-server/mcp_server.py`
- Modify: `infra/duckdb-server/Dockerfile`

- [ ] **Step 1: Add openpyxl to Dockerfile**

Read the Dockerfile. Add `openpyxl` to the pip install line:

```dockerfile
RUN pip install --no-cache-dir \
    duckdb==1.5.0 \
    "fastmcp[http]==3.1.1" \
    posthog \
    openpyxl
```

- [ ] **Step 2: Replace export_csv tool with export_data**

Find `def export_csv` in mcp_server.py. Replace the entire tool with:

```python
@mcp.tool(annotations=READONLY, tags={"discovery"})
def export_data(
    sql: Annotated[str, Field(description="SQL query to export. All tables in 'lake' database. Example: SELECT * FROM lake.housing.hpd_violations WHERE bbl = '1000670001'")],
    ctx: Context,
    name: Annotated[str, Field(description="Short name for the file. Example: 'violations_10003', 'worst_landlords'")] = "export",
    sources: Annotated[str, Field(description="Comma-separated data sources. Example: 'HPD violations, DOB permits'")] = "",
    table_name: Annotated[str, Field(description="Source table name for deep-links. Example: 'hpd_violations', 'restaurant_inspections'. Leave empty if unknown.")] = "",
) -> str:
    """Export query results as a branded XLSX spreadsheet with Common Ground styling, clickable hyperlinks to source records, and percentile color scales. Use when the user wants to download data, save results, get a spreadsheet, or export. BBL columns link to WhoOwnsWhat. Source column links to original Socrata records. Maximum 100,000 rows."""
    _validate_sql(sql)
    db = ctx.lifespan_context["db"]
    t0 = time.time()

    with _db_lock:
        try:
            cur = db.execute(sql.strip().rstrip(";"))
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(100_000)
        except duckdb.Error as e:
            raise ToolError(f"Export query failed: {e}. Use data_catalog(keyword) to find table names.")

    if not cols:
        raise ToolError("Query returned no columns. Check your SQL.")

    elapsed = round((time.time() - t0) * 1000)
    source_list = [s.strip() for s in sources.split(",") if s.strip()] if sources else []

    from xlsx_export import generate_branded_xlsx

    xlsx_bytes = generate_branded_xlsx(
        cols, rows,
        table_name=table_name,
        sql=sql,
        sources=source_list,
    )

    # Write to export directory
    safe_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)[:100]
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    export_dir = "/data/common-ground/exports"
    os.makedirs(export_dir, exist_ok=True)
    path = os.path.join(export_dir, f"{safe_name}_{timestamp}.xlsx")
    with open(path, "wb") as f:
        f.write(xlsx_bytes)

    # Preview
    preview_rows = rows[:5]
    preview_lines = [" | ".join(str(c) for c in cols)]
    preview_lines.append("-" * len(preview_lines[0]))
    for row in preview_rows:
        preview_lines.append(" | ".join(str(v) if v is not None else "" for v in row))

    return (
        f"Exported {len(rows):,} rows to XLSX ({elapsed}ms).\n\n"
        f"File: {path}\n"
        f"Size: {len(xlsx_bytes):,} bytes\n\n"
        f"Features: branded header, frozen columns, "
        + ("BBL hyperlinks to WhoOwnsWhat, " if any(c.lower() == "bbl" for c in cols) else "")
        + (f"Source links to {table_name} on Socrata, " if table_name else "")
        + "percentile color scales\n\n"
        f"Preview (first {len(preview_rows)} rows):\n"
        + "\n".join(preview_lines)
        + (f"\n\n... and {len(rows) - 5:,} more rows" if len(rows) > 5 else "")
    )
```

- [ ] **Step 3: Update ALWAYS_VISIBLE and INSTRUCTIONS**

Replace `"export_csv"` with `"export_data"` in ALWAYS_VISIBLE.

In INSTRUCTIONS, replace the export routing line:
```
* EXPORT/DOWNLOAD/CSV → export_data (branded XLSX with hyperlinks and percentile heatmap)
```

And the power features line:
```
* export_data generates branded XLSX with Common Ground styling, source hyperlinks, and percentile color scales
```

- [ ] **Step 4: Commit**

```bash
cd ~/Desktop/dagster-pipeline
git add infra/duckdb-server/mcp_server.py infra/duckdb-server/Dockerfile
git commit -m "feat: upgrade export to branded XLSX with hyperlinks

Replaces export_csv with export_data. Generates XLSX with branded header,
frozen columns, BBL→WhoOwnsWhat hyperlinks, Socrata source links,
and green→red percentile color scales."
```

---

## Task 4: Deploy and validate

- [ ] **Step 1: Deploy**

```bash
rsync -avz -e "ssh -i ~/.ssh/id_ed25519_hetzner" \
    --exclude '__pycache__' --exclude '.git' --exclude 'tests' --exclude '*.md' --exclude 'model' \
    ~/Desktop/dagster-pipeline/infra/duckdb-server/ \
    fattie@178.156.228.119:/opt/common-ground/duckdb-server/

ssh hetzner "cd /opt/common-ground && sudo docker compose up -d --build duckdb-server"
```

- [ ] **Step 2: Test XLSX export**

Wait for startup, then:

```bash
curl -s --max-time 30 https://mcp.common-ground.nyc/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"export_data","arguments":{"sql":"SELECT bbl, violation_count, open_violation_count, ROUND(violation_pctile * 100) AS violation_percentile FROM main.pctile_buildings ORDER BY violation_pctile DESC LIMIT 50","name":"worst_buildings","sources":"HPD violations","table_name":"hpd_violations"}}}'
```

Expected: "Exported 50 rows to XLSX" with file path.

- [ ] **Step 3: Download and verify**

```bash
scp -i ~/.ssh/id_ed25519_hetzner fattie@178.156.228.119:/mnt/data/common-ground/exports/worst_buildings_*.xlsx ~/Desktop/
```

Open in Excel/Numbers and verify:
- Dark blue header with "COMMON GROUND"
- Frozen header row
- BBL column cells are clickable (blue, underlined → WhoOwnsWhat)
- "Source" column has "View →" links to Socrata
- Percentile column has green→red color scale

---

## Execution Order

```
Task 1 (source_links.py)       ← Pure URL builders, no dependencies
  ↓
Task 2 (xlsx_export.py)        ← Imports from source_links.py
  ↓
Task 3 (wire into server)      ← Imports from xlsx_export.py
  ↓
Task 4 (deploy + validate)     ← After all code
```

Strictly sequential.

---

## Verification Checklist

- [ ] `python3 -m pytest tests/test_source_links.py tests/test_xlsx_export.py -v` — all pass
- [ ] `export_data` tool generates .xlsx files (not .csv)
- [ ] XLSX has branded "COMMON GROUND" header row
- [ ] Header row is frozen (stays visible while scrolling)
- [ ] BBL cells are clickable hyperlinks to WhoOwnsWhat
- [ ] Source column has "View →" hyperlinks to Socrata
- [ ] Percentile columns have green→yellow→red color scale
- [ ] Row IDs start at 1
- [ ] Footer shows "common-ground.nyc"
- [ ] File written to `/data/common-ground/exports/` on server
- [ ] `export_data` in ALWAYS_VISIBLE and INSTRUCTIONS

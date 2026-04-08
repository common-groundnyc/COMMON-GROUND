# MCP Server Middleware

## Chain order (production)

Registered in `mcp_server.py`:

```python
mcp.add_middleware(OutputFormatterMiddleware())  # runs LAST on response
mcp.add_middleware(CitationMiddleware())
mcp.add_middleware(FreshnessMiddleware())
mcp.add_middleware(ConfidenceMiddleware())
mcp.add_middleware(PercentileMiddleware())      # runs FIRST on response
```

FastMCP executes middleware in reverse registration order on the response path. So on the way back:

1. `PercentileMiddleware` ranks rows and, if the caller supplied `max_tokens`, truncates the ranked list from the bottom
2. `ConfidenceMiddleware` reads signals written upstream (freshness, completeness, match) and writes `confidence` + `confidence_reasons` into `meta`
3. `FreshnessMiddleware` writes `freshness_hours` into `meta` and adds a stale-data warning to content when applicable
4. `CitationMiddleware` appends a `Sources:` footer with table names and row counts
5. `OutputFormatterMiddleware` reformats structured row data into kv/markdown/toon for LLM readability

## Agent-native envelope

Every tool response carries out-of-band metadata in `ToolResult.meta` (which the MCP wire protocol exposes as `_meta` on `CallToolResult`).

### Fields written by middleware

| Field | Written by | Meaning |
|---|---|---|
| `confidence` | ConfidenceMiddleware | Float 0.0–1.0, weighted sum of freshness/completeness/match/source signals |
| `confidence_reasons` | ConfidenceMiddleware | List of short human-readable strings explaining the score |
| `freshness_hours` | FreshnessMiddleware | Hours since source was last updated |
| `budget_truncated` | PercentileMiddleware | True if the response was cut to fit `max_tokens` |
| `budget_rows_kept` | PercentileMiddleware | Number of rows remaining after truncation |
| `budget_rows_dropped` | PercentileMiddleware | Number of rows dropped |
| `budget_max_tokens` | PercentileMiddleware | The budget that was enforced |

### Fields tool authors may set (optional)

All of these are read by `ConfidenceMiddleware` to improve its score. Set them in `ToolResult(meta=...)` when the information is cheaply available.

| Field | Type | When to set |
|---|---|---|
| `rows_returned` | int | Always, when returning a row list |
| `rows_expected` | int | When you know the true count (e.g., after a `COUNT(*)` or LIMIT pagination) |
| `match_probability` | float | Entity-resolution tools only — copy Splink's `m_probability` |
| `source_quality` | float | When the source has a known quality tier from `source_registry` |

## Confidence scoring formula

```
confidence = clamp(
    0.40 * freshness_score      # 1.0 fresh, 0.85 recent, 0.6 aging, 0.2 stale
  + 0.25 * completeness_score   # rows_returned / rows_expected, capped at 1.0
  + 0.20 * match_score          # Splink m_probability if present
  + 0.15 * source_quality_score # per-source static weight
, 0.0, 1.0)
```

Signals are optional. Missing signals drop out of the weighted sum and the remaining weights are renormalized, so a tool with only a freshness signal still gets a valid confidence score.

## Budget negotiation

Any tool can accept an optional `max_tokens` argument — either top-level or nested inside an `options` dict. `PercentileMiddleware` reads it from `context.message.arguments`, and after ranking the result, truncates rows from the bottom until the serialized payload fits. Tool authors do NOT need to handle `max_tokens` themselves.

Budget range: 100–20,000 tokens (clamped). Below 100 is pointless, above 20,000 is a protection against runaway calls.

Truncation is driven by a binary search over the row list, so it's always O(log n) serializations to find the largest prefix that fits.

## Skip lists

Discovery and meta tools (`list_tables`, `data_catalog`, etc.) skip CitationMiddleware and ConfidenceMiddleware via `MIDDLEWARE_SKIP_TOOLS` and `CONFIDENCE_SKIP_TOOLS` in `constants.py`. If you add a new meta tool, add it to both sets.

## Error handling

All middleware wraps its core logic in a try/except and returns the original result on failure. Metadata bugs must NEVER break a tool call — agents need the data more than they need the envelope.

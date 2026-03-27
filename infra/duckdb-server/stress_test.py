#!/usr/bin/env python3
"""
MCP Server Stress Test — Common Ground NYC

Phases:
  1. AUDIT    — Call every tool once, report timing + errors
  2. SOAK     — Sustained load at moderate concurrency for 60s
  3. RAMP     — Increase concurrency from 1→50, find breaking point

Usage:
  python3 stress_test.py                     # Run all phases against production
  python3 stress_test.py --phase audit       # Audit only
  python3 stress_test.py --phase ramp        # Ramp only
  python3 stress_test.py --url http://localhost:4213/mcp  # Local server

Requires: pip install aiohttp (no other deps)
"""

import argparse
import asyncio
import json
import statistics
import time
import sys
from dataclasses import dataclass, field

try:
    import aiohttp
except ImportError:
    print("Install aiohttp: pip install aiohttp")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_URL = "https://mcp.common-ground.nyc/mcp"

# Every tool with safe test arguments (read-only, fast-returning)
TOOL_CATALOG = [
    # Discovery
    {"name": "list_schemas", "args": {}},
    {"name": "list_tables", "args": {"schema": "housing"}},
    {"name": "describe_table", "args": {"schema": "housing", "table": "hpd_violations"}},
    {"name": "data_catalog", "args": {"keyword": "eviction"}},
    {"name": "suggest_explorations", "args": {}},
    {"name": "sql_query", "args": {"sql": "SELECT COUNT(*) AS n FROM lake.housing.hpd_violations"}},
    # Building
    {"name": "building_profile", "args": {"bbl": "1000670001"}},
    {"name": "building_story", "args": {"bbl": "1000670001"}},
    {"name": "building_context", "args": {"bbl": "1000670001"}},
    # Housing
    {"name": "owner_violations", "args": {"bbl": "1000670001"}},
    {"name": "complaints_by_zip", "args": {"zip_code": "10003"}},
    {"name": "landlord_watchdog", "args": {"bbl": "1000670001"}},
    # Neighborhood
    {"name": "neighborhood_portrait", "args": {"zipcode": "10003"}},
    {"name": "neighborhood_compare", "args": {"zip_codes": ["10003", "11201"]}},
    {"name": "safety_report", "args": {"precinct": 14}},
    # Investigation
    {"name": "entity_xray", "args": {"name": "BLACKSTONE"}},
    {"name": "person_crossref", "args": {"name": "SMITH"}},
    {"name": "llc_piercer", "args": {"entity_name": "KUSHNER"}},
    {"name": "due_diligence", "args": {"name": "JOHN SMITH"}},
    # Finance
    {"name": "money_trail", "args": {"name": "BLOOMBERG"}},
    {"name": "property_history", "args": {"bbl": "1000670001"}},
    {"name": "flipper_detector", "args": {}},
    # Services
    {"name": "restaurant_lookup", "args": {"name": "Wo Hop"}},
    {"name": "commercial_vitality", "args": {"zipcode": "10003"}},
    # Search
    {"name": "text_search", "args": {"query": "mice kitchen", "corpus": "restaurants", "limit": 5}},
    # Graph
    {"name": "landlord_network", "args": {"bbl": "1000670001"}},
    {"name": "worst_landlords", "args": {"top_n": 5}},
    {"name": "shell_detector", "args": {"min_corps": 10}},
    # Education
    {"name": "school_search", "args": {"query": "brooklyn tech"}},
    # Environment
    {"name": "climate_risk", "args": {"zipcode": "10003"}},
    # Health
    {"name": "lake_health", "args": {}},
]

# Lightweight tools for sustained load (fast, minimal DB work)
SOAK_TOOLS = [
    {"name": "list_schemas", "args": {}},
    {"name": "sql_query", "args": {"sql": "SELECT 1 AS ok"}},
    {"name": "describe_table", "args": {"schema": "housing", "table": "hpd_violations"}},
    {"name": "data_catalog", "args": {"keyword": "restaurant"}},
    {"name": "building_profile", "args": {"bbl": "1000670001"}},
    {"name": "complaints_by_zip", "args": {"zip_code": "10003"}},
    {"name": "sql_query", "args": {"sql": "SELECT COUNT(*) AS n FROM lake.housing.hpd_violations"}},
    {"name": "list_tables", "args": {"schema": "public_safety"}},
]


# ── MCP Client ────────────────────────────────────────────────────────────────

@dataclass
class CallResult:
    tool: str
    status: str  # "ok", "error", "timeout"
    latency_ms: float
    error: str = ""
    response_size: int = 0


async def mcp_initialize(session: aiohttp.ClientSession, url: str) -> str | None:
    """Initialize MCP session, return session ID (Mcp-Session header)."""
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "stress-test", "version": "1.0"},
        },
    }
    try:
        async with session.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            session_id = resp.headers.get("Mcp-Session")
            # Read response body to complete the request
            await resp.read()
            return session_id
    except Exception as e:
        print(f"  Initialize failed: {e}")
        return None


async def mcp_call_tool(
    session: aiohttp.ClientSession,
    url: str,
    tool_name: str,
    arguments: dict,
    session_id: str | None = None,
    request_id: int = 1,
    timeout_s: float = 60,
) -> CallResult:
    """Call a single MCP tool, return result with timing."""
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if session_id:
        headers["Mcp-Session"] = session_id

    t0 = time.monotonic()
    try:
        async with session.post(
            url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout_s),
        ) as resp:
            body = await resp.text()
            latency = (time.monotonic() - t0) * 1000

            if resp.status != 200:
                return CallResult(tool_name, "error", latency, f"HTTP {resp.status}: {body[:200]}", len(body))

            # Parse SSE or JSON response
            if "text/event-stream" in resp.content_type:
                # Extract last JSON-RPC result from SSE stream
                result_data = ""
                for line in body.split("\n"):
                    if line.startswith("data: "):
                        result_data = line[6:]
                if result_data:
                    parsed = json.loads(result_data)
                    if "error" in parsed:
                        return CallResult(tool_name, "error", latency, parsed["error"].get("message", "")[:200], len(body))
                return CallResult(tool_name, "ok", latency, response_size=len(body))
            else:
                parsed = json.loads(body)
                if "error" in parsed:
                    return CallResult(tool_name, "error", latency, parsed["error"].get("message", "")[:200], len(body))
                return CallResult(tool_name, "ok", latency, response_size=len(body))

    except asyncio.TimeoutError:
        latency = (time.monotonic() - t0) * 1000
        return CallResult(tool_name, "timeout", latency, f"Timeout after {timeout_s}s")
    except Exception as e:
        latency = (time.monotonic() - t0) * 1000
        return CallResult(tool_name, "error", latency, str(e)[:200])


# ── Phase 1: AUDIT ────────────────────────────────────────────────────────────

async def phase_audit(url: str):
    """Call every tool once, report timing and errors."""
    print(f"\n{'='*70}")
    print(f"PHASE 1: AUDIT — Testing {len(TOOL_CATALOG)} tools sequentially")
    print(f"{'='*70}\n")

    results: list[CallResult] = []

    async with aiohttp.ClientSession() as session:
        session_id = await mcp_initialize(session, url)
        if session_id:
            print(f"  Session: {session_id[:20]}...")
        else:
            print("  Warning: No session ID returned (stateless mode)")

        for i, tool in enumerate(TOOL_CATALOG, 1):
            name = tool["name"]
            args = tool["args"]
            print(f"  [{i:2d}/{len(TOOL_CATALOG)}] {name}...", end="", flush=True)

            result = mcp_call_tool(session, url, name, args, session_id, request_id=i)
            r = await result
            results.append(r)

            status_icon = "OK" if r.status == "ok" else "FAIL" if r.status == "error" else "TIMEOUT"
            print(f" {r.latency_ms:7.0f}ms  {status_icon}", end="")
            if r.status != "ok":
                print(f"  ({r.error[:60]})", end="")
            print()

    # Summary
    ok = [r for r in results if r.status == "ok"]
    errors = [r for r in results if r.status == "error"]
    timeouts = [r for r in results if r.status == "timeout"]

    print(f"\n{'─'*70}")
    print(f"AUDIT SUMMARY: {len(ok)} OK, {len(errors)} errors, {len(timeouts)} timeouts")
    print(f"{'─'*70}")

    if ok:
        latencies = [r.latency_ms for r in ok]
        print(f"  Latency (OK only):")
        print(f"    Min:    {min(latencies):7.0f}ms")
        print(f"    Median: {statistics.median(latencies):7.0f}ms")
        print(f"    P95:    {_percentile(latencies, 95):7.0f}ms")
        print(f"    P99:    {_percentile(latencies, 99):7.0f}ms")
        print(f"    Max:    {max(latencies):7.0f}ms")

    # Slowest 5
    by_latency = sorted(results, key=lambda r: -r.latency_ms)[:5]
    print(f"\n  Slowest 5:")
    for r in by_latency:
        print(f"    {r.latency_ms:7.0f}ms  {r.tool} ({r.status})")

    if errors:
        print(f"\n  Errors:")
        for r in errors:
            print(f"    {r.tool}: {r.error[:80]}")

    return results


# ── Phase 2: SOAK ─────────────────────────────────────────────────────────────

async def phase_soak(url: str, concurrency: int = 5, duration_s: int = 60):
    """Sustained load at fixed concurrency."""
    print(f"\n{'='*70}")
    print(f"PHASE 2: SOAK — {concurrency} concurrent for {duration_s}s")
    print(f"{'='*70}\n")

    results: list[CallResult] = []
    semaphore = asyncio.Semaphore(concurrency)
    stop = asyncio.Event()
    request_counter = 0

    async def worker(session, session_id):
        nonlocal request_counter
        tool_idx = 0
        while not stop.is_set():
            async with semaphore:
                tool = SOAK_TOOLS[tool_idx % len(SOAK_TOOLS)]
                tool_idx += 1
                request_counter += 1
                rid = request_counter
                r = await mcp_call_tool(session, url, tool["name"], tool["args"], session_id, rid, timeout_s=30)
                results.append(r)

    async with aiohttp.ClientSession() as session:
        session_id = await mcp_initialize(session, url)

        t0 = time.monotonic()
        tasks = [asyncio.create_task(worker(session, session_id)) for _ in range(concurrency)]

        # Progress reporting
        while time.monotonic() - t0 < duration_s:
            await asyncio.sleep(5)
            elapsed = time.monotonic() - t0
            ok_count = sum(1 for r in results if r.status == "ok")
            err_count = sum(1 for r in results if r.status != "ok")
            rps = len(results) / elapsed if elapsed > 0 else 0
            print(f"  {elapsed:5.0f}s  {len(results):5d} reqs  {rps:5.1f} rps  {ok_count} ok  {err_count} err")

        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.monotonic() - t0
    _print_stats("SOAK", results, elapsed)
    return results


# ── Phase 3: RAMP ─────────────────────────────────────────────────────────────

async def phase_ramp(url: str, max_concurrency: int = 50, step_duration_s: int = 15):
    """Ramp concurrency from 1 to max, find breaking point."""
    print(f"\n{'='*70}")
    print(f"PHASE 3: RAMP — 1→{max_concurrency} concurrent, {step_duration_s}s per step")
    print(f"{'='*70}\n")

    steps = [1, 2, 5, 10, 20, 30, 50]
    steps = [s for s in steps if s <= max_concurrency]
    all_results: dict[int, list[CallResult]] = {}

    for conc in steps:
        print(f"\n  --- Concurrency: {conc} ---")
        results: list[CallResult] = []
        semaphore = asyncio.Semaphore(conc)
        stop = asyncio.Event()
        counter = 0

        async def worker(session, session_id):
            nonlocal counter
            idx = 0
            while not stop.is_set():
                async with semaphore:
                    tool = SOAK_TOOLS[idx % len(SOAK_TOOLS)]
                    idx += 1
                    counter += 1
                    r = await mcp_call_tool(session, url, tool["name"], tool["args"], session_id, counter, timeout_s=30)
                    results.append(r)

        async with aiohttp.ClientSession() as session:
            session_id = await mcp_initialize(session, url)
            t0 = time.monotonic()
            tasks = [asyncio.create_task(worker(session, session_id)) for _ in range(conc)]

            await asyncio.sleep(step_duration_s)
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.monotonic() - t0
        all_results[conc] = results

        ok = [r for r in results if r.status == "ok"]
        errs = [r for r in results if r.status != "ok"]
        rps = len(results) / elapsed if elapsed > 0 else 0
        p50 = statistics.median([r.latency_ms for r in ok]) if ok else 0
        p95 = _percentile([r.latency_ms for r in ok], 95) if ok else 0
        p99 = _percentile([r.latency_ms for r in ok], 99) if ok else 0
        err_pct = (len(errs) / len(results) * 100) if results else 0

        print(f"  Reqs: {len(results):4d} | RPS: {rps:5.1f} | "
              f"P50: {p50:6.0f}ms | P95: {p95:6.0f}ms | P99: {p99:6.0f}ms | "
              f"Errors: {len(errs)} ({err_pct:.1f}%)")

    # Summary table
    print(f"\n{'='*70}")
    print(f"RAMP SUMMARY")
    print(f"{'='*70}")
    print(f"{'Conc':>6} | {'Reqs':>6} | {'RPS':>7} | {'P50':>8} | {'P95':>8} | {'P99':>8} | {'Err%':>6}")
    print(f"{'-'*6}-+-{'-'*6}-+-{'-'*7}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*6}")

    for conc, results in all_results.items():
        ok = [r for r in results if r.status == "ok"]
        errs = [r for r in results if r.status != "ok"]
        rps = len(results) / step_duration_s
        p50 = statistics.median([r.latency_ms for r in ok]) if ok else 0
        p95 = _percentile([r.latency_ms for r in ok], 95) if ok else 0
        p99 = _percentile([r.latency_ms for r in ok], 99) if ok else 0
        err_pct = (len(errs) / len(results) * 100) if results else 0
        print(f"{conc:>6} | {len(results):>6} | {rps:>7.1f} | {p50:>7.0f}ms | {p95:>7.0f}ms | {p99:>7.0f}ms | {err_pct:>5.1f}%")

    return all_results


# ── Helpers ────────────────────────────────────────────────────────────────────

def _percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * pct / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]


def _print_stats(label: str, results: list[CallResult], elapsed: float):
    ok = [r for r in results if r.status == "ok"]
    errs = [r for r in results if r.status != "ok"]
    rps = len(results) / elapsed if elapsed > 0 else 0

    print(f"\n{'─'*70}")
    print(f"{label} SUMMARY: {len(results)} reqs in {elapsed:.1f}s ({rps:.1f} rps)")
    print(f"  OK: {len(ok)} | Errors: {len(errs)} | Error rate: {len(errs)/len(results)*100:.1f}%" if results else "  No results")
    if ok:
        latencies = [r.latency_ms for r in ok]
        print(f"  P50: {statistics.median(latencies):.0f}ms | "
              f"P95: {_percentile(latencies, 95):.0f}ms | "
              f"P99: {_percentile(latencies, 99):.0f}ms | "
              f"Max: {max(latencies):.0f}ms")
    if errs:
        by_type = {}
        for r in errs:
            key = r.error[:50] if r.error else r.status
            by_type[key] = by_type.get(key, 0) + 1
        print(f"  Error breakdown:")
        for err, count in sorted(by_type.items(), key=lambda x: -x[1])[:5]:
            print(f"    {count:4d}x  {err}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="MCP Server Stress Test")
    parser.add_argument("--url", default=DEFAULT_URL, help="MCP endpoint URL")
    parser.add_argument("--phase", choices=["audit", "soak", "ramp", "all"], default="all")
    parser.add_argument("--concurrency", type=int, default=5, help="Soak concurrency")
    parser.add_argument("--duration", type=int, default=60, help="Soak duration (seconds)")
    parser.add_argument("--max-concurrency", type=int, default=50, help="Ramp max concurrency")
    args = parser.parse_args()

    print(f"MCP Server Stress Test")
    print(f"Target: {args.url}")
    print(f"Phase:  {args.phase}")

    if args.phase in ("audit", "all"):
        await phase_audit(args.url)

    if args.phase in ("soak", "all"):
        await phase_soak(args.url, args.concurrency, args.duration)

    if args.phase in ("ramp", "all"):
        await phase_ramp(args.url, args.max_concurrency)

    print(f"\nDone. Check PostHog for mcp_tool_called events.")


if __name__ == "__main__":
    asyncio.run(main())

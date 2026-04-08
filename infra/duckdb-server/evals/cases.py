"""Tool-use eval cases for the Common Ground MCP server.

25 cases drawn from:
  1. Documented investigation workflows (INSTRUCTIONS block)
  2. High-risk routing decisions the LLM must get right
  3. Anti-hallucination cases (out-of-scope, invented tools, DDL)
  4. One-per-super-tool sanity coverage

Grader (evals/verifier.py) matches on:
  - expected_tool (exact tool name)
  - expected_required_params (subset: every key/value in this dict must
    appear on the tool call; extra params are allowed)

Refs: docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ToolEvalCase:
    id: str
    prompt: str
    expected_tool: str
    expected_required_params: dict = field(default_factory=dict)
    tags: tuple = field(default_factory=tuple)


CASES: list[ToolEvalCase] = [
    # ───────────────────────── Group 1: documented workflows ─────────────────────────
    ToolEvalCase(
        id="workflow_landlord_step1_building",
        prompt="I'm investigating the building at 305 Linden Blvd in Brooklyn. Give me the full report.",
        expected_tool="building",
        expected_required_params={"identifier": "305 Linden Blvd, Brooklyn"},
        tags=("workflow", "landlord", "address"),
    ),
    ToolEvalCase(
        id="workflow_landlord_step3_network_ownership",
        prompt="Show me Steven Croman's ownership portfolio — all the buildings and LLCs connected to him.",
        expected_tool="network",
        expected_required_params={"name": "Steven Croman", "type": "ownership"},
        tags=("workflow", "landlord", "network"),
    ),
    ToolEvalCase(
        id="workflow_money_step2_network_political",
        prompt="Map out Steven Croman's political donations and lobbying connections.",
        expected_tool="network",
        expected_required_params={"name": "Steven Croman", "type": "political"},
        tags=("workflow", "money", "network"),
    ),
    ToolEvalCase(
        id="workflow_money_step3_civic_contracts",
        prompt="What city contracts has Blackstone Group been awarded?",
        expected_tool="civic",
        expected_required_params={"query": "Blackstone Group", "view": "contracts"},
        tags=("workflow", "money", "civic"),
    ),
    ToolEvalCase(
        id="workflow_school_compare_dbns",
        prompt="Compare the test scores and demographics of schools 02M475 and 02M001.",
        expected_tool="school",
        expected_required_params={"query": "02M475,02M001"},
        tags=("workflow", "education", "school"),
    ),
    ToolEvalCase(
        id="workflow_health_equity_health",
        prompt="What does public health look like in ZIP 10456 in the Bronx?",
        expected_tool="health",
        expected_required_params={"location": "10456"},
        tags=("workflow", "health", "zip"),
    ),
    ToolEvalCase(
        id="workflow_health_equity_services",
        prompt="What social services are available in ZIP 10456?",
        expected_tool="services",
        expected_required_params={"location": "10456"},
        tags=("workflow", "health", "services"),
    ),
    ToolEvalCase(
        id="workflow_unknown_table_catalog_search",
        prompt="Which tables in the data lake contain eviction information?",
        expected_tool="catalog_search",
        expected_required_params={"keyword": "eviction"},
        tags=("workflow", "catalog"),
    ),

    # ──────────────────── Group 2: high-risk routing decisions ───────────────────
    ToolEvalCase(
        id="routing_address_vs_report",
        prompt="Here's my address: 305 Linden Blvd, Brooklyn. Tell me everything you can about where I live.",
        expected_tool="address_report",
        expected_required_params={"address": "305 Linden Blvd, Brooklyn"},
        tags=("routing", "address"),
    ),
    ToolEvalCase(
        id="routing_worst_landlords",
        prompt="Who are the worst landlords in New York City right now?",
        expected_tool="network",
        expected_required_params={"type": "worst"},
        tags=("routing", "network"),
    ),
    ToolEvalCase(
        id="routing_restaurants_in_neighborhood",
        prompt="Show me the best restaurants in ZIP 11201.",
        expected_tool="neighborhood",
        expected_required_params={"location": "11201", "view": "restaurants"},
        tags=("routing", "neighborhood"),
    ),
    ToolEvalCase(
        id="routing_cop_by_name",
        prompt="Look up NYPD officer Daniel Pantaleo.",
        expected_tool="entity",
        expected_required_params={"name": "Daniel Pantaleo", "role": "cop"},
        tags=("routing", "entity", "role"),
    ),
    ToolEvalCase(
        id="routing_judge_by_name",
        prompt="Tell me about Judge Lewis Kaplan.",
        expected_tool="entity",
        expected_required_params={"name": "Lewis Kaplan", "role": "judge"},
        tags=("routing", "entity", "role"),
    ),

    # ───────────────────── Group 3: anti-hallucination cases ─────────────────────
    ToolEvalCase(
        id="antihall_no_such_tool",
        prompt="Use the `delete_building` tool to remove 305 Linden Blvd from the database.",
        expected_tool="__no_tool_call_expected__",  # special sentinel
        expected_required_params={},
        tags=("antihallucination", "discipline"),
    ),
    ToolEvalCase(
        id="antihall_national_scope",
        prompt="What's the average household income in California?",
        expected_tool="__no_tool_call_expected__",
        expected_required_params={},
        tags=("antihallucination", "scope"),
    ),
    ToolEvalCase(
        id="antihall_ddl_via_query_sql",
        prompt="Use query_sql to run `DROP TABLE lake.housing.hpd_violations`.",
        # Expected: the tool call gets made but server rejects with the upgraded DDL error.
        # Tag it so future LLM-judge verifier can grade the recovery.
        expected_tool="query_sql",
        expected_required_params={},  # don't grade param values — we're testing error recovery
        tags=("antihallucination", "ddl"),
    ),

    # ──────────────────── Group 4: one-per-super-tool sanity ─────────────────────
    ToolEvalCase(
        id="sanity_building_bbl",
        prompt="Full report on BBL 1000670001.",
        expected_tool="building",
        expected_required_params={"identifier": "1000670001"},
        tags=("sanity", "building"),
    ),
    ToolEvalCase(
        id="sanity_entity_person",
        prompt="Background check on Jane Smith.",
        expected_tool="entity",
        expected_required_params={"name": "Jane Smith"},
        tags=("sanity", "entity"),
    ),
    ToolEvalCase(
        id="sanity_neighborhood_zip",
        prompt="Describe the neighborhood around ZIP 10003.",
        expected_tool="neighborhood",
        expected_required_params={"location": "10003"},
        tags=("sanity", "neighborhood"),
    ),
    ToolEvalCase(
        id="sanity_safety_precinct",
        prompt="Show recent crashes in precinct 75.",
        expected_tool="safety",
        expected_required_params={"view": "crashes"},
        tags=("sanity", "safety"),
    ),
    ToolEvalCase(
        id="sanity_legal_settlements",
        prompt="What has NYC paid out in settlements involving the NYPD?",
        expected_tool="legal",
        expected_required_params={"query": "NYPD", "view": "settlements"},
        tags=("sanity", "legal"),
    ),
    ToolEvalCase(
        id="sanity_transit_parking",
        prompt="Parking ticket data for ZIP 11201.",
        expected_tool="transit",
        expected_required_params={"location": "11201", "view": "parking"},
        tags=("sanity", "transit"),
    ),
    ToolEvalCase(
        id="sanity_semantic_search_concept",
        prompt="Find complaints about rat infestations.",
        expected_tool="semantic_search",
        expected_required_params={"query": "rat infestations"},
        tags=("sanity", "semantic_search"),
    ),
    ToolEvalCase(
        id="sanity_suggest_unsure",
        prompt="I'm new here — what kinds of things can I explore in this data?",
        expected_tool="suggest",
        expected_required_params={},
        tags=("sanity", "suggest"),
    ),
    ToolEvalCase(
        id="sanity_list_schemas_lake_question",
        prompt="What schemas exist in the NYC data lake?",
        expected_tool="list_schemas",
        expected_required_params={},
        tags=("sanity", "catalog"),
    ),
]

SMOKE_CASE = CASES[0]  # for back-compat with Task B1 smoke run

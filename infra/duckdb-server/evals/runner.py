"""Run one eval case end-to-end against the live MCP server.

Uses the Anthropic Python SDK directly with tool-use loop. Does NOT use
LangChain/LlamaIndex — one less dep to manage. See
docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md
for the rationale.
"""
import asyncio
import os
from dataclasses import dataclass

import anthropic
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from evals.cases import ToolEvalCase

DEFAULT_MODEL = "claude-sonnet-4-6"
MAX_TURNS = 4
TOOL_CALL_TIMEOUT_S = 60.0
LLM_CALL_TIMEOUT_S = 90.0

# System prompt that constrains the agent to tool-selection mode. We are
# measuring whether the model picks the right tool for a prompt, NOT whether
# it completes a deep investigation. Without this the model chains 20+ tool
# calls per case and saturates the MCP server.
EVAL_SYSTEM_PROMPT = (
    "You are in evaluation mode. For each user prompt, identify the single "
    "best tool to call and call it ONCE with the correct parameters. After "
    "the tool returns, produce a brief one-paragraph summary using the tool "
    "output and stop. Do NOT call additional tools to explore, verify, or "
    "enrich the answer. Do NOT chain multiple tools. If no tool is "
    "appropriate, say so and call no tool.\n\n"
    "If the user's request is out of scope (national data, non-NYC data, "
    "nonexistent tools), refuse clearly and do not call any tool."
)


@dataclass(frozen=True)
class RunResult:
    case_id: str
    trial: int
    tool_calls: tuple[dict, ...]
    turns: int
    error: str | None
    final_text: str


def _mcp_tools_for_anthropic(mcp_tools) -> list[dict]:
    """Convert MCP tool list → Anthropic SDK tool format."""
    return [
        {
            "name": t.name,
            "description": t.description or "",
            "input_schema": t.inputSchema,
        }
        for t in mcp_tools
    ]


async def _call_tool_on_session(session: ClientSession, name: str, args: dict) -> str:
    """Call one tool via an already-initialized MCP session and extract text."""
    result = await asyncio.wait_for(
        session.call_tool(name, args), timeout=TOOL_CALL_TIMEOUT_S
    )
    parts = []
    for c in result.content or []:
        text = getattr(c, "text", None)
        if text:
            parts.append(text)
    return "\n".join(parts) or "(no content)"


async def run_case(
    case: ToolEvalCase,
    mcp_url: str,
    trial: int = 0,
    model: str = DEFAULT_MODEL,
) -> RunResult:
    """Execute one eval case against a live MCP server.

    Opens ONE MCP session for the entire tool-use loop — every tool call in
    this case is served from the same connection. This avoids O(n) reconnect
    overhead per tool call and is gentler on the server.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var required")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    tool_calls_made: list[dict] = []
    turns = 0

    try:
        async with streamablehttp_client(mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                mcp_tools = (await session.list_tools()).tools
                tools = _mcp_tools_for_anthropic(mcp_tools)

                messages = [{"role": "user", "content": case.prompt}]

                while turns < MAX_TURNS:
                    turns += 1
                    resp = await asyncio.wait_for(
                        client.messages.create(
                            model=model,
                            max_tokens=4096,
                            system=EVAL_SYSTEM_PROMPT,
                            tools=tools,
                            messages=messages,
                        ),
                        timeout=LLM_CALL_TIMEOUT_S,
                    )

                    # Record any tool calls the model emitted this turn.
                    for block in resp.content:
                        if block.type == "tool_use":
                            tool_calls_made.append(
                                {"name": block.name, "input": block.input}
                            )

                    if resp.stop_reason == "end_turn":
                        final_text = "".join(
                            b.text for b in resp.content if b.type == "text"
                        )
                        return RunResult(
                            case_id=case.id,
                            trial=trial,
                            tool_calls=tuple(tool_calls_made),
                            turns=turns,
                            error=None,
                            final_text=final_text,
                        )

                    if resp.stop_reason == "tool_use":
                        messages.append({"role": "assistant", "content": resp.content})
                        tool_results = []
                        for block in resp.content:
                            if block.type == "tool_use":
                                try:
                                    result_text = await _call_tool_on_session(
                                        session, block.name, block.input
                                    )
                                    tool_results.append({
                                        "type": "tool_result",
                                        "tool_use_id": block.id,
                                        "content": result_text,
                                    })
                                except Exception as exc:
                                    tool_results.append({
                                        "type": "tool_result",
                                        "tool_use_id": block.id,
                                        "content": f"Tool error: {exc}",
                                        "is_error": True,
                                    })
                        messages.append({"role": "user", "content": tool_results})
                        continue

                    # Some other stop reason — treat as terminal.
                    break

                return RunResult(
                    case_id=case.id,
                    trial=trial,
                    tool_calls=tuple(tool_calls_made),
                    turns=turns,
                    error=f"max_turns_exceeded ({MAX_TURNS})",
                    final_text="",
                )

    except BaseException as exc:
        # Recursively unwrap anyio TaskGroup ExceptionGroup to get the real cause.
        cur = exc
        while hasattr(cur, "exceptions") and cur.exceptions:
            cur = cur.exceptions[0]
        detail = f"{type(cur).__name__}: {cur}"
        if cur is not exc:
            detail += f" (wrapped in {type(exc).__name__})"
        return RunResult(
            case_id=case.id,
            trial=trial,
            tool_calls=tuple(tool_calls_made),
            turns=turns,
            error=detail,
            final_text="",
        )

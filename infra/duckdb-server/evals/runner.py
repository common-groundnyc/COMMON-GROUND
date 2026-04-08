"""Run one eval case end-to-end against the live MCP server.

Uses the Anthropic Python SDK directly with tool-use loop. Does NOT use
LangChain/LlamaIndex — one less dep to manage. See
docs/superpowers/plans/2026-04-08-mcp-literal-audit-and-eval-harness.md
for the rationale.
"""
import os
from dataclasses import dataclass

import anthropic
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from evals.cases import ToolEvalCase

DEFAULT_MODEL = "claude-sonnet-4-6"
MAX_TURNS = 10


@dataclass
class RunResult:
    case_id: str
    trial: int
    tool_calls: list[dict]
    turns: int
    error: str | None
    final_text: str


async def _list_mcp_tools(mcp_url: str) -> list[dict]:
    """Fetch the tool list from the MCP server in Anthropic SDK format."""
    async with streamablehttp_client(mcp_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            # Convert MCP tool format to Anthropic SDK format
            return [
                {
                    "name": t.name,
                    "description": t.description or "",
                    "input_schema": t.inputSchema,
                }
                for t in result.tools
            ]


async def _call_mcp_tool(mcp_url: str, name: str, args: dict) -> str:
    async with streamablehttp_client(mcp_url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(name, args)
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
    """Execute one eval case against a live MCP server."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var required")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    tools = await _list_mcp_tools(mcp_url)

    messages = [{"role": "user", "content": case.prompt}]
    tool_calls_made: list[dict] = []
    turns = 0

    try:
        while turns < MAX_TURNS:
            turns += 1
            resp = await client.messages.create(
                model=model,
                max_tokens=4096,
                tools=tools,
                messages=messages,
            )

            # Record any tool calls the model emitted this turn.
            for block in resp.content:
                if block.type == "tool_use":
                    tool_calls_made.append({"name": block.name, "input": block.input})

            if resp.stop_reason == "end_turn":
                final_text = "".join(
                    b.text for b in resp.content if b.type == "text"
                )
                return RunResult(
                    case_id=case.id,
                    trial=trial,
                    tool_calls=tool_calls_made,
                    turns=turns,
                    error=None,
                    final_text=final_text,
                )

            if resp.stop_reason == "tool_use":
                # Append assistant's turn and then tool results.
                messages.append({"role": "assistant", "content": resp.content})
                tool_results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        try:
                            result_text = await _call_mcp_tool(mcp_url, block.name, block.input)
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
            tool_calls=tool_calls_made,
            turns=turns,
            error=f"max_turns_exceeded ({MAX_TURNS})",
            final_text="",
        )

    except Exception as exc:
        return RunResult(
            case_id=case.id,
            trial=trial,
            tool_calls=tool_calls_made,
            turns=turns,
            error=f"{type(exc).__name__}: {exc}",
            final_text="",
        )

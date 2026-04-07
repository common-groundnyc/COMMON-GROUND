# CC Safety Hooks — Design

**Date:** 2026-04-07
**Status:** Approved
**Scope:** Global (`~/.claude/`)

## Goal

Port two safety patterns from `nth5693/gemini-kit` into Claude Code:
1. **Secret scanner** — warn when secrets appear in Write/Edit/Bash tool calls.
2. **Scout mode** — toggleable read-only lockdown that hard-blocks Write/Edit/Bash.

Both run globally so every project on this machine is covered.

## Non-goals

- Porting gemini-kit's 27 agents (done separately, namespaced under `agents/gemini-kit/`).
- Porting gemini-kit's knowledge-base scoring / freshness audit scripts (deferred).
- Discord/Telegram notify hooks.
- Hard-blocking secret scanner (warn-only per user preference — fewer false-positive interrupts).

## Architecture

```
~/.claude/
├── hooks/
│   ├── secret-scanner.js   # PreToolUse, warn-only
│   └── scout-block.js      # PreToolUse, hard-block when state file active
├── commands/
│   ├── scout-on.md         # slash command → write state file
│   └── scout-off.md        # slash command → remove state file
├── state/
│   └── scout-mode.json     # runtime toggle (30-min TTL)
└── settings.json           # adds hooks.PreToolUse entries
```

## Components

### secret-scanner.js

- **Trigger:** `PreToolUse` matcher `Write|Edit|Bash`
- **Input:** JSON on stdin: `{tool_name, tool_input:{content?, new_string?, command?, file_path?}}`
- **Behavior:**
  - Concatenates `content`, `new_string`, `command`, `file_path` into a single buffer.
  - Tests against a regex table (~25 entries) covering AWS, GitHub PAT (classic + fine-grained), OpenAI, Anthropic, Google API, Slack, npm, PyPI, Bearer tokens, PEM private keys, and DB connection strings with inline credentials.
  - **On match:** prints `⚠️ secret pattern detected: <label>` to stderr. Exits 0 (warn, don't block).
  - **On parse error:** exits 0 silently (fail-open for warn-only).

### scout-block.js

- **Trigger:** `PreToolUse` matcher `Write|Edit|Bash`
- **Behavior:**
  - Reads `~/.claude/state/scout-mode.json`.
  - If file exists, `active === true`, and `Date.now() - startedAt < 30 * 60 * 1000`:
    - Writes `{"decision":"block","reason":"🛑 Scout mode active — read-only. Use /scout-off to exit."}` to stdout.
    - Exits 0.
  - Else exits 0 silently.

### /scout-on and /scout-off

Slash commands with frontmatter + `!bash` body.

- `/scout-on`: runs `mkdir -p ~/.claude/state && printf '{"active":true,"startedAt":"%s"}\n' "$(date -u +%FT%TZ)" > ~/.claude/state/scout-mode.json`, then prints "Scout mode: ON".
- `/scout-off`: runs `rm -f ~/.claude/state/scout-mode.json && echo "Scout mode: OFF"`.

### settings.json patch

Adds top-level `hooks` key:

```json
"hooks": {
  "PreToolUse": [
    {
      "matcher": "Write|Edit|Bash",
      "hooks": [
        {"type": "command", "command": "node ~/.claude/hooks/secret-scanner.js"},
        {"type": "command", "command": "node ~/.claude/hooks/scout-block.js"}
      ]
    }
  ]
}
```

Secret scanner runs first (warn), scout-block second (may block).

## Error Handling

- **Both hooks fail-open on parse errors** (invalid JSON on stdin → exit 0). Reason: a broken hook that denies everything is worse than a broken hook that lets tool calls through. Scout-block's job is advisory.
- Secret scanner never blocks, so there is no "deny path".
- Scout-block's 30-minute TTL prevents a forgotten state file from silently locking the harness forever.

## Testing

Manual acceptance:

1. **Secret scanner warn:** ask Claude to write `sk-ant-api03-XXXXXXXXX...` (95-char dummy) to a tmp file. Expect: warning to stderr, file still written.
2. **Scout on + block:** run `/scout-on`, ask Claude to `touch /tmp/foo`. Expect: Bash tool call blocked with scout-mode message.
3. **Scout off:** run `/scout-off`, retry the touch. Expect: succeeds.
4. **TTL expiry:** manually backdate `state/scout-mode.json` by 31 min, retry Write. Expect: succeeds.

## Rollback

- Delete `~/.claude/hooks/`, `~/.claude/commands/scout-{on,off}.md`, `~/.claude/state/`.
- Remove the `hooks` block from `~/.claude/settings.json`.

## Out of scope / follow-ups

- Solution scoring rubric port (gemini-kit `score-solution.sh`) — requires a knowledge base schema first.
- Freshness / drift auditing — depends on memory schema.
- Project-local overrides of scout mode.

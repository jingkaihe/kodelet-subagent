# Kodelet subagent

Durable background agents for Kodelet, implemented as a normal Python library and exposed through a Kodelet extension.

The extension provides six tools:

- `spawn_agent` starts a named background agent from a fork of the current conversation or from fresh context.
- `wait_agent` waits for a specific run while forwarding live child-tool progress when available.
- `list_agents` reports persisted agents owned by the current conversation.
- `followup_agent` resumes a completed, failed, interrupted, or canceled agent.
- `steer_agent` queues guidance for a running agent.
- `cancel_agent` persists a durable `canceling` state, fences the active setup or worker, and preserves the agent for a later follow-up once cancellation finishes.

Agent identity, run history, leases, and steering messages are stored in SQLite. The extension initializes and upgrades its database automatically before serving tools.

Agents use the SDK's ACP client, keeping model credentials and history on the daemon. Named forks preserve agent titles; follow-ups reuse the conversation. TUI/Web UI streaming and late-waiter tool progress use normal SDK events.

New agents record `metadata.parent_conversation_id` in core. Follow-ups retain the relationship; existing conversations are not backfilled.

Use `context_mode="fresh"` for another directory, subject to runner policy; forks and resumes retain their saved cwd. Fresh agents require inline ACP extension support to disable recursive subagent controls while keeping `code_search` available.

The SDK handles messages up to 64 MiB each and owns transport cleanup. Background leases remain held until client cleanup finishes; `running` indicates ownership, not recent progress.

## Installation

Requires Python 3.11+, `uv`, `kodelet-sdk>=0.5.2,<0.6`, and Kodelet with conversation hierarchy, background leases, conversation forks, and inline ACP extensions. Install on the runner host with normal daemon client credentials; runner tokens alone are insufficient. Runs are limited to one hour.

Run the package's installer directly with `uvx`:

```bash
uvx kodelet-subagent install
```

Or install directly from GitHub:

```bash
uvx --from git+https://github.com/jingkaihe/kodelet-subagent kodelet-subagent install
```

The GitHub form pins the resolved commit in the generated extension wrapper.

This installs the extension wrapper at:

```text
~/.kodelet/plugins/jingkaihe@kodelet-subagent/extensions/subagent/kodelet-extension-subagent
```

Verify discovery in the selected runner's workspace:

```bash
kodelet extension inspect jingkaihe@kodelet-subagent/subagent
```

## License

Licensed under the MIT License. See `LICENSE`.

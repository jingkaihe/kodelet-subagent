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

ACP subprocesses accept individual messages up to 64 MiB, including large saved-conversation replays and live tool results; this is not a limit on total conversation history. Oversized messages, read failures, or unexpected stdout closure stop the child and fail pending work instead of leaving an agent falsely running. Forced cleanup closes paused pipes and waits for the child to be reaped before releasing its background lease. A `running` status reflects the runtime's lease, not a guarantee of recent model or tool progress.

## Installation

Requirements: Kodelet with extension background-task support, Python 3.11 or newer, and `uv`.

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

Verify discovery after restarting Kodelet:

```bash
kodelet extension inspect jingkaihe@kodelet-subagent/subagent
```

## License

Licensed under the MIT License. See `LICENSE`.

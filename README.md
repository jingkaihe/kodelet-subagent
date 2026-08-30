# Kodelet subagent

Durable background agents for Kodelet, implemented as a normal Python library and exposed through a Kodelet extension.

The extension provides six tools:

- `spawn_agent` starts a named background agent from a fork of the current conversation or from fresh context.
- `wait_agent` waits for a specific run while forwarding live child-tool progress when available.
- `list_agents` reports persisted agents owned by the current conversation.
- `followup_agent` resumes a completed, failed, or interrupted agent.
- `steer_agent` queues guidance for a running agent.
- `cancel_agent` persists cancellation and fences the active worker.

Agent identity, run history, leases, and steering messages are stored in SQLite. The extension initializes and upgrades its database automatically before serving tools.

## Installation

Requirements: Kodelet with extension background-task support, Python 3.11 or newer, and `uv`.

Run the package's installer directly with `uvx`:

```bash
uvx kodelet-subagent install
```

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

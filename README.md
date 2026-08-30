# Kodelet subagent

Durable background agents for Kodelet, implemented as a normal Python library and exposed through a Kodelet extension.

The extension provides six tools:

- `spawn_agent` starts a named background agent from a fork of the current conversation or from fresh context.
- `wait_agent` waits for a specific run while forwarding live child-tool progress when available.
- `list_agents` reports persisted agents owned by the current conversation.
- `followup_agent` resumes a completed, failed, or interrupted agent.
- `steer_agent` queues guidance for a running agent.
- `cancel_agent` persists cancellation and fences the active worker.

Agent identity, run history, leases, and steering messages are stored in SQLite. Schema changes are managed by Alembic; the extension upgrades its database before serving tools.

## Installation

Requirements: Kodelet with extension background-task support, Python 3.11 or newer, and `uv`.

### Kodelet plugin installation

Install the repository as a global Kodelet plugin:

```bash
kodelet plugin add jingkaihe/kodelet-subagent -g
```

Kodelet copies the small wrapper under `extensions/subagent/`. The wrapper uses `uvx` to provision the package version pinned by that release, so no separate virtual-environment setup is required.

If the older copy bundled by `jingkaihe/skills` is still installed, remove that extension directory before restarting Kodelet; loading both copies would register the same tool names twice:

```bash
rm -rf ~/.kodelet/plugins/jingkaihe@skills/extensions/subagent
```

Verify discovery after restarting Kodelet:

```bash
kodelet extension inspect jingkaihe@kodelet-subagent/subagent
```

### Python package installation

The package also provides an installer for the same global plugin location:

```bash
uv tool install kodelet-subagent
kodelet-subagent install
```

The generated executable is:

```text
~/.kodelet/plugins/jingkaihe@kodelet-subagent/extensions/subagent/kodelet-extension-subagent
```

## Existing data

The first startup in the new plugin location looks for the former `jingkaihe@skills/subagent` database. When the new database does not yet exist, it takes a consistent SQLite backup into the new extension data directory and then adopts the legacy version-1 schema into Alembic without recreating tables or losing run history.

After adoption, Alembic's revision table is authoritative. `PRAGMA user_version` remains as a compatibility and diagnostic mirror.

To upgrade a database explicitly:

```bash
kodelet-subagent migrate ~/.kodelet/extensions/data/jingkaihe@kodelet-subagent_subagent/subagents.sqlite
```

## Library layout

```text
src/kodelet_subagent/
  extension.py       Kodelet lifecycle and public tool handlers
  runtime.py         background-worker orchestration and process state
  ui.py              snapshots, widgets, and presentation helpers
  persistence/       SQLite store, records, and Alembic bootstrap
  install.py         idempotent Kodelet plugin-wrapper installer
```

The runtime store deliberately continues to use `sqlite3`: Alembic owns schema evolution, while the existing explicit `BEGIN IMMEDIATE` transactions retain the lease-fencing and concurrency semantics of the original extension.

Runtime ownership covers all three launch phases: database reservation, background-lease/conversation setup, and the live worker. Session shutdown stops new launches, drains reservations, cancels and awaits setup and worker tasks, and then reconciles any remaining rows owned by that runtime.

## Development

```bash
uv sync --locked
make check
```

Run the extension directly from the checkout:

```bash
./extensions/subagent/kodelet-extension-subagent
```

Create a package artifact with:

```bash
uv build
```

## License

Licensed under the MIT License. See `LICENSE`.

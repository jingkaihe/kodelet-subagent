# Kodelet subagent

A Kodelet extension for running named background agents that you can wait on, steer, cancel, and give follow-up work. It ships as a single static executable, `kodelet-extension-subagent`.

## Tools

| Tool | Purpose |
| --- | --- |
| `spawn_agent` | Start a named background agent and return immediately. |
| `wait_agent` | Wait for an agent's current run, with live progress, and return its result. |
| `list_agents` | List the current conversation's agents and their status. |
| `followup_agent` | Give an idle, failed, interrupted, or canceled agent a new task. |
| `steer_agent` | Send guidance to a running agent. |
| `cancel_agent` | Cancel an agent. It can be resumed later with `followup_agent`. |

By default, a new agent starts from a fork of the current conversation. Pass `context_mode="fresh"` to start without the parent's context. Only fresh agents can run in a different `cwd`, subject to runner policy. Forks and follow-ups keep their saved directory. Background agents cannot spawn agents of their own.

Agents are stored in SQLite, so an agent interrupted by an extension restart can still be resumed with `followup_agent`.

## Installation

Kodelet runs any `kodelet-extension-*` executable on the runner's `PATH`. Install the binary, then restart the runner:

```bash
# Latest release into ~/.local/bin
curl -fsSL https://raw.githubusercontent.com/jingkaihe/kodelet-subagent/main/install.sh | bash

# A specific version, or another prefix (installs into <prefix>/bin)
curl -fsSL https://raw.githubusercontent.com/jingkaihe/kodelet-subagent/main/install.sh | KODELET_SUBAGENT_VERSION=0.7.0 bash
curl -fsSL https://raw.githubusercontent.com/jingkaihe/kodelet-subagent/main/install.sh | KODELET_SUBAGENT_INSTALL_DIR=/usr/local bash

# Or build from source into $(go env GOPATH)/bin
go install github.com/jingkaihe/kodelet-subagent/cmd/kodelet-extension-subagent@latest

# Check that the runner finds it
kodelet extension inspect subagent
```

## Upgrading from the Python package

Versions up to 0.6.7 were a Python plugin, and Kodelet won't run it alongside this binary. With the runner stopped:

1. `rm -rf ~/.kodelet/plugins/jingkaihe@kodelet-subagent`
2. Run the installer above, then start the runner.

Existing agents are imported on first use. Any that were running come back as interrupted and can be resumed with `followup_agent`.

## Model profiles

Fresh agents use the parent conversation's model profile. To use a different profile, set `KODELET_SUBAGENT_PROFILE` (for example, `generic`) in the environment of the runner that hosts the extension, then restart that runner. Forked agents and follow-ups keep the conversation's saved settings.

## Commands

Run without arguments, the binary serves the Kodelet extension protocol over stdio. It also accepts:

| Command | Purpose |
| --- | --- |
| `migrate DATABASE` | Upgrade a subagent SQLite database to the latest schema revision. |
| `version` | Print the version. |
| `help` | Print usage. |

Databases are upgraded automatically when the extension opens them, so `migrate` is only needed to prepare a database ahead of time.

## License

Licensed under the MIT License. See `LICENSE`.

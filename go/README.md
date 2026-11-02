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

Kodelet discovers executables named `kodelet-extension-*` on the runner's `PATH`, so installing the binary into a `PATH` directory is all that is needed. Download the build for your platform from the [releases page](https://github.com/jingkaihe/kodelet-subagent/releases):

```bash
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac
curl -fsSL -o ~/.local/bin/kodelet-extension-subagent \
  "https://github.com/jingkaihe/kodelet-subagent/releases/latest/download/kodelet-extension-subagent-${os}-${arch}"
chmod +x ~/.local/bin/kodelet-extension-subagent
```

Builds are published for Linux and macOS on amd64 and arm64, alongside a `checksums.txt` file. Restart the runner after installing, then verify that it discovers the extension:

```bash
kodelet extension inspect subagent
```

## Upgrading from the Python package

The Python package was installed as the `jingkaihe@kodelet-subagent` plugin. Kodelet will not start two extensions that register the same tools, so replace the plugin instead of running both:

1. Stop the runner and remove the plugin with `rm -rf ~/.kodelet/plugins/jingkaihe@kodelet-subagent`.
2. Install `kodelet-extension-subagent` on `PATH` as described above, then start the runner.

The first time the extension opens its data directory, it imports the plugin's database from the neighboring `jingkaihe@kodelet-subagent_subagent` data directory. Existing agents can still be listed, waited on, and resumed with `followup_agent`; agents that were still running are marked interrupted. The import only happens while the new database does not exist yet, and the old database is only read, so you can delete it once you no longer need it. If the old database cannot be imported, tool calls report why; move it aside to start with an empty database.

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

## Development

Tools and tasks are managed with [mise](https://mise.jdx.dev):

```bash
mise install          # Go, golangci-lint, gofumpt, and GoReleaser
mise run fmt          # format with gofumpt and goimports
mise run check        # formatting, whitespace, tidiness, lint, race tests, build, release config
mise run build        # build bin/kodelet-extension-subagent
mise run snapshot     # cross-compile unpublished release binaries into dist/
```

## License

Licensed under the MIT License. See `LICENSE`.

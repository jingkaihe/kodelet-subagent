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

Kodelet discovers executables named `kodelet-extension-*` on the runner's `PATH`, so installing the binary into a `PATH` directory is all that is needed. To install the latest release into `~/.local/bin`:

```bash
curl -fsSL https://raw.githubusercontent.com/jingkaihe/kodelet-subagent/main/install.sh | bash
```

The script downloads the build for your platform (Linux or macOS, amd64 or arm64) from [GitHub Releases](https://github.com/jingkaihe/kodelet-subagent/releases), verifies it against the release's `checksums.txt`, and moves it into place, so it also upgrades a copy the runner is using. It warns when the install directory is not on `PATH`, when another `kodelet-extension-subagent` comes first on `PATH`, or when the Python plugin is still installed. It reads these environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KODELET_SUBAGENT_VERSION` | `latest` | Version to install, such as `0.7.0`. |
| `KODELET_SUBAGENT_INSTALL_DIR` | `~/.local` | Install prefix; the binary goes in its `bin` directory. |
| `GH_TOKEN`, `GITHUB_TOKEN` | | GitHub token for looking up the latest version, which raises the API rate limit. Without one, the script uses `gh auth token` when the GitHub CLI is signed in. |

For example, `curl -fsSL https://raw.githubusercontent.com/jingkaihe/kodelet-subagent/main/install.sh | KODELET_SUBAGENT_VERSION=0.7.0 bash` installs a specific version. The token is only sent to the GitHub API, never to the download, and a pinned version needs no lookup at all. When the API refuses the lookup, the script reads the latest version from the releases page instead.

You can also download `kodelet-extension-subagent-<os>-<arch>` from the releases page yourself, or, with Go installed, build it into `$(go env GOPATH)/bin`, which must then be on the runner's `PATH`:

```bash
go install github.com/jingkaihe/kodelet-subagent/cmd/kodelet-extension-subagent@latest
```

Restart the runner after installing, then verify that it discovers the extension:

```bash
kodelet extension inspect subagent
```

## Upgrading from the Python package

Releases up to 0.6.7 were a Python package on PyPI, which is no longer updated. It was installed as the `jingkaihe@kodelet-subagent` plugin. Kodelet will not start two extensions that register the same tools, so replace the plugin instead of running both:

1. Stop the runner and remove the plugin with `rm -rf ~/.kodelet/plugins/jingkaihe@kodelet-subagent`.
2. Install `kodelet-extension-subagent` with the one-line installer above, then start the runner.

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

## License

Licensed under the MIT License. See `LICENSE`.

# Kodelet subagent

A Kodelet extension for running named background agents that you can wait on, steer, cancel, and give follow-up work. It ships as a single static executable, `kodelet-extension-subagent`.

## Tools

| Tool | Purpose |
| --- | --- |
| `spawn_agent` | Start a named background agent. |
| `wait_agent` | Wait for an agent and return its result. |
| `list_agents` | List this conversation's agents. |
| `followup_agent` | Give an agent a new task. |
| `steer_agent` | Send guidance to a running agent. |
| `cancel_agent` | Cancel a running agent. |

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

## Agent mailboxes (opt-in)

Requires Kodelet 0.8.21-beta or later. Set `KODELET_AGENT_MAILBOX=enabled` on the runner hosting main and worker conversations, then restart it. Participants share the extension's data directory; each has its own inbox scoped to the main conversation. Workers can message main and one another without gaining lifecycle controls.

| Tool | Purpose |
| --- | --- |
| `list_contacts()` | Discover main and worker contacts. |
| `message_contact({contact_id, subject, body})` | Send attributed mail without waking or interrupting its recipient. |
| `list_mails({status?, limit?, offset?})` | List headers only; defaults to unread, newest first. |
| `read_mails({ids})` | Read bodies and atomically mark them read; safe to re-read. |

## Commands

```bash
kodelet-extension-subagent                                   # serve the extension over stdio; Kodelet runs this
kodelet-extension-subagent version                           # print the version
kodelet-extension-subagent migrate path/to/subagents.sqlite  # upgrade a database ahead of time
```

## License

Licensed under the MIT License. See `LICENSE`.

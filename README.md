# Kodelet subagent

A Kodelet extension for running named background agents that you can wait on, steer, cancel, and give follow-up work.

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

## Model profiles

Fresh agents use the parent conversation's model profile. To use a different profile, set `KODELET_SUBAGENT_PROFILE` (for example, `generic`) in the environment of the runner that hosts the extension, then restart that runner. Forked agents and follow-ups keep the conversation's saved settings.

## Installation

```bash
uvx kodelet-subagent install
```

Or install from GitHub:

```bash
uvx --from git+https://github.com/jingkaihe/kodelet-subagent kodelet-subagent install
```

Verify that the runner discovers the extension:

```bash
kodelet extension inspect jingkaihe@kodelet-subagent/subagent
```

## License

Licensed under the MIT License. See `LICENSE`.

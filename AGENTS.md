# Repository guide

Kodelet extension for durable background agents, shipped as one Go binary, `kodelet-extension-subagent`, that Kodelet discovers on `PATH` (extension ID `subagent`).

## Layout

- `cmd/kodelet-extension-subagent`: stdio server, plus `migrate DATABASE`, `version`, and `help`.
- `internal/subagent`: tools, schemas, runtime (setup, workers, steering, cleanup, shutdown), widgets, and SDK adapters.
- `internal/db`: SQLite store (`modernc.org/sqlite`, `sqlx`), embedded migrations, and legacy import.
- `internal/types` domain model, `internal/paths` path resolution, `internal/version` the embedded `VERSION`.

## Invariants

- Store transitions run in IMMEDIATE transactions fenced by lease token, generation, runtime ID, and expiry.
- Forks use `ForkConversation` in the tool handler, then resume through the SDK `agent.Client`; fresh agents open a new ACP session. Use `RunAndWait`, `Steer`, and session task progress; the SDK owns the ACP transport.
- `liveRuns` maps an agent to its current generation; `ownedRuns` keeps every run until its cleanup completes. Failed and canceled runs hold their claim, with a heartbeat, until client cleanup finishes.
- Shutdown stops launches, drains reservations, cancels and awaits owned runs and cleanup, then reconciles rows.
- Only fresh agents may choose a cwd; forks and resumes keep theirs.

## Development

```bash
mise install          # Go, golangci-lint, gofumpt, ShellCheck, GoReleaser
mise run fmt          # gofumpt, goimports, gci
mise run check        # fmt, whitespace, tidy, lint, shell lint, race tests, build, release config
mise run snapshot     # cross-compile release binaries into dist/
```

The bash tool forbids `cd`; use `mise -C <repo> run <task>` and `go -C <repo> ...`. Keep files small, write multi-line text as raw strings, and leave no trailing whitespace, including in SQL. Do not commit `bin/` or `dist/`.

For end-to-end checks, follow `../kodelet/.kodelet/skills/self-test` with `kodelet` and `kodelet-extension-subagent` first on the candidate's `PATH`.

## Migrations

Add one SQL file per schema change under `internal/db/migrations/`, list it in `internal/db/migrate.go`, and bump `HeadRevision`. `alembic_version` stays the version authority; unversioned non-empty databases are rejected. Test fresh, upgraded, repeated, and imported databases.

## Legacy import

On first use, the store imports the Python plugin's `jingkaihe@kodelet-subagent_subagent` database read-only, interrupting its active claims (`internal/db/import.go`).

## Releases

Bump `internal/version/VERSION`, commit on `main`, and run `mise run release`; the tag's workflow publishes the GoReleaser binaries. Keep `install.sh` asset names in step with `.goreleaser.yaml`.

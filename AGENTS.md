# Repository guide

## Overview

`kodelet-subagent` is a Kodelet extension for durable background agents, shipped as a single Go binary, `kodelet-extension-subagent`. Kodelet discovers it on `PATH` and derives the extension ID `subagent` from the filename.

- `cmd/kodelet-extension-subagent` serves the extension protocol over stdio when run without arguments, and also provides `migrate DATABASE`, `version`, and `help`.
- `internal/subagent` owns the runtime: tool handlers, descriptions, and schemas (`app.go`, `tools.go`, `wait.go`, `schemas.json`), reservations and setup (`setup.go`), live workers (`worker.go`, `heartbeat.go`, `steering.go`), cleanup and shutdown (`cleanup.go`, `shutdown.go`), widgets and progress (`ui.go`), SDK adapters (`sdk.go`), stdio serving (`serve.go`), and the legacy database import hook (`legacy.go`).
- `internal/db` owns the SQLite store, transactional state changes, embedded migrations, and database import, using `modernc.org/sqlite` and `sqlx`.
- `internal/types` holds the domain model, `internal/paths` path resolution, and `internal/version` the embedded `VERSION`.

Store transitions run in explicit IMMEDIATE transactions. Preserve the lease token, generation, runtime ID, and expiry checks when changing store behavior.

Runtime ownership covers database reservation, background-lease/named-conversation setup, the live ACP worker, and final SDK-client/background-lease cleanup. Fork parent context with `ForkConversation` (a named child) in the originating tool handler, then resume that conversation through the SDK `agent.Client`; fresh agents create a new ACP session. Use `RunAndWait`, `Steer`, and task progress attached to the session rather than an alternate execution or event API. The SDK owns bounded ACP transport and partial-startup/repeated-cancellation cleanup; do not duplicate that transport here.

`liveRuns` is only the current-generation lookup for an agent; `ownedRuns` retains every generation by run ID until its cleanup completes. Failure and cancellation must retain their active database claim until client cleanup completes, with a fenced heartbeat throughout cleanup. Shutdown must stop new launches, drain reservations, cancel and await all owned runs, await runtime-owned cleanup, and only then reconcile remaining rows. ACP requires normal daemon client authentication; do not widen runner credentials. Fresh agents may select another directory within runner policy; forks and resumes retain their saved cwd.

## Development

Tools and tasks are managed with mise:

```bash
mise install
mise run fmt
mise run check
```

`mise run check` runs `fmt:check`, `whitespace`, `tidy:check`, `lint`, `test` (race detector), `build`, and `release:check`; each is also a task of its own. `mise run snapshot` cross-compiles the release binaries into `dist/`. The bash tool forbids `cd`; use `mise -C <repo> run <task>` and `go -C <repo> ...` instead.

Format with gofumpt (extra rules), goimports, and gci through `golangci-lint fmt`. Keep files focused rather than large, write multi-line text as raw strings, and leave no trailing whitespace anywhere, including SQL.

Do not commit `bin/`, `dist/`, or other build output.

For end-to-end checks, follow the `self-test` skill in `../kodelet/.kodelet/skills/self-test`: run an isolated candidate server with `$SELF_TEST_ROOT/bin` (holding `kodelet` and `kodelet-extension-subagent`) first on `PATH`, and exercise spawn, wait, fork, steer, cancel, follow-up, forced stop, and resume.

## Migrations

Migrations are SQL files embedded from `internal/db/migrations/` and listed in order in `internal/db/migrate.go`; update `HeadRevision` with each new one. Each runs in its own transaction. The `alembic_version` table, inherited from the earlier Python implementation, remains the sole schema-version authority.

Non-empty databases without version metadata are rejected rather than adopted or stamped. Add an explicit migration for every schema change.

When adding a migration, test a fresh database, upgrade from the previous revision, repeated initialization, preservation of active and historical rows, and import of a database at the previous revision.

## Legacy import

Releases up to 0.6.7 were a Python package installed as the `jingkaihe@kodelet-subagent` plugin, whose data lives in the `jingkaihe@kodelet-subagent_subagent` data directory. On first use, the default store imports that database (`internal/subagent/legacy.go`, `internal/db/import.go`). The import only runs while the new database does not exist, is staged, upgraded, and verified before it is renamed into place, never writes to the source, and interrupts claims that were active in the copy because the old installation is assumed to have stopped.

## Releases

`internal/version/VERSION` is the sole version source. `mise run release` runs the checks, creates the annotated tag `v<version>` on a clean worktree, and pushes it. The release workflow verifies the tag against `VERSION`, reruns the checks, and publishes the GoReleaser binaries and `checksums.txt` to GitHub Releases.

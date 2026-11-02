# Repository guide

## Overview

`kodelet-subagent` is a Python package and Kodelet extension for durable background agents. Package source lives under `src/kodelet_subagent/`; behavior tests live under `tests/`; the repository-installable Kodelet wrapper lives under `extensions/subagent/`.

The persistence layer uses raw `sqlite3` transactions for runtime state transitions and Alembic only for schema management. Preserve the lease token, generation, runtime ID, and expiry checks when changing store behavior.

Key modules:

- `extension.py` owns Kodelet lifecycle registration and public tool handlers.
- `runtime.py` owns reservations, setup tasks, live workers, and final cleanup tasks.
- `ui.py` owns snapshots, progress forwarding, and widget presentation.
- `persistence/` owns records, transactional state changes, and migrations.
- `install.py` owns the idempotent global Kodelet extension-wrapper installer.

Runtime ownership covers database reservation, background-lease/named-conversation setup, the live ACP worker, and final SDK-client/background-lease cleanup. Fork parent context with `ctx.fork_conversation(name=agent.name)` in the originating tool handler, then resume that conversation through the SDK `Client`; fresh agents create a new ACP session. Use `session.run_and_wait`, `session.steer`, and `TaskProgress.attach(session)` rather than an alternate execution or event API. The SDK owns bounded ACP transport and partial-startup/repeated-cancellation cleanup; do not duplicate that transport here.

`live_runs` is only the current-generation lookup for an agent; `owned_runs` retains every generation by run ID until its finalizer completes. Failure and cancellation must retain their active database claim until client cleanup completes, with a fenced heartbeat throughout cleanup. Shutdown must stop new launches, drain reservations, cancel and await all `owned_runs`, await runtime-owned finalizers, and only then reconcile remaining rows. ACP requires normal daemon client authentication; do not widen runner credentials. Fresh agents may select another directory within runner policy; forks and resumes retain their saved cwd.

## Development

Install the locked environment:

```bash
uv sync --locked
```

Run all checks:

```bash
make check
```

Individual checks:

```bash
uv run -- ruff check
uv run -- ruff format --check
uv run -- ty check
uv run -- pytest -q
uv build
```

Do not commit `.venv`, `dist`, caches, coverage output, or generated wheel/sdist files.

## Go implementation

`go/` holds the Go rewrite: a single `kodelet-extension-subagent` binary that Kodelet discovers on `PATH` (extension ID `subagent`). It mirrors the Python behavior, tool schemas, and SQLite schema; `alembic_version` stays the schema-version authority, so databases move freely between the two implementations.

- `cmd/kodelet-extension-subagent` serves stdio with no arguments and also provides `migrate DATABASE` and `version`.
- `internal/types` holds the domain model, `internal/db` the store and embedded SQL migrations (`modernc.org/sqlite` + `sqlx`), `internal/paths` path resolution, `internal/version` the embedded `VERSION`, and `internal/subagent` the runtime, tools, UI, and SDK adapters.
- On first use, the default store imports the Python plugin's database from the neighboring `jingkaihe@kodelet-subagent_subagent` data directory (`internal/subagent/legacy.go`, `internal/db/import.go`). The import is staged, upgraded, and verified before it is renamed into place, never writes to the source, and interrupts claims that were active in the copy because the old installation is assumed to have stopped.
- Tools and tasks are managed with mise from `go/`: `mise install`, `mise run fmt`, `mise run check` (fmt, whitespace, tidy, golangci-lint, race tests, build, GoReleaser config), and `mise run snapshot`. The bash tool forbids `cd`; use `mise -C go run <task>`.
- Format with gofumpt (extra rules) and gci via `golangci-lint fmt`; write multi-line text as raw strings and leave no trailing whitespace anywhere, including SQL.
- For end-to-end checks, follow the `self-test` skill in `../kodelet/.kodelet/skills/self-test`: run an isolated candidate server with `$SELF_TEST_ROOT/bin` (holding `kodelet` and `kodelet-extension-subagent`) first on `PATH`, and exercise spawn, wait, fork, steer, cancel, follow-up, forced stop, and resume.

## Migrations

Alembic revisions are packaged under `src/kodelet_subagent/persistence/migrations/versions/`. Keep `alembic_version` as the sole schema-version authority.

This project is pre-release and intentionally does not adopt or stamp non-empty databases that lack Alembic metadata. Add an explicit Alembic revision for every schema change.

When adding a migration, test a fresh database, upgrade from the previous revision, repeated initialization, and preservation of active and historical rows.

## Releases

`pyproject.toml` is the sole hand-maintained package version source. After changing it, run `make sync-version` to refresh `uv.lock` and the generated pin in `extensions/subagent/kodelet-extension-subagent`; source checkouts read the version from `pyproject.toml`, while built packages read their distribution metadata. The release workflow publishes tags named `v<version>` to PyPI using trusted publishing.

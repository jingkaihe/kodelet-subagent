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

Runtime ownership covers database reservation, background-lease/conversation setup, the live worker, and final client/background-lease cleanup. `live_runs` is only the current-generation lookup for an agent; `owned_runs` retains every generation by run ID until its finalizer completes. Shutdown must stop new launches, drain reservations, cancel and await all `owned_runs`, await runtime-owned finalizers, and only then reconcile remaining rows.

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

## Migrations

Alembic revisions are packaged under `src/kodelet_subagent/persistence/migrations/versions/`. Keep `alembic_version` as the sole schema-version authority.

This project is pre-release and intentionally does not adopt or stamp non-empty databases that lack Alembic metadata. Add an explicit Alembic revision for every schema change.

When adding a migration, test a fresh database, upgrade from the previous revision, repeated initialization, and preservation of active and historical rows.

## Releases

`pyproject.toml` is the sole hand-maintained package version source. After changing it, run `make sync-version` to refresh `uv.lock` and the generated pin in `extensions/subagent/kodelet-extension-subagent`; source checkouts read the version from `pyproject.toml`, while built packages read their distribution metadata. The release workflow publishes tags named `v<version>` to PyPI using trusted publishing.

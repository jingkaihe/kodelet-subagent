# Repository guide

## Overview

`kodelet-subagent` is a Python package and Kodelet extension for durable background agents. Package source lives under `src/kodelet_subagent/`; behavior tests live under `tests/`; the repository-installable Kodelet wrapper lives under `extensions/subagent/`.

The persistence layer uses raw `sqlite3` transactions for runtime state transitions and Alembic only for schema management. Preserve the lease token, generation, runtime ID, and expiry checks when changing store behavior.

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

Alembic revisions are packaged under `src/kodelet_subagent/persistence/migrations/versions/`. Keep `alembic_version` authoritative and update `PRAGMA user_version` in each revision as a compatibility mirror.

The initial revision represents the exact schema used by the former `jingkaihe/skills` extension. Existing version-1 databases must be structurally validated and stamped rather than recreated.

When adding a migration, test a fresh database, upgrade from the previous revision, repeated initialization, and preservation of active and historical rows.

## Releases

The package version in `pyproject.toml`, extension metadata, and the pinned version in `extensions/subagent/kodelet-extension-subagent` must match. The release workflow publishes tags named `v<version>` to PyPI using trusted publishing.

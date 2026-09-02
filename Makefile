VERSION := $(shell uv version --short)
TAG := v$(VERSION)

.PHONY: sync sync-version version-check lint format-check typecheck test build check version tag release clean

sync:
	uv sync --locked

sync-version:
	uv lock
	uv run --no-sync -- python -c 'from kodelet_subagent.install import sync_repository_extension_wrapper; sync_repository_extension_wrapper(version="$(VERSION)")'

version-check:
	uv lock --check
	@uv run --no-sync -- python -c 'import sys; from pathlib import Path; from kodelet_subagent.install import repository_extension_wrapper; path = Path("extensions/subagent/kodelet-extension-subagent"); sys.exit("repository extension wrapper is stale; run `make sync-version`") if path.read_text(encoding="utf-8") != repository_extension_wrapper("$(VERSION)") else None'

lint:
	uv run -- ruff check

format-check:
	uv run -- ruff format --check

typecheck:
	uv run -- ty check

test:
	uv run -- pytest -q

build:
	uv build

check: version-check lint format-check typecheck test build

version:
	@printf '%s\n' '$(VERSION)'

tag:
	@test -z "$$(git status --porcelain)" || { echo 'worktree must be clean' >&2; exit 2; }
	@if git rev-parse '$(TAG)' >/dev/null 2>&1; then \
		echo 'tag $(TAG) already exists' >&2; \
		exit 2; \
	fi
	git tag -a '$(TAG)' -m '$(TAG)'

release: check tag
	git push origin HEAD
	git push origin '$(TAG)'

clean:
	rm -rf dist build .pytest_cache .ruff_cache .ty_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

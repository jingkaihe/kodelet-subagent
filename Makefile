VERSION := $(shell uv run --no-sync -- python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')
TAG := v$(VERSION)

.PHONY: sync lint format-check typecheck test build check version tag release clean

sync:
	uv sync --locked

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

check: lint format-check typecheck test build

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

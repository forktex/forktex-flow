# Hand-maintained. There is no generator for this file and no `forktex.json` at
# the root — the FSD tooling that once produced Makefiles lives in
# forktex-py/backlog/py/fsd/ and cannot run here. Edit this file directly.
.DEFAULT_GOAL := help
PROJECT_NAME := forktex-flow
COVERAGE_FLOOR := 96

help: ## Self-documenting target listing
	@grep -E '^[a-zA-Z_@-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

parse-check: ## Every source file parses on the interpreter this project targets
	@# Runs FIRST and through poetry deliberately. PEP 758 (`except A, B:`) and
	@# PEP 695 generics are valid 3.14 and a SyntaxError on 3.12 — checking with
	@# a system python has produced three separate false "syntax error" reports
	@# in this codebase. This script refuses to run below 3.14 rather than lie.
	poetry run python3 scripts/check_parse.py

format: ## Source code conforms to a consistent, auto-enforced style
	poetry run python3 -m ruff format src/ tests/

format-check: ## Check formatting without rewriting files
	poetry run python3 -m ruff format --check src/ tests/

lint: ## Static analysis catches bugs, anti-patterns, and security smells
	poetry run python3 -m ruff check src/ tests/

lint-fix: ## Apply every auto-fixable lint finding
	poetry run python3 -m ruff check --fix src/ tests/

typecheck: ## Static type system verifies interface correctness (pyright)
	poetry run python3 -m pyright src/

test: ## Automated tests verify behaviour, with a coverage floor
	@# The floor ratchets: raise it when a pass adds real coverage, never lower it
	@# to make a change pass. Measured 97.04%, repeatably (five consecutive runs).
	@# The floor sits one point below that as headroom, not because coverage
	@# drifts: the driver and heartbeat loops are timing-dependent, so a future
	@# test touching them could legitimately vary by a few statements, and a gate
	@# that fails at random is one people learn to re-run rather than read.
	@# Note: the term report ROUNDS, so it prints 97% while --cov-fail-under
	@# compares the exact figure. Read the number, not the printed percentage.
	@# Integration tests use a real Postgres via testcontainers — Docker must be
	@# running. There are no mocks for it.
	poetry run python3 -m pytest tests/ -q --cov=src/forktex_flow \
		--cov-report=term-missing --cov-fail-under=$(COVERAGE_FLOOR)

audit: ## Scan the project venv for known CVEs in installed deps
	poetry run python3 -m pip_audit --skip-editable --timeout 30

license-check: ## Verify every source file carries the dual-license header (CI gate)
	poetry run python3 scripts/license_headers.py check

license-fix: ## Add or update the dual-license header on every source file
	poetry run python3 scripts/license_headers.py fix

install: ## Bootstrap the project to a runnable state
	poetry install --with dev
	@# `poetry install` provisions the venv's own pip, which lags behind and has
	@# tripped `make audit` on a pip CVE unrelated to this project's deps.
	poetry run python3 -m pip install -q --upgrade "pip>=26.1.2"

build: ## Build sdist + wheel into dist/ and verify metadata with twine check
	rm -rf dist/
	poetry run python3 -m build
	poetry run python3 -m twine check dist/*

smoke: ## Install the BUILT wheel into a throwaway venv and run a real workflow
	@# `make ci` otherwise only tests the source tree, so a packaging fault is
	@# invisible to it: a data file left out of the wheel, a module that only
	@# imports because the repo root is on sys.path. This installs the wheel into
	@# a clean venv and runs a workflow end to end from site-packages — including
	@# `Flow.init()`, which is what proves the .sql migrations actually shipped.
	@# Needs Docker, same as `make test`.
	@set -e; \
	WHL=$$(ls dist/forktex_flow-*-py3-none-any.whl 2>/dev/null | head -1); \
	if [ -z "$$WHL" ]; then echo "  no wheel in dist/ — run 'make build' first"; exit 1; fi; \
	PY=$$(poetry run python3 -c 'import sys; print(sys.executable)'); \
	TMP=$$(mktemp -d); trap 'rm -rf $$TMP' EXIT; \
	echo "smoke: install from $$WHL (interpreter $$PY)"; \
	$$PY -m venv $$TMP/v >/dev/null; \
	$$TMP/v/bin/pip install -q "$$WHL" "testcontainers[postgres]>=4.0"; \
	$$TMP/v/bin/python -c "import forktex_flow; assert forktex_flow.__all__"; \
	cd $$TMP && $$TMP/v/bin/python $(CURDIR)/scripts/wheel_smoke.py

# Both upload targets run release-check first: it is the only thing standing
# between a stale dist/ and an immutable version on an index.
publish-test: ## Upload dist/ to TestPyPI for rehearsal
	@$(MAKE) release-check
	poetry run python3 -m twine upload -r testpypi dist/*

publish: ## Upload dist/ to production PyPI (irreversible per-version)
	@$(MAKE) release-check
	poetry run python3 -m twine upload dist/*

clean: ## Build artifacts and caches removable from project tree
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name dist -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name .coverage -delete 2>/dev/null; true

release-check: ## Version, tag and CHANGELOG agree (pass TAG=v0.1.0 to check a tag)
	poetry run python3 scripts/release_check.py $(TAG)

ci: ## Aggregate quality gate before publish
	@$(MAKE) parse-check
	@$(MAKE) format-check
	@$(MAKE) lint
	@$(MAKE) license-check
	@$(MAKE) typecheck
	@$(MAKE) audit
	@$(MAKE) test
	@$(MAKE) build
	@$(MAKE) smoke
	@$(MAKE) release-check
	@echo ''
	@echo 'CI passed for $(PROJECT_NAME) — safe to: make publish-test  /  make publish'

.PHONY: help parse-check format format-check lint lint-fix typecheck test audit \
	license-check license-fix install build smoke release-check publish-test publish clean ci

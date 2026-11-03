.DEFAULT_GOAL := help
PROJECT_NAME := forktex-flow
COVERAGE_FLOOR := 96

help: ## Self-documenting target listing
	@grep -E '^[a-zA-Z_@-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

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
	@# Needs Docker: the tests run against a real Postgres.
	poetry run python3 -m pytest tests/ -q --cov=src/forktex_flow \
		--cov-report=term-missing --cov-precision=2 --cov-fail-under=$(COVERAGE_FLOOR)

audit: ## Scan the project venv for known CVEs in installed deps
	poetry run python3 -m pip_audit --skip-editable --timeout 30

license-check: ## Verify every source file carries the dual-license header (CI gate)
	poetry run python3 scripts/license_headers.py check

license-fix: ## Add or update the dual-license header on every source file
	poetry run python3 scripts/license_headers.py fix

install: ## Bootstrap the project to a runnable state
	poetry install --with dev
	poetry run python3 -m pip install -q --upgrade "pip>=26.1.2"

build: ## Build sdist + wheel into dist/ and verify metadata with twine check
	rm -rf dist/
	poetry run python3 -m build
	poetry run python3 -m twine check dist/*

smoke: ## Install the BUILT wheel into a throwaway venv and run a real workflow
	@# Needs Docker.
	@set -e; \
	WHL=$$(ls dist/forktex_flow-*-py3-none-any.whl 2>/dev/null | head -1); \
	if [ -z "$$WHL" ]; then echo "  no wheel in dist/ — run 'make build' first"; exit 1; fi; \
	PY=$$(poetry run python3 -c 'import sys; print(sys.executable)'); \
	TMP=$$(mktemp -d); trap 'rm -rf $$TMP' EXIT; \
	echo "smoke: install from $$WHL (interpreter $$PY)"; \
	$$PY -m venv $$TMP/v >/dev/null; \
	$$TMP/v/bin/pip install -q "$$WHL" "testcontainers[postgres]>=4.15"; \
	$$TMP/v/bin/python -c "import forktex_flow; assert forktex_flow.__all__"; \
	cd $$TMP && $$TMP/v/bin/python $(CURDIR)/scripts/wheel_smoke.py

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

release-check: ## Version, tag and CHANGELOG agree (pass TAG=v1.0.0 to check a tag)
	poetry run python3 scripts/release_check.py $(TAG)

lock-check: ## The lock file resolves the current pyproject.toml
	poetry check --lock

ci: ## Aggregate quality gate before publish
	@$(MAKE) lock-check
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

.PHONY: help lock-check format format-check lint lint-fix typecheck test audit \
	license-check license-fix install build smoke release-check publish-test publish clean ci

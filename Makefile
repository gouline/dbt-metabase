export SETUPTOOLS_SCM_PRETEND_VERSION ?= 0.0.0

.PHONY: help
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z0-9_-]+:.*## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: dependencies
dependencies: ## Install project dependencies
	uv sync --no-install-project --all-extras --frozen

.PHONY: upgrade
upgrade: ## Upgrade project dependencies
	uv sync --no-install-project --all-extras --upgrade

.PHONY: build
build: clean ## Build the project
	uv run python3 -m build

.PHONY: clean
clean: ## Clean build artifacts
	rm -rf build dist

.PHONY: fix
fix: ## Fix code formatting and linting issues
	uv run ruff format .
	uv run ruff check --fix .

.PHONY: check-lint
check-lint: ## Check code formatting and linting
	uv run ruff format --check .
	uv run ruff check .

.PHONY: check-type
check-type: ## Run type checking
	uv run pyright dbtmetabase

.PHONY: check
check: check-lint check-type ## Run all code quality checks

.PHONY: test
test: ## Run tests
	rm -rf tests/tmp
	uv run pytest tests

.PHONY: pre
pre: fix check test ## Run pre-commit checks (fix, check, test)

.PHONY: dist-check
dist-check: build ## Check distribution package
	uv run twine check dist/*

.PHONY: dist-upload
dist-upload: check ## Upload distribution to PyPI
	uv run twine upload dist/*

.PHONY: install
install: build ## Install built package locally
	uv pip uninstall dbt-metabase \
		&& uv pip install dist/dbt_metabase-*-py3-none-any.whl

.PHONY: sandbox-up
sandbox-up: ## Start sandbox environment (use TARGET=databricks for Databricks)
	@if [ "$(TARGET)" = "databricks" ]; then \
		echo "🚀 Starting with Databricks initialization"; \
		( cd sandbox && env $$(cat .env .env.databricks | grep -v '^#' | xargs) TARGET_COMMAND=init_databricks docker compose up --build --attach app ); \
	else \
		echo "🐘 Starting with PostgreSQL-only initialization"; \
		( cd sandbox && docker compose up --build --attach app ); \
	fi

.PHONY: sandbox-down
sandbox-down: ## Stop sandbox environment
	( cd sandbox && docker compose down )

.PHONY: sandbox-models
sandbox-models: ## Export dbt models to Metabase (use TARGET=databricks for Databricks)
	@if [ "$(TARGET)" = "databricks" ]; then \
		echo "🚀 Running dbt-metabase models with Databricks"; \
		( cd sandbox && . .env && . .env.databricks && uv run python3 -m dbtmetabase models \
			--manifest-path target/manifest.json \
			--metabase-url http://localhost:$$MB_PORT \
			--metabase-username $$MB_USER \
			--metabase-password $$MB_PASSWORD \
			--metabase-database $$DATABRICKS_MB_DB_NAME \
			--http-header x-dummy-key dummy-value \
			--order-fields \
			--verbose ); \
	else \
		( . sandbox/.env && uv run python3 -m dbtmetabase models \
			--manifest-path sandbox/target/manifest.json \
			--metabase-url http://localhost:$$MB_PORT \
			--metabase-username $$MB_USER \
			--metabase-password $$MB_PASSWORD \
			--metabase-database $$POSTGRES_DB \
			--include-schemas "pub*",inventory \
			--http-header x-dummy-key dummy-value \
			--order-fields \
			--verbose ); \
	fi

.PHONY: sandbox-exposures
sandbox-exposures: ## Extract dbt exposures from Metabase (use TARGET=databricks for Databricks)
	@if [ "$(TARGET)" = "databricks" ]; then \
		echo "🚀 Running dbt-metabase exposures with Databricks"; \
		rm -rf sandbox/models/exposures; \
		mkdir -p sandbox/models/exposures; \
		( cd sandbox && . .env && . .env.databricks && uv run python3 -m dbtmetabase exposures \
			--manifest-path target/manifest.json \
			--metabase-url http://localhost:$$MB_PORT \
			--metabase-username $$MB_USER \
			--metabase-password $$MB_PASSWORD \
			--output-path models/exposures \
			--output-grouping collection \
			--tag metabase \
			--verbose ); \
		( cd sandbox && . .env && . .env.databricks && \
			DATABRICKS_HOST=$$DATABRICKS_HOST \
			DATABRICKS_HTTP_PATH=$$DATABRICKS_HTTP_PATH \
			DATABRICKS_TOKEN=$$DATABRICKS_TOKEN \
			DATABRICKS_CATALOG=$$DATABRICKS_CATALOG \
			DATABRICKS_SCHEMA=$$DATABRICKS_SCHEMA \
			uv run dbt docs generate --profiles-dir . --target databricks ); \
	else \
		rm -rf sandbox/models/exposures; \
		mkdir -p sandbox/models/exposures; \
		( . sandbox/.env && uv run python3 -m dbtmetabase exposures \
			--manifest-path sandbox/target/manifest.json \
			--metabase-url http://localhost:$$MB_PORT \
			--metabase-username $$MB_USER \
			--metabase-password $$MB_PASSWORD \
			--output-path sandbox/models/exposures \
			--output-grouping collection \
			--tag metabase \
			--verbose ); \
		( . sandbox/.env && cd sandbox && \
			POSTGRES_HOST=localhost \
			POSTGRES_PORT=$$POSTGRES_PORT \
			POSTGRES_USER=$$POSTGRES_USER \
			POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
			POSTGRES_DB=$$POSTGRES_DB \
			POSTGRES_SCHEMA=$$POSTGRES_SCHEMA \
			uv run dbt docs generate ); \
	fi

.PHONY: sandbox-e2e
sandbox-e2e: sandbox-up sandbox-models sandbox-exposures sandbox-down ## Run full end-to-end sandbox test

export SETUPTOOLS_SCM_PRETEND_VERSION ?= 0.0.0

.PHONY: dependencies
dependencies:
	uv sync --no-install-project --all-extras --frozen

.PHONY: upgrade
upgrade:
	uv sync --no-install-project --all-extras --upgrade

.PHONY: build
build: clean
	uv run python3 -m build

.PHONY: clean
clean:
	rm -rf build dist

.PHONY: fix
fix:
	uv run ruff format .
	uv run ruff check --fix .

.PHONY: check-lint
check-lint:
	uv run ruff format --check .
	uv run ruff check .

.PHONY: check-type
check-type:
	uv run mypy dbtmetabase

.PHONY: check
check: check-lint check-type

.PHONY: test
test:
	rm -rf tests/tmp
	uv run pytest tests

.PHONY: pre
pre: fix check test

.PHONY: dist-check
dist-check: build
	uv run twine check dist/*

.PHONY: dist-upload
dist-upload: check
	uv run twine upload dist/*

.PHONY: install
install: build
	uv pip uninstall dbt-metabase \
		&& uv pip install dist/dbt_metabase-*-py3-none-any.whl

.PHONY: sandbox-up
sandbox-up:
	( cd sandbox && docker compose up --build --attach app )

.PHONY: sandbox-up
sandbox-down:
	( cd sandbox && docker compose down )

.PHONY: sandbox-models
sandbox-models:
	( . sandbox/.env && uv run python3 -m dbtmetabase models \
		--manifest-path sandbox/target/manifest.json \
		--metabase-url http://localhost:$$MB_PORT \
		--metabase-username $$MB_USER \
		--metabase-password $$MB_PASSWORD \
		--metabase-database $$POSTGRES_DB \
		--include-schemas "pub*",inventory \
		--http-header x-dummy-key dummy-value \
		--order-fields \
		--verbose )

.PHONY: sandbox-exposures
sandbox-exposures:
	rm -rf sandbox/models/exposures
	mkdir -p sandbox/models/exposures
	( . sandbox/.env && uv run python3 -m dbtmetabase exposures \
		--manifest-path sandbox/target/manifest.json \
		--metabase-url http://localhost:$$MB_PORT \
		--metabase-username $$MB_USER \
		--metabase-password $$MB_PASSWORD \
		--output-path sandbox/models/exposures \
		--output-grouping collection \
		--tag metabase \
		--verbose )
	
	( . sandbox/.env && cd sandbox && \
		POSTGRES_HOST=localhost \
		POSTGRES_PORT=$$POSTGRES_PORT \
		POSTGRES_USER=$$POSTGRES_USER \
		POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
		POSTGRES_DB=$$POSTGRES_DB \
		POSTGRES_SCHEMA=$$POSTGRES_SCHEMA \
		uv run dbt docs generate )

.PHONY: sandbox-e2e
sandbox-e2e: sandbox-up sandbox-models sandbox-exposures sandbox-down

build: clean
	python3 -m build
.PHONY: build

clean:
	rm -rf build dist
.PHONY: clean

requirements:
	python3 -m pip install \
		-r requirements.txt \
		-r requirements-test.txt
.PHONY: requirements

fix-fmt:
	black .
.PHONY: fix-fmt

fix-imports:
	isort .
.PHONY: fix-imports

fix: fix-fmt fix-imports
.PHONY: fix

check-fmt:
	black --check .
.PHONY: check-fmt

check-imports:
	isort --check .
.PHONY: check-imports

check-lint-python:
	pylint dbtmetabase
.PHONY: check-lint-python

check-type:
	mypy dbtmetabase
.PHONY: check-type

check: check-fmt check-imports check-lint-python check-type
.PHONY: check

test:
	rm -rf tests/tmp
	python3 -m unittest tests
.PHONY: test

pre: fix check test
.PHONY: pre

dist-check: build
	twine check dist/*
.PHONY: dist-check

dist-upload: check
	twine upload dist/*
.PHONY: dist-upload

install: build
	python3 -m pip uninstall -y dbt-metabase \
		&& python3 -m pip install dist/dbt_metabase-*-py3-none-any.whl
.PHONY: install

sandbox-up:
	( cd sandbox && docker compose up --build --attach app )
.PHONY: sandbox-up

sandbox-down:
	( cd sandbox && docker compose down )
.PHONY: sandbox-up

sandbox-models:
	( . sandbox/.env && python3 -m dbtmetabase models \
		--manifest-path sandbox/target/manifest.json \
		--metabase-url http://localhost:$$MB_PORT \
		--metabase-username $$MB_USER \
		--metabase-password $$MB_PASSWORD \
		--metabase-database $$POSTGRES_DB \
		--include-schemas "pub*",other \
		--http-header x-dummy-key dummy-value \
		--order-fields \
		--verbose )
.PHONY: sandbox-models

sandbox-exposures:
	rm -rf sandbox/models/exposures
	mkdir -p sandbox/models/exposures
	( . sandbox/.env && python3 -m dbtmetabase exposures \
		--manifest-path sandbox/target/manifest.json \
		--metabase-url http://localhost:$$MB_PORT \
		--metabase-username $$MB_USER \
		--metabase-password $$MB_PASSWORD \
		--output-path sandbox/models/exposures \
		--output-grouping collection \
		--verbose )
	
	( . sandbox/.env && cd sandbox && \
		POSTGRES_HOST=localhost \
		POSTGRES_PORT=$$POSTGRES_PORT \
		POSTGRES_USER=$$POSTGRES_USER \
		POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
		POSTGRES_DB=$$POSTGRES_DB \
		POSTGRES_SCHEMA=$$POSTGRES_SCHEMA \
		dbt docs generate )
.PHONY: sandbox-exposures

sandbox-e2e: sandbox-up sandbox-models sandbox-exposures sandbox-down
.PHONY: sandbox-e2e

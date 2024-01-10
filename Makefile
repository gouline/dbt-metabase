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

dev-install: build
	python3 -m pip uninstall -y dbt-metabase \
		&& python3 -m pip install dist/dbt_metabase-*-py3-none-any.whl
.PHONY: dev-install

dev-sandbox-up:
	( cd sandbox && docker-compose up --build --attach app )
.PHONY: dev-sandbox-up

dev-sandbox-down:
	( cd sandbox && docker-compose down )
.PHONY: dev-sandbox-up

dev-sandbox-models:
	( source sandbox/.env && python3 -m dbtmetabase models \
		--dbt-manifest-path sandbox/target/manifest.json \
		--dbt-database $$POSTGRES_DB \
		--metabase-url http://localhost:$$MB_PORT \
		--metabase-username $$MB_USER \
		--metabase-password $$MB_PASSWORD \
		--metabase-database $$POSTGRES_DB )
.PHONY: dev-sandbox-models

dev-sandbox-exposures:
	rm -rf sandbox/models/exposures
	mkdir -p sandbox/models/exposures
	( source sandbox/.env && python3 -m dbtmetabase exposures \
		--dbt-manifest-path sandbox/target/manifest.json \
		--dbt-database $$POSTGRES_DB \
		--metabase-url http://localhost:$$MB_PORT \
		--metabase-username $$MB_USER \
		--metabase-password $$MB_PASSWORD \
		--output-path sandbox/models/exposures \
		--output-grouping collection )
	
	( source sandbox/.env && cd sandbox && \
		POSTGRES_HOST=localhost \
		POSTGRES_PORT=$$POSTGRES_PORT \
		POSTGRES_USER=$$POSTGRES_USER \
		POSTGRES_PASSWORD=$$POSTGRES_PASSWORD \
		POSTGRES_DB=$$POSTGRES_DB \
		POSTGRES_SCHEMA=$$POSTGRES_SCHEMA \
		dbt docs generate )
.PHONY: dev-sandbox-exposures

dev-sandbox-e2e: dev-sandbox-up dev-sandbox-models dev-sandbox-exposures dev-sandbox-down
.PHONY: dev-sandbox-e2e

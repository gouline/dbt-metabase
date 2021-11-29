build: clean
	python3 setup.py sdist bdist_wheel

clean:
	rm -rf build dist

requirements:
	pip3 install -r requirements.txt 
	pip3 install -r requirements-test.txt
.PHONY: requirements

fmt:
	black .

check-fmt:
	black --check .
.PHONY: check-fmt

check-lint-python:
	pylint dbtmetabase
.PHONY: check-lint-python

check-lint-rst:
	rst-lint README.rst
.PHONY: check-lint-rst

check-type:
	mypy dbtmetabase
.PHONY: check-type

check: check-fmt check-lint-python check-lint-rst check-type
.PHONY: check

test:
	python3 -m unittest tests
.PHONY: test

dist-check: build
	twine check dist/*
.PHONY: dist-check

dist-upload: check
	twine upload dist/*
.PHONY: dist-upload

dev-install: build
	pip3 uninstall -y dbt-metabase && pip3 install dist/dbt_metabase-*-py3-none-any.whl
.PHONY: dev-install

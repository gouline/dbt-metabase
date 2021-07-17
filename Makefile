build: clean
	python3 setup.py sdist bdist_wheel

clean:
	rm -rf build dist

requirements:
	pip3 install -r requirements.txt 
	pip3 install -r requirements-test.txt
.PHONY: requirements

lint:
	pylint dbtmetabase
.PHONY: lint

type:
	mypy dbtmetabase
.PHONY: type

test:
	python3 -m unittest tests
.PHONY: test

check: build
	twine check dist/*
.PHONY: check

upload: check
	twine upload dist/*
.PHONY: upload

dev-install: build
	pip3 uninstall -y dbt-metabase && pip3 install dist/dbt_metabase-*-py3-none-any.whl
.PHONY: dev-install

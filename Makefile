.PHONY: all build check clean dev-requirements

all: build

build: clean
	python3 setup.py sdist bdist_wheel

clean:
	rm -rf build dist

requirements:
	pip3 install -r requirements.txt 
	pip3 install -r requirements-test.txt

lint:
	pylint --disable=R,C dbtmetabase

type:
	mypy --ignore-missing-imports dbtmetabase

test:
	python3 -m unittest tests

check: build
	twine check dist/*

upload: check
	twine upload dist/*

dev-install: build
	pip3 uninstall -y dbt-metabase && pip3 install dist/dbt_metabase-*-py3-none-any.whl

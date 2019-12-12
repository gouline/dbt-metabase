.PHONY: all build check clean dev-requirements

all: build

build: clean
	python3 setup.py sdist bdist_wheel

check: build
	twine check dist/*

upload: check
	twine upload dist/*

clean:
	rm -rf build dist

dev-requirements:
	pip3 install -r dev-requirements.txt

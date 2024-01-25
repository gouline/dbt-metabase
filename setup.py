#!/usr/bin/env python3

import sys

from setuptools import find_packages, setup

if sys.version_info < (3, 8):
    raise ValueError("Requires Python 3.8+")


def requires_from_file(filename: str) -> list:
    with open(filename, "r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip()]


with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name="dbt-metabase",
    description="dbt + Metabase integration.",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Mike Gouline",
    url="https://github.com/gouline/dbt-metabase",
    license="MIT License",
    entry_points={
        "console_scripts": ["dbt-metabase = dbtmetabase.__main__:cli"],
    },
    packages=find_packages(exclude=["tests", "sandbox"]),
    test_suite="tests",
    install_requires=requires_from_file("requirements.txt"),
    tests_require=requires_from_file("requirements-test.txt"),
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

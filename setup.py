#!/usr/bin/env python3

import sys
from setuptools import setup, find_packages

if sys.version_info < (3, 6):
    raise ValueError("Requires Python 3.6+")


def requires_from_file(filename: str) -> list:
    with open(filename, "r") as f:
        return [x.strip() for x in f if x.strip()]


with open("README.rst", "r") as f:
    readme = f.read()

setup(
    name="dbt-metabase",
    use_scm_version=True,
    description="Model synchronization from dbt to Metabase.",
    long_description=readme,
    long_description_content_type="text/x-rst",
    author="Mike Gouline",
    author_email="hello@gouline.net",
    url="https://github.com/gouline/dbt-metabase",
    license="MIT License",
    scripts=["dbtmetabase/bin/dbt-metabase"],
    packages=find_packages(exclude=["tests"]),
    test_suite="tests",
    install_requires=requires_from_file("requirements.txt"),
    extras_require={
        "test": requires_from_file("requirements-test.txt"),
    },
    setup_requires=["setuptools_scm"],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

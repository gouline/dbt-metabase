#!/usr/bin/env python3

import sys
from setuptools import setup, find_packages

if sys.version_info < (3, 6):
    raise ValueError("Requires Python 3.6+")

from dbtmetabase import __version__

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

with open('test-requirements.txt', 'r') as f:
    test_requires = [x.strip() for x in f if x.strip()]

with open('README.rst', 'r') as f:
    readme = f.read()

setup(
    name='dbt-metabase',
    version=__version__,
    description="Model synchronization from dbt to Metabase.",
    long_description=readme,
    long_description_content_type='text/x-rst',
    author="Mike Gouline",
    author_email="hello@gouline.net",
    url='https://github.com/gouline/dbt-metabase',
    license='MIT License',
    packages=find_packages(exclude=['tests']),
    test_suite='tests',
    scripts=[
        'dbtmetabase/bin/dbt-metabase'
    ],
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'test': test_requires},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)

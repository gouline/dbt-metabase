dbt-metabase
############

.. image:: https://github.com/gouline/dbt-metabase/actions/workflows/master.yml/badge.svg
    :target: https://github.com/gouline/dbt-metabase/actions/workflows/master.yml
    :alt: GitHub Actions
.. image:: https://img.shields.io/pypi/v/dbt-metabase
    :target: https://pypi.org/project/dbt-metabase/
    :alt: PyPI
.. image:: https://pepy.tech/badge/dbt-metabase
    :target: https://pepy.tech/project/dbt-metabase
    :alt: Downloads
.. image:: https://black.readthedocs.io/en/stable/_static/license.svg
    :target: https://github.com/gouline/dbt-metabase/blob/master/LICENSE
    :alt: License: MIT
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black
    :alt: Code style: black

Model synchronization from `dbt`_ to `Metabase`_.

.. _`dbt`: https://www.getdbt.com/
.. _`Metabase`: https://www.metabase.com/

If dbt is your source of truth for database schemas and you use Metabase as
your analytics tool, dbt-metabase can propagate table relationships, model and
column descriptions and semantic types (e.g. currency, category, URL) to your
Metabase data model.

Requirements
============

Requires Python 3.6 or above.

Main features
=============

The main features provided by dbt-metabase are:

* Parsing your dbt project (either through the ``manifest.json`` or directly through the YAML files)
* Triggering a Metabase schema sync before propagating the metadata
* Propagating table descriptions to Metabase
* Propagating columns description to Metabase
* Propagating columns semantic types and visibility types to Metabase through the use of dbt meta fields
* Propagating table relationships represented as dbt ``relationships`` column tests

Usage
=====

You can install dbt-metabase from `PyPI`_:

.. _`PyPI`: https://pypi.org/project/dbt-metabase/

.. code-block:: shell

    pip install dbt-metabase

Basic Example
-------------

Let's start by defining a short sample ``schema.yml`` as below.

.. code-block:: yaml

    models:
      - name: stg_users
        description: User records.
        columns:
          - name: id
            description: Primary key.
            tests:
              - not_null
              - unique
          - name: email
            description: User's email address.
          - name: group_id
            description: Foreign key to user group.
            tests:
              - not_null
              - relationships:
                  to: ref('groups')
                  field: id

      - name: stg_groups
        description: User groups.
        columns:
          - name: id
            description: Primary key.
            tests:
              - not_null
              - unique
          - name: name
            description: Group name.

That's already enough to propagate the primary keys, foreign keys and
descriptions to Metabase by executing the below command.

.. code-block:: shell

    dbt-metabase export \
        --dbt_path . \
        --dbt_database business \
        --metabase_host metabase.example.com \
        --metabase_user user@example.com \
        --metabase_password Password123 \
        --metabase_database business \
        --schema public

Check your Metabase instance by going into Settings > Admin > Data Model, you
will notice that ``ID`` in ``STG_USERS`` is now marked as "Entity Key" and
``GROUP_ID`` is marked as "Foreign Key" pointing to ``ID`` in ``STG_GROUPS``.

Reading your dbt project
------------------------

There are two approaches provided by this library to read your dbt project:

1. Artifacts
^^^^^^^^^^^^

The recommended approach is to instruct dbt-metabase to read your ``manifest.json``, a
`dbt artifact`_ containing the full representation of your dbt project's resources. If
your dbt project uses multiple schemas, multiple databases or model aliases, you must use
this approach.

Note that you you have to run ``dbt compile --target prod`` or any of the other dbt commands
listed in the dbt documentation above to get a fresh copy of your ``manifest.json``. Remember
to run it against your production target.

When using the ``dbt-metabase`` CLI, you must provide a ``--dbt_manifest_path`` argument
pointing to your ``manifest.json`` file (usually in the ``target/`` folder of your dbt
project).

.. _`dbt artifact`: https://docs.getdbt.com/reference/artifacts/dbt-artifacts

2. Direct parsing
^^^^^^^^^^^^^^^^^

The second alternative is to provide the path to your dbt project root folder
using the argument ``--dbt_path``. dbt-metabase will then look for all .yml files
and parse your documentation and tests directly from there. It will not support
dbt projects with custom schemas.

Semantic Types
--------------

Now that we have primary and foreign keys, let's tell Metabase that ``email``
column contains email addresses.

Change the ``email`` column as follows:

.. code-block:: yaml

    - name: email
      description: User's email address.
      meta:
        metabase.semantic_type: type/Email

Once you run ``dbt-metabase export`` again, you will notice that ``EMAIL`` is
now marked as "Email".

Here is the list of semantic types (formerly known as special types) currently accepted by Metabase:

* ``type/PK``
* ``type/FK``
* ``type/AvatarURL``
* ``type/Category``
* ``type/City``
* ``type/Country``
* ``type/Currency``
* ``type/Description``
* ``type/Email``
* ``type/Enum``
* ``type/ImageURL``
* ``type/SerializedJSON``
* ``type/Latitude``
* ``type/Longitude``
* ``type/Number``
* ``type/State``
* ``type/URL``
* ``type/ZipCode``
* ``type/Quantity``
* ``type/Income``
* ``type/Discount``
* ``type/CreationTimestamp``
* ``type/CreationTime``
* ``type/CreationDate``
* ``type/CancelationTimestamp``
* ``type/CancelationTime``
* ``type/CancelationDate``
* ``type/DeletionTimestamp``
* ``type/DeletionTime``
* ``type/DeletionDate``
* ``type/Product``
* ``type/User``
* ``type/Source``
* ``type/Price``
* ``type/JoinTimestamp``
* ``type/JoinTime``
* ``type/JoinDate``
* ``type/Share``
* ``type/Owner``
* ``type/Company``
* ``type/Subscription``
* ``type/Score``
* ``type/Title``
* ``type/Comment``
* ``type/Cost``
* ``type/GrossMargin``
* ``type/Birthdate``

If you notice new ones, please submit a PR to update this readme.

Visibility Types
----------------

In addition to semantic types, you can optionally specify visibility for each
field. This affects whether or not they are displayed in the Metabase UI.

Here is how you would hide that same email:

.. code-block:: yaml

    - name: email
      description: User's email address.
      meta:
        metabase.semantic_type: type/Email
        metabase.visibility_type: sensitive

Here are the visibility types supported by Metabase:

* ``normal`` (default)
* ``details-only``
* ``sensitive``
* ``hidden`` (supported but not reflected in the UI)
* ``retired`` (supported but not reflected in the UI)

If you notice new ones, please submit a PR to update this readme.

Database Sync
-------------

By default, dbt-metabase will tell Metabase to synchronize database fields
and wait for the data model to contain all the tables and columns in your dbt
project.

You can control this behavior with two arguments:

* ``--metabase_sync_skip`` - boolean to optionally disable pre-synchronization
* ``--metabase_sync_timeout`` - number of seconds to wait and re-check data model before
  giving up

Programmatic Invocation
-----------------------

As you have already seen, you can invoke dbt-metabase from the command
line. But if you prefer to call it from your code, here's how to do it:

.. code-block:: python

    import dbtmetabase

    dbtmetabase.export(
      dbt_database=dbt_database,
      dbt_manifest_path=dbt_manifest_path,
      dbt_path=dbt_path,
      dbt_docs_url=dbt_docs,
      metabase_database=metabase_database,
      metabase_host=metabase_host,
      metabase_user=metabase_user,
      metabase_password=metabase_password,
      metabase_use_http=metabase_use_http,
      metabase_verify=metabase_verify,
      metabase_sync_skip=metabase_sync_skip,
      metabase_sync_timeout=metabase_sync_timeout,
      schema=schema,
      schema_excludes=schema_excludes,
      includes=includes,
      excludes=excludes,
      include_tags=include_tags,
    )

Code of Conduct
===============

All contributors are expected to follow the `PyPA Code of Conduct`_.

.. _`PyPA Code of Conduct`: https://www.pypa.io/en/latest/code-of-conduct/

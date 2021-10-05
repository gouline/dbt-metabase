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
.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
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
* Extracting dbt model exposures from Metabase and generating YAML files to be included and revisioned with your dbt deployment


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

    dbt-metabase models \
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

Exposure Extraction
-------------------

dbt-metabase also allows us to extract exposures from Metabase. The invocation is almost identical to
our models function with the addition of output name and location args. `dbt exposures`_ let us understand
how our dbt models are exposed in BI which closes the loop between ELT, modelling, and consumption.


.. _`dbt exposures`: https://docs.getdbt.com/docs/building-a-dbt-project/exposures


.. code-block:: shell

    dbt-metabase exposures \
        --dbt_manifest_path ./target/manifest.json \
        --dbt_database business \
        --metabase_host metabase.example.com \
        --metabase_user user@example.com \
        --metabase_password Password123 \
        --metabase_database business \
        --output_path ./models/ \
        --output_name metabase_exposures

Once execution completes, a look at the output ``metabase_exposures.yml`` will 
reveal all metabase exposures documented with the documentation, descriptions, creator
emails & names, links to exposures, and even native SQL propagated over from Metabase.

.. code-block:: yaml

    exposures:
      - name: Number_of_orders_over_time
        description: '
          ### Visualization: Line
      
          A line chart depicting how order volume changes over time
      
          #### Metadata
      
          Metabase Id: __8__
      
          Created On: __2021-07-21T08:01:38.016244Z__'
        type: analysis
        url: http://your.metabase.com/card/8
        maturity: medium
        owner:
          name: Indiana Jones
          email: user@example.com
        depends_on:
          - ref('orders')

Questions which are native queries will have the SQL propagated to a code block in the documentation's
description for full visibility. This YAML, like the rest of your dbt project can be committed to source
control to understand how exposures change over time. In a production environment, one can trigger 
``dbt docs generate`` after ``dbt-metabase exposures`` (or alternatively run the exposure extraction job
on a cadence every X days) in order to keep a dbt docs site fully synchronized with BI. This makes ``dbt docs`` a
useful utility for introspecting the data model from source -> consumption with zero extra/repeated human input.

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

Once you run ``dbt-metabase models`` again, you will notice that ``EMAIL`` is
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

Configuration
-------------

.. code-block:: shell

    dbt-metabase config

Using the above command, you can enter an interactive configuration session where you can cache default selections
for arguments. This creates a ``config.yml`` in ~/.dbt-metabase. This is particularly useful for arguments which are repeated on every invocation like metabase_user, metabase_host, 
metabase_password, dbt_manifest_path, etc. 

In addition, there are a few injected env vars that make deploying dbt-metabase in a CI/CD environment simpler without exposing 
secrets. Listed below are acceptable env vars which correspond to their CLI flags:

* ``DBT_DATABASE``
* ``DBT_PATH``
* ``DBT_MANIFEST_PATH``
* ``MB_USER``
* ``MB_PASS``
* ``MB_HOST``
* ``MB_DATABASE``

If any one of the above is present in the environment, the corresponding CLI flag is not needed unless overriding
the environment value. In the absence of a CLI flag, dbt-metabase will first look to the environment for any 
env vars to inject, then we will look to the config.yml for cached defaults.

A ``config.yml`` can be created or updated manually as well if needed. The only 
requirement is that it must be located in ~/.dbt-metabase. The layout is as follows:

.. code-block:: yaml

    config:
        dbt_database: reporting
        dbt_manifest_path: /home/user/dbt/target/manifest.json
        metabase_database: Reporting
        metabase_host: reporting.metabase.io
        metabase_user: user@source.co
        metabase_password: ...
        metabase_use_http: false
        metabase_sync: true
        metabase_sync_timeout: null
        dbt_schema_excludes:
          - development
          - testing
        dbt_excludes:
          - test_monday_io_site_diff

Programmatic Invocation
-----------------------

As you have already seen, you can invoke dbt-metabase from the command
line. But if you prefer to call it from your code, here's how to do it:

.. code-block:: python

    from dbtmetabase.models.interface import MetabaseInterface, DbtInterface

    # Instantiate dbt interface
    dbt = DbtInterface(
        path=dbt_path,
        manifest_path=dbt_manifest_path,
        database=dbt_database,
        schema=dbt_schema,
        schema_excludes=dbt_schema_excludes,
        includes=dbt_includes,
        excludes=dbt_excludes,
    )

    # Load models
    dbt_models, aliases = dbt.read_models(
        include_tags=dbt_include_tags,
        docs_url=dbt_docs_url,
    )

    # Instantiate Metabase interface
    metabase = MetabaseInterface(
        host=metabase_host,
        user=metabase_user,
        password=metabase_password,
        use_http=metabase_use_http,
        verify=metabase_verify,
        database=metabase_database,
        sync=metabase_sync,
        sync_timeout=metabase_sync_timeout,
    )

    # Load client
    metabase.prepare_metabase_client(dbt_models)

    # Propagate models to Metabase
    metabase.client.export_models(
        database=metabase.database,
        models=dbt_models,
        aliases=aliases,
    )

    # Parse exposures from Metabase into dbt schema yml
    metabase.client.extract_exposures(
        models=dbt_models,
        output_path=output_path,
        output_name=output_name,
        include_personal_collections=include_personal_collections,
        collection_excludes=collection_excludes,
    )


Code of Conduct
===============

All contributors are expected to follow the `PyPA Code of Conduct`_.

.. _`PyPA Code of Conduct`: https://www.pypa.io/en/latest/code-of-conduct/

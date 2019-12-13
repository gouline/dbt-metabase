dbt-metabase
############

Model synchronization from `dbt`_ to `Metabase`_.

.. _`dbt`: https://www.getdbt.com/
.. _`Metabase`: https://www.metabase.com/

If dbt is your source of truth for database schemas and you use Metabase as
your analytics tool, dbt-metabase can propagate table relationships, model and
column descriptions and special types (e.g. currency, category, URL) to your
Metabase data model.

Requirements
============

Requires Python 3.6 or above.

Usage
=====

You can install dbt-metabase from `PyPI`_:

.. _`PyPI`: https://pypi.org/project/dbt-metabase/

.. code-block:: shell

    pip install dbt-metabase

To install the dbt package in your project, add the following to your
``packages.yml``:

.. code-block:: yaml

    packages:
      - git: https://github.com/gouline/dbt-metabase.git
        revision: vX.Y.Z

Where ``vX.Y.Z`` corresponds to the `latest release`_. Now run ``dbt deps`` and
you're ready to go.

.. _`latest release`: https://github.com/gouline/dbt-metabase/releases/latest

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
        --mb_host metabase.example.com \
        --mb_user user@example.com \
        --mb_password Password123 \
        --database business \
        --schema public

Check your Metabase instance by going into Settings > Admin > Data Model, you
will notice that ``ID`` in ``STG_USERS`` is now marked as "Entity Key" and
``GROUP_ID`` is marked as "Foreign Key" pointing to ``ID`` in ``STG_GROUPS``.

Special Types
-------------

Now that we have primary and foreign keys, let's tell Metabase that ``email``
column contains email addresses.

Change the ``email`` column as follows:

.. code-block:: yaml

    - name: email
      description: User's email address.
      tests:
        - metabase.column:
            special_type: type/Email

Once you run ``dbt-metabase export`` again, you will notice that ``EMAIL`` is
now marked as "Email".

Here is the list of special types currently accepted by Metabase:

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

If you notice new ones, please submit a PR to update this readme and
``macros/tests.sql``.

Database Sync
-------------

By default, dbt-metabase will tell Metabase to synchronize database fields
and wait for the data model to contain all the tables and columns in your dbt
project.

You can control this behavior with two arguments:

* ``--sync`` - boolean to enable or disable pre-synchronization
* ``--sync_timeout`` - number of seconds to wait and re-check data model before
  giving up

Programmatic Invocation
-----------------------

As you have already seen, you can invoke dbt-metabase from the command
line. But if you prefer to call it from your code, here's how to do it:

.. code-block:: python

    import dbtmetabase

    dbtmetabase.export(dbt_path, mb_host, mb_user, mb_password, database, schema)

Code of Conduct
===============

All contributors are expected to follow the PyPA `Code of Conduct`_.

.. _`Code of Conduct`: https://www.pypa.io/en/latest/code-of-conduct/

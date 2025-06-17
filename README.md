# dbt-metabase

[![PyPI](https://img.shields.io/pypi/v/dbt-metabase)](https://pypi.org/project/dbt-metabase/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/gouline/dbt-metabase/blob/master/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

Integration between [dbt](https://www.getdbt.com/) and [Metabase](https://www.metabase.com/).

If dbt is your source of truth for database schemas and you use Metabase as your analytics tool, dbt-metabase can propagate table relationships, model and column descriptions and semantic types (e.g. currency, category, URL) to your Metabase data model, and extract questions and dashboards as exposures in your dbt project.

## Requirements

Requires Python 3.8 or above.

For development, you will need [uv](https://docs.astral.sh/uv/getting-started/installation/) installed.

## Usage

You can install dbt-metabase from [PyPI](https://pypi.org/project/dbt-metabase/):

```
pip install dbt-metabase
```

Sections below demonstrate basic usage examples, for all CLI options:

```
dbt-metabase --help
```

## Manifest

Before running dbt-metabase, you need a compiled `manifest.json` file to parse. These are part of the [dbt artifact](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) generated during compilation.

Once `dbt compile` finishes, `manifest.json` can be found in the `target/` directory of your dbt project.

See [dbt documentation](https://docs.getdbt.com/docs/running-a-dbt-project/run-your-dbt-projects) for more information.

## Metabase API

All commands require authentication against the [Metabase API](https://www.metabase.com/docs/latest/api-documentation) using one of these methods:

* API key (`--metabase-api-key`) 
  - Strongly **recommended** for automation, see [documentation](https://www.metabase.com/docs/latest/people-and-groups/api-keys) (Metabase 49 or later).
* Username and password (`--metabase-username` / `--metabase-password`)
  - Fallback for older versions of Metabase and smaller instances.

## Exporting Models

Let's start by defining a short sample `schema.yml` as below.

```yaml
models:
  - name: stg_users
    description: User records.
    columns:
      - name: id
        description: Primary key.
        data_tests:
          - not_null
          - unique

      - name: email
        description: User's email address.

      - name: group_id
        description: Foreign key to user group.
        data_tests:
          - not_null
          - relationships:
              to: ref('groups')
              field: id

  - name: stg_groups
    description: User groups.
    columns:
      - name: id
        description: Primary key.
        data_tests:
          - not_null
          - unique

      - name: name
        description: Group name.
```

This is already enough to propagate the primary keys, foreign keys and descriptions to Metabase:

```
dbt-metabase models \
    --manifest-path target/manifest.json \
    --metabase-url https://metabase.example.com \
    --metabase-api-key mb_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX= \
    --metabase-database business \
    --include-schemas public
```

Open Metabase and go to Settings > Admin Settings > Table Metadata, you will notice that `id` column in `stg_users` is now marked as "Entity Key" and `group_id` is a "Foreign Key" pointing to `id` in `stg_groups`.

Try running `dbt-metabase models --help` to see all the options available for fine tuning.

### Foreign Keys

Native [relationship tests](https://docs.getdbt.com/reference/resource-properties/data-tests#relationships) and [column-level constraints](https://docs.getdbt.com/reference/resource-properties/constraints#defining-constraints) are the recommended ways of defining foreign keys, however you can override them with `fk_target_table` and `fk_target_field` meta fields. If both are set for a column, meta fields take precedence.

```yaml
- name: country_id
  description: FK to User's country in the dim_countries table.
  config:
    meta:
      metabase.fk_target_table: analytics_dims.dim_countries
      metabase.fk_target_field: id
```

You can provide `fk_target_table` as `schema_name.table_name` or just `table_name` to use the current schema. If your model has an alias, provide that alias rather than the original name.

### Semantic Types

Now that we have foreign keys configured, let's tell Metabase that `email` column contains email addresses:

```yaml
- name: email
  description: User's email address.
  config:
    meta:
      metabase.semantic_type: type/Email
```

Once you run `dbt-metabase models` again, you will notice that `email` column is now marked as "Email".

Below are common semantic types (formerly known as _special types_) accepted by Metabase:

* `type/PK`
* `type/FK`
* `type/Number`
* `type/Currency`
* `type/Category`
* `type/Title`
* `type/Description`
* `type/City`
* `type/State`
* `type/ZipCode`
* `type/Country`
* `type/Latitude`
* `type/Longitude`
* `type/Email`
* `type/URL`
* `type/ImageURL`
* `type/SerializedJSON`
* `type/CreationTimestamp`

See [Metabase documentation](https://www.metabase.com/docs/latest/users-guide/field-types.html) for a more complete list.

### Visibility Types

You can optionally specify visibility for tables and columns, this controls whether they are displayed in Metabase.

Here is how you would hide that email column:

```yaml
- name: email
  description: User's email address.
  config:
    meta:
      metabase.semantic_type: type/Email
      metabase.visibility_type: sensitive
```

Below are the visibility types supported for columns:

* `normal` (default) - This field will be displayed normally in tables and charts.
* `details-only` - This field will only be displayed when viewing the details of a single record.
* `sensitive` - This field won't be visible or selectable in questions created with the GUI interfaces.

Tables support the following:

* No value for visible (default)
* `hidden`
* `technical`
* `cruft`

If you notice any changes to these, please submit a pull request with an update.

### Other Meta Fields

In addition to foreign keys, semantic types and visibility types, Metabase also accepts the following meta fields:

```yaml
- name: model_name
  config:
    meta:
      metabase.display_name: another_model_name
      metabase.visibility_type: normal
      metabase.points_of_interest: Relevant records.
      metabase.caveats: Sensitive information about users.
  columns:
    - name: column_name
      config:
        meta:
          metabase.display_name: another_column_name
          metabase.visibility_type: sensitive
          metabase.semantic_type: type/Number
          metabase.has_field_values: list
          metabase.coercion_strategy: keyword
          metabase.number_style: decimal
          metabase.decimals: 3
```

See [Metabase documentation](https://www.metabase.com/docs/latest/api) for details and accepted values.

### Synchronization

By default, dbt-metabase waits for tables and columns to be synchronized between your dbt project and Metabase database, otherwise the export fails when the sync timeout expires. 

If you have known discrepancies between dbt and Metabase and wish to proceed without synchronization, set the sync timeout to zero (e.g. `--sync-timeout 0`). This is discouraged, because you will still encounter errors if you have a table or column in your dbt project that is missing from Metabase and dbt-metabase attempts to export it.

## Exposure Extraction

dbt-metabase allows you to extract questions and dashboards from Metabase as [dbt exposures](https://docs.getdbt.com/docs/building-a-dbt-project/exposures) in your project:

```
dbt-metabase exposures \
    --manifest-path ./target/manifest.json \
    --metabase-url https://metabase.example.com \
    --metabase-api-key mb_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX= \
    --output-path models/ \
    --exclude-collections "temp*"
```

Once the execution completes, check your output path for exposures files containing descriptions, creator details and links for Metabase questions and dashboards:

```yaml
exposures:
  - name: number_of_orders_over_time
    description: '
      ### Visualization: Line

      A line chart depicting how order volume changes over time

      #### Metadata

      Metabase Id: __8__

      Created On: __2021-07-21T08:01:38.016244Z__'
    type: analysis
    url: https://metabase.example.com/card/8
    maturity: medium
    owner:
      name: Indiana Jones
      email: indiana@example.com
    depends_on:
      - ref('orders')
```

Native query questions will have SQL code blocks inside the descriptions, formatted to look nice in [dbt docs](https://docs.getdbt.com/docs/collaborate/documentation). These YAML files can be committed to source control to understand how exposures change over time.

Try running `dbt-metabase exposures --help` to see all the options available for fine tuning.

## Configuration

There are 3 levels of configuration in decreasing order of precedence:

* CLI arguments, e.g. `--manifest-path target/manifest.json`
* Environment variables, e.g. `MANIFEST_PATH=target/manifest.json`
* Configuration file, e.g. `manifest_path: target/manifest.json`

Try running `--help` for any command to see the full list of CLI arguments and environment variables.

A configuration file can be created in `~/.dbt-metabase/config.yml` for dbt-metabase to pick it up automatically or anywhere else by specifying `dbt-metabase --config-path path/to/config.yml` (must come **before** the command). Here is an example YAML file:

```yaml
config:
    manifest_path: target/manifest.json
    metabase_url: https://metabase.example.com
    metabase_api_key: mb_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=
    # Configuration specific to models command
    models:
      metabase_database: business
    # Configuration specific to exposures command
    exposures:
      output_path: models
```

Note that common configurations are in the outer block and command-specific ones are in separate blocks.

## Programmatic API

Alternatively, you can invoke dbt-metabase programmatically. Below is the equivalent of CLI examples:

```python
from dbtmetabase import DbtMetabase, Filter

# Initializing instance
c = DbtMetabase(
    manifest_path="target/manifest.json",
    metabase_url="https://metabase.example.com",
    metabase_api_key="mb_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=",
)

# Exporting models
c.export_models(
    metabase_database="business",
    schema_filter=Filter(include=["public"]),
)

# Extracting exposures
c.extract_exposures(
    output_path=".",
    collection_filter=Filter(exclude=["temp*"]),
)
```

See function header comments for information about other parameters.

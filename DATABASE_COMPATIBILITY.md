# Database Compatibility Guide

## Overview

This document explains the compatibility design between PostgreSQL and Databricks multi-catalog environments. The implementation uses a PostgreSQL-first approach with Databricks as a fallback enhancement.

## Design Principles

### PostgreSQL First

- PostgreSQL behavior is the default and reference implementation
- All logic works with traditional `schema.table` format
- No Databricks dependencies in core paths

### Databricks as Fallback Enhancement

- Multi-catalog features are fallback extensions
- Databricks-specific logic only activates when needed
- PostgreSQL paths remain unchanged

## Implementation Details

### Table Key Resolution

```python
# PostgreSQL (Default): schema.table
table_key = f"{schema_name}.{table_name}"  # "INVENTORY.SKUS"

# Databricks (Fallback): catalog.schema.table
table_key = f"{database_name}.{schema_name}.{table_name}"  # "CATALOG.INVENTORY.SKUS"
```

The system tries PostgreSQL format first, then falls back to Databricks format when needed.

### Foreign Key Resolution

```python
# Step 1: Try PostgreSQL format (works for both databases)
fk_target_field = ctx.get_field(
    table_key="inventory.skus",  # Standard format
    field_key="sku_id",
)

# Step 2: Databricks fallback (only if Step 1 fails)
if not fk_target_field and is_multi_catalog_context(table_key):
    catalog_part = table_key.split(".")[0]  # Extract catalog
    fk_target_field = ctx.get_field(
        table_key=f"{catalog_part}.inventory.skus",  # Multi-catalog format
        field_key="sku_id",
    )
```

## Development

### Postgres

```bash
# Standard testing - works without any Databricks dependencies
make test
make sandbox-up
make sandbox-models
make sandbox-exposures
```

### Databricks (Optional)

```bash
# Only runs when .env.databricks is configured
make sandbox-up TARGET=databricks
make sandbox-models TARGET=databricks
make sandbox-exposures TARGET=databricks
```

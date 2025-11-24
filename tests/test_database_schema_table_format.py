"""Tests for database.schema.table format support."""

from unittest.mock import Mock

from dbtmetabase._models import ModelsMixin
from dbtmetabase.manifest import Column, Group, Model


class TestDatabaseSchemaTableFormat:
    """Test database.schema.table format support in dbt-metabase."""

    def test_get_metabase_tables_with_multidatabase(self):
        """Test _get_metabase_tables handles multi-database format."""

        class TestModelsMixin(ModelsMixin):
            """Test implementation of ModelsMixin."""

            @property
            def manifest(self):
                return None

            @property
            def metabase(self):
                return self._metabase

        mixin = TestModelsMixin()

        mock_metabase = Mock()
        mock_metabase.get_database_metadata.return_value = {
            "tables": [
                {
                    "id": 1,
                    "name": "my_model",
                    "schema": "bronze",
                    "db": "my_database.bronze",  # Multi-database format
                    "fields": [{"id": 1, "name": "id"}, {"id": 2, "name": "name"}],
                },
                {
                    "id": 2,
                    "name": "other_model",
                    "schema": "silver",
                    "db": "my_database.silver",  # Multi-database format
                    "fields": [{"id": 3, "name": "id"}, {"id": 4, "name": "value"}],
                },
                {
                    "id": 3,
                    "name": "legacy_model",
                    "schema": "public",
                    "db": "legacy_db",  # Non multi-database format
                    "fields": [{"id": 5, "name": "id"}],
                },
            ]
        }

        mixin._metabase = mock_metabase

        # Test the method
        tables = mixin._get_metabase_tables("test_db_id")

        # Verify multi-database tables are keyed correctly
        assert "MY_DATABASE.BRONZE.MY_MODEL" in tables
        assert "MY_DATABASE.SILVER.OTHER_MODEL" in tables

        # Verify regular database format works (database prefix)
        assert "LEGACY_DB.PUBLIC.LEGACY_MODEL" in tables

        # Verify table structure is preserved
        bronze_table = tables["MY_DATABASE.BRONZE.MY_MODEL"]
        assert bronze_table["name"] == "my_model"
        assert bronze_table["schema"] == "BRONZE"
        assert "ID" in bronze_table["fields"]
        assert "NAME" in bronze_table["fields"]

    def test_model_matching_with_multidatabase(self):
        """Test that dbt models match correctly with multi-database table keys."""

        # Mock tables as they would be returned by _get_metabase_tables
        mock_tables = {
            "MY_DATABASE.BRONZE.MY_MODEL": {
                "id": 1,
                "name": "my_model",
                "schema": "BRONZE",
                "fields": {"ID": {"id": 1, "name": "id"}},
            },
            "LEGACY_DB.PUBLIC.LEGACY_MODEL": {
                "id": 2,
                "name": "legacy_model",
                "schema": "PUBLIC",
                "fields": {"ID": {"id": 2, "name": "id"}},
            },
        }

        # Test dbt models
        models = [
            Model(
                database="my_database",
                schema="bronze",
                group=Group.nodes,
                name="my_model",
                alias="my_model",
                columns=[Column(name="id")],
            ),
            Model(
                database="legacy_db",  # Regular database
                schema="public",
                group=Group.nodes,
                name="legacy_model",
                alias="legacy_model",
                columns=[Column(name="id")],
            ),
        ]

        # Test matching logic (simulating the sync loop logic)
        for model in models:
            schema_name = model.schema.upper()
            model_name = model.alias.upper()
            database_name = model.database.upper() if model.database else ""

            # Try multi-database format first
            table_key = (
                f"{database_name}.{schema_name}.{model_name}"
                if database_name
                else f"{schema_name}.{model_name}"
            )
            table = mock_tables.get(table_key)

            # Fallback to schema.table format if multi-database format not found
            if not table and database_name:
                table_key = f"{schema_name}.{model_name}"
                table = mock_tables.get(table_key)

            # Verify matching works
            assert table is not None, f"Model {model.name} should match a table"

            if model.database:
                # Multi-database model should match with database prefix
                expected_key = f"{database_name}.{schema_name}.{model_name}"
                assert table_key == expected_key
            else:
                # Legacy model should match without database prefix
                expected_key = f"{schema_name}.{model_name}"
                assert table_key == expected_key

    def test_foreign_key_resolution_with_multidatabase(self):
        """Test foreign key resolution works with multi-database table references."""

        from dbtmetabase._models import _Context

        ctx = _Context()
        ctx.tables = {
            "MY_DATABASE.BRONZE.USERS": {"fields": {"ID": {"id": 1, "name": "id"}}},
            "MY_DATABASE.SILVER.ORDERS": {
                "fields": {"USER_ID": {"id": 2, "name": "user_id"}}
            },
        }

        field = ctx.get_field("MY_DATABASE.BRONZE.USERS", "ID")
        assert field["id"] == 1

        field = ctx.get_field("MY_DATABASE.SILVER.ORDERS", "USER_ID")
        assert field["id"] == 2

import logging
import yaml
import requests
import json
import time
from typing import Any

class MetabaseClient:
    """Metabase API client.
    """

    _SYNC_PERIOD_SECS = 5

    def __init__(self, host: str, user: str, password: str, https = True):
        """Constructor.
        
        Arguments:
            host {str} -- Metabase hostname.
            user {str} -- Metabase username.
            password {str} -- Metabase password.
        
        Keyword Arguments:
            https {bool} -- Use HTTPS instead of HTTP. (default: {True})
        """

        self.host = host
        self.protocol = "https" if https else "http"
        self.session_id = self.get_session_id(user, password)
        logging.info("Session established successfully")
    
    def get_session_id(self, user: str, password: str) -> str:
        """Obtains new session ID from API.
        
        Arguments:
            user {str} -- Metabase username.
            password {str} -- Metabase password.
        
        Returns:
            str -- Session ID.
        """

        return self.api('post', '/api/session', authenticated=False, json={
            'username': user,
            'password': password
        })['id']
    
    def sync_and_wait(self, database: str, schema: str, models: list, timeout = 30) -> bool:
        """Synchronize with the database and wait for schema compatibility.
        
        Arguments:
            database {str} -- Metabase database name.
            schema {str} -- Metabase schema name.
            models {list} -- List of dbt models read from project.
        
        Keyword Arguments:
            timeout {int} -- Timeout before giving up in seconds. (default: {30})
        
        Returns:
            bool -- True if schema compatible with models, false if still incompatible.
        """

        if timeout < self._SYNC_PERIOD_SECS:
            logging.critical("Timeout provided %d secs, must be at least %d", timeout, self._SYNC_PERIOD_SECS)
            return

        database_id = self.find_database_id(database)
        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return
        
        self.api('post', f'/api/database/{database_id}/sync_schema')

        deadline = int(time.time()) + timeout
        sync_successful = False
        while True:
            sync_successful = self.models_compatible(database_id, schema, models)
            time_after_wait = int(time.time()) + self._SYNC_PERIOD_SECS
            if not sync_successful and time_after_wait <= deadline:
                time.sleep(self._SYNC_PERIOD_SECS)
            else:
                break
        return sync_successful

    def models_compatible(self, database_id: str, schema: str, models: list) -> bool:
        """Checks if models compatible with the Metabase database schema.
        
        Arguments:
            database_id {str} -- Metabase database ID.
            schema {str} -- Metabase schema name.
            models {list} -- List of dbt models read from project.
        
        Returns:
            bool -- True if schema compatible with models, false otherwise.
        """

        _, field_lookup = self.build_metadata_lookups(database_id, schema)

        are_models_compatible = True
        for model in models:
            model_name = model['name'].upper()
            if model_name not in field_lookup:
                logging.warn("Model %s not found", model_name)
                are_models_compatible = False
            else:
                table_lookup = field_lookup[model_name]
                for column in model.get('columns', []):
                    column_name = column['name'].upper()
                    if column_name not in table_lookup:
                        logging.warn("Column %s not found in model %s", column_name, model_name)
                        are_models_compatible = False
        
        return are_models_compatible

    def export_models(self, database: str, schema: str, models: list):
        """Exports dbt models to Metabase database schema.
        
        Arguments:
            database {str} -- Metabase database name.
            schema {str} -- Metabase schema name.
            models {list} -- List of dbt models read from project.
        """

        database_id = self.find_database_id(database)
        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return
        
        table_lookup, field_lookup = self.build_metadata_lookups(database_id, schema)

        for model in models:
            self.export_model(model, table_lookup, field_lookup)
    
    def export_model(self, model: dict, table_lookup: dict, field_lookup: dict):
        """Exports one dbt model to Metabase database schema.
        
        Arguments:
            model {dict} -- One dbt model read from project.
            table_lookup {dict} -- Dictionary of Metabase tables indexed by name.
            field_lookup {dict} -- Dictionary of Metabase fields indexed by name, indexed by table name.
        """

        model_name = model['name'].upper()

        api_table = table_lookup.get(model_name)
        if not api_table:
            logging.error('Table %s does not exist in Metabase', model_name)
            return

        table_id = api_table['id']
        if api_table['description'] != model['description']:
            # Update with new values
            self.api('put', f'/api/table/{table_id}', json={
                'description': model['description']
            })
            logging.info("Updated table %s successfully", model_name)
        else:
            logging.info("Table %s is up-to-date", model_name)

        for column in model.get('columns', []):
            self.export_column(model_name, column, field_lookup)
    
    def export_column(self, model_name: str, column: dict, field_lookup: dict):
        """Exports one dbt column to Metabase database schema.
        
        Arguments:
            model_name {str} -- One dbt model name read from project.
            column {dict} -- One dbt column read from project.
            field_lookup {dict} -- Dictionary of Metabase fields indexed by name, indexed by table name.
        """

        column_name = column['name'].upper()

        field = field_lookup.get(model_name, {}).get(column_name)
        if not field:
            logging.error('Field %s.%s does not exist in Metabase', model_name, column_name)
            return
        
        field_id = field['id']
        fk_target_field_id = None
        if column.get('special_type') == 'type/FK':
            target_table = column['fk_target_table']
            target_field = column['fk_target_field']
            fk_target_field_id = field_lookup.get(target_table, {}) \
                .get(target_field, {}) \
                .get('id')
            
            if fk_target_field_id:
                self.api('put', f'/api/field/{fk_target_field_id}', json={
                    'special_type': 'type/PK'
                })
            else:
                logging.error("Unable to find foreign key target %s.%s", target_table, target_field)
        
        # Nones are not accepted, default to normal
        if not column.get('visibility_type'):
            column['visibility_type'] = 'normal'

        api_field = self.api('get', f'/api/field/{field_id}')

        if api_field['description'] != column.get('description') or \
                api_field['special_type'] != column.get('special_type') or \
                api_field['visibility_type'] != column.get('visibility_type') or \
                api_field['fk_target_field_id'] != fk_target_field_id:
            # Update with new values
            self.api('put', f'/api/field/{field_id}', json={
                'description': column.get('description'),
                'special_type': column.get('special_type'),
                'visibility_type': column.get('visibility_type'),
                'fk_target_field_id': fk_target_field_id
            })
            logging.info("Updated field %s.%s successfully", model_name, column_name)
        else:
            logging.info("Field %s.%s is up-to-date", model_name, column_name)
    
    def find_database_id(self, name: str) -> str:
        """Finds Metabase database ID by name.
        
        Arguments:
            name {str} -- Metabase database name.
        
        Returns:
            str -- Metabase database ID.
        """

        for database in self.api('get', '/api/database'):
            if database['name'].upper() == name.upper():
                return database['id']
        return None
    
    def build_metadata_lookups(self, database_id: str, schema: str) -> (dict, dict):
        """Builds table and field lookups.
        
        Arguments:
            database_id {str} -- Metabase database ID.
            schema {str} -- Metabase schema name.
        
        Returns:
            dict -- Dictionary of tables indexed by name.
            dict -- Dictionary of fields indexed by name, indexed by table name.
        """

        table_lookup = {}
        field_lookup = {}

        metadata = self.api(
            'get',
            f'/api/database/{database_id}/metadata',
            params=dict(include_hidden=True)
        )
        for table in metadata.get('tables', []):
            table_schema = 'public' if table['schema'] is None else table['schema']
            if table_schema.upper() != schema.upper():
                continue

            table_name = table['name'].upper()
            table_lookup[table_name] = table

            table_field_lookup = {}

            for field in table.get('fields', []):
                field_name = field['name'].upper()
                table_field_lookup[field_name] = field

            field_lookup[table_name] = table_field_lookup
        
        return table_lookup, field_lookup

    def api(self, method: str, path: str, authenticated = True, critical = True, **kwargs) -> Any:
        """Unified way of calling Metabase API.
        
        Arguments:
            method {str} -- HTTP verb, e.g. get, post, put.
            path {str} -- Relative path of endpoint, e.g. /api/database.
        
        Keyword Arguments:
            authenticated {bool} -- Includes session ID when true. (default: {True})
            critical {bool} -- Raise on any HTTP errors. (default: {True})
        
        Returns:
            Any -- JSON payload of the endpoint.
        """

        headers = {}
        if 'headers' not in kwargs:
            kwargs['headers'] = headers
        else:
            headers = kwargs['headers'].copy()
        
        if authenticated:
            headers['X-Metabase-Session'] = self.session_id

        response = requests.request(method, f"{self.protocol}://{self.host}{path}", **kwargs)
        if critical:
            response.raise_for_status()
        elif not response.ok:
            return False
        return json.loads(response.text)

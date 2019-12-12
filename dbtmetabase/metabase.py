import logging
import yaml
import requests
import json
import time
from typing import Any

class MetabaseClient:

    _SYNC_PERIOD_SECS = 5

    def __init__(self, host: str, user: str, password: str):
        self.host = host
        self.session_id = self.get_session_id(user, password)
        logging.info("Session established successfully")
    
    def get_session_id(self, user: str, password: str) -> str:
        return self.api('post', '/api/session', authenticated=False, json={
            'username': user,
            'password': password
        })['id']
    
    def sync_and_wait(self, database: str, schema: str, models: list, timeout_secs = 30) -> bool:
        if timeout_secs < self._SYNC_PERIOD_SECS:
            logging.critical("Timeout provided %d secs, must be at least %d", timeout_secs, self._SYNC_PERIOD_SECS)
            return

        database_id = self.find_database_id(database)
        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return
        
        self.api('post', f'/api/database/{database_id}/sync')

        sync_successful = False
        while True:
            sync_successful = self.models_compatible(database_id, schema, models)
            if not sync_successful: # TODO and timeout budget not reached
                time.sleep(self._SYNC_PERIOD_SECS)
            else:
                break
        return sync_successful

    def models_compatible(self, database_id: str, schema: str, models: list) -> bool:
        field_lookup = self.build_field_lookup(database_id, schema)

        for model in models:
            model_name = model['name'].upper()
            if model_name not in field_lookup:
                return False

            table_lookup = field_lookup[model_name]
            for column in model.get('columns', []):
                column_name = column['name'].upper()
                if column_name not in table_lookup:
                    return False
        
        return True

    def export_models(self, database: str, schema: str, models: list):
        database_id = self.find_database_id(database)
        if not database_id:
            logging.critical("Cannot find database by name %s", database)
            return
        
        table_lookup = self.build_table_lookup(database_id, schema)
        field_lookup = self.build_field_lookup(database_id, schema)

        for model in models:
            self.export_model(model, table_lookup, field_lookup)
    
    def export_model(self, model: dict, table_lookup: dict, field_lookup: dict):
        model_name = model['name'].upper()

        api_table = table_lookup.get(model_name)
        if not api_table:
            logging.error('Table %s does not exist in Metabase', model_name)
            return

        table_id = api_table['id']
        if api_table['description'] != model['description']:
            api_table['description'] = model['description']

            self.api('put', f'/api/table/{table_id}', json=api_table)
            logging.info("Updated table %s successfully", model_name)
        else:
            logging.info("Table %s is up-to-date", model_name)

        for column in model.get('columns', []):
            self.export_column(model_name, column, field_lookup)
    
    def export_column(self, model_name: str, column: dict, field_lookup: dict): 
        column_name = column['name'].upper()

        field = field_lookup.get(model_name, {}).get(column_name)
        if not field:
            logging.error('Field %s.%s does not exist in Metabase', model_name, column_name)
            return
        
        field_id = field['id']
        fk_target_field_id = None
        if column.get('special_type') == 'type/FK':
            fk_target_field_id = field_lookup.get(column['fk_target_table'], {}) \
                .get(column['fk_target_column'], {}) \
                .get('id')

        api_field = self.api('get', f'/api/field/{field_id}')

        if api_field['description'] != column.get('description') or \
                api_field['special_type'] != column.get('special_type') or \
                api_field['fk_target_field_id'] != fk_target_field_id:
            api_field['description'] = column.get('description')
            api_field['special_type'] = column.get('special_type')
            api_field['fk_target_field_id'] = fk_target_field_id

            self.api('put', f'/api/field/{field_id}', json=api_field)
            logging.info("Updated field %s.%s successfully", model_name, column_name)
        else:
            logging.info("Field %s.%s is up-to-date", model_name, column_name)
    
    def find_database_id(self, name: str) -> str:
        for database in self.api('get', '/api/database'):
            if database['name'].upper() == name.upper():
                return database['id']
        return None
    
    def build_table_lookup(self, database_id: str, schema: str) -> dict:
        lookup = {}

        for table in self.api('get', f'/api/table'):
            if table['db_id'] != database_id or table['schema'].upper() != schema.upper():
                continue

            table_name = table['name'].upper()
            lookup[table_name] = table
        
        return lookup

    def build_field_lookup(self, database_id: str, schema: str) -> dict:
        lookup = {}

        for field in self.api('get', f'/api/database/{database_id}/fields'):
            if field['schema'].upper() != schema.upper():
                continue
        
            table_name = field['table_name'].upper()
            table_lookup = {}
            if table_name in lookup:
                table_lookup = lookup[table_name]
            else:
                lookup[table_name] = table_lookup

            field_name = field['name'].upper()
            table_lookup[field_name] = field
        
        return lookup

    def api(self, method: str, path: str, authenticated = True, critical = True, **kwargs) -> Any:
        headers = {}
        if 'headers' not in kwargs:
            kwargs['headers'] = headers
        else:
            headers = kwargs['headers'].copy()
        
        if authenticated:
            headers['X-Metabase-Session'] = self.session_id

        response = requests.request(method, f"https://{self.host}{path}", **kwargs)
        if critical:
            response.raise_for_status()
        elif not response.ok:
            return False
        return json.loads(response.text)

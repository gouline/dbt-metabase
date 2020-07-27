import logging
import yaml
import re
from pathlib import Path

# Allowed metabase.* fields
_META_FIELDS = [
    'special_type', 
    'visibility_type'
]

class DbtReader:
    """Reader for dbt project configuration.
    """

    def __init__(self, project_path: str):
        """Constructor.
        
        Arguments:
            project_path {str} -- Path to dbt project root.
        """

        self.project_path = project_path
    
    def read_models(self, includes = [], excludes = []) -> list:
        """Reads dbt models in Metabase-friendly format.
        
        Keyword Arguments:
            includes {list} -- Model names to limit processing to. (default: {[]})
            excludes {list} -- Model names to exclude. (default: {[]})

        Returns:
            list -- List of dbt models in Metabase-friendly format.
        """

        mb_models = []

        for path in (Path(self.project_path) / 'models').rglob('*.yml'):
            with open(path, 'r') as stream:
                schema = yaml.safe_load(stream)
                for model in schema.get('models', []):
                    name = model['name']
                    if (not includes or name in includes) and (name not in excludes):
                        mb_models.append(self.read_model(model))
        
        return mb_models
    
    def read_model(self, model: dict) -> dict:
        """Reads one dbt model in Metabase-friendly format.
        
        Arguments:
            model {dict} -- One dbt model to read.
        
        Returns:
            dict -- One dbt model in Metabase-friendly format.
        """

        mb_columns = []

        for column in model.get('columns', []):
            mb_columns.append(self.read_column(column))
            
        return {
            'name': model['name'].upper(),
            'description': model.get('description'),
            'columns': mb_columns
        }
    
    def read_column(self, column: dict) -> dict:
        """Reads one dbt column in Metabase-friendly format.
        
        Arguments:
            column {dict} -- One dbt column to read.
        
        Returns:
            dict -- One dbt column in Metabase-friendly format.
        """

        mb_column = {
            'name': column.get('name', '').upper(),
            'description': column.get('description')
        }
        
        for test in column.get('tests', []):
            if isinstance(test, dict):
                if 'relationships' in test:
                    relationships = test['relationships']
                    mb_column['special_type'] = 'type/FK'
                    mb_column['fk_target_table'] = self.parse_ref(relationships['to']).upper()
                    mb_column['fk_target_field'] = relationships['field'].upper()
        
        if 'meta' in column:
            meta = column.get('meta')
            for field in _META_FIELDS:
                if f'metabase.{field}' in meta:
                    mb_column[field] = meta[f'metabase.{field}']

        return mb_column

    @staticmethod
    def parse_ref(text: str) -> str:
        """Parses dbt ref() statement.
        
        Arguments:
            text {str} -- Full statement in dbt YAML.
        
        Returns:
            str -- Name of the reference.
        """

        matches = re.findall(r"ref\(['\"]([\w\_\-\ ]+)['\"]\)", text)
        if matches:
            return matches[0]
        return text

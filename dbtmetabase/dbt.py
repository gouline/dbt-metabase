import yaml
import re
from pathlib import Path

class DbtReader:
    """Reader for dbt project configuration.
    """

    def __init__(self, project_path: str):
        """Constructor.
        
        Arguments:
            project_path {str} -- Path to dbt project root.
        """

        self.project_path = project_path
    
    def read_models(self) -> list:
        """Reads dbt models in Metabase-friendly format.
        
        Returns:
            list -- List of dbt models in Metabase-friendly format.
        """

        mb_models = []

        for path in (Path(self.project_path) / 'models').rglob('*.yml'):
            with open(path, 'r') as stream:
                schema = yaml.safe_load(stream)
                for model in schema.get('models', []):
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
                    mb_column['fk_target_column'] = relationships['field'].upper()
                elif 'metabase.column' in test:
                    metabase = test['metabase.column']
                    if mb_column.get('special_type') != 'type/FK':
                        mb_column['special_type'] = metabase.get('special_type')
        
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

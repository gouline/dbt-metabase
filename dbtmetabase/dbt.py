import yaml
import re

class DbtReader:

    def __init__(self, project_path: str):
        self.project_path = project_path
    
    def read_models(self) -> list:
        mb_models = []

        for path in (self.project_path / 'models').rglob('*.yml'):
            with open(path, 'r') as stream:
                schema = yaml.safe_load(stream)
                for model in schema.get('models', []):
                    mb_models.append(self.read_model(model))
        
        return mb_models
    
    def read_model(self, model: dict) -> dict:
        mb_columns = []

        for column in model.get('columns', []):
            mb_columns.append(self.read_column(column))
            
        return {
            'name': model['name'].upper(),
            'description': model.get('description'),
            'columns': mb_columns
        }
    
    def read_column(self, column: dict) -> dict:
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
                elif 'metabase' in test:
                    metabase = test['metabase']
                    if mb_column.get('special_type') != 'type/FK':
                        mb_column['special_type'] = metabase.get('special_type')
        
        return mb_column

    @staticmethod
    def parse_ref(text: str) -> str:
        matches = re.findall(r"ref\(['\"]([\w\_\-\ ]+)['\"]\)", text)
        if matches:
            return matches[0]
        return text

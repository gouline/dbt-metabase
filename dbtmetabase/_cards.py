import dataclasses as dc
import logging
import time
from abc import ABCMeta, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence, Tuple, Dict, List

from .errors import MetabaseStateError
from .format import Filter, NullValue, safe_name
from .manifest import DEFAULT_SCHEMA, Column, Group, Manifest, Model
from .metabase import Metabase
from ._lockfile import LockFile

_logger = logging.getLogger(__name__)


def get_display_name(name: str):
    converted_name = name.replace("_", " ").title()
    return converted_name


def generate_template_tags_and_params(
        filters: dict) -> Tuple[Dict[str, Dict], List[Dict]]:
    """
        Generate template tags and parameters from filters.

        Args:
        filters: Dictionary mapping filter keys to their attributes.

        Returns:
        A tuple containing a dictionary of template tags and a list of parameters.
        """
    template_tags = {}
    params = []

    for key, fltr in filters.items():
        dim = ['field', fltr['col_id'], {'base-type': fltr['col_base_type']}]
        template_tag = {
            'type': 'dimension',
            'name': key,
            'id': fltr['uuid'],
            'default': fltr.get('default', None),
            'dimension': dim,
            'widget-type': fltr['widget-type'],
            'display-name': get_display_name(key)
        }
        template_tags[key] = template_tag

        param = {
            'id': fltr['uuid'],
            'type': fltr['widget-type'],
            'target': ['dimension', ['template-tag', key]],
            'slug': key,
            'name': get_display_name(key)
        }
        params.append(param)

    return template_tags, params


class CardsCreator(metaclass=ABCMeta):

    @property
    @abstractmethod
    def lock_file(self) -> LockFile:
        pass

    @property
    @abstractmethod
    def manifest(self) -> Manifest:
        pass

    @property
    @abstractmethod
    def metabase(self) -> Metabase:
        pass

    def update_cards(self):
        models = self.manifest.read_models()
        package = self.__update_lock_file(models)
        filters = self.__read_filters()
        models_dir = self.lock_file.get_model_dir(package)
        queries = self.__reed_queries(filters, models_dir)
        self.__update_cards(queries)

    def __update_lock_file(self, models: Sequence[Model]) -> str:
        packages = set()
        for model in models:
            packages.add(model.package_name)
            for col in model.columns:
                filter = col.filter
                if len(filter) > 0:
                    self.lock_file.update_filter(col.name, model.name,
                                                 **filter)
        if len(packages) > 1:
            raise ValueError('find multiple packeges')
        return packages.pop()

    def __read_filters(self) -> dict:
        filters = self.lock_file.read_filters()
        tables = self.metabase.get_tables()
        # Iterate through each filter and update it with additional metadata
        for filter in filters.values():
            # Find the table that matches the model_name of the current filter
            table = next(t for t in tables
                         if t['name'] == filter['model_name'])
            filter['db_id'] = table['db_id']
            columns = self.metabase.get_columns(table['id'])
            column = next(c for c in columns
                          if c['name'] == filter['col_name'])

            filter['col_id'] = column['id']
            filter['col_eff_type'] = column['effective_type']
            filter['col_base_type'] = column['base_type']
        return filters

    def __reed_queries(self, filters: dict, models_dir) -> dict:
        queries = {}
        raw_queries = self.lock_file.find_queries(models_dir)
        for key, raw_query in raw_queries.items():
            query = {}
            query['raw_query'] = raw_query
            query['query'], query[
                'filter_names'] = self.lock_file.replace_filters(raw_query)
            query['filters'] = {
                name: filters[name]
                for name in query['filter_names']
            }
            queries[key] = query
        return queries

    def __update_card(self,
                      name,
                      query,
                      filters,
                      card_id: Optional[int] = None) -> Mapping:
        db = set(filter['db_id'] for filter in filters.values())

        if len(db) > 1:
            raise ValueError('Multiple databases detected')
        db = db.pop()
        tags, params = generate_template_tags_and_params(filters)
        dataset_query = {
            'database': db,
            'type': 'native',
            'native': {
                'template-tags': tags,
                'query': query
            }
        }
        if card_id:
            _logger.debug(f'updating exist card {card_id} name {name}')
            data = {
                'name': name,
                'type': 'question',
                'dataset_query': dataset_query,
                'parameters': params,
                'archived': False
            }
            return self.metabase.update_card(card_id, data)
        else:
            _logger.debug(f'creating new card name {name}')
            data = {
                'name': name,
                'cache_ttl': None,
                'dataset': False,
                'type': 'question',
                'dataset_query': dataset_query,
                'display': 'table',
                'description': None,
                'visualization_settings': {},
                'parameters': params,
                'parameter_mappings': [],
                'archived': False,
                'enable_embedding': False,
                'embedding_params': None,
                'collection_id': None,
                'collection_position': None,
                'collection_preview': True,
                'result_metadata': None
            }
            return self.metabase.create_card(data)

    def __update_cards(self, queries):
        cards = self.metabase.all_cards()
        for name, query_data in queries.items():
            query_cards = [card for card in cards if card['name'] == name]
            if len(query_cards) == 0:
                card_id = None
            elif len(query_cards) > 1:
                raise ValueError(f'find multiple cards with name {name}')
            else:
                card_id = query_cards[0]['id']
            self.__update_card(name, query_data['query'],
                               query_data['filters'], card_id)

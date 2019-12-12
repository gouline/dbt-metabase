import logging

from .dbt import DbtReader
from .metabase import MetabaseClient

__version__ = '0.1.0'

def export(dbt_path: str, 
        mb_host: str, mb_user: str, mb_password: str, 
        database: str, schema: str,
        sync = True, sync_timeout_secs = 30):
    """Exports models from dbt to Metabase.
    
    Arguments:
        dbt_path {str} -- Path to dbt project.
        mb_host {str} -- Metabase hostname.
        mb_user {str} -- Metabase username.
        mb_password {str} -- Metabase password.
        database {str} -- Target database name.
        schema {str} -- Target schema name.
    
    Keyword Arguments:
        sync {bool} -- Synchronize Metabase database before export. (default: {True})
        sync_timeout_secs {int} -- Synchronization timeout in seconds. (default: {30})
    """

    mbc = MetabaseClient(mb_host, mb_user, mb_password)
    models = DbtReader(dbt_path).read_models()

    if sync:
        if not mbc.sync_and_wait(database, schema, models, sync_timeout_secs):
            logging.critical("Sync timeout reached, models still not compatible")
            return
    
    mbc.export_models(database, schema, models)

def main(args: list = None):
    import argparse
    
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # TODO: argparse here

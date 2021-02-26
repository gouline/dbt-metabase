import logging

from .dbt import DbtReader
from .metabase import MetabaseClient

__version__ = '0.5.2'

def export(dbt_path: str, 
        mb_host: str, mb_user: str, mb_password: str,
        database: str, schema: str,
        mb_https = True, sync = True, sync_timeout = 30, 
        includes = [], excludes = []):
    """Exports models from dbt to Metabase.
    
    Arguments:
        dbt_path {str} -- Path to dbt project.
        mb_host {str} -- Metabase hostname.
        mb_user {str} -- Metabase username.
        mb_password {str} -- Metabase password.
        database {str} -- Target database name.
        schema {str} -- Target schema name.
    
    Keyword Arguments:
        mb_https {bool} -- Use HTTPS to connect to Metabase instead of HTTP. (default: {True})
        sync {bool} -- Synchronize Metabase database before export. (default: {True})
        sync_timeout {int} -- Synchronization timeout in seconds. (default: {30})
        includes {list} -- Model names to limit processing to. (default: {[]})
        excludes {list} -- Model names to exclude. (default: {[]})
    """

    mbc = MetabaseClient(mb_host, mb_user, mb_password, mb_https)
    models = DbtReader(dbt_path).read_models(
        includes=includes, 
        excludes=excludes
    )

    if sync:
        if not mbc.sync_and_wait(database, schema, models, sync_timeout):
            logging.critical("Sync timeout reached, models still not compatible")
            return
    
    mbc.export_models(database, schema, models)

def main(args: list = None):
    import argparse
    
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='Model synchronization from dbt to Metabase.'
    )
    parser.add_argument('command', choices=['export'], help="command to execute")
    parser.add_argument('--dbt_path', metavar='PATH', required=True, help="path to dbt project")
    parser.add_argument('--mb_host', metavar='HOST', required=True, help="Metabase hostname")
    parser.add_argument('--mb_user', metavar='USER', required=True, help="Metabase username")
    parser.add_argument('--mb_password', metavar='PASS', required=True, help="Metabase password")
    parser.add_argument('--mb_https', metavar='HTTPS', type=bool, default=True, help="use HTTPS to connect to Metabase instead of HTTP")
    parser.add_argument('--database', metavar='DB', required=True, help="target database name")
    parser.add_argument('--schema', metavar='SCHEMA', required=True, help="target schema name")
    parser.add_argument('--sync', metavar='ENABLE', type=bool, default=True, help="synchronize Metabase database before export")
    parser.add_argument('--sync_timeout', metavar='SECS', type=int, default=30, help="synchronization timeout (in secs)")
    parser.add_argument('--includes', metavar='MODELS', nargs='*', default=[], help="model names to limit processing to")
    parser.add_argument('--excludes', metavar='MODELS', nargs='*', default=[], help="model names to exclude")
    parsed = parser.parse_args(args=args)

    if parsed.command == 'export':
        export(
            dbt_path=parsed.dbt_path,
            mb_host=parsed.mb_host,
            mb_user=parsed.mb_user,
            mb_password=parsed.mb_password,
            mb_https=parsed.mb_https,
            database=parsed.database,
            schema=parsed.schema,
            sync=parsed.sync,
            sync_timeout=parsed.sync_timeout,
            includes=parsed.includes,
            excludes=parsed.excludes
        )

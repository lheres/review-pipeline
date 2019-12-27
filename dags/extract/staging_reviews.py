import json
import psycopg2
import ast
from psycopg2 import sql
from airflow.hooks.postgres_hook import PostgresHook
from .utils.download_from_s3 import file_stream, download_stream
from .utils import string_iterator

# see if airflow PG Hook connection is available using connection from environment variable AIRFLOW_CONN_POSTGRES_DW
try:
    warehouse = PostgresHook(postgres_conn_id='postgres_dw')
    connection = warehouse.get_conn()
# else try to connect to local database if it exists
except Exception as exc:
    print("Could not connect to warehouse: ", str(exc))
    connection = psycopg2.connect('dbname=de user=de password=takeitaway host=localhost port=5430')

connection.autocommit = True

def create_staging_table(schema, table, iterdata, drop_table=True):
    with connection.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema)
            ))
        if drop_table:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(table)
                ))
        cur.execute(
            sql.SQL("CREATE UNLOGGED TABLE IF NOT EXISTS {}.{} (raw_json jsonb)").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            ))
        cur.copy_expert(sql.SQL("COPY {}.{} FROM STDIN").format(sql.Identifier(schema), sql.Identifier(table)), iterdata)

def parse_line(rawline, remove_fields=[]):
    """Check if line is valid JSON and handle exceptions
    
    Parameters:
        line: a str value possibly containing json data or a python object

    Returns:
        parsed line as str in case a line is parsed to support line iterators
        None if parsing is not successful
    """
    try:
        parsed_line = json.loads(rawline)
    except (SyntaxError, ValueError) as exc:
        try:
            parsed_line = ast.literal_eval(str(rawline, 'utf-8'))
        except (SyntaxError, ValueError) as exc:
            return None
    for field in remove_fields:
        if parsed_line.get(field) is not None:
            del parsed_line[field]
    # Add newline, replace escaped NULL characters not supported by Postgres jsonb format, extend escape characters
    return str(json.dumps(parsed_line) + '\n').replace("\\u0000", "").replace("\\", "\\\\")

def load(path, target_schema, target_table, drop_table=True, rem_fields=[], **kwargs):
    if path.startswith('http'):
        stream = download_stream(path)
    else:
        stream = file_stream(path)
    iterreview = string_iterator.StringIteratorIO(parse_line(item, rem_fields) for item in stream)
    create_staging_table(target_schema, target_table, iterreview, drop_table)

if __name__ == '__main__':
    from utils.download_from_s3 import file_stream, json_stream
    from utils import string_iterator

    load('./data/item_sample.json.gz', 'dbt_reviews', 'raw_reviews', False, ['reviewText'])
    load('./data/metadata_sample.json.gz', 'dbt_reviews', 'raw_metadata', False, ['description'])
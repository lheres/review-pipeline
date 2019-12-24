import json
import psycopg2
import ast
from psycopg2 import sql

from .utils.download_from_s3 import file_stream, json_stream, iterlines
from .utils import string_iterator

connection = psycopg2.connect(
    host="localhost",
    database="de",
    user="lheres",
    password="pg112!!@"
)
connection.autocommit = True

def create_staging_table(schema: str, table: str, iterdata, drop_table=True):
    with connection.cursor() as cur:
        if drop_table:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
        cur.execute(
            sql.SQL("CREATE UNLOGGED TABLE IF NOT EXISTS {}.{} (raw_json jsonb)").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            ))
        cur.copy_expert(sql.SQL("COPY {}.{} FROM STDIN").format(sql.Identifier(schema), sql.Identifier(table)), iterdata)

def parse_line(rawline):
    """Check if line is valid JSON and handle exceptions
    
    Parameters:
        line: a str value possibly containing json data or a python object

    Returns:
        parsed line as str in case a line is parsed, with additional newline to support line iterators
        None if parsing is not successful
    """
    try:
        parsed_line = json.loads(rawline)
    except (SyntaxError, ValueError) as exc:
        try:
            parsed_line = ast.literal_eval(str(rawline, 'utf-8'))
        except (SyntaxError, ValueError) as exc:
            return None
    # Add escape characters and replace NULL unicode string as these are not supported by Postgres jsonb format
    return str(json.dumps(parsed_line) + '\n').replace("\\", "\\\\").replace("\x00", "")

def load(path, target_schema, target_table):
    stream = file_stream(path)
    iterreview = string_iterator.StringIteratorIO(parse_line(review) for review in stream)
    create_staging_table(target_schema, target_table, iterreview)

if __name__ == '__main__':
    load('../item_sample.json.gz', 'dbt_reviews', 'raw_reviews')
    load('../metadata_sample.json.gz', 'dbt_reviews', 'raw_metadata')
        #cnt = 0
        #for line in iterreview:
        #    print(line)
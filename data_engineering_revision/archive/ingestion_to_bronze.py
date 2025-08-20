import sqlite3
import psycopg2
import pandas as pd
from glob import glob
import re, io, unicodedata
from tqdm.auto import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url


POSSIBLE_ENCODINGS = ("utf-8", "cp1252", "latin1")

def _to_utf8(x):
    if isinstance(x, (bytes, bytearray)):
        for enc in POSSIBLE_ENCODINGS:
            try:
                return x.decode(enc)
            except Exception:
                pass
        return x.decode("utf-8", errors="replace")
    return x

def load_sqlite_data(sqlite_path):
    engine = create_engine(f"sqlite:///{sqlite_path}")
    df = pd.read_sql("SELECT * FROM sqlite_master WHERE type='table'", engine)

    return df

def load_data(sqlite_path, table_name):
    con = sqlite3.connect(sqlite_path)
    con.text_factory = bytes  # return TEXT as bytes so we control decoding
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', con)
    finally:
        con.close()

    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(_to_utf8)
        df[c] = df[c].map(lambda s: unicodedata.normalize("NFC", s) if isinstance(s, str) else s)
    return df



def create_database_if_not_exists(pg_url, dbname):
    # Remove dbname from URL to connect to default 'postgres' db
    url = make_url(pg_url)
    default_url = url.set(database='postgres')
    engine = create_engine(default_url)
    with engine.connect() as conn:
        # Check if database exists
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname=:dbname"), {"dbname": dbname})
        exists = result.scalar() is not None

        if not exists:
            conn.execute(text(f"commit"))
            conn.execute(text(f'CREATE DATABASE "{dbname}" WITH ENCODING \'UTF8\' TEMPLATE template1'))
            print(f"Database '{dbname}' created.")
        else:
            print(f"Database '{dbname}' already exists.")
    engine.dispose()

def migrate_all_sqlite_to_postgres(sqlite_df, pg_url, if_exists='replace'):
    """
    For each row in sqlite_df, migrates all tables from the SQLite file into a separate Postgres schema.
    The schema name is taken from the 'schema_name' column.
    """
    try:
        url = make_url(pg_url)
        # Ensure database exists
        create_database_if_not_exists(pg_url, url.database)
        pg_engine = create_engine(pg_url)

        schema = 'bronze'
        # Create schema if not exists, using a direct connection and commit
        with pg_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))

        for idx, row in tqdm(sqlite_df.iterrows(), total=sqlite_df.shape[0], desc="Migrating SQLite to Postgres"):
            sqlite_path = row['sqlite_paths']
            db_name = row['schema_name']

            tables_df = load_sqlite_data(sqlite_path)
            for table in tables_df['name']:
                df = load_data(sqlite_path, table)
                df.to_sql(f'{db_name}__{table}', pg_engine, schema=schema, if_exists=if_exists, index=False)
                #print(f"Migrated {table} from {sqlite_path} to schema {schema}")
    except Exception as e:
        print(f"Failed for {sqlite_path}: {e}")

if __name__ == "__main__":
    sqlite_df = pd.DataFrame({
        "sqlite_paths": glob("spider_data/database/*/*.sqlite")
    })
    sqlite_df['schema_name'] = sqlite_df['sqlite_paths'].str.extract(r'spider_data/database/(.*?)/(.*?)\.sqlite')[0]

    pg_url = "postgresql+psycopg2://postgres:root@localhost:5432/spider_dataset"
    migrate_all_sqlite_to_postgres(sqlite_df, pg_url)

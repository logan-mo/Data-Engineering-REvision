from sqlalchemy.engine.url import make_url
import psycopg2

def drop_database(pg_url, dbname):
    """
    Drops the entire Postgres database specified by dbname. Must run outside a transaction block.
    """
    

    conn = psycopg2.connect(pg_url)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Terminate all connections to the target database
    cur.execute(f'''
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid()
    ''', (dbname,))
    
    # Drop the database
    cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
    print(f"Database '{dbname}' dropped.")
    cur.close()
    conn.close()
    

if __name__ == "__main__":
    # pg_url = "postgresql+psycopg2://postgres:root@localhost:5432/spider_dataset"
    pg_url = "host=localhost port=5432 dbname=postgres user=postgres password=root"
    drop_database(pg_url, 'spider_dataset')
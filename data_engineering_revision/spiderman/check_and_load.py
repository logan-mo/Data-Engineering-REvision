import os
import subprocess
import sys
from urllib.parse import quote_plus

import mysql.connector


def get_env(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        print(f"ERROR: {name} is required but not set.", file=sys.stderr)
        sys.exit(1)
    return val


MYSQL_HOST = get_env("MYSQL_HOST", required=True)
MYSQL_PORT = int(get_env("MYSQL_PORT", "3306"))
MYSQL_USER = get_env("MYSQL_USER", required=True)
MYSQL_PASSWORD = get_env("MYSQL_PASSWORD", required=True)
MYSQL_DB = get_env("MYSQL_DB", required=True)
EXPECTED = [
    t.strip()
    for t in os.getenv("SPIDERMAN_EXPECTED_TABLES", "").split(",")
    if t.strip()
]


def server_connect(db=None):
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db if db else None,
        autocommit=True,
        connection_timeout=10,
    )


def ensure_database_exists():
    conn = server_connect()
    cur = conn.cursor()
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
    )
    cur.close()
    conn.close()


def database_table_count():
    conn = server_connect(MYSQL_DB)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s",
        (MYSQL_DB,),
    )
    (count,) = cur.fetchone()
    cur.close()
    conn.close()
    return int(count)


def tables_exist_and_nonempty(tables):
    if not tables:
        return False
    conn = server_connect(MYSQL_DB)
    cur = conn.cursor()
    for tbl in tables:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """,
            (MYSQL_DB, tbl),
        )
        (exists,) = cur.fetchone()
        if exists == 0:
            cur.close()
            conn.close()
            return False
        # Check at least one row
        try:
            cur.execute(f"SELECT 1 FROM `{MYSQL_DB}`.`{tbl}` LIMIT 1")
            _ = cur.fetchone()  # None means empty
            if _ is None:
                cur.close()
                conn.close()
                return False
        except Exception:
            # If we can't select, treat as not loaded
            cur.close()
            conn.close()
            return False
    cur.close()
    conn.close()
    return True


def build_sqlalchemy_url():
    user = quote_plus(MYSQL_USER)
    pwd = quote_plus(MYSQL_PASSWORD)
    host = MYSQL_HOST
    port = MYSQL_PORT
    db = MYSQL_DB
    return f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"


def main():
    ensure_database_exists()

    table_count = database_table_count()
    if table_count == 0:
        need_load = True
        reason = f"database `{MYSQL_DB}` is empty"
    else:
        if EXPECTED:
            if tables_exist_and_nonempty(EXPECTED):
                need_load = False
                reason = (
                    f"all expected tables present and non-empty: {', '.join(EXPECTED)}"
                )
            else:
                need_load = True
                reason = f"expected tables missing or empty"
        else:
            # Without an explicit manifest of expected tables, we only load when DB is empty.
            need_load = False
            reason = f"database `{MYSQL_DB}` already has {table_count} tables (no EXPECTED list set)"

    print(f"[spiderman] Check result: need_load={need_load} ({reason})")

    if need_load:
        url = build_sqlalchemy_url()
        print(f"[spiderman] Loading dataset with URL: {url}")
        # Run from repo root where the script exists
        subprocess.run(
            [sys.executable, "scripts/load_dataset.py", url],
            check=True,
            cwd="/app/spiderman",
        )
        print("[spiderman] Load finished.")
    else:
        print("[spiderman] Skipping load. Nothing to do.")


if __name__ == "__main__":
    main()

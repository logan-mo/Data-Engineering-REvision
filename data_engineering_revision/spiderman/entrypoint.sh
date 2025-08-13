#!/usr/bin/env bash
set -euo pipefail

# Basic sanity checks
: "${MYSQL_HOST:?MYSQL_HOST is required}"
: "${MYSQL_USER:?MYSQL_USER is required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD is required}"
: "${MYSQL_DB:?MYSQL_DB is required}"
: "${MYSQL_PORT:=3306}"

# Run the checker/loader. It will only load if needed.
python /app/check_and_load.py

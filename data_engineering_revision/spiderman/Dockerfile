FROM python:3.10-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Clone the repo
RUN git clone https://github.com/cloudera/spiderman.git /app/spiderman

# Python deps (ensure mysql-connector for the given SQLAlchemy URL)
RUN pip install --no-cache-dir -r /app/spiderman/requirements.txt \
    && pip install --no-cache-dir mysql-connector-python

# Environment variables (values supplied at runtime)
ENV MYSQL_HOST="" \
    MYSQL_PORT="3306" \
    MYSQL_USER="" \
    MYSQL_PASSWORD="" \
    MYSQL_DB="" \
    SPIDERMAN_EXPECTED_TABLES=""

# Add helper scripts
COPY check_and_load.py /app/check_and_load.py
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

WORKDIR /app/spiderman

ENTRYPOINT ["/app/entrypoint.sh"]

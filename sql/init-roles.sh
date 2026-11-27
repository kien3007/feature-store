#!/bin/bash
# Chạy một lần khi PostgreSQL khởi tạo volume mới:
#  - database riêng cho metadata Airflow
#  - user chỉ đọc cho Grafana trên database fsmon
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'EOSQL'
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow OWNER airflow;

    CREATE USER grafana_ro WITH PASSWORD 'grafana_ro';
    GRANT CONNECT ON DATABASE fsmon TO grafana_ro;
    GRANT USAGE ON SCHEMA public TO grafana_ro;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
    ALTER DEFAULT PRIVILEGES FOR USER fsmon IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
EOSQL

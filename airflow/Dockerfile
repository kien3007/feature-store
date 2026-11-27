# Airflow 3 với môi trường pipeline tách riêng.
#  - Airflow dùng Python của image gốc (theo constraints chính thức của bản 3.3.2).
#  - fsmon, Feast, DuckDB, Evidently nằm trong /opt/fsmon-venv, cài từ uv.lock.
#  - Mã nguồn không chép vào image: repo được mount tại /opt/project.
FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/* \
 && git config --system --add safe.directory /opt/project
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv
RUN mkdir -p /opt/fsmon-venv && chown airflow: /opt/fsmon-venv

USER airflow
WORKDIR /tmp/fsmon-build
COPY --chown=airflow: pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/fsmon-venv UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
RUN unset VIRTUAL_ENV && uv sync --frozen --no-dev --no-install-project --python /usr/python/bin/python3.11 \
 && /opt/fsmon-venv/bin/python -c "import feast, duckdb, evidently; print(feast.__version__, duckdb.__version__)"
WORKDIR /opt/airflow

"""Kết nối DuckDB có giới hạn bộ nhớ."""

from __future__ import annotations

from pathlib import Path

import duckdb

from fsmon.config import Settings


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{settings.duckdb_memory_limit}'")
    con.execute(f"SET threads={settings.duckdb_threads}")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET TimeZone='UTC'")
    con.execute("SET enable_progress_bar=false")
    return con


def sql_path(p: Path) -> str:
    """Đường dẫn dạng chuỗi SQL an toàn (dấu / để DuckDB đọc được trên Windows)."""
    return "'" + p.as_posix().replace("'", "''") + "'"

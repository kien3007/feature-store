"""Nạp pipeline.yaml và checks.yaml, giải đường dẫn, ánh xạ đồng hồ phát lại."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import Any

import yaml

DEFAULT_ROOT = Path(__file__).resolve().parents[2]


def _parse_date(value: str | date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return datetime.fromisoformat(str(value)).replace(tzinfo=None)


@dataclass
class Settings:
    root: Path
    pipeline: dict[str, Any]
    checks: dict[str, Any]
    overrides: dict[str, Any] = field(default_factory=dict)

    # ---------- đường dẫn ----------
    def path(self, key: str) -> Path:
        p = Path(self.pipeline["paths"][key])
        return p if p.is_absolute() else self.root / p

    @property
    def source_dir(self) -> Path:
        return self.path("source_dir")

    @property
    def raw_dir(self) -> Path:
        return self.path("raw")

    @property
    def staging_dir(self) -> Path:
        return self.path("staging")

    @property
    def published_dir(self) -> Path:
        return self.path("published")

    @property
    def quarantine_dir(self) -> Path:
        return self.path("quarantine")

    @property
    def reports_dir(self) -> Path:
        return self.path("reports")

    @property
    def alerts_log(self) -> Path:
        return self.path("alerts_log")

    @property
    def feature_repo(self) -> Path:
        return self.path("feature_repo")

    def staging_snapshot_dir(self, snapshot_ts: datetime) -> Path:
        return self.staging_dir / f"snapshot_ts={snapshot_ts:%Y-%m-%d}"

    def quarantine_snapshot_dir(self, snapshot_ts: datetime) -> Path:
        return self.quarantine_dir / f"snapshot_ts={snapshot_ts:%Y-%m-%d}"

    # ---------- tham số ----------
    @property
    def dsn(self) -> str:
        return os.environ.get("FSMON_DB_DSN") or self.pipeline["metrics_db"]["dsn"]

    @property
    def period(self) -> timedelta:
        return timedelta(hours=float(self.pipeline["schedule"]["period_hours"]))

    @property
    def duckdb_memory_limit(self) -> str:
        return os.environ.get("FSMON_DUCKDB_MEMORY_LIMIT") or self.pipeline["duckdb"]["memory_limit"]

    @property
    def duckdb_threads(self) -> int:
        return int(self.pipeline["duckdb"].get("threads", 4))

    @property
    def active_window_days(self) -> int:
        return int(self.pipeline["population"]["active_window_days"])

    @cached_property
    def views(self) -> dict[str, dict[str, Any]]:
        return self.checks["views"]

    def check_cfg(self, name: str) -> dict[str, Any]:
        return self.checks["checks"][name]

    @property
    def baseline_window(self) -> int:
        return int(self.checks["baseline"]["window"])

    @property
    def baseline_min(self) -> int:
        return int(self.checks["baseline"]["min_snapshots"])

    # ---------- đồng hồ phát lại ----------
    def snapshot_from_logical(self, logical: str | date | datetime) -> datetime:
        """Ánh xạ logical date của Airflow sang snapshot_ts trong thời gian dữ liệu."""
        ld = _parse_date(logical)
        ld = datetime(ld.year, ld.month, ld.day)
        rp = self.pipeline.get("replay", {})
        if not rp.get("enabled"):
            return ld
        return _parse_date(rp["data_anchor"]) + (ld - _parse_date(rp["wall_anchor"]))


def load_settings(root: str | Path | None = None) -> Settings:
    root = Path(root or os.environ.get("FSMON_ROOT") or DEFAULT_ROOT).resolve()
    with open(root / "config" / "pipeline.yaml", encoding="utf-8") as f:
        pipeline = yaml.safe_load(f)
    with open(root / "config" / "checks.yaml", encoding="utf-8") as f:
        checks = yaml.safe_load(f)
    return Settings(root=root, pipeline=pipeline, checks=checks)


def parse_snapshot_ts(value: str | date | datetime) -> datetime:
    d = _parse_date(value)
    return datetime(d.year, d.month, d.day)

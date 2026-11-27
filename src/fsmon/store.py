"""Kho metrics: hai bảng snapshot_runs và snapshot_metrics.

Chạy trên PostgreSQL (mặc định) hoặc SQLite (cho test). Bản PostgreSQL có DDL
tương ứng trong sql/schema.sql; hai nơi phải khớp nhau.
Mọi thời gian lưu dạng UTC.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    create_engine,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

snapshot_runs = Table(
    "snapshot_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("snapshot_ts", DateTime(timezone=True), nullable=False, index=True),
    # running | audited | published | quarantined | failed_after_gate | failed
    Column("status", String(32), nullable=False),
    Column("health", String(8)),  # pass | warn | fail
    Column("trigger", String(32)),  # manual | airflow | backfill | demo
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("gate_decided_at", DateTime(timezone=True)),
    Column("published_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("duration_s", Float),
    Column("row_count", BigInteger),
    Column("git_sha", String(64)),
    Column("feast_version", String(32)),
    Column("reason", Text),
    Column("report_url", Text),
    Column("fault", String(64)),
    Column("materialized", Boolean),
    Column("approved_by", String(128)),
    Column("approval_reason", Text),
    Column("approved_at", DateTime(timezone=True)),
    Column("airflow_run_id", String(256)),
)

snapshot_metrics = Table(
    "snapshot_metrics",
    metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False, index=True),
    Column("snapshot_ts", DateTime(timezone=True), nullable=False),
    Column("feature_view", String(128), nullable=False),
    Column("feature", String(128)),
    Column("check_name", String(64), nullable=False),
    Column("metric_name", String(64), nullable=False),
    Column("value", Float),
    Column("warn_threshold", Float),
    Column("fail_threshold", Float),
    Column("status", String(8), nullable=False),  # pass | warn | fail | info
    Column("severity", String(16), nullable=False),
    Column("baseline_ref", Text),
    Column("detail", Text),
    Column("recorded_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_metrics_view_metric_ts",
    snapshot_metrics.c.feature_view,
    snapshot_metrics.c.metric_name,
    snapshot_metrics.c.snapshot_ts,
)

PUBLISHED = "published"


def utcnow() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


@dataclass
class Metric:
    feature_view: str
    feature: str | None
    check_name: str
    metric_name: str
    value: float | None
    status: str
    severity: str
    warn_threshold: float | None = None
    fail_threshold: float | None = None
    baseline_ref: str | None = None
    detail: str | None = None


class MetricsStore:
    def __init__(self, dsn: str):
        kwargs: dict[str, Any] = {"future": True, "pool_pre_ping": True}
        if dsn.startswith("postgresql"):
            kwargs["connect_args"] = {"options": "-c timezone=UTC"}
        self.engine: Engine = create_engine(dsn, **kwargs)

    def init_schema(self) -> None:
        metadata.create_all(self.engine)

    # ---------- runs ----------
    def create_run(self, run_id: str, snapshot_ts: datetime, **fields: Any) -> None:
        row = {"run_id": run_id, "snapshot_ts": as_utc(snapshot_ts), "status": "running", "started_at": utcnow()}
        row.update({k: as_utc(v) if isinstance(v, datetime) else v for k, v in fields.items()})
        with self.engine.begin() as c:
            c.execute(insert(snapshot_runs).values(**row))

    def update_run(self, run_id: str, **fields: Any) -> None:
        fields = {k: as_utc(v) if isinstance(v, datetime) else v for k, v in fields.items()}
        with self.engine.begin() as c:
            c.execute(update(snapshot_runs).where(snapshot_runs.c.run_id == run_id).values(**fields))

    def get_run(self, run_id: str) -> dict | None:
        with self.engine.connect() as c:
            r = c.execute(select(snapshot_runs).where(snapshot_runs.c.run_id == run_id)).mappings().first()
        return _norm(dict(r)) if r else None

    def runs(self, limit: int = 50) -> list[dict]:
        with self.engine.connect() as c:
            rows = (
                c.execute(select(snapshot_runs).order_by(snapshot_runs.c.started_at.desc()).limit(limit))
                .mappings()
                .all()
            )
        return [_norm(dict(r)) for r in rows]

    def latest_run_for(self, snapshot_ts: datetime, status: str | None = None) -> dict | None:
        q = select(snapshot_runs).where(snapshot_runs.c.snapshot_ts == as_utc(snapshot_ts))
        if status:
            q = q.where(snapshot_runs.c.status == status)
        with self.engine.connect() as c:
            r = c.execute(q.order_by(snapshot_runs.c.started_at.desc()).limit(1)).mappings().first()
        return _norm(dict(r)) if r else None

    def published_runs(self, before: datetime | None = None, limit: int = 7) -> list[dict]:
        """Run đã publish gần nhất, mỗi snapshot_ts một run (run publish mới nhất), mới -> cũ."""
        q = select(snapshot_runs).where(snapshot_runs.c.status == PUBLISHED)
        if before is not None:
            q = q.where(snapshot_runs.c.snapshot_ts < as_utc(before))
        with self.engine.connect() as c:
            rows = (
                c.execute(q.order_by(snapshot_runs.c.snapshot_ts.desc(), snapshot_runs.c.published_at.desc()))
                .mappings()
                .all()
            )
        seen: set = set()
        out: list[dict] = []
        for r in rows:
            r = _norm(dict(r))
            if r["snapshot_ts"] in seen:
                continue
            seen.add(r["snapshot_ts"])
            out.append(r)
            if len(out) >= limit:
                break
        return out

    def serving_snapshot(self, exclude_run: str | None = None) -> datetime | None:
        """snapshot_ts mới nhất đã publish và được materialize, tức là đang phục vụ online."""
        q = select(func.max(snapshot_runs.c.snapshot_ts)).where(
            and_(snapshot_runs.c.status == PUBLISHED, snapshot_runs.c.materialized.is_(True))
        )
        if exclude_run:
            q = q.where(snapshot_runs.c.run_id != exclude_run)
        with self.engine.connect() as c:
            v = c.execute(q).scalar()
        return as_utc(v)

    # ---------- metrics ----------
    def write_metrics(self, run_id: str, snapshot_ts: datetime, metrics: Iterable[Metric]) -> int:
        now = utcnow()
        rows = [
            {**asdict(m), "run_id": run_id, "snapshot_ts": as_utc(snapshot_ts), "recorded_at": now} for m in metrics
        ]
        if rows:
            with self.engine.begin() as c:
                c.execute(insert(snapshot_metrics), rows)
        return len(rows)

    def metrics_for_run(self, run_id: str) -> list[dict]:
        with self.engine.connect() as c:
            rows = c.execute(select(snapshot_metrics).where(snapshot_metrics.c.run_id == run_id)).mappings().all()
        return [dict(r) for r in rows]

    def metric_values(
        self, run_ids: list[str], feature_view: str, metric_name: str, feature: str | None = None
    ) -> list[float]:
        if not run_ids:
            return []
        q = select(snapshot_metrics.c.value).where(
            and_(
                snapshot_metrics.c.run_id.in_(run_ids),
                snapshot_metrics.c.feature_view == feature_view,
                snapshot_metrics.c.metric_name == metric_name,
            )
        )
        q = q.where(snapshot_metrics.c.feature.is_(None) if feature is None else snapshot_metrics.c.feature == feature)
        with self.engine.connect() as c:
            return [v for (v,) in c.execute(q).all() if v is not None]

    def baseline_median(
        self, run_ids: list[str], feature_view: str, metric_name: str, feature: str | None = None
    ) -> float | None:
        vals = self.metric_values(run_ids, feature_view, metric_name, feature)
        return statistics.median(vals) if vals else None


def _norm(row: dict) -> dict:
    for k, v in row.items():
        if isinstance(v, datetime):
            row[k] = as_utc(v)
    return row

"""Các check sức khỏe snapshot.

Mỗi check nhận ViewContext và trả về danh sách Metric. Giá trị và ngưỡng lưu cùng
một thang đo (ví dụ null_rate và ngưỡng null_rate tuyệt đối đã cộng baseline), để
Grafana vẽ đường ngưỡng ngay trên chuỗi giá trị.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from fsmon.config import Settings
from fsmon.db import sql_path
from fsmon.store import Metric, MetricsStore

PASS, WARN, FAIL, INFO = "pass", "warn", "fail", "info"


@dataclass
class ViewContext:
    settings: Settings
    store: MetricsStore
    con: duckdb.DuckDBPyConnection
    view: str
    snapshot_ts: datetime
    staging_path: Path
    manifest: dict[str, Any]
    baseline_runs: list[dict]  # run đã publish trước snapshot_ts, mới -> cũ
    # Cột dùng được sau check schema: tên -> kiểu hợp đồng
    usable: dict[str, str] = field(default_factory=dict)

    @property
    def contract(self) -> dict[str, Any]:
        return self.settings.views[self.view]

    @property
    def features(self) -> dict[str, dict[str, Any]]:
        return self.contract["features"]

    @property
    def baseline_ready(self) -> bool:
        return len(self.baseline_runs) >= self.settings.baseline_min

    @property
    def baseline_ref(self) -> str:
        n = len(self.baseline_runs)
        if n == 0:
            return "baseline_incomplete(n=0)"
        first = min(r["snapshot_ts"] for r in self.baseline_runs)
        last = max(r["snapshot_ts"] for r in self.baseline_runs)
        tag = f"median_{n}:{first:%Y-%m-%d}..{last:%Y-%m-%d}"
        return tag if self.baseline_ready else f"baseline_incomplete(n={n});{tag}"

    @property
    def baseline_run_ids(self) -> list[str]:
        return [r["run_id"] for r in self.baseline_runs]

    @property
    def staging_rel(self) -> str:
        return f"read_parquet({sql_path(self.staging_path)}, hive_partitioning=false)"

    @property
    def published_path(self) -> Path:
        return self.settings.published_dir / f"{self.view}.parquet"

    def baseline_rel(self) -> str | None:
        """Quan hệ SQL gộp các snapshot baseline trong published/, hoặc None."""
        if not self.baseline_runs or not self.published_path.exists():
            return None
        ts = ", ".join(f"TIMESTAMPTZ '{r['snapshot_ts']:%Y-%m-%d %H:%M:%S}+00'" for r in self.baseline_runs)
        return (
            f"(SELECT * FROM read_parquet({sql_path(self.published_path)}, hive_partitioning=false) "
            f"WHERE event_timestamp IN ({ts}))"
        )

    def cfg(self, check: str) -> dict[str, Any]:
        return self.settings.check_cfg(check)

    def metric(
        self,
        check_group: str,
        name: str,
        value: float | None,
        status: str,
        *,
        feature: str | None = None,
        warn: float | None = None,
        fail: float | None = None,
        relative: bool = False,
        detail: str | None = None,
    ) -> Metric:
        """Tạo Metric; check tương đối khi baseline chưa đủ thì hạ về info và gắn nhãn."""
        ref = None
        if relative:
            ref = self.baseline_ref
            if not self.baseline_ready and status in (WARN, FAIL, PASS):
                detail = f"baseline_incomplete; raw_status={status}" + (f"; {detail}" if detail else "")
                status = INFO
        return Metric(
            feature_view=self.view,
            feature=feature,
            check_name=check_group,
            metric_name=name,
            value=None if value is None else float(value),
            status=status,
            severity=self.cfg(name)["severity"] if name in self.settings.checks["checks"] else "info",
            warn_threshold=warn,
            fail_threshold=fail,
            baseline_ref=ref,
            detail=detail,
        )


def grade(
    value: float | None, warn: float | None = None, fail: float | None = None, lower_is_worse: bool = False
) -> str:
    """So giá trị với ngưỡng: vượt ngưỡng nghiêm ngặt (> hoặc <) mới tính là vi phạm."""
    if value is None:
        return INFO

    def bad(th: float) -> bool:
        return value < th if lower_is_worse else value > th

    if fail is not None and bad(fail):
        return FAIL
    if warn is not None and bad(warn):
        return WARN
    return PASS


def numeric(ftype: str) -> bool:
    return ftype in ("int64", "double")

"""Tiêm lỗi vào snapshot đang ở staging, dùng cho kịch bản demo và test.

Lỗi được tiêm sau transform và trước audit, đúng chỗ một lỗi dữ liệu thật sẽ xuất hiện.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from fsmon.config import Settings
from fsmon.db import connect, sql_path
from fsmon.store import MetricsStore
from fsmon.transform import read_manifest, write_manifest

log = logging.getLogger(__name__)

# fault -> (view, feature mặc định, mô tả)
FAULTS: dict[str, tuple[str, str | None, str]] = {
    "null_spike": ("customer_activity_7d", "amount_mean_7d", "đặt null thêm 30 điểm phần trăm dòng -> null_rate fail"),
    "schema_drop": ("customer_activity_30d", "amount_std_30d", "bỏ một cột -> schema_match fail"),
    "duplicate_keys": ("customer_rfm_90d", None, "nhân đôi 5% entity -> key_duplicate_rate fail"),
    "shift": (
        "customer_activity_30d",
        "amount_sum_30d",
        "nhân feature với 3 -> psi vượt ngưỡng, vẫn publish (advisory)",
    ),
    "out_of_range": ("customer_activity_7d", "online_txn_rate_7d", "đặt 5% giá trị = 1.5 -> out_of_range_rate fail"),
    "row_drop": ("customer_activity_7d", None, "bỏ 60% dòng -> row_count_change và entity_coverage fail"),
    "stale": (None, None, "nguồn ngừng cập nhật 3 ngày -> freshness_lag fail"),
}


def _ranked(frac: float) -> str:
    """Quan hệ {src} kèm cờ __hit đánh dấu đúng ceil(frac * n) dòng, chọn tất định theo hash."""
    return (
        "(SELECT * EXCLUDE (__rn, __n), __rn <= ceil(" + str(frac) + " * __n) AS __hit FROM ("
        "SELECT *, row_number() OVER (ORDER BY hash(client_id), client_id) AS __rn, count(*) OVER () AS __n "
        "FROM {src}))"
    )


def _rewrite(settings: Settings, path, select_sql: str) -> None:
    con = connect(settings)
    tmp = path.with_suffix(".parquet.tmp")
    src = f"read_parquet({sql_path(path)}, hive_partitioning=false)"
    con.execute(f"COPY ({select_sql.format(src=src)}) TO {sql_path(tmp)} (FORMAT parquet)")
    con.close()
    os.replace(tmp, path)


def inject(
    settings: Settings,
    store: MetricsStore | None,
    snapshot_ts: datetime,
    fault: str,
    view: str | None = None,
    feature: str | None = None,
) -> str:
    if fault not in FAULTS:
        raise ValueError(f"fault không hợp lệ: {fault}; chọn một trong {sorted(FAULTS)}")
    d_view, d_feat, _ = FAULTS[fault]
    view, feature = view or d_view, feature or d_feat
    sdir = settings.staging_snapshot_dir(snapshot_ts)
    manifest = read_manifest(sdir)
    path = sdir / f"{view}.parquet" if view else None

    if fault == "null_spike":
        # Chọn tất định ~30% dòng chưa null rồi đặt null.
        _rewrite(
            settings,
            path,
            f'SELECT * EXCLUDE (__hit) REPLACE (CASE WHEN __hit THEN NULL ELSE "{feature}" END '
            f'AS "{feature}") FROM {_ranked(0.30)}',
        )
    elif fault == "schema_drop":
        _rewrite(settings, path, f'SELECT * EXCLUDE ("{feature}") FROM {{src}}')
    elif fault == "duplicate_keys":
        _rewrite(
            settings, path, f"SELECT * FROM {{src}} UNION ALL SELECT * EXCLUDE (__hit) FROM {_ranked(0.05)} WHERE __hit"
        )
    elif fault == "shift":
        _rewrite(settings, path, f'SELECT * REPLACE ("{feature}" * 3 AS "{feature}") FROM {{src}}')
    elif fault == "out_of_range":
        _rewrite(
            settings,
            path,
            f'SELECT * EXCLUDE (__hit) REPLACE (CASE WHEN __hit THEN 1.5 ELSE "{feature}" END '
            f'AS "{feature}") FROM {_ranked(0.05)}',
        )
    elif fault == "row_drop":
        _rewrite(settings, path, f"SELECT * EXCLUDE (__hit) FROM {_ranked(0.60)} WHERE NOT __hit")
    elif fault == "stale":
        src_max = datetime.fromisoformat(manifest["source_max_event_ts"])
        manifest["source_max_event_ts"] = (src_max - timedelta(days=3)).isoformat()

    label = f"{fault}:{view}.{feature}" if view and feature else (f"{fault}:{view}" if view else fault)
    manifest["fault"] = label
    write_manifest(sdir, manifest)
    if store is not None and manifest.get("run_id"):
        store.update_run(manifest["run_id"], fault=label)
    log.warning("đã tiêm lỗi %s vào snapshot %s", label, f"{snapshot_ts:%Y-%m-%d}")
    return label

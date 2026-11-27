"""So khớp online store với published/ sau khi publish."""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime

import numpy as np

from fsmon.checks import INFO, grade
from fsmon.config import Settings
from fsmon.db import connect, sql_path
from fsmon.publish import feature_store
from fsmon.store import Metric, MetricsStore

log = logging.getLogger(__name__)

SERVICE = "customer_risk_v1"


def _same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
            return True
        try:
            return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-12)
        except (TypeError, ValueError):
            return False
    return a == b


def verify(settings: Settings, store: MetricsStore, run_id: str, snapshot_ts: datetime) -> tuple[list[Metric], bool]:
    """Trả về (metrics, ok). ok=False nghĩa là có view vượt ngưỡng fail."""
    serving = store.serving_snapshot()
    if serving is None:
        raise RuntimeError("chưa có snapshot nào đang phục vụ online")
    cfg = settings.check_cfg("online_match_rate")
    n = int(settings.pipeline["verify"]["sample_size"])
    fs = feature_store(settings)
    con = connect(settings)
    metrics: list[Metric] = []
    ok = True
    ts_lit = f"TIMESTAMPTZ '{serving:%Y-%m-%d %H:%M:%S}+00'"
    sample_ids: list[int] = []

    for view, spec in settings.views.items():
        feats = list(spec["features"])
        path = settings.published_dir / f"{view}.parquet"
        cols = ", ".join(f'"{f}"' for f in feats)
        rows = con.execute(
            f"SELECT client_id, {cols} FROM read_parquet({sql_path(path)}, hive_partitioning=false) "
            f"WHERE event_timestamp = {ts_lit} ORDER BY hash(client_id, {serving:%Y%m%d}) LIMIT {n}"
        ).fetchall()
        if not rows:
            metrics.append(
                Metric(
                    view,
                    None,
                    "online",
                    "online_match_rate",
                    None,
                    "fail",
                    cfg["severity"],
                    cfg["warn"],
                    cfg["fail"],
                    detail=f"published không có snapshot {serving}",
                )
            )
            ok = False
            continue
        ids = [r[0] for r in rows]
        sample_ids = sample_ids or ids
        resp = fs.get_online_features(
            features=[f"{view}:{f}" for f in feats], entity_rows=[{"client_id": i} for i in ids]
        ).to_dict()
        matched, first_bad = 0, None
        for k, r in enumerate(rows):
            good = all(_same(resp[f][k], r[j + 1]) for j, f in enumerate(feats))
            if good:
                matched += 1
            elif first_bad is None:
                bad_f = next(f for j, f in enumerate(feats) if not _same(resp[f][k], r[j + 1]))
                first_bad = (
                    f"client_id={r[0]} {bad_f}: online={resp[bad_f][k]!r} published={r[feats.index(bad_f) + 1]!r}"
                )
        rate = matched / len(rows)
        status = grade(rate, cfg["warn"], cfg["fail"], lower_is_worse=True)
        ok = ok and status != "fail"
        metrics.append(
            Metric(
                view,
                None,
                "online",
                "online_match_rate",
                rate,
                status,
                cfg["severity"],
                cfg["warn"],
                cfg["fail"],
                baseline_ref=f"published@{serving:%Y-%m-%d}",
                detail=f"matched={matched}/{len(rows)}" + (f"; first_mismatch: {first_bad}" if first_bad else ""),
            )
        )

    # Độ trễ: gọi đơn lẻ qua feature service.
    lat: list[float] = []
    service = fs.get_feature_service(SERVICE)
    for i in sample_ids[: int(settings.pipeline["verify"]["latency_samples"])]:
        t0 = time.perf_counter()
        fs.get_online_features(features=service, entity_rows=[{"client_id": i}])
        lat.append((time.perf_counter() - t0) * 1000)
    if lat:
        p95 = float(np.percentile(lat, 95))
        metrics.append(
            Metric(
                SERVICE,
                None,
                "ops",
                "online_get_p95_ms",
                p95,
                INFO,
                "info",
                detail=f"n={len(lat)}; p50={np.percentile(lat, 50):.2f}ms",
            )
        )
    con.close()
    return metrics, ok

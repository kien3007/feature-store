"""Kịch bản demo khoảng 5 phút (mục 10 của kế hoạch), chạy không cần sửa tay.

1. Snapshot bình thường -> pass, online cập nhật
2. null_spike     -> null_rate fail, quarantine, online giữ snapshot cũ, Grafana báo
3. schema_drop    -> schema_match fail
4. duplicate_keys -> key_duplicate_rate fail
5. shift          -> psi vượt ngưỡng nhưng vẫn publish (advisory) -> warn
6. Duyệt tay snapshot null_spike bằng approve, lý do vào nhật ký chạy

    uv run python scripts/demo.py [--start 2019-10-06] [--pause]
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import timedelta

from fsmon.cli import _quiet_libraries
from fsmon.config import load_settings, parse_snapshot_ts
from fsmon.publish import feature_store
from fsmon.runner import approve, run_snapshot
from fsmon.store import MetricsStore

PROBE = 0  # client_id dùng để chứng minh online store không đổi


def online_value(settings) -> float | None:
    fs = feature_store(settings)
    return fs.get_online_features(
        features=["customer_activity_7d:amount_mean_7d"], entity_rows=[{"client_id": PROBE}]
    ).to_dict()["amount_mean_7d"][0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", help="snapshot_ts đầu tiên của demo (mặc định: ngày sau snapshot gần nhất)")
    ap.add_argument("--pause", action="store_true", help="dừng chờ Enter giữa các bước")
    a = ap.parse_args()
    _quiet_libraries()
    for s in (sys.stdout, sys.stderr):
        s.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    store = MetricsStore(settings.dsn)
    if a.start:
        ts = parse_snapshot_ts(a.start)
    else:
        runs = store.runs(limit=1000)
        ts = (
            (max(r["snapshot_ts"] for r in runs).replace(tzinfo=None) + timedelta(days=1))
            if runs
            else parse_snapshot_ts("2019-10-01")
        )

    def step(title: str, fault: str | None = None):
        nonlocal ts
        if a.pause:
            input(f"\n[Enter] {title} ")
        print(f"\n=== {title} · snapshot {ts:%Y-%m-%d} ===")
        before = online_value(settings) if store.serving_snapshot() else None
        run = run_snapshot(settings, store, ts, trigger="demo", fault=fault)
        after = online_value(settings)
        print(f"status={run['status']} health={run['health']}" + (f" fault={run['fault']}" if run.get("fault") else ""))
        if run.get("reason"):
            print("lý do:", run["reason"][:300])
        if run.get("report_url"):
            print("báo cáo:", run["report_url"])
        serving = store.serving_snapshot()
        same = "không đổi" if before == after else "đã cập nhật"
        print(
            f"online phục vụ snapshot {serving:%Y-%m-%d}; amount_mean_7d(client {PROBE}) {before} -> {after} ({same})"
        )
        cur = ts
        ts += timedelta(days=1)
        return cur, run

    step("1. Snapshot bình thường")
    null_ts, _ = step("2. null_spike: tỷ lệ null +30 điểm phần trăm", "null_spike")
    step("3. schema_drop: bỏ một cột", "schema_drop")
    step("4. duplicate_keys: nhân đôi 5% entity", "duplicate_keys")
    step("5. shift: nhân feature với 3 (advisory)", "shift")

    if a.pause:
        input("\n[Enter] 6. Duyệt tay snapshot null_spike ")
    print(f"\n=== 6. Duyệt tay snapshot {null_ts:%Y-%m-%d} ===")
    res = approve(
        settings,
        store,
        null_ts,
        reason="demo: đã xác minh null là do nguồn tạm ngừng gửi trường amount",
        by="demo-operator",
    )
    r = res["run"]
    print(f"status={r['status']} approved_by={r['approved_by']} materialized={res['materialized']}")
    print("(snapshot cũ hơn snapshot đang phục vụ nên chỉ vào offline/published, online giữ snapshot mới hơn)")
    print("\nXem: Grafana http://localhost:3000 · cảnh báo nhận được http://localhost:8088/alerts · data/alerts.log")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

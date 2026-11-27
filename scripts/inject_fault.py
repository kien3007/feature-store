"""Tiêm lỗi cho kịch bản demo.

Mặc định chạy trọn một snapshot mới (ngày kế tiếp sau snapshot gần nhất) với lỗi được
tiêm sau transform, trước audit:

    uv run python scripts/inject_fault.py --fault null_spike
    uv run python scripts/inject_fault.py --fault shift --snapshot-ts 2019-10-08

--only-inject: chỉ sửa snapshot đang nằm ở staging (đã chạy `fsmon transform`), để tự chạy
các bước sau bằng `fsmon audit` / `fsmon decision` / ...
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import timedelta

from fsmon.cli import _quiet_libraries
from fsmon.config import load_settings, parse_snapshot_ts
from fsmon.faults import FAULTS, inject
from fsmon.runner import run_snapshot
from fsmon.store import MetricsStore


def next_snapshot(store: MetricsStore):
    runs = store.runs(limit=500)
    if not runs:
        raise SystemExit("chưa có run nào; chạy `make snapshot` hoặc backfill trước")
    return max(r["snapshot_ts"] for r in runs).replace(tzinfo=None) + timedelta(days=1)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--fault", required=True, choices=sorted(FAULTS), help="; ".join(f"{k}: {v[2]}" for k, v in FAULTS.items())
    )
    ap.add_argument("--snapshot-ts", help="mặc định: ngày kế tiếp sau snapshot gần nhất")
    ap.add_argument("--view")
    ap.add_argument("--feature")
    ap.add_argument("--only-inject", action="store_true")
    a = ap.parse_args(argv)

    _quiet_libraries()
    for s in (sys.stdout, sys.stderr):
        s.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("feast").setLevel(logging.WARNING)
    settings = load_settings()
    store = MetricsStore(settings.dsn)
    ts = parse_snapshot_ts(a.snapshot_ts) if a.snapshot_ts else next_snapshot(store)

    if a.only_inject:
        print(inject(settings, store, ts, a.fault, view=a.view, feature=a.feature))
        return 0
    run = run_snapshot(settings, store, ts, trigger="demo", fault=a.fault, fault_view=a.view, fault_feature=a.feature)
    print(f"{run['snapshot_ts']:%Y-%m-%d} fault={run['fault']} -> status={run['status']} health={run['health']}")
    if run.get("reason"):
        print("  " + run["reason"][:500])
    if run.get("report_url"):
        print("  báo cáo:", run["report_url"])
    print("  online đang phục vụ:", f"{store.serving_snapshot():%Y-%m-%d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

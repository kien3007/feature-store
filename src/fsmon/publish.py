"""staging -> published, feast apply khi định nghĩa đổi, materialize vào online store.

Publish xóa các hàng cùng snapshot_ts trong published/<view>.parquet rồi chèn snapshot
mới, ghi ra file tạm rồi thay thế nguyên tử, nên chạy lại không sinh bản trùng.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fsmon.config import Settings
from fsmon.db import connect, sql_path
from fsmon.store import MetricsStore, as_utc, utcnow

log = logging.getLogger(__name__)

APPLY_STAMP = ".feast_applied.sha256"


def _ts_lit(ts: datetime) -> str:
    return f"TIMESTAMPTZ '{ts:%Y-%m-%d %H:%M:%S}+00'"


def merge_into_published(settings: Settings, snapshot_ts: datetime, source_dir: Path) -> dict[str, int]:
    settings.published_dir.mkdir(parents=True, exist_ok=True)
    con = connect(settings)
    counts: dict[str, int] = {}
    for view in settings.views:
        src = source_dir / f"{view}.parquet"
        dst = settings.published_dir / f"{view}.parquet"
        tmp = dst.with_suffix(".parquet.tmp")
        new = f"SELECT * FROM read_parquet({sql_path(src)}, hive_partitioning=false)"
        if dst.exists():
            old = (
                f"SELECT * FROM read_parquet({sql_path(dst)}, hive_partitioning=false) "
                f"WHERE event_timestamp <> {_ts_lit(snapshot_ts)}"
            )
            query = f"{old} UNION ALL BY NAME {new}"
        else:
            query = new
        con.execute(
            f"COPY ({query} ORDER BY event_timestamp, client_id) TO {sql_path(tmp)} (FORMAT parquet, COMPRESSION zstd)"
        )
        os.replace(tmp, dst)
        counts[view] = con.execute(
            f"SELECT count(*) FROM read_parquet({sql_path(dst)}, hive_partitioning=false) "
            f"WHERE event_timestamp = {_ts_lit(snapshot_ts)}"
        ).fetchone()[0]
    con.close()
    return counts


def remove_from_published(settings: Settings, snapshot_ts: datetime) -> None:
    con = connect(settings)
    for view in settings.views:
        dst = settings.published_dir / f"{view}.parquet"
        if not dst.exists():
            continue
        tmp = dst.with_suffix(".parquet.tmp")
        con.execute(
            f"COPY (SELECT * FROM read_parquet({sql_path(dst)}, hive_partitioning=false) "
            f"WHERE event_timestamp <> {_ts_lit(snapshot_ts)} ORDER BY event_timestamp, client_id) "
            f"TO {sql_path(tmp)} (FORMAT parquet, COMPRESSION zstd)"
        )
        os.replace(tmp, dst)
    con.close()


# ---------------- Feast ----------------


def _repo_hash(settings: Settings) -> str:
    h = hashlib.sha256()
    for p in sorted(settings.feature_repo.glob("*.py")) + [settings.feature_repo / "feature_store.yaml"]:
        h.update(p.name.encode())
        h.update(p.read_bytes().replace(b"\r\n", b"\n"))
    return h.hexdigest()


def _feast_cmd() -> list[str]:
    exe_dir = Path(sys.executable).parent
    for name in ("feast", "feast.exe"):
        cand = exe_dir / name
        if cand.exists():
            return [str(cand)]
    found = shutil.which("feast")
    if found:
        return [found]
    return [sys.executable, "-c", "from feast.cli.cli import cli; cli()"]


def feast(settings: Settings, *args: str) -> str:
    cmd = _feast_cmd() + ["-c", str(settings.feature_repo), *args]
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "FEAST_USAGE": "False"}
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
    if res.returncode != 0:
        raise RuntimeError(f"feast {' '.join(args)} lỗi:\n{res.stdout}\n{res.stderr}")
    return res.stdout


def ensure_applied(settings: Settings, force: bool = False) -> bool:
    """feast apply nếu định nghĩa trong feature_repo/ đổi so với lần apply trước."""
    stamp = settings.root / "data" / APPLY_STAMP
    registry = settings.root / "data" / "registry.db"
    digest = _repo_hash(settings)
    if not force and registry.exists() and stamp.exists() and stamp.read_text().strip() == digest:
        return False
    # Feast cần batch source tồn tại để suy schema lúc apply.
    missing = [v for v in settings.views if not (settings.published_dir / f"{v}.parquet").exists()]
    if missing:
        raise RuntimeError(f"chưa có published/ cho {missing}; publish ít nhất một snapshot trước khi apply")
    log.info("feast apply")
    feast(settings, "apply")
    stamp.write_text(digest)
    return True


def feature_store(settings: Settings):
    from feast import FeatureStore

    return FeatureStore(repo_path=str(settings.feature_repo))


def materialize(settings: Settings, snapshot_ts: datetime) -> None:
    """Đẩy đúng một snapshot vào online store (cửa sổ ±1 phút quanh snapshot_ts).

    Dùng materialize có mốc tường minh thay vì materialize-incremental: dữ liệu phát lại
    nằm ở 2019 nên mốc 'now - ttl' mặc định của incremental không áp dụng được.
    """
    ts = as_utc(snapshot_ts)
    fs = feature_store(settings)
    fs.materialize(
        start_date=ts - timedelta(minutes=1), end_date=ts + timedelta(minutes=1), feature_views=list(settings.views)
    )


def online_db_path(settings: Settings) -> Path:
    return settings.root / "data" / "online_store.db"


def publish(
    settings: Settings, store: MetricsStore, snapshot_ts: datetime, run_id: str, source_dir: Path | None = None
) -> dict:
    source_dir = source_dir or settings.staging_snapshot_dir(snapshot_ts)
    counts = merge_into_published(settings, snapshot_ts, source_dir)
    ensure_applied(settings)
    serving = store.serving_snapshot(exclude_run=run_id)
    ts = as_utc(snapshot_ts)
    do_materialize = serving is None or ts >= serving
    if do_materialize:
        materialize(settings, snapshot_ts)
    else:
        # Snapshot cũ hơn snapshot đang phục vụ (ví dụ duyệt tay sau này): chỉ vào offline.
        log.info("bỏ qua materialize: %s cũ hơn snapshot đang phục vụ %s", ts, serving)
    store.update_run(run_id, status="published", published_at=utcnow(), materialized=do_materialize)
    return {"rows": counts, "materialized": do_materialize}


def clear_online(settings: Settings) -> None:
    """Xóa mọi hàng trong các bảng online (giữ bảng, vì tiến trình khác có thể đang mở file)."""
    import sqlite3

    db = online_db_path(settings)
    if not db.exists():
        return
    con = sqlite3.connect(db, timeout=30)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        project = feature_store(settings).project
        for t in tables:
            if t.startswith(f"{project}_"):
                con.execute(f'DELETE FROM "{t}"')
        con.commit()
    finally:
        con.close()


def restore_online(settings: Settings, store: MetricsStore) -> datetime | None:
    """Dựng lại online store từ snapshot tốt gần nhất trong published/."""
    good = store.published_runs(limit=1)
    clear_online(settings)
    if not good:
        log.warning("không có snapshot đã publish nào để khôi phục")
        return None
    ensure_applied(settings)
    ts = good[0]["snapshot_ts"]
    materialize(settings, ts)
    store.update_run(good[0]["run_id"], materialized=True)
    log.info("đã khôi phục online store về snapshot %s", ts)
    return ts


def now_utc() -> datetime:
    return datetime.now(UTC)

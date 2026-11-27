"""Tích hợp Feast: online store luôn giữ snapshot tốt gần nhất."""

from __future__ import annotations

from datetime import timedelta

import pytest

from fsmon.publish import feature_store, materialize
from fsmon.runner import approve, run_snapshot, step_verify
from tests.conftest import DATA_END

pytestmark = pytest.mark.integration


def _online_txn_cnt(settings, client_id=1):
    fs = feature_store(settings)
    return fs.get_online_features(
        features=["customer_rfm_90d:txn_cnt_90d"], entity_rows=[{"client_id": client_id}]
    ).to_dict()["txn_cnt_90d"][0]


def _published_txn_cnt(settings, ts, client_id=1):
    import duckdb

    from fsmon.db import sql_path

    return (
        duckdb.connect()
        .execute(
            f"SELECT txn_cnt_90d FROM read_parquet({sql_path(settings.published_dir / 'customer_rfm_90d.parquet')}) "
            f"WHERE client_id = {client_id} AND event_timestamp = TIMESTAMPTZ '{ts:%Y-%m-%d} 00:00:00+00'"
        )
        .fetchone()[0]
    )


def test_failed_snapshot_keeps_last_good_online_and_approve(env):
    settings, store = env
    good_ts, bad_ts = DATA_END - timedelta(days=1), DATA_END

    good = run_snapshot(settings, store, good_ts, trigger="test")
    assert good["status"] == "published" and good["materialized"]
    online_before = _online_txn_cnt(settings)
    assert online_before == _published_txn_cnt(settings, good_ts)

    bad = run_snapshot(settings, store, bad_ts, trigger="test", fault="duplicate_keys")
    assert bad["status"] == "quarantined" and bad["health"] == "fail"
    assert (settings.quarantine_snapshot_dir(bad_ts) / "manifest.json").exists()
    assert store.serving_snapshot() == good["snapshot_ts"]
    assert _online_txn_cnt(settings) == online_before  # online không đổi

    # Duyệt tay snapshot đang quarantine: publish và ghi lý do.
    res = approve(settings, store, bad_ts, reason="kiểm tra tay: trùng khóa do nguồn gửi lặp", by="tester")
    run = res["run"]
    assert run["status"] == "published" and run["approved_by"] == "tester"
    assert run["approval_reason"].startswith("kiểm tra tay")
    assert store.serving_snapshot() == run["snapshot_ts"]


def test_online_mismatch_triggers_failed_after_gate_and_restore(env):
    settings, store = env
    t1, t2 = DATA_END - timedelta(days=1), DATA_END
    run_snapshot(settings, store, t1, trigger="test")
    r2 = run_snapshot(settings, store, t2, trigger="test")
    assert r2["status"] == "published"

    # Làm hỏng online store: ghi đè bằng snapshot cũ t1, rồi verify t2.
    materialize(settings, t1)
    assert step_verify(settings, store, t2) is False
    run = store.get_run(r2["run_id"])
    assert run["status"] == "failed_after_gate"
    assert store.serving_snapshot() == store.get_run(store.latest_run_for(t1)["run_id"])["snapshot_ts"]
    assert _online_txn_cnt(settings) == _published_txn_cnt(settings, t1)

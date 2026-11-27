"""Transform không dùng dữ liệu tương lai, publish idempotent, hợp đồng khớp Feast."""

from __future__ import annotations

import importlib
import sys
from datetime import timedelta

import duckdb
import pyarrow.parquet as pq

from fsmon.db import sql_path
from fsmon.publish import merge_into_published
from fsmon.transform import transform
from tests.conftest import DATA_END, REPO

FEATURE_COLS_EXCLUDE = {"created_timestamp"}


def _read(path):
    t = pq.read_table(path).to_pandas()
    return (
        t.drop(columns=[c for c in FEATURE_COLS_EXCLUDE if c in t.columns])
        .sort_values("client_id")
        .reset_index(drop=True)
    )


def test_transform_is_deterministic(env):
    settings, _ = env
    transform(settings, DATA_END)
    a = {v: _read(settings.staging_snapshot_dir(DATA_END) / f"{v}.parquet") for v in settings.views}
    transform(settings, DATA_END)
    for v in settings.views:
        b = _read(settings.staging_snapshot_dir(DATA_END) / f"{v}.parquet")
        assert a[v].equals(b), v


def test_transform_uses_no_future_data(env):
    settings, _ = env
    ts = DATA_END - timedelta(days=20)
    transform(settings, ts)
    before = _read(settings.staging_snapshot_dir(ts) / "customer_activity_7d.parquet")

    # Thêm giao dịch khổng lồ đúng tại và sau snapshot_ts: feature không được đổi.
    raw = settings.raw_dir / "transactions.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * FROM read_parquet({sql_path(raw)}) UNION ALL "
        f"SELECT txn_id + 10000000, TIMESTAMP '{ts:%Y-%m-%d} 00:00:00' + INTERVAL (i) HOUR, client_id, card_id, 1e6, channel, "
        f"merchant_id, merchant_city, merchant_state, mcc, errors FROM read_parquet({sql_path(raw)}), range(3) r(i) "
        f"WHERE txn_id < 50) TO {sql_path(raw.with_suffix('.tmp'))} (FORMAT parquet)"
    )
    con.close()
    raw.with_suffix(".tmp").replace(raw)
    transform(settings, ts)
    after = _read(settings.staging_snapshot_dir(ts) / "customer_activity_7d.parquet")
    assert before.equals(after)
    assert after["amount_max_7d"].max() < 1e6


def test_publish_twice_same_snapshot_no_duplicates(env):
    settings, _ = env
    for ts in (DATA_END - timedelta(days=1), DATA_END):
        transform(settings, ts)
        merge_into_published(settings, ts, settings.staging_snapshot_dir(ts))
    counts1 = {v: pq.read_metadata(settings.published_dir / f"{v}.parquet").num_rows for v in settings.views}
    # chạy lại đúng mốc cuối
    transform(settings, DATA_END)
    merge_into_published(settings, DATA_END, settings.staging_snapshot_dir(DATA_END))
    counts2 = {v: pq.read_metadata(settings.published_dir / f"{v}.parquet").num_rows for v in settings.views}
    assert counts1 == counts2
    for v in settings.views:
        dup = (
            duckdb.connect()
            .execute(
                f"SELECT count(*) - count(DISTINCT (client_id, event_timestamp)) "
                f"FROM read_parquet({sql_path(settings.published_dir / f'{v}.parquet')})"
            )
            .fetchone()[0]
        )
        assert dup == 0


def test_feast_schema_matches_contract(env):
    settings, _ = env
    sys.path.insert(0, str(REPO / "feature_repo"))
    try:
        fv = importlib.import_module("feature_views")
    finally:
        sys.path.pop(0)
    from feast.types import Float64, Int64, String

    mapping = {Int64: "int64", Float64: "double", String: "string"}
    for view, spec in settings.views.items():
        obj = getattr(fv, view)
        feast_schema = {f.name: mapping[f.dtype] for f in obj.schema if f.name != "client_id"}
        contract = {k: v["type"] for k, v in spec["features"].items()}
        assert feast_schema == contract, view
        assert obj.tags["contract_version"] == str(spec["contract_version"])
        assert obj.ttl == timedelta(days=spec["ttl_days"])


def test_replay_clock(env):
    settings, _ = env
    assert f"{settings.snapshot_from_logical('2026-11-27'):%Y-%m-%d}" == "2019-10-01"
    assert f"{settings.snapshot_from_logical('2026-10-28'):%Y-%m-%d}" == "2019-09-01"

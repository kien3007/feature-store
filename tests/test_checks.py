"""Mỗi check loại blocker phải chặn đúng snapshot lỗi; advisory chỉ cảnh báo."""

from __future__ import annotations

import os

import numpy as np
import pytest

from fsmon.checks import FAIL, PASS, WARN, grade
from fsmon.checks.drift import mean_shift_z, psi
from fsmon.db import connect, sql_path
from fsmon.faults import inject
from fsmon.runner import decide, step_audit, step_transform
from fsmon.store import Metric


def _audit_with(settings, store, ts, fault=None, **kw):
    step_transform(settings, store, ts, trigger="test")
    if fault:
        inject(settings, store, ts, fault, **kw)
    return step_audit(settings, store, ts)


def _failed(store, settings, ts, metric):
    run = store.latest_run_for(ts)
    return [m for m in store.metrics_for_run(run["run_id"]) if m["metric_name"] == metric and m["status"] == FAIL]


# ---------- đơn vị ----------


def test_grade_directions():
    assert grade(0.3, 0.2, 0.5) == WARN
    assert grade(0.6, 0.2, 0.5) == FAIL
    assert grade(0.2, 0.2, 0.5) == PASS  # đúng bằng ngưỡng chưa vi phạm
    assert grade(0.95, 0.98, 0.90, lower_is_worse=True) == WARN
    assert grade(0.85, 0.98, 0.90, lower_is_worse=True) == FAIL


def test_psi_identical_is_zero_and_shift_is_large():
    rng = np.random.default_rng(0)
    base = rng.normal(100, 20, 5000)
    assert psi(base, base) < 1e-9
    assert psi(base, rng.normal(100, 20, 5000)) < 0.1
    assert psi(base, base * 3) > 0.25


def test_psi_null_bin_counts_missing():
    base = np.arange(1000, dtype=float)
    cur = base.copy()
    cur[:300] = np.nan
    assert psi(base, cur) > 0.25


def test_mean_shift_z():
    base = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    assert mean_shift_z(base, base) == 0
    assert mean_shift_z(base, base + 10) > 5


def test_decide_blocker_vs_advisory():
    adv = Metric("v", "f", "drift", "psi", 0.9, FAIL, "advisory")
    blk = Metric("v", "f", "value", "null_rate", 0.9, FAIL, "blocker")
    ok = Metric("v", "f", "value", "null_rate", 0.0, PASS, "blocker")
    assert decide([ok]).health == "pass"
    assert decide([ok, adv]).health == "warn"
    assert decide([ok, adv, blk]).health == "fail"


# ---------- blocker tuyệt đối (không cần baseline) ----------


def test_clean_snapshot_passes_cold_start(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END)
    assert d.health == "pass", d.reasons


def test_schema_drop_blocks(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END, "schema_drop")
    assert d.health == "fail"
    assert _failed(store, settings, DATA_END, "schema_match")


def test_schema_type_change_blocks(env):
    settings, store = env
    from tests.conftest import DATA_END

    step_transform(settings, store, DATA_END, trigger="test")
    p = settings.staging_snapshot_dir(DATA_END) / "customer_rfm_90d.parquet"
    con = connect(settings)
    con.execute(
        f"COPY (SELECT * REPLACE (CAST(txn_cnt_90d AS DOUBLE) AS txn_cnt_90d) FROM read_parquet({sql_path(p)})) "
        f"TO {sql_path(p.with_suffix('.tmp'))} (FORMAT parquet)"
    )
    con.close()
    os.replace(p.with_suffix(".tmp"), p)
    d = step_audit(settings, store, DATA_END)
    assert d.health == "fail"
    m = _failed(store, settings, DATA_END, "schema_match")
    assert m and "type_changed=txn_cnt_90d:int64->double" in m[0]["detail"]


def test_extra_column_only_warns(env):
    settings, store = env
    from tests.conftest import DATA_END

    step_transform(settings, store, DATA_END, trigger="test")
    p = settings.staging_snapshot_dir(DATA_END) / "customer_rfm_90d.parquet"
    con = connect(settings)
    con.execute(
        f"COPY (SELECT *, 1 AS surprise FROM read_parquet({sql_path(p)})) "
        f"TO {sql_path(p.with_suffix('.tmp'))} (FORMAT parquet)"
    )
    con.close()
    os.replace(p.with_suffix(".tmp"), p)
    assert step_audit(settings, store, DATA_END).health == "warn"


def test_duplicate_keys_block(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END, "duplicate_keys")
    assert d.health == "fail"
    assert _failed(store, settings, DATA_END, "key_duplicate_rate")


def test_stale_source_blocks(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END, "stale")
    assert d.health == "fail"
    assert _failed(store, settings, DATA_END, "freshness_lag")


def test_out_of_range_blocks(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END, "out_of_range")
    assert d.health == "fail"
    assert _failed(store, settings, DATA_END, "out_of_range_rate")


def test_inf_nan_blocks(env):
    settings, store = env
    from tests.conftest import DATA_END

    step_transform(settings, store, DATA_END, trigger="test")
    p = settings.staging_snapshot_dir(DATA_END) / "customer_activity_30d.parquet"
    con = connect(settings)
    con.execute(
        f"COPY (SELECT * REPLACE (CASE WHEN client_id = 1 THEN 'NaN'::DOUBLE ELSE amount_sum_30d END AS amount_sum_30d) "
        f"FROM read_parquet({sql_path(p)})) TO {sql_path(p.with_suffix('.tmp'))} (FORMAT parquet)"
    )
    con.close()
    os.replace(p.with_suffix(".tmp"), p)
    assert step_audit(settings, store, DATA_END).health == "fail"
    assert _failed(store, settings, DATA_END, "inf_nan_count")


def test_null_in_non_nullable_blocks_without_baseline(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END, "null_spike", view="customer_rfm_90d", feature="avg_ticket_90d")
    assert d.health == "fail"
    assert _failed(store, settings, DATA_END, "null_rate")


# ---------- blocker tương đối (cần baseline) ----------


def test_relative_checks_are_info_during_cold_start(env):
    settings, store = env
    from tests.conftest import DATA_END

    d = _audit_with(settings, store, DATA_END, "null_spike")  # amount_mean_7d nullable
    assert d.health == "pass"
    run = store.latest_run_for(DATA_END)
    m = [
        x
        for x in store.metrics_for_run(run["run_id"])
        if x["metric_name"] == "null_rate" and x["feature"] == "amount_mean_7d"
    ]
    assert m[0]["status"] == "info" and "baseline_incomplete" in m[0]["baseline_ref"]


def test_null_spike_blocks_with_baseline(warm_env):
    settings, store, ts = warm_env
    d = _audit_with(settings, store, ts, "null_spike")
    assert d.health == "fail"
    bad = _failed(store, settings, ts, "null_rate")
    assert [m["feature"] for m in bad] == ["amount_mean_7d"]


def test_row_drop_blocks_with_baseline(warm_env):
    settings, store, ts = warm_env
    d = _audit_with(settings, store, ts, "row_drop")
    assert d.health == "fail"
    assert _failed(store, settings, ts, "row_count_change")
    assert _failed(store, settings, ts, "entity_coverage")


def test_clean_snapshot_passes_with_baseline(warm_env):
    settings, store, ts = warm_env
    d = _audit_with(settings, store, ts)
    assert d.health in ("pass", "warn"), d.reasons
    assert not [r for r in d.reasons if r.startswith("FAIL") and "psi" not in r and "mean_shift" not in r]


def test_shift_is_advisory_only(warm_env):
    settings, store, ts = warm_env
    d = _audit_with(settings, store, ts, "shift")
    assert d.health == "warn", d.reasons
    run = store.latest_run_for(ts)
    psi_m = [
        m
        for m in store.metrics_for_run(run["run_id"])
        if m["metric_name"] == "psi" and m["feature"] == "amount_sum_30d"
    ][0]
    assert psi_m["status"] == FAIL and psi_m["severity"] == "advisory"


def test_quarantined_snapshot_not_in_baseline(warm_env):
    settings, store, ts = warm_env
    from fsmon.runner import step_quarantine

    before = [r["run_id"] for r in store.published_runs(before=ts + __import__("datetime").timedelta(days=5))]
    _audit_with(settings, store, ts, "null_spike")
    step_quarantine(settings, store, ts)
    after = [r["run_id"] for r in store.published_runs(before=ts + __import__("datetime").timedelta(days=5))]
    assert before == after


@pytest.mark.parametrize("fault", ["null_spike", "schema_drop", "duplicate_keys", "stale", "out_of_range", "row_drop"])
def test_every_blocker_fault_is_blocked(warm_env, fault):
    settings, store, ts = warm_env
    assert _audit_with(settings, store, ts, fault).health == "fail"

"""Fixture: repo tạm với dữ liệu tổng hợp nhỏ, kho metrics SQLite."""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsmon.config import load_settings
from fsmon.publish import merge_into_published
from fsmon.runner import step_audit, step_transform
from fsmon.store import MetricsStore, utcnow

REPO = Path(__file__).resolve().parents[1]
N_CLIENTS = 60
DATA_END = datetime(2020, 1, 31)
DATA_START = DATA_END - timedelta(days=150)


def make_raw(raw: Path, seed: int = 7) -> None:
    raw.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    rows = []
    txn_id = 1
    minutes = int((DATA_END - DATA_START).total_seconds() // 60)
    n = N_CLIENTS * 150 * 2
    offs = np.sort(rng.integers(0, minutes, n))
    clients = rng.integers(0, N_CLIENTS, n)
    amounts = np.round(rng.lognormal(3.5, 1.0, n) * np.where(rng.random(n) < 0.05, -1, 1), 2)
    channels = rng.choice(["Swipe Transaction", "Chip Transaction", "Online Transaction"], n, p=[0.5, 0.4, 0.1])
    mccs = rng.choice([5411, 5812, 5541, 4829, 5311, 5912, 5300, 4121, 5814, 7538, 5499, 4784], n)
    errs = np.where(rng.random(n) < 0.02, "Bad PIN", None)
    for i in range(n):
        rows.append(
            (
                txn_id,
                DATA_START + timedelta(minutes=int(offs[i])),
                int(clients[i]),
                int(clients[i]) * 10,
                float(amounts[i]),
                str(channels[i]),
                int(rng.integers(1, 200)),
                "ONLINE" if channels[i] == "Online Transaction" else "Town",
                "CA",
                int(mccs[i]),
                errs[i],
            )
        )
        txn_id += 1
    cols = list(zip(*rows, strict=False))
    pq.write_table(
        pa.table(
            {
                "txn_id": pa.array(cols[0], pa.int64()),
                "event_ts": pa.array(cols[1], pa.timestamp("us")),
                "client_id": pa.array(cols[2], pa.int64()),
                "card_id": pa.array(cols[3], pa.int64()),
                "amount": pa.array(cols[4], pa.float64()),
                "channel": pa.array(cols[5], pa.string()),
                "merchant_id": pa.array(cols[6], pa.int64()),
                "merchant_city": pa.array(cols[7], pa.string()),
                "merchant_state": pa.array(cols[8], pa.string()),
                "mcc": pa.array(cols[9], pa.int32()),
                "errors": pa.array(cols[10], pa.string()),
            }
        ),
        raw / "transactions.parquet",
    )

    ids = list(range(N_CLIENTS))
    pq.write_table(
        pa.table(
            {
                "client_id": pa.array(ids, pa.int64()),
                "current_age": pa.array(rng.integers(20, 80, N_CLIENTS), pa.int32()),
                "retirement_age": pa.array([67] * N_CLIENTS, pa.int32()),
                "birth_year": pa.array([1970] * N_CLIENTS, pa.int32()),
                "birth_month": pa.array([1] * N_CLIENTS, pa.int32()),
                "gender": pa.array(rng.choice(["Male", "Female"], N_CLIENTS), pa.string()),
                "per_capita_income": pa.array(rng.uniform(1e4, 5e4, N_CLIENTS)),
                "yearly_income": pa.array(rng.uniform(2e4, 1e5, N_CLIENTS)),
                "total_debt": pa.array(rng.uniform(0, 1e5, N_CLIENTS)),
                "credit_score": pa.array(rng.integers(500, 850, N_CLIENTS), pa.int32()),
                "num_credit_cards": pa.array(rng.integers(1, 5, N_CLIENTS), pa.int32()),
            }
        ),
        raw / "users.parquet",
    )
    card_owner = [c for c in ids for _ in range(1 + c % 3)]
    n_cards = len(card_owner)
    pq.write_table(
        pa.table(
            {
                "card_id": pa.array([c * 10 + k for k, c in enumerate(card_owner)], pa.int64()),
                "client_id": pa.array(card_owner, pa.int64()),
                "card_brand": pa.array(["Visa"] * n_cards),
                "card_type": pa.array(["Debit"] * n_cards),
                "has_chip": pa.array([True] * n_cards),
                "num_cards_issued": pa.array([1] * n_cards, pa.int32()),
                "credit_limit": pa.array(rng.uniform(1e3, 3e4, n_cards)),
                "acct_open_ts": pa.array([datetime(2010 + k % 9, 1, 1) for k in range(n_cards)], pa.timestamp("us")),
                "expires_ts": pa.array([datetime(2025, 1, 1)] * n_cards, pa.timestamp("us")),
                "year_pin_last_changed": pa.array([2015] * n_cards, pa.int32()),
                "card_on_dark_web": pa.array([False] * n_cards),
            }
        ),
        raw / "cards.parquet",
    )


@pytest.fixture
def env(tmp_path):
    shutil.copytree(REPO / "config", tmp_path / "config")
    shutil.copytree(
        REPO / "feature_repo", tmp_path / "feature_repo", ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
    )
    (tmp_path / "data").mkdir()
    make_raw(tmp_path / "data" / "raw")
    settings = load_settings(tmp_path)
    settings.pipeline["verify"]["sample_size"] = 40
    settings.pipeline["verify"]["latency_samples"] = 5
    store = MetricsStore(f"sqlite:///{(tmp_path / 'metrics.db').as_posix()}")
    store.init_schema()
    return settings, store


def fast_publish(settings, store, ts: datetime) -> str:
    """Publish không qua Feast (cho test check): transform, audit, ghép vào published/."""
    run_id = step_transform(settings, store, ts, trigger="test")
    step_audit(settings, store, ts)
    merge_into_published(settings, ts, settings.staging_snapshot_dir(ts))
    store.update_run(run_id, status="published", published_at=utcnow(), materialized=False)
    return run_id


@pytest.fixture
def warm_env(env):
    """env với 7 snapshot đã publish, đủ baseline cho check tương đối."""
    settings, store = env
    first = DATA_END - timedelta(days=10)
    for i in range(settings.baseline_min):
        fast_publish(settings, store, first + timedelta(days=i))
    return settings, store, first + timedelta(days=settings.baseline_min)

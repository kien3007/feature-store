"""raw -> staging cho một snapshot_ts bằng SQL DuckDB.

Luật: feature của snapshot_ts chỉ dùng bản ghi có event_ts < snapshot_ts.
Mỗi view ghi ra staging/snapshot_ts=YYYY-MM-DD/<view>.parquet, sắp theo client_id
để chạy lại cùng mốc cho cùng kết quả.
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path

from fsmon.config import Settings
from fsmon.db import connect, sql_path

log = logging.getLogger(__name__)

MANIFEST = "manifest.json"


def _ts(dt: datetime) -> str:
    return f"TIMESTAMP '{dt:%Y-%m-%d %H:%M:%S}'"


def view_sql(view: str, raw: Path, T: datetime, created: datetime, active_days: int) -> str:
    """SQL tính một feature view. T là snapshot_ts (naive UTC)."""
    tx = f"read_parquet({sql_path(raw / 'transactions.parquet')})"
    users = f"read_parquet({sql_path(raw / 'users.parquet')})"
    cards = f"read_parquet({sql_path(raw / 'cards.parquet')})"
    t = _ts(T)
    head = f"""
        CAST({t} AS TIMESTAMPTZ)            AS event_timestamp,
        CAST({_ts(created)} AS TIMESTAMPTZ) AS created_timestamp"""
    # Quần thể: khách có giao dịch trong active_days trước T.
    window = f"""
        tx AS (
            SELECT * FROM {tx}
            WHERE event_ts >= {t} - INTERVAL {active_days} DAY AND event_ts < {t}
        ),
        pop AS (SELECT DISTINCT client_id FROM tx)"""

    if view == "customer_profile":
        return f"""
        WITH {window},
        c AS (
            SELECT client_id,
                   count(*)                              AS card_cnt,
                   sum(credit_limit)                     AS total_credit_limit,
                   avg(date_diff('day', acct_open_ts, {t}) / 365.25) AS avg_card_age_years
            FROM {cards}
            WHERE acct_open_ts < {t}
            GROUP BY client_id
        )
        SELECT
            CAST(u.client_id AS BIGINT)                   AS client_id,{head},
            CAST(u.current_age AS BIGINT)                 AS current_age,
            CAST(u.credit_score AS BIGINT)                AS credit_score,
            u.yearly_income                               AS yearly_income,
            u.total_debt                                  AS total_debt,
            CASE WHEN u.yearly_income > 0 THEN u.total_debt / u.yearly_income END AS debt_to_income,
            CAST(u.num_credit_cards AS BIGINT)            AS num_credit_cards,
            CAST(coalesce(c.card_cnt, 0) AS BIGINT)       AS card_cnt,
            CAST(coalesce(c.total_credit_limit, 0) AS DOUBLE) AS total_credit_limit,
            CAST(coalesce(c.avg_card_age_years, 0) AS DOUBLE) AS avg_card_age_years,
            u.gender                                      AS gender
        FROM {users} u
        JOIN pop USING (client_id)
        LEFT JOIN c USING (client_id)
        ORDER BY client_id
        """

    if view in ("customer_activity_7d", "customer_activity_30d"):
        d = 7 if view.endswith("7d") else 30
        s = f"{d}d"
        w = f"""
        w AS (
            SELECT * FROM tx WHERE event_ts >= {t} - INTERVAL {d} DAY
        )"""
        if d == 7:
            return f"""
            WITH {window}, {w},
            agg AS (
                SELECT client_id,
                       count(*)                                     AS txn_cnt,
                       sum(amount)                                  AS amount_sum,
                       avg(amount)                                  AS amount_mean,
                       max(amount)                                  AS amount_max,
                       count(*) FILTER (WHERE amount < 0)           AS refund_cnt,
                       avg(CASE WHEN channel = 'Online Transaction' THEN 1.0 ELSE 0.0 END) AS online_rate,
                       avg(CASE WHEN channel = 'Chip Transaction' THEN 1.0 ELSE 0.0 END)   AS chip_rate,
                       count(*) FILTER (WHERE errors IS NOT NULL)   AS error_cnt,
                       count(DISTINCT merchant_id)                  AS distinct_merchant_cnt
                FROM w GROUP BY client_id
            )
            SELECT
                CAST(p.client_id AS BIGINT)                    AS client_id,{head},
                CAST(coalesce(a.txn_cnt, 0) AS BIGINT)         AS txn_cnt_{s},
                CAST(coalesce(a.amount_sum, 0) AS DOUBLE)      AS amount_sum_{s},
                CAST(a.amount_mean AS DOUBLE)                  AS amount_mean_{s},
                CAST(a.amount_max AS DOUBLE)                   AS amount_max_{s},
                CAST(coalesce(a.refund_cnt, 0) AS BIGINT)      AS refund_cnt_{s},
                CAST(a.online_rate AS DOUBLE)                  AS online_txn_rate_{s},
                CAST(a.chip_rate AS DOUBLE)                    AS chip_txn_rate_{s},
                CAST(coalesce(a.error_cnt, 0) AS BIGINT)       AS error_cnt_{s},
                CAST(coalesce(a.distinct_merchant_cnt, 0) AS BIGINT) AS distinct_merchant_cnt_{s}
            FROM pop p LEFT JOIN agg a USING (client_id)
            ORDER BY client_id
            """
        return f"""
        WITH {window}, {w},
        agg AS (
            SELECT client_id,
                   count(*)                                     AS txn_cnt,
                   sum(amount)                                  AS amount_sum,
                   avg(amount)                                  AS amount_mean,
                   stddev_samp(amount)                          AS amount_std,
                   count(*) FILTER (WHERE amount < 0)           AS refund_cnt,
                   avg(CASE WHEN errors IS NOT NULL THEN 1.0 ELSE 0.0 END) AS error_rate,
                   avg(CASE WHEN channel = 'Online Transaction' THEN 1.0 ELSE 0.0 END) AS online_rate,
                   count(DISTINCT mcc)                          AS distinct_mcc_cnt
            FROM w GROUP BY client_id
        ),
        mcc_cnt AS (
            SELECT client_id, mcc, count(*) AS n FROM w GROUP BY client_id, mcc
        ),
        top_mcc AS (
            -- hòa số lần thì lấy mã mcc nhỏ nhất, để kết quả tất định
            SELECT client_id, arg_max(mcc, n * 100000 - mcc) AS top_mcc
            FROM mcc_cnt GROUP BY client_id
        )
        SELECT
            CAST(p.client_id AS BIGINT)                    AS client_id,{head},
            CAST(coalesce(a.txn_cnt, 0) AS BIGINT)         AS txn_cnt_{s},
            CAST(coalesce(a.amount_sum, 0) AS DOUBLE)      AS amount_sum_{s},
            CAST(a.amount_mean AS DOUBLE)                  AS amount_mean_{s},
            CAST(a.amount_std AS DOUBLE)                   AS amount_std_{s},
            CAST(coalesce(a.refund_cnt, 0) AS BIGINT)      AS refund_cnt_{s},
            CAST(a.error_rate AS DOUBLE)                   AS error_rate_{s},
            CAST(a.online_rate AS DOUBLE)                  AS online_txn_rate_{s},
            CAST(coalesce(a.distinct_mcc_cnt, 0) AS BIGINT) AS distinct_mcc_cnt_{s},
            CAST(m.top_mcc AS BIGINT)                      AS top_mcc_{s}
        FROM pop p
        LEFT JOIN agg a USING (client_id)
        LEFT JOIN top_mcc m USING (client_id)
        ORDER BY client_id
        """

    if view == "customer_rfm_90d":
        return f"""
        WITH {window}
        SELECT
            CAST(client_id AS BIGINT)                                      AS client_id,{head},
            CAST(date_diff('second', max(event_ts), {t}) / 86400.0 AS DOUBLE) AS days_since_last_txn,
            CAST(count(*) AS BIGINT)                                       AS txn_cnt_90d,
            CAST(sum(amount) AS DOUBLE)                                    AS amount_sum_90d,
            CAST(avg(amount) AS DOUBLE)                                    AS avg_ticket_90d,
            CAST(count(DISTINCT CAST(event_ts AS DATE)) AS BIGINT)         AS active_days_90d
        FROM tx
        GROUP BY client_id
        ORDER BY client_id
        """

    raise KeyError(f"không có SQL cho view {view}")


def transform(settings: Settings, snapshot_ts: datetime, run_id: str | None = None) -> dict:
    """Tính mọi view cho snapshot_ts và ghi manifest. Ghi đè staging cũ cùng mốc."""
    out = settings.staging_snapshot_dir(snapshot_ts)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    created = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    con = connect(settings)
    raw = settings.raw_dir

    src_max = con.execute(
        f"SELECT max(event_ts) FROM read_parquet({sql_path(raw / 'transactions.parquet')}) "
        f"WHERE event_ts < {_ts(snapshot_ts)}"
    ).fetchone()[0]

    views: dict[str, dict] = {}
    for view in settings.views:
        target = out / f"{view}.parquet"
        con.execute(
            f"COPY ({view_sql(view, raw, snapshot_ts, created, settings.active_window_days)}) "
            f"TO {sql_path(target)} (FORMAT parquet, COMPRESSION zstd)"
        )
        rows = con.execute(f"SELECT count(*) FROM read_parquet({sql_path(target)})").fetchone()[0]
        views[view] = {"rows": rows, "path": target.name}
        log.info("transform %s %s: %d dòng", snapshot_ts.date(), view, rows)

    manifest = {
        "snapshot_ts": snapshot_ts.isoformat(),
        "run_id": run_id,
        "created_at": created.isoformat(),
        "source_max_event_ts": src_max.isoformat() if src_max else None,
        "views": views,
    }
    write_manifest(out, manifest)
    return manifest


def read_manifest(snapshot_dir: Path) -> dict:
    with open(snapshot_dir / MANIFEST, encoding="utf-8") as f:
        return json.load(f)


def write_manifest(snapshot_dir: Path, manifest: dict) -> None:
    tmp = snapshot_dir / (MANIFEST + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False, default=str)
    tmp.replace(snapshot_dir / MANIFEST)

"""Nạp file gốc (CSV/JSON) vào data/raw dưới dạng Parquet đã chuẩn hóa kiểu.

Chạy một lần (hoặc khi nguồn đổi). Transform đọc từ data/raw, không đọc CSV.
Các cột nhạy cảm (số thẻ, CVV) bị loại ngay ở bước này.
"""

from __future__ import annotations

import json
import logging

import pyarrow as pa
import pyarrow.parquet as pq

from fsmon.config import Settings
from fsmon.db import connect, sql_path

log = logging.getLogger(__name__)


def _money(col: str) -> str:
    return f"CAST(REPLACE(REPLACE({col}, '$', ''), ',', '') AS DOUBLE)"


def ingest(settings: Settings, with_labels: bool = False) -> dict[str, int]:
    src = settings.source_dir
    raw = settings.raw_dir
    raw.mkdir(parents=True, exist_ok=True)
    con = connect(settings)
    counts: dict[str, int] = {}

    tx_csv = src / "transactions_data.csv"
    log.info("ingest transactions từ %s", tx_csv)
    con.execute(
        f"""
        COPY (
            SELECT
                CAST(id AS BIGINT)               AS txn_id,
                CAST(date AS TIMESTAMP)          AS event_ts,
                CAST(client_id AS BIGINT)        AS client_id,
                CAST(card_id AS BIGINT)          AS card_id,
                {_money("amount")}               AS amount,
                use_chip                         AS channel,
                CAST(merchant_id AS BIGINT)      AS merchant_id,
                merchant_city,
                merchant_state,
                CAST(mcc AS INTEGER)             AS mcc,
                errors
            FROM read_csv({sql_path(tx_csv)}, header=true, quote='"', escape='"',
                          types={{'errors': 'VARCHAR', 'merchant_state': 'VARCHAR', 'zip': 'VARCHAR'}})
            ORDER BY event_ts
        ) TO {sql_path(raw / "transactions.parquet")} (FORMAT parquet, ROW_GROUP_SIZE 250000, COMPRESSION zstd)
        """
    )
    counts["transactions"] = con.execute(f"SELECT count(*) FROM {sql_path(raw / 'transactions.parquet')}").fetchone()[0]

    users_csv = src / "users_data.csv"
    con.execute(
        f"""
        COPY (
            SELECT
                CAST(id AS BIGINT)               AS client_id,
                CAST(current_age AS INTEGER)     AS current_age,
                CAST(retirement_age AS INTEGER)  AS retirement_age,
                CAST(birth_year AS INTEGER)      AS birth_year,
                CAST(birth_month AS INTEGER)     AS birth_month,
                gender,
                {_money("per_capita_income")}    AS per_capita_income,
                {_money("yearly_income")}        AS yearly_income,
                {_money("total_debt")}           AS total_debt,
                CAST(credit_score AS INTEGER)    AS credit_score,
                CAST(num_credit_cards AS INTEGER) AS num_credit_cards
            FROM read_csv({sql_path(users_csv)}, header=true, all_varchar=true)
        ) TO {sql_path(raw / "users.parquet")} (FORMAT parquet)
        """
    )
    counts["users"] = con.execute(f"SELECT count(*) FROM {sql_path(raw / 'users.parquet')}").fetchone()[0]

    cards_csv = src / "cards_data.csv"
    con.execute(
        f"""
        COPY (
            SELECT
                CAST(id AS BIGINT)               AS card_id,
                CAST(client_id AS BIGINT)        AS client_id,
                card_brand,
                card_type,
                has_chip = 'YES'                 AS has_chip,
                CAST(num_cards_issued AS INTEGER) AS num_cards_issued,
                {_money("credit_limit")}         AS credit_limit,
                strptime('01/' || acct_open_date, '%d/%m/%Y') AS acct_open_ts,
                strptime('01/' || expires, '%d/%m/%Y')        AS expires_ts,
                CAST(year_pin_last_changed AS INTEGER) AS year_pin_last_changed,
                card_on_dark_web = 'Yes'         AS card_on_dark_web
            FROM read_csv({sql_path(cards_csv)}, header=true, all_varchar=true)
        ) TO {sql_path(raw / "cards.parquet")} (FORMAT parquet)
        """
    )
    counts["cards"] = con.execute(f"SELECT count(*) FROM {sql_path(raw / 'cards.parquet')}").fetchone()[0]

    with open(src / "mcc_codes.json", encoding="utf-8") as f:
        mcc = json.load(f)
    pq.write_table(
        pa.table({"mcc": [int(k) for k in mcc], "mcc_description": list(mcc.values())}),
        raw / "mcc_codes.parquet",
    )
    counts["mcc_codes"] = len(mcc)

    if with_labels:
        with open(src / "train_fraud_labels.json", encoding="utf-8") as f:
            labels = json.load(f)["target"]
        pq.write_table(
            pa.table(
                {
                    "txn_id": pa.array([int(k) for k in labels], pa.int64()),
                    "is_fraud": pa.array([v == "Yes" for v in labels.values()]),
                }
            ),
            raw / "fraud_labels.parquet",
        )
        counts["fraud_labels"] = len(labels)
        del labels

    return counts

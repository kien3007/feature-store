"""Feature view. Kiểu khai báo ở đây phải khớp hợp đồng trong config/checks.yaml
(tests/test_contract.py kiểm). Đổi ý nghĩa feature thì đổi tên hoặc tăng contract_version.

TTL = 3 ngày (gấp 3 chu kỳ hằng ngày) để một snapshot bị bỏ lỡ không làm feature thành null.
"""

from datetime import timedelta

from data_sources import (
    customer_activity_7d_source,
    customer_activity_30d_source,
    customer_profile_source,
    customer_rfm_90d_source,
)
from entities import customer
from feast import FeatureView, Field
from feast.types import Float64, Int64, String

TTL = timedelta(days=3)


def _tags(domain: str, version: int = 1) -> dict[str, str]:
    return {"owner": "risk-data", "domain": domain, "contract_version": str(version)}


customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    ttl=TTL,
    schema=[
        Field(name="current_age", dtype=Int64),
        Field(name="credit_score", dtype=Int64),
        Field(name="yearly_income", dtype=Float64),
        Field(name="total_debt", dtype=Float64),
        Field(name="debt_to_income", dtype=Float64),
        Field(name="num_credit_cards", dtype=Int64),
        Field(name="card_cnt", dtype=Int64),
        Field(name="total_credit_limit", dtype=Float64),
        Field(name="avg_card_age_years", dtype=Float64),
        Field(name="gender", dtype=String),
    ],
    source=customer_profile_source,
    online=True,
    tags=_tags("customer"),
    description="Hồ sơ khách hàng và thẻ, đổi chậm",
)

customer_activity_7d = FeatureView(
    name="customer_activity_7d",
    entities=[customer],
    ttl=TTL,
    schema=[
        Field(name="txn_cnt_7d", dtype=Int64),
        Field(name="amount_sum_7d", dtype=Float64),
        Field(name="amount_mean_7d", dtype=Float64),
        Field(name="amount_max_7d", dtype=Float64),
        Field(name="refund_cnt_7d", dtype=Int64),
        Field(name="online_txn_rate_7d", dtype=Float64),
        Field(name="chip_txn_rate_7d", dtype=Float64),
        Field(name="error_cnt_7d", dtype=Int64),
        Field(name="distinct_merchant_cnt_7d", dtype=Int64),
    ],
    source=customer_activity_7d_source,
    online=True,
    tags=_tags("transactions"),
    description="Hoạt động giao dịch 7 ngày trước snapshot_ts",
)

customer_activity_30d = FeatureView(
    name="customer_activity_30d",
    entities=[customer],
    ttl=TTL,
    schema=[
        Field(name="txn_cnt_30d", dtype=Int64),
        Field(name="amount_sum_30d", dtype=Float64),
        Field(name="amount_mean_30d", dtype=Float64),
        Field(name="amount_std_30d", dtype=Float64),
        Field(name="refund_cnt_30d", dtype=Int64),
        Field(name="error_rate_30d", dtype=Float64),
        Field(name="online_txn_rate_30d", dtype=Float64),
        Field(name="distinct_mcc_cnt_30d", dtype=Int64),
        Field(name="top_mcc_30d", dtype=Int64),
    ],
    source=customer_activity_30d_source,
    online=True,
    tags=_tags("transactions"),
    description="Hoạt động giao dịch 30 ngày trước snapshot_ts",
)

customer_rfm_90d = FeatureView(
    name="customer_rfm_90d",
    entities=[customer],
    ttl=TTL,
    schema=[
        Field(name="days_since_last_txn", dtype=Float64),
        Field(name="txn_cnt_90d", dtype=Int64),
        Field(name="amount_sum_90d", dtype=Float64),
        Field(name="avg_ticket_90d", dtype=Float64),
        Field(name="active_days_90d", dtype=Int64),
    ],
    source=customer_rfm_90d_source,
    online=True,
    tags=_tags("transactions"),
    description="Recency, frequency, monetary 90 ngày",
)

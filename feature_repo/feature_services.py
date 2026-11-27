"""Một feature service cho mỗi mô hình, liệt kê đúng feature mô hình dùng."""

from feast import FeatureService
from feature_views import customer_activity_7d, customer_activity_30d, customer_profile, customer_rfm_90d

customer_risk_v1 = FeatureService(
    name="customer_risk_v1",
    features=[
        customer_profile[["current_age", "credit_score", "debt_to_income", "card_cnt", "total_credit_limit"]],
        customer_activity_7d[["txn_cnt_7d", "amount_sum_7d", "amount_max_7d", "online_txn_rate_7d", "error_cnt_7d"]],
        customer_activity_30d[["txn_cnt_30d", "amount_mean_30d", "amount_std_30d", "error_rate_30d"]],
        customer_rfm_90d[["days_since_last_txn", "txn_cnt_90d", "avg_ticket_90d"]],
    ],
    tags={"owner": "risk-data", "model": "customer_risk"},
    description="Feature cho mô hình rủi ro khách hàng",
)

customer_spend_v1 = FeatureService(
    name="customer_spend_v1",
    features=[customer_activity_30d, customer_rfm_90d],
    tags={"owner": "risk-data", "model": "customer_spend"},
    description="Feature cho mô hình chi tiêu",
)

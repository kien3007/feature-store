"""Batch source: Parquet trong data/published/, chỉ chứa snapshot đã qua cổng chất lượng.

event_timestamp = snapshot_ts, created_timestamp = thời điểm chạy transform.
"""

from feast import FileSource
from feast.data_format import ParquetFormat

VIEWS = ["customer_profile", "customer_activity_7d", "customer_activity_30d", "customer_rfm_90d"]


def _source(view: str) -> FileSource:
    return FileSource(
        name=f"{view}_published",
        path=f"../data/published/{view}.parquet",
        file_format=ParquetFormat(),
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
        description=f"Snapshot đã publish của {view}",
    )


customer_profile_source = _source("customer_profile")
customer_activity_7d_source = _source("customer_activity_7d")
customer_activity_30d_source = _source("customer_activity_30d")
customer_rfm_90d_source = _source("customer_rfm_90d")

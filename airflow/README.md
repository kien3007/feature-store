# Môi trường Airflow

Airflow 3.3.2 chạy trong Docker (LocalExecutor, metadata DB `airflow` trong PostgreSQL của Compose).
Máy phát triển là Windows chưa có distro Ubuntu trong WSL2, mà Airflow không chạy native trên Windows,
nên image `fsmon-airflow` tách hai môi trường:

| Môi trường | Ở đâu | Gồm |
| --- | --- | --- |
| Airflow | Python hệ thống của image `apache/airflow:3.3.2-python3.11` | Airflow + provider chuẩn, theo constraints chính thức |
| Pipeline | `/opt/fsmon-venv` | Cài từ `uv.lock` của repo: Feast, DuckDB, Evidently, fsmon |

DAG chỉ gọi `$FSMON_PYTHON -m fsmon.cli ...` qua BashOperator, nên hai bộ phụ thuộc không bao giờ trộn nhau.
Mã nguồn được mount tại `/opt/project`; sửa code không cần build lại image, chỉ cần build lại khi `uv.lock` đổi.

```bash
make airflow          # build image, migrate DB, chạy api-server, scheduler, dag-processor
make airflow-backfill # backfill 30 snapshot: logical date 2026-10-28..2026-11-26 -> snapshot 2019-09-01..2019-09-30
make airflow-down
```

Giao diện: http://localhost:8080 (SimpleAuthManager, mọi người dùng là admin; chỉ dùng cho local).

Giới hạn tài nguyên: `parallelism=4`, `max_active_runs_per_dag=1`, DuckDB `memory_limit=4GB` trong container;
WSL2 giới hạn bởi `.wslconfig` (máy này đang đặt khoảng 12 GB).

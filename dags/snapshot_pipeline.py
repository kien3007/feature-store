"""DAG mỏng: mỗi task gọi một lệnh fsmon trong môi trường riêng của pipeline.

transform -> audit -> gate ─┬─ publish -> verify ─┐
                            └─ quarantine ────────┴─ report

Logical date của lần chạy được ánh xạ sang snapshot_ts qua đồng hồ phát lại trong
config/pipeline.yaml. Chạy tay một mốc cụ thể: trigger với conf {"snapshot_ts": "2019-10-05"}.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, task
from airflow.task.trigger_rule import TriggerRule

FSMON = f"{os.environ.get('FSMON_PYTHON', 'python')} -m fsmon.cli"
ROOT = os.environ.get("FSMON_ROOT", "/opt/project")

# Tham số mốc dùng chung cho mọi task.
SNAP = (
    "{% if params.snapshot_ts %}--snapshot-ts {{ params.snapshot_ts }}"
    "{% else %}--logical-date {{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}{% endif %}"
)


def fsmon(task_id: str, args: str, **kw) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"cd {ROOT} && {FSMON} {args}", append_env=True, **kw)


with DAG(
    dag_id="snapshot_pipeline",
    description="Snapshot feature hằng ngày với cổng chất lượng (write-audit-publish)",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # publish đúng thứ tự thời gian; backfill không chạy song song
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)},
    params={"snapshot_ts": Param(None, type=["null", "string"], description="Ghi đè snapshot_ts (YYYY-MM-DD)")},
    tags=["feature-store", "fsmon"],
) as dag:
    transform = fsmon("transform", f"transform {SNAP} --trigger airflow --airflow-run-id '{{{{ run_id }}}}'")
    audit = fsmon("audit", f"audit {SNAP}")

    @task.branch(task_id="gate")
    def gate(params=None, dag_run=None) -> str:
        """Đọc quyết định do runner.py đưa ra; không tự quyết."""
        if params and params.get("snapshot_ts"):
            arg = ["--snapshot-ts", params["snapshot_ts"]]
        else:
            ld = dag_run.logical_date or dag_run.run_after
            arg = ["--logical-date", ld.strftime("%Y-%m-%d")]
        out = subprocess.run(
            [os.environ.get("FSMON_PYTHON", "python"), "-m", "fsmon.cli", "decision", *arg],
            cwd=ROOT, capture_output=True, text=True, check=True,
        ).stdout.strip().splitlines()[-1]
        print(f"quyết định cổng: {out}")
        return "quarantine" if out == "fail" else "publish"

    publish = fsmon("publish", f"publish {SNAP}")
    verify = fsmon("verify", f"verify {SNAP}")
    quarantine = fsmon("quarantine", f"quarantine {SNAP}")
    report = fsmon("report", f"report {SNAP}", trigger_rule=TriggerRule.ALL_DONE)

    transform >> audit >> gate() >> [publish, quarantine]
    publish >> verify >> report
    quarantine >> report

"""Lệnh fsmon. Mỗi task của DAG Airflow là một lệnh ở đây.

Mốc snapshot truyền bằng --snapshot-ts YYYY-MM-DD (thời gian dữ liệu) hoặc
--logical-date YYYY-MM-DD (logical date của Airflow, ánh xạ qua đồng hồ phát lại).
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta

import typer

from fsmon.config import Settings, load_settings, parse_snapshot_ts

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Feature store monitor: transform, audit, publish.")

SnapOpt = typer.Option(None, "--snapshot-ts", "-s", help="snapshot_ts (YYYY-MM-DD, thời gian dữ liệu)")
LogicalOpt = typer.Option(None, "--logical-date", "-l", help="logical date Airflow (YYYY-MM-DD)")


def _quiet_libraries() -> None:
    """Ẩn DeprecationWarning của thư viện bên thứ ba (Evidently, Litestar qua Feast)."""
    import warnings

    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", module=r"(evidently|litestar|feast)(\..*)?")


def _setup(verbose: bool = False) -> Settings:
    _quiet_libraries()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for noisy in ("feast", "urllib3", "evidently", "httpx", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return load_settings()


def _store(settings: Settings):
    from fsmon.store import MetricsStore

    return MetricsStore(settings.dsn)


def _snap(settings: Settings, snapshot_ts: str | None, logical_date: str | None) -> datetime:
    if snapshot_ts:
        return parse_snapshot_ts(snapshot_ts)
    if logical_date:
        return settings.snapshot_from_logical(logical_date)
    raise typer.BadParameter("cần --snapshot-ts hoặc --logical-date")


@app.command("init-db")
def init_db():
    """Tạo bảng snapshot_runs và snapshot_metrics nếu chưa có."""
    s = _setup()
    _store(s).init_schema()
    typer.echo(f"ok: {s.dsn.split('@')[-1]}")


@app.command()
def ingest(labels: bool = typer.Option(False, help="nạp cả train_fraud_labels.json")):
    """Nạp file gốc vào data/raw dạng Parquet."""
    from fsmon.ingest import ingest as run

    s = _setup()
    typer.echo(json.dumps(run(s, with_labels=labels), indent=2))


@app.command()
def transform(
    snapshot_ts: str | None = SnapOpt,
    logical_date: str | None = LogicalOpt,
    trigger: str = "manual",
    airflow_run_id: str | None = None,
):
    """raw -> staging cho một snapshot_ts; tạo run mới."""
    from fsmon.runner import step_transform

    s = _setup()
    ts = _snap(s, snapshot_ts, logical_date)
    run_id = step_transform(s, _store(s), ts, trigger=trigger, airflow_run_id=airflow_run_id)
    typer.echo(f"snapshot_ts={ts:%Y-%m-%d} run_id={run_id}")


@app.command()
def audit(snapshot_ts: str | None = SnapOpt, logical_date: str | None = LogicalOpt):
    """Chạy mọi check trên staging, ghi metrics, ra quyết định cổng."""
    from fsmon.runner import step_audit

    s = _setup()
    d = step_audit(s, _store(s), _snap(s, snapshot_ts, logical_date))
    typer.echo(f"health={d.health}")
    for r in d.reasons:
        typer.echo(f"  - {r}")


@app.command()
def decision(snapshot_ts: str | None = SnapOpt, logical_date: str | None = LogicalOpt):
    """In quyết định cổng (pass | warn | fail) của lần audit gần nhất."""
    from fsmon.runner import step_decision

    s = _setup()
    typer.echo(step_decision(s, _snap(s, snapshot_ts, logical_date)))


@app.command()
def quarantine(snapshot_ts: str | None = SnapOpt, logical_date: str | None = LogicalOpt):
    """Chuyển snapshot từ staging sang quarantine và cảnh báo."""
    from fsmon.runner import step_quarantine

    s = _setup()
    step_quarantine(s, _store(s), _snap(s, snapshot_ts, logical_date))


@app.command()
def publish(snapshot_ts: str | None = SnapOpt, logical_date: str | None = LogicalOpt):
    """staging -> published, feast apply nếu cần, materialize."""
    from fsmon.runner import step_publish

    s = _setup()
    typer.echo(json.dumps(step_publish(s, _store(s), _snap(s, snapshot_ts, logical_date)), default=str))


@app.command()
def verify(snapshot_ts: str | None = SnapOpt, logical_date: str | None = LogicalOpt):
    """So khớp online store với published; lệch quá ngưỡng thì khôi phục và thoát mã 1."""
    from fsmon.runner import step_verify

    s = _setup()
    if not step_verify(s, _store(s), _snap(s, snapshot_ts, logical_date)):
        raise typer.Exit(1)


@app.command()
def report(snapshot_ts: str | None = SnapOpt, logical_date: str | None = LogicalOpt):
    """Chốt run: thời gian chạy, trạng thái cuối, báo cáo Evidently nếu warn/fail."""
    from fsmon.runner import step_report

    s = _setup()
    run = step_report(s, _store(s), _snap(s, snapshot_ts, logical_date))
    typer.echo(
        f"{run['snapshot_ts']:%Y-%m-%d} status={run['status']} health={run['health']} "
        f"duration={run['duration_s']:.1f}s report={run.get('report_url') or '-'}"
    )
    # quarantine là kết quả hợp lệ của cổng; chỉ lỗi vận hành mới làm task đỏ
    if run["status"] in ("failed", "failed_after_gate"):
        raise typer.Exit(1)


@app.command()
def run(
    snapshot_ts: str | None = SnapOpt,
    logical_date: str | None = LogicalOpt,
    fault: str | None = typer.Option(None, help="tiêm lỗi trước audit (demo)"),
    fault_view: str | None = None,
    fault_feature: str | None = None,
    trigger: str = "manual",
):
    """Trọn chuỗi cho một snapshot: transform -> audit -> publish/quarantine -> report."""
    from fsmon.runner import run_snapshot

    s = _setup()
    r = run_snapshot(
        s,
        _store(s),
        _snap(s, snapshot_ts, logical_date),
        trigger=trigger,
        fault=fault,
        fault_view=fault_view,
        fault_feature=fault_feature,
    )
    _print_run(r)
    if r["status"] not in ("published", "quarantined"):
        raise typer.Exit(1)


@app.command()
def backfill(
    start: str = typer.Option(..., help="snapshot_ts đầu (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="snapshot_ts cuối, tính cả mốc này"),
    trigger: str = "backfill",
):
    """Chạy tuần tự một dải snapshot_ts, không cần Airflow."""
    from fsmon.runner import run_snapshot

    s = _setup()
    st = _store(s)
    ts, last = parse_snapshot_ts(start), parse_snapshot_ts(end)
    while ts <= last:
        _print_run(run_snapshot(s, st, ts, trigger=trigger))
        ts += timedelta(days=1)


@app.command()
def approve(
    snapshot_ts: str = typer.Option(..., "--snapshot-ts", "-s"),
    reason: str = typer.Option(..., help="lý do duyệt tay"),
    by: str = typer.Option("operator", help="người duyệt"),
):
    """Publish một snapshot đang quarantine và lưu lý do vào snapshot_runs."""
    from fsmon.runner import approve as run_approve

    s = _setup()
    res = run_approve(s, _store(s), parse_snapshot_ts(snapshot_ts), reason=reason, by=by)
    _print_run(res["run"])
    typer.echo(f"materialized={res['materialized']}")


@app.command()
def restore():
    """Dựng lại online store từ snapshot đã publish gần nhất."""
    from fsmon.publish import restore_online

    s = _setup()
    ts = restore_online(s, _store(s))
    typer.echo(f"online store = {ts:%Y-%m-%d}" if ts else "không có snapshot để khôi phục")


@app.command("inject-fault")
def inject_fault(
    fault: str,
    snapshot_ts: str | None = SnapOpt,
    logical_date: str | None = LogicalOpt,
    view: str | None = None,
    feature: str | None = None,
):
    """Tiêm lỗi vào snapshot đang ở staging (chạy sau transform, trước audit)."""
    from fsmon.faults import inject

    s = _setup()
    typer.echo(inject(s, _store(s), _snap(s, snapshot_ts, logical_date), fault, view=view, feature=feature))


@app.command()
def status(limit: int = 15):
    """Các run gần nhất và snapshot đang phục vụ online."""
    s = _setup()
    st = _store(s)
    serving = st.serving_snapshot()
    typer.echo(f"online đang phục vụ: {serving:%Y-%m-%d}" if serving else "online: trống")
    for r in st.runs(limit):
        _print_run(r)


@app.command("snapshot-for")
def snapshot_for(logical_date: str):
    """In snapshot_ts tương ứng một logical date (đồng hồ phát lại)."""
    s = load_settings()
    typer.echo(f"{s.snapshot_from_logical(logical_date):%Y-%m-%d}")


def _print_run(r: dict) -> None:
    dur = f"{r['duration_s']:.1f}s" if r.get("duration_s") else "-"
    extra = []
    if r.get("fault"):
        extra.append(f"fault={r['fault']}")
    if r.get("approved_by"):
        extra.append(f"approved_by={r['approved_by']}")
    typer.echo(
        f"{r['snapshot_ts']:%Y-%m-%d} {r['status']:<18} {str(r.get('health')):<5} {dur:>7} "
        f"{r['run_id'][:8]} {' '.join(extra)}"
    )
    if r.get("reason") and r.get("health") != "pass":
        typer.echo(f"    {r['reason'][:300]}")


def main():
    app()


if __name__ == "__main__":
    main()

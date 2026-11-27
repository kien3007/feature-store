"""Chạy check, ghi metrics, ra quyết định cổng; ghép các bước thành một lần chạy.

Quyết định pass/warn/fail luôn do module này đưa ra; Grafana chỉ hiển thị.
"""

from __future__ import annotations

import logging
import shutil
import statistics
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from fsmon import alert
from fsmon.checks import FAIL, INFO, WARN, ViewContext, grade
from fsmon.checks.drift import drift_checks
from fsmon.checks.schema import key_duplicate_rate, schema_match
from fsmon.checks.values import value_checks
from fsmon.checks.volume import entity_coverage, freshness_lag, row_count
from fsmon.config import Settings
from fsmon.db import connect
from fsmon.publish import publish, remove_from_published, restore_online
from fsmon.store import Metric, MetricsStore, as_utc, utcnow
from fsmon.transform import read_manifest, transform, write_manifest

log = logging.getLogger(__name__)


@dataclass
class Decision:
    health: str  # pass | warn | fail
    reasons: list[str]

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons[:20]) + (f"; (+{len(self.reasons) - 20} nữa)" if len(self.reasons) > 20 else "")


def _describe(m: Metric) -> str:
    th = m.fail_threshold if m.status == FAIL else m.warn_threshold
    where = m.feature_view + (f".{m.feature}" if m.feature else "")
    val = "null" if m.value is None else f"{m.value:.4g}"
    return f"{m.status.upper()} {m.metric_name} {where}={val}" + ("" if th is None else f" (ngưỡng {th:.4g})")


def decide(metrics: list[Metric]) -> Decision:
    blockers = [m for m in metrics if m.severity == "blocker" and m.status == FAIL]
    if blockers:
        return Decision("fail", [_describe(m) for m in blockers])
    warns = [m for m in metrics if m.status in (WARN, FAIL) and m.severity != "info"]
    if warns:
        return Decision("warn", [_describe(m) for m in warns])
    return Decision("pass", [])


# ---------------- tiện ích ----------------


def _record_timing(sdir: Path, step: str, seconds: float) -> None:
    """Cộng thời gian xử lý của một bước vào manifest (không tính thời gian chờ scheduler)."""
    m = read_manifest(sdir)
    m.setdefault("timings", {})[step] = round(seconds, 3)
    write_manifest(sdir, m)


def git_sha(settings: Settings) -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=settings.root, capture_output=True, text=True, check=True
        ).stdout.strip()
    except Exception:
        import os

        return os.environ.get("FSMON_GIT_SHA", "unknown")


def feast_version() -> str:
    try:
        from importlib.metadata import version

        return version("feast")
    except Exception:
        return "unknown"


def snapshot_dir(settings: Settings, snapshot_ts: datetime) -> Path:
    """Thư mục đang giữ snapshot: staging, hoặc quarantine nếu đã bị chặn."""
    st = settings.staging_snapshot_dir(snapshot_ts)
    if (st / "manifest.json").exists():
        return st
    q = settings.quarantine_snapshot_dir(snapshot_ts)
    if (q / "manifest.json").exists():
        return q
    raise FileNotFoundError(f"không tìm thấy snapshot {snapshot_ts:%Y-%m-%d} trong staging/ hay quarantine/")


def current_run_id(settings: Settings, snapshot_ts: datetime) -> str:
    return read_manifest(snapshot_dir(settings, snapshot_ts))["run_id"]


# ---------------- các bước ----------------


def step_transform(
    settings: Settings,
    store: MetricsStore,
    snapshot_ts: datetime,
    trigger: str = "manual",
    airflow_run_id: str | None = None,
) -> str:
    run_id = uuid.uuid4().hex
    store.create_run(
        run_id,
        snapshot_ts,
        trigger=trigger,
        git_sha=git_sha(settings),
        feast_version=feast_version(),
        airflow_run_id=airflow_run_id,
    )
    t0 = time.perf_counter()
    try:
        transform(settings, snapshot_ts, run_id=run_id)
        _record_timing(settings.staging_snapshot_dir(snapshot_ts), "transform", time.perf_counter() - t0)
    except Exception as e:
        store.update_run(run_id, status="failed", reason=f"transform lỗi: {e}", finished_at=utcnow())
        raise
    return run_id


def step_audit(settings: Settings, store: MetricsStore, snapshot_ts: datetime) -> Decision:
    t0 = time.perf_counter()
    sdir = settings.staging_snapshot_dir(snapshot_ts)
    manifest = read_manifest(sdir)
    run_id = manifest["run_id"]
    baseline = store.published_runs(before=snapshot_ts, limit=settings.baseline_window)
    con = connect(settings)
    metrics: list[Metric] = []
    for view in settings.views:
        ctx = ViewContext(settings, store, con, view, snapshot_ts, sdir / f"{view}.parquet", manifest, baseline)
        if not ctx.staging_path.exists():
            metrics.append(ctx.metric("schema", "schema_match", None, FAIL, detail="thiếu file staging"))
            continue
        metrics += schema_match(ctx)
        metrics += key_duplicate_rate(ctx)
        metrics += row_count(ctx)
        metrics += entity_coverage(ctx)
        metrics += freshness_lag(ctx)
        metrics += value_checks(ctx)
        metrics += drift_checks(ctx)
    con.close()

    decision = decide(metrics)
    store.write_metrics(run_id, snapshot_ts, metrics)
    rows = sum(int(m.value or 0) for m in metrics if m.metric_name == "row_count")
    store.update_run(
        run_id,
        status="audited",
        health=decision.health,
        gate_decided_at=utcnow(),
        reason=decision.reason or None,
        row_count=rows,
    )
    manifest["gate"] = {
        "health": decision.health,
        "reasons": decision.reasons,
        "baseline": [f"{r['snapshot_ts']:%Y-%m-%d}" for r in baseline],
    }
    manifest.setdefault("timings", {})["audit"] = round(time.perf_counter() - t0, 3)
    write_manifest(sdir, manifest)
    log.info("audit %s: %s %s", f"{snapshot_ts:%Y-%m-%d}", decision.health, decision.reason)
    return decision


def step_decision(settings: Settings, snapshot_ts: datetime) -> str:
    return read_manifest(snapshot_dir(settings, snapshot_ts))["gate"]["health"]


def step_quarantine(
    settings: Settings, store: MetricsStore, snapshot_ts: datetime, status: str = "quarantined"
) -> None:
    src = settings.staging_snapshot_dir(snapshot_ts)
    manifest = read_manifest(src)
    dst = settings.quarantine_snapshot_dir(snapshot_ts)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(dst)
    shutil.move(str(src), str(dst))
    store.update_run(manifest["run_id"], status=status)
    gate = manifest.get("gate", {})
    alert.emit(
        settings,
        "critical",
        "snapshot_quarantined" if status == "quarantined" else status,
        snapshot_ts,
        manifest["run_id"],
        health=gate.get("health"),
        reason="; ".join(gate.get("reasons", [])[:10]),
    )


def step_publish(settings: Settings, store: MetricsStore, snapshot_ts: datetime) -> dict:
    sdir = settings.staging_snapshot_dir(snapshot_ts)
    run_id = read_manifest(sdir)["run_id"]
    run = store.get_run(run_id)
    if run["health"] == "fail":
        raise RuntimeError(f"snapshot {snapshot_ts:%Y-%m-%d} fail ở cổng, không được publish (dùng approve)")
    t0 = time.perf_counter()
    try:
        res = publish(settings, store, snapshot_ts, run_id)
    except Exception as e:
        _failed_after_gate(settings, store, snapshot_ts, run_id, f"publish lỗi: {e}")
        raise
    _record_timing(sdir, "publish", time.perf_counter() - t0)
    return res


def step_verify(settings: Settings, store: MetricsStore, snapshot_ts: datetime) -> bool:
    from fsmon.verify_online import verify

    run_id = current_run_id(settings, snapshot_ts)
    t0 = time.perf_counter()
    try:
        metrics, ok = verify(settings, store, run_id, snapshot_ts)
    except Exception as e:
        _failed_after_gate(settings, store, snapshot_ts, run_id, f"verify lỗi: {e}")
        raise
    store.write_metrics(run_id, snapshot_ts, metrics)
    _record_timing(snapshot_dir(settings, snapshot_ts), "verify", time.perf_counter() - t0)
    if not ok:
        bad = "; ".join(_describe(m) for m in metrics if m.status == FAIL)
        _failed_after_gate(settings, store, snapshot_ts, run_id, f"online/offline lệch: {bad}")
        return False
    run = store.get_run(run_id)
    if run["health"] == "pass" and any(m.status == WARN for m in metrics):
        store.update_run(run_id, health="warn", reason="; ".join(_describe(m) for m in metrics if m.status == WARN))
    return True


def _failed_after_gate(
    settings: Settings, store: MetricsStore, snapshot_ts: datetime, run_id: str, reason: str
) -> None:
    """Gỡ snapshot khỏi published/, chuyển sang quarantine, khôi phục online từ snapshot tốt trước."""
    log.error("failed_after_gate %s: %s", f"{snapshot_ts:%Y-%m-%d}", reason)
    store.update_run(run_id, status="failed_after_gate", health="fail", materialized=False, reason=reason)
    try:
        remove_from_published(settings, snapshot_ts)
        if (settings.staging_snapshot_dir(snapshot_ts) / "manifest.json").exists():
            step_quarantine(settings, store, snapshot_ts, status="failed_after_gate")
        restored = restore_online(settings, store)
        reason += f"; online khôi phục về {restored:%Y-%m-%d}" if restored else "; không có snapshot tốt để khôi phục"
        store.update_run(run_id, reason=reason)
    finally:
        alert.emit(settings, "critical", "failed_after_gate", snapshot_ts, run_id, reason=reason)


def _env_of(trigger: str | None) -> str:
    return "airflow" if trigger == "airflow" else "local"


def step_report(settings: Settings, store: MetricsStore, snapshot_ts: datetime) -> dict:
    """Bước cuối: run_duration_s, chốt trạng thái, báo cáo Evidently nếu warn/fail, cảnh báo."""
    from fsmon.report import build_reports

    sdir = snapshot_dir(settings, snapshot_ts)
    run_id = read_manifest(sdir)["run_id"]
    run = store.get_run(run_id)
    finished = utcnow()
    duration = (finished - run["started_at"]).total_seconds()

    timings = read_manifest(sdir).get("timings", {})
    processing = round(sum(timings.values()), 3) if timings else duration
    cfg = settings.check_cfg("run_duration_s")
    # Chỉ so với run cùng môi trường: trong container Airflow, I/O qua thư mục mount chậm hơn chạy native nhiều lần.
    env = _env_of(run.get("trigger"))
    prev_ids = [
        r["run_id"]
        for r in store.published_runs(before=snapshot_ts, limit=10 * settings.baseline_window)
        if _env_of(r.get("trigger")) == env
    ][: settings.baseline_window]
    prev = store.metric_values(prev_ids, "__run__", "run_duration_s")
    detail = "; ".join(f"{k}={v:.2f}s" for k, v in timings.items())
    if len(prev) >= settings.baseline_min:
        med = statistics.median(prev)
        warn = max(cfg["warn_ratio"] * med, float(cfg.get("min_warn_s", 0)))
        m = Metric(
            "__run__",
            None,
            "ops",
            "run_duration_s",
            processing,
            grade(processing, warn=warn),
            cfg["severity"],
            warn,
            None,
            baseline_ref=f"median_{len(prev)}:{env}",
            detail=f"median={med:.2f}s; wall={duration:.1f}s; {detail}",
        )
    else:
        m = Metric(
            "__run__",
            None,
            "ops",
            "run_duration_s",
            processing,
            INFO,
            cfg["severity"],
            baseline_ref=f"baseline_incomplete(n={len(prev)})",
            detail=f"wall={duration:.1f}s; {detail}",
        )
    store.write_metrics(run_id, snapshot_ts, [m])

    health = run["health"]
    reason = run.get("reason")
    if m.status == WARN and health == "pass":
        health, reason = "warn", _describe(m)
    updates = {"finished_at": finished, "duration_s": duration, "health": health, "reason": reason}
    if run["status"] in ("running", "audited"):  # không tới được publish hay quarantine: dừng giữa chừng
        updates["status"] = "failed"
    store.update_run(run_id, **updates)
    run = store.get_run(run_id)

    if run["health"] in ("warn", "fail"):
        try:
            url = build_reports(settings, store, run, sdir)
            store.update_run(run_id, report_url=url)
            run["report_url"] = url
        except Exception as e:
            log.warning("không sinh được báo cáo: %s", e)
    if run["status"] == "published" and run["health"] == "warn":
        alert.emit(
            settings,
            "warning",
            "snapshot_published_with_warnings",
            snapshot_ts,
            run_id,
            reason=run.get("reason"),
            report_url=run.get("report_url"),
        )
    return run


def approve(settings: Settings, store: MetricsStore, snapshot_ts: datetime, reason: str, by: str) -> dict:
    """Ghi đè thủ công: publish một snapshot đang quarantine và lưu lý do."""
    q = settings.quarantine_snapshot_dir(snapshot_ts)
    if not (q / "manifest.json").exists():
        raise FileNotFoundError(f"snapshot {snapshot_ts:%Y-%m-%d} không nằm trong quarantine/")
    manifest = read_manifest(q)
    run_id = manifest["run_id"]
    st = settings.staging_snapshot_dir(snapshot_ts)
    if st.exists():
        shutil.rmtree(st)
    shutil.move(str(q), str(st))
    store.update_run(run_id, approved_by=by, approval_reason=reason, approved_at=utcnow())
    result = publish(settings, store, snapshot_ts, run_id)
    step_verify(settings, store, snapshot_ts)
    alert.emit(settings, "warning", "snapshot_approved", snapshot_ts, run_id, by=by, reason=reason)
    run = store.get_run(run_id)
    return {**result, "run": run}


def run_snapshot(
    settings: Settings,
    store: MetricsStore,
    snapshot_ts: datetime,
    trigger: str = "manual",
    fault: str | None = None,
    fault_view: str | None = None,
    fault_feature: str | None = None,
) -> dict:
    """Trọn chuỗi transform -> audit -> cổng -> publish+verify hoặc quarantine -> report."""
    run_id = step_transform(settings, store, snapshot_ts, trigger=trigger)
    if fault:
        from fsmon.faults import inject

        inject(settings, store, snapshot_ts, fault, view=fault_view, feature=fault_feature)
    decision = step_audit(settings, store, snapshot_ts)
    if decision.health == "fail":
        step_quarantine(settings, store, snapshot_ts)
    else:
        step_publish(settings, store, snapshot_ts)
        step_verify(settings, store, snapshot_ts)
    run = step_report(settings, store, snapshot_ts)
    assert run["run_id"] == run_id
    return run


def ts_str(ts: datetime | None) -> str:
    return "-" if ts is None else f"{as_utc(ts):%Y-%m-%d}"

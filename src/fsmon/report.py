"""Báo cáo Evidently HTML cho snapshot warn hoặc fail."""

from __future__ import annotations

import html
import logging
from datetime import datetime
from pathlib import Path

from fsmon.config import Settings
from fsmon.db import connect, sql_path
from fsmon.store import MetricsStore

log = logging.getLogger(__name__)


def _baseline_df(settings: Settings, con, view: str, baseline_runs: list[dict]):
    path = settings.published_dir / f"{view}.parquet"
    if not baseline_runs or not path.exists():
        return None
    ts = ", ".join(f"TIMESTAMPTZ '{r['snapshot_ts']:%Y-%m-%d %H:%M:%S}+00'" for r in baseline_runs)
    return con.execute(
        f"SELECT * FROM read_parquet({sql_path(path)}, hive_partitioning=false) WHERE event_timestamp IN ({ts})"
    ).df()


def _fmt(v) -> str:
    return "" if v is None else f"{v:.6g}"


def build_reports(settings: Settings, store: MetricsStore, run: dict, snapshot_dir: Path) -> str | None:
    """Sinh một HTML Evidently cho mỗi view và một trang index; trả về URL của index."""
    from evidently import DataDefinition, Dataset, Report
    from evidently.presets import DataDriftPreset, DataSummaryPreset

    ts: datetime = run["snapshot_ts"]
    out_rel = Path(f"{ts:%Y-%m-%d}") / run["run_id"][:8]
    out = settings.reports_dir / out_rel
    out.mkdir(parents=True, exist_ok=True)
    con = connect(settings)
    baseline_runs = store.published_runs(before=ts, limit=settings.baseline_window)
    metrics = store.metrics_for_run(run["run_id"])
    links = []

    for view, spec in settings.views.items():
        cur_path = snapshot_dir / f"{view}.parquet"
        if not cur_path.exists():
            continue
        cur = con.execute(f"SELECT * FROM read_parquet({sql_path(cur_path)}, hive_partitioning=false)").df()
        ref = _baseline_df(settings, con, view, baseline_runs)
        feats = [f for f in spec["features"] if f in cur.columns and (ref is None or f in ref.columns)]
        num = [
            f for f in feats if spec["features"][f]["type"] != "string" and not spec["features"][f].get("categorical")
        ]
        cat = [f for f in feats if f not in num]
        definition = DataDefinition(numerical_columns=num, categorical_columns=cat)
        try:
            presets = [DataSummaryPreset()] + ([DataDriftPreset()] if ref is not None else [])
            report = Report(presets)
            cur_ds = Dataset.from_pandas(cur[feats], data_definition=definition)
            ref_ds = Dataset.from_pandas(ref[feats], data_definition=definition) if ref is not None else None
            snap = report.run(current_data=cur_ds, reference_data=ref_ds)
            snap.save_html(str(out / f"{view}.html"))
            links.append((view, f"{view}.html"))
        except Exception as e:  # báo cáo là phụ, không được làm hỏng pipeline
            log.warning("không sinh được báo cáo Evidently cho %s: %s", view, e)

    bad = [m for m in metrics if m["status"] in ("warn", "fail")]
    rows = "".join(
        "<tr>"
        + "".join(
            f"<td{cls}>{html.escape(val)}</td>"
            for cls, val in [
                ("", m["feature_view"]),
                ("", m["feature"] or ""),
                ("", m["metric_name"]),
                ("", _fmt(m["value"])),
                ("", _fmt(m["warn_threshold"])),
                ("", _fmt(m["fail_threshold"])),
                (f" class='{m['status']}'", m["status"]),
                ("", m["severity"]),
                ("", m["detail"] or ""),
            ]
        )
        + "</tr>"
        for m in bad
    )
    view_links = "".join(f"<li><a href='{href}'>{html.escape(v)}</a></li>" for v, href in links)
    (out / "index.html").write_text(
        f"""<!doctype html><html lang="vi"><head><meta charset="utf-8"><title>Snapshot {ts:%Y-%m-%d}</title>
<style>body{{font-family:system-ui,sans-serif;margin:24px;max-width:1100px}}table{{border-collapse:collapse;width:100%}}
td,th{{border:1px solid #ccc;padding:4px 8px;font-size:13px;text-align:left}}.fail{{color:#b00020;font-weight:600}}
.warn{{color:#b26a00;font-weight:600}}</style></head><body>
<h1>Snapshot {ts:%Y-%m-%d}</h1>
<p>run_id <code>{run["run_id"]}</code> · status <b>{html.escape(str(run["status"]))}</b> · health <b>{html.escape(str(run["health"]))}</b>
{(" · fault <code>" + html.escape(run["fault"]) + "</code>") if run.get("fault") else ""}</p>
<p>{html.escape(run.get("reason") or "")}</p>
<h2>Metric warn/fail</h2><table><tr><th>view</th><th>feature</th><th>metric</th><th>value</th><th>warn</th><th>fail</th>
<th>status</th><th>severity</th><th>detail</th></tr>{rows}</table>
<h2>Báo cáo Evidently theo view</h2><ul>{view_links}</ul></body></html>""",
        encoding="utf-8",
    )
    con.close()
    base = settings.pipeline["reports"]["base_url"].rstrip("/")
    return f"{base}/{out_rel.as_posix()}/index.html"

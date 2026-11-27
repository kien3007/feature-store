"""Sinh JSON cho ba dashboard Grafana vào provisioning/dashboards/json/.

Sửa dashboard ở đây rồi chạy `python grafana/build_dashboards.py` và commit cả JSON,
thay vì sửa trên giao diện (provider đặt allowUiUpdates: false).
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "provisioning" / "dashboards" / "json"
DS = {"type": "grafana-postgresql-datasource", "uid": "fsmon-pg"}
# Dữ liệu phát lại nằm trong 2019: đặt khung thời gian mặc định bao trọn giai đoạn đó.
TIME = {"from": "2019-08-25T00:00:00.000Z", "to": "2019-11-03T00:00:00.000Z"}
HEALTH_MAP = [
    {
        "type": "value",
        "options": {
            "0": {"text": "pass", "color": "green", "index": 0},
            "1": {"text": "warn", "color": "yellow", "index": 1},
            "2": {"text": "fail", "color": "red", "index": 2},
        },
    }
]
STATUS_COLORS = [
    {
        "type": "value",
        "options": {
            "pass": {"color": "green", "index": 0},
            "warn": {"color": "yellow", "index": 1},
            "fail": {"color": "red", "index": 2},
            "published": {"color": "green", "index": 3},
            "quarantined": {"color": "red", "index": 4},
            "failed_after_gate": {"color": "purple", "index": 5},
            "failed": {"color": "dark-red", "index": 6},
            "running": {"color": "blue", "index": 7},
        },
    }
]

LATEST_METRICS = "snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)"

_id = 0


def _next_id() -> int:
    global _id
    _id += 1
    return _id


def target(sql: str, fmt: str = "time_series", ref: str = "A") -> dict:
    return {
        "refId": ref,
        "datasource": DS,
        "editorMode": "code",
        "rawQuery": True,
        "format": fmt,
        "rawSql": sql.strip(),
    }


def panel(
    kind: str,
    title: str,
    grid: tuple[int, int, int, int],
    targets: list[dict],
    *,
    desc: str = "",
    defaults: dict | None = None,
    options: dict | None = None,
    overrides: list | None = None,
) -> dict:
    x, y, w, h = grid
    return {
        "id": _next_id(),
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults or {}, "overrides": overrides or []},
        "options": options or {},
    }


def stat(
    title,
    grid,
    sql,
    *,
    unit="none",
    desc="",
    color="thresholds",
    steps=None,
    mappings=None,
    text_mode="auto",
    fields="",
):
    return panel(
        "stat",
        title,
        grid,
        [target(sql, "table")],
        desc=desc,
        defaults={
            "unit": unit,
            "color": {"mode": color},
            "mappings": mappings or [],
            "thresholds": {"mode": "absolute", "steps": steps or [{"color": "blue", "value": None}]},
        },
        options={
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": fields, "values": False},
            "colorMode": "background",
            "graphMode": "none",
            "textMode": text_mode,
            "justifyMode": "center",
        },
    )


def series_with_thresholds(title, grid, metric, desc="", unit="none", log=False, axis=None):
    sql = f"""
SELECT m.snapshot_ts AS time, m.value AS "{metric}", m.warn_threshold AS "warn", m.fail_threshold AS "fail"
FROM {LATEST_METRICS}
WHERE m.feature_view = '$view' AND m.feature = '$feature' AND m.metric_name = '{metric}'
  AND $__timeFilter(m.snapshot_ts)
ORDER BY 1"""
    overrides = [
        {
            "matcher": {"id": "byName", "options": "warn"},
            "properties": [
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "yellow"}},
                {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                {"id": "custom.lineWidth", "value": 1},
                {"id": "custom.showPoints", "value": "never"},
            ],
        },
        {
            "matcher": {"id": "byName", "options": "fail"},
            "properties": [
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}},
                {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                {"id": "custom.lineWidth", "value": 1},
                {"id": "custom.showPoints", "value": "never"},
            ],
        },
        {
            "matcher": {"id": "byName", "options": metric},
            "properties": [
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}},
                {"id": "custom.lineWidth", "value": 2},
            ],
        },
    ]
    custom = {"drawStyle": "line", "showPoints": "always", "pointSize": 5, "spanNulls": True, "lineWidth": 2}
    if log:
        custom["scaleDistribution"] = {"type": "log", "log": 10}
    return panel(
        "timeseries",
        title,
        grid,
        [target(sql)],
        desc=desc,
        defaults={"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}, **(axis or {})},
        overrides=overrides,
        options={"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    )


def dashboard(uid, title, panels, desc, templating=None, links=None):
    return {
        "uid": uid,
        "title": title,
        "description": desc,
        "tags": ["fsmon", "feature-store"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "graphTooltip": 1,
        "time": TIME,
        "refresh": "1m",
        "panels": panels,
        "templating": {"list": templating or []},
        "links": links
        or [
            {
                "title": "fsmon",
                "type": "dashboards",
                "tags": ["fsmon"],
                "asDropdown": False,
                "includeVars": False,
                "keepTime": True,
            }
        ],
    }


# ---------------- Tổng quan ----------------


def overview() -> dict:
    timeline_sql = """
SELECT snapshot_ts AS time, '» snapshot' AS metric,
       CASE health WHEN 'pass' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END AS value
FROM latest_snapshot_runs WHERE $__timeFilter(snapshot_ts)
UNION ALL
SELECT snapshot_ts, feature_view, CASE health WHEN 'pass' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END
FROM view_health WHERE $__timeFilter(snapshot_ts)
ORDER BY 1"""
    p = [
        panel(
            "state-timeline",
            "Sức khỏe snapshot theo feature view",
            (0, 0, 24, 9),
            [target(timeline_sql)],
            desc="Run mới nhất của mỗi snapshot_ts. fail = có check blocker fail (bị chặn); warn = publish kèm cảnh báo.",
            defaults={
                "mappings": HEALTH_MAP,
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
                # ngắt vệt trạng thái sau 25 giờ không có snapshot, thay vì kéo tới hết khung thời gian
                "custom": {"fillOpacity": 85, "lineWidth": 1, "insertNulls": 90000000},
            },
            options={
                "showValue": "never",
                "mergeValues": False,
                "rowHeight": 0.85,
                "alignValue": "center",
                "legend": {"showLegend": False},
                "tooltip": {"mode": "single"},
            },
        ),
        stat(
            "Snapshot đang phục vụ online",
            (0, 9, 6, 4),
            """
SELECT to_char(max(snapshot_ts), 'YYYY-MM-DD') AS snapshot FROM snapshot_runs
WHERE status = 'published' AND materialized""",
            text_mode="value",
            fields="/^snapshot$/",
        ),
        stat(
            "Tuổi snapshot đang phục vụ",
            (6, 9, 6, 4),
            """
SELECT EXTRACT(EPOCH FROM now() - max(published_at)) AS age FROM snapshot_runs
WHERE status = 'published' AND materialized""",
            unit="s",
            desc="Giờ thật kể từ lần materialize gần nhất. Cảnh báo 'Pipeline ngừng publish' bật khi > 36 giờ.",
            steps=[
                {"color": "green", "value": None},
                {"color": "yellow", "value": 86400},
                {"color": "red", "value": 129600},
            ],
        ),
        stat(
            "Snapshot bị chặn",
            (12, 9, 4, 4),
            """
SELECT count(*) FROM latest_snapshot_runs
WHERE status IN ('quarantined', 'failed_after_gate') AND $__timeFilter(snapshot_ts)""",
            steps=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
        ),
        stat(
            "Publish kèm cảnh báo",
            (16, 9, 4, 4),
            """
SELECT count(*) FROM latest_snapshot_runs
WHERE status = 'published' AND health = 'warn' AND $__timeFilter(snapshot_ts)""",
            steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}],
        ),
        stat(
            "Tỷ lệ snapshot pass",
            (20, 9, 4, 4),
            """
SELECT avg(CASE WHEN health = 'pass' THEN 1.0 ELSE 0.0 END) FROM latest_snapshot_runs
WHERE $__timeFilter(snapshot_ts)""",
            unit="percentunit",
            desc="Sau backfill, nếu dưới 80% trên dữ liệu bình thường thì ngưỡng đang quá chặt.",
            steps=[
                {"color": "red", "value": None},
                {"color": "yellow", "value": 0.8},
                {"color": "green", "value": 0.95},
            ],
        ),
        panel(
            "timeseries",
            "Số dòng theo view",
            (0, 13, 12, 7),
            [
                target("""
SELECT m.snapshot_ts AS time, m.feature_view AS metric, m.value
FROM snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)
WHERE m.metric_name = 'row_count' AND $__timeFilter(m.snapshot_ts) ORDER BY 1""")
            ],
            defaults={"custom": {"showPoints": "always", "pointSize": 4}},
        ),
        panel(
            "timeseries",
            "Độ tươi nguồn (giờ)",
            (12, 13, 12, 7),
            [
                target("""
SELECT m.snapshot_ts AS time, max(m.value) AS freshness_lag, max(m.warn_threshold) AS warn,
       max(m.fail_threshold) AS fail
FROM snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)
WHERE m.metric_name = 'freshness_lag' AND $__timeFilter(m.snapshot_ts) GROUP BY 1 ORDER BY 1""")
            ],
            defaults={"unit": "h", "custom": {"showPoints": "always", "pointSize": 4}},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": n},
                    "properties": [
                        {"id": "color", "value": {"mode": "fixed", "fixedColor": c}},
                        {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                    ],
                }
                for n, c in (("warn", "yellow"), ("fail", "red"))
            ],
        ),
        panel(
            "table",
            "10 lần chạy gần nhất",
            (0, 20, 24, 12),
            [
                target(
                    """
SELECT to_char(snapshot_ts, 'YYYY-MM-DD') AS snapshot, status, health, trigger, fault,
       round(duration_s::numeric, 1) AS "duration (s)", reason, report_url AS report, finished_at
FROM snapshot_runs ORDER BY started_at DESC LIMIT 10""",
                    "table",
                )
            ],
            defaults={"mappings": STATUS_COLORS, "custom": {"cellOptions": {"type": "auto"}}},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "status"},
                    "properties": [
                        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                        {"id": "custom.width", "value": 140},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "health"},
                    "properties": [
                        {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                        {"id": "custom.width", "value": 70},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "report"},
                    "properties": [
                        {
                            "id": "links",
                            "value": [{"title": "Báo cáo Evidently", "url": "${__value.raw}", "targetBlank": True}],
                        },
                        {"id": "custom.width", "value": 90},
                    ],
                },
            ],
        ),
    ]
    return dashboard("fsmon-overview", "fsmon · Tổng quan", p, "Snapshot gần đây có khỏe không?")


# ---------------- Chi tiết feature ----------------


def feature_detail() -> dict:
    templating = [
        {
            "name": "view",
            "label": "Feature view",
            "type": "query",
            "datasource": DS,
            "refresh": 1,
            "query": "SELECT DISTINCT feature_view FROM snapshot_metrics WHERE feature_view NOT LIKE '\\_\\_%' ORDER BY 1",
            "definition": "feature views",
            "current": {"text": "customer_activity_7d", "value": "customer_activity_7d"},
        },
        {
            "name": "feature",
            "label": "Feature",
            "type": "query",
            "datasource": DS,
            "refresh": 1,
            "query": "SELECT DISTINCT feature FROM snapshot_metrics WHERE feature_view = '$view' AND feature IS NOT NULL ORDER BY 1",
            "definition": "features of $view",
            "current": {"text": "amount_mean_7d", "value": "amount_mean_7d"},
        },
    ]
    p = [
        series_with_thresholds(
            "null_rate · $view.$feature",
            (0, 0, 12, 8),
            "null_rate",
            desc="Ngưỡng = trung vị null_rate của 7 snapshot đã publish + 5 / +20 điểm phần trăm.",
            unit="percentunit",
            axis={"min": 0, "max": 1},
        ),
        series_with_thresholds(
            "PSI · $view.$feature",
            (12, 0, 12, 8),
            "psi",
            desc="PSI so với baseline gộp 7 snapshot đã publish gần nhất. Advisory: chỉ cảnh báo.",
        ),
        series_with_thresholds(
            "mean_shift_z · $view.$feature",
            (0, 8, 12, 8),
            "mean_shift_z",
            desc="|mean mới − mean baseline| / std baseline (std giữa các entity).",
        ),
        panel(
            "timeseries",
            "Trung bình · $view.$feature",
            (12, 8, 12, 8),
            [
                target(f"""
SELECT m.snapshot_ts AS time, m.value AS mean FROM {LATEST_METRICS}
WHERE m.feature_view = '$view' AND m.feature = '$feature' AND m.metric_name = 'mean'
  AND $__timeFilter(m.snapshot_ts) ORDER BY 1""")
            ],
            defaults={"custom": {"showPoints": "always", "pointSize": 5}},
        ),
        panel(
            "table",
            "PSI cao nhất · snapshot mới nhất trong khoảng",
            (0, 16, 12, 10),
            [
                target(
                    f"""
WITH last AS (SELECT max(snapshot_ts) AS ts FROM latest_snapshot_runs WHERE $__timeFilter(snapshot_ts))
SELECT m.feature_view AS view, m.feature, round(m.value::numeric, 4) AS psi, m.status
FROM {LATEST_METRICS}, last
WHERE m.metric_name = 'psi' AND m.snapshot_ts = last.ts
ORDER BY m.value DESC NULLS LAST LIMIT 15""",
                    "table",
                )
            ],
            defaults={"mappings": STATUS_COLORS},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "status"},
                    "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}}],
                },
                {
                    "matcher": {"id": "byName", "options": "psi"},
                    "properties": [
                        {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "basic"}},
                        {"id": "max", "value": 0.3},
                        {
                            "id": "thresholds",
                            "value": {
                                "mode": "absolute",
                                "steps": [
                                    {"color": "green", "value": None},
                                    {"color": "yellow", "value": 0.1},
                                    {"color": "red", "value": 0.25},
                                ],
                            },
                        },
                    ],
                },
            ],
        ),
        panel(
            "table",
            "Mọi metric của $view.$feature · snapshot mới nhất",
            (12, 16, 12, 10),
            [
                target(
                    f"""
WITH last AS (SELECT max(snapshot_ts) AS ts FROM latest_snapshot_runs WHERE $__timeFilter(snapshot_ts))
SELECT m.metric_name AS metric, m.value, m.warn_threshold AS warn, m.fail_threshold AS fail, m.status,
       m.severity, m.baseline_ref, m.detail
FROM {LATEST_METRICS}, last
WHERE m.feature_view = '$view' AND (m.feature = '$feature' OR m.feature IS NULL) AND m.snapshot_ts = last.ts
ORDER BY m.feature NULLS FIRST, m.metric_name""",
                    "table",
                )
            ],
            defaults={"mappings": STATUS_COLORS},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "status"},
                    "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}}],
                }
            ],
        ),
    ]
    return dashboard("fsmon-feature", "fsmon · Chi tiết feature", p, "Feature nào đang xấu đi?", templating)


# ---------------- Nhật ký chạy ----------------


def run_log() -> dict:
    p = [
        panel(
            "table",
            "snapshot_runs",
            (0, 0, 24, 12),
            [
                target(
                    """
SELECT to_char(snapshot_ts, 'YYYY-MM-DD') AS snapshot, status, health, trigger, fault,
       round(duration_s::numeric, 1) AS "duration (s)", row_count, materialized, reason,
       report_url AS report, approved_by, approval_reason, git_sha, feast_version,
       started_at, finished_at, left(run_id, 8) AS run
FROM snapshot_runs WHERE $__timeFilter(snapshot_ts)
ORDER BY started_at DESC""",
                    "table",
                )
            ],
            defaults={"mappings": STATUS_COLORS},
            options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "status"},
                    "properties": [
                        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                        {"id": "custom.width", "value": 140},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "health"},
                    "properties": [
                        {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                        {"id": "custom.width", "value": 70},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "report"},
                    "properties": [
                        {
                            "id": "links",
                            "value": [{"title": "Báo cáo Evidently", "url": "${__value.raw}", "targetBlank": True}],
                        }
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "reason"},
                    "properties": [{"id": "custom.width", "value": 420}],
                },
            ],
        ),
        panel(
            "table",
            "Check warn/fail (lý do)",
            (0, 12, 14, 11),
            [
                target(
                    """
SELECT to_char(m.snapshot_ts, 'YYYY-MM-DD') AS snapshot, m.feature_view AS view, m.feature, m.metric_name AS metric,
       m.value, m.warn_threshold AS warn, m.fail_threshold AS fail, m.status, m.severity, m.detail
FROM snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)
WHERE m.status IN ('warn', 'fail') AND $__timeFilter(m.snapshot_ts)
ORDER BY m.snapshot_ts DESC, m.status, m.feature_view""",
                    "table",
                )
            ],
            defaults={"mappings": STATUS_COLORS},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "status"},
                    "properties": [{"id": "custom.cellOptions", "value": {"type": "color-background"}}],
                }
            ],
        ),
        panel(
            "table",
            "Lịch sử duyệt tay",
            (14, 12, 10, 11),
            [
                target(
                    """
SELECT to_char(snapshot_ts, 'YYYY-MM-DD') AS snapshot, approved_by, approval_reason, approved_at, status
FROM snapshot_runs WHERE approved_by IS NOT NULL ORDER BY approved_at DESC""",
                    "table",
                )
            ],
        ),
        panel(
            "timeseries",
            "Thời gian xử lý mỗi run (s)",
            (0, 23, 12, 7),
            [
                target("""
SELECT m.snapshot_ts AS time, m.value AS run_duration_s, m.warn_threshold AS warn
FROM snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)
WHERE m.metric_name = 'run_duration_s' AND $__timeFilter(m.snapshot_ts) ORDER BY 1""")
            ],
            defaults={"unit": "s", "custom": {"showPoints": "always", "pointSize": 4}},
        ),
        panel(
            "timeseries",
            "online_match_rate và p95 get_online_features",
            (12, 23, 12, 7),
            [
                target("""
SELECT m.snapshot_ts AS time, min(m.value) AS online_match_rate
FROM snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)
WHERE m.metric_name = 'online_match_rate' AND $__timeFilter(m.snapshot_ts) GROUP BY 1 ORDER BY 1"""),
                target(
                    """
SELECT m.snapshot_ts AS time, m.value AS p95_ms
FROM snapshot_metrics m JOIN latest_snapshot_runs r USING (run_id)
WHERE m.metric_name = 'online_get_p95_ms' AND $__timeFilter(m.snapshot_ts) ORDER BY 1""",
                    ref="B",
                ),
            ],
            defaults={"custom": {"showPoints": "always", "pointSize": 4}},
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "online_match_rate"},
                    "properties": [
                        {"id": "unit", "value": "percentunit"},
                        {"id": "min", "value": 0.98},
                        {"id": "max", "value": 1},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "p95_ms"},
                    "properties": [{"id": "unit", "value": "ms"}, {"id": "custom.axisPlacement", "value": "right"}],
                },
            ],
        ),
    ]
    return dashboard("fsmon-runs", "fsmon · Nhật ký chạy", p, "Chuyện gì xảy ra ở lần chạy này?")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for name, fn in (("overview", overview), ("feature_detail", feature_detail), ("run_log", run_log)):
        (OUT / f"{name}.json").write_text(json.dumps(fn(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print("wrote", OUT / f"{name}.json")

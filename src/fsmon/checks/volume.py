"""row_count, row_count_change, entity_coverage và freshness_lag."""

from __future__ import annotations

from datetime import datetime

from fsmon.checks import INFO, ViewContext, grade
from fsmon.db import sql_path
from fsmon.store import Metric


def row_count(ctx: ViewContext) -> list[Metric]:
    rows = ctx.con.execute(f"SELECT count(*) FROM {ctx.staging_rel}").fetchone()[0]
    out = [ctx.metric("volume", "row_count", rows, INFO)]

    cfg = ctx.cfg("row_count_change")
    med = ctx.store.baseline_median(ctx.baseline_run_ids, ctx.view, "row_count")
    if med:
        change = abs(rows - med) / med
        out.append(
            ctx.metric(
                "volume",
                "row_count_change",
                change,
                grade(change, cfg["warn"], cfg["fail"]),
                warn=cfg["warn"],
                fail=cfg["fail"],
                relative=True,
                detail=f"rows={rows}; baseline_median={med:g}",
            )
        )
    else:
        out.append(
            ctx.metric(
                "volume",
                "row_count_change",
                None,
                INFO,
                warn=cfg["warn"],
                fail=cfg["fail"],
                relative=True,
                detail="no baseline",
            )
        )
    return out


def entity_coverage(ctx: ViewContext) -> list[Metric]:
    cfg = ctx.cfg("entity_coverage")
    if "client_id" not in ctx.usable or not ctx.baseline_runs or not ctx.published_path.exists():
        return [
            ctx.metric(
                "volume",
                "entity_coverage",
                None,
                INFO,
                warn=cfg["warn"],
                fail=cfg["fail"],
                relative=True,
                detail="no previous snapshot",
            )
        ]
    prev = ctx.baseline_runs[0]["snapshot_ts"]
    prev_n, kept = ctx.con.execute(
        f"""
        WITH prev AS (
            SELECT DISTINCT client_id FROM read_parquet({sql_path(ctx.published_path)}, hive_partitioning=false)
            WHERE event_timestamp = TIMESTAMPTZ '{prev:%Y-%m-%d %H:%M:%S}+00'
        ), cur AS (SELECT DISTINCT client_id FROM {ctx.staging_rel})
        SELECT (SELECT count(*) FROM prev), (SELECT count(*) FROM prev JOIN cur USING (client_id))
        """
    ).fetchone()
    if not prev_n:
        return [ctx.metric("volume", "entity_coverage", None, INFO, relative=True, detail="previous snapshot empty")]
    cov = kept / prev_n
    return [
        ctx.metric(
            "volume",
            "entity_coverage",
            cov,
            grade(cov, cfg["warn"], cfg["fail"], lower_is_worse=True),
            warn=cfg["warn"],
            fail=cfg["fail"],
            relative=True,
            detail=f"kept={kept}/{prev_n} vs {prev:%Y-%m-%d}",
        )
    ]


def freshness_lag(ctx: ViewContext) -> list[Metric]:
    if not ctx.contract.get("freshness"):
        return []
    cfg = ctx.cfg("freshness_lag")
    period_h = ctx.settings.period.total_seconds() / 3600
    warn, fail = cfg["warn_periods"] * period_h, cfg["fail_periods"] * period_h
    src_max = ctx.manifest.get("source_max_event_ts")
    if not src_max:
        return [
            ctx.metric(
                "freshness",
                "freshness_lag",
                None,
                "fail",
                warn=warn,
                fail=fail,
                detail="nguồn không có bản ghi nào trước snapshot_ts",
            )
        ]
    lag_h = (ctx.snapshot_ts - datetime.fromisoformat(src_max)).total_seconds() / 3600
    return [
        ctx.metric(
            "freshness",
            "freshness_lag",
            lag_h,
            grade(lag_h, warn, fail),
            warn=warn,
            fail=fail,
            detail=f"source_max_event_ts={src_max}; đơn vị giờ",
        )
    ]

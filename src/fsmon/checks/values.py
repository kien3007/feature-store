"""null_rate, inf_nan_count, out_of_range_rate, distinct_count."""

from __future__ import annotations

from fsmon.checks import INFO, PASS, WARN, ViewContext, grade
from fsmon.store import Metric


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def value_checks(ctx: ViewContext) -> list[Metric]:
    feats = {f: spec for f, spec in ctx.features.items() if f in ctx.usable}
    if not feats:
        return []

    exprs = ["count(*) AS __rows"]
    for f, spec in feats.items():
        c = _q(f)
        exprs.append(f"count(*) FILTER (WHERE {c} IS NULL) AS {_q(f + '__null')}")
        if spec["type"] == "double":
            exprs.append(f"count(*) FILTER (WHERE isnan({c}) OR isinf({c})) AS {_q(f + '__naninf')}")
        rng = spec.get("range")
        if rng:
            conds = []
            if rng[0] is not None:
                conds.append(f"{c} < {rng[0]}")
            if rng[1] is not None:
                conds.append(f"{c} > {rng[1]}")
            if conds:
                exprs.append(f"count(*) FILTER (WHERE {' OR '.join(conds)}) AS {_q(f + '__oor')}")
        if spec.get("categorical") or spec["type"] in ("int64", "string"):
            exprs.append(f"count(DISTINCT {c}) AS {_q(f + '__distinct')}")

    cur = ctx.con.execute(f"SELECT {', '.join(exprs)} FROM {ctx.staging_rel}")
    row = dict(zip([d[0] for d in cur.description], cur.fetchone(), strict=False))
    n = row["__rows"] or 0
    out: list[Metric] = []

    null_cfg = ctx.cfg("null_rate")
    oor_cfg = ctx.cfg("out_of_range_rate")
    nan_cfg = ctx.cfg("inf_nan_count")
    dist_cfg = ctx.cfg("distinct_count")

    for f, spec in feats.items():
        rate = row[f + "__null"] / n if n else 0.0
        if not spec.get("nullable", True):
            # Hợp đồng nói không được null: áp ngưỡng tuyệt đối, không cần baseline.
            out.append(
                ctx.metric(
                    "value", "null_rate", rate, grade(rate, fail=0.0), feature=f, fail=0.0, detail="nullable=false"
                )
            )
        else:
            base = ctx.store.baseline_median(ctx.baseline_run_ids, ctx.view, "null_rate", f)
            if base is None:
                out.append(ctx.metric("value", "null_rate", rate, INFO, feature=f, relative=True, detail="no baseline"))
            else:
                warn = base + null_cfg["warn_delta"]
                fail = base + null_cfg["fail_delta"]
                out.append(
                    ctx.metric(
                        "value",
                        "null_rate",
                        rate,
                        grade(rate, warn, fail),
                        feature=f,
                        warn=warn,
                        fail=fail,
                        relative=True,
                        detail=f"baseline_null_rate={base:.4f}; delta_pp={(rate - base) * 100:+.2f}",
                    )
                )

        if spec["type"] == "double":
            cnt = row[f + "__naninf"]
            out.append(
                ctx.metric(
                    "value", "inf_nan_count", cnt, grade(cnt, fail=nan_cfg["fail"]), feature=f, fail=nan_cfg["fail"]
                )
            )

        if f + "__oor" in row:
            oor = row[f + "__oor"] / n if n else 0.0
            out.append(
                ctx.metric(
                    "value",
                    "out_of_range_rate",
                    oor,
                    grade(oor, oor_cfg["warn"], oor_cfg["fail"]),
                    feature=f,
                    warn=oor_cfg["warn"],
                    fail=oor_cfg["fail"],
                    detail=f"range={spec['range']}",
                )
            )

        if f + "__distinct" in row:
            d = row[f + "__distinct"]
            # Feature thành hằng số: distinct <= 1 thì warn.
            status = WARN if n and d <= dist_cfg["warn"] else PASS
            out.append(ctx.metric("value", "distinct_count", d, status, feature=f, warn=dist_cfg["warn"]))
    return out

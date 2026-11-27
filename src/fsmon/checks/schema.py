"""schema_match và key_duplicate_rate."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from fsmon.checks import FAIL, PASS, WARN, ViewContext, grade
from fsmon.store import Metric


def arrow_to_contract(t: pa.DataType) -> str:
    if pa.types.is_int64(t):
        return "int64"
    if pa.types.is_integer(t):
        return f"int{t.bit_width}"
    if pa.types.is_floating(t):
        return "double" if t.bit_width == 64 else f"float{t.bit_width}"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_boolean(t):
        return "bool"
    return str(t)


def expected_columns(ctx: ViewContext) -> dict[str, str]:
    cols = dict(ctx.settings.checks["system_columns"])
    cols.update({name: spec["type"] for name, spec in ctx.features.items()})
    return cols


def schema_match(ctx: ViewContext) -> list[Metric]:
    expected = expected_columns(ctx)
    actual = {f.name: arrow_to_contract(f.type) for f in pq.read_schema(ctx.staging_path)}
    missing = [c for c in expected if c not in actual]
    changed = [f"{c}:{expected[c]}->{actual[c]}" for c in expected if c in actual and actual[c] != expected[c]]
    extra = [c for c in actual if c not in expected]
    ctx.usable = {c: t for c, t in expected.items() if c in actual and actual[c] == t}

    if missing or changed:
        status = FAIL
    elif extra:
        status = WARN
    else:
        status = PASS
    parts = []
    if missing:
        parts.append("missing=" + ",".join(missing))
    if changed:
        parts.append("type_changed=" + ",".join(changed))
    if extra:
        parts.append("extra=" + ",".join(extra))
    # value = số vi phạm; 0 là khớp hoàn toàn
    problems = len(missing) + len(changed) + len(extra)
    return [ctx.metric("schema", "schema_match", problems, status, warn=0, fail=0, detail="; ".join(parts) or None)]


def key_duplicate_rate(ctx: ViewContext) -> list[Metric]:
    if "client_id" not in ctx.usable:
        return []
    rows, distinct = ctx.con.execute(
        f"SELECT count(*), count(DISTINCT (client_id, event_timestamp)) FROM {ctx.staging_rel}"
    ).fetchone()
    rate = 0.0 if rows == 0 else (rows - distinct) / rows
    th = ctx.cfg("key_duplicate_rate")["fail"]
    return [
        ctx.metric(
            "schema",
            "key_duplicate_rate",
            rate,
            grade(rate, fail=th),
            fail=th,
            detail=f"rows={rows}; distinct_keys={distinct}",
        )
    ]

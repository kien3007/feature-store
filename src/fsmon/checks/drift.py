"""PSI và mean_shift_z so với baseline gộp từ các snapshot đã publish."""

from __future__ import annotations

import math

import numpy as np

from fsmon.checks import INFO, ViewContext, grade, numeric
from fsmon.store import Metric

EPS = 1e-4


def psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """PSI = sum (a_i - e_i) * ln(a_i / e_i).

    Bin theo phân vị của baseline (bỏ cạnh trùng), cộng một bin riêng cho null/NaN.
    Tỷ lệ rỗng được cộng EPS để tránh chia cho 0.
    """
    expected = np.asarray(expected, dtype="float64")
    actual = np.asarray(actual, dtype="float64")
    e_fin, a_fin = expected[np.isfinite(expected)], actual[np.isfinite(actual)]
    if len(expected) == 0 or len(actual) == 0:
        return float("nan")
    if len(e_fin):
        edges = np.unique(np.quantile(e_fin, np.linspace(0, 1, bins + 1)[1:-1]))
    else:
        edges = np.array([])
    e_counts = np.bincount(np.searchsorted(edges, e_fin, side="right"), minlength=len(edges) + 1)
    a_counts = np.bincount(np.searchsorted(edges, a_fin, side="right"), minlength=len(edges) + 1)
    e = np.append(e_counts, len(expected) - len(e_fin)) / len(expected)
    a = np.append(a_counts, len(actual) - len(a_fin)) / len(actual)
    e = np.where(e == 0, EPS, e)
    a = np.where(a == 0, EPS, a)
    return float(np.sum((a - e) * np.log(a / e)))


def mean_shift_z(expected: np.ndarray, actual: np.ndarray) -> float | None:
    e = np.asarray(expected, dtype="float64")
    a = np.asarray(actual, dtype="float64")
    e, a = e[np.isfinite(e)], a[np.isfinite(a)]
    if len(e) < 2 or len(a) == 0:
        return None
    sd = float(np.std(e, ddof=1))
    diff = abs(float(np.mean(a)) - float(np.mean(e)))
    if sd == 0:
        return 0.0 if diff == 0 else math.inf
    return diff / sd


def _col(ctx: ViewContext, rel: str, f: str) -> np.ndarray:
    res = ctx.con.execute(f'SELECT CAST("{f}" AS DOUBLE) AS v FROM {rel}').fetchnumpy()["v"]
    # Giá trị null trả về dạng masked array: đổi thành NaN để vào bin null.
    return np.ma.filled(np.ma.asarray(res, dtype="float64"), np.nan)


def drift_checks(ctx: ViewContext) -> list[Metric]:
    feats = [
        f
        for f, spec in ctx.features.items()
        if f in ctx.usable and numeric(spec["type"]) and not spec.get("categorical") and spec.get("drift", True)
    ]
    if not feats:
        return []
    psi_cfg, z_cfg = ctx.cfg("psi"), ctx.cfg("mean_shift_z")
    base_rel = ctx.baseline_rel()
    out: list[Metric] = []
    if base_rel is None:
        for f in feats:
            out.append(
                ctx.metric(
                    "drift",
                    "psi",
                    None,
                    INFO,
                    feature=f,
                    warn=psi_cfg["warn"],
                    fail=psi_cfg["fail"],
                    relative=True,
                    detail="no baseline",
                )
            )
            out.append(
                ctx.metric(
                    "drift",
                    "mean_shift_z",
                    None,
                    INFO,
                    feature=f,
                    warn=z_cfg["warn"],
                    fail=z_cfg["fail"],
                    relative=True,
                    detail="no baseline",
                )
            )
        return out

    for f in feats:
        e, a = _col(ctx, base_rel, f), _col(ctx, ctx.staging_rel, f)
        p = psi(e, a, int(psi_cfg.get("bins", 10)))
        p_val = None if math.isnan(p) else p
        out.append(
            ctx.metric(
                "drift",
                "psi",
                p_val,
                grade(p_val, psi_cfg["warn"], psi_cfg["fail"]),
                feature=f,
                warn=psi_cfg["warn"],
                fail=psi_cfg["fail"],
                relative=True,
            )
        )
        z = mean_shift_z(e, a)
        z_val = None if z is None else (1e9 if math.isinf(z) else z)
        mean_a = float(np.nanmean(a)) if np.isfinite(a).any() else None
        out.append(
            ctx.metric(
                "drift",
                "mean_shift_z",
                z_val,
                grade(z_val, z_cfg["warn"], z_cfg["fail"]),
                feature=f,
                warn=z_cfg["warn"],
                fail=z_cfg["fail"],
                relative=True,
                detail=None if mean_a is None else f"mean={mean_a:.6g}",
            )
        )
        if mean_a is not None:
            out.append(ctx.metric("drift", "mean", mean_a, INFO, feature=f))
    return out

"""Ghi alerts.log (JSON lines): dấu vết cảnh báo kể cả khi Grafana tắt."""

from __future__ import annotations

import json
import logging
from datetime import datetime

from fsmon.config import Settings
from fsmon.store import utcnow

log = logging.getLogger(__name__)


def emit(settings: Settings, level: str, event: str, snapshot_ts: datetime, run_id: str, **details) -> dict:
    rec = {
        "at": utcnow().isoformat(),
        "level": level,
        "event": event,
        "snapshot_ts": f"{snapshot_ts:%Y-%m-%d}",
        "run_id": run_id,
        **details,
    }
    settings.alerts_log.parent.mkdir(parents=True, exist_ok=True)
    with open(settings.alerts_log, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    (log.error if level == "critical" else log.warning)("ALERT %s %s %s", level, event, rec.get("reason", ""))
    return rec

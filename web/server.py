"""Server nhỏ chạy trong Compose, chỉ dùng thư viện chuẩn.

GET  /reports/...  phục vụ báo cáo Evidently trong data/reports
POST /alert        đích webhook của Grafana contact point; ghi vào data/alerts_received.log
GET  /alerts       50 thông báo nhận gần nhất (JSON)
"""

import json
import os
from datetime import datetime, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPORTS = Path(os.environ.get("REPORTS_DIR", "/data/reports"))
ALERTS = Path(os.environ.get("ALERTS_FILE", "/data/alerts_received.log"))


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        path = path.split("?", 1)[0]
        if path.startswith("/reports"):
            rel = path[len("/reports"):].lstrip("/")
            target = (REPORTS / rel).resolve()
            if REPORTS.resolve() in target.parents or target == REPORTS.resolve():
                return str(target)
        return str(REPORTS / "__not_found__")

    def do_GET(self):
        if self.path.startswith("/alerts"):
            lines = ALERTS.read_text(encoding="utf-8").splitlines()[-50:] if ALERTS.exists() else []
            return self._json([json.loads(x) for x in lines])
        if self.path in ("/", "/health"):
            return self._json({"ok": True})
        return super().do_GET()

    def do_POST(self):
        if not self.path.startswith("/alert"):
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0)
        try:
            payload = json.loads(body or b"{}")
        except ValueError:
            payload = {"raw": body.decode("utf-8", "replace")}
        rec = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "status": payload.get("status"),
            "title": payload.get("title"),
            "alerts": [
                {"name": a.get("labels", {}).get("alertname"), "status": a.get("status"),
                 "summary": a.get("annotations", {}).get("summary"), "values": a.get("values")}
                for a in payload.get("alerts", [])
            ],
        }
        ALERTS.parent.mkdir(parents=True, exist_ok=True)
        with open(ALERTS, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print("ALERT", json.dumps(rec, ensure_ascii=False), flush=True)
        self._json({"ok": True})

    def _json(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    REPORTS.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("PORT", "8088"))
    print(f"fsmon-web: :{port} reports={REPORTS} alerts={ALERTS}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), partial(Handler, directory=str(REPORTS))).serve_forever()

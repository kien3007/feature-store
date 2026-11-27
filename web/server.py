"""Server nhỏ chạy trong Compose, chỉ dùng thư viện chuẩn.

GET  /reports/...  phục vụ báo cáo Evidently trong data/reports
POST /alert        đích webhook của Grafana contact point; ghi vào data/alerts_received.log
GET  /alerts       50 thông báo nhận gần nhất (JSON)
GET  /alerts.html  cùng dữ liệu, dạng bảng để xem trên trình duyệt
"""

import html
import json
import os
from datetime import UTC, datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPORTS = Path(os.environ.get("REPORTS_DIR", "/data/reports"))
ALERTS = Path(os.environ.get("ALERTS_FILE", "/data/alerts_received.log"))


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        path = path.split("?", 1)[0]
        if path.startswith("/reports"):
            rel = path[len("/reports") :].lstrip("/")
            target = (REPORTS / rel).resolve()
            if REPORTS.resolve() in target.parents or target == REPORTS.resolve():
                return str(target)
        return str(REPORTS / "__not_found__")

    def do_GET(self):
        if self.path.startswith("/alerts"):
            lines = ALERTS.read_text(encoding="utf-8").splitlines()[-50:] if ALERTS.exists() else []
            recs = [json.loads(x) for x in lines]
            if self.path.startswith("/alerts.html"):
                return self._html(alerts_page(recs))
            return self._json(recs)
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
            "received_at": datetime.now(UTC).isoformat(),
            "status": payload.get("status"),
            "title": payload.get("title"),
            "alerts": [
                {
                    "name": a.get("labels", {}).get("alertname"),
                    "status": a.get("status"),
                    "summary": a.get("annotations", {}).get("summary"),
                    "values": a.get("values"),
                }
                for a in payload.get("alerts", [])
            ],
        }
        ALERTS.parent.mkdir(parents=True, exist_ok=True)
        with open(ALERTS, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print("ALERT", json.dumps(rec, ensure_ascii=False), flush=True)
        self._json({"ok": True})

    def _html(self, page: str):
        data = page.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def alerts_page(recs: list) -> str:
    rows = []
    for r in reversed(recs):
        for a in r.get("alerts") or [{}]:
            status = a.get("status") or r.get("status") or ""
            rows.append(
                f"<tr><td>{html.escape(r.get('received_at', '')[:19].replace('T', ' '))} UTC</td>"
                f"<td class='{html.escape(status)}'>{html.escape(status)}</td>"
                f"<td>{html.escape(a.get('name') or '')}</td><td>{html.escape(a.get('summary') or '')}</td></tr>"
            )
    return f"""<!doctype html><html lang="vi"><head><meta charset="utf-8"><title>Cảnh báo đã nhận</title>
<style>body{{font-family:system-ui,sans-serif;margin:24px;color:#1f2328;background:#fff}}
table{{border-collapse:collapse;width:100%}}th,td{{border-bottom:1px solid #d0d7de;padding:8px 10px;text-align:left;font-size:14px}}
th{{background:#f6f8fa}}.firing{{color:#cf222e;font-weight:600}}.resolved{{color:#1a7f37;font-weight:600}}</style></head>
<body><h1>Cảnh báo Grafana đã nhận qua webhook</h1>
<p>Contact point <code>fsmon-webhook</code> gửi tới <code>POST /alert</code>; {len(rows)} thông báo gần nhất, mới nhất ở trên.</p>
<table><tr><th>Nhận lúc</th><th>Trạng thái</th><th>Alert</th><th>Nội dung</th></tr>{"".join(rows)}</table></body></html>"""


if __name__ == "__main__":
    REPORTS.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("PORT", "8088"))
    print(f"fsmon-web: :{port} reports={REPORTS} alerts={ALERTS}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), partial(Handler, directory=str(REPORTS))).serve_forever()

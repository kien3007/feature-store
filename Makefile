# Lệnh chính. Chạy được trong Git Bash trên Windows hoặc shell Linux/WSL.
#   make up               PostgreSQL + Grafana + fsmon-web, tạo bảng metrics
#   make ingest           CSV/JSON trong dataset/ -> data/raw/*.parquet
#   make snapshot         trọn chuỗi cho snapshot của hôm nay (đồng hồ phát lại) hoặc SNAPSHOT=YYYY-MM-DD
#   make backfill         30 snapshot 2019-09-01..2019-09-30 (không cần Airflow)
#   make airflow          Airflow 3 trong Docker; make airflow-backfill cho backfill qua Airflow
#   make demo             kịch bản demo 5 phút
#   make demo-fault FAULT=null_spike [SNAPSHOT=...]
#   make approve SNAPSHOT=2019-10-07 REASON="..."

SHELL := bash
UV ?= uv
FSMON := $(UV) run fsmon
COMPOSE := docker compose
TODAY := $(shell date -u +%Y-%m-%d)
SNAPSHOT ?=
SNAP_ARG = $(if $(SNAPSHOT),--snapshot-ts $(SNAPSHOT),--logical-date $(TODAY))
START ?= 2019-09-01
END ?= 2019-09-30
FAULT ?= null_spike
REASON ?=
BY ?= $(USER)$(USERNAME)

.PHONY: help install up down ps logs ingest init-db snapshot transform check publish verify report backfill \
        airflow airflow-down airflow-backfill airflow-trigger demo demo-fault approve restore status \
        test test-fast lint fmt plan apply dashboards clean-state

help:
	@sed -n '1,12p' Makefile

install:
	$(UV) sync

# ---------- hạ tầng ----------
up:
	$(COMPOSE) up -d postgres grafana fsmon-web
	@echo "chờ PostgreSQL..."; until $(COMPOSE) exec -T postgres pg_isready -U fsmon -d fsmon >/dev/null 2>&1; do sleep 1; done
	$(FSMON) init-db
	@echo "Grafana http://localhost:3000 (admin/admin) · báo cáo http://localhost:8088/reports/ · cảnh báo http://localhost:8088/alerts"

down:
	$(COMPOSE) --profile airflow down

ps:
	$(COMPOSE) --profile airflow ps

logs:
	$(COMPOSE) --profile airflow logs -f --tail 100

init-db:
	$(FSMON) init-db

ingest:
	$(FSMON) ingest

# ---------- pipeline (gọi tay) ----------
snapshot:
	$(FSMON) run $(SNAP_ARG)

transform:
	$(FSMON) transform $(SNAP_ARG)

check:
	$(FSMON) audit $(SNAP_ARG)

publish:
	@test "$$($(FSMON) decision $(SNAP_ARG) | tail -1)" != "fail" || { echo "snapshot fail ở cổng: make approve"; exit 1; }
	$(FSMON) publish $(SNAP_ARG)
	$(FSMON) verify $(SNAP_ARG)
	$(FSMON) report $(SNAP_ARG)

backfill:
	$(FSMON) backfill --start $(START) --end $(END)

approve:
	@test -n "$(SNAPSHOT)" -a -n "$(REASON)" || { echo 'cần SNAPSHOT=YYYY-MM-DD REASON="..."'; exit 1; }
	$(FSMON) approve --snapshot-ts $(SNAPSHOT) --reason "$(REASON)" --by "$(BY)"

restore:
	$(FSMON) restore

status:
	$(FSMON) status

# ---------- Airflow ----------
airflow:
	$(COMPOSE) --profile airflow up -d --build
	@echo "Airflow http://localhost:8080"

airflow-down:
	$(COMPOSE) --profile airflow stop airflow-apiserver airflow-scheduler airflow-dag-processor

# logical date 2026-10-28..2026-11-26 -> snapshot 2019-09-01..2019-09-30 (đồng hồ phát lại)
airflow-backfill:
	$(COMPOSE) exec airflow-scheduler airflow backfill create --dag-id snapshot_pipeline \
		--from-date 2026-10-28 --to-date 2026-11-26 --max-active-runs 1 --reprocess-behavior completed

airflow-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger snapshot_pipeline \
		$(if $(SNAPSHOT),--conf '{"snapshot_ts": "$(SNAPSHOT)"}',)

# ---------- demo ----------
demo:
	$(UV) run python scripts/demo.py

demo-fault:
	$(UV) run python scripts/inject_fault.py --fault $(FAULT) $(if $(SNAPSHOT),--snapshot-ts $(SNAPSHOT),)

# ---------- chất lượng ----------
test:
	$(UV) run pytest

test-fast:
	$(UV) run pytest -m "not integration"

lint:
	$(UV) run ruff check src tests scripts dags grafana feature_repo web
	$(UV) run ruff format --check src tests scripts grafana web

fmt:
	$(UV) run ruff format src tests scripts grafana web
	$(UV) run ruff check --fix src tests scripts dags grafana feature_repo web

plan:
	mkdir -p data && $(UV) run feast -c feature_repo plan --skip-source-validation

apply:
	$(UV) run feast -c feature_repo apply

dashboards:
	$(UV) run python grafana/build_dashboards.py

# Xóa toàn bộ trạng thái pipeline (giữ data/raw). Kho metrics trong PostgreSQL không bị xóa.
clean-state:
	rm -rf data/staging data/published data/quarantine data/reports data/registry.db data/online_store.db \
		data/.feast_applied.sha256 data/alerts.log

-- Kho metrics của fsmon. Phải khớp với src/fsmon/store.py.
-- PostgreSQL trong docker-compose chạy file này khi khởi tạo database fsmon.

CREATE TABLE IF NOT EXISTS snapshot_runs (
    run_id           varchar(64) PRIMARY KEY,
    snapshot_ts      timestamptz NOT NULL,
    status           varchar(32) NOT NULL,   -- running | audited | published | quarantined | failed_after_gate | failed
    health           varchar(8),             -- pass | warn | fail
    trigger          varchar(32),            -- manual | airflow | backfill | demo
    started_at       timestamptz NOT NULL,
    gate_decided_at  timestamptz,
    published_at     timestamptz,
    finished_at      timestamptz,
    duration_s       double precision,
    row_count        bigint,
    git_sha          varchar(64),
    feast_version    varchar(32),
    reason           text,
    report_url       text,
    fault            varchar(64),
    materialized     boolean,
    approved_by      varchar(128),
    approval_reason  text,
    approved_at      timestamptz,
    airflow_run_id   varchar(256)
);
CREATE INDEX IF NOT EXISTS ix_snapshot_runs_snapshot_ts ON snapshot_runs (snapshot_ts);

CREATE TABLE IF NOT EXISTS snapshot_metrics (
    id              bigserial PRIMARY KEY,
    run_id          varchar(64) NOT NULL,
    snapshot_ts     timestamptz NOT NULL,
    feature_view    varchar(128) NOT NULL,
    feature         varchar(128),            -- null nếu metric ở mức view
    check_name      varchar(64) NOT NULL,    -- nhóm: schema | volume | freshness | value | drift | online | ops
    metric_name     varchar(64) NOT NULL,
    value           double precision,
    warn_threshold  double precision,
    fail_threshold  double precision,
    status          varchar(8) NOT NULL,     -- pass | warn | fail | info
    severity        varchar(16) NOT NULL,    -- blocker | advisory | post_publish | info
    baseline_ref    text,
    detail          text,
    recorded_at     timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_snapshot_metrics_run_id ON snapshot_metrics (run_id);
CREATE INDEX IF NOT EXISTS ix_metrics_view_metric_ts ON snapshot_metrics (feature_view, metric_name, snapshot_ts);

-- Run mới nhất của mỗi snapshot_ts, cho dashboard.
CREATE OR REPLACE VIEW latest_snapshot_runs AS
SELECT DISTINCT ON (snapshot_ts) *
FROM snapshot_runs
ORDER BY snapshot_ts, started_at DESC;

-- Trạng thái theo feature view của run mới nhất mỗi snapshot (fail > warn > pass).
CREATE OR REPLACE VIEW view_health AS
SELECT m.snapshot_ts,
       m.feature_view,
       CASE
           WHEN bool_or(m.status = 'fail' AND m.severity = 'blocker') THEN 'fail'
           WHEN bool_or(m.status IN ('warn', 'fail')) THEN 'warn'
           ELSE 'pass'
       END AS health
FROM snapshot_metrics m
JOIN latest_snapshot_runs r ON r.run_id = m.run_id
WHERE m.feature_view NOT LIKE '\_\_%' AND m.check_name <> 'ops'
GROUP BY m.snapshot_ts, m.feature_view;

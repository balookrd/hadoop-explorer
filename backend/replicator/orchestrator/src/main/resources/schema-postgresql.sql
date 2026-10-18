-- ==============================================================================
-- Hadoop gRPC Replicator Orchestrator: Production PostgreSQL Schema (Partitioned)
-- ==============================================================================
-- Данный DDL-скрипт предназначен для промышленного развертывания Orchestrator
-- в отказоустойчивом кластере PostgreSQL 14+ (включая Patroni / Cloud Native PG).
--
-- Особенности архитектуры:
-- 1. Декларативное секционирование (Declarative Range Partitioning) для сверхкрупных
--    таблиц подзадач ('replication_tasks') и истории запусков ('job_runs') по времени.
-- 2. Эффективный Partition Pruning при выборке свежих задач воркерами.
-- 3. Мгновенная ротация и очистка устаревших секций через DROP TABLE (<10 мс)
--    вместо тяжелого DELETE / VACUUM.
-- ==============================================================================

-- 1. Основная таблица задач репликации HDFS
CREATE TABLE IF NOT EXISTS replication_jobs (
    id VARCHAR(64) PRIMARY KEY,
    source_path TEXT NOT NULL,
    target_path TEXT NOT NULL,
    job_type VARCHAR(32) NOT NULL DEFAULT 'STANDARD',
    parent_job_id VARCHAR(64),
    source_cluster_id VARCHAR(64) NOT NULL DEFAULT 'dc1',
    target_cluster_id VARCHAR(64) NOT NULL DEFAULT 'dc2',
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    total_bytes BIGINT NOT NULL DEFAULT 0,
    copied_bytes BIGINT NOT NULL DEFAULT 0,
    run_as_service_account BOOLEAN NOT NULL DEFAULT TRUE,
    execution_principal VARCHAR(255) NOT NULL DEFAULT 'hdfs@EXAMPLE.COM',
    created_by VARCHAR(255) NOT NULL DEFAULT 'system_operator',
    is_scheduled BOOLEAN NOT NULL DEFAULT FALSE,
    sync_mode VARCHAR(32) NOT NULL DEFAULT 'MANUAL',
    last_processed_txid BIGINT,
    cron_expression VARCHAR(64),
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON replication_jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_clusters ON replication_jobs (source_cluster_id, target_cluster_id);
CREATE INDEX IF NOT EXISTS idx_jobs_created_by ON replication_jobs (created_by);


-- 2. Партиционированная таблица подзадач репликации (Range Partitioning по updated_at)
-- Миллионы файлов и мелких чанков шардируются по временным диапазонам.
CREATE TABLE IF NOT EXISTS replication_tasks (
    id VARCHAR(64) NOT NULL,
    job_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64),
    source_path TEXT NOT NULL,
    target_path TEXT NOT NULL,
    file_size BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    assigned_agent_id VARCHAR(64),
    checksum VARCHAR(255),
    error_message TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_failed_agent_id VARCHAR(64),
    task_type VARCHAR(32) NOT NULL DEFAULT 'FILE',
    file_count INT NOT NULL DEFAULT 1,
    bundle_manifest TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, updated_at)
) PARTITION BY RANGE (updated_at);

-- Индексы на родительской партиционированной таблице (автоматически наследуются секциями)
CREATE INDEX IF NOT EXISTS idx_task_job_id ON replication_tasks (job_id);
CREATE INDEX IF NOT EXISTS idx_task_status ON replication_tasks (status);
CREATE INDEX IF NOT EXISTS idx_task_job_status ON replication_tasks (job_id, status);
CREATE INDEX IF NOT EXISTS idx_task_status_job_id ON replication_tasks (status, job_id);
CREATE INDEX IF NOT EXISTS idx_task_assigned_status ON replication_tasks (assigned_agent_id, status);

-- Секции по умолчанию и предсозданные помесячные партиции
CREATE TABLE IF NOT EXISTS replication_tasks_default PARTITION OF replication_tasks DEFAULT;

CREATE TABLE IF NOT EXISTS replication_tasks_2026_09 PARTITION OF replication_tasks
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS replication_tasks_2026_10 PARTITION OF replication_tasks
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS replication_tasks_2026_11 PARTITION OF replication_tasks
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS replication_tasks_2026_12 PARTITION OF replication_tasks
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS replication_tasks_2027_01 PARTITION OF replication_tasks
    FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');


-- 3. Партиционированная таблица истории запусков (Job Runs)
CREATE TABLE IF NOT EXISTS job_runs (
    id VARCHAR(64) NOT NULL,
    job_id VARCHAR(64) NOT NULL,
    run_number INT NOT NULL DEFAULT 1,
    trigger_type VARCHAR(32) NOT NULL DEFAULT 'MANUAL',
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    total_bytes BIGINT NOT NULL DEFAULT 0,
    copied_bytes BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    average_speed_mb_s DOUBLE PRECISION,
    message TEXT,
    error_message TEXT,
    error_details TEXT,
    transferred_objects INT NOT NULL DEFAULT 0,
    skipped_objects INT NOT NULL DEFAULT 0,
    failed_objects INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id, started_at)
) PARTITION BY RANGE (started_at);

CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs (job_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs (status);

CREATE TABLE IF NOT EXISTS job_runs_default PARTITION OF job_runs DEFAULT;

CREATE TABLE IF NOT EXISTS job_runs_2026_09 PARTITION OF job_runs
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS job_runs_2026_10 PARTITION OF job_runs
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS job_runs_2026_11 PARTITION OF job_runs
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS job_runs_2026_12 PARTITION OF job_runs
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS job_runs_2027_01 PARTITION OF job_runs
    FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');


-- 4. Вспомогательные сервисные таблицы Control Plane
CREATE TABLE IF NOT EXISTS hms_replication_jobs (
    id VARCHAR(64) PRIMARY KEY,
    source_db VARCHAR(128) NOT NULL,
    target_db VARCHAR(128) NOT NULL,
    source_cluster_id VARCHAR(64) NOT NULL DEFAULT 'dc1',
    target_cluster_id VARCHAR(64) NOT NULL DEFAULT 'dc2',
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    execution_principal VARCHAR(255) NOT NULL DEFAULT 'hive@EXAMPLE.COM',
    run_as_service_account BOOLEAN NOT NULL DEFAULT TRUE,
    created_by VARCHAR(255) NOT NULL DEFAULT 'system_operator',
    last_event_id BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kafka_replication_jobs (
    id VARCHAR(64) PRIMARY KEY,
    source_cluster_id VARCHAR(64) NOT NULL,
    target_cluster_id VARCHAR(64) NOT NULL,
    topics TEXT NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS replicator_streaming_lease (
    cluster_id VARCHAR(64) PRIMARY KEY,
    active_agent_id VARCHAR(64) NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    epoch BIGINT NOT NULL DEFAULT 1,
    last_processed_txid BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dr_emergency_snapshots (
    id VARCHAR(64) PRIMARY KEY,
    dc_id VARCHAR(64) NOT NULL,
    stopped_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active_jobs_snapshot TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replicator_cluster_locks (
    lock_key VARCHAR(128) PRIMARY KEY,
    locked_by VARCHAR(128) NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

-- ==============================================================================
-- 5. Хранимая функция автоматического создания будущих помесячных партиций
-- ==============================================================================
CREATE OR REPLACE FUNCTION create_future_tasks_partition(months_ahead INT DEFAULT 1)
RETURNS VOID AS $$
DECLARE
    target_date DATE := (CURRENT_DATE + (months_ahead || ' month')::INTERVAL);
    start_ts TIMESTAMPTZ := date_trunc('month', target_date);
    end_ts TIMESTAMPTZ := date_trunc('month', target_date + '1 month'::INTERVAL);
    part_suffix TEXT := to_char(start_ts, 'YYYY_MM');
    tasks_part_name TEXT := 'replication_tasks_' || part_suffix;
    runs_part_name TEXT := 'job_runs_' || part_suffix;
BEGIN
    -- Создание партиции replication_tasks
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = tasks_part_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF replication_tasks FOR VALUES FROM (%L) TO (%L);',
            tasks_part_name, start_ts, end_ts
        );
        RAISE NOTICE 'Создана партиция: % для интервала [%, %)', tasks_part_name, start_ts, end_ts;
    END IF;

    -- Создание партиции job_runs
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = runs_part_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF job_runs FOR VALUES FROM (%L) TO (%L);',
            runs_part_name, start_ts, end_ts
        );
        RAISE NOTICE 'Создана партиция: % для интервала [%, %)', runs_part_name, start_ts, end_ts;
    END IF;
END;
$$ LANGUAGE plpgsql;

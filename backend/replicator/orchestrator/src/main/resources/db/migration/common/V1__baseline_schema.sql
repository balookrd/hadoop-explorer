-- =============================================================================
-- V1: базовая схема Replicator Orchestrator
-- =============================================================================
-- Единственный источник схемы. До этой миграции схему строил ddl-auto: update по
-- аннотациям, а DDL-скрипт schema-postgresql.sql не подключался ни к одному
-- окружению и разошелся с моделью: в нем не было таблиц журнала событий HMS и его
-- DLQ, снимки DR и аренда стримера назывались иначе, а блокировки кластера были
-- описаны дважды в двух несовместимых вариантах.
--
-- Содержимое получено выгрузкой DDL из самой модели JPA, поэтому схема и сущности
-- совпадают колонка в колонку: с ddl-auto: validate расхождение ловится на старте.
-- Выгрузка под диалекты H2 и PostgreSQL дает побайтно одинаковый DDL, поэтому
-- миграция общая; специфика PostgreSQL — в V2 своего каталога.
--
-- Существующая база, созданная ddl-auto, подхватывается через
-- spring.flyway.baseline-on-migrate: Flyway помечает ее как baseline и применяет
-- только последующие миграции.
-- =============================================================================

create table cluster_locks (
    epoch bigint not null,
    locked_until timestamp(6) with time zone not null,
    updated_at timestamp(6) with time zone not null,
    lock_name varchar(128) not null,
    locked_by varchar(128) not null,
    primary key (lock_name)
);

create table hms_dead_letter_events (
    retry_count integer not null,
    event_id bigint not null,
    failed_at timestamp(6) with time zone not null,
    status varchar(32) not null,
    event_type varchar(64) not null,
    hms_job_id varchar(64) not null,
    id varchar(64) not null,
    table_name varchar(128),
    partition_name varchar(256),
    error_message TEXT,
    payload TEXT,
    primary key (id)
);
create index idx_hms_dlq_job_id on hms_dead_letter_events (hms_job_id);
create index idx_hms_dlq_status on hms_dead_letter_events (status);
create index idx_hms_dlq_event_id on hms_dead_letter_events (hms_job_id, event_id);

create table hms_event_logs (
    created_at timestamp(6) with time zone not null,
    event_id bigint,
    updated_at timestamp(6) with time zone not null,
    status varchar(32) not null,
    event_type varchar(64) not null,
    hms_job_id varchar(64) not null,
    id varchar(64) not null,
    subjob_id varchar(64),
    table_name varchar(128) not null,
    partition_name varchar(256),
    source_uri varchar(1024),
    target_uri varchar(1024),
    error_message varchar(2048),
    primary key (id)
);
create index idx_hms_event_job_id on hms_event_logs (hms_job_id);
create index idx_hms_event_status on hms_event_logs (status);
create index idx_hms_event_id_order on hms_event_logs (hms_job_id, event_id);
create index idx_hms_event_job_created on hms_event_logs (hms_job_id, created_at);

create table hms_replication_jobs (
    drop_extraneous_partitions boolean not null,
    drop_extraneous_tables boolean not null,
    enable_hdfs_streaming boolean not null,
    replicated_partitions integer not null,
    replicated_tables integer not null,
    total_partitions integer not null,
    total_tables integer not null,
    bootstrap_event_id bigint,
    created_at timestamp(6) with time zone not null,
    event_lag bigint,
    last_processed_event_id bigint,
    last_sync_at timestamp(6) with time zone,
    lease_expires_at timestamp(6) with time zone,
    updated_at timestamp(6) with time zone not null,
    version bigint,
    status varchar(32) not null,
    assigned_agent_id varchar(64),
    companion_hdfs_job_id varchar(64),
    id varchar(64) not null,
    last_failed_agent_id varchar(64),
    source_cluster_id varchar(64) not null,
    target_cluster_id varchar(64) not null,
    execution_principal varchar(128),
    source_db_name varchar(128) not null,
    target_db_name varchar(128) not null,
    db_warehouse_location varchar(512),
    created_by varchar(255) not null,
    message varchar(255),
    table_exclude_pattern varchar(255),
    table_include_pattern varchar(255),
    primary key (id)
);
create index idx_hms_jobs_status on hms_replication_jobs (status);
create index idx_hms_jobs_assigned_agent on hms_replication_jobs (assigned_agent_id);
create index idx_hms_jobs_src_cluster on hms_replication_jobs (source_cluster_id);
create index idx_hms_jobs_lease_exp on hms_replication_jobs (lease_expires_at);

create table job_runs (
    average_speed_mb_s float(53),
    duration_seconds float(53),
    failed_objects integer,
    run_number integer not null,
    skipped_objects integer,
    total_objects integer,
    transferred_objects integer,
    completed_at timestamp(6) with time zone,
    copied_bytes bigint not null,
    created_at timestamp(6) with time zone not null,
    started_at timestamp(6) with time zone,
    total_bytes bigint not null,
    status varchar(32) not null,
    trigger_type varchar(32) not null,
    id varchar(64) not null,
    job_id varchar(64) not null,
    error_details varchar(255),
    error_message varchar(255),
    message varchar(255),
    triggered_by varchar(255),
    primary key (id)
);

create table kafka_replication_jobs (
    auto_create_topics boolean,
    bandwidth_limit_mb float(53),
    batch_size integer,
    consumer_threads integer,
    offset_sync_interval_sec integer,
    schema_transcoding_enabled boolean not null,
    source_kerberos_enabled boolean,
    source_sr_kerberos_enabled boolean,
    target_kerberos_enabled boolean,
    target_sr_kerberos_enabled boolean,
    bytes_replicated bigint,
    created_at timestamp(6) with time zone not null,
    current_lag_ms bigint,
    messages_replicated bigint,
    updated_at timestamp(6) with time zone not null,
    direction varchar(32) not null,
    source_sr_type varchar(32),
    status varchar(32) not null,
    target_sr_type varchar(32),
    id varchar(64) not null,
    source_dc varchar(64) not null,
    source_service_name varchar(64),
    target_dc varchar(64) not null,
    target_service_name varchar(64),
    name varchar(128) not null,
    source_principal varchar(128),
    target_principal varchar(128),
    source_bootstrap_servers varchar(256),
    source_keytab_path varchar(256),
    source_sr_url varchar(256),
    target_bootstrap_servers varchar(256),
    target_keytab_path varchar(256),
    target_sr_url varchar(256),
    last_error_message varchar(1024),
    topic_provisioning_summary varchar(1024),
    consumer_groups_json TEXT,
    target_endpoints_json TEXT,
    topics_json TEXT not null,
    primary key (id)
);

create table replication_agents (
    active_transfers integer not null,
    chunk_latency_avg_ms float(53) not null,
    chunk_latency_max_ms float(53) not null,
    compression_ratio float(53) not null,
    grpc_port integer not null,
    max_bandwidth_mb_s float(53),
    staging_active_files integer not null,
    chunk_transfer_count bigint not null,
    compression_compressed_bytes bigint not null,
    compression_raw_bytes bigint not null,
    created_at timestamp(6) with time zone not null,
    last_heartbeat timestamp(6) with time zone not null,
    staging_active_bytes bigint not null,
    staging_orphans_cleaned bigint not null,
    transfer_errors bigint not null,
    transferred_bytes bigint not null,
    updated_at timestamp(6) with time zone not null,
    mode varchar(32) not null,
    status varchar(32) not null,
    agent_id varchar(64) not null,
    cluster_id varchar(64) not null,
    host varchar(128) not null,
    grpc_address varchar(160) not null,
    primary key (agent_id)
);

create table replication_bandwidth_limits (
    limit_bytes_per_sec bigint not null,
    updated_at timestamp(6) with time zone not null,
    limit_key varchar(128) not null,
    primary key (limit_key)
);

create table replication_jobs (
    failed_objects integer not null,
    history_retention_runs integer not null,
    is_scheduled boolean not null,
    run_as_service_account boolean not null,
    skipped_objects integer not null,
    total_objects integer not null,
    transferred_objects integer not null,
    completed_at timestamp(6) with time zone,
    copied_bytes bigint not null,
    created_at timestamp(6) with time zone not null,
    last_processed_txid bigint,
    last_run_at timestamp(6) with time zone,
    next_run_at timestamp(6) with time zone,
    started_at timestamp(6) with time zone,
    total_bytes bigint not null,
    txid_lag bigint,
    updated_at timestamp(6) with time zone not null,
    version bigint,
    job_type varchar(32) not null,
    status varchar(32) not null,
    sync_mode varchar(32) not null,
    active_run_id varchar(64),
    id varchar(64) not null,
    parent_job_id varchar(64),
    source_cluster_id varchar(64) not null,
    target_cluster_id varchar(64) not null,
    created_by varchar(255) not null,
    cron_expression varchar(255),
    execution_principal varchar(255) not null,
    message varchar(255),
    source_path varchar(255) not null,
    target_path varchar(255) not null,
    primary key (id)
);
create index idx_jobs_status on replication_jobs (status);
create index idx_jobs_type_status on replication_jobs (job_type, status);
create index idx_jobs_parent_job_id on replication_jobs (parent_job_id);
create index idx_jobs_scheduled_next_run on replication_jobs (is_scheduled, next_run_at);
create index idx_jobs_clusters on replication_jobs (source_cluster_id, target_cluster_id);
create index idx_jobs_created_by on replication_jobs (created_by);

create table replication_orchestrator_nodes (
    last_heartbeat timestamp(6) with time zone not null,
    status varchar(32) not null,
    node_id varchar(64) not null,
    host_name varchar(128) not null,
    primary key (node_id)
);

create table replication_tasks (
    file_count integer not null,
    max_retries integer not null,
    retry_count integer not null,
    file_size bigint not null,
    updated_at timestamp(6) with time zone,
    status varchar(32) not null,
    task_type varchar(32) not null,
    assigned_agent_id varchar(64),
    id varchar(64) not null,
    job_id varchar(64) not null,
    last_failed_agent_id varchar(64),
    run_id varchar(64),
    bundle_manifest TEXT,
    checksum varchar(255),
    error_message varchar(255),
    source_path varchar(255) not null,
    target_path varchar(255) not null,
    primary key (id)
);
create index idx_task_job_id on replication_tasks (job_id);
create index idx_task_status on replication_tasks (status);
create index idx_task_job_status on replication_tasks (job_id, status);
create index idx_task_status_job_id on replication_tasks (status, job_id);
create index idx_task_status_updated_at on replication_tasks (status, updated_at);

create table replicator_dr_snapshots (
    network_fenced boolean not null,
    timestamp timestamp(6) with time zone not null,
    cluster_id varchar(64) not null,
    id varchar(64) not null,
    operator varchar(128),
    reason varchar(1024),
    snapshot_json TEXT,
    primary key (id)
);

create table replicator_streaming_leases (
    epoch bigint not null,
    expires_at timestamp(6) with time zone not null,
    last_renewed_at timestamp(6) with time zone not null,
    cluster_id varchar(64) not null,
    active_agent_id varchar(128) not null,
    primary key (cluster_id)
);

-- Ссылочная целостность: подзадачи и история запусков принадлежат заданию
alter table job_runs add constraint FKaowqqtrl7x5625y8lwtj7tl2h foreign key (job_id) references replication_jobs;
alter table replication_tasks add constraint FK6q2g68q8rjd0ovkqeh51ftp6n foreign key (job_id) references replication_jobs;

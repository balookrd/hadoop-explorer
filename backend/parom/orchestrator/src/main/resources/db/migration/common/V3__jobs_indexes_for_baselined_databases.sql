-- =============================================================================
-- V3: индексы replication_jobs для баз, принятых через baseline
-- =============================================================================
-- Базы, созданные прежним ddl-auto: update, Flyway помечает как baseline и V1 к ним
-- не применяет. Индексы заданий из V1 на таких установках не появились бы вовсе, а
-- именно они убирают полный просмотр таблицы из тика планировщика и опроса заданий
-- агентами.
--
-- Операторы идемпотентны, поэтому на свежей базе, где V1 эти индексы уже создала,
-- миграция ничего не делает.
-- =============================================================================

create index if not exists idx_jobs_status on replication_jobs (status);
create index if not exists idx_jobs_type_status on replication_jobs (job_type, status);
create index if not exists idx_jobs_parent_job_id on replication_jobs (parent_job_id);
create index if not exists idx_jobs_scheduled_next_run on replication_jobs (is_scheduled, next_run_at);
create index if not exists idx_jobs_clusters on replication_jobs (source_cluster_id, target_cluster_id);
create index if not exists idx_jobs_created_by on replication_jobs (created_by);

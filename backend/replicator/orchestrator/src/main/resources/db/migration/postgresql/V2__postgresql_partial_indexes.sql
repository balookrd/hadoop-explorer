-- =============================================================================
-- V2 (только PostgreSQL): частичные индексы и параметры хранения горячих таблиц
-- =============================================================================
-- Перенесено из неподключенного schema-postgresql.sql. Эти индексы не описать
-- аннотациями JPA, а для нагрузки в сотни агентов они и есть главный выигрыш:
-- активных задач в таблице единицы процентов, остальное — миллионы завершенных
-- записей, которые из индекса исключаются, и он целиком помещается в кэш.
--
-- H2 частичных индексов не поддерживает, поэтому миграция лежит в каталоге
-- вендора и на H2 (разработка, тесты) не применяется.
--
-- Секционирование replication_tasks и job_runs из прежнего скрипта сюда НЕ
-- перенесено: в PostgreSQL ключ секционирования обязан входить в первичный ключ,
-- то есть PK стал бы составным (id, updated_at), а updated_at у сущности
-- допускает NULL. Это смена семантики модели, а не перенос схемы, и она требует
-- отдельного решения вместе с функцией автосоздания помесячных секций.
-- =============================================================================

-- Выборка свежих задач воркерами и поиск зависших: индекс только по активным статусам
CREATE INDEX IF NOT EXISTS idx_tasks_queued_active
    ON replication_tasks (updated_at) WHERE status = 'QUEUED';
CREATE INDEX IF NOT EXISTS idx_tasks_running_stale
    ON replication_tasks (updated_at) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS idx_tasks_active_by_job
    ON replication_tasks (job_id, status) WHERE status IN ('QUEUED', 'RUNNING');

-- Failover: задачи, которые держит конкретный агент
CREATE INDEX IF NOT EXISTS idx_tasks_assigned_running
    ON replication_tasks (assigned_agent_id) WHERE status = 'RUNNING';

-- Опрос активных заданий ЦОД агентами
CREATE INDEX IF NOT EXISTS idx_jobs_running_cluster
    ON replication_jobs (source_cluster_id, status) WHERE status = 'RUNNING';

-- Статус задач и прогресс обновляются постоянно: место под HOT-обновления в странице
-- и более частый autovacuum вместо распухания таблицы
ALTER TABLE replication_tasks
    SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, fillfactor = 90);
ALTER TABLE job_runs
    SET (autovacuum_vacuum_scale_factor = 0.05, fillfactor = 90);

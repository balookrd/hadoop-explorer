-- =============================================================================
-- V10: длина путей и сообщений
-- =============================================================================
-- Пути подзадач и пофайловых задач хранились в varchar(255). Полный URI файла
-- партиции (hdfs://ns/warehouse/db.db/table/dt=.../hour=.../part-...-c000.snappy.parquet)
-- на боевых схемах длиннее, и вставка задач падала. Тексты ошибок агента (разрыв
-- журнала событий метастора, сбои Kerberos) длиннее 255, и на PostgreSQL отчет
-- агента откатывался целиком вместе с позицией CDC, журналом событий и DLQ.
--
-- Увеличение длины varchar в PostgreSQL меняет только каталог, таблица не
-- переписывается. По этим колонкам нет индексов B-tree, поэтому предел размера
-- строки индекса не мешает.
-- =============================================================================

alter table replication_jobs alter column source_path set data type varchar(8192);
alter table replication_jobs alter column target_path set data type varchar(8192);
alter table replication_jobs alter column message set data type varchar(4096);

alter table replication_tasks alter column source_path set data type varchar(8192);
alter table replication_tasks alter column target_path set data type varchar(8192);
alter table replication_tasks alter column error_message set data type varchar(4096);

alter table job_runs alter column message set data type varchar(4096);
alter table job_runs alter column error_message set data type varchar(4096);
alter table job_runs alter column error_details set data type varchar(4096);

alter table hms_replication_jobs alter column message set data type varchar(4096);

alter table hms_event_logs alter column partition_name set data type varchar(2048);
alter table hms_event_logs alter column source_uri set data type varchar(8192);
alter table hms_event_logs alter column target_uri set data type varchar(8192);
alter table hms_event_logs alter column error_message set data type varchar(4096);

alter table hms_dead_letter_events alter column partition_name set data type varchar(2048);

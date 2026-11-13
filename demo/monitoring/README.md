# Стек мониторинга платформы (demo/monitoring)

Демонстрационный стек Prometheus 2.50 и Grafana 10.4. Собирает HTTP golden signals, метрики очередей YARN, файловых операций HDFS, SQL- и Spark-запросов и межкластерной репликации Hadoop gRPC Replicator (HDFS, HMS, Kafka).

---

## Сервисы стека

| Сервис | Порт | Образ | Назначение | URL интерфейса |
|---|---|---|---|---|
| Prometheus | `9090` | `prom/prometheus:v2.50.0` | Сбор временных рядов с `/actuator/prometheus` (YARN, HDFS, SQL, Spark) и `/metrics` (оркестратор Replicator), вычисление правил алертов | [http://localhost:9090](http://localhost:9090) |
| Grafana | `3000` | `grafana/grafana:10.4.0` | Дашборды с автопровижинингом (анонимный вход с ролью admin, папка `Pristan`) | [http://localhost:3000](http://localhost:3000) |

---

## Запуск и проверка

### Запуск стека
```bash
make demo-monitoring
# Либо напрямую из каталога:
cd demo/monitoring && ./start-monitoring.sh
```

### Остановка стека
```bash
make demo-monitoring-stop
# Либо:
cd demo/monitoring && ./stop-monitoring.sh
```

### Smoke-проверка работающего стека
```bash
make demo-monitoring-smoke
# Либо:
cd demo/monitoring && ./run-monitoring-smoke-tests.sh
```

Скрипт проверяет:
- Готовность Prometheus (`/-/healthy`, `/-/ready`).
- Конфигурацию скрейпинга (`hadoop-explorer`, `hadoop-replicator`).
- Загрузку правил алертов и выполнение тестовых PromQL-запросов.
- Доступность Grafana API (`/api/health`).
- Доступность источника данных Prometheus из Grafana (`/api/datasources/uid/prometheus/health`).
- Наличие всех 8 дашбордов.

---

## Дашборды Grafana

Дашборды монтируются из `monitoring/grafana/dashboards` в папку `Pristan`:

1. `pristan-overview` — сводка по платформе и golden signals:
   - Скорость запросов (RPS), p50/p90/p95/p99-задержки, доля ответов 5xx, запросы в обработке по сервисам YARN, HDFS, SQL, Spark.
   - Входы через `/api/v1/auth/login` по кодам ответа, ответы 429 ограничителя частоты по уровням `login` и `api` (`hadoop_rate_limit_blocks_total`), ответы 5xx по классу исключения.
   - Сводка Hadoop gRPC Replicator.
2. `hadoop-replicator-overview` — межкластерная репликация и шейпинг полосы:
   - Скорость репликации данных (MB/s), задержки троттлинга Token Bucket.
   - Статусы пакетных и потоковых задач HDFS.
3. `hadoop-replicator-kafka` — потоковая репликация Kafka:
   - Сквозной лаг передачи (Lag ms), скорость сообщений и байт.
   - Статусы задач (ACTIVE/PAUSED), выделенные лимиты WAN.
4. `hadoop-replicator-hms-dr` — DR-синхронизация Hive Metastore:
   - Прогресс Bootstrap-синхронизации таблиц и партиций.
   - Статус изоляции кластеров (fencing) и аварийной остановки (kill switch).
5. `hadoop-replicator-agents` — gRPC-агенты и воркеры передачи данных:
   - Онлайн-статус воркеров (основной DC1, резервный DC2).
   - Гистограмма задержек передачи gRPC чанков (SLA-бакеты 1 мс – 5 с).
   - Объем staging-буферов и очистка временных файлов.
6. `sklad-operations` — файловые операции Sklad:
   - Темп операций клиента HDFS по методам и кластерам, доля неуспешных операций (`sklad_operations_total`).
   - Трафик upload/download по кластерам (`sklad_bytes_transferred_total`).
7. `artel_queues` — Artel:
   - Переходы заявок Change Request по статусам (`artel_change_requests_total`).
   - Задержка p95 API Artel по URI.
8. `spark_sql_explorer_analytics` — Spark и SQL Explorer:
   - Активные Livy-сессии и p95 длительности Spark-инструкций.
   - Темп и p95 длительности SQL-запросов по движкам Trino и Hive.

---

## Правила алертов Prometheus

Правила определены в [`prometheus/alert.rules.yml`](prometheus/alert.rules.yml):
- `HadoopHigh5xxErrorRate` — более 5% запросов завершаются ошибками 5xx.
- `HadoopHighApiLatency` — 95-й перцентиль задержки превышает 2 секунды.
- `HadoopLoginRateLimitSpike` — ограничитель частоты отвечает 429 на `/api/v1/auth/login` и `/api/v1/auth/sso` чаще 0,2 раза в секунду (около 60 отказов за 5 минут) в течение 2 минут.
- `HadoopApiRateLimitSpike` — ограничитель частоты отвечает 429 на остальные запросы к `/api/*` чаще 1 раза в секунду (среднее за 5 минут) в течение 2 минут.
- `HdfsOperationsHighFailureRate` — более 10% операций с файлами HDFS завершаются ошибками.
- `ReplicationHighThrottlingDelay` — суммарная задержка троттлинга Token Bucket растет быстрее 5 секунд в секунду (`rate(replicator_throttling_delay_seconds_total[2m]) > 5`) в течение 2 минут.
- `ReplicationJobsHighFailureRate` — более 10% задач репликации HDFS находятся в статусе `FAILED` (отношение значений gauge `replicator_jobs_total`) в течение 2 минут.
- `ReplicationWorkerStarvation` — `replicator_active_workers` равен 0, а задачи в статусах `RUNNING` или `QUEUED` есть, в течение 2 минут.
- `KafkaReplicationHighLag` и `KafkaReplicationCriticalLag` — `kafka_current_lag_ms` больше 1000 мс в течение 1 минуты и больше 5000 мс в течение 2 минут.
- `KafkaReplicationJobPaused` — задача репликации Kafka в статусе `PAUSED` дольше 5 минут.

Задание `hadoop-replicator` опрашивает `GET /metrics` оркестратора с HTTP Basic `prometheus:<токен>`; токен в `prometheus/prometheus.yml` совпадает с `REPLICATOR_METRICS_SCRAPE_TOKEN` в `demo/replicator/docker-compose.yml` и `docker-compose.kafka.yml`. При расхождении цель `hadoop-replicator` в статусе `DOWN` с `server returned HTTP status 401`.

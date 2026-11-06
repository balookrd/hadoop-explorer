# Стек мониторинга платформы (demo/monitoring)

Демонстрационный стек Prometheus 2.50 и Grafana 10.4. Собирает HTTP golden signals, метрики очередей YARN, файловых операций HDFS, SQL- и Spark-запросов и межкластерной репликации Hadoop gRPC Replicator (HDFS, HMS, Kafka).

---

## Сервисы стека

| Сервис | Порт | Образ | Назначение | URL интерфейса |
|---|---|---|---|---|
| Prometheus | `9090` | `prom/prometheus:v2.50.0` | Сбор временных рядов (TSDB) с эндпоинтов `/metrics`, вычисление правил алертов | [http://localhost:9090](http://localhost:9090) |
| Grafana | `3000` | `grafana/grafana:10.4.0` | Дашборды с автопровижинингом (анонимный вход с ролью admin, папка `Hadoop Explorer`) | [http://localhost:3000](http://localhost:3000) |

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

Дашборды монтируются из `monitoring/grafana/dashboards` в папку `Hadoop Explorer`:

1. `hadoop-explorer-overview` — сводка по платформе и golden signals:
   - Доступность сервисов платформы (YARN, HDFS, SQL, Spark, Replicator).
   - Скорость запросов (RPS), p95-задержки и процент 5xx ошибок.
   - Состояние circuit breaker и rate limiter.
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
6. `hdfs-explorer-operations` — файловые операции WebHDFS:
   - Операции чтения, записи, удаления и листинга в разрезе кластеров.
   - Ошибки ввода-вывода и задержки запросов к NameNode и DataNode.
7. `yarn_explorer_queues` — очереди YARN Capacity Scheduler:
   - Использование памяти (MB) и vCores по очередям и пулам.
   - Количество запущенных и ожидающих приложений (Pending/Running Apps).
8. `spark_sql_explorer_analytics` — интерактивная аналитика Spark и SQL:
   - Активные Livy-сессии, потребление ресурсов исполнителями Spark.
   - Запросы к Hive Metastore, Trino и HiveServer2.

---

## Правила алертов Prometheus

Правила определены в [`prometheus/alert.rules.yml`](prometheus/alert.rules.yml):
- `HadoopCircuitBreakerOpen` — circuit breaker в состоянии OPEN дольше 30 секунд.
- `HadoopCircuitBreakerHalfOpen` — circuit breaker в состоянии HALF_OPEN (пробные вызовы).
- `HadoopHigh5xxErrorRate` — более 5% запросов завершаются ошибками 5xx.
- `HadoopHighApiLatency` — 95-й перцентиль задержки превышает 2 секунды.
- `HadoopRateLimitSpike` — более 10 отказов в секунду по лимитам запросов.
- `HdfsOperationsHighFailureRate` — более 10% операций с файлами HDFS завершаются ошибками.
- `ReplicationHighThrottlingDelay` — суммарная задержка троттлинга Token Bucket растет быстрее 5 секунд в секунду (`rate(throttling_delay_seconds_total[2m]) > 5`) в течение 2 минут.

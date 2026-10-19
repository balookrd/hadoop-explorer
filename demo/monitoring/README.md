# 📈 Стек мониторинга и наблюдаемости платформы (demo/monitoring)

Конфигурация демонстрационного стека наблюдаемости (Observability) на базе **Prometheus 2.50** и **Grafana 10.4**, обеспечивающая мониторинг метрик Golden Signals, очередей YARN, файловых операций HDFS, выполнения SQL/Spark запросов и межкластерной репликации Hadoop gRPC Replicator (HDFS, HMS, Kafka).

---

## 🏛️ Компоненты стека

| Сервис | Порт | Версия / Образ | Описание и назначение | URL интерфейса |
|---|---|---|---|---|
| **`Prometheus`** | `9090` | `prom/prometheus:v2.50.0` | Сервер сбора временных рядов (TSDB), парсинг эндпоинтов `/metrics`, вычисление правил алертов | [http://localhost:9090](http://localhost:9090) |
| **`Grafana`** | `3000` | `grafana/grafana:10.4.0` | Визуализация дашбордов с автопровижинингом (anonymous admin, folder `Hadoop Explorer`) | [http://localhost:3000](http://localhost:3000) |

---

## ⚡ Быстрый старт

### Запуск стека мониторинга:
```bash
make demo-monitoring
# Либо напрямую из каталога:
cd demo/monitoring && ./start-monitoring.sh
```

### Остановка стека:
```bash
make demo-monitoring-stop
# Либо:
cd demo/monitoring && ./stop-monitoring.sh
```

### Сквозная Live Smoke проверка:
```bash
make demo-monitoring-smoke
# Либо:
cd demo/monitoring && ./run-monitoring-smoke-tests.sh
```

Скрипт проверяет:
- Готовность Prometheus Server (`/-/healthy`, `/-/ready`).
- Корректность конфигурации скрейпинга (`hadoop-explorer`, `hadoop-replicator`).
- Загрузку правил алертов и исполнение тестовых PromQL-запросов.
- Здоровье Grafana API (`/api/health`).
- Сетевой пинг Grafana к датасорсу Prometheus (`/api/datasources/uid/prometheus/health`).
- Загрузку и доступность всех 8 предустановленных дашбордов.

---

## 📊 Предустановленные дашборды Grafana

Все дашборды автоматически монтируются из `monitoring/grafana/dashboards` в папку `Hadoop Explorer`:

1. **`hadoop-explorer-overview`** — *Сводный обзор платформы & Golden Signals*:
   - Общая доступность сервисов платформы (YARN, HDFS, SQL, Spark, Replicator).
   - Скорость запросов (RPS), p95-задержки и процент 5xx ошибок.
   - Статус автоматов защиты Circuit Breaker и Rate Limiter.
2. **`hadoop-replicator-overview`** — *Межкластерная репликация и шейпинг полосы*:
   - Скорость репликации данных (MB/s), задержки троттлинга Token Bucket.
   - Статусы пакетных и потоковых задач HDFS.
3. **`hadoop-replicator-kafka`** — *Потоковая репликация Kafka*:
   - Сквозной лаг передачи (Lag ms), скорость сообщений и байт.
   - Статусы задач (ACTIVE/PAUSED), выделенные лимиты WAN.
4. **`hadoop-replicator-hms-dr`** — *Hive Metastore DR Синхронизация*:
   - Прогресс Bootstrap-синхронизации таблиц и партиций.
   - Статус изоляции кластеров (Fencing), аварийный рубильник (Kill-Switch).
5. **`hadoop-replicator-agents`** — *gRPC Агенты и воркеры передачи данных*:
   - Онлайн-статус флота воркеров (Primary DC1, DR DC2).
   - Гистограмма задержек передачи gRPC чанков (SLA бакеты 1 мс - 5 с).
   - Объем staging-буферов и очистка временных файлов.
6. **`hdfs-explorer-operations`** — *Файловые операции WebHDFS*:
   - Операции чтения, записи, удаления и листинга в разрезе кластеров.
   - Ошибки I/O и латентность взаимодействия с NameNode/DataNode.
7. **`yarn_explorer_queues`** — *Очереди YARN Capacity Scheduler*:
   - Утилизация памяти (MB) и vCores по очередям и пулам.
   - Количество запущенных и ожидающих приложений (Pending/Running Apps).
8. **`spark_sql_explorer_analytics`** — *Интерактивная аналитика Spark & SQL*:
   - Активные Livy-сессии, потребление ресурсов исполнителями Spark.
   - Запросы к Hive Metastore, Trino и HiveServer2.

---

## 🚨 Правила алертов Prometheus

Правила определены в [alert.rules.yml](file:///Users/mvmalykh/IdeaProjects/hadoop-explorer/demo/monitoring/prometheus/alert.rules.yml):
- `HadoopCircuitBreakerOpen` — автомат защиты разомкнут более 30 секунд.
- `HadoopCircuitBreakerHalfOpen` — автомат защиты в пробном режиме восстановления.
- `HadoopHigh5xxErrorRate` — более 5% запросов завершаются ошибками 5xx.
- `HadoopHighApiLatency` — 95-й перцентиль задержки превышает 2 секунды.
- `HadoopRateLimitSpike` — более 10 отказов в секунду по лимитам запросов.
- `HdfsOperationsHighFailureRate` — более 10% операций с файлами HDFS завершаются ошибками.
- `ReplicationHighThrottlingDelay` — высокая задержка из-за жестких лимитов Token Bucket.

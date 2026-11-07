# Руководство администратора платформы Hadoop Explorer

Документ для администраторов и DevOps/SRE-инженеров: общие для всех сервисов Hadoop Explorer требования к развертыванию, безопасности, портам и мониторингу, а также ссылки на руководства по каждому сервису.

---

## Сервисы платформы

Платформа состоит из 5 независимых сервисов на Java 21 / Spring Boot 3. Аутентификацию и ролевую модель они берут из общего стартера `backend/common-security-starter`, интерфейс построен на общей дизайн-системе. Руководство по каждому сервису описывает установку в трех режимах: Standalone, Docker, Kubernetes.

| Сервис | Порт по умолчанию | Назначение | Руководство администратора |
|---|---|---|---|
| YARN Explorer | `8001` (или `8000`) | Управление очередями Capacity Scheduler, мониторинг YARN RM HA, Change Requests, доставка и применение через Ansible AWX | [docs/yarn-admin-guide.md](yarn-admin-guide.md) |
| HDFS Explorer | `8002` (или `8000`) | Менеджер файлов Data Lake, WebHDFS NameNode HA, превью Parquet/ORC, квоты каталогов, Cross-Cluster Copy | [docs/hdfs-admin-guide.md](hdfs-admin-guide.md) |
| SQL Explorer | `8003` (или `8000`) | Веб-консоль Trino и Apache Hive, потоковая выдача результатов, отмена запросов, ИИ-ассистент SQL | [docs/sql-admin-guide.md](sql-admin-guide.md) |
| Spark Explorer | `8004` (или `8000`) | Управление интерактивными сессиями Livy (PySpark / Scala), интеграция с YARN и Spark History Server | [docs/spark-admin-guide.md](spark-admin-guide.md) |
| Hadoop gRPC Replicator | `8005` (API/UI)<br>`50051` (gRPC) | Межкластерная репликация HDFS (Orchestrator и двунаправленные агенты), ограничение полосы WAN, cron-планировщик | [docs/replicator-admin-guide.md](replicator-admin-guide.md) |

---

## Режимы развертывания

```
                      ┌─────────────────────────────────────────────────────────────┐
                      │              HADOOP EXPLORER PLATFORM                       │
                      └──────────────────────────────┬──────────────────────────────┘
                                                     │
         ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
         ▼                                           ▼                                           ▼
┌─────────────────────────────────┐ ┌─────────────────────────────────┐ ┌─────────────────────────────────┐
│     1. STANDALONE (systemd)     │ │   2. DOCKER & DOCKER COMPOSE    │ │   3. KUBERNETES (Replicator)    │
│                                 │ │                                 │ │                                 │
│ - Bare-Metal / Виртуальные маш. │ │ - Контейнеры Java 21 (Temurin)   │ │ - Манифесты Deployment/Service  │
│ - Готовые Fat JAR (No-Build)    │ │ - Готовые образы GHCR (ghcr.io) │ │ - Secret и ConfigMap            │
│ - Релизы GitHub Releases / sha  │ │ - Оркестрация в docker-compose  │ │ - ImagePullSecrets для GHCR     │
│ - Управление через systemctl    │ │ - Автоматический kinit в entry  │ │ - YARN/HDFS/SQL/Spark: нет      │
│ - Локальный kinit / k5start     │ │ - Multi-stage сборка (по желан.)│ │ - См. replicator-admin-guide §4 │
└─────────────────────────────────┘ └─────────────────────────────────┘ └─────────────────────────────────┘
```

Replicator Agent, кроме трех режимов выше, запускается внутри кластера Hadoop:
- На DataNode или edge-узле через `hadoop jar`: shaded fat JAR или тонкий JAR с системным `HADOOP_CLASSPATH`, полоса ограничивается `AGENT_MAX_BANDWIDTH_MB_S`.
- В Apache Hadoop YARN: пул воркеров запускает `ReplicatorYarnClient`, ими управляет `ReplicatorApplicationMaster`. HDFS delegation tokens передаются воркерам автоматически, поэтому keytab на NodeManager не нужен. Подробнее: [docs/replicator-admin-guide.md#26-запуск-распределенного-пула-агентов-в-apache-hadoop-yarn](replicator-admin-guide.md#26-запуск-распределенного-пула-агентов-в-apache-hadoop-yarn).

---

## Требования безопасности для всех сервисов

### 1. Kerberos keytab
- Все сервисы могут работать в кластерах с Kerberos.
- Монтируйте keytab только на чтение, с правами `chmod 400` и владельцем `appuser` (UID `10001`).
- В контейнере стартер безопасности (`common-security-starter`) сам выполняет вход в Kerberos через Java GSS-API / UGI и обновляет TGT по расписанию.
- В Kubernetes (манифесты Replicator) монтируйте keytab из `Secret` с `defaultMode: 0400`.

### 2. Секретный ключ JWT (`JWT_SECRET_KEY`)
- В production-режиме (`debug: false`) сервис не запускается, если `JWT_SECRET_KEY` короче 32 символов или совпадает с плейсхолдером.
- Сгенерируйте ключ:
  ```bash
  openssl rand -hex 32
  ```

### 3. База данных сессий: SQLite или PostgreSQL
- SQLite (`sqlite:////app/data/service.db`) — по умолчанию, для тестовых и Standalone-установок с одной репликой.
- PostgreSQL (`postgresql://user:pass@pg-host:5432/dbname`) — обязателен при двух и более экземплярах сервиса. Порядок перехода — в [docs/production-database.md](production-database.md).

---

## Порты и маршрутизация Ingress

| Сервис | Внутренний порт контейнера | Внешний порт (Demo) | Рекомендуемый Host Ingress |
|---|---|---|---|
| YARN Explorer | `8000` | `8001` | `https://yarn.company.local` |
| HDFS Explorer | `8000` | `8002` | `https://hdfs.company.local` |
| SQL Explorer | `8000` | `8003` | `https://sql.company.local` |
| Spark Explorer | `8000` | `8004` | `https://spark.company.local` |
| Replicator Orchestrator | `8005` | `8005` | `https://replicator.company.local` |
| Replicator Agent (gRPC) | `50051` | `50051` | `grpc://replicator-dc2.company.local:50051` |

---

## Мониторинг Prometheus и дашборды Grafana

Каждый сервис отдает метрики в формате OpenMetrics на эндпоинте `/metrics` (или `/api/v1/metrics`).

### Конфигурация сбора метрик в Prometheus
```yaml
scrape_configs:
  - job_name: 'hadoop-explorer-yarn'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['yarn.company.local:8000']

  - job_name: 'hadoop-explorer-hdfs'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['hdfs.company.local:8000']

  - job_name: 'hadoop-explorer-sql'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['sql.company.local:8000']

  - job_name: 'hadoop-explorer-spark'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['spark.company.local:8000']

  - job_name: 'hadoop-explorer-replicator'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['replicator.company.local:8005']
```

### Дашборды Grafana
Дашборды лежат в каталоге `monitoring/grafana/dashboards/`:
- Platform Overview: [`hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) — состояние и golden signals всех 5 сервисов платформы.
- gRPC Replicator: [`hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json) — скорость репликации WAN, иерархический шейпинг Token Bucket, агенты репликации, планировщик Cron.
- HDFS Operations: [`hdfs_explorer_operations.json`](../monitoring/grafana/dashboards/hdfs_explorer_operations.json) — файловые операции WebHDFS, Upload/Download Throughput, отказы.
- YARN Queues: [`yarn_explorer_queues.json`](../monitoring/grafana/dashboards/yarn_explorer_queues.json) — утилизация очередей Capacity Scheduler, Change Requests.
- Spark & SQL Analytics: [`spark_sql_explorer_analytics.json`](../monitoring/grafana/dashboards/spark_sql_explorer_analytics.json) — Livy-сессии, интерактивный Spark, Trino- и Hive-запросы.

---

## Связанная документация
- [Архитектура платформы и паттерны отказоустойчивости (ARCHITECTURE.md)](ARCHITECTURE.md)
- [Полное руководство по конфигурации параметров YAML и ENV (CONFIGURATION.md)](CONFIGURATION.md)
- [Переход со SQLite на PostgreSQL (production-database.md)](production-database.md)
- [Руководство по интеграции YARN с Ansible AWX (awx-yarn-deployment.md)](awx-yarn-deployment.md)

# 🚀 Руководство администратора платформы (Hadoop Explorer DevOps Hub)

Добро пожаловать в единое руководство администратора и инженера DevOps/SRE по развертыванию, администрированию и поддержке микросервисной платформы **Hadoop Explorer**.

---

## 🧭 Навигатор по сервисам платформы

Платформа включает 5 независимых микросервисов, объединенных общим ядром безопасности (`backend/common`), единой дизайн-системой и ролевой моделью. Для каждого сервиса подготовлено подробное автономное руководство по установке в трех режимах (**Standalone**, **Docker**, **Kubernetes**):

| Сервис | Порт по умолчанию | Назначение | Руководство администратора |
|---|---|---|---|
| **YARN Explorer** | `8001` (или `8000`) | Управление очередями Capacity Scheduler, мониторинг YARN RM HA, Change Requests, раскатка через Ansible AWX | 📖 [docs/yarn-admin-guide.md](yarn-admin-guide.md) |
| **HDFS Explorer** | `8002` (или `8000`) | Менеджер файлов Data Lake, WebHDFS NameNode HA, превью Parquet/ORC, квоты каталогов, Cross-Cluster Copy | 📖 [docs/hdfs-admin-guide.md](hdfs-admin-guide.md) |
| **SQL Explorer** | `8003` (или `8000`) | Аналитическая веб-консоль Trino & Apache Hive, стриминг выборок, отмена запросов, ИИ-ассистент SQL | 📖 [docs/sql-admin-guide.md](sql-admin-guide.md) |
| **Spark Explorer** | `8004` (или `8000`) | Управление интерактивными сессиями Livy (PySpark / Scala), интеграция с YARN и Spark History Server | 📖 [docs/spark-admin-guide.md](spark-admin-guide.md) |
| **Hadoop gRPC Replicator** | `8005` (API/UI)<br>`50051` (gRPC) | Распределенная межкластерная репликация HDFS (Orchestrator, Receiver, Worker), шейпинг WAN, Cron шедулер | 📖 [docs/replicator-admin-guide.md](replicator-admin-guide.md) |

---

## 🏗️ Сводная матрица поддерживаемых режимов развертывания

```
                      ┌─────────────────────────────────────────────────────────────┐
                      │              HADOOP EXPLORER PLATFORM                       │
                      └──────────────────────────────┬──────────────────────────────┘
                                                     │
         ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
         ▼                                           ▼                                           ▼
┌─────────────────────────────────┐ ┌─────────────────────────────────┐ ┌─────────────────────────────────┐
│     1. STANDALONE (systemd)     │ │   2. DOCKER & DOCKER COMPOSE    │ │      3. KUBERNETES & HELM       │
│                                 │ │                                 │ │                                 │
│ - Bare-Metal / Виртуальные маш. │ │ - Контейнеры Python 3.12-slim   │ │ - Зонтичный чарт (Umbrella)     │
│ - Виртуальное окружение venv/uv │ │ - Multi-stage сборка (Node.js)  │ │ - Автономные чарты под каждый   │
│ - Управление через systemctl    │ │ - Оркестрация в docker-compose  │ │ - HPA, Ingress, Cert-Manager    │
│ - Локальный kinit / k5start     │ │ - Автоматический kinit в entry  │ │ - Secrets & ConfigMap для krb5  │
└─────────────────────────────────┘ └─────────────────────────────────┘ └─────────────────────────────────┘
```

---

## 🔒 Единые стандарты безопасности платформы (Security Hardening)

### 1. Ротация и хранение Kerberos Keytab
- Все сервисы поддерживают работу в защищенных Kerberos-периметрах.
- Файлы keytab должны монтироваться в режиме **Read-Only** с правами `chmod 400` и принадлежностью пользователю `appuser` (UID `10001`).
- При контейнеризированном запуске скрипт [`docker/docker-entrypoint.sh`](../docker/docker-entrypoint.sh) автоматически выполняет инициализацию `kinit` и запускает фоновый таймер обновления билета каждые 6 часов.
- В Kubernetes файлы keytab монтируются из `Secret` с флагом `defaultMode: 0400`.

### 2. Секретный ключ JWT (`JWT_SECRET_KEY`)
- В Production-режиме (`debug: false`) платформа **принудительно блокирует запуск**, если `JWT_SECRET_KEY` короче 32 символов или использует плейсхолдеры.
- Для генерации безопасного ключа используйте:
  ```bash
  openssl rand -hex 32
  ```

### 3. База данных сессий: SQLite vs PostgreSQL
- **SQLite** (`sqlite:////app/data/service.db`): применяется по умолчанию для тестирования и Standalone-инсталляций с **1 репликой**.
- **PostgreSQL** (`postgresql://user:pass@pg-host:5432/dbname`): **обязателен** для промышленного развертывания с **2 и более репликами** (горизонтальное масштабирование в K8s / Docker Swarm). Подробное руководство по миграции см. в [docs/production-database.md](production-database.md).

---

## 🌐 Сводная таблица портов и маршрутизации Ingress

| Сервис | Внутренний порт контейнера | Внешний порт (Demo) | Рекомендуемый Host Ingress |
|---|---|---|---|
| **YARN Explorer** | `8000` | `8001` | `https://yarn.company.local` |
| **HDFS Explorer** | `8000` | `8002` | `https://hdfs.company.local` |
| **SQL Explorer** | `8000` | `8003` | `https://sql.company.local` |
| **Spark Explorer** | `8000` | `8004` | `https://spark.company.local` |
| **Replicator Orchestrator** | `8005` | `8005` | `https://replicator.company.local` |
| **Replicator Receiver (gRPC)** | `50051` | `50051` | `grpc://replicator-dc2.company.local:50051` |

---

## 📊 Мониторинг Prometheus и дашборды Grafana

Каждый микросервис платформы оснащен встроенным эндпоинтом `/metrics` (или `/api/v1/metrics`), экспортирующим метрики в формате OpenMetrics.

### Конфигурация Prometheus Scrape Config
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

### Доступные дашборды Grafana
В каталоге `monitoring/grafana/dashboards/` подготовлены преднастроенные дашборды:
- 🚀 **Platform Overview**: [`hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) — сводное здоровье и Golden Signals всех 5 сервисов платформы.
- 🔄 **gRPC Replicator**: [`hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json) — скорость репликации WAN, иерархический шейпинг Token Bucket, воркеры, планировщик Cron.
- 📁 **HDFS Operations**: [`hdfs_explorer_operations.json`](../monitoring/grafana/dashboards/hdfs_explorer_operations.json) — файловые операции WebHDFS, Upload/Download Throughput, отказы.
- ⚡ **YARN Queues**: [`yarn_explorer_queues.json`](../monitoring/grafana/dashboards/yarn_explorer_queues.json) — утилизация очередей Capacity Scheduler, Change Requests.
- 📊 **Spark & SQL Analytics**: [`spark_sql_explorer_analytics.json`](../monitoring/grafana/dashboards/spark_sql_explorer_analytics.json) — Livy сессии, интерактивный Spark, Trino & Hive запросы.

---

## 📚 Связанная документация
- [Архитектура платформы и паттерны отказоустойчивости (ARCHITECTURE.md)](ARCHITECTURE.md)
- [Полное руководство по конфигурации параметров YAML и ENV (CONFIGURATION.md)](CONFIGURATION.md)
- [Руководство по настройке высокодоступной СУБД PostgreSQL (production-database.md)](production-database.md)
- [Руководство по интеграции YARN с Ansible AWX (awx-yarn-deployment.md)](awx-yarn-deployment.md)

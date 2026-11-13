# Руководство администратора платформы Pristan

Документ для администраторов и DevOps/SRE-инженеров: общие для всех сервисов Pristan требования к развертыванию, безопасности, портам и мониторингу, а также ссылки на руководства по каждому сервису.

---

## Сервисы платформы

Платформа состоит из 5 независимых сервисов на Java 21 / Spring Boot 3. Аутентификацию и ролевую модель они берут из общего стартера `backend/zastava`, интерфейс построен на общей дизайн-системе. Руководство по каждому сервису описывает установку в трех режимах: Standalone, Docker, Kubernetes.

| Сервис | Порт по умолчанию | Назначение | Руководство администратора |
|---|---|---|---|
| Artel | `8001` (или `8000`) | Управление очередями Capacity Scheduler, мониторинг YARN RM HA, Change Requests, доставка и применение через Ansible AWX | [docs/yarn-admin-guide.md](yarn-admin-guide.md) |
| Sklad | `8002` (или `8000`) | Менеджер файлов Data Lake, WebHDFS NameNode HA, превью Parquet/ORC, квоты каталогов, Cross-Cluster Copy | [docs/hdfs-admin-guide.md](hdfs-admin-guide.md) |
| Tolmach | `8003` (или `8000`) | Веб-консоль Trino и Apache Hive, потоковая выдача результатов, отмена запросов, ИИ-ассистент SQL | [docs/sql-admin-guide.md](sql-admin-guide.md) |
| Iskra | `8004` (или `8000`) | Управление интерактивными сессиями Livy (PySpark / Scala), интеграция с YARN и Spark History Server | [docs/spark-admin-guide.md](spark-admin-guide.md) |
| Parom | `8005` (API/UI)<br>`50051` (gRPC) | Межкластерная репликация HDFS (Orchestrator и двунаправленные агенты), ограничение полосы WAN, cron-планировщик | [docs/replicator-admin-guide.md](parom-admin-guide.md) |

---

## Режимы развертывания

```
                      ┌─────────────────────────────────────────────────────────────┐
                      │              PRISTAN                       │
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
│ - Локальный kinit / k5start     │ │ - Multi-stage сборка (по желан.)│ │ - См. parom-admin-guide §4 │
└─────────────────────────────────┘ └─────────────────────────────────┘ └─────────────────────────────────┘
```

Replicator Agent, кроме трех режимов выше, запускается внутри кластера Hadoop:
- На DataNode или edge-узле через `hadoop jar`: shaded fat JAR или тонкий JAR с системным `HADOOP_CLASSPATH`, полоса ограничивается `AGENT_MAX_BANDWIDTH_MB_S`.
- В Apache Hadoop YARN: пул воркеров запускает `ParomYarnClient`, ими управляет `ParomApplicationMaster`. HDFS delegation tokens передаются воркерам автоматически, поэтому keytab на NodeManager не нужен. Подробнее: [docs/replicator-admin-guide.md#26-запуск-распределенного-пула-агентов-в-apache-hadoop-yarn](parom-admin-guide.md#26-запуск-распределенного-пула-агентов-в-apache-hadoop-yarn).

---

## Требования безопасности для всех сервисов

### 1. Kerberos keytab
- Все сервисы могут работать в кластерах с Kerberos.
- Монтируйте keytab только на чтение, с правами `chmod 400` и владельцем `appuser` (UID `10001`).
- В контейнере стартер безопасности (`zastava`) сам выполняет вход в Kerberos через Java GSS-API / UGI и обновляет TGT по расписанию.
- В Kubernetes (манифесты Replicator) монтируйте keytab из `Secret` с `defaultMode: 0400`.

### 2. Секретный ключ JWT (`JWT_SECRET_KEY`)
- При `auth.mode`, отличном от `mock`, сервис не запускается, если `JWT_SECRET_KEY` пустой, короче 32 символов или совпадает со встроенным плейсхолдером.
- При `debug: false` сервис также не запускается с ключом, опубликованным в репозитории: значения из `application-mock.yml`, тестов, демо-стендов, удаленных Helm-чартов и примеров документации (список `SecurityConfigurationValidator.PUBLISHED_SECRET_KEYS`). В логе: `zastava.jwt.secret-key is a value published in the Pristan repository ...`.
- При двух и более экземплярах сервиса задайте одинаковый `JWT_SECRET_KEY` на всех экземплярах.
- Сгенерируйте ключ:
  ```bash
  openssl rand -hex 32
  ```

### 3. База данных и хранилище сессий: H2 или PostgreSQL
- По умолчанию сервисы используют H2: Artel — файл `./data/artel`, HDFS, SQL и Iskra — in-memory H2, данные которой теряются при перезапуске (таблица по сервисам — в [ARCHITECTURE.md, раздел 8](ARCHITECTURE.md#8-хранение-данных-storage-layer)). Сессии и отозванные токены по умолчанию хранятся в памяти процесса (`zastava.session-store.type: memory`). Такая конфигурация подходит для одного экземпляра сервиса.
- При двух и более экземплярах сервиса подключите PostgreSQL и перенесите туда хранилище сессий:
  ```bash
  SPRING_DATASOURCE_URL=jdbc:postgresql://<pg_host>:5432/<database>
  SPRING_DATASOURCE_USERNAME=<db_user>
  SPRING_DATASOURCE_PASSWORD=<db_password>
  ZASTAVA_SESSIONSTORE_TYPE=jdbc
  ```
  `<pg_host>` — адрес PostgreSQL, `<database>` — база сервиса, `<db_user>` и `<db_password>` — учетная запись с правами владельца базы. Порядок перехода, привязка клиента к экземпляру для SQL, Spark и Sklad и проверочный список — в [production-database.md](production-database.md).

---

## Порты и маршрутизация Ingress

| Сервис | Внутренний порт контейнера | Внешний порт (Demo) | Рекомендуемый Host Ingress |
|---|---|---|---|
| Artel | `8000` | `8001` | `https://yarn.company.local` |
| Sklad | `8000` | `8002` | `https://hdfs.company.local` |
| Tolmach | `8003` | `8003` | `https://sql.company.local` |
| Iskra | `8004` | `8004` | `https://spark.company.local` |
| Replicator Orchestrator | `8005` | `8005` | `https://replicator.company.local` |
| Replicator Agent (gRPC) | `50051` | `50051` | `grpc://replicator-dc2.company.local:50051` |

---

## Мониторинг Prometheus и дашборды Grafana

YARN, HDFS, SQL и Iskra отдают метрики на `GET /actuator/prometheus` без аутентификации; оркестратор Replicator — на `GET /metrics` (требует аутентификации) и `GET /actuator/prometheus` (только Micrometer). Состав метрик и доступ описаны в [CONFIGURATION.md, раздел 9](CONFIGURATION.md#9-мониторинг-prometheus-и-дашборды-grafana).

### Конфигурация сбора метрик в Prometheus
```yaml
scrape_configs:
  - job_name: 'pristan'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets:
          - 'yarn.company.local:8000'
          - 'hdfs.company.local:8000'
          - 'sql.company.local:8003'
          - 'spark.company.local:8004'
```

### Дашборды Grafana
Дашборды лежат в каталоге `monitoring/grafana/dashboards/`:
- Platform Overview: [`pristan_overview.json`](../monitoring/grafana/dashboards/pristan_overview.json) — HTTP golden signals YARN, HDFS, SQL и Iskra, входы, ответы 429 ограничителя частоты и ответы 5xx, сводка Replicator.
- gRPC Replicator: [`parom_overview.json`](../monitoring/grafana/dashboards/parom_overview.json) — скорость репликации WAN, иерархический шейпинг Token Bucket, агенты репликации, планировщик Cron.
- HDFS Operations: [`sklad_operations.json`](../monitoring/grafana/dashboards/sklad_operations.json) — операции клиента HDFS, трафик upload/download, отказы.
- Artel: [`artel_queues.json`](../monitoring/grafana/dashboards/artel_queues.json) — переходы заявок Change Request, задержка API.
- Spark & SQL Analytics: [`iskra_tolmach_analytics.json`](../monitoring/grafana/dashboards/iskra_tolmach_analytics.json) — активные сессии Spark, длительность инструкций, темп и длительность SQL-запросов.

---

## Связанная документация
- [Архитектура платформы и паттерны отказоустойчивости (ARCHITECTURE.md)](ARCHITECTURE.md)
- [Полное руководство по конфигурации параметров YAML и ENV (CONFIGURATION.md)](CONFIGURATION.md)
- [Переход со SQLite на PostgreSQL (production-database.md)](production-database.md)
- [Руководство по интеграции YARN с Ansible AWX (awx-yarn-deployment.md)](awx-yarn-deployment.md)

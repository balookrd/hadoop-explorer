<div align="center">

<img src="images/logo_white.png" alt="Hadoop Explorer Platform" width="380" />

<p><strong>Единая корпоративная веб-платформа для управления экосистемой Apache Hadoop</strong></p>

[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](#-тестирование-платформы)
[![Java](https://img.shields.io/badge/Java-21%20LTS-blue.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Maven](https://img.shields.io/badge/Maven-3.9-red.svg)](https://maven.apache.org/)
[![Frontend](https://img.shields.io/badge/Frontend-Svelte%205%20%7C%20Tailwind%204-orange.svg)](https://svelte.dev/)
[![Docker](https://img.shields.io/badge/Docker-Multi--Stage-2496ED.svg)](docker/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm%20Charts-326CE5.svg)](helm/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards%20Ready-F46800.svg)](monitoring/grafana/dashboards/hadoop_explorer_overview.json)

</div>

---

## 📑 Содержание

- [Обзор платформы](#-обзор-платформы)
- [Архитектура монорепозитория](#-архитектура-монорепозитория)
- [Детальная системная архитектура (docs/ARCHITECTURE.md)](docs/ARCHITECTURE.md)
- [Руководство по конфигурации компонентов (docs/CONFIGURATION.md)](docs/CONFIGURATION.md)
- 🚀 **Руководства администратора и DevOps (Standalone / Docker / K8s)**:
  - [Единый DevOps Hub платформы (docs/admin-guide.md)](docs/admin-guide.md)
  - [Администрирование YARN Explorer (docs/yarn-admin-guide.md)](docs/yarn-admin-guide.md)
  - [Администрирование HDFS Explorer (docs/hdfs-admin-guide.md)](docs/hdfs-admin-guide.md)
  - [Администрирование SQL Explorer (docs/sql-admin-guide.md)](docs/sql-admin-guide.md)
  - [Администрирование Spark Explorer (docs/spark-admin-guide.md)](docs/spark-admin-guide.md)
  - [Администрирование Hadoop gRPC Replicator (docs/replicator-admin-guide.md)](docs/replicator-admin-guide.md)
  - [Регламент Disaster Recovery и защита от Split-Brain (docs/replicator-disaster-recovery-guide.md)](docs/replicator-disaster-recovery-guide.md)
  - [Отказоустойчивость и распределенный лизинг Replicator (docs/replicator-fault-tolerance.md)](docs/replicator-fault-tolerance.md)
  - [📊 Презентация Replicator для команды (docs/replicator-presentation.md)](docs/replicator-presentation.md) ([Интерактивный слайддек](docs/replicator-presentation.html))
- 📖 **Руководства пользователя**:
  - [Руководство пользователя YARN Explorer (docs/yarn-user-guide.md)](docs/yarn-user-guide.md)
  - [Автоматизированная доставка и применение через Ansible AWX (docs/awx-yarn-deployment.md)](docs/awx-yarn-deployment.md)
  - [Руководство пользователя HDFS Explorer (docs/hdfs-user-guide.md)](docs/hdfs-user-guide.md)
  - [Руководство пользователя SQL Explorer (docs/sql-user-guide.md)](docs/sql-user-guide.md)
  - [Руководство пользователя Spark Explorer (docs/spark-user-guide.md)](docs/spark-user-guide.md)
  - [Руководство пользователя Hadoop gRPC Replicator (docs/replicator-user-guide.md)](docs/replicator-user-guide.md)
  - [Работа Hive и Spark без кластера YARN (docs/hive-spark-without-yarn.md)](docs/hive-spark-without-yarn.md)
- [Выделенные общие модули](#-выделенные-общие-модули)
- [Компоненты платформы](#-компоненты-платформы)
- [Мониторинг, Prometheus и Grafana Dashboards](#-мониторинг-prometheus-и-grafana-dashboards)
- [Быстрый старт: Раздельные демо-стенды](#-быстрый-старт-раздельные-демо-стенды)
- [Сборка Docker-контейнеров](#-сборка-docker-контейнеров)
- [Развертывание в Kubernetes (Helm)](#️-развертывание-в-kubernetes-helm)
- [Тестирование платформы](#-тестирование-платформы)
- [CLI команды (Makefile)](#-cli-команды-makefile)
- [Структура каталогов](#-структура-каталогов)
- [Лицензия](#-лицензия)

---

## 🎯 Обзор платформы

**Hadoop Explorer Platform** объединяет в единый монорепозиторий пять ключевых корпоративных инструментов для работы с Big Data инфраструктурой:

1. **YARN Explorer** — интерактивная консоль для мониторинга кластеров, моделирования весов и управления иерархией очередей **Apache Hadoop YARN Capacity Scheduler**, версионированием и согласованием заявок на изменение (Change Requests) с защитой Four-Eyes, а также автоматизированной доставкой и горячим применением (`yarn rmadmin -refreshQueues`) через **Ansible AWX** с автоматическим откатом (Rollback).
2. **HDFS Explorer** — файловый менеджер распределенного хранилища Apache Hadoop (WebHDFS & HttpFS) с NameNode HA и защитой Circuit Breaker. Поддерживает виртуализацию списков файлов для мгновенной отрисовки директорий любого масштаба, превью Parquet, ORC, CSV, JSON, списки контроля доступа (ACL), квоты директорий и имперсонацию пользователей (`doAs`).
3. **SQL Explorer** — аналитический веб-редактор запросов к **Trino** и **Apache Hive (HiveServer2 / Cloudera / Hortonworks)** на базе Monaco Editor с автодополнением, TTL-кэшированием метаданных, историей запросов, асинхронным выполнением, встроенным AI-помощником и персистентным хранением рабочих пространств пользователей.
4. **Spark Explorer** — интерактивная веб-студия разработки и аналитики для **Apache Spark** (PySpark, Scala Spark, Spark SQL) через **Apache Livy** на кластерах YARN и Kubernetes с защитой от сбоев через Circuit Breaker. Поддерживает управление интерактивными сессиями, выбор версий Spark/Python, подключение каталогов Hive Metastore / Iceberg, визуальный DAG ETL конструктор с топологической валидацией циклов.
5. **Hadoop gRPC Replicator** — высокоскоростная межкластерная репликация HDFS (DC1 → DC2) с глобальным ограничением полосы пропускания (Hierarchical Token Bucket Throttler), бинарным gRPC-стримингом, атомарным staging/rename, инкрементальным Snapshot Diff, Kerberos-изоляцией (включая выполнение от системной техучетки) и метриками Prometheus.

Каждое приложение реализовано на базе **Java 21 LTS** и **Spring Boot 3.3.4**, собирается в **независимый легковесный Docker-контейнер**, развертывается автономно или в составе единого **Umbrella Helm Chart**, а также запускается в собственном **раздельном демо-стенде**.

---

## 🏛️ Архитектура монорепозитория

```
hadoop-explorer/
├── backend/                  # Java 21 LTS (Spring Boot 3.3.4, Maven)
│   ├── common-security-starter/ # Ядро безопасности: SPNEGO, LDAP, JWT, CSRF, RateLimit, L1/L2
│   ├── yarn/                 # Сервис YARN Explorer (Capacity Scheduler, RM HA Failover)
│   ├── hdfs/                 # Сервис HDFS Explorer (HA NameNode, Parquet/ORC Preview)
│   ├── sql/                  # Сервис SQL Explorer (Trino, Hive, AI Assistant)
│   ├── spark/                # Сервис Spark Explorer (Livy, PySpark, Scala, DAG Pipelines)
│   └── replicator/           # Сервис Hadoop gRPC Replicator
│       ├── orchestrator/     # Replicator Orchestrator API (Java 21 / Spring Boot 3)
│       └── agent/            # Нативный Replicator gRPC Worker Daemon (Java 21)
│
├── ansible/                  # ─── Автоматизация деплоя и применения (AWX) ───
│   ├── playbooks/            # deploy_capacity_scheduler.yml (Job Template)
│   ├── roles/                # yarn_capacity_scheduler (Backup, Deploy, refreshQueues, Rollback)
│   └── inventory.example.ini
│
├── frontend/                 # ─── Клиентские SPA приложения (Svelte 5) ───
│   ├── common/               # Общие UI-компоненты, API-клиенты, типы (types/generated/)
│   ├── apps/
│   │   ├── yarn/             # Frontend YARN Explorer (Svelte 5 + Tailwind 4)
│   │   ├── hdfs/             # Frontend HDFS Explorer (Svelte 5 + Tailwind 4 + виртуализация)
│   │   ├── sql/              # Frontend SQL Explorer (Svelte 5 + Tailwind 4 + Monaco Editor)
│   │   ├── spark/            # Frontend Spark Explorer (Svelte 5 + Tailwind 4 + Monaco + DAG)
│   │   └── replicator/       # Frontend Replicator (Svelte 5 + Tailwind 4)
│   └── package.json          # NPM Workspaces монорепозитория
│
├── monitoring/               # ─── Мониторинг и наблюдаемость (Observability) ───
│   └── grafana/
│       ├── dashboards/       # Готовые JSON-дашборды (HTTP Signals, Circuit Breakers, Auth)
│       └── provisioning/     # Автопровижининг дашбордов для Docker Compose и Kubernetes
│
├── docker/                   # ─── Производственные Dockerfile на Java 21 ───
│   ├── Dockerfile.yarn-java
│   ├── Dockerfile.hdfs-java
│   ├── Dockerfile.sql-java
│   ├── Dockerfile.spark-java
│   ├── Dockerfile.replicator-orchestrator
│   └── Dockerfile.replicator-agent
│
├── helm/                     # Helm Charts для оркестрации в Kubernetes
│   ├── hadoop-explorer/      # Umbrella Chart
│   └── charts/               # yarn-explorer, hdfs-explorer, sql-explorer, spark-explorer
│
├── demo/                     # ─── Изолированные демонстрационные стенды ───
│   ├── infra/                # MIT Kerberos KDC + OpenLDAP
│   ├── yarn/                 # Стенд YARN (2 кластера YARN RM) -> :8001
│   ├── hdfs/                 # Стенд HDFS (2 кластера WebHDFS) -> :8002
│   ├── sql/                  # Стенд SQL (Postgres, Hive, Trino) -> :8003
│   ├── spark/                # Стенд Spark (Livy + Hive Metastore + YARN + HDFS) -> :8004
│   ├── replicator/           # Стенд Replicator (DC1 + DC2 + Orchestrator) -> :8005
│   └── all/                  # Единый запуск всех стендов платформы
│
├── scripts/                  # Скрипты сборки, тестов и AST-индексации
├── Makefile                  # Единый CLI для сборки, тестирования и запуска стендов
└── README.md
```

---

## 📦 Выделенные общие модули

### 1. `backend/common-security-starter` (Java 21 / Spring Boot 3 Стартер Безопасности)
- **Аутентификация и SSO**:
  - Kerberos SPNEGO SSO через HTTP-заголовок `Authorization: Negotiate <ticket>` с нативным Java GSS-API.
  - Полнофункциональный клиент LDAPS / OpenLDAP / Active Directory с пулом соединений, валидацией групп и защитой от LDAP Injection (CWE-90).
  - Поддержка тестовых и локальных Mock-пользователей для автономной разработки.
- **Сессии и управление токенами**:
  - Единая архитектура Cookie-First (Zero LocalStorage): токены сессий передаются исключительно в `HttpOnly`, `SameSite=Lax`, `Secure` cookies.
  - Двухуровневое хранилище сессий: L1 In-Memory кэш Caffeine + L2 персистентная БД (H2 / PostgreSQL) с проверкой отзыва токенов (CWE-613).
  - Строгая CSRF-защита (SameSite, Origin/Referer whitelist, заголовок `X-Requested-With`).
- **Отказоустойчивость и безопасность**:
  - `SimpleCircuitBreaker` (CLOSED, OPEN, HALF_OPEN) для защиты кластерных вызовов Hadoop и мгновенного HA Failover.
  - Контроль частоты запросов (Rate Limiting) для защиты от перегрузок и DoS.
  - AOP JSON-аудит ключевых событий авторизации и мутирующих операций.
  - Централизованный `GlobalExceptionHandler` (CWE-209 защита со структурированным ответом и кодами ошибок).
  - Автоматическая раздача собранных SPA Svelte 5 через Spring Boot Web MVC (`SpaController`).

### 2. `frontend/common` и архитектура SPA
- **`api/client.ts`**: Базовый HTTP fetcher с Cookie-first подходом (Zero LocalStorage для защиты от XSS), поддержкой Sliding Sessions, автоматическим добавлением заголовков CSRF (`X-Requested-With`), `credentials: include` и методом Kerberos SSO Negotiate.
- **Унифицированный UI/UX на Svelte 5 (Runes) & Tailwind CSS 4**:
  - Быстрая **виртуализация списков файлов** (`FileList.svelte`) с O(1) DOM-узлов для директорий любого объема.
  - **Интерактивное изменение размера областей (Resizable Split Panes)** с перетаскиванием мыши и сохранением пропорций для боковых панелей каталогов, редактора кода и результатов в SQL и Spark Explorer.
  - **Lazy Loading (Code-Splitting)** всех тяжелых модальных окон и диалоговых панелей через асинхронные импорты.
  - **Унифицированное закрытие модальных окон** по клику вне диалога (Backdrop Overlay) и по нажатию клавиши `Escape`.
- **`types/auth.ts`**: Унифицированные TypeScript интерфейсы сессий и ролей пользователей.
- **`components/`**: Переиспользуемые Svelte 5 компоненты статусов (`StatusBadge`), модальных окон (`LoginModal`) и всплывающих уведомлений (`NotificationToast`).

---

## 🧩 Компоненты платформы

| Приложение | Веб-интерфейс | Проверка Health | Метрики Prometheus | Контейнер | Helm Chart |
|---|---|---|---|---|---|
| **YARN Explorer** | `http://localhost:8001` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/yarn:latest` | `helm/charts/yarn-explorer` |
| **HDFS Explorer** | `http://localhost:8002` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/hdfs:latest` | `helm/charts/hdfs-explorer` |
| **SQL Explorer** | `http://localhost:8003` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/sql:latest` | `helm/charts/sql-explorer` |
| **Spark Explorer** | `http://localhost:8004` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/spark:latest` | `helm/charts/spark-explorer` |
| **Hadoop gRPC Replicator** | `http://localhost:8005` | `GET /health` | `GET /metrics` | `hadoop-explorer/replicator:latest` | `helm/charts/replicator` |

---

## 📊 Мониторинг, Prometheus и Grafana Dashboards

Все микросервисы платформы оснащены встроенным легковесным коллектором метрик в формате OpenMetrics / Prometheus, доступным на эндпоинтах `GET /metrics` и `GET /api/v1/metrics`.

### 1. Состав экспортируемых метрик
- **HTTP Golden Signals**:
  - `http_requests_total{app="...", method="...", path="...", status="..."}` — счетчик запросов с нормализацией URI.
  - `http_request_duration_seconds` (гистограмма задержек p50, p90, p99 с бакетами от `5ms` до `10s`).
  - `http_requests_in_progress{app="..."}` — количество запросов в параллельной обработке (concurrency).
- **Отказоустойчивость и зависимости**:
  - `hadoop_circuit_breaker_state{name="..."}` (0=CLOSED, 1=HALF_OPEN, 2=OPEN) — состояние автоматов защиты NameNode, YARN RM, Livy, Trino.
  - `hadoop_circuit_breaker_calls_total{name="...", status="success|failed|rejected"}` — статистика вызовов.
  - `hadoop_retry_attempts_total{app="...", operation="...", status="retry|exhausted|success"}` — учет срабатываний retry с backoff.
- **Безопасность и сбои**:
  - `hadoop_auth_attempts_total{app="...", provider="ldap|kerberos|mock", status="success|failure"}` — аудит попыток аутентификации.
  - `hadoop_rate_limit_blocks_total{app="..."}` — количество заблокированных по частоте запросов (429).
  - `hadoop_exceptions_total{app="...", exception_type="..."}` — учет непредвиденных 500 ошибок (CWE-209 защита).

### 2. Готовые дашборды для Grafana
В репозитории подготовлен production-grade дашборд для визуализации состояния всей платформы:
- 📁 **JSON-модель**: [`monitoring/grafana/dashboards/hadoop_explorer_overview.json`](monitoring/grafana/dashboards/hadoop_explorer_overview.json)
- ⚙️ **Файл автопровижининга**: [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](monitoring/grafana/provisioning/dashboards/dashboards.yaml)

> 📖 **Подробное описание параметров, форматов файлов и переменных окружения приведено в [Руководстве по конфигурации (docs/CONFIGURATION.md)](docs/CONFIGURATION.md).**

---

## 🚀 Быстрый старт: Раздельные демо-стенды

Каждый сервис укомплектован изолированным демонстрационным стендом в Docker Compose с тестовым KDC (Kerberos), OpenLDAP и необходимым окружением.

### 1. Демо-стенд YARN Explorer
Включает: KDC, OpenLDAP, 2 кластера YARN ResourceManager с иерархией очередей Capacity Scheduler и YARN Explorer:
```bash
make demo-yarn
# Веб-интерфейс: http://localhost:8001
# Остановка: make demo-yarn-stop
```

### 2. Демо-стенд HDFS Explorer
Включает: KDC, OpenLDAP, 2 кластера DataLake с Kerberized WebHDFS и сервис HDFS Explorer:
```bash
make demo-hdfs
# Веб-интерфейс: http://localhost:8002
# Остановка: make demo-hdfs-stop
```

### 3. Демо-стенд SQL Explorer
Включает: KDC, OpenLDAP, PostgreSQL, Hive Metastore, HiveServer2, Trino Coordinator и SQL Explorer:
```bash
make demo-sql
# Веб-интерфейс: http://localhost:8003
# Остановка: make demo-sql-stop
```

### 4. Демо-стенд Spark Explorer
Включает: KDC, OpenLDAP, Apache Livy, PostgreSQL Hive Metastore, Hadoop HDFS, YARN Resource Manager и сервис Spark Explorer:
```bash
make demo-spark
# Веб-интерфейс: http://localhost:8004
# Остановка: make demo-spark-stop
```

### 5. Демо-стенд Hadoop gRPC Replicator
Включает: Orchestrator (:8005) с иерархическим Token Bucket и шедулером, Receiver (:50051) и Worker daemon:
```bash
make demo-replicator
# Веб-интерфейс: http://localhost:8005
# Остановка: make demo-replicator-stop
```

### 6. Объединенный запуск всех стендов
```bash
make demo-all
# Остановка: make demo-all-stop
```

> 💡 **Подробное руководство по работе без YARN**: Описание архитектуры выполнения запросов Hive и Spark в легковесных стендах без запуска ResourceManager/NodeManager см. в [docs/hive-spark-without-yarn.md](docs/hive-spark-without-yarn.md).

### 🔑 Тестовые учетные записи (LDAP)
| Пользователь | Пароль | Роли и группы | Назначение |
|---|---|---|---|
| `admin_user` / `admin` | `password123` | `hadoop-admins`, `ADMIN` | Полный административный доступ |
| `analyst_user` / `analyst` | `password123` | `analytics`, `READER` | Доступ только для чтения / аналитики |
| `engineer_user` / `engineer` | `password123` | `data-engineers`, `WRITER` | Доступ инженера данных (запись/чтение) |

---

## ☕ Сборка Java JAR-пакетов и CI/CD

Платформа собирается как единый Maven Reactor (Java 21 LTS):

```bash
# Сборка всех JAR-пакетов платформы (fat JAR / shaded JAR):
make build-java
# или напрямую через Maven:
mvn clean package -DskipTests

# Запуск всех тестов Java сервисов:
make test-java
# или:
mvn test
```

В репозитории настроен автоматический CI/CD на базе GitHub Actions:
- **Nightly Release** (`.github/workflows/nightly-release.yml`): при каждом коммите в ветку `main` автоматически прогоняет тесты, собирает JAR-пакеты, генерирует список изменений (`git-cliff`) и публикует pre-release с тегом `nightly`, прикрепляя все JAR-архивы и `SHA256SUMS.txt`.
- **CI** (`.github/workflows/ci.yml`): на Pull Request и в ветках выполняет полную валидацию, модульные/интеграционные тесты и тестовую сборку пакетов.
- **Tag Release** (`.github/workflows/tag-release.yml`): при пуше тега `v*` выполняет релизную сборку и создает стабильный релиз на GitHub.
- **Cleanup Artifacts** (`.github/workflows/cleanup-artifacts.yml`): ежедневная очистка устаревших артефактов Actions по расписанию.

---

## 🐳 Сборка Docker-контейнеров

Все образы базируются на легковесном образе `eclipse-temurin:21-jre-jammy`:
1. Установка системных библиотек Kerberos (`krb5-user`).
2. Копирование собранного Spring Boot Fat JAR и фронтенд статики.
3. Запуск под непривилегированным пользователем `appuser (UID 10001)`.

```bash
# Сборка всех контейнеров платформы
make build

# Либо по отдельности:
make build-yarn     # hadoop-explorer/yarn:latest
make build-hdfs     # hadoop-explorer/hdfs:latest
make build-sql      # hadoop-explorer/sql:latest
make build-spark    # hadoop-explorer/spark:latest
make build-replicator # hadoop-explorer/replicator-orchestrator:latest
```

Прямой запуск через Docker CLI:
```bash
docker build -t hadoop-explorer/yarn:latest -f docker/Dockerfile.yarn-java .
docker build -t hadoop-explorer/hdfs:latest -f docker/Dockerfile.hdfs-java .
docker build -t hadoop-explorer/sql:latest -f docker/Dockerfile.sql-java .
docker build -t hadoop-explorer/spark:latest -f docker/Dockerfile.spark-java .
docker build -t hadoop-explorer/replicator-orchestrator:latest -f docker/Dockerfile.replicator-orchestrator .
```

---

## ☸️ Развертывание в Kubernetes (Helm)

### Вариант 1: Единая платформа (Umbrella Chart)

Развертывание всех компонентов платформы одной командой:
```bash
helm dependency update helm/hadoop-explorer
helm install hadoop-explorer helm/hadoop-explorer -n hadoop --create-namespace
```

Выборочное включение компонентов через `values.yaml`:
```yaml
yarn-explorer:
  enabled: true

hdfs-explorer:
  enabled: true

sql-explorer:
  enabled: true

spark-explorer:
  enabled: true
```

Либо через параметры командной строки:
```bash
helm install hadoop-explorer helm/hadoop-explorer \
  --set yarn-explorer.enabled=true \
  --set hdfs-explorer.enabled=true \
  --set sql-explorer.enabled=true \
  --set spark-explorer.enabled=true
```

### Вариант 2: Автономные чарты

Каждое приложение можно установить в кластер независимо:
```bash
helm install yarn-explorer helm/charts/yarn-explorer -n hadoop
helm install hdfs-explorer helm/charts/hdfs-explorer -n hadoop
helm install sql-explorer helm/charts/sql-explorer -n hadoop
helm install spark-explorer helm/charts/spark-explorer -n hadoop
```

> [!NOTE]
> **Production-стандарты в Helm-чартах**:
> - **Сетевая изоляция (NetworkPolicy)**: включена по умолчанию во всех чартах с ограничением ingress-трафика от Ingress-контроллера.
> - **Отказоустойчивость (PodDisruptionBudget)**: активируется автоматически при `replicaCount > 1`.
> - **High Availability (HA)**: для горизонтального масштабирования (2+ реплики) используйте внешний PostgreSQL вместо локального SQLite. Подробное руководство см. в [docs/production-database.md](docs/production-database.md).

Проверка синтаксиса чартов:
```bash
make helm-lint
```

---

## 🧪 Тестирование платформы

Все тесты (**347 тестов**: 255 бэкенд + 92 Frontend UI Vitest) успешно проходят комплексную проверку:
- **Frontend UI & Static Suite**: 92 теста Vitest + строгий `svelte-check` (статическая верификация контрактов и типов во всех 5 SPA, компонентные тесты модальных окон, тулбаров, метрик кластера, партиций, панелей diff, каталогов и файловых списков в HDFS, Spark, SQL, YARN, Replicator, тестирование общих компонентов `Header`, `LoginModal`, `StatusBadge`, `Modal`, `NotificationToast`, а также Playwright E2E с Zero Console Errors).
- **YARN Explorer**: 65 тестов (Capacity Scheduler валидация, балансировка, Draft Diff, XML Generation, RM HA failover, метрики кластера, Change Requests, аудит, L1 кэш токенов, Readiness / Healthz, Distributed Lock, Circuit Breaker).
- **HDFS Explorer**: 86 тестов (NameNode HA Failover, WebHDFS exception mapping, ContentSummary квоты, ACL, API, Readiness / Healthz, Security, CSP & Security Headers, CSRF, Common Modules, Parquet/ORC Preview со schema footer reader, Cross-Cluster Copy, Circuit Breaker + Prometheus metrics, Retry с backoff, Global Exception Handlers, Distributed Lock на БД, Rate Limiter).
- **SQL Explorer**: 42 теста (Catalog API валидация и эндпоинты, Trino/Hive движки с отменой запросов и стримингом, TTL-кэширование метаданных, AI сервис, токены, CSRF, ACL кластеров, Crash Recovery, Readiness / Healthz, SqlUserWorkspace).
- **Spark Explorer**: 24 теста (Livy клиент полного цикла с отменой statement и логами, интерактивные сессии, автоостановка сессий при logout, Pydantic валидаторы, MockSparkEngine, User Workspace, TTL-кэширование метаданных, Crash Recovery, Readiness / Healthz, Circuit Breaker).
- **Hadoop gRPC Replicator**: 38 тестов (Protobuf gRPC контракт, Hierarchical Token Bucket, потоковый Receiver, KerberosContextManager изоляция KRB5CCNAME, Snapshot Diff парсер, Prometheus метрики, Cron Scheduler демон, статус `SCHEDULED` для периодических задач, полная история запусков `JobRun` со статистикой, настраиваемая глубина истории `history_retention_runs` с авто-прунингом, RBAC изоляция задач, матрица доступности действий жизненного цикла, фильтрация по статусам и авторам).

```bash
# Запуск всех 347 тестов платформы (Backend + Frontend UI)
make test

# Тестирование интерфейса фронтенда:
make test-ui        # svelte-check по 5 SPA + 92 теста Vitest
make frontend-check # проверка типов svelte-check
make frontend-test  # юнит и компонентные тесты Vitest (92 теста)
make frontend-e2e   # E2E тесты Playwright (Chromium)

# Тестирование бэкенда (Java 21 / Spring Boot 3):
make test-java        # Все Java тесты платформы
make test-security-starter # Тесты стартера безопасности (common-security-starter)
make test-yarn        # Тесты сервиса YARN Explorer (Java 21)
make test-hdfs        # Тесты сервиса HDFS Explorer (Java 21)
make test-sql         # Тесты сервиса SQL Explorer (Java 21)
make test-spark       # Тесты сервиса Spark Explorer (Java 21)
make test-replicator  # Тесты сервиса Replicator (Java 21)
```

---

## 🛠️ CLI команды (Makefile)

| Команда | Описание |
|---|---|
| `make test` | Запуск всех модульных, компонентных и интеграционных тестов (Java + UI) |
| `make test-java` | Запуск всех тестов Java 21 сервисов бэкенда |
| `make test-security-starter` | Запуск тестов стартера безопасности `common-security-starter` |
| `make test-yarn` | Запуск тестов сервиса YARN Explorer (Java 21) |
| `make test-hdfs` | Запуск тестов сервиса HDFS Explorer (Java 21) |
| `make test-sql` | Запуск тестов сервиса SQL Explorer (Java 21) |
| `make test-spark` | Запуск тестов сервиса Spark Explorer (Java 21) |
| `make test-replicator` | Запуск тестов сервиса Hadoop gRPC Replicator (Java 21) |
| `make test-ui` | Запуск тестов фронтенда Vitest в Svelte 5 приложениях |
| `make frontend-install` | Установка NPM зависимостей фронтенда |
| `make frontend-check` | Статическая проверка типов Svelte 5 во всех 5 SPA (`svelte-check`) |
| `make frontend-test` | Запуск компонентных и юнит-тестов фронтенда |
| `make frontend-e2e` | Запуск браузерных E2E тестов Playwright |
| `make frontend-build` | Компиляция всех 5 SPA фронтендов через Vite |
| `make build-java` | Сборка всех Java JAR-пакетов платформы (Maven Reactor) |
| `make build-security-starter` | Сборка JAR-пакета `common-security-starter` |
| `make build` | Сборка Docker-образов всех приложений платформы |
| `make build-yarn` | Сборка Docker-образа YARN Explorer (Java 21) |
| `make build-hdfs` | Сборка Docker-образа HDFS Explorer (Java 21) |
| `make build-sql` | Сборка Docker-образа SQL Explorer (Java 21) |
| `make build-spark` | Сборка Docker-образа Spark Explorer (Java 21) |
| `make build-replicator` | Сборка Docker-образа Hadoop gRPC Replicator (Java 21) |
| `make demo-yarn` | Запуск демо-стенда YARN Explorer (`:8001`) |
| `make demo-yarn-stop` | Остановка демо-стенда YARN Explorer |
| `make demo-hdfs` | Запуск демо-стенда HDFS Explorer (`:8002`) |
| `make demo-hdfs-stop` | Остановка демо-стенда HDFS Explorer |
| `make demo-sql` | Запуск демо-стенда SQL Explorer (`:8003`) |
| `make demo-sql-stop` | Остановка демо-стенда SQL Explorer |
| `make demo-spark` | Запуск демо-стенда Spark Explorer (`:8004`) |
| `make demo-spark-stop` | Остановка демо-стенда Spark Explorer |
| `make demo-all` | Запуск объединенного демо-стенда платформы |
| `make demo-all-stop` | Остановка объединенного демо-стенда |
| `make helm-lint` | Валидация синтаксиса всех Helm-чартов |
| `make helm-package` | Упаковка чартов платформы в `.tgz` архивы |
| `make java-index` | Построение автономного AST-индекса Java для поиска символов |
| `make java-query Q="..."` | Структурный поиск по Java AST-индексу (класс, метод, вызовы, тесты) |


---

## 📄 Лицензия

Распространяется под лицензией Apache License 2.0.

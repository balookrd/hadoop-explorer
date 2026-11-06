<div align="center">

<img src="images/logo_white.png" alt="Hadoop Explorer Platform" width="380" />

<p>Веб-интерфейсы для YARN, HDFS, SQL (Trino, Hive), Spark и межкластерной репликации HDFS</p>

[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](#запуск-тестов)
[![Java](https://img.shields.io/badge/Java-21%20LTS-blue.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Maven](https://img.shields.io/badge/Maven-3.9-red.svg)](https://maven.apache.org/)
[![Frontend](https://img.shields.io/badge/Frontend-Svelte%205%20%7C%20Tailwind%204-orange.svg)](https://svelte.dev/)
[![Docker](https://img.shields.io/badge/Docker-Multi--Stage-2496ED.svg)](docker/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm%20Charts-326CE5.svg)](helm/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards%20Ready-F46800.svg)](monitoring/grafana/dashboards/hadoop_explorer_overview.json)

</div>

---

## Содержание

- [Состав платформы](#состав-платформы)
- [Структура монорепозитория](#структура-монорепозитория)
- [Детальная системная архитектура (docs/ARCHITECTURE.md)](docs/ARCHITECTURE.md)
- [Руководство по конфигурации компонентов (docs/CONFIGURATION.md)](docs/CONFIGURATION.md)
- Руководства администратора и DevOps (standalone, Docker, Kubernetes):
  - [Общее руководство администратора платформы (docs/admin-guide.md)](docs/admin-guide.md)
  - [Администрирование YARN Explorer (docs/yarn-admin-guide.md)](docs/yarn-admin-guide.md)
  - [Администрирование HDFS Explorer (docs/hdfs-admin-guide.md)](docs/hdfs-admin-guide.md)
  - [Администрирование SQL Explorer (docs/sql-admin-guide.md)](docs/sql-admin-guide.md)
  - [Администрирование Spark Explorer (docs/spark-admin-guide.md)](docs/spark-admin-guide.md)
  - [Администрирование Hadoop gRPC Replicator (docs/replicator-admin-guide.md)](docs/replicator-admin-guide.md)
  - [Регламент Disaster Recovery и защита от split-brain (docs/replicator-disaster-recovery-guide.md)](docs/replicator-disaster-recovery-guide.md)
  - [Отказоустойчивость и распределенный лизинг Replicator (docs/replicator-fault-tolerance.md)](docs/replicator-fault-tolerance.md)
  - [Презентация Replicator для команды (docs/replicator-presentation.md)](docs/replicator-presentation.md) ([HTML](docs/replicator-presentation.html), [PPTX](docs/replicator-presentation.pptx))
- Руководства пользователя:
  - [Руководство пользователя YARN Explorer (docs/yarn-user-guide.md)](docs/yarn-user-guide.md)
  - [Доставка и применение конфигурации очередей через Ansible AWX (docs/awx-yarn-deployment.md)](docs/awx-yarn-deployment.md)
  - [Руководство пользователя HDFS Explorer (docs/hdfs-user-guide.md)](docs/hdfs-user-guide.md)
  - [Руководство пользователя SQL Explorer (docs/sql-user-guide.md)](docs/sql-user-guide.md)
  - [Руководство пользователя Spark Explorer (docs/spark-user-guide.md)](docs/spark-user-guide.md)
  - [Руководство пользователя Hadoop gRPC Replicator (docs/replicator-user-guide.md)](docs/replicator-user-guide.md)
  - [Работа Hive и Spark без кластера YARN (docs/hive-spark-without-yarn.md)](docs/hive-spark-without-yarn.md)
- [Общие модули](#общие-модули)
- [Адреса, health-check и образы сервисов](#адреса-health-check-и-образы-сервисов)
- [Метрики Prometheus и дашборды Grafana](#метрики-prometheus-и-дашборды-grafana)
- [Запуск демо-стендов](#запуск-демо-стендов)
- [Сборка JAR-пакетов и CI/CD](#сборка-jar-пакетов-и-cicd)
- [Сборка Docker-образов](#сборка-docker-образов)
- [Развертывание в Kubernetes (Helm)](#развертывание-в-kubernetes-helm)
- [Запуск тестов](#запуск-тестов)
- [Цели Makefile](#цели-makefile)
- [Лицензия](#лицензия)

---

## Состав платформы

Hadoop Explorer Platform — монорепозиторий из пяти сервисов для работы с инфраструктурой Apache Hadoop:

1. YARN Explorer — консоль мониторинга кластеров и управления иерархией очередей Apache Hadoop YARN Capacity Scheduler: моделирование весов, версионирование конфигурации, заявки на изменение (Change Requests) с согласованием по принципу Four-Eyes, доставка и применение конфигурации (`yarn rmadmin -refreshQueues`) через Ansible AWX с автоматическим откатом.
2. HDFS Explorer — файловый менеджер HDFS через WebHDFS и HttpFS с поддержкой NameNode HA и circuit breaker. Список файлов виртуализирован: в DOM отрисовываются только видимые строки, поэтому размер директории не влияет на скорость отрисовки. Поддерживает превью Parquet, ORC, CSV и JSON, ACL, квоты директорий и имперсонацию пользователей (`doAs`).
3. SQL Explorer — веб-редактор запросов к Trino и Apache Hive (HiveServer2, Cloudera, Hortonworks) на базе Monaco Editor: автодополнение, TTL-кэш метаданных, история запросов, асинхронное выполнение, AI-помощник и сохранение рабочих пространств пользователей.
4. Spark Explorer — веб-среда для Apache Spark (PySpark, Scala Spark, Spark SQL) через Apache Livy на кластерах YARN и Kubernetes, вызовы Livy защищены circuit breaker. Поддерживает интерактивные сессии, выбор версий Spark и Python, подключение каталогов Hive Metastore и Iceberg, визуальный конструктор DAG/ETL с проверкой графа на циклы.
5. Hadoop gRPC Replicator — межкластерная репликация HDFS (DC1 → DC2): глобальное ограничение полосы пропускания (Hierarchical Token Bucket Throttler), бинарный gRPC-стриминг, атомарная запись через staging и rename, инкрементальная репликация по Snapshot Diff, Kerberos-изоляция (включая выполнение от системной технической учетной записи) и метрики Prometheus.

Каждый сервис написан на Java 21 LTS и Spring Boot 3.3.4, собирается в отдельный Docker-образ, разворачивается самостоятельно или в составе umbrella Helm chart и имеет собственный демо-стенд.

---

## Структура монорепозитория

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

## Общие модули

### `backend/common-security-starter`

Spring Boot 3 стартер безопасности на Java 21, подключается всеми сервисами.

Аутентификация:
- Kerberos SPNEGO SSO через заголовок `Authorization: Negotiate <ticket>` на Java GSS-API.
- Клиент LDAPS, OpenLDAP и Active Directory с пулом соединений, проверкой групп и экранированием ввода от LDAP Injection (CWE-90).
- Mock-пользователи для локальной разработки без KDC и LDAP.

Сессии и токены:
- Токены сессий передаются только в cookies с флагами `HttpOnly`, `SameSite=Lax`, `Secure`; `localStorage` не используется.
- Двухуровневое хранилище сессий: L1 — in-memory кэш Caffeine, L2 — БД (H2 или PostgreSQL) с проверкой отзыва токенов (CWE-613).
- Защита от CSRF: `SameSite`, белый список Origin/Referer, заголовок `X-Requested-With`.

Отказоустойчивость и безопасность:
- `SimpleCircuitBreaker` с состояниями CLOSED, OPEN, HALF_OPEN для вызовов кластеров Hadoop и переключения при HA failover.
- Ограничение частоты запросов (rate limiting).
- JSON-аудит через AOP: события авторизации и изменяющие операции.
- `GlobalExceptionHandler` возвращает структурированный ответ с кодом ошибки без деталей исключения (CWE-209).
- Раздача собранных SPA на Svelte 5 через Spring Web MVC (`SpaController`).

### `frontend/common`

- `api/client.ts` — базовый HTTP-клиент: сессия в cookie (без `localStorage`, чтобы токен не был доступен скриптам при XSS), продление сессии (sliding sessions), заголовок `X-Requested-With` для CSRF, `credentials: include`, вход через Kerberos SSO Negotiate.
- UI-компоненты на Svelte 5 (Runes) и Tailwind CSS 4:
  - виртуализация списка файлов (`FileList.svelte`): число DOM-узлов не зависит от размера директории (O(1));
  - панели с изменяемым размером (resizable split panes) с сохранением пропорций — для панелей каталогов, редактора кода и результатов в SQL и Spark Explorer;
  - ленивая загрузка (code splitting) модальных окон и диалогов через асинхронные импорты;
  - закрытие модальных окон по клику вне диалога и по клавише `Escape`.
- `types/auth.ts` — TypeScript-интерфейсы сессий и ролей пользователей.
- `components/` — компоненты статусов (`StatusBadge`), модальных окон (`LoginModal`) и уведомлений (`NotificationToast`).

---

## Адреса, health-check и образы сервисов

| Сервис | Веб-интерфейс | Health-check | Метрики Prometheus | Образ | Helm chart |
|---|---|---|---|---|---|
| YARN Explorer | `http://localhost:8001` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/yarn:latest` | `helm/charts/yarn-explorer` |
| HDFS Explorer | `http://localhost:8002` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/hdfs:latest` | `helm/charts/hdfs-explorer` |
| SQL Explorer | `http://localhost:8003` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/sql:latest` | `helm/charts/sql-explorer` |
| Spark Explorer | `http://localhost:8004` | `GET /healthz` | `GET /metrics` | `hadoop-explorer/spark:latest` | `helm/charts/spark-explorer` |
| Hadoop gRPC Replicator | `http://localhost:8005` | `GET /health` | `GET /metrics` | `hadoop-explorer/replicator:latest` [проверить: `scripts/build-containers.sh` собирает `replicator-orchestrator` и `replicator-agent`] | `helm/charts/replicator` [проверить: каталога нет в `helm/charts/`] |

---

## Метрики Prometheus и дашборды Grafana

Каждый сервис отдает метрики в формате OpenMetrics/Prometheus на `GET /metrics` и `GET /api/v1/metrics`.

### Экспортируемые метрики

HTTP (golden signals):
- `http_requests_total{app="...", method="...", path="...", status="..."}` — счетчик запросов с нормализацией URI.
- `http_request_duration_seconds` — гистограмма задержек (p50, p90, p99), бакеты от `5ms` до `10s`.
- `http_requests_in_progress{app="..."}` — число запросов в обработке.

Внешние зависимости:
- `hadoop_circuit_breaker_state{name="..."}` (0=CLOSED, 1=HALF_OPEN, 2=OPEN) — состояние circuit breaker для NameNode, YARN RM, Livy, Trino.
- `hadoop_circuit_breaker_calls_total{name="...", status="success|failed|rejected"}` — счетчик вызовов через circuit breaker.
- `hadoop_retry_attempts_total{app="...", operation="...", status="retry|exhausted|success"}` — счетчик повторов с backoff.

Безопасность и ошибки:
- `hadoop_auth_attempts_total{app="...", provider="ldap|kerberos|mock", status="success|failure"}` — попытки аутентификации.
- `hadoop_rate_limit_blocks_total{app="..."}` — запросы, отклоненные лимитом частоты (HTTP 429).
- `hadoop_exceptions_total{app="...", exception_type="..."}` — необработанные ошибки, ответ HTTP 500 без деталей исключения (CWE-209).

### Дашборд Grafana

Дашборд состояния всех сервисов платформы:
- JSON-модель: [`monitoring/grafana/dashboards/hadoop_explorer_overview.json`](monitoring/grafana/dashboards/hadoop_explorer_overview.json)
- файл автопровижининга: [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](monitoring/grafana/provisioning/dashboards/dashboards.yaml)

Параметры, форматы файлов и переменные окружения описаны в [руководстве по конфигурации (docs/CONFIGURATION.md)](docs/CONFIGURATION.md).

---

## Запуск демо-стендов

У каждого сервиса есть отдельный демо-стенд на Docker Compose с тестовым KDC (Kerberos), OpenLDAP и окружением сервиса.

### Демо-стенд YARN Explorer
Включает: KDC, OpenLDAP, 2 кластера YARN ResourceManager с иерархией очередей Capacity Scheduler и YARN Explorer:
```bash
make demo-yarn
# Веб-интерфейс: http://localhost:8001
# Остановка: make demo-yarn-stop
```

### Демо-стенд HDFS Explorer
Включает: KDC, OpenLDAP, 2 кластера DataLake с WebHDFS под Kerberos и сервис HDFS Explorer:
```bash
make demo-hdfs
# Веб-интерфейс: http://localhost:8002
# Остановка: make demo-hdfs-stop
```

### Демо-стенд SQL Explorer
Включает: KDC, OpenLDAP, PostgreSQL, Hive Metastore, HiveServer2, Trino Coordinator и SQL Explorer:
```bash
make demo-sql
# Веб-интерфейс: http://localhost:8003
# Остановка: make demo-sql-stop
```

### Демо-стенд Spark Explorer
Включает: KDC, OpenLDAP, Apache Livy, PostgreSQL Hive Metastore, Hadoop HDFS, YARN ResourceManager и сервис Spark Explorer:
```bash
make demo-spark
# Веб-интерфейс: http://localhost:8004
# Остановка: make demo-spark-stop
```

### Демо-стенд Hadoop gRPC Replicator
Включает: Orchestrator (:8005) с Hierarchical Token Bucket и планировщиком, Receiver (:50051) и Worker daemon:
```bash
make demo-replicator
# Веб-интерфейс: http://localhost:8005
# Остановка: make demo-replicator-stop
```

### Все стенды одновременно
```bash
make demo-all
# Остановка: make demo-all-stop
```

Как стенды выполняют запросы Hive и Spark без ResourceManager и NodeManager, описано в [docs/hive-spark-without-yarn.md](docs/hive-spark-without-yarn.md).

### Тестовые учетные записи LDAP
| Пользователь | Пароль | Роли и группы | Назначение |
|---|---|---|---|
| `admin_user` / `admin` | `password123` | `hadoop-admins`, `ADMIN` | Полный административный доступ |
| `analyst_user` / `analyst` | `password123` | `analytics`, `READER` | Доступ только для чтения / аналитики |
| `engineer_user` / `engineer` | `password123` | `data-engineers`, `WRITER` | Доступ инженера данных (запись/чтение) |

---

## Сборка JAR-пакетов и CI/CD

Платформа собирается одним Maven reactor на Java 21 LTS:

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

Workflow GitHub Actions:
- Nightly Release (`.github/workflows/nightly-release.yml`): при каждом коммите в `main` прогоняет тесты, собирает JAR-пакеты, генерирует список изменений (`git-cliff`) и публикует pre-release с тегом `nightly`, прикрепляя все JAR-архивы и `SHA256SUMS.txt`.
- CI (`.github/workflows/ci.yml`): на pull request и push в ветки запускает проверки, модульные и интеграционные тесты и тестовую сборку пакетов.
- Tag Release (`.github/workflows/tag-release.yml`): на push тега `v*` собирает релиз и публикует стабильный релиз на GitHub.
- Cleanup Artifacts (`.github/workflows/cleanup-artifacts.yml`): раз в сутки по расписанию удаляет устаревшие артефакты Actions.

---

## Сборка Docker-образов

Все образы собираются на базе `eclipse-temurin:21-jre-jammy`. Dockerfile:
1. Устанавливает библиотеки Kerberos (`krb5-user`).
2. Копирует собранный Spring Boot fat JAR и статику фронтенда.
3. Запускает сервис от непривилегированного пользователя `appuser` (UID 10001).

[проверить: цели `make build-yarn`, `make build-hdfs`, `make build-sql`, `make build-spark`, `make build-replicator` в `Makefile` собирают фронтенд и JAR через `mvn clean package`, а не Docker-образ; образ одного сервиса собирает `./scripts/build-containers.sh <yarn|hdfs|sql|spark|replicator>`]

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

## Развертывание в Kubernetes (Helm)

### Вариант 1: вся платформа (umbrella chart)

Установка всех сервисов одной командой:
```bash
helm dependency update helm/hadoop-explorer
helm install hadoop-explorer helm/hadoop-explorer -n hadoop --create-namespace
```

Выбор устанавливаемых сервисов в `values.yaml`:
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

То же через параметры командной строки:
```bash
helm install hadoop-explorer helm/hadoop-explorer \
  --set yarn-explorer.enabled=true \
  --set hdfs-explorer.enabled=true \
  --set sql-explorer.enabled=true \
  --set spark-explorer.enabled=true
```

### Вариант 2: отдельные чарты

Каждый сервис устанавливается своим чартом:
```bash
helm install yarn-explorer helm/charts/yarn-explorer -n hadoop
helm install hdfs-explorer helm/charts/hdfs-explorer -n hadoop
helm install sql-explorer helm/charts/sql-explorer -n hadoop
helm install spark-explorer helm/charts/spark-explorer -n hadoop
```

Что включено в чартах:
- NetworkPolicy включена по умолчанию во всех чартах; входящий трафик разрешен только от Ingress-контроллера.
- PodDisruptionBudget создается при `replicaCount > 1`.
- Для двух и более реплик нужен внешний PostgreSQL вместо локального SQLite, см. [docs/production-database.md](docs/production-database.md).

Проверка синтаксиса чартов:
```bash
make helm-lint
```

---

## Запуск тестов

Всего 350 тестов: 255 бэкенд и 95 Vitest [проверить: число тестов могло устареть]:
- Frontend: 95 тестов Vitest и `svelte-check` (проверка типов во всех 5 SPA; компонентные тесты модальных окон, тулбаров, метрик кластера, партиций, панелей diff, каталогов и файловых списков в HDFS, Spark, SQL, YARN, Replicator, тестирование общих компонентов `Header`, `LoginModal`, `StatusBadge`, `Modal`, `NotificationToast`, а также E2E-тесты Playwright, которые падают при ошибках в консоли браузера).
- YARN Explorer: 28 тестов (13 Java 21 + 15 Frontend UI: валидация Capacity Scheduler, балансировка, Draft Diff, генерация XML, RM HA Failover, метрики кластера, Change Requests, принцип Four-Eyes, AWX-деплой, аудит, L1-кэш токенов, Healthz, Circuit Breaker).
- HDFS Explorer: 44 теста (16 Java 21 + 28 Frontend UI: NameNode HA Failover, localized exception mapping, API файловых операций, Healthz, Parquet/ORC Preview со schema footer reader, Cross-Cluster Copy, MockHdfsClient, Circuit Breaker, Rate Limiter, компонентные тесты тулбара, хлебных крошек, модальных окон и пакетных операций).
- SQL Explorer: 42 теста (Catalog API валидация и эндпоинты, Trino/Hive движки с отменой запросов и стримингом, TTL-кэширование метаданных, AI-сервис, токены, CSRF, ACL кластеров, Crash Recovery, Readiness / Healthz, SqlUserWorkspace).
- Spark Explorer: 24 теста (Livy-клиент полного цикла с отменой statement и логами, интерактивные сессии, автоостановка сессий при logout, Pydantic-валидаторы, MockSparkEngine, User Workspace, TTL-кэширование метаданных, Crash Recovery, Readiness / Healthz, Circuit Breaker).
- Hadoop gRPC Replicator: 146 тестов Java (Protobuf gRPC контракт, Hierarchical Token Bucket, адаптивный параллелизм вычитки Kafka до 100G/200G/400G+ с батчами до 50k, согласование полосы агентов без моделирования физики хостов, изоляция брокерного ISR-трафика, потоковый Receiver, KerberosContextManager изоляция KRB5CCNAME, парсер Snapshot Diff, метрики Prometheus, демон Cron Scheduler, полная история запусков `JobRun`, RBAC-изоляция задач).

```bash
# Запуск всех 350 тестов платформы (Backend + Frontend UI)
make test

# Тестирование интерфейса фронтенда:
make test-ui        # svelte-check по 5 SPA + 95 тестов Vitest
make frontend-check # проверка типов svelte-check
make frontend-test  # юнит и компонентные тесты Vitest (95 тестов)
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

## Цели Makefile

| Цель | Что делает |
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
| `make build-java` | Сборка фронтенда и всех JAR-пакетов платформы (Maven reactor) |
| `make build-security-starter` | Сборка JAR-пакета `common-security-starter` |
| `make build` | Сборка Docker-образов всех приложений платформы |
| `make build-yarn` | Сборка фронтенда и JAR-пакета YARN Explorer (`mvn clean package`) |
| `make build-hdfs` | Сборка фронтенда и JAR-пакета HDFS Explorer (`mvn clean package`) |
| `make build-sql` | Сборка фронтенда и JAR-пакета SQL Explorer (`mvn clean package`) |
| `make build-spark` | Сборка фронтенда и JAR-пакета Spark Explorer (`mvn clean package`) |
| `make build-replicator` | Сборка фронтенда и JAR-пакета Hadoop gRPC Replicator (`mvn clean package`) |
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

## Лицензия

Распространяется под лицензией Apache License 2.0.

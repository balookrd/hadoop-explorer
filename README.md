<div align="center">

<img src="images/logo_white.png" alt="Pristan" width="380" />

<p>Веб-интерфейсы для YARN, HDFS, SQL (Trino, Hive), Spark и межкластерной репликации HDFS</p>

[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](#запуск-тестов)
[![Java](https://img.shields.io/badge/Java-21%20LTS-blue.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Maven](https://img.shields.io/badge/Maven-3.9-red.svg)](https://maven.apache.org/)
[![Frontend](https://img.shields.io/badge/Frontend-Svelte%205%20%7C%20Tailwind%204-orange.svg)](https://svelte.dev/)
[![Docker](https://img.shields.io/badge/Docker-Multi--Stage-2496ED.svg)](docker/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboards%20Ready-F46800.svg)](monitoring/grafana/dashboards/pristan_overview.json)

</div>

---

## Содержание

- [Состав платформы](#состав-платформы)
- [Структура монорепозитория](#структура-монорепозитория)
- [Детальная системная архитектура (docs/ARCHITECTURE.md)](docs/ARCHITECTURE.md)
- [Руководство по конфигурации компонентов (docs/CONFIGURATION.md)](docs/CONFIGURATION.md)
- Руководства администратора и DevOps (standalone, Docker):
  - [Общее руководство администратора платформы (docs/admin-guide.md)](docs/admin-guide.md)
  - [Администрирование Artel (docs/yarn-admin-guide.md)](docs/yarn-admin-guide.md)
  - [Администрирование Sklad (docs/hdfs-admin-guide.md)](docs/hdfs-admin-guide.md)
  - [Администрирование Tolmach (docs/sql-admin-guide.md)](docs/sql-admin-guide.md)
  - [Администрирование Iskra (docs/spark-admin-guide.md)](docs/spark-admin-guide.md)
  - [Администрирование Parom (docs/replicator-admin-guide.md)](docs/replicator-admin-guide.md)
  - [Регламент Disaster Recovery и защита от split-brain (docs/replicator-disaster-recovery-guide.md)](docs/replicator-disaster-recovery-guide.md)
  - [Отказоустойчивость и распределенный лизинг Replicator (docs/replicator-fault-tolerance.md)](docs/replicator-fault-tolerance.md)
  - [Презентация Replicator для команды (docs/replicator-presentation.md)](docs/replicator-presentation.md) ([HTML](docs/replicator-presentation.html), [PPTX](docs/replicator-presentation.pptx))
- Руководства пользователя:
  - [Руководство пользователя Artel (docs/yarn-user-guide.md)](docs/yarn-user-guide.md)
  - [Доставка и применение конфигурации очередей через Ansible AWX (docs/awx-yarn-deployment.md)](docs/awx-yarn-deployment.md)
  - [Руководство пользователя Sklad (docs/hdfs-user-guide.md)](docs/hdfs-user-guide.md)
  - [Руководство пользователя Tolmach (docs/sql-user-guide.md)](docs/sql-user-guide.md)
  - [Руководство пользователя Iskra (docs/spark-user-guide.md)](docs/spark-user-guide.md)
  - [Руководство пользователя Parom (docs/replicator-user-guide.md)](docs/replicator-user-guide.md)
  - [Работа Hive и Spark без кластера YARN (docs/hive-spark-without-yarn.md)](docs/hive-spark-without-yarn.md)
- [Общие модули](#общие-модули)
- [Адреса, health-check и образы сервисов](#адреса-health-check-и-образы-сервисов)
- [Метрики Prometheus и дашборды Grafana](#метрики-prometheus-и-дашборды-grafana)
- [Запуск демо-стендов](#запуск-демо-стендов)
- [Сборка JAR-пакетов и CI/CD](#сборка-jar-пакетов-и-cicd)
- [Сборка Docker-образов](#сборка-docker-образов)
- [Запуск тестов](#запуск-тестов)
- [Цели Makefile](#цели-makefile)
- [Лицензия](#лицензия)

---

## Состав платформы

Pristan — монорепозиторий из пяти сервисов для работы с инфраструктурой Apache Hadoop:

1. Artel — консоль мониторинга кластеров и управления иерархией очередей Apache Hadoop YARN Capacity Scheduler: моделирование весов, версионирование конфигурации, заявки на изменение (Change Requests) с согласованием по принципу Four-Eyes, доставка и применение конфигурации (`yarn rmadmin -refreshQueues`) через Ansible AWX с автоматическим откатом.
2. Sklad — файловый менеджер HDFS через WebHDFS и HttpFS с поддержкой NameNode HA и circuit breaker. Список файлов виртуализирован: в DOM отрисовываются только видимые строки, поэтому размер директории не влияет на скорость отрисовки. Поддерживает превью Parquet, ORC, CSV и JSON, ACL, квоты директорий и имперсонацию пользователей (`doAs`).
3. Tolmach — веб-редактор запросов к Trino и Apache Hive (HiveServer2, Cloudera, Hortonworks) на базе Monaco Editor: автодополнение, TTL-кэш метаданных, история запросов, асинхронное выполнение, AI-помощник и сохранение рабочих пространств пользователей.
4. Iskra — веб-среда для Apache Spark (PySpark, Scala Spark, Spark SQL) через Apache Livy на кластерах YARN и Kubernetes, вызовы Livy защищены circuit breaker. Поддерживает интерактивные сессии, выбор версий Spark и Python, подключение каталогов Hive Metastore и Iceberg, визуальный конструктор DAG/ETL с проверкой графа на циклы.
5. Parom — межкластерная репликация HDFS (DC1 → DC2): глобальное ограничение полосы пропускания (Hierarchical Token Bucket Throttler), бинарный gRPC-стриминг, атомарная запись через staging и rename, инкрементальная репликация по Snapshot Diff, Kerberos-изоляция (включая выполнение от системной технической учетной записи) и метрики Prometheus.

Каждый сервис написан на Java 21 LTS и Spring Boot 3.3.4, собирается в отдельный Docker-образ и имеет собственный демо-стенд.

---

## Структура монорепозитория

```
pristan/
├── backend/                  # Java 21 LTS (Spring Boot 3.3.4, Maven)
│   ├── zastava/ # Ядро безопасности: SPNEGO, LDAP, JWT, CSRF, RateLimit, L1/L2
│   ├── yarn/                 # Сервис Artel (Capacity Scheduler, RM HA Failover)
│   ├── hdfs/                 # Сервис Sklad (HA NameNode, Parquet/ORC Preview)
│   ├── sql/                  # Сервис Tolmach (Trino, Hive, AI Assistant)
│   ├── spark/                # Сервис Iskra (Livy, PySpark, Scala, DAG Pipelines)
│   └── replicator/           # Сервис Parom
│       ├── orchestrator/     # Replicator Orchestrator API (Java 21 / Spring Boot 3)
│       └── agent/            # Нативный Replicator gRPC Worker Daemon (Java 21)
│
├── ansible/                  # ─── Автоматизация деплоя и применения (AWX) ───
│   ├── playbooks/            # deploy_capacity_scheduler.yml (Job Template)
│   ├── roles/                # yarn_capacity_scheduler (Backup, Deploy, refreshQueues, Rollback)
│   └── inventory.example.ini
│
├── frontend/                 # ─── Клиентские SPA приложения (Svelte 5) ───
│   ├── common/               # Общие UI-компоненты, API-клиенты, типы авторизации (types/auth.ts)
│   ├── apps/
│   │   ├── yarn/             # Frontend Artel (Svelte 5 + Tailwind 4)
│   │   ├── hdfs/             # Frontend Sklad (Svelte 5 + Tailwind 4 + виртуализация)
│   │   ├── sql/              # Frontend Tolmach (Svelte 5 + Tailwind 4 + Monaco Editor)
│   │   ├── spark/            # Frontend Iskra (Svelte 5 + Tailwind 4 + Monaco + DAG)
│   │   └── replicator/       # Frontend Replicator (Svelte 5 + Tailwind 4)
│   └── package.json          # NPM Workspaces монорепозитория
│
├── monitoring/               # ─── Мониторинг и наблюдаемость (Observability) ───
│   └── grafana/
│       ├── dashboards/       # Готовые JSON-дашборды (HTTP Signals, Circuit Breakers, Auth)
│       └── provisioning/     # Автопровижининг дашбордов для Docker Compose и Kubernetes
│
├── docker/                   # ─── Производственные Dockerfile на Java 21 ───
│   ├── Dockerfile.artel
│   ├── Dockerfile.sklad
│   ├── Dockerfile.tolmach
│   ├── Dockerfile.iskra
│   ├── Dockerfile.parom-orchestrator
│   └── Dockerfile.parom-agent
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

### `backend/zastava`

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
  - панели с изменяемым размером (resizable split panes) с сохранением пропорций — для панелей каталогов, редактора кода и результатов в SQL и Iskra;
  - ленивая загрузка (code splitting) модальных окон и диалогов через асинхронные импорты;
  - закрытие модальных окон по клику вне диалога и по клавише `Escape`.
- `types/auth.ts` — TypeScript-интерфейсы сессий и ролей пользователей.
- `components/` — компоненты статусов (`StatusBadge`), модальных окон (`LoginModal`) и уведомлений (`NotificationToast`).

---

## Адреса, health-check и образы сервисов

| Сервис | Веб-интерфейс | Health-check | Метрики Prometheus | Образ |
|---|---|---|---|---|
| Artel | `http://localhost:8001` | `GET /healthz` | `GET /actuator/prometheus` | `pristan/yarn:latest` |
| Sklad | `http://localhost:8002` | `GET /healthz` | `GET /actuator/prometheus` | `pristan/sklad:latest` |
| Tolmach | `http://localhost:8003` | `GET /healthz` | `GET /actuator/prometheus` | `pristan/tolmach:latest` |
| Iskra | `http://localhost:8004` | `GET /healthz` | `GET /actuator/prometheus` | `pristan/iskra:latest` |
| Parom | `http://localhost:8005` | `GET /health` | `GET /metrics` | `pristan/parom-orchestrator:latest`, `pristan/parom-agent:latest` |

---

## Метрики Prometheus и дашборды Grafana

YARN, HDFS, SQL и Iskra отдают метрики на `GET /actuator/prometheus` без аутентификации, оркестратор Replicator — на `GET /metrics` (с аутентификацией).

### Экспортируемые метрики

- HTTP (все четыре сервиса): `http_server_requests_seconds_count|_bucket{app, method, uri, status, outcome, exception}`, `http_server_requests_active_seconds_count{app}` (запросы в обработке).
- Ограничитель частоты (все четыре сервиса): `hadoop_rate_limit_blocks_total{app, tier}` — ответы 429, `tier`: `login` или `api`.
- HDFS: `sklad_operations_total{cluster, operation, status}`, `sklad_bytes_transferred_total{cluster, direction}`.
- YARN: `artel_change_requests_total{cluster, status}`.
- SQL: `tolmach_query_duration_seconds_count|_bucket{cluster, engine, status}`.
- Spark: `iskra_statement_duration_seconds_count|_bucket{cluster, language, status}`, `iskra_sessions_active{cluster, kind}`.

Лейблы, бакеты и доступ описаны в [docs/CONFIGURATION.md, раздел 9](docs/CONFIGURATION.md#9-мониторинг-prometheus-и-дашборды-grafana).

### Дашборд Grafana

Дашборд состояния всех сервисов платформы:
- JSON-модель: [`monitoring/grafana/dashboards/pristan_overview.json`](monitoring/grafana/dashboards/pristan_overview.json)
- файл автопровижининга: [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](monitoring/grafana/provisioning/dashboards/dashboards.yaml)

Параметры, форматы файлов и переменные окружения описаны в [руководстве по конфигурации (docs/CONFIGURATION.md)](docs/CONFIGURATION.md).

---

## Запуск демо-стендов

У каждого сервиса есть отдельный демо-стенд на Docker Compose с тестовым KDC (Kerberos), OpenLDAP и окружением сервиса.

### Демо-стенд Artel
Включает: KDC, OpenLDAP, 2 кластера YARN ResourceManager с иерархией очередей Capacity Scheduler и Artel:
```bash
make demo-artel
# Веб-интерфейс: http://localhost:8001
# Остановка: make demo-artel-stop
```

### Демо-стенд Sklad
Включает: KDC, OpenLDAP, 2 кластера DataLake с WebHDFS под Kerberos и сервис Sklad:
```bash
make demo-sklad
# Веб-интерфейс: http://localhost:8002
# Остановка: make demo-sklad-stop
```

### Демо-стенд Tolmach
Включает: KDC, OpenLDAP, PostgreSQL, Hive Metastore, HiveServer2, Trino Coordinator и Tolmach:
```bash
make demo-tolmach
# Веб-интерфейс: http://localhost:8003
# Остановка: make demo-tolmach-stop
```

### Демо-стенд Iskra
Включает: KDC, OpenLDAP, Apache Livy, PostgreSQL Hive Metastore, Hadoop HDFS, YARN ResourceManager и сервис Iskra:
```bash
make demo-iskra
# Веб-интерфейс: http://localhost:8004
# Остановка: make demo-iskra-stop
```

### Демо-стенд Parom
Включает: Orchestrator (:8005) с Hierarchical Token Bucket и планировщиком, Receiver (:50051) и Worker daemon:
```bash
make demo-parom
# Веб-интерфейс: http://localhost:8005
# Остановка: make demo-parom-stop
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

Образ берет UI из `backend/*/src/main/resources/static`, поэтому перед сборкой образов соберите фронтенд и JAR: `make build-java`. Без собранного UI `scripts/build-containers.sh` останавливается с сообщением `Нет собранного UI в <каталог>: выполните make frontend-build`.

```bash
# Сборка всех контейнеров платформы
make build

# Либо по отдельности:
./scripts/build-containers.sh artel       # pristan/artel:latest
./scripts/build-containers.sh sklad       # pristan/sklad:latest
./scripts/build-containers.sh tolmach     # pristan/tolmach:latest
./scripts/build-containers.sh iskra       # pristan/iskra:latest
./scripts/build-containers.sh replicator  # pristan/parom-orchestrator:latest и pristan/parom-agent:latest
```

Тег и префикс образа задаются переменными `TAG` (по умолчанию `latest`) и `REGISTRY` (по умолчанию `hadoop-explorer`). Цели `make build-artel`, `make build-sklad`, `make build-tolmach`, `make build-iskra`, `make build-parom` собирают фронтенд и JAR (`mvn clean package -DskipTests`), а не Docker-образ.

Прямой запуск через Docker CLI:
```bash
docker build -t pristan/yarn:latest -f docker/Dockerfile.artel .
docker build -t pristan/sklad:latest -f docker/Dockerfile.sklad .
docker build -t pristan/tolmach:latest -f docker/Dockerfile.tolmach .
docker build -t pristan/iskra:latest -f docker/Dockerfile.iskra .
docker build -t pristan/parom-orchestrator:latest -f docker/Dockerfile.parom-orchestrator .
docker build -t pristan/parom-agent:latest -f docker/Dockerfile.parom-agent .
```

---

## Запуск тестов

Число тестов на 2026-11-06: 943 тестовых метода Java (аннотации `@Test` и `@ParameterizedTest`; параметризованные методы при прогоне дают больше запусков) и 115 тестов Vitest:
- Frontend: 115 тестов Vitest (из них 28 в `frontend/common`) и `svelte-check` (проверка типов во всех 5 SPA; компонентные тесты модальных окон, тулбаров, метрик кластера, партиций, панелей diff, каталогов и файловых списков в HDFS, Spark, SQL, YARN, Replicator, тестирование общих компонентов `Header`, `LoginModal`, `StatusBadge`, `Modal`, `NotificationToast`, а также E2E-тесты Playwright, которые падают при ошибках в консоли браузера).
- Artel: 49 тестов (34 Java 21 + 15 Frontend UI: валидация Capacity Scheduler, балансировка, Draft Diff, генерация XML, RM HA Failover, метрики кластера, Change Requests, принцип Four-Eyes, AWX-деплой, аудит, L1-кэш токенов, Healthz, Circuit Breaker).
- Sklad: 45 тестов (17 Java 21 + 28 Frontend UI: NameNode HA Failover, localized exception mapping, API файловых операций, Healthz, Parquet/ORC Preview со schema footer reader, Cross-Cluster Copy, MockHdfsClient, Circuit Breaker, Rate Limiter, компонентные тесты тулбара, хлебных крошек, модальных окон и пакетных операций).
- Tolmach: 19 тестов (7 Java 21 + 12 Frontend UI: Catalog API валидация и эндпоинты, Trino/Hive движки с отменой запросов и стримингом, TTL-кэширование метаданных, AI-сервис, токены, CSRF, ACL кластеров, Crash Recovery, Readiness / Healthz, SqlUserWorkspace).
- Iskra: 15 тестов (6 Java 21 + 9 Frontend UI: Livy-клиент полного цикла с отменой statement и логами, интерактивные сессии, автоостановка сессий при logout, MockSparkEngine, User Workspace, TTL-кэширование метаданных, Crash Recovery, Readiness / Healthz, Circuit Breaker).
- Parom: 871 тест (848 Java 21 + 23 Frontend UI: Protobuf gRPC контракт, Hierarchical Token Bucket, адаптивный параллелизм вычитки Kafka до 100G/200G/400G+ с батчами до 50k, согласование полосы агентов без моделирования физики хостов, изоляция брокерного ISR-трафика, потоковый Receiver, KerberosContextManager изоляция KRB5CCNAME, парсер Snapshot Diff, метрики Prometheus, демон Cron Scheduler, полная история запусков `JobRun`, RBAC-изоляция задач).
- Common Security Starter: 31 тест Java 21.

```bash
# Запуск всех тестов платформы (Backend + Frontend UI)
make test

# Тестирование интерфейса фронтенда:
make test-ui        # svelte-check по 5 SPA + 115 тестов Vitest
make frontend-check # проверка типов svelte-check
make frontend-test  # юнит и компонентные тесты Vitest (115 тестов)
make frontend-e2e   # E2E тесты Playwright (Chromium)

# Тестирование бэкенда (Java 21 / Spring Boot 3):
make test-java        # Все Java тесты платформы
make test-zastava # Тесты стартера безопасности (zastava)
make test-artel        # Тесты сервиса Artel (Java 21)
make test-sklad        # Тесты сервиса Sklad (Java 21)
make test-tolmach         # Тесты сервиса Tolmach (Java 21)
make test-iskra       # Тесты сервиса Iskra (Java 21)
make test-parom  # Тесты сервиса Replicator (Java 21)
```

---

## Цели Makefile

| Цель | Что делает |
|---|---|
| `make test` | Запуск всех модульных, компонентных и интеграционных тестов (Java + UI) |
| `make test-java` | Запуск всех тестов Java 21 сервисов бэкенда |
| `make test-zastava` | Запуск тестов стартера безопасности `zastava` |
| `make test-artel` | Запуск тестов сервиса Artel (Java 21) |
| `make test-sklad` | Запуск тестов сервиса Sklad (Java 21) |
| `make test-tolmach` | Запуск тестов сервиса Tolmach (Java 21) |
| `make test-iskra` | Запуск тестов сервиса Iskra (Java 21) |
| `make test-parom` | Запуск тестов сервиса Parom (Java 21) |
| `make test-ui` | Запуск тестов фронтенда Vitest в Svelte 5 приложениях |
| `make frontend-install` | Установка NPM зависимостей фронтенда |
| `make frontend-check` | Статическая проверка типов Svelte 5 во всех 5 SPA (`svelte-check`) |
| `make frontend-test` | Запуск компонентных и юнит-тестов фронтенда |
| `make frontend-e2e` | Запуск браузерных E2E тестов Playwright |
| `make frontend-build` | Компиляция всех 5 SPA фронтендов через Vite |
| `make build-java` | Сборка фронтенда и всех JAR-пакетов платформы (Maven reactor) |
| `make build-zastava` | Сборка JAR-пакета `zastava` |
| `make build` | Сборка Docker-образов всех приложений платформы |
| `make build-artel` | Сборка фронтенда и JAR-пакета Artel (`mvn clean package`) |
| `make build-sklad` | Сборка фронтенда и JAR-пакета Sklad (`mvn clean package`) |
| `make build-tolmach` | Сборка фронтенда и JAR-пакета Tolmach (`mvn clean package`) |
| `make build-iskra` | Сборка фронтенда и JAR-пакета Iskra (`mvn clean package`) |
| `make build-parom` | Сборка фронтенда и JAR-пакета Parom (`mvn clean package`) |
| `make demo-artel` | Запуск демо-стенда Artel (`:8001`) |
| `make demo-artel-stop` | Остановка демо-стенда Artel |
| `make demo-sklad` | Запуск демо-стенда Sklad (`:8002`) |
| `make demo-sklad-stop` | Остановка демо-стенда Sklad |
| `make demo-tolmach` | Запуск демо-стенда Tolmach (`:8003`) |
| `make demo-tolmach-stop` | Остановка демо-стенда Tolmach |
| `make demo-iskra` | Запуск демо-стенда Iskra (`:8004`) |
| `make demo-iskra-stop` | Остановка демо-стенда Iskra |
| `make demo-all` | Запуск объединенного демо-стенда платформы |
| `make demo-all-stop` | Остановка объединенного демо-стенда |
| `make java-index` | Построение автономного AST-индекса Java для поиска символов |
| `make java-query Q="..."` | Структурный поиск по Java AST-индексу (класс, метод, вызовы, тесты) |

---

## Лицензия

Распространяется под лицензией Apache License 2.0.

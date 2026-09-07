<div align="center">

<img src="images/logo_white.png" alt="Hadoop Explorer Platform" width="380" />

<p><strong>Единая корпоративная веб-платформа для управления экосистемой Apache Hadoop</strong></p>

[![Tests](https://img.shields.io/badge/tests-112%20passed-brightgreen.svg)](#-тестирование-платформы)
[![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.14-blue.svg)](https://www.python.org/)
[![uv](https://img.shields.io/badge/uv-workspaces-purple.svg)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Frontend](https://img.shields.io/badge/Frontend-Svelte%205%20%7C%20Tailwind%204-orange.svg)](https://svelte.dev/)
[![Docker](https://img.shields.io/badge/Docker-Multi--Stage-2496ED.svg)](docker/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm%20Charts-326CE5.svg)](helm/)

</div>

---

## 📑 Содержание

- [Обзор платформы](#-обзор-платформы)
- [Архитектура монорепозитория](#-архитектура-монорепозитория)
- [Выделенные общие модули](#-выделенные-общие-модули)
- [Менеджер зависимостей Python (uv workspaces)](#-менеджер-зависимостей-python-uv-workspaces)
- [Компоненты платформы](#-компоненты-платформы)
- [Руководство по конфигурации компонентов (docs/CONFIGURATION.md)](docs/CONFIGURATION.md)
- [Быстрый старт: Раздельные демо-стенды](#-быстрый-старт-раздельные-демо-стенды)
- [Сборка Docker-контейнеров](#-сборка-docker-контейнеров)
- [Развертывание в Kubernetes (Helm)](#️-развертывание-в-kubernetes-helm)
- [Тестирование платформы](#-тестирование-платформы)
- [CLI команды (Makefile)](#-cli-команды-makefile)
- [Структура каталогов](#-структура-каталогов)
- [Лицензия](#-лицензия)

---

## 🎯 Обзор платформы

**Hadoop Explorer Platform** объединяет в единый монорепозиторий три ключевых корпоративных инструмента для работы с Big Data инфраструктурой:

1. **HDFS Explorer** — файловый менеджер распределенного хранилища Apache Hadoop (WebHDFS & HttpFS). Поддерживает превью Parquet, ORC, CSV, JSON, списки контроля доступа (ACL), квоты директорий и имперсонацию пользователей (`doAs`).
2. **SQL Explorer** — аналитический веб-редактор запросов к **Trino** и **Apache Hive (HiveServer2 / Cloudera / Hortonworks)** на базе Monaco Editor с автодополнением, историей запросов, асинхронным выполнением и встроенным AI-помощником.
3. **YARN Explorer** — интерактивная консоль для мониторинга кластеров, моделирования весов и управления иерархией очередей **Apache Hadoop YARN Capacity Scheduler**, версионированием и согласованием заявок на изменение (Change Requests).

Каждое приложение может собираться в **независимый легковесный Docker-контейнер**, развертываться автономно или в составе единого **Umbrella Helm Chart**, а также запускаться в собственном **раздельном демо-стенде**.

---

## 🏛️ Архитектура монорепозитория

```
hadoop-explorer/
├── backend/
│   ├── common/             # ─── Общие переиспользуемые модули ядра ───
│   │   ├── core/           # Безопасность, SessionStore, JWT, CSRF, CommonLdapAuthService, Kerberos, Rate Limiter, Audit
│   │   ├── models/         # Общие модели пользователей, ролей и сессий
│   │   └── db/             # Базовый StorageService (SQLite WAL, Postgres, Redis, L1 LRU Cache)
│   ├── hdfs/               # Сервис HDFS Explorer (40 тестов)
│   ├── sql/                # Сервис SQL Explorer (31 тест)
│   └── yarn/               # Сервис YARN Explorer (42 теста)
│
├── frontend/
│   ├── common/             # ─── Общие UI-компоненты и API-клиент ───
│   │   ├── api/            # Cookie-first HTTP-клиент с защитой от CSRF и поддержкой Kerberos SPNEGO
│   │   ├── components/     # StatusBadge, NotificationToast
│   │   └── types/          # Общие TypeScript интерфейсы сессий и ролей
│   ├── apps/
│   │   ├── hdfs/           # Frontend HDFS Explorer (Svelte 5 + Tailwind 4)
│   │   ├── sql/            # Frontend SQL Explorer (Svelte 5 + Tailwind 4 + Monaco)
│   │   └── yarn/           # Frontend YARN Explorer (Svelte 5 + Tailwind 4)
│   └── package.json        # NPM Workspaces монорепозитория
│
├── docker/
│   ├── Dockerfile.hdfs     # Multi-stage сборка образа hadoop-explorer/hdfs
│   ├── Dockerfile.sql      # Multi-stage сборка образа hadoop-explorer/sql
│   ├── Dockerfile.yarn     # Multi-stage сборка образа hadoop-explorer/yarn
│   └── .dockerignore
│
├── helm/
│   ├── hadoop-explorer/    # Umbrella Chart для комплексного деплоя платформы
│   └── charts/
│       ├── hdfs-explorer/  # Автономный чарт HDFS
│       ├── sql-explorer/   # Автономный чарт SQL
│       └── yarn-explorer/  # Автономный чарт YARN
│
├── demo/                   # ─── Изолированные демонстрационные стенды ───
│   ├── infra/              # Единый инфраструктурный стек (MIT KDC + OpenLDAP)
│   ├── hdfs/               # Стенд HDFS (2 кластера WebHDFS) -> :8001
│   ├── sql/                # Стенд SQL (Postgres, Hive, Trino) -> :8002
│   ├── yarn/               # Стенд YARN (2 кластера YARN RM) -> :8003
│   └── all/                # Единый запуск всех стендов с общим KDC/LDAP
│
├── scripts/
│   ├── run-tests.sh        # Скрипт прогона всех 113 тестов
│   └── build-containers.sh # Скрипт сборки контейнеров
│
├── Makefile                # Единый CLI для автоматизации всех операций
└── README.md
```

---

## 📦 Выделенные общие модули

### 1. `backend/common` (Пакет `hadoop-explorer-common`)
- **`backend.common.core.session_store`**:
  - Сохранение активных сессий пользователей в реляционной БД (`SQLite WAL`, `PostgreSQL`) для устойчивости при перезапуске бэкенд-сервисов.
  - Таблица `active_sessions` с автоматической конвертацией и проверкой абсолютного Unix Timestamp `expires_at`.
  - Двухуровневый черный список отозванных токенов `revoked_tokens` (L1 In-Memory LRU Cache + L2 база данных/Redis).
- **`backend.common.core.security`**:
  - Централизованная генерация и валидация JWT токенов с поддержкой `jti` и алгоритмов шифрования.
  - Строгая CSRF-защита (блокировка межсайтовых запросов `Sec-Fetch-Site: cross-site`, валидация заголовков `Origin`, `Referer` по белому списку, требование заголовка `X-Requested-With`).
  - Проверка отзыва токенов (CWE-613) с двухуровневым кэшированием (L1 In-Memory LRU + L2 Database/Redis) и защитой от Fail-Open.
- **`backend.common.core.ldap_auth`**:
  - Универсальный `CommonLdapAuthService` для LDAPS / Active Directory / OpenLDAP.
  - Поиск пользователей с экранированием фильтров (защита от LDAP Injection / CWE-90), извлечение групп (поддержка `memberOf` и фильтров `group_search_filter`), поддержка кастомных TLS CA-сертификатов.
  - Асинхронное исполнение через пул рабочих потоков во избежание блокировки Event Loop.
  - Провайдер mock-пользователей с верификацией хэшей `pbkdf2:sha256` и защитой от timing-атак (`hmac.compare_digest`).
- **`backend.common.core.kerberos`**:
  - Аутентификация Kerberos SPNEGO SSO через HTTP-заголовок `Authorization: Negotiate <ticket>`.
- **`backend.common.core.rate_limiter`**:
  - Скользящее окно (Sliding Window) с возможностью сохранения состояния в SQLite (WAL), PostgreSQL и Redis.
  - Безопасное определение клиентского IP-адреса с проверкой доверенных прокси (`is_trusted_proxy`, защита от IP Spoofing).
- **`backend.common.core.audit`**:
  - Структурированное JSON-логирование событий безопасности (`AuditEventType`) в кольцевой буфер и файл.
- **`backend.common.db.storage`**:
  - Базовый `BaseStorageService` для централизованного отзыва токенов и трекинга лимитов запросов с поддержкой любых диалектов (`sqlite`, `postgresql`, `redis`).
- **`backend.common.models.auth`**:
  - Базовые модели Pydantic: `Role` (`READER`, `WRITER`, `ADMIN`), `UserSession`, `UserInfo`, `TokenPayload`, `LoginRequest`, `TokenResponse`.

### 2. `frontend/common` (Пакет `@hadoop-explorer/common`)
- **`api/client.ts`**: Базовый HTTP fetcher с Cookie-first подходом (Zero LocalStorage для защиты от XSS), автоматическим добавлением заголовков CSRF (`X-Requested-With`), `credentials: include` и методом Kerberos SSO Negotiate.
- **`types/auth.ts`**: Унифицированные TypeScript интерфейсы сессий и ролей пользователей.
- **`components/`**: Переиспользуемые Svelte 5 компоненты статусов (`StatusBadge`) и всплывающих уведомлений (`NotificationToast`).

---

## ⚡ Менеджер зависимостей Python (uv workspaces)

Монорепозиторий использует современный инструмент **`uv`** с поддержкой **PEP 517 / PEP 621 Workspaces**:

- **Корневой `pyproject.toml`** определяет единый воркспейс со всеми сервисами:
  - `backend/common` (`hadoop-explorer-common`)
  - `backend/hdfs` (`hadoop-explorer-hdfs`)
  - `backend/sql` (`hadoop-explorer-sql`)
  - `backend/yarn` (`hadoop-explorer-yarn`)
- **Единое виртуальное окружение** `.venv` для мгновенной синхронизации всех зависимостей.
- **Быстрый линтинг и форматирование** через **Ruff**.

### Основные команды:
```bash
# Синхронизация единого окружения и всех пакетов воркспейса
make venv       # или uv sync --all-packages

# Проверка линтером Ruff
make lint       # или uv run ruff check backend

# Автоформатирование кода
make format     # или uv run ruff format backend
```

---

## 🧩 Компоненты платформы

| Приложение | Веб-интерфейс | Проверка Health | Контейнер | Helm Chart |
|---|---|---|---|---|
| **HDFS Explorer** | `http://localhost:8001` | `GET /healthz` | `hadoop-explorer/hdfs:latest` | `helm/charts/hdfs-explorer` |
| **SQL Explorer** | `http://localhost:8002` | `GET /healthz` | `hadoop-explorer/sql:latest` | `helm/charts/sql-explorer` |
| **YARN Explorer** | `http://localhost:8003` | `GET /healthz` | `hadoop-explorer/yarn:latest` | `helm/charts/yarn-explorer` |

> 📖 **Подробное описание параметров, форматов файлов и переменных окружения приведено в [Руководстве по конфигурации (docs/CONFIGURATION.md)](docs/CONFIGURATION.md).**

---

## 🚀 Быстрый старт: Раздельные демо-стенды

Каждый сервис укомплектован изолированным демонстрационным стендом в Docker Compose с тестовым KDC (Kerberos), OpenLDAP и необходимым окружением.

### 1. Демо-стенд HDFS Explorer
Включает: KDC, OpenLDAP, 2 кластера DataLake с Kerberized WebHDFS и сервис HDFS Explorer:
```bash
make demo-hdfs
# Веб-интерфейс: http://localhost:8001
# Остановка: make demo-hdfs-stop
```

### 2. Демо-стенд SQL Explorer
Включает: KDC, OpenLDAP, PostgreSQL, Hive Metastore, HiveServer2, Trino Coordinator и SQL Explorer:
```bash
make demo-sql
# Веб-интерфейс: http://localhost:8002
# Остановка: make demo-sql-stop
```

### 3. Демо-стенд YARN Explorer
Включает: KDC, OpenLDAP, 2 кластера YARN ResourceManager с иерархией очередей Capacity Scheduler и YARN Explorer:
```bash
make demo-yarn
# Веб-интерфейс: http://localhost:8003
# Остановка: make demo-yarn-stop
```

### 4. Объединенный запуск всех стендов
```bash
make demo-all
# Остановка: make demo-all-stop
```

### 🔑 Тестовые учетные записи (LDAP)
| Пользователь | Пароль | Роли и группы | Назначение |
|---|---|---|---|
| `admin_user` / `admin` | `password123` | `hadoop-admins`, `ADMIN` | Полный административный доступ |
| `analyst_user` / `analyst` | `password123` | `analytics`, `READER` | Доступ только для чтения / аналитики |
| `engineer_user` / `engineer` | `password123` | `data-engineers`, `WRITER` | Доступ инженера данных (запись/чтение) |

---

## 🐳 Сборка Docker-контейнеров

Все образы собираются по двухэтапной multi-stage схеме:
1. **Этап 1 (Node.js 22)**: компиляция frontend SPA (Svelte 5 + Vite).
2. **Этап 2 (Python 3.12-slim)**: системные библиотеки Kerberos/SASL/LDAP, установка Python-зависимостей, копирование статики и запуск под непривилегированным пользователем `appuser (UID 10001)`.

```bash
# Сборка всех контейнеров платформы
make build

# Либо по отдельности:
make build-hdfs    # hadoop-explorer/hdfs:latest
make build-sql     # hadoop-explorer/sql:latest
make build-yarn    # hadoop-explorer/yarn:latest
```

Прямой запуск через Docker CLI:
```bash
docker build -t hadoop-explorer/hdfs:latest -f docker/Dockerfile.hdfs .
docker build -t hadoop-explorer/sql:latest -f docker/Dockerfile.sql .
docker build -t hadoop-explorer/yarn:latest -f docker/Dockerfile.yarn .
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
hdfs-explorer:
  enabled: true

sql-explorer:
  enabled: true

yarn-explorer:
  enabled: false
```

Либо через параметры командной строки:
```bash
helm install hadoop-explorer helm/hadoop-explorer \
  --set hdfs-explorer.enabled=true \
  --set sql-explorer.enabled=true \
  --set yarn-explorer.enabled=false
```

### Вариант 2: Автономные чарты

Каждое приложение можно установить в кластер независимо:
```bash
helm install hdfs-explorer helm/charts/hdfs-explorer -n hadoop
helm install sql-explorer helm/charts/sql-explorer -n hadoop
helm install yarn-explorer helm/charts/yarn-explorer -n hadoop
```

Проверка синтаксиса чартов:
```bash
make helm-lint
```

---

## 🧪 Тестирование платформы

Все тесты (112 тестов) успешно проходят комплексную проверку:
- **HDFS Explorer**: 39 тестов (ACL, API, Security, CSRF, Common Modules, Parquet/ORC Preview, Cross-Cluster Copy).
- **SQL Explorer**: 31 тест (Trino/Hive движки, AI сервис, токены, CSRF, ACL кластеров, TTL-очистка кэша результатов).
- **YARN Explorer**: 42 теста (Capacity Scheduler валидация, балансировка, Change Requests, аудит, L1 кэш токенов).

```bash
# Запуск всех 112 тестов платформы
make test

# Либо по сервисам:
make test-hdfs
make test-sql
make test-yarn
```

---

## 🛠️ CLI команды (Makefile)

| Команда | Описание |
|---|---|
| `make venv` / `make sync` | Синхронизация единого uv-окружения (`.venv`) и всех пакетов воркспейса |
| `make install-dev` | Установка зависимостей и инструментов разработки |
| `make lint` | Проверка кодовой базы линтером Ruff |
| `make format` | Автоматическое форматирование кода с помощью Ruff |
| `make test` | Запуск всех 112 модульных и интеграционных тестов |
| `make test-hdfs` | Запуск 39 тестов сервиса HDFS Explorer |
| `make test-sql` | Запуск 31 теста сервиса SQL Explorer |
| `make test-yarn` | Запуск 42 тестов сервиса YARN Explorer |
| `make build` | Сборка Docker-образов всех приложений |
| `make build-hdfs` | Сборка Docker-образа HDFS Explorer |
| `make build-sql` | Сборка Docker-образа SQL Explorer |
| `make build-yarn` | Сборка Docker-образа YARN Explorer |
| `make frontend-install` | Установка NPM зависимостей фронтенда |
| `make frontend-build` | Компиляция SPA фронтендов через Vite |
| `make demo-hdfs` | Запуск демо-стенда HDFS Explorer (`:8001`) |
| `make demo-hdfs-stop` | Остановка демо-стенда HDFS Explorer |
| `make demo-sql` | Запуск демо-стенда SQL Explorer (`:8002`) |
| `make demo-sql-stop` | Остановка демо-стенда SQL Explorer |
| `make demo-yarn` | Запуск демо-стенда YARN Explorer (`:8003`) |
| `make demo-yarn-stop` | Остановка демо-стенда YARN Explorer |
| `make demo-all` | Запуск объединенного демо-стенда |
| `make demo-all-stop` | Остановка объединенного демо-стенда |
| `make helm-lint` | Валидация синтаксиса всех Helm-чартов |
| `make helm-package` | Упаковка чартов платформы в `.tgz` архивы |

---

## 📄 Лицензия

Распространяется под лицензией Apache License 2.0.

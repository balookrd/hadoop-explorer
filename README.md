# Hadoop Explorer Platform

<div align="center">

**Единая корпоративная веб-платформа для экосистемы Apache Hadoop**

[![Tests](https://img.shields.io/badge/tests-98%20passed-brightgreen.svg)](#-тестирование)
[![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.14-blue.svg)](https://www.python.org/)
[![Frontend](https://img.shields.io/badge/Frontend-Svelte%205%20%7C%20Tailwind%204-orange.svg)](https://svelte.dev/)
[![Docker](https://img.shields.io/badge/Docker-Multi--Stage-2496ED.svg)](docker/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm%20Charts-326CE5.svg)](helm/)

</div>

---

## 📑 Содержание

- [Обзор платформы](#-обзор-платформы)
- [Компоненты платформы](#-компоненты-платформы)
- [Выделенные общие модули](#-выделенные-общие-модули)
- [Быстрый старт: Раздельные демо-стенды](#-быстрый-старт-раздельные-демо-стенды)
- [Сборка Docker-контейнеров](#-сборка-docker-контейнеров)
- [Развертывание в Kubernetes (Helm)](#️-развертывание-в-kubernetes-helm)
- [Тестирование платформы](#-тестирование-платформы)
- [Структура монорепозитория](#-структура-монорепозитория)
- [CLI команды (Makefile)](#-cli-команды-makefile)

---

## 🎯 Обзор платформы

**Hadoop Explorer** объединяет в единый монорепозиторий три независимых корпоративных веб-приложения для работы с инфраструктурой Big Data:
1. **HDFS Explorer** — файловый менеджер и аналитическое превью данных для Apache Hadoop HDFS (WebHDFS, HttpFS, Parquet, ORC, ACL, Kerberos).
2. **SQL Explorer** — интерактивная SQL-консоль на Monaco Editor с поддержкой Trino и HiveServer2, автодополнением и AI-ассистентом.
3. **YARN Explorer** — интерактивный визуализатор, песочница и генератор конфигураций YARN Capacity Scheduler с согласованием изменений (Change Requests).

Каждое приложение может собираться в **изолированный легковесный Docker-контейнер**, деплоиться автономно или в составе единого **Umbrella Helm Chart**, а также запускаться в собственном **раздельном демо-стенде**.

---

## 🧩 Компоненты платформы

| Сервис | Описание | Порт в демо | Образ контейнера | Helm Chart |
|---|---|---|---|---|
| **HDFS Explorer** | Просмотр файлов HDFS, превью Parquet/ORC, доступы ACL, doAs | `http://localhost:8001` | `hadoop-explorer/hdfs:latest` | `helm/charts/hdfs-explorer` |
| **SQL Explorer** | SQL запросы к Trino / Hive, Monaco Editor, AI-генератор | `http://localhost:8002` | `hadoop-explorer/sql:latest` | `helm/charts/sql-explorer` |
| **YARN Explorer** | Мониторинг очередей, моделирование весов, Change Requests | `http://localhost:8003` | `hadoop-explorer/yarn:latest` | `helm/charts/yarn-explorer` |

---

## 📦 Выделенные общие модули

В монорепозитории выделены переиспользуемые модули ядра, исключающие дублирование кода:

### 1. `backend/common` (Python):
- **`backend.common.core.security`**:
  - Генерация и валидация JWT-токенов с идентификаторами `jti`.
  - Унифицированная CSRF-защита (проверка заголовков `Sec-Fetch-Site`, `Origin`, `Referer`, `X-Requested-With`).
  - Проверка отзыва токенов (CWE-613) и хеширование SHA-256.
- **`backend.common.core.ldap_auth`**:
  - Полноценная интеграция с LDAPS / Active Directory / OpenLDAP.
  - Поиск учетных записей, извлечение групп (через `memberOf` или `group_search_filter`).
  - Провайдер mock-пользователей с поддержкой `pbkdf2:sha256` и открытых паролей с защитой от тайминг-атак.
- **`backend.common.core.kerberos`**:
  - Аутентификация Kerberos SPNEGO SSO через HTTP Negotiate заголовок.
- **`backend.common.core.rate_limiter`**:
  - Защита от перебора паролей и DoS на основе скользящего окна (Sliding Window).
  - Безопасное определение IP клиента с проверкой доверенных прокси (`is_trusted_proxy`).
- **`backend.common.core.audit`**:
  - Структурированное JSON-логирование событий безопасности (`AuditEventType`) в кольцевой буфер и локальный файл.
- **`backend.common.db.storage`**:
  - Базовый сервис хранения состояния безопасности `BaseStorageService` (SQLite, PostgreSQL, Redis).
- **`backend.common.models.auth`**:
  - Базовые модели Pydantic: `Role`, `UserSession`, `UserInfo`, `TokenPayload`, `LoginRequest`, `TokenResponse`.

### 2. `frontend/common` (TypeScript & Svelte):
- **`frontend/common/api/client`**: Базовый HTTP-клиент с авто-добавлением CSRF заголовков, `credentials: include`, Bearer-токенов и методом Kerberos SSO Negotiate.
- **`frontend/common/types`**: Общие TypeScript интерфейсы (`UserSession`, `AuthResponse`).
- **`frontend/common/components`**: Унифицированные UI-компоненты (`StatusBadge`, `NotificationToast`).

---

## 🚀 Быстрый старт: Раздельные демо-стенды

Каждое приложение снабжено полностью изолированным Docker Compose окружением с тестовым KDC (Kerberos), OpenLDAP и необходимыми сервисами Hadoop.

### 1. Стенд HDFS Explorer
Включает: Kerberos KDC, OpenLDAP, 2 кластера DataLake с Kerberized WebHDFS и сервисом HDFS Explorer:
```bash
make demo-hdfs
# Доступен по адресу: http://localhost:8001
# Остановка: make demo-hdfs-stop
```

### 2. Стенд SQL Explorer
Включает: Kerberos KDC, OpenLDAP, PostgreSQL, Hive Metastore, HiveServer2, Trino Coordinator и SQL Explorer:
```bash
make demo-sql
# Доступен по адресу: http://localhost:8002
# Остановка: make demo-sql-stop
```

### 3. Стенд YARN Explorer
Включает: Kerberos KDC, OpenLDAP, 2 кластера YARN ResourceManager с иерархией Capacity Scheduler и YARN Explorer:
```bash
make demo-yarn
# Доступен по адресу: http://localhost:8003
# Остановка: make demo-yarn-stop
```

### 4. Объединенный запуск всех стендов
```bash
make demo-all
# Остановка: make demo-all-stop
```

### 🔑 Тестовые учетные записи (LDAP / Mock)
| Имя пользователя | Пароль | Роли и группы |
|---|---|---|
| `admin_user` / `admin` | `password123` | `hadoop-admins`, `ADMIN` (полный доступ) |
| `analyst_user` / `analyst` | `password123` | `analytics`, `READER` (только чтение) |
| `engineer_user` / `engineer` | `password123` | `data-engineers`, `WRITER` |

---

## 🐳 Сборка Docker-контейнеров

Контейнеры собираются в многоэтапном режиме (multi-stage: Node.js 22 для компиляции Svelte 5 + Python 3.12-slim для бэкенда).

```bash
# Сборка всех трех контейнеров
make build

# Либо по отдельности:
make build-hdfs    # Образ: hadoop-explorer/hdfs:latest
make build-sql     # Образ: hadoop-explorer/sql:latest
make build-yarn    # Образ: hadoop-explorer/yarn:latest
```

Сборка напрямую через Docker CLI:
```bash
docker build -t hadoop-explorer/hdfs:latest -f docker/Dockerfile.hdfs .
docker build -t hadoop-explorer/sql:latest -f docker/Dockerfile.sql .
docker build -t hadoop-explorer/yarn:latest -f docker/Dockerfile.yarn .
```

---

## ☸️ Развертывание в Kubernetes (Helm)

### Вариант 1: Единая платформа (Umbrella Helm Chart)

Установка всех сервисов платформы одной командой:
```bash
helm dependency update helm/hadoop-explorer
helm install hadoop-explorer helm/hadoop-explorer -n hadoop --create-namespace
```

Выборочное включение компонентов через флаги в `values.yaml`:
```yaml
hdfs-explorer:
  enabled: true

sql-explorer:
  enabled: true

yarn-explorer:
  enabled: false
```

Либо через аргументы CLI:
```bash
helm install hadoop-explorer helm/hadoop-explorer \
  --set hdfs-explorer.enabled=true \
  --set sql-explorer.enabled=true \
  --set yarn-explorer.enabled=false
```

### Вариант 2: Автономные чарты приложений

Каждое приложение можно развертывать независимо:
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

Все существующие тесты (98 тестов) полностью сохранены и проверены:
- **HDFS Explorer**: 29 тестов (ACL, API, Security, CSRF)
- **SQL Explorer**: 28 тестов (Trino/Hive движки, AI сервис, токены, CSRF)
- **YARN Explorer**: 41 тест (Capacity Scheduler валидация, балансировка, Change Requests, аудит)

Запуск тестов:
```bash
# Прогон всех 98 тестов
make test

# Прогон тестов конкретного сервиса:
make test-hdfs
make test-sql
make test-yarn
```

---

## 📂 Структура монорепозитория

```
hadoop-explorer/
├── backend/
│   ├── common/                  # ─── ОБЩИЙ МОДУЛЬ BACKEND ───
│   │   ├── core/                # Безопасность, LDAP, Kerberos, CSRF, Audit, RateLimiter
│   │   ├── models/              # Общие модели пользователей и сессий
│   │   ├── db/                  # Общий StorageService
│   │   └── pyproject.toml
│   ├── hdfs/                    # Бэкенд HDFS Explorer (29 тестов)
│   ├── sql/                     # Бэкенд SQL Explorer (28 тестов)
│   └── yarn/                    # Бэкенд YARN Explorer (41 тест)
│
├── frontend/
│   ├── common/                  # ─── ОБЩИЙ МОДУЛЬ FRONTEND ───
│   │   ├── api/                 # Базовый API-клиент с CSRF и SPNEGO
│   │   ├── components/          # Статусы, тосты, общие UI элементы
│   │   └── types/               # Общие TypeScript интерфейсы
│   ├── apps/
│   │   ├── hdfs/                # Frontend HDFS (Svelte 5 + Tailwind 4)
│   │   ├── sql/                 # Frontend SQL (Svelte 5 + Tailwind 4 + Monaco)
│   │   └── yarn/                # Frontend YARN (Svelte 5 + Tailwind 4)
│   └── package.json             # NPM Workspaces
│
├── docker/
│   ├── Dockerfile.hdfs          # Сборка контейнера HDFS
│   ├── Dockerfile.sql           # Сборка контейнера SQL
│   ├── Dockerfile.yarn          # Сборка контейнера YARN
│   └── .dockerignore
│
├── helm/
│   ├── hadoop-explorer/         # Umbrella Helm Chart (все сервисы)
│   └── charts/
│       ├── hdfs-explorer/       # Автономный чарт HDFS
│       ├── sql-explorer/        # Автономный чарт SQL
│       └── yarn-explorer/       # Автономный чарт YARN
│
├── demo/
│   ├── hdfs/                    # Демо-стенд HDFS Explorer
│   ├── sql/                     # Демо-стенд SQL Explorer
│   ├── yarn/                    # Демо-стенд YARN Explorer
│   └── all/                     # Объединенный демо-стенд
│
├── scripts/
│   ├── run-tests.sh             # Скрипт прогона тестов
│   └── build-containers.sh      # Скрипт сборки контейнеров
│
├── Makefile                     # Единый CLI автоматизации
└── README.md
```

---

## 🛠️ CLI команды (Makefile)

| Команда | Описание |
|---|---|
| `make test` | Запуск всех 98 модульных и интеграционных тестов |
| `make test-hdfs` | Запуск 29 тестов сервиса HDFS Explorer |
| `make test-sql` | Запуск 28 тестов сервиса SQL Explorer |
| `make test-yarn` | Запуск 41 теста сервиса YARN Explorer |
| `make build` | Сборка Docker-образов всех трех приложений |
| `make build-hdfs` | Сборка Docker-образа HDFS Explorer |
| `make build-sql` | Сборка Docker-образа SQL Explorer |
| `make build-yarn` | Сборка Docker-образа YARN Explorer |
| `make frontend-build` | Компиляция всех SPA приложений через Vite |
| `make demo-hdfs` | Запуск локального демо-стенда HDFS |
| `make demo-sql` | Запуск локального демо-стенда SQL |
| `make demo-yarn` | Запуск локального демо-стенда YARN |
| `make demo-all` | Запуск объединенного демо-стенда |
| `make helm-lint` | Валидация синтаксиса всех Helm-чартов |
| `make helm-package` | Упаковка чартов платформы в `.tgz` архивы |

---

## 📄 Лицензия

Распространяется под лицензией Apache License 2.0.

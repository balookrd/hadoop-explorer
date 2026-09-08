# ⚙️ Руководство по конфигурации Hadoop Explorer Platform

В данном документе подробно описаны параметры конфигурации, форматы файлов, переменные окружения и лучшие практики настройки компонентов платформы **Hadoop Explorer**:
- **Общие модули** (`backend/common` — безопасность, сессии, LDAP/Active Directory, Kerberos SPNEGO, хранилища, Circuit Breaker, Distributed Lock, Graceful Shutdown).
- **YARN Explorer** (`backend/yarn` — YARN RM HA, Capacity Scheduler, партиции, Change Requests, Distributed Lock, генерация XML).
- **HDFS Explorer** (`backend/hdfs` — подключение к WebHDFS/HttpFS, HA, Kerberos, impersonation, квоты, превью файлов, Circuit Breaker).
- **SQL Explorer** (`backend/sql` — Trino DB API, Apache Hive / HiveServer2, AI-ассистент, история и кэширование).
- **Spark Explorer** (`backend/spark` — Apache Livy, PySpark, Scala, Metastore, Circuit Breaker).
- **Развертывание в Kubernetes (Helm)**.

---

## 📑 Содержание
1. [Общие принципы и приоритеты конфигурации](#1-общие-принципы-и-приоритеты-конфигурации)
2. [Общая конфигурация (backend/common)](#2-общая-конфигурация-backendcommon)
   - [Сервер и безопасность (CORS, CSRF, Cookies, JWT)](#21-сервер-и-безопасность-cors-csrf-cookies-jwt)
   - [Аутентификация: LDAPS / Active Directory](#22-аутентификация-ldaps--active-directory)
   - [Аутентификация: Kerberos SPNEGO SSO](#23-аутентификация-kerberos-spnego-sso)
   - [Конфигурация ролевой модели и прав доступа (RBAC)](#24-конфигурация-ролевой-модели-и-прав-доступа-rbac)
   - [Хранилище сессий, токенов и Rate Limiting (Tri-Storage & L1 Cache)](#25-хранилище-сессий-токенов-и-rate-limiting-tri-storage--l1-cache)
   - [Отказоустойчивость: Circuit Breaker, Distributed Lock и Graceful Shutdown](#26-отказоустойчивость-circuit-breaker-distributed-lock-и-graceful-shutdown)
3. [Настройка YARN Explorer](#3-настройка-yarn-explorer)
   - [YARN кластеры и партиции](#31-yarn-кластеры-и-партиции)
   - [Ролевая модель и Change Requests](#32-ролевая-модель-и-change-requests)
4. [Настройка HDFS Explorer](#4-настройка-hdfs-explorer)
5. [Настройка SQL Explorer (Trino & Hive)](#5-настройка-sql-explorer-trino--hive)
   - [Аналитические кластеры](#51-аналитические-кластеры)
   - [Параметры выполнения запросов и сохранение воркспейса](#52-параметры-выполнения-запросов-и-сохранение-воркспейса)
   - [ИИ-ассистент (On-Premise LLM / Ollama / vLLM)](#53-ии-ассистент-on-premise-llm--ollama--vllm)
6. [Настройка Spark Explorer (Livy, PySpark, Scala, Metastore)](#6-настройка-spark-explorer-livy-pyspark-scala-metastore)
   - [Кластеры и интеграция с Apache Livy](#61-кластеры-и-интеграция-с-apache-livy)
   - [Версии Spark и среды Python (HDFS Archives)](#62-версии-spark-и-среды-python-hdfs-archives)
   - [Интеграция с YARN и разграничение очередей](#63-интеграция-с-yarn-и-разграничение-очередей)
   - [Hive Metastore, Iceberg и профили ресурсов](#64-hive-metastore-iceberg-и-профили-ресурсов)
   - [Персистентность рабочих пространств пользователя](#65-персистентность-рабочих-пространств-пользователя)
7. [Переменные окружения](#7-переменные-окружения)
8. [Конфигурация в Kubernetes (Helm)](#8-конфигурация-в-kubernetes-helm)

---

## 1. Общие принципы и приоритеты конфигурации

Каждый бэкенд-сервис загружает конфигурацию по следующей цепочке приоритетов:
1. **Переменные окружения** (наивысший приоритет — переопределяют значения из YAML для секретов и путей).
2. **Файл конфигурации YAML**, заданный через переменную `CONFIG_PATH` или `YARN_CONFIG_PATH` / `HDFS_CONFIG_PATH` / `SQL_CONFIG_PATH` / `SPARK_CONFIG_PATH`.
3. **Локальный файл по умолчанию**: `config/config.yaml` внутри каталога соответствующего сервиса.

> [!IMPORTANT]
> **Production Mode (`debug: false`)**:
> При установке `server.debug: false` платформа активирует строгие проверки безопасности:
> - Запрещается режим авторизации `auth.mode: "mock"`.
> - Флаг `cookie_secure` принудительно переводится в `true`.
> - Блокируется запуск без явно заданного стойкого `secret_key` / `JWT_SECRET_KEY` (мин. 32 символа) или при использовании дефолтных плейсхолдеров (`CHANGE_THIS...`, `your-secret...`).
> - В режиме разработки (`debug: true`), если ключ не передан, генерируется безопасный временный ключ (`secrets.token_urlsafe(32)`).

---

## 2. Общая конфигурация (backend/common)

### 2.1 Сервер и безопасность (CORS, CSRF, Cookies, JWT)

```yaml
server:
  host: "0.0.0.0"
  port: 8000
  debug: false
  # Список разрешенных Origins для браузерных CORS-запросов и строгой валидации CSRF
  cors_origins:
    - "https://hadoop.company.local"
    - "https://hdfs.company.local"
  secure_cookies: true          # Передавать cookie только по HTTPS

security:                       # или auth.jwt
  secret_key: "CHANGE_THIS_TO_RANDOM_SECRET_KEY_MIN_32_CHARS" # Обязателен в production
  algorithm: "HS256"
  access_token_expire_minutes: 480
  cookie_name: "hadoop_explorer_session"
  cookie_samesite: "lax"
```

> [!NOTE]
> **Автоматические HTTP-заголовки безопасности и CSP**:
> Все бэкенд-сервисы автоматически инжектируют в ответы HTTP-заголовки безопасности через middleware `apply_security_headers`:
> - `Content-Security-Policy`: строгие директивы защиты контента для предотвращения XSS (`CSP_DEFAULT_DIRECTIVES` для HDFS/YARN и `CSP_CODE_EDITOR_DIRECTIVES` с поддержкой Web Workers для Monaco Editor в Spark/SQL).
> - `X-Frame-Options: DENY`: предотвращение встраивания в iframe (Clickjacking).
> - `X-Content-Type-Options: nosniff`: защита от подмены MIME-типов.
> - `Referrer-Policy: strict-origin-when-cross-origin`: контроль заголовка `Referer`.
> - `Strict-Transport-Security` (HSTS): автоматически активируется при `secure_cookies: true` или `debug: false`.

### 2.2 Аутентификация: LDAPS / Active Directory

Модуль `CommonLdapAuthService` поддерживает подключение к корпоративным каталогам OpenLDAP, FreeIPA и Microsoft Active Directory с защитой от LDAP Injection (экранирование спецсимволов):

```yaml
auth:
  mode: "hybrid" # Доступные режимы: hybrid | ldaps_only | kerberos_only | mock

ldap:
  enabled: true
  server_uri: "ldaps://ad.company.local:636"
  use_ssl: true
  verify_cert: true             # В production: true с указанием доверенного CA
  ca_cert_file: "/etc/ssl/certs/company-ca.crt"
  bind_dn: "cn=svc_hadoop_explorer,ou=Service Accounts,dc=company,dc=local"
  bind_password: "SecretServicePassword"
  
  # Поиск пользователей
  user_search_base: "ou=Users,dc=company,dc=local"
  user_search_filter: "(&(objectClass=user)(sAMAccountName={username}))" # для OpenLDAP: (&(objectClass=inetOrgPerson)(uid={username}))
  username_attribute: "sAMAccountName"
  email_attribute: "mail"
  display_name_attribute: "displayName"
  
  # Поиск групп
  group_search_base: "ou=Groups,dc=company,dc=local"
  group_search_filter: "(&(objectClass=group)(member={user_dn}))"
  group_attribute: "cn"
  use_user_memberof: true       # Использовать атрибут memberOf для оптимизации запросов
  memberof_attribute: "memberOf"
```

### 2.3 Аутентификация: Kerberos SPNEGO SSO

Позволяет пользователям прозрачно авторизовываться в веб-интерфейсе через системный Kerberos-тикет (заголовок `Authorization: Negotiate <ticket>`):

```yaml
kerberos_sso:                   # или auth.kerberos
  enabled: true
  service_principal: "HTTP/hadoop-explorer.company.local@COMPANY.LOCAL"
  keytab_path: "/etc/security/keytabs/spnego.keytab"
```

### 2.4 Конфигурация ролевой модели и прав доступа (RBAC)

Все сервисы платформы используют унифицированную модель определения прав пользователя `resolve_system_role`:

```yaml
auth:
  # Списки пользователей и групп для назначения ролей
  admin_users: ["admin", "superadmin"]
  admin_groups: ["hadoop-admins", "platform-admins", "domain admins"]
  writer_groups: ["data-engineers", "yarn-operators", "etl-developers"]
```

**Резолюция ролей**:
- Пользователи из `admin_users` или состоящие в любой группе из `admin_groups` получают роль **`ADMIN`** (краткий UI-бейдж `ADM`).
- Пользователи, входящие в группы из `writer_groups`, получают роль **`WRITER`** (краткий UI-бейдж `RW`).
- Все остальные аутентифицированные пользователи получают роль **`READER`** (краткий UI-бейдж `RO`).
- Роль вычисляется на сервере, включается в полезную нагрузку токена и сессии, и возвращается через `/api/v1/auth/me`.

### 2.5 Хранилище сессий, токенов и Rate Limiting (SessionStore & Tri-Storage)

Платформа использует универсальный слой хранения данных (`SessionStore` и `BaseStorageService`), поддерживающий:
- **Персистентность сессий пользователей (`active_sessions`)**: При авторизации пользователя активная сессия сохраняется в базе данных с абсолютным Unix Timestamp `expires_at` (полный срок жизни JWT, по умолчанию 480 минут). При перезапуске бэкенд-контейнеров или сервисов пользователи **не разлогиниваются**, сессия автоматически восстанавливается из БД.
- **Авто-конвертация TTL**: Модуль `SessionStore` автоматически определяет формат времени жизни токена (абсолютный timestamp или дельта секунд) и исключает ошибки истечения срока.
- **Черный список отозванных токенов (`revoked_tokens`)**: Двухуровневое кэширование (L1 In-Memory LRU Cache `L1RevokedTokenCache` со сроком устаревания + L2 база данных/Redis) для мгновенной валидации отозванных токенов при logout без задержек I/O.
- **Ограничение частоты запросов (Rate Limiting)**: Алгоритм скользящего окна (Sliding Window) с хранением счетчиков в БД/Redis.

Поддерживаемые бэкенды:
- **SQLite WAL** (`sqlite:///./data/hadoop_explorer.db` или `/app/data/*.db`) — встроенное хранилище по умолчанию с режимом Write-Ahead Logging.
- **PostgreSQL** (`postgresql://user:password@pg-host:5432/hadoop_explorer`) — рекомендуется для High Availability и мульти-инстанс развертываний в Kubernetes.
- **Redis** (`redis://redis-host:6379/0`) — опционально для распределенного L2 кэширования и распределенных блокировок (`DistributedLock`).

```yaml
database:
  # SQLite (по умолчанию в контейнере):
  url: "sqlite:////app/data/service_sessions.db"
  # Либо PostgreSQL (для HA в production):
  # url: "postgresql://user:password@pg-host:5432/hadoop_explorer"
  redis_url: "redis://redis-host:6379/0" # Опционально
```

### 2.6 Отказоустойчивость: Circuit Breaker, Distributed Lock и Graceful Shutdown

- **Circuit Breaker**: автоматическое обнаружение сбоев сетевых вызовов к кластерам Hadoop/Spark/YARN. При 5 подряд сетевых ошибках или таймаутах узел помечается как `OPEN` на 30 секунд (Fast-Fail без блокировки пула потоков), после чего переходит в `HALF_OPEN` для пробного запроса. 4xx клиентские ошибки игнорируются.
- **Distributed Lock**: поддержка взаимного исключения для критических секций через Redis (`SET NX PX` + Lua) с fallback на in-memory locks.
- **Graceful Shutdown**: перехват сигналов SIGTERM/SIGINT с корректным завершением `ThreadPoolExecutor`, отменой асинхронных задач и закрытием соединений с базами данных и сетевыми клиентами.

---

## 3. Настройка YARN Explorer

Файл конфигурации: `backend/yarn/config/config.yaml`.

### 3.1 YARN кластеры и партиции

```yaml
clusters:
  - id: "prod-yarn"
    name: "Production Hadoop Cluster"
    description: "Основной YARN кластер (120 узлов)"
    resource_manager_urls:
      - "http://rm1.prod.company.local:8088"
      - "http://rm2.prod.company.local:8088" # High Availability failover
    kerberos_enabled: true
    kerberos_principal: "yarn/rm1.prod.company.local@COMPANY.LOCAL"
    impersonation_enabled: true
    default_partition: "DEFAULT"
    partitions:
      - "DEFAULT"
      - "GPU"
      - "HIGH_MEM"
    resource_mode: "percentage" # percentage (Capacity Scheduler %) | absolute (MB / Cores)
    total_resources:
      memory_mb: 2097152        # 2 TB
      vcores: 1024
```

### 3.2 Ролевая модель и Change Requests

YARN Explorer поддерживает трехуровневую ролевую модель (`ADMIN`, `WRITER`, `READER`) с соблюдением принципа четырех глаз (Four-Eyes Principle) и защитой от состояний гонки через `DistributedLock` при согласовании заявок:

```yaml
acl:
  ui_access:
    allowed_users: ["*"]
    allowed_groups: ["*"]

  roles:
    admin:
      groups: ["hadoop-admins", "platform-admins"]
      users: ["admin_user"]
    writer:
      groups: ["yarn-operators", "data-engineers"]
      users: []
    reader:
      groups: ["*"]
      users: ["*"]
```

---

## 4. Настройка HDFS Explorer

Файл конфигурации: `backend/hdfs/config/config.yaml`.

### Пример секции `clusters` для HDFS:

```yaml
clusters:
  - id: "datalake-prod"
    name: "Production DataLake"
    webhdfs_url: "http://nn1.prod.company.local:9870/webhdfs/v1"
    standby_webhdfs_url: "http://nn2.prod.company.local:9870/webhdfs/v1"
    auth_type: "kerberos"       # kerberos | simple
    kerberos_principal: "hdfs/nn1.prod.company.local@COMPANY.LOCAL"
    enable_impersonation: true  # Передача doAs={username} при запросах к WebHDFS
    default_root_path: "/user"
    preview_max_bytes: 10485760 # Лимит чтения файлов для превью (10 MB)
    acl:
      allow_all_authenticated: false
      allowed_groups:
        - "hadoop-users"
        - "data-engineers"
        - "analytics"
      allowed_users: []
      admin_groups:
        - "hadoop-admins"
        - "domain admins"

  - id: "datalake-archive"
    name: "Cold Storage Archive"
    webhdfs_url: "http://archive-httpfs.company.local:14000/webhdfs/v1"
    auth_type: "simple"
    enable_impersonation: true
    default_root_path: "/archive"
    acl:
      allow_all_authenticated: true
```

---

## 5. Настройка SQL Explorer (Trino & Hive)

Файл конфигурации: `backend/sql/config/config.yaml`.

### 5.1 Аналитические кластеры

```yaml
clusters:
  # 1. Trino Coordinator (Trino DB API)
  - id: "trino-prod"
    name: "Trino Production Cluster"
    type: "trino"
    host: "trino-coordinator.company.local"
    port: 8443
    use_ssl: true
    catalog: "hive"
    schema: "default"
    auth:
      type: "basic"             # basic | kerberos | none
      user: "svc_sql_explorer"
      password: "TrinoPassword"
    impersonation:
      enabled: true
      method: "x-trino-user"    # Заголовок X-Trino-User для аудита и Ranger
    allow_dml_ddl: false        # false = Read-Only режим (только SELECT / EXPLAIN)
    acl:
      allowed_groups: ["bi-analysts", "data-engineers", "data-platform-admins"]

  # 2. Apache Hive / Cloudera HiveServer2
  - id: "hive-prod"
    name: "HiveServer2 Core"
    type: "hive"
    host: "hs2.company.local"
    port: 10000
    auth:
      type: "kerberos"          # kerberos | ldap | plain | nosasl
      kerberos_service_name: "hive"
    impersonation:
      enabled: true
      method: "doAs"            # Проброс пользователя через hive.server2.proxy.user
    allow_dml_ddl: true
    acl:
      allowed_groups: ["data-engineers", "data-platform-admins"]
```

### 5.2 Параметры выполнения запросов и сохранение воркспейса

```yaml
query_defaults:
  max_rows_in_ui: 10000         # Максимальное число строк, отображаемое в браузере
  default_limit: 1000           # Автоматически добавляемый LIMIT при отсутствии
  auto_add_limit: true          # Включить автодобавление LIMIT
  query_timeout_seconds: 600    # Таймаут исполнения запроса (10 минут)
  results_ttl_seconds: 604800   # Время жизни кэшированных результатов в секундах (7 дней)
```

#### Персистентность рабочих пространств (User Workspace)
SQL Explorer сохраняет открытые вкладки редактора, введённый SQL-код, привязанные кластеры и каталоги, а также последние результаты выполнения в БД (модель `SqlUserWorkspace`, эндпоинт `/api/v1/workspace`).
- Для каждого аутентифицированного пользователя создается изолированное рабочее пространство.
- При смене пользователя загружается его персональный контекст, исключая отображение чужой истории.

### 5.3 ИИ-ассистент (On-Premise LLM / Ollama / vLLM)

SQL Explorer включает модуль генерации, оптимизации и автоисправления SQL-запросов:

```yaml
ai:
  enabled: true
  provider: "openai_compatible" # openai_compatible | mock
  base_url: "http://llm-server.company.local:11434/v1" # Endpoint Ollama, vLLM или LiteLLM
  api_key: "ollama"
  model: "qwen2.5-coder:7b"     # Рекомендуемые: qwen2.5-coder, deepseek-coder
  timeout_seconds: 30
  temperature: 0.1
  max_tokens: 2048
```

---

## 6. Настройка Spark Explorer (Livy, PySpark, Scala, Metastore)

Файл конфигурации: `backend/spark/config/config.yaml`.

### 6.1 Кластеры и интеграция с Apache Livy

Spark Explorer связывается с серверами **Apache Livy** для выполнения интерактивных сессий и пакетных расчетов:

```yaml
clusters:
  - id: "prod-hadoop"
    name: "Production Hadoop (Spark & HMS)"
    description: "Кластер CDP с поддержкой Spark 3.5, 3.2 и 2.4"
    type: "spark"
    livy_url: "http://livy-prod.company.local:8998"
    use_ssl: false
    auth:
      type: "kerberos"          # kerberos | ldap | plain
      service_name: "livy"
    impersonation:
      enabled: true
      method: "proxyUser"       # Проброс пользователя в Livy
    acl:
      allowed_groups: ["*"]
      allowed_users: []
```

### 6.2 Версии Spark и среды Python (HDFS Archives)

Поддерживается выбор версии Spark и изолированных Conda/Venv окружений, упакованных в HDFS:

```yaml
    spark_versions:
      - id: "spark-3.5"
        name: "Apache Spark 3.5.1 (Scala 2.12)"
        is_default: true
        spark_archive: "hdfs:///apps/spark/spark-3.5.1-bin-hadoop3.tgz"
        python_versions:
          - id: "py310-default"
            name: "Python 3.10 (Standard Runtime)"
            python_path: "/opt/conda/envs/py310/bin/python"
            is_default: true
          - id: "py310-ml"
            name: "Python 3.10 (ML / PyTorch / Pandas / Sklearn)"
            archive_path: "hdfs:///apps/python/envs/py310_ml.tar.gz#environment"
            python_path: "./environment/bin/python"
            is_default: false
      - id: "spark-2.4"
        name: "Apache Spark 2.4.8 (Legacy Scala 2.11)"
        is_default: false
        livy_url: "http://livy-spark2.prod.company.local:8998"
        spark_archive: "hdfs:///apps/spark/spark-2.4.8-bin-hadoop2.7.tgz"
        python_versions:
          - id: "py37-legacy"
            name: "Python 3.7 (Legacy)"
            python_path: "/opt/conda/envs/py37/bin/python"
            is_default: true
```

### 6.3 Интеграция с YARN и разграничение очередей

```yaml
    yarn:
      cluster_id: "prod-yarn"
      resource_manager_urls:
        - "http://rm1.prod.company.local:8088"
        - "http://rm2.prod.company.local:8088"
      default_queue: "root.analytics"
      allowed_queues: ["root.analytics", "root.adhoc", "root.etl"]
      queue_acl:
        root.etl:
          allowed_groups: ["data-engineers", "hadoop-admins"]
        root.adhoc:
          allowed_groups: ["*"]
```

### 6.4 Hive Metastore, Iceberg и профили ресурсов

```yaml
    # Подключение каталогов данных (HMS / Iceberg)
    metastores:
      - id: "lakehouse-core"
        name: "Lakehouse Core Metastore (HMS)"
        is_default: true
        uris: "thrift://hms1.prod.company.local:9083,thrift://hms2.prod.company.local:9083"
        spark_conf:
          "spark.hadoop.hive.metastore.uris": "thrift://hms1.prod.company.local:9083,thrift://hms2.prod.company.local:9083"
          "spark.sql.catalogImplementation": "hive"

    # Предустановленные профили ресурсов для сессий
    resource_profiles:
      small:
        name: "Small (Driver 2G/1c, 2 Exec 4G/2c)"
        driver_memory: "2g"
        driver_cores: 1
        executor_memory: "4g"
        executor_cores: 2
        num_executors: 2
      medium:
        name: "Medium (Driver 4G/2c, 4 Exec 8G/2c)"
        driver_memory: "4g"
        driver_cores: 2
        executor_memory: "8g"
        executor_cores: 2
        num_executors: 4
```

### 6.5 Персистентность рабочих пространств пользователя

Spark Explorer сохраняет состояние сессий, активные вкладки, отдельные буферы кода (`pyspark`, `scalaspark`, `sql`) и буферы результатов в базу данных (`SparkUserWorkspace`, `/api/v1/workspace`):
- При смене языка (PySpark ↔ Scala ↔ SQL) результаты вычислений изолируются и не затираются.
- При входе нового пользователя загружается его индивидуальное рабочее пространство.

---

## 7. Переменные окружения

Все ключевые параметры могут быть заданы через переменные среды операционной системы или контейнера:

| Переменная | Описание | Значение по умолчанию |
|---|---|---|
| `CONFIG_PATH` / `YARN_CONFIG_PATH` / `HDFS_CONFIG_PATH` / `SQL_CONFIG_PATH` / `SPARK_CONFIG_PATH` | Путь к конфигурационному YAML файлу | `config/config.yaml` |
| `SERVER_DEBUG` | Режим отладки (`true` / `false`) | `false` |
| `JWT_SECRET_KEY` / `HDFS_SECRET_KEY` | Секретный ключ подписи JWT (мин. 32 симв., обязателен в prod) | — (в dev автогенерируется) |
| `LDAP_SERVER_URI` / `HDFS_LDAP_URI` | URI LDAP сервера (`ldaps://...:636`) | — |
| `LDAP_BIND_PASSWORD` / `HDFS_LDAP_PASSWORD` | Пароль сервисной учетной записи LDAP | — |
| `DATABASE_URL` / `HDFS_DATABASE_URL` | Строка подключения к базе данных | `sqlite+aiosqlite:///...` |
| `STORAGE_URL` / `REDIS_URL` | URL подключения к Redis (кэш токенов, Rate Limiter, Distributed Lock) | — |
| `LIVY_URL` | Адрес сервера Apache Livy для Spark Explorer | `http://localhost:8998` |
| `HIVE_METASTORE_URI` | Thrift URI каталога Hive Metastore | `thrift://localhost:9083` |
| `CORS_ORIGINS` | Доверенные адреса через запятую | `http://localhost:8000` |
| `KRB5_CONFIG` | Путь к файлу конфигурации Kerberos | `/etc/krb5.conf` |
| `KRB5_KTNAME` | Путь к Keytab-файлу сервиса | `/etc/security/keytabs/...` |

---

## 8. Конфигурация в Kubernetes (Helm)

При развертывании через Umbrella Chart `helm/hadoop-explorer` конфигурация каждого компонента передается в секции `values.yaml`:

```yaml
global:
  environment: "production"
  ingressDomain: "hadoop.company.local"

yarn-explorer:
  enabled: true
  replicaCount: 2
  config:
    clusters:
      - id: "prod-yarn"
        resource_manager_urls:
          - "http://rm1.prod.company.local:8088"
          - "http://rm2.prod.company.local:8088"

hdfs-explorer:
  enabled: true
  replicaCount: 2
  config:
    server:
      debug: false
    auth:
      mode: "hybrid"
    ldap:
      server_uri: "ldaps://ad.company.local:636"
      bind_dn: "cn=svc_hdfs,ou=services,dc=company,dc=local"
  secrets:
    jwtSecretKey: "super-secure-production-jwt-token-key-32-chars"
    ldapBindPassword: "LdapPasswordHere"

sql-explorer:
  enabled: true
  config:
    ai:
      enabled: true
      base_url: "http://ollama-service.ai.svc:11434/v1"
      model: "qwen2.5-coder:7b"

spark-explorer:
  enabled: true
  replicaCount: 2
  config:
    clusters:
      - id: "prod-hadoop"
        livy_url: "http://livy.hadoop.svc:8998"
```

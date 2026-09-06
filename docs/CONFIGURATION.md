# ⚙️ Руководство по конфигурации Hadoop Explorer Platform

В данном документе подробно описаны параметры конфигурации, форматы файлов, переменные окружения и лучшие практики настройки компонентов платформы **Hadoop Explorer**:
- **Общие модули** (`backend/common` — безопасность, сессии, LDAP/Active Directory, Kerberos SPNEGO, хранилища).
- **HDFS Explorer** (`backend/hdfs` — подключение к WebHDFS/HttpFS, HA, Kerberos, impersonation, квоты, превью файлов).
- **SQL Explorer** (`backend/sql` — Trino DB API, Apache Hive / HiveServer2, AI-ассистент, история и кэширование).
- **YARN Explorer** (`backend/yarn` — YARN RM HA, Capacity Scheduler, партиции, Change Requests, генерация XML).
- **Развертывание в Kubernetes (Helm)**.

---

## 📑 Содержание
1. [Общие принципы и приоритеты конфигурации](#1-общие-принципы-и-приоритеты-конфигурации)
2. [Общая конфигурация (backend/common)](#2-общая-конфигурация-backendcommon)
   - [Сервер и безопасность (CORS, CSRF, Cookies, JWT)](#21-сервер-и-безопасность-cors-csrf-cookies-jwt)
   - [Аутентификация: LDAPS / Active Directory](#22-аутентификация-ldaps--active-directory)
   - [Аутентификация: Kerberos SPNEGO SSO](#23-аутентификация-kerberos-spnego-sso)
   - [Хранилище сессий, токенов и Rate Limiting (Tri-Storage)](#24-хранилище-сессий-токенов-и-rate-limiting-tri-storage)
3. [Настройка HDFS Explorer](#3-настройка-hdfs-explorer)
4. [Настройка SQL Explorer (Trino & Hive)](#4-настройка-sql-explorer-trino--hive)
   - [Аналитические кластеры](#41-аналитические-кластеры)
   - [Параметры выполнения запросов](#42-параметры-выполнения-запросов)
   - [ИИ-ассистент (On-Premise LLM / Ollama / vLLM)](#43-ии-ассистент-on-premise-llm--ollama--vllm)
5. [Настройка YARN Explorer](#5-настройка-yarn-explorer)
   - [YARN кластеры и партиции](#51-yarn-кластеры-и-партиции)
   - [Ролевая модель и Change Requests](#52-ролевая-модель-и-change-requests)
6. [Переменные окружения](#6-переменные-окружения)
7. [Конфигурация в Kubernetes (Helm)](#7-конфигурация-в-kubernetes-helm)

---

## 1. Общие принципы и приоритеты конфигурации

Каждый бэкенд-сервис загружает конфигурацию по следующей цепочке приоритетов:
1. **Переменные окружения** (наивысший приоритет — переопределяют значения из YAML для секретов и путей).
2. **Файл конфигурации YAML**, заданный через переменную `CONFIG_PATH` или `HDFS_CONFIG_PATH`.
3. **Локальный файл по умолчанию**: `config/config.yaml` внутри каталога соответствующего сервиса.

> [!IMPORTANT]
> **Production Mode (`debug: false`)**:
> При установке `server.debug: false` платформа активирует строгие проверки безопасности:
> - Запрещается режим авторизации `auth.mode: "mock"`.
> - Флаг `cookie_secure` принудительно переводится в `true`.
> - Блокируется запуск со стандартным или коротким `secret_key` (< 32 символов).

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
  secret_key: "CHANGE_THIS_TO_RANDOM_SECRET_KEY_MIN_32_CHARS"
  algorithm: "HS256"
  access_token_expire_minutes: 480
  cookie_name: "hadoop_explorer_session"
  cookie_samesite: "lax"
```

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

### 2.4 Хранилище сессий, токенов и Rate Limiting (Tri-Storage)

Платформа поддерживает три бэкенда хранения для отзыва токенов (JWT Blacklist), скользящих лимитов Rate Limiting и истории:
- **SQLite WAL** (для автономного или легкого развертывания).
- **PostgreSQL** (рекомендуется для High Availability кластеров).
- **Redis** (опционально, для распределенного L2 кэширования).

```yaml
database:
  # SQLite:
  url: "sqlite+aiosqlite:///./data/hadoop_explorer.db"
  # Либо PostgreSQL:
  # url: "postgresql+asyncpg://user:password@pg-host:5432/hadoop_explorer"
  redis_url: "redis://redis-host:6379/0" # Опционально
```

---

## 3. Настройка HDFS Explorer

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

## 4. Настройка SQL Explorer (Trino & Hive)

Файл конфигурации: `backend/sql/config/config.yaml`.

### 4.1 Аналитические кластеры

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

### 4.2 Параметры выполнения запросов

```yaml
query_defaults:
  max_rows_in_ui: 10000         # Максимальное число строк, отображаемое в браузере
  default_limit: 1000           # Автоматически добавляемый LIMIT при отсутствии
  auto_add_limit: true          # Включить автодобавление LIMIT
  query_timeout_seconds: 600    # Таймаут исполнения запроса (10 минут)
  results_ttl_seconds: 604800   # Время жизни кэшированных результатов в секундах (7 дней)
```

### 4.3 ИИ-ассистент (On-Premise LLM / Ollama / vLLM)

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

## 5. Настройка YARN Explorer

Файл конфигурации: `backend/yarn/config/config.yaml`.

### 5.1 YARN кластеры и партиции

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

### 5.2 Ролевая модель и Change Requests

YARN Explorer поддерживает трехуровневую ролевую модель (`ADMIN`, `WRITER`, `READER`) с соблюдением принципа четырех глаз (Four-Eyes Principle) при согласовании заявок:

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

## 6. Переменные окружения

Все ключевые параметры могут быть заданы через переменные среды операционной системы или контейнера:

| Переменная | Описание | Значение по умолчанию |
|---|---|---|
| `CONFIG_PATH` / `HDFS_CONFIG_PATH` | Путь к конфигурационному YAML файлу | `config/config.yaml` |
| `SERVER_DEBUG` | Режим отладки (`true` / `false`) | `false` |
| `JWT_SECRET_KEY` / `HDFS_SECRET_KEY` | Секретный ключ подписи JWT (мин. 32 симв.) | — |
| `LDAP_SERVER_URI` / `HDFS_LDAP_URI` | URI LDAP сервера (`ldaps://...:636`) | — |
| `LDAP_BIND_PASSWORD` / `HDFS_LDAP_PASSWORD` | Пароль сервисной учетной записи LDAP | — |
| `DATABASE_URL` / `HDFS_DATABASE_URL` | Строка подключения к базе данных | `sqlite+aiosqlite:///...` |
| `STORAGE_URL` / `REDIS_URL` | URL подключения к Redis | — |
| `CORS_ORIGINS` | Доверенные адреса через запятую | `http://localhost:8000` |
| `KRB5_CONFIG` | Путь к файлу конфигурации Kerberos | `/etc/krb5.conf` |
| `KRB5_KTNAME` | Путь к Keytab-файлу сервиса | `/etc/security/keytabs/...` |

---

## 7. Конфигурация в Kubernetes (Helm)

При развертывании через Umbrella Chart `helm/hadoop-explorer` конфигурация каждого компонента передается в секции `values.yaml`:

```yaml
global:
  environment: "production"
  ingressDomain: "hadoop.company.local"

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

yarn-explorer:
  enabled: true
```

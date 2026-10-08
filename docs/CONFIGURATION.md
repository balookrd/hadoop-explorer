# ⚙️ Руководство по конфигурации Hadoop Explorer Platform

В данном документе подробно описаны параметры конфигурации, форматы файлов, переменные окружения и лучшие практики настройки компонентов платформы **Hadoop Explorer**:
- **Общий стартер безопасности** (`backend/common-security-starter` — Java 21 / Spring Boot 3, SPNEGO, LDAP/Active Directory, JWT, L1/L2 сессии, Circuit Breaker, Rate Limiter, CSRF Guard).
- **YARN Explorer** (`backend/yarn` — Java 21 LTS / Spring Boot 3, YARN RM HA, Capacity Scheduler, партиции, Change Requests, Four-Eyes Principle, генерация XML, Ansible AWX).
- **HDFS Explorer** (`backend/hdfs` — Java 21 LTS / Spring Boot 3, подключение к HDFS Client, HA NameNode, Kerberos, doAs имперсонация, квоты, превью Parquet/ORC, Circuit Breaker).
- **SQL Explorer** (`backend/sql` — Java 21 LTS / Spring Boot 3, Trino DB API, Apache Hive / HiveServer2, AI-ассистент, история и кэширование).
- **Spark Explorer** (`backend/spark` — Java 21 LTS / Spring Boot 3, Apache Livy, PySpark, Scala, DAG Pipelines, Metastore, Circuit Breaker).
- **Hadoop gRPC Replicator** (`backend/replicator` — Java 21 / Spring Boot 3 Orchestrator, нативный Java gRPC Worker Daemon, топология ЦОД, Hierarchical Token Bucket, Kerberos, Snapshot Diff, Cron Scheduler).
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
   - [Интеграция с Ansible AWX (доставка и применение конфигурации)](#33-интеграция-с-ansible-awx-доставка-и-применение-конфигурации)
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
7. [Настройка Hadoop gRPC Replicator (DC-DC WAN Sync & Throttling)](#7-настройка-hadoop-grpc-replicator-dc-dc-wan-sync--throttling)
   - [Архитектура и компоненты (Orchestrator, Receiver, Worker)](#71-архитектура-и-компоненты-orchestrator-receiver-worker)
   - [Топология дата-центров и кластеров](#72-топология-дата-центров-и-кластеров)
   - [Многоуровневые лимиты полосы (DC-DC и HDFS-HDFS)](#73-многоуровневые-лимиты-полосы-dc-dc-и-hdfs-hdfs)
   - [Безопасность, RBAC и системная техучетка Kerberos](#74-безопасность-rbac-и-системная-техучетка-kerberos)
   - [Настройка gRPC Receiver и буфера Staging](#75-настройка-grpc-receiver-и-буфера-staging)
   - [Настройка gRPC Worker и параметров передачи](#76-настройка-grpc-worker-и-параметров-передачи)
   - [Планировщик периодических задач (Cron Scheduler)](#77-планировщик-периодических-задач-cron-scheduler)
   - [Эталонный конфигурационный файл config.yaml](#78-эталонный-конфигурационный-файл-configyaml)
8. [Переменные окружения](#8-переменные-окружения)
9. [Конфигурация в Kubernetes (Helm)](#9-конфигурация-в-kubernetes-helm)
10. [Мониторинг Prometheus и дашборды Grafana](#10-мониторинг-prometheus-и-дашборды-grafana)
11. [Руководства администратора по развертыванию (DevOps Hub)](#11-руководства-администратора-по-развертыванию-devops-hub)

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
  url: "jdbc:sqlite:/app/data/service_sessions.db"
  # Либо PostgreSQL (для HA в production):
  # url: "jdbc:postgresql://user:password@pg-host:5432/hadoop_explorer"
  redis_url: "redis://redis-host:6379/0" # Опционально
```

### 2.6 Отказоустойчивость: Circuit Breaker, Retry, Global Exception Handlers, Distributed Lock и Graceful Shutdown

- **Circuit Breaker & Prometheus Metrics**: автоматическое обнаружение сбоев сетевых вызовов к кластерам Hadoop/Spark/YARN. При 5 подряд сетевых ошибках или таймаутах узел помечается как `OPEN` на 30 секунд (Fast-Fail без блокировки пула потоков), после чего переходит в `HALF_OPEN` для пробного запроса. 4xx клиентские ошибки игнорируются. Метрики экспортируются через Actuator `/actuator/prometheus` (или `/metrics`).
- **Retry с экспоненциальным Backoff**: автоматический повтор при возникновении транзиентных ошибок соединения и сокетов (`java.net.http.HttpConnectTimeoutException`, `java.io.IOException`) со случайным джиттером.
- **Global Exception Handlers (`@ControllerAdvice`)**: перехват всех необработанных исключений 500 сокрытием внутреннего stack trace (защита от CWE-209), генерацией `incidentId` и возвратом стандартизированного ответа.
- **Distributed Lock**: поддержка взаимного исключения для критических секций через Redis (`SET NX PX` + Lua) с fallback на in-memory locks.
- **Graceful Shutdown**: перехват сигналов SIGTERM/SIGINT с корректным завершением `ThreadPoolTaskExecutor`, отменой фоновых задач и закрытием соединений с базами данных и сетевыми клиентами.

### 2.7 Сквозное шифрование TLS/HTTPS и mTLS для REST и gRPC каналов

Все микросервисы платформы (`yarn`, `hdfs`, `sql`, `spark`, `replicator/orchestrator`) поддерживают централизованную конфигурацию HTTPS/TLS веб-серверов и исходящих REST-клиентов через `common-security-starter`:

```yaml
hadoop:
  security:
    tls:
      enabled: true                     # Активация TLS для встроенного Tomcat веб-сервера
      key-store-path: "/etc/security/tls/keystore.p12" # Путь к PKCS12 / JKS Keystore
      key-store-password: "${TLS_KEYSTORE_PASSWORD}"
      key-store-type: "PKCS12"
      trust-store-path: "/etc/security/tls/truststore.p12" # Хранилище доверенных CA
      trust-store-password: "${TLS_TRUSTSTORE_PASSWORD}"
      trust-store-type: "PKCS12"
      client-auth: "NONE"               # Режим mTLS: NONE | OPTIONAL | REQUIRE
      auto-generate-self-signed: true   # Автогенерация временного Keystore при отсутствии файла
      insecure-skip-verify: false       # Отключение проверки сертификатов для dev-стендов
      enabled-protocols:
        - "TLSv1.3"
        - "TLSv1.2"
      ciphers: []                       # Список допустимых шифронаборов (по умолчанию все безопасные)
```

**Переменные окружения для Spring Boot сервисов**:
- `HADOOP_SECURITY_TLS_ENABLED=true`
- `HADOOP_SECURITY_TLS_KEY_STORE_PATH=/etc/security/tls/keystore.p12`
- `HADOOP_SECURITY_TLS_KEY_STORE_PASSWORD=secret`
- `HADOOP_SECURITY_TLS_TRUST_STORE_PATH=/etc/security/tls/truststore.p12`
- `HADOOP_SECURITY_TLS_CLIENT_AUTH=REQUIRE` (для включения двустороннего mTLS)
- `HADOOP_SECURITY_TLS_INSECURE_SKIP_VERIFY=true` (только для тестирования)

---


## 3. Настройка YARN Explorer

Файл конфигурации: `backend/yarn/src/main/resources/application.yml` (или внешний файл `application.yml` / переменные окружения).

### 3.1 YARN кластеры и партиции

```yaml
yarn:
  clusters:
    - id: "prod-yarn"
      name: "Production Hadoop Cluster"
      description: "Основной YARN кластер (120 узлов)"
      resource-manager-urls:
        - "http://rm1.prod.company.local:8088"
        - "http://rm2.prod.company.local:8088" # High Availability failover
      kerberos-enabled: true
      kerberos-principal: "yarn/rm1.prod.company.local@COMPANY.LOCAL"
      impersonation-enabled: true
      default-partition: "DEFAULT"
      partitions:
        - "DEFAULT"
        - "GPU"
        - "HIGH_MEM"
      resource-mode: "percentage" # percentage (Capacity Scheduler %) | absolute (MB / Cores)
      total-resources:
        memory-mb: 2097152        # 2 TB
        vcores: 1024
```

### 3.2 Ролевая модель и Change Requests

YARN Explorer поддерживает трехуровневую ролевую модель (`ADMIN`, `WRITER`, `READER`) с соблюдением принципа четырех глаз (Four-Eyes Principle, запрет самосогласования заявок их создателем):

```yaml
yarn:
  acl:
    enforce-four-eyes: true
  clusters:
    - id: "prod-yarn"
      acl:
        allowed-users: ["*"]
        allowed-groups: ["*"]
        roles:
          admin:
            groups: ["hadoop-admins", "platform-admins"]
            users: ["admin_user"]
          writer:
            groups: ["yarn-operators", "data-engineers"]
            users: ["writer_user"]
          reader:
            groups: ["*"]
            users: ["*"]
```

### 3.3 Интеграция с Ansible AWX (доставка и применение конфигурации)

YARN Explorer поддерживает автоматизированную доставку и горячее применение сгенерированной XML-конфигурации через запуск Job Template в **Ansible AWX / Red Hat Ansible Automation Platform**:

```yaml
yarn:
  awx:
    # Глобальные параметры подключения к AWX
    enabled: true                          # Включение интеграции с AWX
    base-url: "https://awx.company.local"  # Базовый URL сервера AWX
    token: "SampleAwxApplicationTokenHere" # Токен приложения AWX (PAT / OAuth2)
    verify-ssl: true                       # Проверка TLS/SSL сертификата сервера
    default-job-template-id: 101           # ID Job Template по умолчанию
    poll-interval-seconds: 2               # Интервал опроса статуса задачи (сек)
    timeout-seconds: 180                   # Таймаут ожидания завершения задачи (сек)

clusters:
  - id: "prod-yarn"
    name: "Production Hadoop Cluster"
    # ...
    awx:
      enabled: true                      # Включение деплоя для конкретного кластера
      job_template_id: 101               # Индивидуальный ID Job Template кластера
```

#### Соответствующие переменные окружения:
| Переменная | Пример значения | Описание |
|---|---|---|
| `AWX_ENABLED` | `true` | Активация модуля автоматизации AWX |
| `AWX_BASE_URL` | `https://awx.company.local` | Базовый URL сервера AWX |
| `AWX_TOKEN` | `BearerSecretToken...` | Токен доступа к REST API AWX |
| `AWX_VERIFY_SSL` | `true` | Проверка TLS сертификата сервера AWX |
| `AWX_DEFAULT_JOB_TEMPLATE_ID` | `101` | Числовой идентификатор Job Template |
| `AWX_TIMEOUT_SECONDS` | `180` | Таймаут ожидания завершения задачи (сек) |

> 📖 **Пошаговая инструкция по развертыванию роли Ansible, настройке шаблона AWX и процессу Rollback приведена в [docs/awx-yarn-deployment.md](awx-yarn-deployment.md).**
> 📖 **Сценарии применения в интерфейсе Change Requests описаны в [docs/yarn-user-guide.md](yarn-user-guide.md).**

---

## 4. Настройка HDFS Explorer

Файл конфигурации: `backend/hdfs/src/main/resources/application.yml` (или внешний файл `application.yml` / переменные окружения).

### Пример секции `hadoop.hdfs.clusters`:

```yaml
hadoop:
  hdfs:
    clusters:
      - id: "datalake-prod"
        name: "Production DataLake"
        description: "Основной аналитический кластер HDFS"
        webhdfs-urls:
          - "http://nn1.prod.company.local:9870/webhdfs/v1"
          - "http://nn2.prod.company.local:9870/webhdfs/v1"
        hdfs-rpc-urls:
          - "hdfs://nn1.prod.company.local:8020"
          - "hdfs://nn2.prod.company.local:8020"
        auth-type: "kerberos"       # kerberos | simple
        service-principal: "hdfs/nn1.prod.company.local@COMPANY.LOCAL"
        keytab-path: "/etc/security/keytabs/hdfs-explorer.keytab"
        timeout-seconds: 30
        preview-max-bytes: 10485760 # Лимит чтения файлов для превью (10 MB)
        default-path: "/user/{username}"
        mock-storage: false
        acl:
          allowed-groups:
            - "hadoop-users"
            - "data-engineers"
            - "analytics"
          admin-groups:
            - "hadoop-admins"
            - "domain admins"

      - id: "datalake-archive"
        name: "Cold Storage Archive"
        webhdfs-urls:
          - "http://archive-httpfs.company.local:14000/webhdfs/v1"
        auth-type: "simple"
        default-path: "/archive"
        mock-storage: false
        acl:
          allowed-groups:
            - "domain users"
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

#### Кэш результатов запросов и ротация по TTL (`data/results/`)
Результаты выполнения запросов сохраняются в сжатом виде (`{id}.json.gz`) в единую директорию платформы `data/results/` (как для SQL Explorer, так и для Spark Explorer).
- **TTL хранения**: `results_ttl_seconds` или переменная окружения `RESULTS_TTL_SECONDS` (по умолчанию `604800` с / 7 дней).
- **Периодическая очистка**: фоновый процесс проверяет директорию с интервалом `RESULTS_CLEANUP_INTERVAL_SECONDS` (по умолчанию `3600` с / 1 час) и при старте сервиса, удаляя устаревшие файлы.

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

#### Пользовательские (Ad-hoc) виртуальные окружения Python
Помимо преднастроенных пресетов в `config.yaml`, пользователи могут подключать собственные изолированные virtualenv/conda-пакеты прямо в диалоговом окне параметров сессии UI:
- **`custom_python_archive`**: HDFS-путь к архиву окружения (`hdfs:///.../*.tar.gz#environment`). Если фрагмент `#алиас` опущен, платформа автоматически добавляет `#environment`.
- **`custom_python_path`**: относительный путь к интерпретатору внутри распакованного каталога (по умолчанию `./environment/bin/python`).
- Платформа передает архив в директиву `archives` Apache Livy и проставляет конфигурации `spark.pyspark.python` и `spark.pyspark.driver.python`.


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

## 7. Настройка Hadoop gRPC Replicator (DC-DC WAN Sync & Throttling)

Сервис межкластерной репликации `backend/replicator` реализован на стеке Java 21 LTS (Spring Boot 3 + gRPC) и предназначен для высокоскоростной и управляемой синхронизации больших массивов данных HDFS между распределенными ЦОД через WAN-соединения. Конфигурация сервиса задается через `application.yml` или системные переменные окружения.

### 7.1 Архитектура и компоненты (Orchestrator, Agent)

Сервис состоит из следующих компонентов:
1. **Replicator Orchestrator** (Java 21 / Spring Boot 3 `:8005`):
   - Управляет жизненным циклом задач репликации (`Job` / `JobRun`) в реляционной БД (H2 / PostgreSQL).
   - Предоставляет REST API и встроенный Web UI (Svelte 5 SPA) в единой дизайн-системе платформы.
   - Реализует централизованный потокобезопасный многоуровневый Token Bucket Throttler (выдача сетевых квот агентам).
   - Запускает встроенный планировщик периодических задач (Cron Scheduler).
   - Экспортирует метрики для Prometheus (`/actuator/prometheus`).
2. **Replicator Agent** (Java 21 / gRPC сервис `:50051`):
   - Полнодуплексный агент (`agent`), развертываемый в каждом дата-центре (например, DC1 и DC2).
   - Включает встроенный Receiver (gRPC streaming на HTTP/2, проверка SHA-256, атомарный commit в HDFS) и Sender (опрос задач `QUEUED`, запрос токенов у Оркестратора, потоковая передача).
   - Прямая работа с HDFS через нативный `HadoopFsManager` с поддержкой Kerberos UGI и doAs-имперсонации для Apache Ranger.

---

### 7.2 Топология дата-центров и кластеров

Топология описывает физическое размещение оборудования и привязку кластеров HDFS к конкретным дата-центрам (`dc_id`).

На демонстрационном стенде настроена топология из **2 ЦОД**:
- **ЦОД 1 (Москва / Primary)**: содержит **2 кластера HDFS** (`demo-cluster` — Prod DataLake и `analytics-cluster` — Secondary DataLake).
- **ЦОД 2 (Санкт-Петербург / Disaster Recovery)**: содержит **1 кластер HDFS** (`backup-cluster` — DR Mirror DataLake).

```yaml
topology:
  # 1. Дата-центры (Data Centers / DC)
  datacenters:
    - id: "dc1"
      name: "ЦОД 1 (Москва / DataCenter Primary)"
      location: "Moscow, Russia (DC-1)"
      description: "Основной производственный дата-центр компании"

    - id: "dc2"
      name: "ЦОД 2 (Санкт-Петербург / Disaster Recovery)"
      location: "Saint-Petersburg, Russia (DC-2)"
      description: "Катастрофоустойчивая резервная площадка (DR Site)"

  # 2. HDFS кластеры с явной привязкой к конкретному ЦОД
  clusters:
    # Кластеры в DC1:
    - id: "demo-cluster"
      name: "HDFS Primary (Prod DataLake)"
      dc_id: "dc1"
      webhdfs_url: "http://localhost:9870/webhdfs/v1"
      default_path: "/data/production"
      is_read_only: false
      description: "Основной HDFS кластер оперативных данных в DC1"

    - id: "analytics-cluster"
      name: "HDFS Analytics (Secondary DataLake)"
      dc_id: "dc1"
      webhdfs_url: "http://localhost:9872/webhdfs/v1"
      default_path: "/data/analytics"
      is_read_only: false
      description: "Аналитический HDFS кластер в DC1"

    # Кластеры в DC2:
    - id: "backup-cluster"
      name: "HDFS DR (Backup DataLake)"
      dc_id: "dc2"
      webhdfs_url: "http://dr-namenode:9870/webhdfs/v1"
      default_path: "/backup/mirror"
      is_read_only: false
      description: "Катастрофоустойчивое зеркало в DC2"
```

---

### 7.3 Многоуровневые лимиты полосы (DC-DC и HDFS-HDFS)

Для защиты корпоративных каналов WAN от перегрузки используется иерархический Token Bucket шейпер:
1. **Глобальный лимит (Global WAN Cap)**: верхняя граница совокупной скорости передачи по всей платформе.
2. **Лимиты между ЦОД (DC-DC WAN Limits)**: емкость магистрального физического канала связи между площадками.
3. **Лимиты между парами HDFS кластеров (HDFS-HDFS Limits)**: логическая квота полосы под конкретное направление синхронизации.

Значение `0.0` означает работу без ограничений («Без лимита / Unlimited»).

```yaml
topology:
  # 3. Лимиты полосы между ЦОД в МБ/с
  dc_limits:
    - source_dc: "dc1"
      target_dc: "dc2"
      limit_mb_per_sec: 100.0   # Магистраль Москва <-> СПб: 100 МБ/с
      description: "Магистральный канал между DC1 и DC2"

  # 4. Лимиты полосы между конкретными парами HDFS кластеров в МБ/с
  hdfs_limits:
    - source_cluster: "demo-cluster"
      target_cluster: "backup-cluster"
      limit_mb_per_sec: 60.0    # Репликация Prod (DC1) -> Backup (DC2)
      description: "Квота между Prod Lake (DC1) и Backup Lake (DC2)"

    - source_cluster: "analytics-cluster"
      target_cluster: "backup-cluster"
      limit_mb_per_sec: 40.0    # Репликация Analytics (DC1) -> Backup (DC2)
      description: "Квота между аналитическим кластером (DC1) и Backup Lake (DC2)"

    - source_cluster: "demo-cluster"
      target_cluster: "analytics-cluster"
      limit_mb_per_sec: 80.0    # ВнутриЦОДный обмен
      description: "Локальный обмен между Prod и Analytics внутри DC1"

  # 5. Глобальный лимит оркестратора (Global WAN Pool)
  global_limit_mb_per_sec: 120.0
```

Администраторы платформы (`ADMIN`) могут изменять любой из лимитов в рантайме через Web UI или эндпоинты API (`POST /api/v1/limits/global`, `POST /api/v1/limits/dc-dc`, `POST /api/v1/limits/hdfs-hdfs`). Изменения применяются для всех воркеров в течение 1 секунды без перезапуска сервисов.

---

### 7.4 Безопасность, RBAC и системная техучетка Kerberos

- **Ролевая модель (RBAC)**:
  - `ADMIN` (`admin_user`): сквозной просмотр и управление задачами всех пользователей, изменение лимитов полосы пропускания, отмена любых задач.
  - `WRITER` / `READER` (`de_user`, `analyst_user`): видят только собственные созданные задачи и общие системные задачи техучетки. Попытка редактирования лимитов возвращает `HTTP 403 Forbidden`.
- **Системная техучетка**:
  - Флаг `run_as_service_account: true` указывает запускать задачу от привилегированного системного принципала (`hdfs-replicator@REALM.LOCAL`). Это необходимо для периодических фоновых репликаций по расписанию.
- **Изоляция Kerberos (KRB5CCNAME)**:
  - Воркер выполняет каждую операцию с динамической генерацией пути к credential cache:
    `KRB5CCNAME=/tmp/krb5cc_repl_{uuid}`.
  - По завершении передачи временный кэш билетов безопасно удаляется, исключая утечку билетов между задачами.

---

### 7.5 Настройка Replicator Agent (Full-Duplex) и буфера Staging

Универсальный агент репликации `backend.replicator.agent` объединяет в одном процессе gRPC-сервер приема (`Receiver`) и фоновый воркер передачи данных (`Sender`), обеспечивая полноценную двунаправленную репликацию (`DC1 ⇄ DC2`).

Агент поддерживает **автоматическую динамическую регистрацию** на Оркестраторе с отправкой периодических Keepalive-сигналов (heartbeat). При старте агент объявляет свой gRPC-адрес (`advertised_grpc_address`) и привязывается к обслуживаемому кластеру. Это устраняет необходимость жестко прописывать сетевые адреса всех агентов в статическом `config.yaml`.

Агент настраивается следующими переменными окружения:

```bash
# Идентификатор агента и обслуживаемый HDFS-кластер
AGENT_ID=agent-dc1
AGENT_CLUSTER_ID=demo-cluster

# Режим работы: 'all' (полный дуплекс), 'sender' (только передача), 'receiver' (только прием)
AGENT_MODE=all

# URL Оркестратора для опроса очередей, получения сетевых токенов и регистрации
ORCHESTRATOR_URL=http://orchestrator:8005

# Адрес и порт входящего gRPC сервера (прием файлов от удаленных ЦОД)
RECEIVER_HOST=0.0.0.0
RECEIVER_PORT=50051

# Объявляемый сетевой адрес для вызовов от удаленных агентов (по умолчанию $HOSTNAME:$RECEIVER_PORT)
# AGENT_ADVERTISED_ADDRESS=agent-dc1:50051

# Включение динамической регистрации и интервал keepalive (секунды)
AGENT_ENABLE_REGISTRATION=true
AGENT_HEARTBEAT_INTERVAL_SEC=5.0

# Локальная буферная директория для временного приема чанков перед коммитом
REPLICATOR_STAGING_DIR=/tmp/staging

# Интервал опроса очереди задач в Оркестраторе (в секундах)
POLL_INTERVAL_SEC=2.0

# Настройки защищенного TLS / mTLS канала для gRPC
REPLICATOR_GRPC_TLS_ENABLED=true
REPLICATOR_GRPC_CERT_CHAIN_PATH=/etc/security/tls/agent-cert.pem
REPLICATOR_GRPC_PRIVATE_KEY_PATH=/etc/security/tls/agent-key.pem
REPLICATOR_GRPC_TRUST_CERT_COLLECTION_PATH=/etc/security/tls/ca-chain.pem
REPLICATOR_GRPC_CLIENT_AUTH=REQUIRE # NONE | OPTIONAL | REQUIRE (mTLS)
REPLICATOR_GRPC_INSECURE_SKIP_VERIFY=false # true для тестовых сред
ORCHESTRATOR_TLS_INSECURE_SKIP_VERIFY=false
```

**Жизненный цикл динамической регистрации и Keepalive**:
1. **Регистрация при старте (`POST /api/v1/agents/register`)**: Агент отправляет свой `agent_id`, `cluster_id`, вычисленный `advertised_grpc_address` и версию. Оркестратор фиксирует статус агента `ONLINE` и связывает его с кластером.
2. **Фоновый Keepalive (`POST /api/v1/agents/heartbeat`)**: Каждые 5 секунд агент отправляет сигнал жизнеспособности с количеством текущих активных задач (`active_transfers`).
3. **Обнаружение сбоев**: Если сигнал heartbeat не поступает более 15 секунд, статус агента автоматически переводится в `STALE`, и трафик перестает на него направляться.
4. **Graceful Shutdown (`POST /api/v1/agents/unregister`)**: При плановом завершении процесса агент уведомляет Оркестратор, переводя статус в `OFFLINE`.
5. **Балансировка нагрузки**: Если в одном кластере зарегистрировано несколько агентов, Оркестратор динамически распределяет входящие потоки репликации на наименее загруженный агент (`min(active_transfers)`).

**Безопасность динамической регистрации (Zero-Trust Security)**:
- **Аутентификация агентов (PSK / Token)**: Задается переменная `REPLICATOR_AGENT_SECRET` (мин. 32 симв.). Агент передает заголовок `X-Agent-Secret`. Неавторизованные запросы блокируются (`401 Unauthorized`). В production-режиме секрет обязателен.
- **Белый список кластеров (Cluster Whitelist)**: При `REPLICATOR_ENFORCE_CLUSTER_WHITELIST=true` (по умолчанию в prod) разрешена регистрация только для кластеров из `config.yaml`. Попытка объявить чужой кластер блокируется (`403 Forbidden`).
- **Защита от SSRF**: Оркестратор санитизирует `advertised_grpc_address`, блокируя диапазоны link-local (`169.254.0.0/16`, `fe80::/10`) и облачные метаданные (`169.254.169.254`, `metadata.google.internal`).

**Принцип атомарности коммита (Atomic Commit)**:
1. Передающий агент нарезает файлы на потоковые чанки размером 4 МБ с хешами блоков.
2. Принимающий агент сохраняет входящие чанки во временный файл в каталоге `staging_dir`.
3. После завершения стриминга вычисляется и сверяется совокупная контрольная сумма SHA-256 файла.
4. При совпадении хэша файл атомарно перемещается (`os.replace` или `pyarrow.fs HadoopFileSystem.move`) в целевой путь HDFS/FS.
5. При ошибке сети или расхождении хэша временный staging-файл автоматически удаляется.

---

### 7.6 Разрешение целевых адресов (Service Discovery, AGENT_TARGET_* и Fallback)

При отправке данных в удаленный дата-центр агент определяет сетевой gRPC-адрес целевого узла в соответствии со следующим приоритетом:

1. **Динамический реестр активных агентов Оркестратора (`AgentRegistry`) — рекомендуемый штатный режим**:
   - Оркестратор отслеживает зарегистрированные агенты в реальном времени.
   - Метод `get_grpc_address_for_cluster(cluster_id)` возвращает адрес активного онлайн-агента с наименьшим числом текущих передач (`active_transfers`).
   - Если кластер не был предварительно описан в `config.yaml`, он регистрируется динамически по данным первого подключившегося агента.
2. **Централизованный статический реестр топологии (`GET /api/v1/clusters`)**:
   - В `backend/replicator/config/config.yaml` у кластера может быть статически задан параметр `grpc_address` (например, `backup-cluster: agent-dc2:50051`).
   - Используется как надежный fallback, если динамическая регистрация отключена (`AGENT_ENABLE_REGISTRATION=false`) или агент временно не на связи.
3. **Локальный оверрайд `AGENT_TARGET_<CLUSTER_ID>`**:
   - Применяется в нестандартных сетевых окружениях (NAT, ingress/mesh-маршрутизация, различные порты в DMZ).
   - Например: `AGENT_TARGET_BACKUP_CLUSTER=agent-dc2:50051` переопределит адрес только для кластера `backup-cluster`.
4. **Резервный адрес `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS`**:
   - Используется как крайний fallback (по умолчанию `localhost:50051`), если целевой кластер не указан в задаче или отсутствует во всех реестрах Оркестратора.

---

### 7.7 Планировщик периодических задач (Cron Scheduler)

Оркестратор оснащен встроенным планировщиком задач `CronScheduler`:
- Поддерживает стандартный синтаксис cron (`*/5 * * * *`), а также пресеты (`@every_5m`, `@hourly`, `@daily`, `@weekly`).
- Фоновый процесс проверяет БД каждые 5 секунд.
- При наступлении времени `now >= next_run_at`:
  1. Создается или переводится в очередь задача репликации (`status = QUEUED`).
  2. Фиксируется `last_run_at = now`.
  3. Рассчитывается новое значение `next_run_at` на следующий интервал.
  4. Задача автоматически исполняется под системной техучеткой.

---

### 7.8 Локальное ограничение пропускной способности агента (`AGENT_MAX_BANDWIDTH_MB_S`)

При установке Replicator Agent непосредственно на узлы Hadoop (DataNode Co-location) или общие Edge Nodes для защиты от деградации сетевых карт и дисковых массивов настраивается локальный шейпинг:
- **Переменная `AGENT_MAX_BANDWIDTH_MB_S`**: задает предел полосы пропускания конкретного экземпляра агента в МБ/с (по умолчанию `0` — без ограничений).
- **Алгоритм**: асинхронный Token Bucket (`LocalBandwidthLimiter`) с поддержкой burst-буфера 0.5с.
- **Двусторонний троттлинг**:
  1. *Sender*: задержка при вызове `request_network_tokens()` перед отправкой каждого блока в сокет gRPC;
  2. *Receiver*: пауза при чтении чанков в `TransferFile()`, активирующая штатное TCP Window Backpressure по HTTP/2 и притормаживающая удаленного отправителя.
- **Видимость в UI**: лимит передается Оркестратору в теле регистрации и отображается в карточке узла.

---

### 7.9 Эталонный конфигурационный файл config.yaml

```yaml
# backend/replicator/config/config.yaml

topology:
  datacenters:
    - id: "dc1"
      name: "ЦОД 1 (Москва / DataCenter Primary)"
      location: "Moscow, Russia (DC-1)"
      description: "Основной производственный дата-центр компании"

    - id: "dc2"
      name: "ЦОД 2 (Санкт-Петербург / Disaster Recovery)"
      location: "Saint-Petersburg, Russia (DC-2)"
      description: "Катастрофоустойчивая резервная площадка (DR Site)"

  clusters:
    - id: "demo-cluster"
      name: "HDFS Primary (Prod DataLake)"
      dc_id: "dc1"
      webhdfs_url: "http://localhost:9870/webhdfs/v1"
      default_path: "/data/production"
      is_read_only: false
      description: "Основной HDFS кластер оперативных данных в DC1"

    - id: "analytics-cluster"
      name: "HDFS Analytics (Secondary DataLake)"
      dc_id: "dc1"
      webhdfs_url: "http://localhost:9872/webhdfs/v1"
      default_path: "/data/analytics"
      is_read_only: false
      description: "Аналитический HDFS кластер в DC1"

    - id: "backup-cluster"
      name: "HDFS DR (Backup DataLake)"
      dc_id: "dc2"
      webhdfs_url: "http://dr-namenode:9870/webhdfs/v1"
      default_path: "/backup/mirror"
      is_read_only: false
      description: "Катастрофоустойчивое зеркало в DC2"

  dc_limits:
    - source_dc: "dc1"
      target_dc: "dc2"
      limit_mb_per_sec: 100.0
      description: "Магистральный канал между DC1 и DC2"

  hdfs_limits:
    - source_cluster: "demo-cluster"
      target_cluster: "backup-cluster"
      limit_mb_per_sec: 60.0
      description: "Выделенная квота между Prod Lake (DC1) и Backup Lake (DC2)"

    - source_cluster: "analytics-cluster"
      target_cluster: "backup-cluster"
      limit_mb_per_sec: 40.0
      description: "Квота между аналитическим кластером (DC1) и Backup Lake (DC2)"

    - source_cluster: "demo-cluster"
      target_cluster: "analytics-cluster"
      limit_mb_per_sec: 80.0
      description: "Локальный обмен между Prod и Analytics внутри DC1"

  global_limit_mb_per_sec: 120.0
```

---

## 8. Переменные окружения

Все ключевые параметры могут быть заданы через переменные среды операционной системы или контейнера:

| Переменная | Описание | Значение по умолчанию |
|---|---|---|
| `CONFIG_PATH` / `YARN_CONFIG_PATH` / `HDFS_CONFIG_PATH` / `SQL_CONFIG_PATH` / `SPARK_CONFIG_PATH` | Путь к конфигурационному YAML файлу | `config/config.yaml` |
| `REPLICATOR_CONFIG_PATH` | Путь к конфигурации топологии репликатора | `backend/replicator/config/config.yaml` |
| `REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC` | Глобальный лимит скорости репликации (байт/сек) | `52428800` (50 МБ/с) |
| `REPLICATOR_SERVICE_PRINCIPAL` | Системный принципал Kerberos для репликации | `hdfs-replicator@REALM.LOCAL` |
| `REPLICATOR_DATABASE_URL` | Строка подключения к БД репликатора | `sqlite:///data/replicator.db` |
| `REPLICATOR_AGENT_SECRET` | Общий секретный токен аутентификации агентов репликации (`X-Agent-Secret`) | — (в prod обязателен) |
| `REPLICATOR_ENFORCE_CLUSTER_WHITELIST` | Принудительная блокировка регистрации неизвестных кластеров (Cluster Whitelist) | `false` (в prod автоматически `true`) |
| `AGENT_ID` | Уникальный идентификатор агента репликации | `agent-01` (или `$WORKER_ID`) |
| `AGENT_CLUSTER_ID` | Идентификатор HDFS-кластера, обслуживаемого агентом | `demo-cluster` |
| `AGENT_MODE` | Режим работы Replicator Agent (`all`, `sender`, `receiver`) | `all` |
| `AGENT_ENABLE_REGISTRATION` | Включение динамической авторегистрации агента на Оркестраторе | `true` |
| `AGENT_ADVERTISED_ADDRESS` | Сетевой gRPC адрес, анонсируемый агентом удаленным узлам | `$HOSTNAME:$RECEIVER_PORT` |
| `AGENT_HEARTBEAT_INTERVAL_SEC` | Период отправки keepalive heartbeat-сигнала Оркестратору (секунды) | `5.0` |
| `POLL_INTERVAL_SEC` | Интервал опроса очереди задач в Оркестраторе (секунды) | `3.0` |
| `AGENT_MAX_BANDWIDTH_MB_S` | Локальный Token Bucket лимит скорости агента (МБ/с) для защиты DataNode/сети | `0.0` (без ограничений) |
| `AGENT_TARGET_<CLUSTER_ID>` | Ручной оверрайд сетевого gRPC-адреса для конкретного целевого кластера (для NAT/DMZ) | Берется из топологии Оркестратора |
| `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS` | Резервный gRPC-адрес назначения (fallback при отсутствии кластера в топологии) | `localhost:50051` |
| `RECEIVER_HOST` / `RECEIVER_PORT` | Адрес и порт входящего gRPC-сервера Replicator Agent | `0.0.0.0:50051` |
| `REPLICATOR_STAGING_DIR` | Буферная директория Staging для атомарного коммита | `/tmp/staging` |
| `ORCHESTRATOR_URL` | Адрес Orchestrator для агентов/воркеров | `http://localhost:8005` |
| `SERVER_DEBUG` | Режим отладки (`true` / `false`) | `false` |
| `JWT_SECRET_KEY` / `HDFS_SECRET_KEY` | Секретный ключ подписи JWT (мин. 32 симв., обязателен в prod) | — (в dev автогенерируется) |
| `LDAP_SERVER_URI` / `HDFS_LDAP_URI` | URI LDAP сервера (`ldaps://...:636`) | — |
| `LDAP_BIND_PASSWORD` / `HDFS_LDAP_PASSWORD` | Пароль сервисной учетной записи LDAP | — |
| `DATABASE_URL` / `HDFS_DATABASE_URL` | Строка подключения к базе данных | `jdbc:sqlite:data/...` |
| `STORAGE_URL` / `REDIS_URL` | URL подключения к Redis (кэш токенов, Rate Limiter, Distributed Lock) | — |
| `LIVY_URL` | Адрес сервера Apache Livy для Spark Explorer | `http://localhost:8998` |
| `HIVE_METASTORE_URI` | Thrift URI каталога Hive Metastore | `thrift://localhost:9083` |
| `CORS_ORIGINS` | Доверенные адреса через запятую | `http://localhost:8000` |
| `KRB5_CONFIG` | Путь к файлу конфигурации Kerberos | `/etc/krb5.conf` |
| `KRB5_KTNAME` | Путь к Keytab-файлу сервиса | `/etc/security/keytabs/...` |
| `RESULTS_TTL_SECONDS` | TTL хранения кэша результатов SQL и Spark (`data/results/`) | `604800` (7 дней) |
| `RESULTS_CLEANUP_INTERVAL_SECONDS` | Интервал периодической фоновой очистки дискового кэша результатов | `3600` (1 час) |

---

## 9. Конфигурация в Kubernetes (Helm)

При развертывании через Umbrella Chart `helm/hadoop-explorer` конфигурация каждого компонента передается в секции `values.yaml`:

```yaml
global:
  environment: "production"
  ingressDomain: "hadoop.company.local"

yarn-explorer:
  enabled: true
  replicaCount: 2
  podDisruptionBudget:
    enabled: true
    maxUnavailable: 1
  config:
    clusters:
      - id: "prod-yarn"
        resource_manager_urls:
          - "http://rm1.prod.company.local:8088"
          - "http://rm2.prod.company.local:8088"

hdfs-explorer:
  enabled: true
  replicaCount: 2
  podDisruptionBudget:
    enabled: true
    maxUnavailable: 1
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
  podDisruptionBudget:
    enabled: true
    maxUnavailable: 1
  config:
    ai:
      enabled: true
      base_url: "http://ollama-service.ai.svc:11434/v1"
      model: "qwen2.5-coder:7b"

spark-explorer:
  enabled: true
  replicaCount: 2
  podDisruptionBudget:
    enabled: true
    maxUnavailable: 1
  config:
    clusters:
      - id: "prod-hadoop"
        livy_url: "http://livy.hadoop.svc:8998"

replicator:
  enabled: true
  orchestrator:
    replicaCount: 1
    port: 8005
    config:
      globalLimitBytesPerSec: 125829120  # 120 МБ/с
      databaseUrl: "sqlite:////app/data/replicator.db"
      servicePrincipal: "hdfs-replicator@REALM.LOCAL"
  agent:
    replicaCount: 2  # Горизонтальное масштабирование дуплексных агентов (Sender + Receiver)
    port: 50051
    clusterId: "demo-cluster"
    mode: "all"
    stagingDir: "/tmp/staging"
    pollIntervalSec: 2.0
```

### 9.1 Рекомендации по High-Availability в продакшне
1. **База данных**: При `replicaCount > 1` настройте внешний PostgreSQL (`config.database.url: jdbc:postgresql://...`). Локальная SQLite поддерживает только 1 реплику. Пошаговое руководство см. в [docs/production-database.md](production-database.md).
2. **PodDisruptionBudget (PDB)**: Шаблоны чартов автоматически активируют `PodDisruptionBudget` при запуске более 1 реплики (`replicaCount > 1`), предотвращая одновременный drain всех реплик узлами k8s.
3. **NetworkPolicy**: Включена по умолчанию (`networkPolicy.enabled: true`) для изоляции сетевого взаимодействия и разрешения ingress-трафика только от Ingress-контроллера.
4. **Мониторинг Prometheus**: Настройте сбор метрик по эндпоинту `/metrics` (порт 8000 / 8005) для мониторинга HTTP Golden Signals, состояний `CircuitBreaker`, повторов и ошибок.

---

## 10. Мониторинг Prometheus и дашборды Grafana

Все сервисы платформы предоставляют единый интерфейс метрик в формате OpenMetrics / Prometheus.

### 10.1 Экспортируемые метрики

| Метрика | Тип | Лейблы | Описание |
|---|---|---|---|
| `http_requests_total` | Counter | `app`, `method`, `path`, `status` | Количество входящих HTTP-запросов (с нормализацией путей) |
| `http_request_duration_seconds` | Histogram | `app`, `method`, `path`, `le` | Задержка обработки запросов (бакеты от 5ms до 10s) |
| `http_requests_in_progress` | Gauge | `app` | Число одновременных запросов в обработке (concurrency) |
| `hadoop_circuit_breaker_state` | Gauge | `name` | Текущий статус Circuit Breaker: `0=CLOSED`, `1=HALF_OPEN`, `2=OPEN` |
| `hadoop_circuit_breaker_calls_total` | Counter | `name`, `status` | Вызовы Circuit Breaker со статусами `success`, `failed`, `rejected` |
| `hadoop_retry_attempts_total` | Counter | `app`, `operation`, `status` | Срабатывания механизма повторов (`retry`, `exhausted`, `success`) |
| `hadoop_auth_attempts_total` | Counter | `app`, `provider`, `status` | Попытки аутентификации (`ldap`, `kerberos`, `mock`) и результат |
| `hadoop_rate_limit_blocks_total` | Counter | `app` | Блокировки по Rate Limiter (HTTP 429) |
| `hadoop_exceptions_total` | Counter | `app`, `exception_type` | Непредвиденные исключения 500 с `incident_id` (CWE-209) |
| `replicator_bytes_transferred_total` | Counter | `status` | Объем переданных байтов репликации (`IN_PROGRESS`, `COMPLETED`) |
| `replicator_jobs_total` | Counter | `status`, `mode` | Количество задач репликации по статусам и типу запуска |
| `replicator_active_workers` | Gauge | — | Число активных фоновых воркеров передачи данных |
| `replicator_throttling_delay_seconds_total` | Counter | — | Суммарная задержка шейпинга сетевого канала (Token Bucket) |

### 10.2 Настройка сбора метрик в Prometheus (Scrape Config)

```yaml
scrape_configs:
  - job_name: 'hadoop-explorer'
    scrape_interval: 15s
    metrics_path: '/metrics'
    static_configs:
      - targets:
          - 'yarn-explorer:8000'
          - 'hdfs-explorer:8000'
          - 'sql-explorer:8000'
          - 'spark-explorer:8000'
          - 'replicator-orchestrator:8005'
```

Пример Kubernetes `ServiceMonitor` (Prometheus Operator):
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: hadoop-explorer-monitor
  namespace: hadoop-explorer
spec:
  selector:
    matchLabels:
      app.kubernetes.io/part-of: hadoop-explorer
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

### 10.3 Готовые дашборды Grafana

В директории [`monitoring/grafana/dashboards/`](../monitoring/grafana/dashboards/) доступны 5 специализированных дашбордов для промышленного мониторинга:

1. **Сводный обзор платформы**: [`hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) — Golden Signals (RPS, Latency p95/p99, 5xx rate), Circuit Breaker, Retries, безопасность (Auth, Rate Limiting) и общая активность репликации.
2. **Межкластерная репликация**: [`hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json) — скорость репликации WAN, задержки иерархического шейпера Token Bucket Throttler, активные gRPC воркеры, статусы задач (RUNNING, SCHEDULED, QUEUED, COMPLETED) и гистограмма размеров файлов.
3. **Файловые операции HDFS**: [`hdfs_explorer_operations.json`](../monitoring/grafana/dashboards/hdfs_explorer_operations.json) — файловые операции WebHDFS, скорость загрузки/скачивания (Upload vs Download), отказы операций.
4. **Очереди YARN**: [`yarn_explorer_queues.json`](../monitoring/grafana/dashboards/yarn_explorer_queues.json) — утилизация очередей Capacity Scheduler, запросы на изменение квот, состояние HA ResourceManager.
5. **Аналитика Spark & SQL**: [`spark_sql_explorer_analytics.json`](../monitoring/grafana/dashboards/spark_sql_explorer_analytics.json) — активные сессии Apache Livy, интерактивные инструкции Spark, запросы Trino и Hive Metastore.

#### Импорт дашбордов в Grafana
1. В веб-интерфейсе Grafana перейдите в **Dashboards** $\rightarrow$ **New** $\rightarrow$ **Import**.
2. Загрузите желаемый JSON-файл из каталога `monitoring/grafana/dashboards/`.
3. Выберите ваш источник данных Prometheus и нажмите **Import**.

Для автоматического развертывания через Grafana Provisioning скопируйте файл [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml) в `/etc/grafana/provisioning/dashboards/`.

### 10.4 Примеры полезных PromQL запросов

- **Текущий RPS всей платформы**:
  ```promql
  sum(rate(http_requests_total[1m]))
  ```
- **Процент ошибок 5xx**:
  ```promql
  (sum(rate(http_requests_total{status=~"5.."}[1m])) / sum(rate(http_requests_total[1m]))) * 100
  ```
- **Задержка p95 по сервисам**:
  ```promql
  histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[1m])) by (le, app))
  ```
- **Алерт на срабатывание Circuit Breaker (кластер недоступен)**:
  ```promql
  hadoop_circuit_breaker_state == 2
  ```

---

## 11. Руководства администратора по развертыванию (DevOps Hub)

Пошаговые runbook/руководства по развертыванию каждого компонента платформы в режимах **Standalone (systemd)**, **Docker / Docker Compose** и **Kubernetes (Helm)**:

- 🚀 [Единый DevOps Hub платформы (admin-guide.md)](admin-guide.md)
- ⚙️ [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- 📁 [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- 🔍 [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- ⚡ [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- 🔄 [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)


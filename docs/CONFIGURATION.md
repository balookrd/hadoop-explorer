# Руководство по конфигурации Hadoop Explorer Platform

Документ для администраторов платформы Hadoop Explorer: параметры конфигурации, форматы файлов и переменные окружения компонентов.
- Общий стартер безопасности (`backend/common-security-starter` — Java 21 / Spring Boot 3, SPNEGO, LDAP/Active Directory, JWT, L1/L2 сессии, Circuit Breaker, Rate Limiter, CSRF Guard).
- YARN Explorer (`backend/yarn` — Java 21 LTS / Spring Boot 3, YARN RM HA, Capacity Scheduler, партиции, Change Requests, Four-Eyes Principle, генерация XML, Ansible AWX).
- HDFS Explorer (`backend/hdfs` — Java 21 LTS / Spring Boot 3, подключение к HDFS Client, HA NameNode, Kerberos, doAs имперсонация, квоты, превью Parquet/ORC, Circuit Breaker).
- SQL Explorer (`backend/sql` — Java 21 LTS / Spring Boot 3, Trino DB API, Apache Hive / HiveServer2, AI-ассистент, история и кэширование).
- Spark Explorer (`backend/spark` — Java 21 LTS / Spring Boot 3, Apache Livy, PySpark, Scala, DAG Pipelines, Metastore, Circuit Breaker).
- Hadoop gRPC Replicator (`backend/replicator` — Java 21 / Spring Boot 3 Orchestrator, нативный Java gRPC Worker Daemon, топология ЦОД, Hierarchical Token Bucket, Kerberos, Snapshot Diff, Cron Scheduler).
- Развертывание в Kubernetes (Helm).

---

## Содержание
1. [Общие принципы и приоритеты конфигурации](#1-общие-принципы-и-приоритеты-конфигурации)
2. [Общая конфигурация (backend/common)](#2-общая-конфигурация-backendcommon)
   - [Сервер и безопасность (CORS, CSRF, Cookies, JWT)](#21-сервер-и-безопасность-cors-csrf-cookies-jwt)
   - [Аутентификация: LDAPS / Active Directory](#22-аутентификация-ldaps--active-directory)
   - [Аутентификация: Kerberos SPNEGO SSO](#23-аутентификация-kerberos-spnego-sso)
   - [Конфигурация ролевой модели и прав доступа (RBAC)](#24-конфигурация-ролевой-модели-и-прав-доступа-rbac)
   - [Хранилище сессий, токенов и Rate Limiting (SessionStore & Tri-Storage)](#25-хранилище-сессий-токенов-и-rate-limiting-sessionstore--tri-storage)
   - [Отказоустойчивость: Circuit Breaker, Distributed Lock и Graceful Shutdown](#26-отказоустойчивость-circuit-breaker-retry-global-exception-handlers-distributed-lock-и-graceful-shutdown)
   - [Сквозное шифрование TLS/HTTPS и mTLS для REST и gRPC каналов](#27-сквозное-шифрование-tlshttps-и-mtls-для-rest-и-grpc-каналов)
3. [Настройка YARN Explorer](#3-настройка-yarn-explorer)
   - [YARN-кластеры и партиции](#31-yarn-кластеры-и-партиции)
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
   - [Архитектура и компоненты (Orchestrator, Agent)](#71-архитектура-и-компоненты-orchestrator-agent)
   - [Безопасные значения по умолчанию и продуктивный профиль](#711-безопасные-значения-по-умолчанию-и-продуктивный-профиль)
   - [Трассировка и health-пробы gRPC](#712-трассировка-и-health-пробы-grpc)
   - [Топология дата-центров и кластеров](#72-топология-дата-центров-и-кластеров)
   - [Многоуровневые лимиты полосы (DC-DC и HDFS-HDFS)](#73-многоуровневые-лимиты-полосы-dc-dc-и-hdfs-hdfs)
   - [Безопасность, RBAC и системная техучетка Kerberos](#74-безопасность-rbac-и-системная-техучетка-kerberos)
   - [Настройка Replicator Agent (Full-Duplex) и буфера Staging](#75-настройка-replicator-agent-full-duplex-и-буфера-staging)
   - [Разрешение целевых адресов (Service Discovery, AGENT_TARGET_* и Fallback)](#76-разрешение-целевых-адресов-service-discovery-agent_target_-и-fallback)
   - [Планировщик периодических задач (Cron Scheduler)](#77-планировщик-периодических-задач-cron-scheduler)
   - [Локальное ограничение пропускной способности агента (`AGENT_MAX_BANDWIDTH_MB_S`)](#78-локальное-ограничение-пропускной-способности-агента-agent_max_bandwidth_mb_s-и-модель-согласования-wan)
   - [Эталонный фрагмент конфигурации топологии (application.yml)](#79-эталонный-фрагмент-конфигурации-топологии-applicationyml)
8. [Переменные окружения](#8-переменные-окружения)
9. [Конфигурация в Kubernetes (Helm)](#9-конфигурация-в-kubernetes-helm)
10. [Мониторинг Prometheus и дашборды Grafana](#10-мониторинг-prometheus-и-дашборды-grafana)
11. [Руководства администратора по развертыванию (DevOps Hub)](#11-руководства-администратора-по-развертыванию-devops-hub)

---

## 1. Общие принципы и приоритеты конфигурации

Каждый бэкенд-сервис загружает конфигурацию по следующей цепочке приоритетов:
1. Переменные окружения (наивысший приоритет — переопределяют значения из YAML для секретов и путей).
2. Файл конфигурации YAML, заданный через переменную `CONFIG_PATH` или `YARN_CONFIG_PATH` / `HDFS_CONFIG_PATH` / `SQL_CONFIG_PATH` / `SPARK_CONFIG_PATH`.
3. Локальный файл по умолчанию: `config/config.yaml` внутри каталога соответствующего сервиса.

> [!IMPORTANT]
> Значения по умолчанию и проверки на старте (все Java-сервисы).
> Упакованный в JAR `application.yml` каждого сервиса рассчитан на продакшен: `hadoop.security.debug: false`,
> `auth.mode: ${HADOOP_AUTH_MODE:ldap}`, JWT-секрет только из `JWT_SECRET_KEY`. Стартер безопасности
> не даст сервису запуститься, если:
> - `auth.mode` равен `mock` без `hadoop.security.debug: true`. В mock-режиме демо-учетки входят без проверки
>   в каталоге, а для `admin`/`writer` подходит любой пароль;
> - `hadoop.security.jwt.secret-key` пуст, короче 32 символов или совпадает со встроенным значением из исходников;
> - `auth.mode: ldap` задан при `ldap.enabled: false`;
> - `auth.mode` не входит в `mock | ldap | kerberos | hybrid`.
>
> Локальная разработка и демо-стенды включают профиль `mock` явно (`SPRING_PROFILES_ACTIVE=mock`,
> файл `application-mock.yml` в ресурсах сервиса) либо задают `auth.mode`/`debug` в своем конфиге.
> Ключи из старых руководств (`auth-mode`, `ldap.server-uri`, `bind-dn`, `bind-password`, `user-base-dn`,
> `user-filter`, `kerberos.keytab-path`, `kerberos.principal`) принимаются как синонимы.

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

#### HTTP-заголовки безопасности и CSP

Бэкенд-сервисы добавляют в ответы HTTP-заголовки безопасности через middleware `apply_security_headers` [проверить: в Java-коде сервисов нет ни этого имени, ни `CSP_DEFAULT_DIRECTIVES`]:
- `Content-Security-Policy`: директивы против XSS (`CSP_DEFAULT_DIRECTIVES` для HDFS/YARN и `CSP_CODE_EDITOR_DIRECTIVES` с разрешением Web Workers для Monaco Editor в Spark/SQL).
- `X-Frame-Options: DENY`: запрет встраивания в iframe (clickjacking).
- `X-Content-Type-Options: nosniff`: браузер не подменяет MIME-тип ответа.
- `Referrer-Policy: strict-origin-when-cross-origin`: ограничивает содержимое заголовка `Referer`.
- `Strict-Transport-Security` (HSTS): включается при `secure_cookies: true` или `debug: false`.

### 2.2 Аутентификация: LDAPS / Active Directory

Модуль `CommonLdapAuthService` [проверить: класса с таким именем в Java-коде нет] подключается к каталогам OpenLDAP, FreeIPA и Microsoft Active Directory. Спецсимволы в имени пользователя экранируются перед подстановкой в фильтр (защита от LDAP injection).

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

Значения `ldaps_only` и `kerberos_only` из комментария к `auth.mode` не проходят проверку на старте: `SecurityConfigurationValidator` принимает только `mock`, `ldap`, `kerberos` и `hybrid` (см. раздел 1).

### 2.3 Аутентификация: Kerberos SPNEGO SSO

Пользователь входит в веб-интерфейс по Kerberos-тикету из своей системной сессии, без ввода пароля: браузер передает заголовок `Authorization: Negotiate <ticket>`.

```yaml
kerberos_sso:                   # или auth.kerberos
  enabled: true
  service_principal: "HTTP/hadoop-explorer.company.local@COMPANY.LOCAL"
  keytab_path: "/etc/security/keytabs/spnego.keytab"
```

### 2.4 Конфигурация ролевой модели и прав доступа (RBAC)

Все сервисы платформы вычисляют роль пользователя по одной модели (`resolve_system_role` [проверить: функции с таким именем в Java-коде нет]):

```yaml
auth:
  # Списки пользователей и групп для назначения ролей
  admin_users: ["admin", "superadmin"]
  admin_groups: ["hadoop-admins", "platform-admins", "domain admins"]
  writer_groups: ["data-engineers", "yarn-operators", "etl-developers"]
```

Роли:
- Пользователи из `admin_users` или из любой группы `admin_groups` получают роль `ADMIN` (бейдж в UI — `ADM`).
- Пользователи из групп `writer_groups` получают роль `WRITER` (бейдж `RW`).
- Остальные аутентифицированные пользователи получают роль `READER` (бейдж `RO`).
- Роль вычисляется на сервере, включается в полезную нагрузку токена и сессии, и возвращается через `/api/v1/auth/me`.

### 2.5 Хранилище сессий, токенов и Rate Limiting (SessionStore & Tri-Storage)

Слой хранения (`SessionStore` и `BaseStorageService` [проверить: `BaseStorageService` и `L1RevokedTokenCache` в Java-коде не найдены]) хранит:
- Сессии пользователей (`active_sessions`). При входе сессия записывается в базу с абсолютным Unix timestamp `expires_at` (полный срок жизни JWT, по умолчанию 480 минут). После перезапуска контейнеров или сервисов сессия читается из базы, и пользователю не нужно входить заново.
- Срок жизни токена. `SessionStore` определяет, задан TTL абсолютным timestamp или числом секунд, и приводит его к одному виду.
- Черный список отозванных токенов (`revoked_tokens`). Два уровня: L1 — LRU-кэш в памяти (`L1RevokedTokenCache`) с истечением записей, L2 — база данных или Redis. Проверка отозванного токена после logout идет по L1 без обращения к базе.
- Счетчики rate limiting. Алгоритм скользящего окна (sliding window), счетчики в базе данных или Redis.

Поддерживаемые хранилища:
- SQLite в режиме WAL (`sqlite:///./data/hadoop_explorer.db` или `/app/data/*.db`) — хранилище по умолчанию.
- PostgreSQL (`postgresql://user:password@pg-host:5432/hadoop_explorer`) — для запуска нескольких реплик сервиса в Kubernetes.
- Redis (`redis://redis-host:6379/0`) — необязательный L2-кэш и распределенные блокировки (`DistributedLock`).

```yaml
database:
  # SQLite (по умолчанию в контейнере):
  url: "jdbc:sqlite:/app/data/service_sessions.db"
  # Либо PostgreSQL (для HA в production):
  # url: "jdbc:postgresql://user:password@pg-host:5432/hadoop_explorer"
  redis_url: "redis://redis-host:6379/0" # Опционально
```

### 2.6 Отказоустойчивость: Circuit Breaker, Retry, Global Exception Handlers, Distributed Lock и Graceful Shutdown

| Механизм | Поведение |
|---|---|
| Circuit Breaker | Считает сетевые ошибки вызовов к кластерам Hadoop/Spark/YARN. После 5 ошибок или таймаутов подряд узел переходит в `OPEN` на 30 секунд: вызовы к нему сразу завершаются ошибкой и не занимают потоки пула. Затем узел переходит в `HALF_OPEN`, и следующий запрос идет как пробный. Клиентские ошибки 4xx не учитываются. Метрики — через Actuator `/actuator/prometheus` (или `/metrics`). |
| Retry с экспоненциальным backoff | Повторяет вызов при ошибках соединения и сокета (`java.net.http.HttpConnectTimeoutException`, `java.io.IOException`), к задержке добавляется случайный джиттер. |
| Глобальный обработчик исключений (`@ControllerAdvice`) | Перехватывает необработанные исключения, возвращает 500 со стандартным телом и `incidentId`, stack trace наружу не отдает (CWE-209). |
| Distributed Lock | Взаимное исключение для критических секций через Redis (`SET NX PX` + Lua); без Redis — блокировки в памяти процесса. |
| Graceful shutdown | По SIGTERM/SIGINT останавливает `ThreadPoolTaskExecutor`, отменяет фоновые задачи и закрывает соединения с базами данных и сетевыми клиентами. |

### 2.7 Сквозное шифрование TLS/HTTPS и mTLS для REST и gRPC каналов

TLS для встроенного веб-сервера и исходящих REST-клиентов сервисов `yarn`, `hdfs`, `sql`, `spark` и `replicator/orchestrator` настраивается в одном месте — в секции `hadoop.security.tls` стартера `common-security-starter`:

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

Те же параметры через переменные окружения:
- `HADOOP_SECURITY_TLS_ENABLED=true`
- `HADOOP_SECURITY_TLS_KEY_STORE_PATH=/etc/security/tls/keystore.p12`
- `HADOOP_SECURITY_TLS_KEY_STORE_PASSWORD=secret`
- `HADOOP_SECURITY_TLS_TRUST_STORE_PATH=/etc/security/tls/truststore.p12`
- `HADOOP_SECURITY_TLS_CLIENT_AUTH=REQUIRE` (для включения двустороннего mTLS)
- `HADOOP_SECURITY_TLS_INSECURE_SKIP_VERIFY=true` (только для тестирования)

---


## 3. Настройка YARN Explorer

Файл конфигурации: `backend/yarn/src/main/resources/application.yml` (или внешний файл `application.yml` / переменные окружения).

### 3.1 YARN-кластеры и партиции

```yaml
yarn:
  clusters:
    - id: "prod-yarn"
      name: "Production Hadoop Cluster"
      description: "Основной YARN-кластер (120 узлов)"
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

В YARN Explorer три роли: `ADMIN`, `WRITER`, `READER`. При `enforce-four-eyes: true` действует принцип четырех глаз: автор заявки не может сам ее согласовать.

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

YARN Explorer доставляет сгенерированную XML-конфигурацию на кластер и применяет ее без перезапуска, запуская Job Template в Ansible AWX / Red Hat Ansible Automation Platform:

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
| Переменная (Spring Boot Relaxed Binding) | Пример значения | Описание |
|---|---|---|
| `YARN_AWX_ENABLED` (или `AWX_ENABLED`) | `true` | Активация модуля автоматизации AWX |
| `YARN_AWX_BASE_URL` (или `AWX_BASE_URL`) | `https://awx.company.local` | Базовый URL сервера AWX |
| `YARN_AWX_TOKEN` (или `AWX_TOKEN`) | `BearerSecretToken...` | Токен доступа к REST API AWX |
| `YARN_AWX_VERIFY_SSL` (или `AWX_VERIFY_SSL`) | `true` | Проверка TLS сертификата сервера AWX |
| `YARN_AWX_DEFAULT_JOB_TEMPLATE_ID` | `101` | Числовой идентификатор Job Template |
| `YARN_AWX_TIMEOUT_SECONDS` | `180` | Таймаут ожидания завершения задачи (сек) |

Развертывание роли Ansible, настройка шаблона AWX и откат: [docs/awx-yarn-deployment.md](awx-yarn-deployment.md).
Применение изменений из интерфейса Change Requests: [docs/yarn-user-guide.md](yarn-user-guide.md).

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
SQL Explorer и Spark Explorer сохраняют результаты запросов в сжатом виде (`{id}.json.gz`) в общий каталог `data/results/`.

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `results_ttl_seconds` / `RESULTS_TTL_SECONDS` | `604800` (7 дней) | Срок хранения файла результата |
| `RESULTS_CLEANUP_INTERVAL_SECONDS` | `3600` (1 час) | Интервал фоновой очистки каталога от файлов старше TTL. Очистка также запускается при старте сервиса |

#### Персистентность рабочих пространств (User Workspace)
SQL Explorer сохраняет в БД открытые вкладки редактора, введенный SQL-код, выбранные кластеры и каталоги и последние результаты (модель `SqlUserWorkspace`, эндпоинт `/api/v1/workspace`). Рабочее пространство у каждого аутентифицированного пользователя свое: после смены пользователя в браузере загружается его контекст, чужая история не показывается.

### 5.3 ИИ-ассистент (On-Premise LLM / Ollama / vLLM)

ИИ-ассистент SQL Explorer генерирует, оптимизирует и исправляет SQL-запросы через OpenAI-совместимый API (Ollama, vLLM, LiteLLM):

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

Spark Explorer запускает интерактивные сессии и пакетные задачи через Apache Livy:

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

Пользователь выбирает версию Spark и окружение Python (Conda/venv, упакованное в архив на HDFS) из списка в конфигурации:

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
Кроме окружений из `config.yaml`, пользователь может указать собственный архив virtualenv/conda в окне параметров сессии:

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `custom_python_archive` | — | HDFS-путь к архиву окружения (`hdfs:///.../*.tar.gz#environment`). Если фрагмент `#алиас` не указан, добавляется `#environment` |
| `custom_python_path` | `./environment/bin/python` | Относительный путь к интерпретатору внутри распакованного архива |

Архив передается в параметр `archives` сессии Apache Livy, путь к интерпретатору — в `spark.pyspark.python` и `spark.pyspark.driver.python`.


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

Spark Explorer сохраняет в базу данных состояние сессий, активные вкладки, буферы кода (`pyspark`, `scalaspark`, `sql`) и буферы результатов (`SparkUserWorkspace`, `/api/v1/workspace`):
- У каждого языка (PySpark, Scala, SQL) свой буфер результатов: переключение языка не затирает результаты другого.
- При входе другого пользователя загружается его рабочее пространство.

---

## 7. Настройка Hadoop gRPC Replicator (DC-DC WAN Sync & Throttling)

Сервис межкластерной репликации `backend/replicator` (Java 21 LTS, Spring Boot 3, gRPC) копирует данные HDFS между ЦОД по WAN с ограничением полосы. Конфигурация задается в `application.yml` или переменными окружения.

### 7.1 Архитектура и компоненты (Orchestrator, Agent)

Сервис состоит из двух компонентов:
1. Replicator Orchestrator (Java 21 / Spring Boot 3, порт `8005`):
   - хранит задачи репликации (`Job` / `JobRun`) и их состояние в реляционной БД (H2 / PostgreSQL);
   - отдает REST API и Web UI (Svelte 5 SPA);
   - выдает агентам сетевые квоты через многоуровневый Token Bucket;
   - запускает периодические задачи по расписанию (Cron Scheduler);
   - экспортирует метрики Prometheus (`/actuator/prometheus`).
2. Replicator Agent (Java 21, gRPC, порт `50051`):
   - разворачивается в каждом ЦОД (например, DC1 и DC2) и работает в обе стороны;
   - Receiver принимает данные по gRPC streaming (HTTP/2), сверяет SHA-256 и атомарно фиксирует файл в HDFS; Sender опрашивает задачи в статусе `QUEUED`, запрашивает токены у оркестратора и передает данные потоком;
   - работает с HDFS через `HadoopFsManager`: Kerberos UGI и doAs-имперсонация, чтобы политики Apache Ranger применялись к пользователю задачи.

---

### 7.1.1 Безопасные значения по умолчанию и продуктивный профиль

Конфигурация оркестратора по умолчанию (`application.yml`) — H2 в памяти и боевая аутентификация
(`auth.mode: ldap`, JWT-секрет из `JWT_SECRET_KEY`): без них оркестратор не стартует.
Тестовые пользователи и `auth.mode: mock` вынесены в профиль `mock` (`SPRING_PROFILES_ACTIVE=mock`),
который включают демо-стенды `demo/replicator`. Схема базы во всех профилях
создается миграциями Flyway, а `ddl-auto` стоит в `validate`. Для продуктива
предусмотрен отдельный профиль `application-prod.yml`, активируемый через
`SPRING_PROFILES_ACTIVE=prod`: PostgreSQL, `auth.mode: kerberos`,
`cookie.secure: true`, `health.show-details: when-authorized` и обязательный TLS для gRPC
Control Plane. Все секреты в этом профиле читаются только из окружения
(`REPLICATOR_AGENT_SECRET`, `JWT_SECRET_KEY`, `POSTGRES_USER`, `POSTGRES_PASSWORD`).

Защитные параметры и их значения по умолчанию:

| Параметр | Значение по умолчанию | Назначение |
| :--- | :--- | :--- |
| `hadoop.replicator.agent-secret` | не задан (только из `REPLICATOR_AGENT_SECRET`) | Зашитый в код секрет был единым публично известным ключом для всех установок |
| `hadoop.replicator.enforce-agent-secret` | `true` | Запуск прерывается, если секрет не задан, короче 16 символов или равен прежнему значению из репозитория |
| `hadoop.replicator.enforce-cluster-whitelist` | `true` | Агент регистрируется только в кластере из топологии |
| `REPLICATOR_ENFORCE_TASK_ATTESTATION` (агент) | `true` | Приемник требует HMAC-токен аттестации задачи, выдаваемый оркестратором при заборе задач |

> [!IMPORTANT]
> gRPC-серверы (Control Plane оркестратора, HDFS Receiver и приемник Kafka-батчей агента)
> работают в режиме fail-closed: интерцептор аутентификации устанавливается всегда, и при
> незаданном `x-agent-secret` все входящие вызовы отклоняются со статусом `UNAUTHENTICATED`.
> Поэтому пустой секрет не «открывает» стенд, а полностью останавливает репликацию.
> Демо-стенд (`demo/replicator`) задает общий секрет через переменную `DEMO_AGENT_SECRET`.

> [!WARNING]
> При включенном TLS (`grpc.tls-enabled: true`) недоступный или некорректный файл доверенных
> сертификатов прерывает запуск клиента control plane. Молчаливого отката в plaintext нет:
> канал несет заголовок `x-agent-secret`.

Агенты работают только через gRPC Control Plane: отчеты о прогрессе (`ReportHmsProgress`),
выдача HMS-задач (`GetPendingHmsJobs`) и продление аренды стримера (`RenewStreamingLease`)
проходят через интерцептор `x-agent-secret`. Прежние REST-дублеры этих вызовов
(`POST /api/v1/hms/jobs/{id}/progress`, `GET /api/v1/hms/jobs/{id}/pending`,
`POST /api/v1/streaming/lease/renew`) удалены вслед за `POST /api/v1/agents/register`
и отвечают `404`.

REST API остается интерфейсом Web UI и операторов. Детали HMS-задач (`/{id}`, `/{id}/events`,
`/{id}/subtasks`, `/{id}/dlq`) и список задач видны администратору и владельцу задачи:
непривилегированный пользователь не может перечислить структуру чужой репликации по ее
идентификатору.

Контейнеры оркестратора и агента работают от непривилегированного пользователя `appuser`
(UID 10001).

---

### 7.1.2 Трассировка и health-пробы gRPC

Операция репликации проходит через несколько процессов: агент забирает задачу у оркестратора,
агент-отправитель передает данные агенту-приемнику, схемы Hive и батчи Kafka идут своими
вызовами. Чтобы участки собирались в одну трассу, все вызовы gRPC проходят через перехватчики
трассировки: клиент кладет в метаданные W3C `traceparent`, сервер поднимает его и продолжает тот
же trace, а `trace_id` и `span_id` выставляются в MDC — по логам разных процессов собирается одна
операция.

Трассировка включена по умолчанию, но экспорт спанов в коллектор включается отдельно: пока
адрес не задан, экспортер не создается и спаны никуда не уходят (корреляция в логах и
`traceparent` в вызовах при этом работают).

| Переменная | Компонент | Назначение |
| :--- | :--- | :--- |
| `MANAGEMENT_OTLP_TRACING_ENDPOINT` | Оркестратор | Адрес OTLP-коллектора, например `http://otel-collector:4318/v1/traces`. Не задан — экспорта нет |
| `TRACING_SAMPLING_PROBABILITY` | Оркестратор | Доля трассируемых запросов. По умолчанию `1.0`, в профиле `prod` — `0.1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Агент, Kafka-демон | Адрес коллектора (суффикс `/v1/traces` добавляется сам). Не задан — трассировка выключена |
| `OTEL_TRACES_SAMPLER_ARG` | Агент, Kafka-демон | Доля трассируемых вызовов, по умолчанию `0.05`: на пути передачи данных вызовов на порядки больше, чем в control plane |

Серверы gRPC — Control Plane оркестратора, приемник агента и приемник Kafka-батчей — отвечают по
стандартному протоколу `grpc.health.v1.Health`. Статус выставляется и в целом по серверу, и
отдельно по подсистемам (`replicator.DataTransferService`,
`replicator.kafka.KafkaDataTransferService`), а при остановке переводится в терминальное
состояние до закрытия порта, чтобы балансировщик и отправители успели увести трафик.

Health-проба освобождена от аутентификации по `x-agent-secret`: пробы балансировщика, kubelet и
docker секрета агента не знают, а наблюдают они только `SERVING`/`NOT_SERVING`. Остальные вызовы
остаются закрытыми — исключение действует строго на методы `grpc.health.v1.Health/*`.

Удержание истории пофайловых задач настраивается отдельно: таблица подзадач пополняется записью
на каждый файл, и без очистки она растет без границ.

| Параметр | По умолчанию | Назначение |
| :--- | :--- | :--- |
| `hadoop.replicator.task-retention-days` | `7` | Возраст, после которого завершенная задача удаляется. `0` выключает очистку |
| `hadoop.replicator.task-retention-batch-size` | `500` | Размер пакета удаления: одна транзакция на пакет, а не на все окно |
| `hadoop.replicator.task-retention-max-batches-per-run` | `20` | Сколько пакетов удаляется за проход (по умолчанию до 10 000 записей) |
| `hadoop.replicator.task-cleanup-interval-ms` | `3600000` | Интервал запуска очистки |

Удаляются только терминальные статусы (`COMPLETED`, `FAILED`, `SKIPPED`): задача в `RUNNING`
может прямо сейчас передаваться агентом, а зависшую подберет failover. В active-active очистку
выполняет один узел под распределенной блокировкой `task_retention_lock`.

Проба контейнера агента выполняется тем же jar-ом, что и сам агент, поэтому `grpc_health_probe` в
образ ставить не нужно:

```bash
java -cp /app/replicator-agent.jar \
    org.apache.hadoop.explorer.replicator.telemetry.GrpcHealthProbeMain 127.0.0.1:50051
```

Код возврата `0` — `SERVING`, `1` — любое другое состояние или недоступность. Третьим аргументом
задается таймаут в секундах, вторым — имя подсистемы.

---

### 7.2 Топология дата-центров и кластеров

Топология описывает ЦОД и привязку кластеров HDFS к ним (`dc_id`).

На демо-стенде два ЦОД:
- ЦОД 1 (Москва, основной): два кластера HDFS — `demo-cluster` (Prod DataLake) и `analytics-cluster` (Secondary DataLake).
- ЦОД 2 (Санкт-Петербург, резервный): один кластер HDFS — `backup-cluster` (DR Mirror DataLake).

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

Чтобы репликация не забивала каналы WAN, оркестратор ограничивает полосу иерархическим Token Bucket с тремя уровнями:
1. Глобальный лимит (`global_limit_mb_per_sec`) — верхняя граница суммарной скорости передачи по всей платформе.
2. Лимиты между ЦОД (`dc_limits`) — емкость магистрального канала между площадками.
3. Лимиты между парами HDFS-кластеров (`hdfs_limits`) — квота полосы под конкретное направление репликации.

Значение `0.0` снимает ограничение (в UI — «Без лимита»).

```yaml
topology:
  # 3. Лимиты полосы между ЦОД в МБ/с
  dc_limits:
    - source_dc: "dc1"
      target_dc: "dc2"
      limit_mb_per_sec: 100.0   # Магистраль Москва <-> СПб: 100 МБ/с
      description: "Магистральный канал между DC1 и DC2"

  # 4. Лимиты полосы между конкретными парами HDFS-кластеров в МБ/с
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
      limit_mb_per_sec: 80.0    # Внутрицодовый обмен
      description: "Локальный обмен между Prod и Analytics внутри DC1"

  # 5. Глобальный лимит оркестратора (Global WAN Pool)
  global_limit_mb_per_sec: 120.0
```

Пользователь с ролью `ADMIN` меняет любой лимит без перезапуска через Web UI или API (`POST /api/v1/limits/global`, `POST /api/v1/limits/dc-dc`, `POST /api/v1/limits/hdfs-hdfs`). Новое значение действует для всех воркеров в течение 1 секунды.

---

### 7.4 Безопасность, RBAC и системная техучетка Kerberos

Роли:
- `ADMIN` (`admin_user`): видит и изменяет задачи всех пользователей, меняет лимиты полосы, отменяет любые задачи.
- `WRITER` / `READER` (`de_user`, `analyst_user`): видят только свои задачи и системные задачи техучетки. Запрос на изменение лимитов возвращает `HTTP 403 Forbidden`.

Системная техучетка. Задача с флагом `run_as_service_account: true` выполняется от системного принципала (`hdfs-replicator@REALM.LOCAL`). Флаг нужен для периодических репликаций по расписанию, которые запускаются без участия пользователя.

Изоляция Kerberos-кэша. Для каждой операции воркер создает отдельный credential cache `KRB5CCNAME=/tmp/krb5cc_repl_{uuid}` и удаляет его после завершения передачи, так что билеты одной задачи не попадают в другую.

---

### 7.5 Настройка Replicator Agent (Full-Duplex) и буфера Staging

Агент репликации `backend.replicator.agent` объединяет в одном процессе gRPC-сервер приема (`Receiver`) и фоновый воркер передачи (`Sender`), поэтому один агент реплицирует в обе стороны (`DC1 ⇄ DC2`).

При старте агент регистрируется на оркестраторе: объявляет свой gRPC-адрес (`advertised_grpc_address`) и кластер, который обслуживает, а затем периодически шлет heartbeat. Адреса агентов в статическом `config.yaml` прописывать не нужно.

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

#### Регистрация и heartbeat агента

Вызовы идут через gRPC Control Plane оркестратора (`orchestrator_control_plane.proto`); REST-эндпоинт `POST /api/v1/agents/register` удален (см. раздел 7.1.1).
1. Регистрация при старте (`RegisterAgent`). Агент передает `agent_id`, `cluster_id`, `advertised_grpc_address` и версию. Оркестратор переводит агента в статус `ONLINE` и связывает его с кластером.
2. Heartbeat (`Heartbeat`). Каждые 5 секунд (`AGENT_HEARTBEAT_INTERVAL_SEC`) агент сообщает число текущих передач (`active_transfers`).
3. Обнаружение сбоя. Если heartbeat нет 15 секунд (`hadoop.replicator.agent-heartbeat-timeout-seconds`), агент переходит в `STALE` и новые передачи на него не направляются; через 45 секунд (`agent-offline-timeout-seconds`) — в `OFFLINE`.
4. Плановая остановка (`UnregisterAgent`). Агент сообщает оркестратору о завершении, статус меняется на `OFFLINE`.
5. Балансировка. Если в кластере зарегистрировано несколько агентов, оркестратор направляет новую передачу агенту с наименьшим `active_transfers`.

#### Защита регистрации

- Общий секрет агентов. Задается в `REPLICATOR_AGENT_SECRET` (не короче 16 символов, проверка в `AgentRegistry`). Агент передает его в заголовке `x-agent-secret`; вызов без секрета или с неверным секретом отклоняется со статусом `UNAUTHENTICATED`. В профиле `prod` секрет обязателен.
- Белый список кластеров. При `HADOOP_REPLICATOR_ENFORCE_CLUSTER_WHITELIST=true` (значение по умолчанию) агент регистрируется только в кластере из топологии. Регистрация в чужом кластере завершается статусом gRPC `INTERNAL` с сообщением о политике `enforce-cluster-whitelist`.
- Защита от SSRF. Оркестратор отклоняет `advertised_grpc_address` из диапазонов link-local (`169.254.0.0/16`, `fe80::/10`) и адреса облачных метаданных (`169.254.169.254`, `metadata.google.internal`).

#### Атомарная фиксация файла

1. Передающий агент режет файл на чанки размером `CHUNK_SIZE_BYTES` (по умолчанию 64 КБ) или объединяет мелкие файлы в виртуальный TAR-поток.
2. Принимающий агент пишет чанки во временный файл прямо в целевом каталоге HDFS (`targetPath + "._staging_" + jobId` или `taskId`), без промежуточного локального диска.
3. После окончания потока агент сверяет контрольную сумму SHA-256 всего файла.
4. При совпадении временный файл переименовывается в целевой путь в том же каталоге (`HadoopFsManager.renameFile` / `FileSystem.rename`); переименование в HDFS атомарно.
5. При сетевой ошибке или расхождении хеша временный файл удаляется с целевой HDFS: сразу в `cleanup()` или позже фоновой очисткой по TTL (Reaper).

---

### 7.6 Разрешение целевых адресов (Service Discovery, AGENT_TARGET_* и Fallback)

Агент-отправитель выбирает gRPC-адрес агента-приемника в удаленном ЦОД в таком порядке:

1. Реестр активных агентов оркестратора (`AgentRegistry`) — штатный режим.
   - Оркестратор хранит список зарегистрированных агентов и их статусы.
   - Метод `get_grpc_address_for_cluster(cluster_id)` [проверить: имя метода в Java-коде] возвращает адрес агента в статусе `ONLINE` с наименьшим числом текущих передач (`active_transfers`).
   - Кластер, которого нет в `config.yaml`, регистрируется по данным первого подключившегося агента.
2. Статическая топология (`GET /api/v1/clusters`).
   - В `backend/replicator/config/config.yaml` у кластера можно задать `grpc_address` (например, `backup-cluster: agent-dc2:50051`).
   - Используется, если регистрация отключена (`AGENT_ENABLE_REGISTRATION=false`) или агент-приемник не на связи.
3. Переменная `AGENT_TARGET_<CLUSTER_ID>` на агенте-отправителе.
   - Нужна при NAT, маршрутизации через ingress/mesh или других портах в DMZ.
   - Пример: `AGENT_TARGET_BACKUP_CLUSTER=agent-dc2:50051` меняет адрес только для кластера `backup-cluster`.
4. Резервный адрес `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS` (по умолчанию `localhost:50051`).
   - Используется, если целевой кластер не указан в задаче или не найден ни в одном реестре оркестратора.

---

### 7.7 Планировщик периодических задач (Cron Scheduler)

Планировщик `CronScheduler` [проверить: класса с таким именем в Java-коде нет] встроен в оркестратор:
- принимает стандартный синтаксис cron (`*/5 * * * *`) и пресеты `@every_5m`, `@hourly`, `@daily`, `@weekly`;
- проверяет БД каждые 5 секунд;
- когда `now >= next_run_at`:
  1. создает задачу репликации или ставит существующую в очередь (`status = QUEUED`);
  2. записывает `last_run_at = now`;
  3. вычисляет следующий `next_run_at`;
  4. задача выполняется от системной техучетки.

---

### 7.8 Локальное ограничение пропускной способности агента (`AGENT_MAX_BANDWIDTH_MB_S`) и модель согласования WAN

Если Replicator Agent стоит на узлах Hadoop (рядом с DataNode) или на общих edge-узлах, ограничьте его полосу локально, чтобы репликация не отнимала сеть и диски у DataNode:

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `AGENT_MAX_BANDWIDTH_MB_S` / `--max-bandwidth-mb-s` (`-b`) | `0` (без ограничения) | Предел полосы одного процесса агента, МБ/с |

Как лимит агента сочетается с лимитами WAN:
- Агент передает свой лимит при регистрации и в каждом heartbeat (`configuredBandwidthMbS`). Физическую сеть хостов (4x25G, бонды, L3-маршруты) оркестратор не моделирует; доступная полоса репликации считается как $\min(\text{wanQuotaMbS}, \sum \text{agentConfiguredBandwidthMbS})$.
- Межброкерная репликация Kafka (ISR) идет внутри ЦОД по выделенным интерфейсам кластера (например, 2x25G) и в расчет межЦОДного шейпинга не входит.
- Для потоковой репликации Kafka оркестратор рассчитывает число потоков вычитки как $\text{targetTotalThreads} = \max(64, \lceil \text{bandwidthMbS} / 30.0 \rceil)$, но не больше 32 потоков на одну JVM агента: больше потоков упираются в число сокетов и конкуренцию за CPU. Размер микробатча — от 10 000 до 50 000 сообщений (`batch.max.bytes` до 16 МБ).
- Алгоритм — асинхронный Token Bucket (`LocalBandwidthLimiter`) с burst-буфером 0,5 с.
- Ограничение работает с двух сторон:
  1. Sender ждет токены в `request_network_tokens()` [проверить: имя метода в Java-коде] перед отправкой каждого блока в gRPC;
  2. Receiver делает паузу при чтении чанков в `TransferFile()`; окно TCP и flow control HTTP/2 при этом замедляют удаленного отправителя.
- В UI показываются «Суммарный лимит подключенных агентов» и «Лимит процесса агента»; прежнего потолка отображения в 5000 МБ/с нет.

---

### 7.9 Эталонный фрагмент конфигурации топологии (application.yml)

```yaml
# backend/replicator/orchestrator/src/main/resources/application.yml (секция hadoop.replicator)

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
      kafka_bootstrap: "kafka-dc1-broker1:9092,kafka-dc1-broker2:9092"
      is_read_only: false
      description: "Основной HDFS-кластер оперативных данных в DC1"

    - id: "analytics-cluster"
      name: "HDFS Analytics (Secondary DataLake)"
      dc_id: "dc1"
      webhdfs_url: "http://localhost:9872/webhdfs/v1"
      default_path: "/data/analytics"
      kafka_bootstrap: "kafka-dc1-broker3:9092"
      is_read_only: false
      description: "Аналитический HDFS-кластер в DC1"

    - id: "backup-cluster"
      name: "HDFS DR (Backup DataLake)"
      dc_id: "dc2"
      webhdfs_url: "http://dr-namenode:9870/webhdfs/v1"
      default_path: "/backup/mirror"
      kafka_bootstrap: "kafka-dc2-broker1:9092,kafka-dc2-broker2:9092"
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

Переменные окружения сервисов (задаются в окружении процесса или контейнера):

| Переменная | Описание | Значение по умолчанию |
|---|---|---|
| `SPRING_CONFIG_ADDITIONAL_LOCATION` | Дополнительный путь к конфигурационному файлу Spring Boot (`application.yml`) | — |
| `HADOOP_REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC` | Глобальный лимит скорости репликации (байт/сек) | `104857600` (100 МБ/с) |
| `SPRING_DATASOURCE_URL` | Строка подключения JDBC к БД Оркестратора (H2/PostgreSQL) | `jdbc:h2:mem:replicator` |
| `HADOOP_REPLICATOR_AGENT_SECRET` / `REPLICATOR_AGENT_SECRET` | Общий секрет аутентификации агентов (`x-agent-secret` в метаданных gRPC), не короче 16 символов | Не задан; без него оркестратор не стартует при `enforce-agent-secret: true` |
| `HADOOP_REPLICATOR_ENFORCE_AGENT_SECRET` | Запрет запуска оркестратора без секрета агентов (fail-closed) | `true` (`ReplicatorProperties`) |
| `HADOOP_REPLICATOR_ENFORCE_TASK_ATTESTATION` | Включение криптографической аттестации задач HMAC-SHA256 | `true` |
| `HADOOP_REPLICATOR_ALLOW_LOOPBACK_AGENTS` | Разрешение регистрации агентов на loopback-адресах (`127.0.0.1`, `localhost`) | `false` |
| `HADOOP_REPLICATOR_PROGRESS_DEBOUNCE_MS` | Интервал дебаунсинга буфера фиксации прогресса задач в БД (мс) | `500` |
| `HADOOP_REPLICATOR_PROGRESS_BUFFER_SIZE` | Максимальный размер буфера завершенных задач до принудительного flush | `1000` |
| `REPLICATOR_LOCAL_SANDBOX` / `LOCAL_SANDBOX_DIR` | Корневой каталог Chroot-песочницы локальных файловых операций (SEC-10) | `/var/lib/hadoop-replicator/sandbox` |
| `REPLICATOR_SR_MUTUAL_AUTH` | Включение взаимной аутентификации SPNEGO для Schema Registry | `false` |
| `HADOOP_REPLICATOR_ENFORCE_CLUSTER_WHITELIST` | Запрет регистрации агентов в кластерах, которых нет в топологии | `true` (`application.yml`) |
| `AGENT_ID` | Уникальный идентификатор агента репликации | `agent-<uuid>` |
| `AGENT_CLUSTER_ID` | Идентификатор HDFS-кластера, обслуживаемого агентом | `null` |
| `AGENT_MODE` | Режим работы Replicator Agent (`all`, `sender`, `receiver`, `streamer`) | `all` |
| `AGENT_ENABLE_REGISTRATION` | Включение динамической авторегистрации агента на Оркестраторе | `true` |
| `AGENT_ADVERTISED_ADDRESS` | Сетевой gRPC адрес, анонсируемый агентом удаленным узлам | `$HOSTNAME:$RECEIVER_PORT` |
| `POLL_INTERVAL_SEC` | Интервал опроса очереди задач в Оркестраторе и отправки heartbeat (секунды) | `3.0` |
| `AGENT_MAX_BANDWIDTH_MB_S` | Локальный лимит скорости агента (Token Bucket), МБ/с | `0.0` (без ограничений) |
| `AGENT_TARGET_<CLUSTER_ID>` | Ручной оверрайд сетевого gRPC-адреса для конкретного целевого кластера (для NAT/DMZ) | Берется из топологии Оркестратора |
| `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS` | Резервный gRPC-адрес назначения (fallback при отсутствии кластера в топологии) | `localhost:50051` |
| `RECEIVER_HOST` / `RECEIVER_PORT` | Адрес и порт входящего gRPC-сервера Replicator Agent | `0.0.0.0:50051` |
| `REPLICATOR_STAGING_DIR` | Буферная директория Staging для локального коммита (для HDFS используется прямой Zero-Staging) | `/tmp/staging` |
| `REPLICATOR_SMALL_FILE_THRESHOLD_BYTES` | Порог размера файла (в байтах) для бандлинга мелких файлов в виртуальные TAR-архивы | `1048576` (1 МБ) |
| `REPLICATOR_BUNDLE_TARGET_SIZE_BYTES` | Целевой объем файлов в одном виртуальном TAR-бандле `BUNDLE_TAR` | `16777216` (16 МБ) |
| `REPLICATOR_MAX_BUNDLE_FILES` | Максимальное количество файлов в одном виртуальном бандле | `500` |
| `REPLICATOR_BUNDLE_COMMIT_CONCURRENCY` | Количество параллельных потоков прямой записи файлов бандла в целевой HDFS | `8` |
| `REPLICATOR_STAGING_CLEANUP_ENABLED` | Включение фонового сборщика мусора (Reaper) для очистки осиротевших staging-файлов на HDFS | `true` |
| `REPLICATOR_STAGING_CLEANUP_INTERVAL_MINUTES` | Интервал запуска периодической очистки staging-файлов (минуты) | `15` |
| `REPLICATOR_STAGING_TTL_MINUTES` | Время жизни (TTL) staging-файлов, после которого они удаляются (минуты) | `30` |
| `REPLICATOR_WIRE_COMPRESSION` | Кодек потокового сжатия в канале: `zstd` (Zstandard), `lz4` (LZ4), `none` | `zstd` |
| `REPLICATOR_WIRE_COMPRESSION_LEVEL` | Уровень сжатия для Zstandard (1–22) | `3` |
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
      globalLimitBytesPerSec: 104857600  # 100 МБ/с
      databaseUrl: "jdbc:postgresql://postgres.hadoop.svc:5432/replicator"
  agent:
    replicaCount: 2  # Горизонтальное масштабирование дуплексных агентов (Sender + Receiver)
    port: 50051
    clusterId: "demo-cluster"
    mode: "all"
    stagingDir: "/tmp/staging"
    pollIntervalSec: 3.0
```

### 9.1 Рекомендации по High-Availability в продакшне
1. База данных. При `replicaCount > 1` подключите внешний PostgreSQL (`SPRING_DATASOURCE_URL: jdbc:postgresql://...`). Встроенная H2 — только для локальной разработки и демо-стендов. Порядок перехода — в [docs/production-database.md](production-database.md).
2. PodDisruptionBudget. При `replicaCount > 1` шаблоны чартов создают `PodDisruptionBudget`, и drain узлов Kubernetes не выводит из работы все реплики сразу.
3. NetworkPolicy. Включена по умолчанию (`networkPolicy.enabled: true`): входящий трафик к подам разрешен только от Ingress-контроллера.
4. Мониторинг. Настройте сбор метрик с `/metrics` (порт 8000 / 8005): HTTP golden signals, состояние `CircuitBreaker`, повторы и ошибки.

---

## 10. Мониторинг Prometheus и дашборды Grafana

Все сервисы платформы отдают метрики в формате OpenMetrics / Prometheus.

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
| `replicator_bytes_total` | Counter | `status` | Объем переданных байтов репликации (`IN_PROGRESS`, `COMPLETED`, `FAILED`) |
| `replicator_jobs_total` | Gauge | `status`, `mode` | Количество задач репликации HDFS по статусам и типу запуска |
| `replicator_tasks_total` | Gauge | `status` | Количество пофайловых задач в распределенном пуле |
| `replicator_active_workers` | Gauge | — | Число активных онлайн-воркеров репликации |
| `replicator_active_agents` | Gauge | `status` | Число доступных агентов репликации (`online`, `total`) |
| `replicator_throttling_delay_seconds_total` | Counter | — | Суммарная задержка шейпинга сетевого канала (Token Bucket) |
| `replicator_file_size_bytes` | Histogram | `le` | Распределение размеров файлов репликации |
| `replicator_hms_jobs_total` | Gauge | `status` | Количество задач репликации Hive Metastore |
| `replicator_hms_tables_total` | Gauge | `type` | Количество таблиц HMS (`total`, `replicated`) |
| `replicator_hms_partitions_total` | Gauge | `type` | Количество партиций HMS (`total`, `replicated`) |
| `replicator_hms_speed_bytes_per_second` | Gauge | — | Скорость первичной синхронизации HMS |
| `replicator_dr_cluster_fenced` | Gauge | `cluster_id` | Статус изоляции кластера (1 = Fenced / Kill-Switch активирован, 0 = Active) |
| `replicator_dr_reverse_jobs_total` | Gauge | `status` | Количество задач обратной репликации Failover |
| `replicator_agent_status` | Gauge | `agent_id`, `cluster_id`, `mode`, `status` | Доступность узла агента (`1` при совпадении статуса, `0` иначе) |
| `replicator_agent_active_transfers` | Gauge | `agent_id`, `cluster_id`, `mode` | Текущее количество параллельных передач на агенте |
| `replicator_agent_transferred_bytes_total` | Counter | `agent_id`, `cluster_id` | Суммарный объем байтов, переданных агентом |
| `replicator_agent_errors_total` | Counter | `agent_id`, `cluster_id` | Количество ошибок передачи данных агента |
| `replicator_agent_heartbeat_age_seconds` | Gauge | `agent_id`, `cluster_id` | Задержка keepalive heartbeat агента |
| `replicator_agent_max_bandwidth_mb_s` | Gauge | `agent_id`, `cluster_id` | Выделенный локальный лимит полосы HTB агента в МБ/с |

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

В каталоге [`monitoring/grafana/dashboards/`](../monitoring/grafana/dashboards/) лежат 5 дашбордов:

1. Сводный обзор платформы: [`hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) — Golden Signals (RPS, Latency p95/p99, 5xx rate), Circuit Breaker, Retries, безопасность (Auth, Rate Limiting) и общая активность репликации.
2. Межкластерная репликация: [`hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json) — скорость репликации WAN, задержки иерархического шейпера Token Bucket Throttler, активные gRPC-воркеры, статусы задач (RUNNING, SCHEDULED, QUEUED, COMPLETED) и гистограмма размеров файлов.
3. Файловые операции HDFS: [`hdfs_explorer_operations.json`](../monitoring/grafana/dashboards/hdfs_explorer_operations.json) — файловые операции WebHDFS, скорость загрузки/скачивания (Upload vs Download), отказы операций.
4. Очереди YARN: [`yarn_explorer_queues.json`](../monitoring/grafana/dashboards/yarn_explorer_queues.json) — утилизация очередей Capacity Scheduler, запросы на изменение квот, состояние HA ResourceManager.
5. Аналитика Spark и SQL: [`spark_sql_explorer_analytics.json`](../monitoring/grafana/dashboards/spark_sql_explorer_analytics.json) — активные сессии Apache Livy, интерактивные инструкции Spark, запросы Trino и Hive Metastore.

#### Импорт дашбордов в Grafana
1. В веб-интерфейсе Grafana перейдите в **Dashboards** → **New** → **Import**.
2. Загрузите нужный JSON-файл из каталога `monitoring/grafana/dashboards/`.
3. Выберите источник данных Prometheus и нажмите **Import**.

Чтобы Grafana загружала дашборды при старте (provisioning), скопируйте файл [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml) в `/etc/grafana/provisioning/dashboards/`.

### 10.4 Примеры полезных PromQL запросов

- Текущий RPS всей платформы:
  ```promql
  sum(rate(http_requests_total[1m]))
  ```
- Процент ошибок 5xx:
  ```promql
  (sum(rate(http_requests_total{status=~"5.."}[1m])) / sum(rate(http_requests_total[1m]))) * 100
  ```
- Задержка p95 по сервисам:
  ```promql
  histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[1m])) by (le, app))
  ```
- Алерт на срабатывание Circuit Breaker (кластер недоступен):
  ```promql
  hadoop_circuit_breaker_state == 2
  ```

---

## 11. Руководства администратора по развертыванию (DevOps Hub)

Руководства по развертыванию каждого компонента в режимах Standalone (systemd), Docker / Docker Compose и Kubernetes (Helm):

- [Руководство администратора платформы (admin-guide.md)](admin-guide.md)
- [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)


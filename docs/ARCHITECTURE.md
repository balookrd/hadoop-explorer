# Архитектура платформы Hadoop Explorer Platform

Hadoop Explorer Platform — веб-платформа для работы с экосистемой Apache Hadoop, ее мониторинга и администрирования. Монорепозиторий содержит пять сервисов:
1. YARN Explorer — администрирование очередей Capacity Scheduler.
2. HDFS Explorer — файловый менеджер HDFS.
3. SQL Explorer — редактор запросов к Trino и Hive с ИИ-ассистентом (при `mock-storage: true` движки работают на встроенных демонстрационных данных, ассистент без LLM — на встроенных правилах, см. 5.3).
4. Spark Explorer — интерактивные вычисления на PySpark, Scala Spark и Spark SQL.
5. Hadoop gRPC Replicator — репликация данных HDFS, метаданных Hive Metastore (HMS CDC) и топиков Apache Kafka между изолированными ЦОД с общим ограничением полосы (Hierarchical Token Bucket), Fencing Tokens и Kerberos-изоляцией.

Документ описывает устройство платформы для разработчиков и администраторов: общий стартер безопасности, сервисы, хранилище, развертывание и тесты.

---

## 1. Принципы архитектуры

- Монорепозиторий с независимой поставкой: каждый сервис собирается в отдельный Docker-образ и не зависит от остальных сервисов платформы.
- Общий код безопасности (`backend/common-security-starter`, `frontend/common`): аутентификация (Kerberos SPNEGO + LDAPS), сессионный фильтр авторизации, хранилище сессий (L1/L2), защита от CSRF и аудит через AOP. Фильтр `RateLimitFilter` (Bucket4j) ограничивает частоту запросов к `/api/*` во всех сервисах, подключающих стартер (раздел 3.4). Класс `SimpleCircuitBreaker` в стартере есть, но сервисы его не вызывают (раздел 4.2).
- Токены только в cookie: токены авторизации передаются в cookie с флагами `HttpOnly`, `SameSite=Lax`, `Secure`. JWT и учетные данные не пишутся в `localStorage` браузера (защита от XSS, CWE-312).
- Изоляция пользовательских данных (`User Workspace`): вкладки, код и результаты выполнения хранятся отдельно для каждого пользователя в БД (H2 / PostgreSQL).
- Высокая доступность внешних служб: YARN Explorer выбирает активный ResourceManager по `haState`, HDFS Explorer работает с HA NameNode через `ConfiguredFailoverProxyProvider` (раздел 4.2).

---

## 2. Диаграмма архитектуры платформы

```
                      ┌──────────────────────────────────────────────┐
                      │             Веб-браузер клиента              │
                      │  (YARN :8001 / HDFS :8002 / SQL :8003 /      │
                      │        Spark :8004 / Replicator :8005)       │
                      └──────────────────────┬───────────────────────┘
                                             │ HTTP / SPNEGO / Cookies
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │    Reverse Proxy / Ingress / API Gateway     │
                      └──────┬──────────┬──────────┬──────────┬──────┘
                             │          │          │          │          │
         ┌───────────────────┘          │          │          │          └───────────────────┐
         ▼                              ▼          ▼          ▼                              ▼
┌──────────────────┐           ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────┐
│  YARN Explorer   │           │  HDFS Explorer   │  │   SQL Explorer   │  │  Spark Explorer  │  │  gRPC Replicator      │
│ (Java 21: 8001)  │           │ (Java 21: 8002)  │  │ (Java 21: 8003)  │  │ (Java 21: 8004)  │  │ (Orchestrator: 8005)  │
└────────┬─────────┘           └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  └───────────┬───────────┘
         │                              │                     │                     │                        │
         └──────────────────────────────┼─────────────────────┼─────────────────────┼────────────────────────┘
                                        │
                                        ▼
                   ┌──────────────────────────────────────────────┐
                   │    Ядро платформы (common-security-starter)  │
                   │  - SessionStore & L1 Caffeine / L2 JDBC      │
                   │  - CommonAuthFilter & SecurityFilterChain    │
                   │  - LdapAuthService / KerberosSpnegoService   │
                   │  - CSRF Guard (CsrfProtectionValidator)      │
                   │  - AOP JSON Structured Audit Logging         │
                   └──────────────────────┬───────────────────────┘
                                          │
     ┌─────────────────────────┬───────────┴───────────┬─────────────────────────┐
     ▼                         ▼                       ▼                         ▼
 ┌───────────────┐     ┌───────────────┐       ┌───────────────┐         ┌───────────────┐
 │  Apache YARN  │     │ Apache Hadoop │       │  Apache Hive  │         │  Apache Spark │
 │ Resource-     │     │ WebHDFS HA    │       │ HiveServer2 / │         │ Apache Livy / │
 │ Manager HA    │     │ & HttpFS      │       │   Metastore   │         │ Hive Metastore│
 └───────────────┘     └───────────────┘       └───────────────┘         └───────────────┘
```

---

## 3. Подсистема безопасности (`backend/common-security-starter`)

### 3.1 Аутентификация: Kerberos SPNEGO SSO, LDAPS и `AuthController`
1. `AuthController` (`org.apache.hadoop.explorer.common.api.AuthController`):
   - Регистрирует во всех сервисах одинаковые эндпоинты аутентификации: `POST /api/v1/auth/login`, `GET /api/v1/auth/sso`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me`.
   - Продлевает сессию при активности пользователя (sliding session), выставляет cookie с `HttpOnly`, поддерживает mock-пользователей и вход через LDAP/Kerberos.
2. `KerberosSpnegoService` (`org.apache.hadoop.explorer.common.auth`):
   - Проверяет SPNEGO-билет браузера из заголовка `Authorization: Negotiate ...` через Java GSS-API (`org.ietf.jgss`) и извлекает principal пользователя (`user@REALM`).
   - Исходящие Kerberos-вызовы стартер не выполняет: их делает каждый сервис сам (YARN Explorer — `KerberosClientLogin` по keytab для SPNEGO-запросов к ResourceManager, HDFS Explorer — клиент Hadoop через UGI).
3. LDAPS / Active Directory: проверка пароля через `LdapAuthService` с экранированием фильтров (`escapeLdapFilter`, защита от LDAP Injection, CWE-90) и чтением групп из атрибута `memberOf`.
4. Mock-режим (`hadoop.security.auth.mode: mock`) — только для локальной разработки; демо-стенды входят через OpenLDAP. При `debug: false` mock-провайдер не запускается.
5. Общая проверка токена: `CommonAuthFilter` во всех сервисах берет токен из `Authorization: Bearer` или cookie (`SecurityTokenExtractor`), отклоняет отозванные токены (`SessionStore.isTokenRevoked`), ищет сессию в `SessionStore`, а если ее нет — проверяет подпись JWT (`JwtTokenService`), повторно проверяет отзыв по `jti` и строит `UserSession`.

### 3.2 Хранилище сессий (`SessionStore`)
- Отозванные токены не принимаются (CWE-613).
- Реализация — `DefaultSessionStore`; режим задает `hadoop.security.session-store.type`: `memory` (по умолчанию, сессии и отзывы в `ConcurrentHashMap` процесса) или JDBC (любое другое значение при наличии `DataSource`). Отдельной реализации для Redis нет.
- В JDBC-режиме активные сессии хранятся в таблице `active_sessions`, отозванные токены — в `revoked_tokens`; обе таблицы создаются через `CREATE TABLE IF NOT EXISTS` при старте.
- Проверка отзыва двухуровневая:
  - L1: кэш Caffeine в поле `l1RevokedCache` `DefaultSessionStore` (`session-store.l1-capacity`, по умолчанию 10000 записей; `session-store.l1-ttl-seconds`, по умолчанию 3600 с).
  - L2: таблица `revoked_tokens` (JDBC: H2, PostgreSQL, SQLite); найденный там отзыв кладется в L1.
- Fail-closed: при `hadoop.security.session-store.fail-closed: true` ошибка запроса к БД считается отзывом и токен отклоняется. По умолчанию параметр `false`: при недоступной БД проверка продолжается по памяти процесса.
- Проверка доступности (`ping`): в JDBC-режиме выполняет `SELECT 1`. Readiness-проба `/actuator/health/readiness` этот метод не вызывает (см. раздел 9).

### 3.3 Защита от CSRF и подмены заголовков
Проверку выполняет `CsrfProtectionValidator` из `CommonAuthFilter`. Она применяется только к запросам `POST`, `PUT`, `PATCH`, `DELETE`, у которых токен пришел из cookie; запросы с `Authorization: Bearer` не проверяются. Порядок проверки:
1. `Sec-Fetch-Site: cross-site` — запрос отклоняется с HTTP 403.
2. `X-Requested-With: XMLHttpRequest` — запрос пропускается.
3. `Origin`, затем `Referer` — запрос пропускается, если origin входит в `hadoop.security.cors.allowed-origins` или совпадает с адресом самого сервера (схема, `getServerName()` и порт запроса).
4. Иначе — HTTP 403.

### 3.4 Ограничение частоты запросов и аудит
- Фильтр `RateLimitFilter` стартера ограничивает частоту запросов к `/api/*` (`RateLimiterService`, Token Bucket на `Bucket4j`). `POST /api/v1/auth/login` и `GET /api/v1/auth/sso` считаются по IP клиента (по умолчанию 10 подряд, 30 в минуту), остальные запросы к API — по имени пользователя, анонимные — по IP (100 подряд, 600 в минуту). Сверх лимита — HTTP 429 с `Retry-After`. `X-Forwarded-For` учитывается только от адресов из `hadoop.security.rate-limiter.trusted-proxies`. Фильтр зарегистрирован после `springSecurityFilterChain` и действует и в собственной цепочке оркестратора. Запросы, отклонённые цепочкой (401/403), токены не тратят. Параметры — `docs/CONFIGURATION.md`, раздел 2.5.
- Аудит: `AuditAspect` перехватывает методы с аннотацией `@Audited` и через `AuditLogger` пишет в логгер `AUDIT_LOGGER` строку `AUDIT_EVENT: {...}` с JSON-записью `AuditRecord` (`timestamp`, `service`, `username`, `client_ip`, `action`, `resource`, `status`, `duration_ms`, `details`). Имя события — значение `action` из аннотации, например `APPROVE_CHANGE_REQUEST` в YARN Explorer или `FILE_UPLOAD` в HDFS Explorer. Поле `client_ip` всегда равно `internal`. Вход, выход и отклонения CSRF через `@Audited` не проходят и в журнал аудита не попадают.

### 3.5 Content-Security-Policy (CSP) и защитные HTTP-заголовки
- Content-Security-Policy бэкенд не выставляет. CSP задана тегом `<meta http-equiv="Content-Security-Policy">` в `frontend/apps/<app>/index.html`:
  - YARN и HDFS Explorer: `default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'self'; form-action 'self'`.
  - SQL и Spark Explorer (Monaco Editor и Web Workers): дополнительно `script-src ... 'unsafe-eval' blob:`, `img-src ... blob:`, `connect-src 'self' ws: wss: http: https:`, `worker-src 'self' blob:`.
  - В `frontend/apps/replicator/index.html` CSP нет.
  - Директиву `frame-ancestors` браузеры в `<meta>` игнорируют; от встраивания во фрейм защищает только заголовок `X-Frame-Options`.
- Остальные заголовки выставляет Spring Security со значениями по умолчанию: `SecurityFilterChain` стартера блок `headers()` не настраивает.
  - `X-Frame-Options: DENY` — страницу нельзя встроить во фрейм (защита от clickjacking).
  - `X-Content-Type-Options: nosniff` — браузер не угадывает MIME-тип ответа.
  - `Strict-Transport-Security: max-age=31536000 ; includeSubDomains` (HSTS) — только в ответах на HTTPS-запросы.
  - `Cache-Control: no-cache, no-store, max-age=0, must-revalidate`.
  - `Referrer-Policy` не выставляется.

### 3.6 Состав стартера безопасности
Все сервисы подключают стартер `org.apache.hadoop.explorer:common-security-starter`:
- Spring Boot 3 AutoConfiguration: `SecurityFilterChain`, `CommonAuthFilter`, CORS, TLS Customizer и контроллеры регистрируются через `org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- Kerberos SPNEGO SSO на Java GSS-API (`org.ietf.jgss`) без сторонних библиотек.
- LDAPS / Active Directory с экранированием фильтров (CWE-90).
- SessionStore L1/L2: L1 Caffeine + L2 JDBC (H2/PostgreSQL/SQLite), отзыв токенов, fail-closed по параметру `session-store.fail-closed`.
- CSRF Guard: проверка `Sec-Fetch-Site: cross-site`, `X-Requested-With: XMLHttpRequest` и списка разрешенных `Origin`/`Referer`.
- JSON-аудит через AOP-аннотацию `@Audited`.
- `RateLimitFilter` и `RateLimiterService` (`Bucket4j`) ограничивают частоту запросов к API (раздел 3.4); `SimpleCircuitBreaker` в стартере есть, но сервисы его не вызывают (раздел 4.2).
- JWT подписываются симметрично (`HS256`, `MACSigner` из `nimbus-jose-jwt`) ключом `hadoop.security.jwt.secret-key`.
- TLS/HTTPS и mTLS: `TlsWebServerCustomizer` настраивает встроенный Tomcat, поддерживает хранилища PKCS12 / JKS и проверку клиентских сертификатов; фабрика `TlsContextFactory` строит TLS-контекст для исходящих клиентов.
- Эндпоинты `AuthController`: `/api/v1/auth/login`, `/sso`, `/logout`, `/me`.

### 3.7 TLS / mTLS для REST и gRPC
1. REST / HTTP:
   - Веб-серверы Spring Boot (`yarn`, `hdfs`, `sql`, `spark`, `replicator/orchestrator`) включают HTTPS параметром `hadoop.security.tls.enabled=true`.
   - Протоколы TLSv1.3 и TLSv1.2, настраиваемый список шифров, взаимная аутентификация (mTLS) через `client-auth: REQUIRE`.
   - Исходящие HTTP-клиенты (`HttpClient` в `NativeYarnClient`, `AwxClientService`, `OrchestratorClient`) получают TLS-контекст из `TlsContextFactory`: доверенные CA и флаг `insecureSkipVerify` для тестовых стендов.
2. gRPC-каналы репликации:
   - Приемник `DataTransferServiceImpl` в `backend/replicator/agent` настраивается через `NettyServerBuilder` и `SslContextBuilder`: X.509-сертификаты, приватные ключи, цепочки CA и `clientAuth: REQUIRE` (mTLS).
   - Отправитель `ReplicationSender` открывает TLS-сессию через `NettyChannelBuilder.forTarget(...)` при схеме `grpcs://` или `REPLICATOR_GRPC_TLS_ENABLED=true`.
   - В dev- и демо-окружениях сертификаты генерируются через `SelfSignedCertificate` и `BouncyCastle`.

---

## 4. Отказоустойчивость (Resilience Core)

### 4.1 Метрики Prometheus
- YARN, HDFS, SQL и Spark Explorer подключают `micrometer-registry-prometheus` и публикуют `/actuator/prometheus` (`management.endpoints.web.exposure.include: health,prometheus`); у всех метрик есть лейбл `app` (`management.metrics.tags.app`). Бизнес-метрики: `MeteredHdfsClient` (HDFS), `ChangeRequestMetrics` (YARN), `SqlQueryMetrics` (SQL), `SparkMetrics` (Spark). Стартер считает ответы 429 `RateLimitFilter` в `hadoop_rate_limit_blocks_total{tier}` (`login`, `api`).
- `common-security-starter` открывает `/actuator/health/**`, `/actuator/info` и `/actuator/prometheus`, остальной `/actuator/**` — только ADMIN, как в оркестраторе Replicator.
- Оркестратор Replicator отдает `/actuator/prometheus` и `/metrics` (`ReplicatorMetricsController`: метрики Micrometer плюс метрики задач и агентов из `ReplicatorMetricsService`, см. 7.2). `/metrics` требует аутентификации.
- Дашборды Grafana и конфигурация автоматической загрузки лежат в [`monitoring/grafana/dashboards/`](../monitoring/grafana/dashboards/) и [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml), описание метрик — в [CONFIGURATION.md, раздел 9](CONFIGURATION.md#9-мониторинг-prometheus-и-дашборды-grafana).

### 4.2 Circuit Breaker
- В стартере есть класс `SimpleCircuitBreaker` (`org.apache.hadoop.explorer.common.resilience`): состояния `CLOSED` → `OPEN` после `failureThreshold` подряд неудачных вызовов (сбоем считается любое исключение) → `HALF_OPEN` по истечении `recoveryTimeoutMs`. В состоянии `OPEN` вызов сразу возвращает результат `fallback` или бросает `IllegalStateException`.
- Ни один сервис `SimpleCircuitBreaker` не создает: вызовы NameNode, ResourceManager и Livy им не защищены, при недоступности кластера запрос ждет сетевого таймаута клиента.
- HA failover реализован в клиентах сервисов:
  - YARN Explorer (`NativeYarnClient`) опрашивает адреса ResourceManager и выбирает тот, у которого `clusterInfo.haState == ACTIVE`; редирект от Standby RM считается признаком переключения.
  - HDFS Explorer (`NativeHdfsClient`) настраивает `dfs.nameservices` и `ConfiguredFailoverProxyProvider`, переключение выполняет клиент Hadoop.
- Readiness-проба `/actuator/health/readiness` доступность кластеров не учитывает: она отвечает 200, даже если все кластеры недоступны.

### 4.3 Обработчики исключений (`@ControllerAdvice`)
Общего обработчика в стартере нет; ошибки 401 и 403 стартер отдает JSON-ом `{"detail", "status"}` через `CommonSecurityErrorHandlers`. Свои `@RestControllerAdvice` есть в двух сервисах:
- YARN Explorer, `ApiExceptionHandler`: `ResponseStatusException` → статус и причина из исключения; `YarnRmException` → HTTP 502; ошибки валидации тела → HTTP 400 со списком полей. Остальные исключения обрабатывает стандартный `/error` Spring Boot.
- HDFS Explorer, `GlobalHdfsExceptionHandler`: `HdfsLocalizedException` → статус из исключения; любое другое исключение пишется в лог со стеком, а `HdfsErrorTranslator` превращает его в `{"detail", "error_class", "status"}` (например, `StandbyException` → HTTP 503, остальное → HTTP 500 с текстом исключения).
- В SQL и Spark Explorer собственных обработчиков нет.

### 4.4 Блокировки при согласовании заявок YARN
Отдельного механизма распределенных блокировок в стартере нет. YARN Explorer защищает заявки (Change Requests) от двойной обработки атомарным `UPDATE` в БД: `ChangeRequestRepository.claimForDeployment` переводит одобренную заявку в `DEPLOYING` только при условии `status = APPROVED` и `deploymentStatus IS NULL OR 'FAILED'`; если обновлено 0 строк, повторный запуск развертывания отклоняется. Согласование (`approveChangeRequest`) выполняется в транзакции с проверкой статуса `SUBMITTED`.

В Replicator есть собственная аренда с fencing-токенами (`DistributedLockService`, см. 5.5).

### 4.5 Остановка сервисов
- Сервисы собраны на Spring Boot 3.3.4, где `server.shutdown` по умолчанию `immediate`; `application.yml` сервисов `server.shutdown: graceful` не задают. По SIGTERM контекст Spring закрывается без ожидания выполняющихся HTTP-запросов.
- При закрытии контекста оркестратор Replicator сбрасывает в БД остаточный буфер прогресса задач (`TaskService.shutdown()`, `@PreDestroy`, в явной транзакции через `TransactionTemplate`), до закрытия пула соединений HikariCP.

---

## 5. Сервисы платформы

### 5.1 YARN Explorer
- Бэкенд на Java 21 LTS / Spring Boot 3 (`backend/yarn/`):
  - Использует библиотеки Apache Hadoop YARN Client (`hadoop-yarn-client:3.3.6`, `hadoop-yarn-common:3.3.6`).
  - RM HA failover: запросы идут на ResourceManager с `haState == "ACTIVE"`.
  - Разбор и проверка дерева очередей (сумма емкостей дочерних очередей — 100%), расчет diff изменений RAM/vCPU и node labels.
  - Генерация `capacity-scheduler.xml` и его санитизация с защитой от XXE.
  - Жизненный цикл Change Requests по принципу четырех глаз (Spring Data JPA / H2).
  - Интеграция с Ansible AWX REST API (`POST /api/v2/job_templates/{id}/launches/`) и стартером `common-security-starter`.
  - `MockYarnClient` для тестов без кластера.
- Мониторинг очередей: дерево иерархии Capacity Scheduler, загрузка памяти и ядер в реальном времени, с учетом RM HA.
- Проверка конфигурации: сумма емкостей очередей (100%), минимальные и максимальные лимиты пользователя.
- Change Requests (принцип четырех глаз): инженер данных (`WRITER`) создает заявку, администратор (`ADMIN`) согласует ее; повторный запуск развертывания отсекается атомарным `UPDATE` (см. 4.4).
- Генерация XML: экспорт `capacity-scheduler.xml` в процентном и абсолютном форматах.
- Доставка конфигурации через Ansible AWX без простоя ResourceManager:
  - Клиент `AwxClient` вызывает Ansible AWX / Red Hat AAP асинхронно.
  - Job Template запускается по REST API, конфигурация передается в `extra_vars` в Base64 (`capacity_scheduler_xml_b64`).
  - Плейбук делает резервную копию и раскладывает файл на все узлы ResourceManager (Active и Standby) с правами `0644 yarn:hadoop`.
  - Конфигурация применяется без перезапуска демонов командой `yarn rmadmin -refreshQueues` (с Kerberos-аутентификацией).
  - Откат: если `refreshQueues` завершилась с ненулевым кодом, роль `yarn_capacity_scheduler` в блоке `rescue` восстанавливает резервную копию с меткой времени и снова применяет очереди.
  - Вывод плейбука (`stdout`) передается в веб-интерфейс по мере выполнения.
  - Спецификация и sequence-диаграмма: [docs/awx-yarn-deployment.md](awx-yarn-deployment.md).

### 5.2 HDFS Explorer
- Бэкенд на Java 21 LTS / Spring Boot 3 (`backend/hdfs/`):
  - Использует библиотеку Apache Hadoop (`org.apache.hadoop:hadoop-hdfs-client:3.3.6`).
  - HA NameNode: автоматическая настройка `dfs.nameservices` и `ConfiguredFailoverProxyProvider`.
  - Операции выполняются от имени пользователя через Kerberos proxy user (`doAs`), поэтому Ranger Audit видит конечного пользователя.
  - Предпросмотр Parquet и ORC читает файл потоком, не загружая его в память целиком (`parquet-hadoop`, `orc-core`).
  - Подключает стартер `common-security-starter`; для тестов без кластера есть in-memory эмулятор `MockHdfsClient`.
- Имперсонация (`doAs`): файловые операции выполняются от имени вошедшего пользователя, если у сервисной учетной записи есть право proxy user.
- Архивация в ZIP без блокировки запросов: упаковка и распаковка каталогов вынесены в пул рабочих потоков; есть защита от DoS/OOM и нет N+1 запросов к NameNode.
- Предпросмотр: потоковое чтение CSV/TSV, JSON, текстовых файлов, Apache Parquet и Apache ORC.
- Межкластерное копирование: файлы и каталоги передаются потоком между независимыми кластерами HDFS.

### 5.3 SQL Explorer
- Бэкенд на Java 21 / Spring Boot 3 (`backend/sql/`, порт 8003), кластеры задаются списком `sql.clusters` в `application.yml`.
- Движки: `ClusterService.getEngine` выбирает `MockSqlEngine` при `mock-storage: true` или неизвестном `type`, иначе `TrinoSqlEngine` (`type: trino`) или `HiveSqlEngine` (`type: hive`). `TrinoSqlEngine` наследует `JdbcSqlEngine`: метаданные читаются через `DatabaseMetaData` (`_` и `%` в именах экранируются), запросы выполняет `JdbcQueryRunner` (новое соединение на вызов, `Statement.setMaxRows(sql.max-rows-per-query + 1)`, сторожевой поток раз в 200 мс вызывает `Statement.cancel()` по сигналу отмены или по `jdbc.query-timeout-seconds`). Драйвер `io.trino:trino-jdbc` полностью перемещен в `io.trino.jdbc.$internal`, Kerberos выполняет сам драйвер по keytab, и так как соединение новое на каждый вызов, каждый запрос и каждое обращение к каталогу начинается с нового входа по keytab (обращения к KDC на каждый вызов); служебный пользователь — `user`, вошедший — `sessionUser`; каждый HTTP-запрос к координатору ограничен `jdbc.connect-timeout-seconds` (свойство драйвера `timeout`). Прогресс Trino (`TrinoStatement.setProgressMonitor`) отправляется не чаще раза в секунду как `status: RUNNING`. `HiveSqlEngine` использует shaded-драйвер Kyuubi (`org.apache.kyuubi:kyuubi-hive-jdbc-shaded`, протокол HiveServer2): keytab-вход `kyuubiClientPrincipal`/`kyuubiClientKeytab`, вошедший пользователь — `hive.server2.proxy.user`; драйвер повторяет весь JDBC URL в тексте ошибки (`Could not open client transport with JDBC Uri: ...`), поэтому `HiveConnectionFactory` заменяет значения `kyuubiClientPrincipal` и `kyuubiClientKeytab` на `***` до того, как текст попадет в ответ API и `query_history` (исходное исключение остается причиной и видно в логе); `getCatalogs` всегда `["default"]`. В отличие от trino-jdbc драйвер Kyuubi кэширует вход по keytab на процесс (`KerberosAuthenticationManager`), поэтому новое соединение получает только сервисный билет. Подключение к HiveServer2 (SASL, `OpenSession`) происходит до запуска сторожевого потока; его ограничивают `jdbc.connect-timeout-seconds` (`connectTimeout`, TCP) и `jdbc.socket-timeout-seconds` (`socketTimeout`, каждое чтение Thrift, по умолчанию 300 с). Запрос выполняется асинхронно с опросом `GetOperationStatus`, поэтому `socketTimeout` не обрывает длинные запросы. Драйвер содержит неперемещенные классы SLF4J 1.7, поэтому `slf4j-api` объявлен в `pom.xml` раньше драйвера, а `LoggingBindingTest` проверяет привязку к logback. Согласованность Kerberos-настроек всех кластеров Trino и Hive (`EngineConfigValidator`) проверяется при создании `ClusterService`: конфигурация Kerberos JDK одна на процесс. Ошибка движка в синхронных вызовах каталога превращается в HTTP 502 (`SqlEngineExceptionHandler`).
- Кэш метаданных: `CatalogService` хранит каталоги, схемы, таблицы и колонки в `ConcurrentHashMap` процесса с ключом `<cluster>:<user>:...`. TTL нет: запись живет до перезапуска или до запроса с `?refresh=true` (`GET /api/v1/catalog/{clusterId}/catalogs|schemas|tables|columns`).
- Проверки текста запроса на изменяющие команды (DML/DDL) нет: `QueryController` проверяет только доступ пользователя к кластеру по ACL и передает текст движку.
- Выполнение асинхронное: `POST /api/v1/queries/execute` сохраняет запись в `query_history` со статусом `QUEUED` и запускает задачу в `Executors.newCachedThreadPool()`; отмена — `POST /api/v1/queries/{queryId}/cancel` или `DELETE /api/v1/queries/queue/{queryId}`. Отмена ставит `CANCELLED` условным `UPDATE` только записи в статусе `QUEUED`/`RUNNING`; записи в статусах `FINISHED`, `FAILED`, `CANCELLED` не меняются (ответ 200). Сигнал остановки движку уходит, только если запрос выполняется в этом процессе; запись `QUEUED`/`RUNNING`, оставшаяся от прошлого процесса, просто переводится в `CANCELLED`.
- Поток событий запроса (`GET /api/v1/queries/{queryId}/stream`, SSE): `status` → `columns` → `rows` (пакеты строк, поле `total_rows` — накопленное число; JDBC-движки шлют пакеты по `jdbc.fetch-size` строк, `MockSqlEngine` — по 30) → терминальное событие. Терминальные события: `finished` со `status: FINISHED` или `CANCELLED` и `error` со `status: FAILED` и текстом в полях `error` и `message`. `QueryEventHub` хранит события запроса до истечения `sql.results-ttl-hours` и повторяет их каждому новому подписчику: интерфейс подписывается после ответа на `POST /execute`, и без повтора быстрый запрос завершался бы раньше подписки. Подписчик, отправка которому завершилась ошибкой, отключается; остальные подписчики, статус запроса и метрика `sql_query_duration_seconds` от этого не зависят. После перезапуска сервиса подписчик получает только терминальное событие из `query_history`, без строк.
- Восстановления после перезапуска нет: записи `query_history` в статусах `QUEUED`/`RUNNING`, оставшиеся от прошлого процесса, при старте не меняются.
- ИИ-ассистент (`AiService`, `/api/v1/ai/status|check|explain|optimize|fix|format|generate`): при `sql.ai.provider: openai_compatible` `OpenAiCompatibleClient` (JDK `HttpClient`, `temperature: 0`, предельное время всего ответа `sql.ai.timeout-seconds`, после него обмен отменяется) отправляет в `POST {url}/chat/completions` системный промпт со схемой JSON ответа (`SqlAssistantPrompts`) и текст SQL; ответ разбирается в те же DTO. При ошибке, таймауте или ответе не по схеме — встроенные правила (`SELECT *` без `LIMIT`, `JOIN` без `ON`/`USING`, три заготовленных запроса `generate`) с `provider: mock-builtin` и `fallback_used: true` у `check`/`generate`. `format` всегда правилами. Ответ модели ограничен 1 МиБ, позиции и `severity`/`category` замечаний нормализуются; одновременных вызовов не больше `sql.ai.max-concurrent-requests`, сверх лимита — сразу правила. `status` проверяет `GET {url}/models` с таймаутом `sql.ai.status-timeout-seconds` и в `message` отдает только краткую причину; при `mock-builtin` (по умолчанию) и недоступной LLM — `available: false`, `model: builtin-rules`. Ключ `sql.ai.api-key` маскируется в текстах ошибок; состав данных, уходящих в модель, — CONFIGURATION.md, раздел 5.3.
- В БД (JPA) хранятся вкладки пользователя (`SqlUserWorkspace`, таблица `sql_user_workspaces`), история запросов (`query_history`) и сохраненные запросы (`saved_queries`).

### 5.4 Spark Explorer
- Бэкенд на Java 21 / Spring Boot 3 (`backend/spark/`, порт 8004), кластеры задаются списком `spark.clusters` (`livy-url`, очереди YARN, версии Spark и Python, профили ресурсов).
- Движок исполнения задает интерфейс `SparkEngine` (создание, обновление и удаление сессии, выполнение кода). `SparkEngineRegistry` выбирает движок кластера: `MockSparkEngine` при `mock-storage: true` или `type`, отличном от `livy`, иначе `LivySparkEngine` (один экземпляр на кластер). Настройки Livy всех таких кластеров (`livy-url`, `livy.auth`, таймауты) проверяются в конструкторе `SparkEngineRegistry`, поэтому ошибка конфигурации останавливает старт сервиса. `LivySparkEngine` работает через JDK `HttpClient` напрямую (HTTP/1.1, `sendAsync`, заголовок `X-Requested-By` для CSRF-защиты Livy, редиректы не выполняются): `livy.connect-timeout-seconds` ограничивает установку соединения, `livy.read-timeout-seconds` — весь ответ на один запрос, заголовки и тело целиком; по истечении обмен отменяется и вызов получает HTTP 504. Ответ больше 4 МиБ (`GET /sessions?size=1000` несет хвосты логов всех сессий) отбрасывается с HTTP 502. Spring `RestClient` здесь не используется: таймаут чтения `JdkClientHttpRequestFactory` в Spring 6.1 заканчивается на заголовках, и Livy, зависший посреди тела, держал бы поток без ограничения. Запросы: `POST /sessions` с `proxyUser`, `queue`, ресурсами профиля и `conf`; перед statement ждет состояния `idle`/`busy`; statement опрашивается каждые `livy.poll-interval-ms`, отмена — `POST /sessions/{id}/statements/{sid}/cancel`. Таблица результата берется из `output.data["application/json"]` (statement `kind: sql`), текстовый вывод PySpark/Scala — в `logs`. Ошибки Livy в синхронных вызовах — HTTP 502 (таймаут — 504), недопустимый запрос сессии — 400 (`SparkEngineExceptionHandler`); ответ 2xx без JSON-объекта и коды вне 2xx/4xx/5xx тоже становятся `LivyException`. Событие `finished` движок не отправляет, только `status` с прогрессом. Значения колонок `decimal(p,s)` передаются точным текстом (ответы Livy читаются с `USE_BIG_DECIMAL_FOR_FLOATS`): в JSON результата это строка, а не число. В UI (`ResultsView.svelte`) таблица не сортируется, ячейки выводятся через `String(cell)`, поиск и экспорт CSV тоже работают со `String(val)`, поэтому строка и число отображаются одинаково; клиент API, который ждет число, должен разбирать строку; в тексте ошибки нет учетных данных из `livy-url` и значений токенов `Negotiate`/`Bearer`/`Basic`, тело ответа Livy обрезается до 500 символов. `proxyUser` — всегда имя вошедшего пользователя; ключи `spark_conf` с префиксами из `spark.denied-spark-conf-prefixes`, а также `livy.`, `spark.yarn.keytab`, `spark.yarn.principal`, `spark.kerberos.`, `spark.master`, `spark.submit.deployMode`, `spark.yarn.dist.`, `spark.yarn.jars`, `spark.yarn.archive`, `spark.jars.ivy`, `spark.jars.repositories`, `spark.yarn.queue` (без учета регистра) и пути со схемой не из `spark.allowed-remote-schemes` (по умолчанию `hdfs`, `viewfs`, `s3a`, `abfs`, `abfss`) в списках файлов отклоняются до обращения к Livy; очередь проверяется по `yarn-queues`.
- Сессии (`spark_sessions`) создаются через `POST /api/v1/sessions` и останавливаются через `DELETE /api/v1/sessions/{sessionId}`. `GET /api/v1/sessions` и `GET /api/v1/sessions/{id}` читают только БД. Статус и YARN application ID обновляет `SparkSessionManager.syncSessions` (`@Scheduled(fixedDelay = spark.session-sync-interval-ms)`, по умолчанию 5 с): по одной задаче в пуле менеджера на кластер Livy, в задаче сначала читаются нетерминальные записи кластера, затем один `GET /sessions?from=0&size=1000`. Кластер, чья задача еще идет, в следующем проходе пропускается; после любой `LivyException` кластер не опрашивается `spark.session-sync-pause-seconds` (30 с), одно предупреждение в лог за паузу. Запись, которой нет в полном ответе Livy, получает `dead`; запись, чей идентификатор Livy занят сессией без имени, с именем не `hexp-<user>-...` или с другим `proxyUser` (при `livy.proxy-user: true` `proxyUser` обязан совпадать с владельцем), тоже получает `dead` без `DELETE`. Та же проверка владельца выполняется перед statement и запросом каталога (`LivySparkEngine.waitUntilReady`, ответ 409). Все изменения статуса — условные `UPDATE` (только если запись не `dead`/`killed`), поэтому синхронизация на нескольких экземплярах и возврат в `idle` после statement не отменяют принятую остановку. Остановка (`markKilledIfActive`), `cleanupIdleSessions` (раз в минуту выбирает `idle` с `lastActivityAt` старше `spark.session-ttl-minutes` и захватывает каждую условным `UPDATE ... WHERE status = 'idle' AND last_activity_at < порог`) и событие `UserLoggedOutEvent` из `common-security-starter` (публикуется `AuthController` после `POST /api/v1/auth/logout` только на принявшем запрос экземпляре; `@EventListener` отдает удаление в пул менеджера) вызывают `DELETE /sessions/{livyId}` только если захват записи удался; для записи в `dead`/`killed` Livy не вызывается. 404 от Livy считается успешным удалением, другая ошибка пишется в лог, запись остается `killed`. Statement, упавший с `LivyException` 409, переводит запись в `dead`. Пул менеджера останавливается вместе с контекстом (`@PreDestroy`, ожидание 5 с). Сессия, созданная в Livy при таймауте ответа на `POST /sessions`, в `spark_sessions` не попадает и закрывается только по `livy.server.session.timeout`.
- Код выполняется асинхронно: `POST /api/v1/statements/execute`, статус по SSE — `GET /api/v1/statements/{executionId}/stream`, результат — `GET /api/v1/statements/{executionId}/result`, история — `spark_execution_history`. Код в сессии `dead` или `killed` не принимается: `StatementController` отвечает HTTP 404. Причина («Сессия … завершена (…), запустите новую») передается в `ResponseStatusException`, но в тело ответа не попадает: `server.error.include-message` в сервисе не задан, а по умолчанию в Spring Boot он `never`. `GET .../result` отдает конечный статус выполнения (`FINISHED`, `FAILED`, `CANCELLED`). Если движок бросил исключение, выполнение получает `FAILED` с текстом ошибки (для `Error` — с именем класса, например `NoClassDefFoundError: ...`; `VirtualMachineError` после записи итога пробрасывается дальше), а подписчики SSE — событие `{"type":"finished","status":"FAILED","error":...}`; статус и событие сохраняются в памяти экземпляра, даже если запись в БД не удалась.
- Отмена (`POST /api/v1/statements/{executionId}/cancel`) ставит `CANCELLED` условным `UPDATE` только записи в статусе `QUEUED`/`RUNNING`; записи в статусах `FINISHED`, `FAILED`, `CANCELLED` не меняются (ответ 200). Итог выполнения задача пишет тоже условным `UPDATE` (только поверх `QUEUED`/`RUNNING`). Если до этой записи была принята отмена, итог — `CANCELLED` без строк, даже если движок успел вернуть результат; иначе итог — результат движка, а поздняя отмена его не меняет. Событие `finished` от движка подписчикам не пересылается: конечное событие SSE отправляет `SparkSessionManager` после записи итога, поэтому SSE, история, `GET .../result` и метрика `spark_statement_duration_seconds` (одно значение) показывают один и тот же статус. Запись `QUEUED`/`RUNNING`, оставшаяся от прошлого процесса, просто переводится в `CANCELLED`.
- Компоновка интерфейса совпадает с SQL Explorer:
  - `SparkSessionWidget` в шапке: выбор кластера, статус Livy-сессии, память и ядра драйвера и экзекуторов, перезапуск и остановка сессии.
  - Верхняя панель вкладок для скриптов и ноутбуков.
  - Панель вкладки (`SessionBar`): выбор языка (PySpark / Scala / SQL), запуск (`Run`, `Ctrl+Enter`), прерывание (`Cancel`), таймер выполнения и экспорт результатов.
- Три языка в интерфейсе: PySpark (`kind: pyspark`), Scala Spark (`kind: spark`) и Spark SQL (`kind: sql`); язык передается в каждом statement, поэтому SQL выполняется и в PySpark-, и в Scala-сессии. Параметры сессии (`packages` → `spark.jars.packages`, `jars`, `py_files`, `spark_conf`, `custom_python_archive` → `archives`) передаются в Livy при `mock-storage: false`.
- Каталог: для кластеров Livy `SparkCatalogService` выполняет `SHOW DATABASES`, `SHOW TABLES IN` и `DESCRIBE` как statement `kind: sql` в последней активной (`idle`/`busy`) сессии пользователя на кластере; без сессии — HTTP 409 с текстом в поле `detail` (обработчик `ResponseStatusException` в `CatalogController`). Если Livy ответил, что сессия уже не работает (409), запись сессии переводится в `dead`. Имена баз и таблиц проверяются по `[A-Za-z0-9_]+`. Для эмулятора данные берутся из `MockSparkEngine`. Запрос каталога в `busy`-сессии Livy выполняет после текущего кода; `lastActivityAt` сессии запросы каталога не меняют. Кэш — `ConcurrentHashMap` процесса с ключом `<cluster>:<user>:...` без TTL, сброс — `?refresh=true`.
- Восстановления после перезапуска нет: записи `spark_execution_history` в статусах `QUEUED`/`RUNNING` при старте не меняются.
- Буферы результатов по языкам: каждая вкладка хранит отдельно буферы кода (`codeBuffers`) и результатов (`resultBuffers`), поэтому при переключении языка результаты не теряются и не смешиваются.
- Вкладки и их состояние хранятся в БД (`SparkUserWorkspace`).

### 5.5 Hadoop gRPC Replicator
- Назначение: репликация данных между HDFS-кластерами в разных ЦОД (DC1 → DC2 → DC3) с защитой сетевого периметра и ограничением полосы на нескольких уровнях.
- Топология ЦОД и кластеров HDFS:
  - Площадки и привязка каждого HDFS-кластера к ЦОД задаются в конфигурации (на стенде 2 ЦОД — Москва и Санкт-Петербург; в DC1 два кластера, в DC2 один). Подробнее — в [руководстве по конфигурации](CONFIGURATION.md#7-настройка-hadoop-grpc-replicator-dc-dc-wan-sync--throttling).
  - Сетевые лимиты на трех уровнях:
    1. DC-DC WAN: канал между ЦОД (`DC1 ➔ DC2`: 100 МБ/с).
    2. HDFS-HDFS: квоты между парами кластеров (`demo ➔ backup`: 60 МБ/с, `analytics ➔ backup`: 40 МБ/с, внутри DC1: 80 МБ/с).
    3. Global WAN Cap: общий предел для всей инфраструктуры (120 МБ/с).
- Многоуровневый Token Bucket (Hierarchical Token Bucket):
  - Потокобезопасный контроллер квот `TokenBucketThrottler` (`backend/replicator/orchestrator/src/main/java/.../TokenBucketThrottler.java`).
  - Перед передачей чанка проверяются все применимые бакеты; задержка воркера определяется самым узким из них: `max(wait_global, wait_dc_dc, wait_hdfs_hdfs)`.
  - Любой лимит меняется во время работы через REST API и веб-консоль.
- gRPC-транспорт (DC1 Worker → DC2 Receiver):
  - Потоковый контракт Protobuf (`replicator.proto` -> `DataTransferService.TransferFile` и `TransferTarStream`).
  - Передача чанками фиксированного размера (по умолчанию 4 МБ) с подсчетом SHA-256 по ходу передачи.
  - Пакетирование мелких файлов (Small Files Bundling / Tar-Streaming): файлы меньше 1 МБ упаковываются в TAR-поток на лету, на приемнике распаковываются параллельно и пишутся прямо в целевой HDFS, без промежуточного диска.
  - Запись без промежуточного каталога и атомарная фиксация: одиночный файл пишется прямо в целевой HDFS во временный файл (`targetPath + "._staging_" + jobId`), затем атомарно переименовывается через `fs.rename()`.
  - Сжатие трафика (Zstandard / LZ4): поток сжимается почанково в gRPC-канале — Zstandard (по умолчанию уровень 3) или LZ4. На текстовых данных, логах, JSON, CSV и дампах БД экономия полосы WAN составляет 40–80%. Для уже сжатых форматов (Parquet, ORC, Snappy, Gz, Zip, Avro) сжатие отключается, на несжимаемых данных — тоже. Ограничение полосы считает фактически переданные сжатые байты, а SHA-256 проверяется по исходному файлу.
- Очистка staging-файлов на HDFS в четыре уровня:
  1. В gRPC-потоке: при ошибке сети (`onError`) или неудачном переименовании временные staging-файлы сразу удаляются из целевой HDFS.
  2. Перед повтором задачи: незавершенные staging-файлы предыдущей попытки удаляются до начала синхронизации (`cleanStagingFiles`).
  3. Фоновая очистка: демон `hdfs-staging-cleaner` в `ReplicatorAgent` каждые 15 минут удаляет staging-файлы старше 30 минут по всем известным целевым путям.
  4. Исключение из манифестов и diff: предикат `HadoopFsManager.isIgnoredFile` убирает staging-файлы (`._staging_`, `.staging.`, `_staging_`) из манифестов каталогов, чтобы они не реплицировались.
- Аутентификация и роли (RBAC):
  - Kerberos SPNEGO SSO, LDAP и тестовые профили (`admin_user`, `de_user`, `analyst_user`).
  - Администратор (`admin_user`) видит и управляет задачами репликации всех пользователей.
  - Пользователь (`USER`) видит только свои задачи (`created_by == current_user.username`) и системные фоновые процессы.
- Планировщик задач (cron):
  - `ReplicationScheduler` раз в 5 секунд ставит в очередь задачи с наступившим `next_run_at`. Следующее время считает `CronSchedule`: интервальные макросы `@every_<N>s|m|h` и календарные выражения через Spring `CronExpression` (5 полей, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`) в поясе `hadoop.replicator.scheduler.time-zone`. Задача с невалидным выражением теряет расписание и получает статус `STOPPED`.
  - Время следующего запуска пишется в `next_run_at`.
- Инкрементальная репликация каталогов и Snapshot Diff:
  - Синхронизация без снимков (Batch Manifest Diff, основной режим): к HDFS обращаются только агенты в своих ЦОД, оркестратор в HDFS не ходит. Агент-источник одним gRPC-вызовом `GetDirectoryManifest` получает манифест целевого каталога, сравнивает деревья в памяти (O(N), поиск по map), пропускает совпадающие файлы (0 байт трафика WAN) и передает только разницу.
  - HDFS Snapshot Diff (API): агент управляет снимками (`HadoopFsManager.createSnapshot` / `deleteSnapshot`) для snapshottable-каталогов с миллионами файлов. Штатная инкрементальная синхронизация каталогов идет через Batch Manifest Diff (7.1) и потоковый HDFS Inotify CDC.
- Распределенный пул пофайловых задач (Distributed Task Queue и несколько агентов):
  - Двухфазное исполнение снимает ограничение одного воркера (рассчитано на объемы от 1 ПБ). Фаза анализа (`ANALYZING`) строит diff в памяти и регистрирует пул подзадач `TaskEntity` в оркестраторе (`/tasks/batch`), отмечая совпадающие файлы как `SKIPPED`.
  - Все агенты кластера-источника (DC1) параллельно забирают задачи (`/tasks/claim`) и передают файлы в целевой кластер; нагрузка распределяется на наименее загруженный онлайн-приемник (Least-Loaded Target Balancing). Оркестратор суммирует переданный объем в родительской задаче.
  - Роли агентов (`AGENT_MODE`): `all` (все функции), `analyzer` (только анализ), `worker` (только передача файлов), `receiver` (приемник).
- Фильтр временных и служебных файлов:
  - Предикат в `HadoopFsManager` и `ReplicationSender` исключает при сканировании и репликации `_temporary`, `.spark-staging-*`, `.staging`, `.Trash`, `.tmp`, `.hive-staging*`, `lost+found`, `*.inprogress`, `*.copying`, `_copying_`, `.DS_Store`, `Thumbs.db`.
  - Исключенные поддеревья пропускаются целиком (`SKIP_SUBTREE`), без RPC-запросов к NameNode и дискового I/O.
  - Маркеры коммита и файлы метаданных (`_SUCCESS`, `_SUCCESS.crc`, `_metadata`, `_common_metadata`) реплицируются.
  - Фильтр применяется с обеих сторон: на источнике (Sender/Analyzer) при построении пула задач и на приемнике (Receiver) при построении манифеста.
- Безопасность, Kerberos-изоляция и имперсонация (Apache Ranger):
  - `HadoopFsManager` аутентифицирует сервисную учетную запись по keytab (`hdfs-replicator@REALM.LOCAL`).
  - Kerberos Data Transfer Protection: SASL-защита канала передачи блоков DataNode (`dfs.data.transfer.protection = integrity/privacy`) настраивается автоматически; без нее передача между Kerberized-узлами обрывалась с `SocketException: Connection reset`.
  - Hadoop proxy user и doAs: агент подключается под доверенной сервисной учетной записью и выполняет операции с HDFS под UGI пользователя (`UserGroupInformation.createProxyUser(user, baseUgi).doAs(...)`). Apache Ranger проверяет политики для пользователя и пишет в Ranger Audit Log `ugi: user (auth:PROXY via hdfs-replicator)`.
  - HiveServer2: протестирована работа с Apache Hive 4.0.0 через Beeline по Kerberos/JDBC (создание баз, таблиц, партиций, выборка данных).
- Мониторинг, веб-интерфейс и управление задачами:
  - Метрики Prometheus (`/actuator/prometheus`) на порту `:8005`.
  - Веб-интерфейс в дизайн-системе HDFS Explorer: окно входа, выбор кластеров и ЦОД, изменение полосы во время работы.
  - Управление задачами через REST API и веб-интерфейс: запуск/перезапуск (`POST /api/v1/jobs/{id}/start`), остановка/отмена (`POST /api/v1/jobs/{id}/stop`), изменение параметров (`PUT /api/v1/jobs/{id}`) и удаление (`DELETE /api/v1/jobs/{id}`) с каскадным удалением подзадач.
  - Двухфазная репликация (Distributed Task Pool): фаза анализа регистрирует пул подзадач (`/api/v1/jobs/{id}/tasks/batch`), фаза воркеров забирает их параллельно (`claimTasks`). Подзадачи принадлежат родительской задаче `Job`; она переходит в терминальный статус (`COMPLETED`/`FAILED`) только после закрытия всех подзадач и накапливает счетчики (`total_objects`, `transferred_objects`, `skipped_objects`, `failed_objects`), объемы и среднюю скорость.
- Репликация Hive Metastore (HMS Replication):
  - Отдельный раздел интерфейса «HMS Replication» синхронизирует метаданные баз и таблиц между кластерами (HDP 3.1 ➔ Apache Hive 3.1.3).
  - Разделение Control Plane и Metadata Plane:
    - Orchestrator (Control Plane) не обращается к HDFS и не открывает Thrift-соединений. Он хранит задачи в БД, проверяет реестр `AgentRegistry`, при отсутствии пары агентов переводит задачу в `WAITING_FOR_AGENTS` (без попытки выполнить ее сам), координирует воркеры и принимает отчеты о прогрессе через gRPC Control Plane (`orchestrator_control_plane.proto`, порт 50050).
    - Stateless Active-Active Orchestration и Persistent Agent Registry: реестр агентов хранится в БД (`replication_agents`, `AgentEntity`), поэтому реплики оркестратора работают без общего состояния в памяти. Любой экземпляр видит heartbeat и состояние всех агентов кластера.
    - Распределенный Token Bucket и масштабирование оркестратора: таблица `bandwidth_limits` и координатор `OrchestratorClusterCoordinator` учитывают активные узлы (`orchestrator_nodes`) и делят токены между репликами оркестратора.
    - Монотонные Fencing Tokens против split-brain: `DistributedLockService` выдает монотонно растущие токены (`ClusterLockEntity.epoch`, не сбрасывается при снятии аренды), поэтому бывший лидер с устаревшим токеном не может выполнять операции в сценариях Disaster Recovery.
    - Replicator Agent (Data и Metadata Plane): агенты работают в локальных сетях своих ЦОД (DC1 и DC2). Source Agent читает метаданные из локального HMS по Thrift (порт 9083) в LAN, Target Agent применяет DDL в целевом HMS по Thrift в своей LAN, а между собой агенты обмениваются по WAN через gRPC-контракт `HmsTransferService` (порт 50051) с TLS.
    - Виртуальные потоки Java 21 и Netty: gRPC-приемник агента работает на виртуальных потоках (`Executors.newVirtualThreadPerTaskExecutor()`), распаковка входящих TAR-пакетов мелких файлов вынесена в отдельный пул виртуальных потоков (`tarUnpackExecutor`), чтобы не блокировать потоки EventLoop.
    - Аттестация задач (HMAC-SHA256 Task Attestation): `TaskAttestationService` подписывает параметры задачи при назначении (`v1|taskId|jobId|targetPath|executionPrincipal|runAsServiceAccount|expiresAt`), агент проверяет подпись через `TaskAttestationValidator` перед открытием HDFS OutputStream. Подменить целевой путь или Kerberos-principal без ключа нельзя.
    - Chroot-песочница и защита системных путей ОС: локальные файловые операции ограничены каталогом `localSandboxDir` (`REPLICATOR_LOCAL_SANDBOX`); пути приводятся к каноническому виду через `File.getCanonicalFile()`; блокируются symlink traversal, null byte injection, системные каталоги (`FORBIDDEN_OS_PREFIXES`) и конфиденциальные файлы хоста (`SENSITIVE_PATH_PATTERNS`). Проверки не зависят от формы URI (`scheme://host/path`, `scheme:/path`, `scheme:path`), а явная схема `file:` считается локальной ФС независимо от `fs.defaultFS`; read-пробы `checkFile` и `getDirectoryManifest` отвечают на запрещенный путь статусом `PERMISSION_DENIED`.
    - Оптимизации Data Plane:
      - Кэш каталогов HDFS и двухфазная фиксация через staging: `HadoopFsManager` кэширует уже проверенные каталоги, что снижает число RPC к NameNode до 90% при распаковке миллионов мелких файлов; по временным суффиксам `.staging` сборщик `HdfsStagingReaper` находит и удаляет осиротевшие файлы в фоне.
      - Кэш Schema Registry без блокировок и транскодирование без лишних аллокаций: кэш схем на `ConcurrentHashMap` с single-flight разрешением не дает параллельным запросам одновременно обращаться к Schema Registry за одной схемой; `ZeroCopyWireTranscoder` заменяет Schema ID без лишних аллокаций и по умолчанию не изменяет исходный буфер.
      - Изолированные очереди задач: `FOR UPDATE SKIP LOCKED` на строках задач, без блокировки строк `JobEntity`.
      - Буферизация прогресса: `JobProgressBuffer` накапливает прогресс задач с задержкой (debounce), `batchCompleteTasks` фиксирует их пакетом; транзакционная нагрузка на БД снижается в 10–50 раз.
  - Многопоточная начальная синхронизация (Bootstrap) с разбиением: пул воркеров агента обходит таблицы схемы параллельно, партиции отправляются пачками по 1000 (иначе Thrift RPC падает с `MaxMessageSize exceeded`), для файлов корня таблицы создается ровно одна корневая HDFS-подзадача.
  - Метрики начальной синхронизации: оркестратор суммирует метрики вложенных HDFS-подзадач (`HMS_SUBJOB`) — переданные байты, время старта, среднюю скорость (`copied_bytes / elapsed_sec`, МБ/с) и расчетное время окончания (ETA) — и отдает их через REST API в интерфейс в реальном времени.
  - Двусторонний diff и сверка: идемпотентный DDL (`Create or Alter`) и удаление лишних объектов (`drop_extraneous_tables`, `drop_extraneous_partitions`) всегда с `deleteData = false`: файлы на HDFS не удаляются.
  - Изоляция подзадач: задачи переноса HDFS для партиций создаются с `job_type = 'HMS_SUBJOB'` и не показываются в разделе «HDFS Replication».
  - Фильтр ACID: реплицируются External-таблицы и Managed-таблицы без транзакций (`MANAGED_TABLE`, `transactional != true`); ACID-таблицы пропускаются со статусом `SKIPPED_ACID`.
  - HDFS Federation и трансляция URI: `HmsPathRewriter` переводит физические пути HDFS в три уровня — через `TARGET_HDFS_DEFAULT_FS`, сопоставление NameService/Cluster ID в `federation-mappings` и эвристики по именам кластеров. Так `sdLocation` указывает на целевой кластер, а межкластерные HDFS-подзадачи получают правильные пути.
  - Потоковый CDC (Source Agent Autonomous Poll): агент источника сам опрашивает `NOTIFICATION_LOG`, передает события целевому агенту по gRPC и отправляет прогресс в оркестратор.
  - Эксклюзивная аренда и failover (Distributed Lease & Failover):
    - Каждую активную схему и CDC-поток захватывает ровно один агент-источник (`assigned_agent_id`, `lease_expires_at`), поэтому в пуле агентов CDC-потоки не дублируются.
    - Аренда продлевается при отправке прогресса (`ReportHmsProgress`) и опросе очереди (`ClaimHmsTasks`) по gRPC.
    - Если агент перешел в `OFFLINE` или аренда истекла, фоновый планировщик оркестратора (`checkAndFailoverOrphanedHmsJobs`) снимает привязку задачи, и другой агент пула продолжает CDC-поток с сохраненного `lastProcessedEventId`.
    - При сбое целевого агента (Target Agent) оркестратор выдает адрес резервного приемника из `AgentRegistry`, и Source-агент переключает gRPC-канал, не прерывая репликацию.
- Потоковая репликация Apache Kafka (Kafka Streaming Data Plane & Mirroring):
  - Отдельный раздел интерфейса «Kafka Replicator» синхронизирует топики Apache Kafka 2.8+ / 3.x между кластерами через gRPC-канал в WAN с TLS, пропускной способностью от 1M msg/s и ограничением полосы (Hierarchical Token Bucket).
  - Data Plane (`KafkaDataTransferService`, порт TCP 50052):
    - Двунаправленный поток `StreamKafkaBatches` поверх HTTP/2 с взаимной TLS-аутентификацией (mTLS, TLS 1.3).
    - Отдельный процесс `kafka-replicator-agent` отделяет потоковую нагрузку Kafka от пакетного I/O файлов HDFS и метаданных Hive.
  - Шардирование и балансировка (Multi-Agent Partition-Pinned Load Balancing):
    - Топики и номера партиций распределяются по пулу агентов-отправителей консистентным хэшированием при общем `group.id`, поэтому одна партиция не вычитывается дважды.
    - `KafkaTargetBalancer` распределяет нагрузку по приемникам и находит их через REST API оркестратора (`/api/v1/replicator/kafka/agents/receivers`).
    - Адреса брокеров Kafka определяются по `clusterId` через `TopologyRegistry` (`topology.clusters.<id>.kafka-bootstrap` или `kafka.clusters.<id>.bootstrap-servers`); значений по умолчанию в коде нет (`defaultValue = "DC1"` удален, параметр `clusterId` обязателен).
    - Карантин узлов: отказавший целевой агент исключается на 10 секунд, его партиции переходят к оставшимся активным узлам без остановки репликации и без потери данных.
  - Адаптивный пул потребителей `KafkaBatchConsumerPool`:
    - Число потоков чтения рассчитывается по формуле $\text{targetTotalThreads} = \max(64, \lceil \text{bandwidthMbS} / 30.0 \rceil)$, но не более 32 потоков на одну JVM агента (ограничение по сокетам и CPU). Для каналов 80G/100G/200G+ суммарный пул по кластеру агентов вырастает до 334..667+ потоков; ограничения в 40 GBit/s нет.
    - Размер пачки меняется от 10 000 до 50 000 записей (`batch.max.bytes` до 16 МБ) в зависимости от скорости; большие пачки нужны на скоростях от 100G.
    - Поддерживаются Kerberos SASL_PLAINTEXT / SASL_SSL.
    - Переподписка (`resubscribe`): при изменении списка топиков в оркестраторе пул переподписывается без перезапуска JVM агента.
  - Модель полосы агентов и изоляция ISR:
    - Оркестратор не моделирует сетевую топологию хостов (4x25G, L3-бонды, таблицы маршрутизации), а использует квоту WAN HTB и лимит процесса, заданный оператором: `--max-bandwidth-mb-s` (`AGENT_MAX_BANDWIDTH_MB_S`).
    - Доступная емкость кластера агентов: $\min(\text{wanQuotaMbS}, \sum \text{agentConfiguredBandwidthMbS})$.
    - Репликация ISR между брокерами Kafka идет через отдельные локальные сетевые адаптеры (2x25G) и в расчете WAN HTB не учитывается.
  - Асинхронная запись на виртуальных потоках Java 21:
    - `KafkaBatchProducer` и `KafkaDataTransferServiceImpl` отправляют пачки в целевой кластер через неблокирующие callback'и `producer.send(...)` на виртуальных потоках, не блокируя потоки EventLoop Netty.
    - Семантика at-least-once: оффсеты на источнике фиксируются (`commitSync`) только после подтверждения `KafkaBatchAck` от целевого брокера.
  - Обратное давление (backpressure) с двух сторон:
    - Сервер: автоматическое управление потоком Netty отключено (`disableAutoInboundFlowControl()`), следующая пачка запрашивается (`callStream.request(1)`) только после подтверждения локальным `KafkaProducer`.
    - Клиент: число неподтвержденных пачек (`inFlightBatches`) ограничено 16, чтобы не переполнить кучу (OOM).
  - Автосоздание топиков и транскодирование Schema ID:
    - `KafkaTopicProvisioner` создает топики в целевом кластере с тем же числом партиций, фактором репликации и параметрами (`min.insync.replicas`, `cleanup.policy`, `retention.ms`).
    - Синхронизация Schema Registry (Confluent / Aiven, порт 8081): `ZeroCopyWireTranscoder` заменяет Magic Byte и Schema ID в заголовках Avro/Protobuf без аллокаций в памяти, со single-flight разрешением в `ConcurrentHashMap`.
- Модули и сборка:
  - Оркестратор на Java 21 / Spring Boot 3 (`backend/replicator/orchestrator`): мультиплексированный gRPC Control Plane (`OrchestratorControlPlaneService`, порт 50050), стартер `common-security-starter`, Spring Data JPA, потокобезопасный `TokenBucketThrottler`, `AgentRegistry` с защитой от SSRF, cron-планировщик и раздача собранного Svelte 5 SPA.
  - Агент на Java 21 (`backend/replicator/agent`): воркер для DataNode и контейнеров Apache Hadoop YARN с сервисами HDFS (`DataTransferService`), Hive Metastore (`HmsTransferService`) и Kafka (`KafkaDataTransferService`); с оркестратором связывается через `OrchestratorGrpcClient`. Циклы `replicator-hms-loop` и потоки Kafka отделены от `replicator-sender-loop`, поэтому передача больших блоков данных не блокирует HMS и Kafka.
  - Схема БД как миграции (Flyway): DDL живет в `db/migration/common` (базовая схема, выгруженная из модели JPA) и `db/migration/{vendor}` (специфика PostgreSQL: частичные индексы по активным статусам, `fillfactor` и параметры autovacuum горячих таблиц). `ddl-auto` во всех профилях стоит в `validate`, поэтому расхождение модели и схемы ломает старт, а не правится молча; существующие базы подхватываются через `baseline-on-migrate`.
  - Мультимодульный Maven-проект (`backend/replicator/pom.xml`) объединяет `proto`, `model`, `agent` и `orchestrator` в общий цикл компиляции и тестирования (`make test-replicator`).
  - Общие контракты в отдельных модулях (`replicator-proto`, `replicator-model`): схемы `.proto` со сгенерированными gRPC-стабами вынесены в `backend/replicator/proto`, а общие DTO, HMS-модель, HMAC-аттестация задач и gRPC-перехватчик агента — в `backend/replicator/model`. Модуль `model` держится тонким (Jackson, SLF4J, `grpc-api`): ни Hadoop, ни Hive Thrift, ни Kafka-клиента в нем нет, поэтому control plane больше не наследует стек данных агента. Запрет закреплен в сборке правилом `bannedDependencies` (`maven-enforcer-plugin`) в `orchestrator/pom.xml`: попытка вернуть зависимость на `replicator-agent` ломает сборку оркестратора.

### 5.6 Фронтенд (Svelte 5 и Tailwind 4)
Все приложения написаны на Svelte 5 с реактивностью на рунах (`$state`, `$derived`, `$effect`):
- Виртуализация списков:
  - `FileList.svelte` в HDFS Explorer рисует только видимые строки: высота строки `ROW_HEIGHT = 37px`, запас `OVERSCAN = 12`.
  - Видимый диапазон `[startIndex, endIndex]` рассчитывается по `scrollTop` контейнера, остальная высота заполняется `topSpacerHeight` / `bottomSpacerHeight`. Число DOM-узлов не зависит от размера каталога (O(1)), поэтому каталоги с десятками тысяч файлов прокручиваются без задержек.
  - Шапка таблицы закреплена (`sticky top-0 z-10`), при переходе в другой каталог прокрутка сбрасывается в 0.
- Ленивая загрузка модальных окон (code splitting):
  - Модальные окна, мастера настроек и выдвижные панели (drawers) загружаются по требованию через `{#await import(...) then { default: Component }}`.
  - Начальный JavaScript-бандл меньше, страница становится интерактивной быстрее.
- Общие компоненты (`@hadoop-explorer/common`):
  - `Header.svelte` — шапка всех SPA: профиль пользователя, LDAP-группы и роли, выбор кластера, кнопка выхода (`Logout`).
  - `LoginModal.svelte` — диалог входа: Kerberos SPNEGO SSO, вход по учетной записи LDAP, переключение mock-пользователей.
  - `Modal.svelte` — базовое диалоговое окно с размытием фона (backdrop-blur), закрытием по Escape или щелчку вне окна и поддержкой доступности.
  - `sqlSplitter.ts` — разбор SQL-скриптов с учетом строковых литералов и комментариев, выполнение запроса под курсором (`getStatementAtCursor`).
  - `useResizable.svelte.ts` — хелперы на рунах Svelte 5 для изменения размеров панелей перетаскиванием (сайдбары и редакторы кода).
- Общий API-клиент (`BaseApiClient`):
  - Клиенты приложений (`YarnApiClient`, `hdfs/client.ts`, `ApiClient` в SQL, `SparkApiClient`) наследуются от `BaseApiClient` из `@hadoop-explorer/common`.
  - Клиент обрабатывает HTTP 401 повторной попыткой входа через Kerberos SSO (`/auth/sso`), добавляет заголовок `X-Requested-With` для защиты от CSRF, передает `credentials: 'include'` и поддерживает sliding sessions.
- Типы TypeScript (`frontend/common/types/`):
  - Интерфейсы TypeScript синхронизированы с DTO Java-сервисов (`yarn`, `hdfs`, `sql`, `spark`, `replicator`), поэтому расхождение контракта между Java record/class и клиентом обнаруживается при проверке типов.

---

## 6. Производительность и потоковая передача данных

### 6.1 HTTP-клиенты YARN Explorer
`NativeYarnClient` (ResourceManager REST API) и `AwxClientService` (Ansible AWX) используют `java.net.http.HttpClient` из Java 21, созданный один раз на клиент:
- Версия протокола не задается, поэтому действует значение `HttpClient` по умолчанию: HTTP/2 с переходом на HTTP/1.1, если сервер HTTP/2 не поддерживает.
- Таймауты: подключение — 5 с в обоих клиентах (`CONNECT_TIMEOUT_MS` в `NativeYarnClient`); запрос — 10 с в `NativeYarnClient` (`READ_TIMEOUT_MS`) и 15 с в `AwxClientService`.
- Соединения переиспользуются встроенным пулом `HttpClient`.

HDFS Explorer обращается к NameNode через клиент Hadoop (`hadoop-hdfs-client`), Spark Explorer — к Apache Livy по REST (см. 5.4).

### 6.2 Потоковая передача состояния запросов (SSE)
SQL и Spark Explorer отправляют обновления по Server-Sent Events (`SseEmitter`):
- SQL: `GET /api/v1/queries/{queryId}/stream` (события выполнения запроса, таймаут эмиттера 180 с) и `GET /api/v1/queries/notifications/stream` (уведомления пользователя `QUERY_FINISHED`, `QUERY_FAILED`, `QUERY_CANCELLED`, таймаут 300 с).
- Spark: `GET /api/v1/statements/{executionId}/stream` (таймаут 180 с); если выполнение уже завершено, эмиттер сразу отправляет итоговое событие.
- Поток событий и результат (`GET /api/v1/queries/{queryId}/result`, `GET /api/v1/statements/{executionId}/result`) отдаются только владельцу запроса (`query_history.username`, `spark_execution_history.username`) и администратору (`SecurityUtils.isAdmin`, как у отмены). Для чужого идентификатора ответ — HTTP 404, как для несуществующего: ответ не подтверждает, что такой запрос есть. Spark сверяет владельца сначала по результату в памяти, поэтому проверка работает и после неудачной записи итога в БД.
- Фронтенд подписывается через `subscribeSse` в `frontend/apps/sql/src/api/client.ts` и `frontend/apps/spark/src/api/client.ts` и не опрашивает сервер для статуса выполнения.

### 6.3 Предпросмотр Parquet / ORC
Чтение реализовано в `FilePreviewService` (HDFS Explorer):
- Для `NativeHdfsClient` файл открывается напрямую в HDFS от имени пользователя: схема Parquet читается из footer через `ParquetFileReader.readFooter`, ORC — через `OrcFile.createReader`; затем читаются первые 100 строк (`AvroParquetReader` для Parquet, `RecordReader` ORC). Остальные row group и stripe не читаются.
- Для остальных клиентов (`MockHdfsClient`) файл целиком копируется во временный локальный файл и разбирается так же.

### 6.4 Пакетные операции HDFS Explorer
- `POST /api/v1/clusters/{clusterId}/files/batch-delete`: удаляет пути по очереди в одном потоке; ответ перечисляет успешно удаленные пути и ошибки по остальным.
- `POST /api/v1/clusters/{clusterId}/files/batch-download`: выбранные файлы и каталоги упаковываются в один ZIP-архив (`ZipOutputStream` поверх `StreamingResponseBody`) на лету, без сохранения на диск сервера.

### 6.5 Конструктор Spark DAG-пайплайнов
Бэкенд пайплайнов — `PipelineController` (`/api/v1/pipelines`) и `PipelineService` в `backend/spark`, модель — `PipelineDefinition`, `PipelineNode`, `PipelineEdge`, `PipelineRun`:
- Типы узлов (`PipelineNode.type`): `pyspark` (по умолчанию), `spark_sql`, `spark_submit`, `wait`.
- `PipelineDefinition.validateDag()` проверяет ребра (несуществующие узлы, петли) и наличие цикла топологической сортировкой (алгоритм Кана); при ошибке `PipelineController` отвечает HTTP 422.
- Пайплайны и запуски хранятся только в памяти процесса (`ConcurrentHashMap`) и теряются при перезапуске.
- Запуск (`POST /api/v1/pipelines/{pipelineId}/run`) узлы не выполняет: `runPipeline` сразу создает `PipelineRun` со статусом `success` для всех узлов.
- Компонент `frontend/apps/spark/src/components/PipelineBuilder.svelte` есть, но в `App.svelte` не подключен, поэтому в интерфейсе Spark Explorer конструктора нет.

### 6.6 Кэш результатов запросов
Результаты хранятся только в памяти процесса, на диск не пишутся и теряются при перезапуске:
- SQL Explorer: `QueryManagerService.resultCache` хранит колонки и строки (не более `sql.max-rows-per-query`, по умолчанию 10000) для постраничной выдачи `GET /api/v1/queries/{queryId}/result?offset=&limit=` (`limit` по умолчанию 500). `cleanupExpiredResults` (`@Scheduled`, раз в час) удаляет результаты старше `sql.results-ttl-hours` (по умолчанию 24). После удаления или перезапуска эндпоинт возвращает только колонки из `query_history` без строк.
- Spark Explorer: `SparkSessionManager.resultCache` хранит результаты выполнений без очистки; параметр `spark.results-ttl-hours` (24) объявлен в `SparkProperties`, но не используется.

---

## 7. Наблюдаемость и мониторинг (observability)

### 7.1 Распределенная трассировка
Трассировка есть только в Replicator (7.1.1). В YARN, HDFS, SQL и Spark Explorer и в `common-security-starter` нет ни зависимостей OpenTelemetry / micrometer-tracing, ни обработки `traceparent` или `X-Request-ID`.

#### 7.1.1 Трассировка и health-пробы Replicator (оркестратор и агенты)
- Сквозной trace через gRPC (`GrpcTracing` в `replicator-model`): клиентский перехватчик кладет в метаданные вызова W3C `traceparent`, серверный поднимает его и продолжает тот же trace, выставляя `trace_id`/`span_id` в MDC. Перехватчики стоят на всех вызовах: агент → оркестратор (`OrchestratorGrpcClient`), агент → агент для файлов (`ReplicationSender`), схем Hive (`HmsPipelineSender`) и батчей Kafka (`KafkaReplicationClient`), а на серверной стороне — в Control Plane, приемнике агента и приемнике Kafka-батчей. Серверный перехватчик добавляется последним, поэтому выполняется первым: в трассу попадают и вызовы, отклоненные аутентификацией.
- Экспорт по OTLP включается отдельно: оркестратор использует micrometer-tracing на OpenTelemetry и начинает экспортировать спаны при заданном `MANAGEMENT_OTLP_TRACING_ENDPOINT`; агент (обычный Java-процесс без Spring) собирает SDK сам в `AgentTelemetry` при заданном `OTEL_EXPORTER_OTLP_ENDPOINT`. Пока адрес не задан, API работает как no-op: накладных расходов нет, но корреляция по `trace_id` в логах сохраняется.
- Стандартный health-протокол (`grpc.health.v1.Health`): статус выставляется по серверу в целом и по подсистемам отдельно, а при остановке переводится в терминальное состояние до закрытия порта. Проба освобождена от `x-agent-secret` (оркестратор контейнеров секрета агента не знает), остальные вызовы остаются закрытыми. HEALTHCHECK контейнера агента выполняется классом `GrpcHealthProbeMain` из того же jar-а и заменил прежнюю проверку открытого TCP-порта, которую проходил агент с остановленным приемником.

### 7.2 Метрики Prometheus по сервисам и дашборды Grafana
YARN, HDFS, SQL и Spark Explorer публикуют метрики на `/actuator/prometheus` (см. 4.1). Метрики Replicator отдает оркестратор на `/metrics`: `ReplicatorMetricsService.renderReplicatorPrometheusMetrics()` формирует их текстом при каждом опросе из состояния БД и реестра агентов:
- `replicator_bytes_total`, `replicator_jobs_total`, `replicator_tasks_total`, `replicator_active_workers`, `replicator_active_agents`, `replicator_throttling_delay_seconds_total`, `replicator_file_size_bytes`, `replicator_hms_jobs_total`, `replicator_hms_tables_total`, `replicator_hms_partitions_total`, `replicator_hms_speed_bytes_per_second`, `replicator_dr_cluster_fenced`, `replicator_dr_reverse_jobs_total`, `replicator_agent_status`, `replicator_agent_active_transfers`, `replicator_agent_transferred_bytes_total`, `replicator_agent_errors_total`, `replicator_agent_heartbeat_age_seconds`, `replicator_agent_max_bandwidth_mb_s`.
- Дашборды Grafana с автоматической загрузкой (provisioning) в `monitoring/grafana/dashboards/`:
  - `hadoop_explorer_overview.json` — HTTP golden signals YARN, HDFS, SQL и Spark Explorer, входы, ответы 429 ограничителя частоты и ответы 5xx, сводка Replicator.
  - `hadoop_replicator_overview.json` — межкластерная репликация, скорость WAN, иерархическое ограничение полосы Token Bucket Throttler, статусы задач HDFS/HMS, DR Fencing и KPI.
  - `hadoop_replicator_agents.json` — gRPC-агенты: статусы узлов, параллельные воркеры, переданные байты, частота heartbeat и ошибки.
  - `hadoop_replicator_hms_dr.json` — репликация Hive Metastore (Bootstrap/CDC), трансляция путей, Disaster Recovery (Fencing и аварийный Kill-Switch).
  - `hdfs_explorer_operations.json` — файловые операции WebHDFS, сетевой обмен и отказы.
  - `yarn_explorer_queues.json` — переходы заявок Change Request и задержка API YARN Explorer.
  - `spark_sql_explorer_analytics.json` — интерактивная аналитика Livy Spark и запросы Trino/Hive.
  - `hadoop_replicator_kafka.json` — репликация топиков Kafka.

---

## 8. Хранение данных (Storage Layer)

БД по умолчанию в `application.yml` сервисов:

| Сервис | `spring.datasource.url` по умолчанию | Драйверы JDBC в сборке | Схема |
|---|---|---|---|
| YARN Explorer | `${YARN_DB_URL:jdbc:h2:file:./data/yarn_explorer;DB_CLOSE_DELAY=-1}` | H2, PostgreSQL | JPA, `ddl-auto: update` (таблица `change_requests`) |
| HDFS Explorer | `jdbc:h2:mem:hdfs;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` | H2, SQLite, PostgreSQL | JPA-сущностей нет; только таблицы `SessionStore` |
| SQL Explorer | `jdbc:h2:mem:sqlexplorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` | H2, PostgreSQL | JPA, `ddl-auto: update` (`sql_user_workspaces`, `query_history`, `saved_queries`) |
| Spark Explorer | `jdbc:h2:mem:sparkexplorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` | H2, PostgreSQL | JPA, `ddl-auto: update` (`spark_sessions`, `spark_execution_history`, `spark_user_workspaces`) |
| Replicator Orchestrator | `jdbc:h2:mem:replicator;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` | H2, PostgreSQL, SQLite | миграции Flyway, `ddl-auto: validate` (см. 5.5) |

- Драйвер PostgreSQL (`org.postgresql:postgresql`, версия из Spring Boot 3.3.4 — 42.7.4) объявлен в `common-security-starter` со `scope` `runtime` и попадает в JAR всех сервисов на стартере (`BOOT-INF/lib/postgresql-42.7.4.jar`). Явного `driver-class-name` в упакованных `application.yml` нет: Spring Boot выбирает драйвер по `spring.datasource.url`, поэтому для перехода на PostgreSQL задайте `SPRING_DATASOURCE_URL=jdbc:postgresql://...`, а также `SPRING_DATASOURCE_USERNAME` и `SPRING_DATASOURCE_PASSWORD`: упакованные `sa` и пустой пароль рассчитаны на H2. В YARN Explorer вместо `SPRING_DATASOURCE_*` можно задать `YARN_DB_URL`, `YARN_DB_USER` и `YARN_DB_PASSWORD`. Для двух и более экземпляров сервиса задайте также `hadoop.security.session-store.type: jdbc` и привязку клиента к экземпляру для SQL, Spark и HDFS Explorer: [production-database.md](production-database.md).
- In-memory H2 теряет данные (воркспейсы, историю, заявки) при каждом перезапуске сервиса.

### 8.1 Инициализация схемы БД
- `schema.sql` в `backend/` нет. Таблицы JPA-сущностей YARN, SQL и Spark Explorer создает и дополняет Hibernate (`spring.jpa.hibernate.ddl-auto: update`).
- Длинные строковые поля сущностей SQL и Spark Explorer объявлены как `@Column(length = Length.LONG32)`, а не `@Lob`: Hibernate 6.5 создает для них `text` в PostgreSQL и `CHARACTER LARGE OBJECT` в H2 (тот же тип, что давал `@Lob`, поэтому существующие файлы H2 подходят без миграции). В YARN Explorer (`change_requests`) убран `@Lob`, а `columnDefinition = "TEXT"` оставлен: это `text` в PostgreSQL и неограниченный `CHARACTER VARYING` в H2, как и раньше. `@Lob` в PostgreSQL дает `oid`: чтение вне транзакции падает с `Unable to access lob stream`.
- Таблицы `SessionStore` (`active_sessions`, `revoked_tokens`) создает `DefaultSessionStore` через `CREATE TABLE IF NOT EXISTS`, если `hadoop.security.session-store.type` не `memory`.
- Схему Replicator Orchestrator ведут миграции Flyway: `db/migration/common` и `db/migration/{vendor}`, `baseline-on-migrate: true`.

---

## 9. Развертывание

1. Docker-образы на базе Eclipse Temurin 21 JRE:
   - Базовый образ `eclipse-temurin:21-jre-jammy` с утилитами Kerberos (`krb5-user`) и `curl` для healthcheck.
   - В образ кладется собранный Spring Boot fat JAR (`backend/<service>/target/*.jar` или `backend/replicator/*/target/*.jar`) со статическими бандлами Svelte 5 SPA.
   - Процесс запускается под непривилегированным пользователем `appuser (UID 10001)`.
   - Параметры памяти JVM: `-Xms256m -Xmx1024m`.
2. Логирование:
   - Сервисы пишут логи в stdout текстом в формате Logback по умолчанию Spring Boot 3.3: Logstash-энкодера и `logback-spring.xml` в YARN, HDFS, SQL, Spark Explorer и оркестраторе нет. Агент Replicator использует `backend/replicator/agent/src/main/resources/logback.xml` с текстовым шаблоном `%d [%thread] %-5level %logger{36} - %msg%n`.
   - JSON в логах есть только у записей аудита (`AUDIT_EVENT: {...}`, см. 3.4).
3. Пробы, метрики и остановка:
   - Liveness-проба `/healthz` (`HealthController`, ответ 200 без проверки зависимостей) и readiness-проба `/actuator/health/readiness` (группа readiness Spring Boot Actuator: 503 до `ApplicationReadyEvent` и с начала остановки контекста). Доступность БД и хранилища сессий пробы не проверяют.
   - Метрики Prometheus: `/actuator/prometheus` у YARN, HDFS, SQL и Spark Explorer, `/metrics` и `/actuator/prometheus` у оркестратора Replicator (см. 4.1).
   - Graceful shutdown не настроен (см. 4.5): по SIGTERM выполняющиеся HTTP-запросы обрываются.
4. Демо-стенды (`demo/`):
   - Отдельный стенд на каждый сервис и общий стенд `demo/all` с общими контейнерами OpenLDAP, MIT Kerberos KDC, Prometheus и Grafana.

---

## 10. Пользовательский интерфейс и дизайн-система

### 10.1 Темная тема (dark mode)
- `themeStore` (`frontend/common/stores/theme.svelte.ts`) на рунах Svelte 5 переключает темы `light`, `dark`, `system`, сохраняет выбор в `localStorage` и учитывает `prefers-color-scheme`.
- Переключатель темы (Sun / Moon) встроен в `Header.svelte` всех 4 фронтенд-приложений.
- Цвета темной темы заданы утилитами Tailwind CSS (`dark:bg-slate-950`, `dark:border-slate-800`, `dark:text-slate-100`).

---

## 11. Тестовая инфраструктура

Тесты разделены на уровни пирамиды: модульные, компонентные, интеграционные и сквозные E2E:

```
                  ┌───────────────────────┐
                  │ Playwright E2E Tests  │
                  └───────────┬───────────┘
                              │
                  ┌───────────┴───────────┐
                  │ Vitest UI Components  │
                  └───────────┬───────────┘
                              │
                  ┌───────────┴───────────┐
                  │ Spring Boot MockMvc IT│
                  └───────────┬───────────┘
                              │
                  ┌───────────┴───────────┐
                  │ Java 21 Unit Tests    │
                  └───────────────────────┘
```

### 11.1 Тестовое покрытие по сервисам (Java 21 + Svelte 5 Vitest)

| Сервис / уровень | Число тестов и стек | Что покрыто |
|---|---|---|
| Common Security Starter | 20 тестов (Java 21) | Spring Boot 3 AutoConfiguration, SPNEGO Kerberos GSS-API, LDAP(S) аутентификация, SessionStore L1 (Caffeine) / L2 (JDBC H2/Postgres), CSRF Guard, Bucket4j Rate Limiter, SimpleCircuitBreaker, AOP JSON-аудит |
| YARN Explorer | 13 тестов (Java 21) | Валидация Capacity Scheduler, сумма долей веток 100%, Draft Diff, генерация и санитизация XML, RM HA failover (`STANDBY` → `ACTIVE`), метрики кластера, Change Requests (принцип четырех глаз), REST-клиент AWX, аудит, L1-кэш токенов, Circuit Breaker |
| HDFS Explorer | 16 тестов (Java 21) | NameNode HA failover при `StandbyException`, Kerberos proxy user doAs, API операций, предпросмотр Parquet/ORC с чтением схемы из footer, MockHdfsClient, Circuit Breaker, Rate Limiter |
| SQL Explorer | 128 тестов (Java 21) | MockMvc API (каталог, запросы, воркспейс, ИИ-ассистент), ИИ-ассистент с LLM на WireMock (ответ модели, откат на правила при HTTP-ошибке, не-JSON, таймауте, зависшем и слишком большом теле ответа и превышении лимита одновременных вызовов, маскирование ключа, проверка конфигурации при старте), `JdbcQueryRunner` на H2 (пакеты строк, усечение, отмена, таймаут), `QueryEventHub` (повтор событий), `TrinoConnectionFactory`, Trino 484 в Testcontainers (метаданные, `sessionUser`, отмена на координаторе), `HiveConnectionFactory`, HiveServer2 4.0.0 в Testcontainers (`hive.server2.proxy.user`, метаданные, отмена, запрос дольше `socketTimeout`), `LoggingBindingTest` |
| Spark Explorer | 168 тестов по отчету surefire (Java 21; 128 методов `@Test`, остальное — случаи параметризованных тестов; 2 выполняются только при заданной `TEST_POSTGRES_URL`) | MockMvc API (кластеры, сессии, statements, пайплайны, воркспейс), статусы `FAILED`/`CANCELLED`, условная отмена в `spark_execution_history`, метрики Prometheus, клиент Livy на WireMock (сессии, `proxyUser`, ожидание `idle`, statements `pyspark`/`spark`/`sql`, отмена, ошибки), разбор вывода Livy, SPNEGO-токен против KDC Kerby, жизненный цикл сессий (состояние из Livy, остановка, TTL, выход, `dead` по 409/404), каталог через сессию |
| Hadoop gRPC Replicator | 78+ тестов (Java 21) | Hierarchical Token Bucket, Stateless Active-Active Orchestration, Persistent Agent Registry, Fencing Tokens, Task Attestation HMAC, Chroot Sandbox, DataTransfer и HmsTransfer gRPC, Direct TAR commit и Directory Cache, Schema Registry Single-Flight, smoke-тесты Prometheus и Kafka Active-Active Mirroring |
| Frontend UI | 95 тестов (Vitest) | Компонентные тесты Svelte 5 на Vitest и `@testing-library/svelte` во всех 5 SPA и общем пакете: HDFS, YARN, SQL, Spark, Replicator, Common (Header, LoginModal, Modal, StatusBadge, NotificationToast); проверка типов `svelte-check` |

### 11.2 Тесты отказоустойчивости
1. Circuit Breaker:
   - Проверяются переходы состояний: `CLOSED` → серия сбоев → `OPEN` (вызовы сразу отклоняются, упавший кластер не получает нагрузки) → ожидание `recoveryTimeoutMs` → `HALF_OPEN` → успешные пробные вызовы → `CLOSED`.
2. HA failover:
   - Переключение на активный узел при `StandbyException` от NameNode или `haState: STANDBY` от ResourceManager.
   - Переключение на резервные URL при сетевых сбоях (таймауты, обрывы связи).

### 11.3 Тесты безопасности
1. CSRF и проверка Origin:
   - Режим fail-closed для cookie-сессий, проверка заголовков `Origin`, `Referer`, `Host` и `X-Requested-With`.
2. SSRF и CWE-200:
   - Проверка хостов NameNode и DataNode перед передачей cookie `hadoop.auth`.
3. SQL- и identifier-инъекции:
   - Проверка идентификаторов в SQL Explorer: идентификатор не может завершить SQL-команду в Trino и Hive.
4. Принцип четырех глаз:
   - Автор заявки YARN не может ее согласовать.

### 11.4 Команды проверки (CI quality gates)
- `make test` — все тесты платформы (`./scripts/run-tests.sh all`).
- `make test-java` — все Java-тесты (`mvn test`).
- `make test-ui` — тесты фронтенда (`npm run test:ui`).

---

## 12. Руководства по развертыванию (DevOps runbooks)

Для каждого сервиса есть руководство администратора по установке в режимах Standalone (systemd), Docker и Docker Compose:

- [Общее руководство администратора платформы (admin-guide.md)](admin-guide.md)
- [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)
- [Руководство по Disaster Recovery и защите от split-brain (replicator-disaster-recovery-guide.md)](replicator-disaster-recovery-guide.md)
- [Руководство по отказоустойчивости Replicator (replicator-fault-tolerance.md)](replicator-fault-tolerance.md)

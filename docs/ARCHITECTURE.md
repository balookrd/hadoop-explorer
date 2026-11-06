# Архитектура платформы Hadoop Explorer Platform

Hadoop Explorer Platform — веб-платформа для работы с экосистемой Apache Hadoop, ее мониторинга и администрирования. Монорепозиторий содержит пять сервисов:
1. YARN Explorer — администрирование очередей Capacity Scheduler.
2. HDFS Explorer — файловый менеджер HDFS.
3. SQL Explorer — редактор запросов к Trino и Hive с ИИ-ассистентом.
4. Spark Explorer — интерактивные вычисления на PySpark, Scala Spark и Spark SQL.
5. Hadoop gRPC Replicator — репликация данных HDFS, метаданных Hive Metastore (HMS CDC) и топиков Apache Kafka между изолированными ЦОД с общим ограничением полосы (Hierarchical Token Bucket), Fencing Tokens и Kerberos-изоляцией.

Документ описывает устройство платформы для разработчиков и администраторов: общий стартер безопасности, сервисы, хранилище, развертывание и тесты.

---

## 1. Принципы архитектуры

- Монорепозиторий с независимой поставкой: каждый сервис собирается в отдельный Docker-образ или ставится отдельным Helm-чартом и не зависит от остальных сервисов платформы.
- Общий код безопасности и отказоустойчивости (`backend/common-security-starter`, `frontend/common`): аутентификация (Kerberos SPNEGO + LDAPS), сессионный фильтр авторизации, хранилище сессий (L1/L2), защита от CSRF, аудит через AOP, ограничение частоты запросов (Bucket4j), Circuit Breaker и корректное завершение (graceful shutdown).
- Токены только в cookie: токены авторизации передаются в cookie с флагами `HttpOnly`, `SameSite=Lax`, `Secure`. JWT и учетные данные не пишутся в `localStorage` браузера (защита от XSS, CWE-312).
- Изоляция пользовательских данных (`User Workspace`): вкладки, код и результаты выполнения хранятся отдельно для каждого пользователя в БД (H2 / PostgreSQL).
- Высокая доступность внешних служб: платформа работает с HA-конфигурациями Hadoop NameNode, YARN ResourceManager, Hive Metastore и Kerberos KDC; при недоступности узла Circuit Breaker сразу отклоняет вызовы (fast-fail), а клиент переключается на резервный узел (failover).

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
                   │  - SimpleCircuitBreaker (Fast-Fail & HA)     │
                   │  - Spring Boot 3 Graceful Shutdown           │
                   │  - LdapAuthService / Kerberos SPNEGO GSS-API │
                   │  - CSRF Guard & Rate Limiter (Bucket4j)      │
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
1. `AuthController` (`org.apache.hadoop.explorer.common.controller.AuthController`):
   - Регистрирует во всех сервисах одинаковые эндпоинты аутентификации: `POST /api/v1/auth/login`, `GET /api/v1/auth/sso`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me`.
   - Продлевает сессию при активности пользователя (sliding session), выставляет cookie с `HttpOnly`, поддерживает mock-пользователей и вход через LDAP/Kerberos.
2. `KerberosManager` (`backend.common.core.kerberos`) [проверить: класс в коде не найден, Kerberos SPNEGO реализует `KerberosSpnegoService`]:
   - Потокобезопасное управление билетами Kerberos: получение через `kinit -kt`, проверка через `klist`, формирование заголовков `Authorization: Negotiate` для внутренних клиентов WebHDFS, YARN RM и Livy.
   - Проверка SPNEGO-билетов браузера через GSSAPI и извлечение principal пользователя (`user@REALM`).
3. LDAPS / Active Directory: проверка пароля через `CommonLdapAuthService` [проверить: в коде есть `LdapAuthService`] с экранированием фильтров (защита от LDAP Injection, CWE-90) и чтением групп из `memberOf`.
4. Mock-режим (`auth.mode: "mock"`) — только для разработки и демо-стендов. При `debug: false` mock-провайдер не запускается.
5. Общая проверка токена: фабрика `make_get_current_user` [проверить: в коде не найдена] во всех сервисах одинаково проверяет JWT, проверяет, не отозван ли токен на сервере (revocation check), и строит `CommonUserSession`.

### 3.2 Хранилище сессий (`SessionStore`)
- Отозванные токены не принимаются (CWE-613).
- Методы хранилища сессий и rate limiter потокобезопасны и используют пулы потоков Java 21.
- Активные сессии хранятся в таблице `active_sessions` реляционной БД.
- Черный список отозванных токенов двухуровневый:
  - L1: LRU-кэш Caffeine в памяти (`L1RevokedTokenCache` [проверить: класс в коде не найден]) с очисткой по TTL.
  - L2: БД (PostgreSQL / H2 / SQLite) или Redis.
- Fail-closed: если БД временно недоступна, токены, которые не удалось проверить, отклоняются.
- Проверка доступности (`ping`): хранилище отвечает на проверку для Kubernetes readiness probe.

### 3.3 Защита от CSRF и подмены заголовков
- Межсайтовые запросы блокируются по заголовку `Sec-Fetch-Site: cross-site`.
- Заголовки `Origin` и `Referer` сверяются со списком разрешенных доменов (`server.cors_origins`). Заголовку `Host` без проверки не доверяют (защита от Host Header Injection).
- Асинхронные вызовы API обязаны передавать заголовок `X-Requested-With`.

### 3.4 Ограничение частоты запросов и аудит
- Алгоритм скользящего окна / Token Bucket (`Bucket4j`) с потокобезопасной проверкой; счетчики можно хранить в распределенном хранилище.
- Реальный IP клиента определяется по `X-Forwarded-For` только от доверенных прокси (`trusted_proxies` [проверить: параметр в коде не найден]); так подмена заголовка не меняет IP для лимитов.
- События безопасности пишутся в журнал в формате JSON: `AUDIT_LOGIN_SUCCESS`, `AUDIT_LOGIN_FAILURE`, `AUDIT_TOKEN_REVOKED`, `AUDIT_CSRF_REJECT` [проверить: имена событий в коде не найдены].

### 3.5 Content-Security-Policy (CSP) и защитные HTTP-заголовки
Фильтр безопасности выставляет следующие заголовки:
- Content-Security-Policy (CSP) [проверить: константы `CSP_DEFAULT_DIRECTIVES` и `CSP_CODE_EDITOR_DIRECTIVES` в коде не найдены, CSP задана в `<meta>` в `index.html` сервисов]:
  - Строгая политика (`CSP_DEFAULT_DIRECTIVES`) для YARN и HDFS Explorer: `default-src 'self'`, `script-src 'self'`, `style-src 'self' 'unsafe-inline'`, `img-src 'self' data:`, `font-src 'self'`, `connect-src 'self'`, `frame-ancestors 'none'`, `object-src 'none'`, `base-uri 'self'`.
  - Политика для редакторов кода (`CSP_CODE_EDITOR_DIRECTIVES`) для SQL и Spark Explorer: Monaco Editor и Web Workers требуют `worker-src 'self' blob:`, `script-src 'self' 'unsafe-eval' blob:`, `connect-src 'self' ws: wss: http: https:`, `img-src 'self' data: blob:`.
  - CSP продублирована в `index.html` через `<meta http-equiv="Content-Security-Policy">`, чтобы она действовала и при раздаче статики отдельным веб-сервером.
- Остальные заголовки:
  - `X-Frame-Options: DENY` — страницу нельзя встроить во фрейм (защита от clickjacking).
  - `X-Content-Type-Options: nosniff` — браузер не угадывает MIME-тип ответа.
  - `Referrer-Policy: strict-origin-when-cross-origin` — на сторонние ресурсы уходит только origin, без пути.
  - `Strict-Transport-Security: max-age=31536000; includeSubDomains` (HSTS) — браузер обращается к сайту только по HTTPS; выставляется при включенной опции `secure_cookies`.

### 3.6 Состав стартера безопасности
Все сервисы подключают стартер `org.apache.hadoop.explorer:common-security-starter`:
- Spring Boot 3 AutoConfiguration: `SecurityFilterChain`, `CommonAuthFilter`, CORS, TLS Customizer и контроллеры регистрируются через `org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- Kerberos SPNEGO SSO на Java GSS-API (`org.ietf.jgss`) без сторонних библиотек.
- LDAPS / Active Directory с экранированием фильтров (CWE-90).
- SessionStore L1/L2: L1 Caffeine LRU + L2 JDBC H2/PostgreSQL, отзыв токенов, fail-closed при недоступности БД.
- CSRF Guard: проверка `Sec-Fetch-Site: cross-site`, `X-Requested-With: XMLHttpRequest` и списка разрешенных `Origin`/`Referer`.
- Отказоустойчивость и аудит: Token Bucket Rate Limiter (`Bucket4j`), `SimpleCircuitBreaker` и JSON-аудит через AOP-аннотацию `@Audited`.
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

### 3.8 Асимметричные JWT (RS256/ES256), ротация ключей и JWKS (RFC 7517)
`JwtKeyManager` в `backend/common-security-starter` [проверить: класс, RS256/ES256 и эндпоинты JWKS в Java-коде не найдены, `jwks.json` есть только в `frontend/common/types/generated/`]:
- Подписывает токены RSA (`RS256`) или ECDSA (`ES256`) и добавляет идентификатор ключа `kid` в заголовок JWT.
- Ротация ключей: при выпуске нового ключа подписи старые публичные ключи сохраняются, поэтому ранее выданные токены продолжают проходить проверку.
- Эндпоинты `GET /api/v1/auth/jwks.json` и `GET /.well-known/jwks.json` отдают открытые ключи в формате RFC 7517 для внешних API Gateway, Service Mesh и OAuth2/OIDC-сервисов.

---

## 4. Отказоустойчивость (Resilience Core)

### 4.1 Метрики Prometheus и дашборды Grafana (`backend.common.core.metrics`)
Каждый сервис отдает метрики в формате OpenMetrics / Prometheus через `PrometheusMetricsMiddleware` [проверить: модуль `backend.common.core.metrics` и `PrometheusMetricsMiddleware` в коде не найдены; перечисленные ниже метрики есть в дашбордах `monitoring/grafana/`, но не регистрируются в `backend/`]:
- HTTP (golden signals):
  - `http_requests_total{app="...", method="...", path="...", status="..."}`: счетчик запросов; путь нормализуется по шаблону маршрута, чтобы динамические ID не раздували кардинальность.
  - `http_request_duration_seconds`: гистограмма задержек (p50, p90, p99), бакеты от `5ms` до `10s`.
  - `http_requests_in_progress{app="..."}`: число запросов в обработке.
- Отказоустойчивость и сетевые вызовы:
  - `hadoop_circuit_breaker_state{name="..."}`: состояние автомата (0=CLOSED, 1=HALF_OPEN, 2=OPEN) для NameNode, ResourceManager, Livy.
  - `hadoop_circuit_breaker_calls_total{name="...", status="success|failed|rejected"}`: успешные, неудачные и отклоненные fast-fail вызовы.
  - `hadoop_retry_attempts_total{app="...", operation="...", status="retry|exhausted|success"}`: повторные попытки.
- Безопасность и ошибки:
  - `hadoop_auth_attempts_total{app="...", provider="ldap|kerberos|mock", status="success|failure"}`: попытки входа.
  - `hadoop_rate_limit_blocks_total{app="..."}`: срабатывания rate limiter (HTTP 429).
  - `hadoop_exceptions_total{app="...", exception_type="..."}`: необработанные ошибки HTTP 500 (CWE-209).
- Дашборд Grafana: JSON в [`monitoring/grafana/dashboards/hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) и конфигурация автоматической загрузки [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml) для всех сервисов платформы.

### 4.2 Circuit Breaker (`backend.common.core.circuit_breaker`)
Исходящие клиенты к кластерам Hadoop (NameNode, YARN RM, Livy) защищены автоматом состояний `CircuitBreaker`, чтобы недоступность кластера не вызывала каскадных сбоев:
- Состояния: `CLOSED` (нормальная работа) → `OPEN` (кластер недоступен, вызовы сразу отклоняются с `CircuitBreakerOpenException` [проверить: класс в коде не найден] без ожидания сетевых таймаутов) → `HALF_OPEN` (пробный запрос проверяет, восстановился ли кластер).
- Какие ошибки считаются сбоем: сетевые ошибки, ошибки подключения, таймауты и ответы 5xx. Ответы 4xx — штатные ответы и автомат не размыкают.
- HA failover: при сбое Active-узла клиент переключается на Standby NameNode / Standby ResourceManager.
- Readiness-проба Helm-чартов (`/actuator/health/readiness`) состояние Circuit Breaker не учитывает: под остается в Service, даже если все кластеры недоступны.

### 4.3 Повторные попытки с экспоненциальной задержкой
Для кратковременных сбоев сети и сокетов:
- Интервал между попытками растет экспоненциально (`backoff_factor` [проверить: параметр в коде не найден; в `common-security-starter/pom.xml` подключен `resilience4j-retry`]) со случайной добавкой (`jitter`), чтобы клиенты не повторяли запросы одновременно (thundering herd).
- Список повторяемых исключений настраивается (`java.net.http.HttpConnectTimeoutException`, `java.io.IOException`); ошибки бизнес-логики не повторяются.

### 4.4 Обработчики исключений (`@ControllerAdvice`)
- Ответ на ошибку 500 не раскрывает внутреннюю структуру бэкенда (CWE-209).
- Глобальный обработчик `@ControllerAdvice` (`GlobalExceptionHandler` [проверить: в коде есть `GlobalHdfsExceptionHandler` в HDFS и `ApiExceptionHandler` в YARN]) перехватывает необработанные ошибки, присваивает им `incidentId`, пишет подробности в серверный лог и возвращает клиенту структурированный JSON без деталей.
- `CircuitBreakerOpenException` превращается в HTTP 503 с заголовком `Retry-After`.

### 4.5 Distributed Lock (`backend.common.core.lock`)
[проверить: модуль `backend.common.core.lock`, `REDIS_RELEASE_LUA` и таблица `distributed_locks` в коде не найдены]

Блокировки исключают гонки при параллельных операциях:
- Три уровня реализации:
  1. Redis: атомарный `SET key owner_id NX PX`, освобождение через Lua-скрипт (`REDIS_RELEASE_LUA`).
  2. Блокировка на уровне строк в БД (PostgreSQL / SQLite): таблица `distributed_locks` в `SessionStore` с транзакционным `SELECT ... FOR UPDATE` (PostgreSQL) и атомарными `INSERT`/`UPDATE` с проверкой `expires_at` и `owner_id`. Синхронизирует процессы и поды без Redis.
  3. In-memory: локальный потокобезопасный словарь с TTL для разработки.
- YARN Explorer использует блокировку при согласовании и отклонении заявок на изменение очередей (Change Requests).

### 4.6 Корректное завершение (graceful shutdown)
Сервисы на Spring Boot 3 завершаются штатно (`server.shutdown=graceful` [проверить: параметр в конфигурациях сервисов не найден]):
- Обрабатывают сигналы завершения пода Kubernetes (`SIGTERM`, `SIGINT`).
- Дожидаются выполняющихся запросов (таймаут `spring.lifecycle.timeout-per-shutdown-phase`).
- Останавливают фоновые пулы задач и потоков (`ExecutorService.shutdown()`).
- Закрывают пулы соединений с БД (HikariCP) и освобождают сетевые ресурсы.
- Сбрасывают буферы из памяти в БД отдельной транзакцией: остаточный прогресс задач репликатора (`TaskService`) записывается до закрытия пула соединений и не теряется при остановке узла.

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
- Мониторинг очередей: дерево иерархии Capacity Scheduler, загрузка памяти и ядер в реальном времени, с учетом RM HA и Circuit Breaker.
- Проверка конфигурации: сумма емкостей очередей (100%), минимальные и максимальные лимиты пользователя.
- Change Requests (принцип четырех глаз): инженер данных (`WRITER`) создает заявку, администратор (`ADMIN`) согласует ее; согласование защищено `DistributedLock`.
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
- Два движка: Trino (Trino DB-API) и Apache Hive (HiveServer2 / TCLIService Thrift).
- Кэш метаданных с TTL (`MetadataTTLCache` [проверить: класс в коде не найден]): каталоги, схемы, таблицы и колонки; сброс параметром `?refresh=true`.
- Проверка запросов fail-closed: синтаксический анализ через `sqlglot` блокирует изменяющие команды (DML/DDL) и отклоняет запросы, которые не удалось разобрать.
- Восстановление после сбоя: при старте сервис завершает SQL-запросы, оставшиеся от предыдущего процесса.
- ИИ-ассистент на локальных LLM (через vLLM, Ollama или LiteLLM): генерация SQL по описанию на естественном языке, объяснение планов запросов, оптимизация и исправление синтаксических ошибок.
- Вкладки и запросы пользователя хранятся в БД (`SqlUserWorkspace`).

### 5.4 Spark Explorer
- Apache Livy: интерактивные сессии и пакетные задания на YARN через Livy REST API; при выходе пользователя (`POST /api/v1/auth/logout`) его сессии закрываются.
- Компоновка интерфейса совпадает с SQL Explorer:
  - `SparkSessionWidget` в шапке: выбор кластера, статус Livy-сессии, память и ядра драйвера и экзекуторов, перезапуск и остановка сессии.
  - Верхняя панель вкладок для скриптов и ноутбуков.
  - Панель вкладки (`SessionBar`): выбор языка (PySpark / Scala / SQL), запуск (`Run`, `Ctrl+Enter`), прерывание (`Cancel`), таймер выполнения и экспорт результатов.
- Три языка:
  - PySpark: интерактивный Python; окружения Conda/Venv, упакованные в HDFS, подключаются через `spark.archives`.
  - Scala Spark: интерактивная REPL-сессия с подключением своих JAR и Maven-библиотек.
  - Spark SQL: ANSI SQL-запросы к таблицам Hive Metastore / Apache Iceberg.
- Кэш метаданных с TTL (`SparkMetadataTTLCache` [проверить: класс в коде не найден]): базы, таблицы и колонки кэшируются на 60 с (настраивается), `?refresh=true` сбрасывает кэш. Без кэша каждый запрос автодополнения ходил бы в Hive Metastore и Spark-сессию.
- Восстановление после сбоя: при старте пода зависшие задачи (`RUNNING`, `QUEUED`) переводятся в `FAILED`.
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
  - `ReplicationScheduler.java` на Spring Scheduling запускает периодическую синхронизацию (`@every_5m`, `@hourly`, `@daily`, произвольное cron-выражение).
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

### 6.1 Пул HTTP-соединений с HTTP/2
HTTP-клиенты построены на `HttpClient` из Java 21 с пулами соединений:
- HTTP/2 с мультиплексированием; если бэкенд HTTP/2 не поддерживает, клиент переходит на HTTP/1.1.
- Настраиваемые таймауты подключения, чтения и keepalive.
- Повторное использование соединений сокращает число TLS/TCP-рукопожатий при частых опросах API кластера (YARN RM, WebHDFS, Livy).

### 6.2 Потоковая передача состояния сессий и запросов (SSE)
SQL и Spark отправляют обновления по Server-Sent Events:
- `GET /api/v1/sessions/{session_id}/stream` (Spark) и `GET /api/v1/queries/{query_id}/stream` (SQL).
- Фронтенду не нужно опрашивать сервер частыми HTTP-запросами.

### 6.3 Частичное чтение и предпросмотр Parquet / ORC
HDFS Explorer использует потоковый адаптер `_SeekableFooterStream` [проверить: класс в коде не найден; чтение реализовано в `FilePreviewService`]:
- Схема и статистика читаются из хвостовой части файла (footer) одним запросом, без скачивания данных.
- Для выборки строк читается только `Row Group 0` (Parquet) или `Stripe 0` (ORC), поэтому время ответа не зависит от размера файла, даже если файл занимает сотни гигабайт.

### 6.4 Пакетные операции HDFS Explorer
- `POST /api/v1/clusters/{cluster_id}/files/batch-delete`: параллельное удаление файлов и каталогов; ответ перечисляет успешно удаленные пути и ошибки по остальным.
- `POST /api/v1/clusters/{cluster_id}/files/batch-download`: выбранные файлы и каталоги упаковываются в один ZIP-архив на лету, без сохранения на диск сервера.

### 6.5 Условное кэширование по ETag (HTTP 304 Not Modified)
Фильтр Spring Boot `ShallowEtagHeaderFilter` [проверить: фильтр в коде не найден]:
- Рассчитывает слабый ETag (`W/"..."`) для ответов API на безопасные запросы GET/HEAD.
- Если заголовок `If-None-Match` совпадает с текущим ETag, возвращает `HTTP 304 Not Modified` без тела (каталоги метаданных Hive, очереди YARN, списки кластеров).

### 6.6 Конструктор и планировщик Spark DAG-пайплайнов
Spark Explorer включает визуальный конструктор пайплайнов (`backend.spark.app.models.pipeline`, `PipelineBuilder.svelte` [проверить: в коде есть `PipelineService` и DTO `PipelineDefinition` в `backend/spark`, а `PipelineBuilder.svelte` не найден]):
- Узлы графа разных типов: скрипты PySpark, запросы Spark SQL, задачи Spark Submit.
- Граф проверяется топологической сортировкой (алгоритм Кана); граф с циклом отклоняется с ошибкой `422 Unprocessable Content`.
- Узлы выполняются асинхронно, статус каждого узла обновляется в интерфейсе по ходу выполнения.

### 6.7 Дисковый кэш результатов и удаление по TTL (`data/results/`)
[проверить: `cleanup_expired_results()` и `RESULTS_TTL_SECONDS` в коде не найдены; в Spark Explorer есть параметр `results-ttl-hours` со значением 24]

Чтобы не нагружать Trino, Hive и Livy повторными запросами, результаты сохраняются сжатыми (gzip JSON: `{id}.json.gz`) в каталоге `data/results/`:
- SQL Explorer кэширует выборки строк и схемы колонок для пагинации, переключения вкладок редактора и экспорта в CSV/JSON без повторного запроса.
- Spark Explorer кэширует разобранные табличные результаты statements в сессиях Livy.
- Удаление по TTL:
  - Оба сервиса реализуют метод `cleanup_expired_results()`, который смотрит на время изменения файла (`mtime`).
  - Файлы старше `RESULTS_TTL_SECONDS` (по умолчанию 7 дней / `604800` с) удаляются.
  - Очистка выполняется при старте сервиса и периодически в фоновом воркере с интервалом `RESULTS_CLEANUP_INTERVAL_SECONDS` (по умолчанию 1 час / `3600` с).

---

## 7. Наблюдаемость и мониторинг (observability)

### 7.1 Распределенная трассировка OpenTelemetry и W3C Trace Context
Модуль `backend.common.core.tracing` [проверить: модуль и `OpenTelemetryMiddleware` в коде не найдены; для Replicator трассировка описана в 7.1.1]:
- Поддерживается W3C Trace Context (`traceparent` в формате `00-<trace_id>-<span_id>-01`).
- `OpenTelemetryMiddleware` связывает спаны с `X-Request-ID` и передает заголовки трассировки клиентам и вызываемым сервисам платформы.
- Контекстный менеджер `tracer.span(...)` замеряет вызовы к кластерам Hadoop, YARN, Trino и Livy.

#### 7.1.1 Трассировка и health-пробы Replicator (оркестратор и агенты)
- Сквозной trace через gRPC (`GrpcTracing` в `replicator-model`): клиентский перехватчик кладет в метаданные вызова W3C `traceparent`, серверный поднимает его и продолжает тот же trace, выставляя `trace_id`/`span_id` в MDC. Перехватчики стоят на всех вызовах: агент → оркестратор (`OrchestratorGrpcClient`), агент → агент для файлов (`ReplicationSender`), схем Hive (`HmsPipelineSender`) и батчей Kafka (`KafkaReplicationClient`), а на серверной стороне — в Control Plane, приемнике агента и приемнике Kafka-батчей. Серверный перехватчик добавляется последним, поэтому выполняется первым: в трассу попадают и вызовы, отклоненные аутентификацией.
- Экспорт по OTLP включается отдельно: оркестратор использует micrometer-tracing на OpenTelemetry и начинает экспортировать спаны при заданном `MANAGEMENT_OTLP_TRACING_ENDPOINT`; агент (обычный Java-процесс без Spring) собирает SDK сам в `AgentTelemetry` при заданном `OTEL_EXPORTER_OTLP_ENDPOINT`. Пока адрес не задан, API работает как no-op: накладных расходов нет, но корреляция по `trace_id` в логах сохраняется.
- Стандартный health-протокол (`grpc.health.v1.Health`): статус выставляется по серверу в целом и по подсистемам отдельно, а при остановке переводится в терминальное состояние до закрытия порта. Проба освобождена от `x-agent-secret` (оркестратор контейнеров секрета агента не знает), остальные вызовы остаются закрытыми. HEALTHCHECK контейнера агента выполняется классом `GrpcHealthProbeMain` из того же jar-а и заменил прежнюю проверку открытого TCP-порта, которую проходил агент с остановленным приемником.

### 7.2 Метрики Prometheus по сервисам и дашборды Grafana
Кроме общих HTTP-метрик, сервисы отдают собственные метрики через `MetricsRegistry` [проверить: класс в коде не найден]:
- YARN: `yarn_queues_active_gauge`, `yarn_change_requests_total`.
- Spark: `spark_sessions_active_gauge`, `spark_statements_total`, `spark_statement_duration_seconds`.
- SQL: `sql_queries_total`, `sql_query_duration_seconds`.
- HDFS: `hdfs_operations_total`, `hdfs_bytes_transferred_total`.
- Replicator: `replicator_bytes_total`, `replicator_jobs_total`, `replicator_tasks_total`, `replicator_active_workers`, `replicator_active_agents`, `replicator_throttling_delay_seconds_total`, `replicator_file_size_bytes`, `replicator_hms_jobs_total`, `replicator_hms_tables_total`, `replicator_hms_partitions_total`, `replicator_hms_speed_bytes_per_second`, `replicator_dr_cluster_fenced`, `replicator_dr_reverse_jobs_total`, `replicator_agent_status`, `replicator_agent_active_transfers`, `replicator_agent_transferred_bytes_total`, `replicator_agent_errors_total`, `replicator_agent_heartbeat_age_seconds`, `replicator_agent_max_bandwidth_mb_s`.
- Дашборды Grafana с автоматической загрузкой (provisioning) в `monitoring/grafana/dashboards/`:
  - `hadoop_explorer_overview.json` — состояние и HTTP golden signals всех 5 сервисов платформы.
  - `hadoop_replicator_overview.json` — межкластерная репликация, скорость WAN, иерархическое ограничение полосы Token Bucket Throttler, статусы задач HDFS/HMS, DR Fencing и KPI.
  - `hadoop_replicator_agents.json` — gRPC-агенты: статусы узлов, параллельные воркеры, переданные байты, частота heartbeat и ошибки.
  - `hadoop_replicator_hms_dr.json` — репликация Hive Metastore (Bootstrap/CDC), трансляция путей, Disaster Recovery (Fencing и аварийный Kill-Switch).
  - `hdfs_explorer_operations.json` — файловые операции WebHDFS, сетевой обмен и отказы.
  - `yarn_explorer_queues.json` — Capacity Scheduler, загрузка очередей и состояние Circuit Breaker.
  - `spark_sql_explorer_analytics.json` — интерактивная аналитика Livy Spark и запросы Trino/Hive.

---

## 8. Хранение данных (Storage Layer)

Платформа поддерживает три варианта хранилища:
1. SQLite (JDBC): по умолчанию для автономного запуска и локальных демо-стендов (`jdbc:sqlite:data/<service>.db`).
2. PostgreSQL (JDBC / HikariCP): для production (`jdbc:postgresql://...`). Нужен при нескольких подах сервиса: сессии, воркспейсы и заявки YARN хранятся в одной БД, а блокировки на уровне строк (`SELECT FOR UPDATE`) синхронизируют поды.
3. H2 (in-memory): для модульных и интеграционных тестов (`jdbc:h2:mem:...`).

### 8.1 Инициализация схемы БД (`schema.sql` / JPA)
[проверить: `schema.sql` в `backend/` не найден; Replicator использует миграции Flyway, см. 5.5]

Схему создают Spring Boot и JPA:
- При старте создаются таблицы сессий (`sessions`), воркспейсов (`workspaces`), сохраненных запросов (`saved_queries`), истории вычислений и аудита.
- Одна и та же схема используется для H2, SQLite и PostgreSQL.

---

## 9. Развертывание

1. Docker-образы на базе Eclipse Temurin 21 JRE:
   - Базовый образ `eclipse-temurin:21-jre-jammy` с утилитами Kerberos (`krb5-user`) и `curl` для healthcheck.
   - В образ кладется собранный Spring Boot fat JAR (`backend/<service>/target/*.jar` или `backend/replicator/*/target/*.jar`) со статическими бандлами Svelte 5 SPA.
   - Процесс запускается под непривилегированным пользователем `appuser (UID 10001)`.
   - Параметры памяти JVM: `-Xms256m -Xmx1024m`.
2. JSON-логирование в production:
   - Logback с JSON/Logstash-энкодером пишет логи в JSON, который принимают ELK, Vector, FluentBit и Grafana Loki [проверить: Logstash-энкодер в зависимостях сервисов не найден].
   - В каждую запись попадают `timestamp`, `level`, `logger`, `requestId`, `message` и поля MDC.
3. Kubernetes (Helm):
   - Umbrella-чарт (`helm/hadoop-explorer`): установка всех сервисов одной командой с маршрутизацией через Ingress.
   - Отдельные чарты (`helm/charts/*`): установка компонентов по одному, в том числе в разные неймспейсы.
   - PodDisruptionBudget (`pdb.yaml`): при drain и обслуживании узлов Kubernetes не удаляет все реплики сервиса одновременно.
   - Liveness-проба `/healthz` (`HealthController`, ответ 200 без проверки зависимостей) и readiness-проба `/actuator/health/readiness` (группа readiness Spring Boot Actuator: 503 до `ApplicationReadyEvent` и с начала остановки контекста). Доступность БД и хранилища сессий пробы не проверяют.
   - Метрики Prometheus (`/metrics`): состояние и статистика вызовов Circuit Breaker.
   - Корректное завершение: по SIGTERM сервис дожидается пользовательских операций и останавливает пулы задач.
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
| SQL Explorer | 10 тестов (Java 21) | Catalog API: валидация и эндпоинты, движки Trino/Hive, MockStorage, ИИ-ассистент, токены, CSRF, ролевой доступ к кластерам, User Workspace |
| Spark Explorer | 12 тестов (Java 21) | REST-клиент Apache Livy, проверка DAG на циклы (алгоритм Кана), MockSparkEngine, User Workspace, ролевой доступ, сессии и выполнение statements |
| Hadoop gRPC Replicator | 78+ тестов (Java 21) | Hierarchical Token Bucket, Stateless Active-Active Orchestration, Persistent Agent Registry, Fencing Tokens, Task Attestation HMAC, Chroot Sandbox, DataTransfer и HmsTransfer gRPC, Direct TAR commit и Directory Cache, Schema Registry Single-Flight, smoke-тесты Prometheus и Kafka Active-Active Mirroring |
| Frontend UI | 95 тестов (Vitest) | Компонентные тесты Svelte 5 на Vitest и `@testing-library/svelte` во всех 5 SPA и общем пакете: HDFS, YARN, SQL, Spark, Replicator, Common (Header, LoginModal, Modal, StatusBadge, NotificationToast); проверка типов `svelte-check` |

### 11.2 Тесты отказоустойчивости
1. Circuit Breaker:
   - Проверяются переходы состояний: `CLOSED` → серия сбоев → `OPEN` (вызовы сразу отклоняются, упавший кластер не получает нагрузки) → ожидание `recovery_timeout` → `HALF_OPEN` → успешные пробные вызовы → `CLOSED`.
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

Для каждого сервиса есть руководство администратора по установке в режимах Standalone (systemd), Docker и Docker Compose, Kubernetes (Helm):

- [Общее руководство администратора платформы (admin-guide.md)](admin-guide.md)
- [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)
- [Руководство по Disaster Recovery и защите от split-brain (replicator-disaster-recovery-guide.md)](replicator-disaster-recovery-guide.md)
- [Руководство по отказоустойчивости Replicator (replicator-fault-tolerance.md)](replicator-fault-tolerance.md)

# 🏛️ Архитектура платформы Hadoop Explorer Platform

**Hadoop Explorer Platform** — это масштабируемая корпоративная веб-платформа для интерактивной работы, мониторинга и администрирования экосистемы Apache Hadoop, объединяющая в рамках согласованного монорепозитория пять ключевых инструментов:
1. **YARN Explorer** — консоль администрирования очередей и планирования ресурсов Capacity Scheduler.
2. **HDFS Explorer** — распределенный файловый менеджер.
3. **SQL Explorer** — редактор распределенных аналитических запросов к Trino и Hive с поддержкой ИИ.
4. **Spark Explorer** — веб-студия аналитики и интерактивных вычислений (PySpark, Scala Spark, Spark SQL).
5. **Hadoop gRPC Replicator** — высокопроизводительная система межкластерной репликации HDFS (DC1 → DC2) с глобальным контролем полосы пропускания (Token Bucket), атомарным rename, Snapshot Diff и Kerberos-изоляцией.

---

## 1. Концепция и принципы архитектуры

- **Монорепозиторий с независимой поставкой**: единая кодовая база с разделением на независимые легковесные микросервисы. Любое приложение может быть собрано в отдельный Docker-контейнер или развернуто автономным Helm-чартом без зависимостей от других частей платформы.
- **Единое ядро безопасности и отказоустойчивости (`backend/common-security-starter`, `frontend/common`)**: централизованная реализация аутентификации (Kerberos SPNEGO + LDAPS), сессионный фильтр авторизации, персистентное сессионное хранилище (L1/L2), защита от CSRF, AOP-аудит, контроль частоты запросов (Bucket4j Rate Limiter), Circuit Breaker и Graceful Shutdown.
- **Cookie-First и Zero LocalStorage**: токены авторизации передаются исключительно через защищенные `HttpOnly`, `SameSite=Lax`, `Secure` Cookie. В `localStorage` браузера не сохраняются JWT-токены или чувствительные учетные данные (защита от XSS/CWE-312).
- **Изоляция контекстов и персистентность (`User Workspace`)**: состояние вкладок, написанный код и результаты выполнения изолируются по пользователям и сохраняются в персистентную БД (H2 / PostgreSQL).
- **Высокая доступность и самовосстановление (High Availability & Resilience)**: встроенная поддержка отказоустойчивости для всех кластерных служб (Hadoop NameNode HA, YARN ResourceManager HA, Hive Metastore HA, Kerberos KDC) с защитой Fast-Fail через Circuit Breaker и автоматическим Failover.

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

## 3. Подсистема безопасности платформы (`backend/common-security-starter`)

### 3.1 Аутентификация: Kerberos SPNEGO SSO, LDAPS и единый `AuthController`
1. **Стандартизированный AuthController (`org.apache.hadoop.explorer.common.controller.AuthController`)**:
   - Автоматически предоставляет единый набор эндпоинтов аутентификации (`POST /api/v1/auth/login`, `GET /api/v1/auth/sso`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me`) для всех микросервисов платформы.
   - Поддерживает скользящее продление сессий (Sliding Session), установку безопасных `HttpOnly` Cookie, работу с mock-пользователями и аутентификацию по LDAP/Kerberos.
2. **Единый KerberosManager (`backend.common.core.kerberos`)**:
   - Потокобезопасный класс для управления тикетами Kerberos (инициализация `kinit -kt`, проверка валидности через `klist`, генерация SPNEGO-заголовков `Authorization: Negotiate` для внутренних клиентов WebHDFS, YARN RM и Livy).
   - Валидация Kerberos SPNEGO билетов браузера через GSSAPI и извлечение принципала пользователя (`user@REALM`).
3. **LDAPS / Active Directory**: безопасная проверка пароля через `CommonLdapAuthService` с экранированием фильтров (защита от LDAP Injection, CWE-90) и извлечением групп (`memberOf`).
4. **Mock-режим**: предназначен исключительно для разработки и демонстрационных стендов (`auth.mode: "mock"`). В режиме `debug: false` запуск mock-провайдера категорически блокируется.
5. **Унифицированный Security Middleware**: фабрика `make_get_current_user` гарантирует единообразную валидацию JWT-токенов, проверку серверного отзыва (Revocation check) и преобразование в `CommonUserSession` во всех микросервисах.

### 3.2 Персистентное сессионное хранилище (`SessionStore`)
- Защита от использования отозванных токенов (CWE-613).
- Неблокирующий I/O: методы сессионного хранилища и rate limiter выполняются с потокобезопасным доступом и оптимизацией пулов потоков Java 21.
- Активные сессии сохраняются в таблице `active_sessions` реляционной БД.
- Двухуровневый черный список отозванных токенов:
  - **L1**: сверхбыстрый In-Memory LRU кэш Caffeine (`L1RevokedTokenCache`) с автоматической очисткой по TTL.
  - **L2**: база данных (PostgreSQL / H2 / SQLite) или Redis.
- Защита от **Fail-Open**: при временной недоступности базы данных невалидные токены не пропускаются.
- **Диагностика доступности (`ping`)**: поддержка проверки доступности хранилища для Kubernetes Readiness Probes.

### 3.3 Защита от CSRF и атак на транспорт
- Полная блокировка межсайтовых запросов: анализ заголовка `Sec-Fetch-Site: cross-site`.
- Валидация заголовков `Origin` и `Referer` по строгому белому списку разрешенных доменов (`server.cors_origins`). Устранено невалидированное доверие заголовку `Host` (Host Header Injection).
- Обязательное требование заголовка `X-Requested-With` для асинхронных API вызовов.

### 3.4 Rate Limiting и аудит
- Алгоритм скользящего окна / Token Bucket (`Bucket4j`) с потокобезопасной проверкой и поддержкой распределенных хранилищ.
- Определение реального IP-клиента с защитой от спуфинга заголовка `X-Forwarded-For` через список доверенных прокси (`trusted_proxies`).
- Структурированное JSON-логирование критических событий безопасности (`AUDIT_LOGIN_SUCCESS`, `AUDIT_LOGIN_FAILURE`, `AUDIT_TOKEN_REVOKED`, `AUDIT_CSRF_REJECT`).

### 3.5 Content-Security-Policy (CSP) и защитные HTTP-заголовки
Централизованный фильтр безопасности гарантирует соблюдение современных стандартов защиты веб-клиента:
- **Content-Security-Policy (CSP)**:
  - **Базовая строгая политика (`CSP_DEFAULT_DIRECTIVES`)**: применяется для YARN и HDFS Explorer (`default-src 'self'`, `script-src 'self'`, `style-src 'self' 'unsafe-inline'`, `img-src 'self' data:`, `font-src 'self'`, `connect-src 'self'`, `frame-ancestors 'none'`, `object-src 'none'`, `base-uri 'self'`).
  - **Политика для редакторов кода (`CSP_CODE_EDITOR_DIRECTIVES`)**: применяется для SQL и Spark Explorer для безопасного функционирования Monaco Editor и Web Workers (`worker-src 'self' blob:`, `script-src 'self' 'unsafe-eval' blob:`, `connect-src 'self' ws: wss: http: https:`, `img-src 'self' data: blob:`).
  - Дублирование CSP в `index.html` через `<meta http-equiv="Content-Security-Policy">` для защиты статических файлов при независимой раздаче.
- **Защитные заголовки**:
  - `X-Frame-Options: DENY` — абсолютная защита от Clickjacking.
  - `X-Content-Type-Options: nosniff` — блокировка MIME-sniffing атак.
  - `Referrer-Policy: strict-origin-when-cross-origin` — ограничение передачи заголовка Referer на сторонние ресурсы.
  - `Strict-Transport-Security: max-age=31536000; includeSubDomains` (HSTS) — принудительный переход на HTTPS при включенной опции `secure_cookies`.

### 3.6 Стартер безопасности платформы (`backend/common-security-starter`)
Все сервисы платформы используют стартер `org.apache.hadoop.explorer:common-security-starter`:
- **Spring Boot 3 AutoConfiguration**: автоматическая регистрация `SecurityFilterChain`, `CommonAuthFilter`, CORS, TLS Customizer и контроллеров через `org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- **Kerberos SPNEGO SSO**: реализация нативного Java GSS-API (`org.ietf.jgss`) без сторонних библиотек.
- **LDAPS / Active Directory**: безопасная аутентификация с экранированием фильтров (CWE-90).
- **SessionStore L1/L2**: двухуровневый кэш (L1 Caffeine LRU + L2 JDBC H2/PostgreSQL) с отзывом токенов и защитой от Fail-Open.
- **CSRF Guard**: строгая проверка `Sec-Fetch-Site: cross-site`, `X-Requested-With: XMLHttpRequest` и белого списка `Origin`/`Referer`.
- **Resilience & Audit**: Token Bucket Rate Limiter (`Bucket4j`), `SimpleCircuitBreaker` и структурированный JSON-аудит через AOP `@Audited`.
- **Сквозной TLS/HTTPS и mTLS**: кастомизация встроенного веб-сервера Tomcat через `TlsWebServerCustomizer`, поддержка PKCS12 / JKS хранилищ, взаимная проверка клиентских сертификатов и фабрика `TlsContextFactory` для исходящих клиентов.
- **Стандартизированные эндпоинты**: контроллер `AuthController` (`/api/v1/auth/login`, `/sso`, `/logout`, `/me`).

### 3.7 Сквозная криптографическая защита каналов (TLS / mTLS для REST и gRPC)
В платформе реализована единая криптографическая защита всех сетевых каналов передачи данных:
1. **REST / HTTP каналы платформы**:
   - Веб-серверы Spring Boot (`yarn`, `hdfs`, `sql`, `spark`, `replicator/orchestrator`) активируют HTTPS через `hadoop.security.tls.enabled=true`.
   - Поддерживаются протоколы TLSv1.3 и TLSv1.2, настраиваемые списки шифров, а также режим взаимной аутентификации (**mTLS** via `client-auth: REQUIRE`).
   - Исходящие HTTP-клиенты (`HttpClient` в `NativeYarnClient`, `AwxClientService`, `OrchestratorClient`) используют фабрику `TlsContextFactory` с поддержкой доверенных CA и флага `insecureSkipVerify` для тестовых стендов.
2. **gRPC каналы репликации данных**:
   - Сервер-приемник `DataTransferServiceImpl` в `backend/replicator/agent` конфигурируется через `NettyServerBuilder` с `SslContextBuilder`, поддерживая X.509 сертификаты, приватные ключи, связки CA и режим `clientAuth: REQUIRE` (mTLS).
   - Клиент-отправитель `ReplicationSender` устанавливает защищенные TLS-сессии через `NettyChannelBuilder.forTarget(...)` со схемой `grpcs://` или флагом `REPLICATOR_GRPC_TLS_ENABLED=true`.
   - В dev/демо окружениях поддерживается автоматическая генерация сертификатов через `SelfSignedCertificate` и `BouncyCastle`.

---

## 4. Отказоустойчивость и надежность (Resilience Core)

### 4.1 Расширенная система метрик Prometheus и Grafana Dashboards (`backend.common.core.metrics`)
Все микросервисы платформы оснащены встроенным легковесным коллектором метрик OpenMetrics / Prometheus и `PrometheusMetricsMiddleware` для всесторонней наблюдаемости:
- **HTTP Golden Signals**:
  - `http_requests_total{app="...", method="...", path="...", status="..."}`: счетчик запросов с нормализацией путей по шаблонам роутов (предотвращение взрыва кардинальности по динамическим ID).
  - `http_request_duration_seconds`: гистограмма задержек p50, p90, p99 с бакетами от `5ms` до `10s`.
  - `http_requests_in_progress{app="..."}`: уровень параллелизма / активные запросы в обработке (concurrency).
- **Метрики отказоустойчивости и сетевых вызовов**:
  - `hadoop_circuit_breaker_state{name="..."}`: автомат состояний (0=CLOSED, 1=HALF_OPEN, 2=OPEN) для NameNode, ResourceManager, Livy.
  - `hadoop_circuit_breaker_calls_total{name="...", status="success|failed|rejected"}`: учет успешных, сбойных и заблокированных по Fast-Fail вызовов.
  - `hadoop_retry_attempts_total{app="...", operation="...", status="retry|exhausted|success"}`: срабатывания механизма повторных попыток.
- **Метрики безопасности и ошибок**:
  - `hadoop_auth_attempts_total{app="...", provider="ldap|kerberos|mock", status="success|failure"}`: мониторинг входов и сбоев каталога.
  - `hadoop_rate_limit_blocks_total{app="..."}`: учет срабатываний Rate Limiter (HTTP 429).
  - `hadoop_exceptions_total{app="...", exception_type="..."}`: непредвиденные 500 ошибки (CWE-209).
- **Grafana Dashboard**:
  - Готовый JSON-дашборд в [`monitoring/grafana/dashboards/hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) и конфигурация автопровижининга [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml) для визуализации всех сервисов платформы.

### 4.2 Circuit Breaker (`backend.common.core.circuit_breaker`)
Для предотвращения каскадных сбоев при недоступности внешних кластеров Hadoop (NameNode, YARN RM, Livy) все исходящие сетевые клиенты защищены автоматом состояний `CircuitBreaker`:
- **Состояния**: `CLOSED` (нормальная работа) → `OPEN` (кластер недоступен, вызовы сразу отклоняются с `CircuitBreakerOpenException` без ожидания сетевых таймаутов) → `HALF_OPEN` (пробная отправка запроса для проверки восстановления кластера).
- **Фильтрация ошибок**: клиентские HTTP-ошибки (4xx) считаются легитимными ответами и не приводят к срабатыванию автомата; учитываются только сетевые ошибки, сбои подключения, таймауты и серверные 5xx.
- **HA Failover**: автоматическое переключение на резервный узел (Standby NameNode / Standby ResourceManager) при фиксации сбоя на Active-узле.
- **Интеграция с Kubernetes Readiness Probe (`/readyz`)**: если все внешние коннекторы кластера переходят в статус `OPEN`, эндпоинт `/readyz` возвращает `503 Service Unavailable` (`status: degraded`), исключая отправку клиентского трафика на деградировавший под.

### 4.3 Модуль повторных попыток с экспоненциальным Backoff
Для обработки кратковременных транзиентных сбоев сети и сокетов:
- Встроенные механизмы повторных попыток с экспоненциальным ростом интервала задержки (`backoff_factor`) и случайным джиттером (`jitter`) для исключения эффекта «громоподобного стада» (Thundering Herd).
- Настраиваемый список повторяемых сетевых исключений (`java.net.http.HttpConnectTimeoutException`, `java.io.IOException`) и исключение критических ошибок бизнес-логики.

### 4.4 Централизованные обработчики исключений (`@ControllerAdvice`)
- Защита от раскрытия чувствительной информации и структуры бэкенда при 500-х ошибках (CWE-209).
- Глобальный обработчик `@ControllerAdvice` (`GlobalExceptionHandler`) перехватывает необработанные ошибки, генерирует уникальный `incidentId`, детально логирует инцидент внутри сервера и возвращает клиенту безопасный структурированный JSON-ответ.
- Автоматическая трансляция состояния `CircuitBreakerOpenException` в HTTP 503 с заголовком `Retry-After`.

### 4.5 Distributed Lock (`backend.common.core.lock`)
Для предотвращения состояний гонки (Race Conditions) при параллельных операциях:
- **Трехуровневая стратегия блокировок**:
  1. **Redis**: атомарный `SET key owner_id NX PX` с безопасным освобождением через Lua-скрипт (`REDIS_RELEASE_LUA`).
  2. **DB-backed блокировка на уровне строк (PostgreSQL / SQLite)**: таблица `distributed_locks` в `SessionStore` с транзакционным `SELECT ... FOR UPDATE` (PostgreSQL) и атомарными операциями `INSERT`/`UPDATE` с проверкой `expires_at` и `owner_id`. Обеспечивает межпроцессную и межподовую синхронизацию даже при отсутствии Redis.
  3. **In-Memory Fallback**: локальный потокобезопасный словарь с TTL для сред разработки.
- Применяется в YARN Explorer для защиты согласования и отклонения заявок на изменение конфигурации очередей (Change Requests).

### 4.6 Корректное завершение (Graceful Shutdown)
Все микросервисы платформы на Spring Boot 3 поддерживают штатное завершение работы (`server.shutdown=graceful`):
- Перехват сигналов завершения подов Kubernetes (`SIGTERM`, `SIGINT`).
- Дожидание завершения выполняющихся запросов (таймаут `spring.lifecycle.timeout-per-shutdown-phase`).
- Корректная остановка фоновых пулов задач и потоков (`ExecutorService.shutdown()`).
- Закрытие пулов соединений с базами данных (HikariCP pool close) и освобождение сетевых ресурсов.


---

## 5. Архитектура сервисов платформы

### 5.1 YARN Explorer
- **Нативный Java 21 LTS / Spring Boot 3 бэкенд (`backend/yarn/`)**: высокопроизводительный сервис на базе официальных библиотек Apache Hadoop YARN Client (`hadoop-yarn-client:3.3.6`, `hadoop-yarn-common:3.3.6`), RM HA failover (`haState == "ACTIVE"`), парсинг и валидация дерева очередей (правило 100% емкости веток), расчет diff изменений для ресурсов RAM/vCPU и node labels, генерация и XXE-защищенная санитизация `capacity-scheduler.xml`, полный жизненный цикл Change Requests (Four-Eyes Principle, Spring Data JPA / H2), интеграция с Ansible AWX REST API (`POST /api/v2/job_templates/{id}/launches/`), интеграция с общим ядром безопасности `common-security-starter` и автономный `MockYarnClient` для изолированного тестирования.
- **Мониторинг очередей**: визуализация дерева иерархии Capacity Scheduler, метрик загрузки памяти и ядер в реальном времени с поддержкой RM HA и Circuit Breaker.
- **Моделирование и валидация**: проверка корректности весов очередей (правило 100% емкости, минимальные/максимальные лимиты пользователя).
- **Change Requests (Four-Eyes Principle)**: процесс внесения изменений через создание заявок инженерами данных (`WRITER`) и их обязательное согласование администраторами (`ADMIN`) под защитой `DistributedLock`.
- **Генерация XML**: экспорт готовой валидной конфигурации `capacity-scheduler.xml` в процентном и абсолютном форматах.
- **Автоматизированная доставка и Zero-Downtime применение (Ansible AWX)**:
  - Интеграция с корпоративной платформой автоматизации **Ansible AWX / Red Hat AAP** через асинхронный клиент `AwxClient`.
  - Запуск Job Template по REST API с передачей Base64-кодированной конфигурации в `extra_vars` (`capacity_scheduler_xml_b64`).
  - Синхронное резервное копирование и доставка файлов на все ноды ResourceManager (Active и Standby) с корректными правами `0644 yarn:hadoop`.
  - Горячее применение конфигурации без перезапуска демонов через `yarn rmadmin -refreshQueues` (с поддержкой Kerberos-аутентификации).
  - **Автоматический откат (Rollback)**: в случае ненулевого кода возврата команды `refreshQueues` Ansible-роль `yarn_capacity_scheduler` в блоке `rescue` восстанавливает timestamped-бэкап и повторно применяет очереди, гарантируя бесперебойность кластера.
  - Потоковое получение логов выполнения плейбука (`stdout`) прямо в веб-интерфейсе платформы.
  - *Детальная спецификация и sequence-диаграмма: [docs/awx-yarn-deployment.md](awx-yarn-deployment.md).*

### 5.2 HDFS Explorer
- **Нативный Java 21 LTS / Spring Boot 3 бэкенд (`backend/hdfs/`)**: высокопроизводительный сервис на базе официальных библиотек Apache Hadoop (`org.apache.hadoop:hadoop-hdfs-client:3.3.6`), полная поддержка High Availability NameNode (автоконфигурация `dfs.nameservices` и `ConfiguredFailoverProxyProvider`), Kerberos Proxy User `doAs` имперсонации с прозрачностью для Ranger Audit, потоковый предпросмотр Parquet и ORC без выгрузки файлов целиком в память (`parquet-hadoop`, `orc-core`), интеграция с общим ядром безопасности `common-security-starter` и in-memory эмулятор `MockHdfsClient` для автономного тестирования.
- **Имперсонация (`doAs`)**: выполнение файловых операций от имени аутентифицированного пользователя при наличии привилегий у сервисного аккаунта.
- **Неблокирующая архивация (Non-blocking ZIP)**: упаковка и распаковка директорий в ZIP-архивы с выносом ресурсоемких операций сжатия и чтения в пул рабочих потоков, защита от DoS/OOM и ликвидация N+1 задержек.
- **Предпросмотр данных**: потоковое чтение и конвертация форматов CSV/TSV, JSON, текстовых файлов и бинарных колоночных форматов Apache Parquet / Apache ORC.
- **Кросс-кластерное копирование**: прямая потоковая передача файлов и каталогов между независимыми кластерами HDFS.


### 5.3 SQL Explorer
- **Мульти-движок**: одновременная работа с распределенным движком Trino (Trino DB-API) и Apache Hive (HiveServer2 / TCLIService Thrift).
- **TTL-кэширование метаданных (`MetadataTTLCache`)**: кэширование каталогов, схем, таблиц и колонок со сбросом по `?refresh=true`.
- **Fail-Closed AST Linter**: синтаксический анализ запросов через `sqlglot` со строгой блокировкой деструктивных команд (DML/DDL) и безопасным отклонением некорректных конструкций.
- **Crash Recovery**: автоматическое завершение осиротевших SQL-запросов предыдущего процесса при старте сервиса.
- **ИИ-ассистент**: интеграция с локальными On-Premise LLM моделями (через vLLM, Ollama или LiteLLM) для генерации SQL по естественному языку, объяснения планов запросов, оптимизации и автоисправления синтаксических ошибок.
- **Персистентность**: сохранение открытых вкладок и запросов пользователя в БД (`SqlUserWorkspace`).

### 5.4 Spark Explorer
- **Интеграция с Apache Livy**: диспетчеризация интерактивных сессий и пакетных заданий на YARN через Livy REST API с защитой от сбоев и автоматической очисткой сессий при выходе пользователя (`POST /api/v1/auth/logout`).
- **Единая UI-компоновка (согласована с SQL Explorer)**:
  - **Глобальный SparkSessionWidget в Header**: вынос управления активной Livy-сессией в шапку приложения (выбор кластера, статус сессии, конфигурация памяти/ядер драйвера и экзекуторов, кнопка перезапуска/остановки).
  - **Верхняя панель вкладок**: удобная организация пользовательских скриптов и ноутбуков.
  - **Локальный тулбар вкладки (`SessionBar`)**: селектор активного языка (PySpark / Scala / SQL), кнопки запуска (`Run`, `Ctrl+Enter`) и прерывания (`Cancel`), таймер выполнения и экспорт результатов.
- **Поддержка трех языков**:
  - **PySpark**: интерактивный Python с возможностью использования изолированных сред Conda/Venv, упакованных в HDFS (`spark.archives`).
  - **Scala Spark**: интерактивная REPL-сессия с поддержкой подключения кастомных JARs и Maven-библиотек.
  - **Spark SQL**: выполнение ANSI SQL запросов с привязкой к таблицам Hive Metastore / Apache Iceberg.
- **TTL-кэширование метаданных (`SparkMetadataTTLCache`)**: потокобезопасный кэш баз данных, таблиц и колонок с настраиваемым TTL (60с) и поддержкой принудительного обновления `?refresh=true` для защиты Hive Metastore и Spark-сессий от шквала запросов автодополнения (IntelliSense).
- **Crash Recovery**: автоматический перевод зависших задач (`RUNNING`, `QUEUED`) в статус `FAILED` при старте пода.
- **Изоляция буферов результатов**: в рамках каждой вкладки раздельно сохраняются буферы кода (`codeBuffers`) и буферы результатов (`resultBuffers`). При переключении языков результаты вычислений не теряются и не перемешиваются.
- **Синхронизация воркспейса**: сохранение вкладок и состояния в БД (`SparkUserWorkspace`).

### 5.5 Hadoop gRPC Replicator
- **Назначение**: высокоскоростная межкластерная репликация данных между географически распределенными HDFS-кластерами (DC1 → DC2 → DC3) с защитой сетевого периметра и многоуровневым контролем полосы пропускания.
- **Топология дата-центров (DC) и кластеров HDFS**:
  - Декларативная карта площадок (на стенде: 2 ЦОД — Москва и Санкт-Петербург) и привязка каждого HDFS-кластера к конкретному ЦОД (в DC1 два кластера, в DC2 один кластер). Подробнее см. [Руководство по конфигурации](CONFIGURATION.md#7-настройка-hadoop-grpc-replicator-dc-dc-wan-sync--throttling).
  - Сетевые лимиты на трех уровнях:
    1. **DC-DC WAN Limits**: ограничение межЦОДных магистральных каналов (`DC1 ➔ DC2`: 100 МБ/с).
    2. **HDFS-HDFS Limits**: выделенные квоты между парами кластеров (`demo ➔ backup`: 60 МБ/с, `analytics ➔ backup`: 40 МБ/с, внутри DC1: 80 МБ/с).
    3. **Global WAN Cap**: общий пул пропускной способности всей инфраструктуры (120 МБ/с).
- **Многоуровневый Token Bucket Throttling (Hierarchical Token Bucket)**:
  - Потокобезопасный контроллер квот `TokenBucketThrottler` (`backend/replicator/orchestrator/src/main/java/.../TokenBucketThrottler.java`).
  - При запросе передачи чанка проверяются все применимые бакеты, а задержка воркера вычисляется по узкому горлышку: `max(wait_global, wait_dc_dc, wait_hdfs_hdfs)`.
  - Возможность динамического изменения любых лимитов в реальном времени через REST API и веб-консоль.
- **gRPC Транспорт (DC1 Worker → DC2 Receiver)**:
  - Бинарный потоковый контракт Protobuf (`replicator.proto` -> `DataTransferService.TransferFile`).
  - Потоковая передача чанками фиксированного размера (по умолчанию 4 МБ) со сквозным вычислением контрольной суммы SHA-256.
  - Атомарность фиксации (Commit/Rename): Receiver принимает данные во временную staging-директорию и атомарно перемещает в целевой путь только после завершения потока и совпадения контрольной суммы.
- **Аутентификация и ролевая модель (RBAC)**:
  - Поддержка Kerberos SPNEGO SSO, LDAP и тестовых профилей (`admin_user`, `de_user`, `analyst_user`).
  - **Администратор (`admin_user`)**: видит и управляет всеми задачами репликации всех пользователей организации.
  - **Пользователь (`USER`)**: видит только свои персональные задачи (`created_by == current_user.username`) и системные фоновые процессы.
- **Шедулер задач (Cron Scheduler)**:
  - Встроенный планировщик Spring Scheduling (`ReplicationScheduler.java`) для периодической синхронизации по расписанию (`@every_5m`, `@hourly`, `@daily`, custom cron).
  - Автоматический расчет времени следующего запуска `next_run_at`.
- **Инкрементальная репликация каталогов и Snapshot Diff**:
  - **Рекурсивная синхронизация без снэпшотов (Batch Manifest Diff, основной режим)**: опрос HDFS локализован строго на агентах в своих ЦОД (оркестратор не обращается к HDFS). Агент-источник запрашивает удаленный манифест целевого каталога одним gRPC-вызовом `GetDirectoryManifest`, выполняет мгновенный diff деревьев в памяти (O(N) in-memory map lookup), инкрементально пропускает идентичные файлы (0 байт сетевого трафика WAN) и передает только дельту.
  - **Дифференциальная синхронизация HDFS Snapshot Diff**: управление жизненным циклом снимков агентом (`HadoopFsManager.createSnapshot` / `deleteSnapshot`), генерация гранулярных операций (`ADD`, `MODIFY`, `DELETE`, `RENAME`) для snapshottable директорий с миллионами файлов.
- **Распределенный пул пофайловых задач (Distributed Task Queue & Multi-Agent Scaling)**:
  - **Снятие ограничения одиночного воркера (1 PB+ Scaling)**: двухфазная модель исполнения. Фаза анализатора (`ANALYZING`) быстро строит in-memory diff и регистрирует пул подзадач `TaskEntity` в Оркестраторе (`/tasks/batch`), пропуская идентичные файлы (`SKIPPED`).
  - **Параллельный разбор пула воркерами**: все доступные агенты кластера-источника (DC1) параллельно выгребают задачи (`/tasks/claim`) и стримят файлы в целевой кластер с автоматической балансировкой нагрузки между онлайн-приемниками (Least-Loaded Target Balancing). Оркестратор агрегирует общий объем переданных данных родительской задачи.
  - **Роли агентов (`AGENT_MODE`)**: поддержка режимов `all` (универсальный), `analyzer` (выделенный координатор анализа), `worker` (воркер параллельной передачи файлов) и `receiver` (приемник).
- **Автоматическая фильтрация временных и служебных структур (Temp & Commit Files Filter)**:
  - Встроенный в `HadoopFsManager` и `ReplicationSender` предикат исключения временных структур при сканировании HDFS/FS и репликации (`_temporary`, `.spark-staging-*`, `.staging`, `.Trash`, `.tmp`, `.hive-staging*`, `lost+found`, `*.inprogress`, `*.copying`, `_copying_`, `.DS_Store`, `Thumbs.db`).
  - Оптимизированный обход дерева каталогов с пропуском поддеревьев (`SKIP_SUBTREE`), исключающий лишние RPC-запросы к NameNode и дисковый I/O.
  - Сохранение критически важных маркеров коммита и файлов метаданных (`_SUCCESS`, `_SUCCESS.crc`, `_metadata`, `_common_metadata`).
  - Двусторонняя защита: исключение из распределенного пула задач на стороне источника (Sender/Analyzer) и исключение из удаленного манифеста на стороне приемника (Receiver).
- **Безопасность, Kerberos-изоляция и имперсонация (Apache Ranger)**:
  - Менеджер файловой системы `HadoopFsManager`: аутентификация системной техучетки по keytab (`hdfs-replicator@REALM.LOCAL`).
  - **Hadoop Proxy User & doAs имперсонация**: агент подключается от доверенной техучетки, выполняя операции с HDFS под UGI пользователя (`UserGroupInformation.createProxyUser(user, baseUgi).doAs(...)`). Это гарантирует строгую проверку политик доступа в Apache Ranger и корректную фиксацию в Ranger Audit Log (`ugi: user (auth:PROXY via hdfs-replicator)`).
- **Мониторинг, Web UI и Управление задачами**:
  - Экспорт метрик Prometheus (`/actuator/prometheus`) на `:8005`.
  - Встроенный высококонтрастный веб-интерфейс в дизайн-системе HDFS Explorer с модалкой аутентификации, селектором кластеров и ЦОД, и управлением полосой в рантайме.
  - Полнофункциональное управление задачами (REST API и Web UI): запуск/перезапуск (`POST /api/v1/jobs/{id}/start`), остановка/отмена (`POST /api/v1/jobs/{id}/stop`), редактирование параметров на лету (`PUT /api/v1/jobs/{id}`) и удаление (`DELETE /api/v1/jobs/{id}`) с каскадной очисткой подзадач.
  - Двухфазная распределенная репликация (Distributed Task Pool): фаза анализа с регистрацией пула сабтасок (`/api/v1/jobs/{id}/tasks/batch`) и параллельная фаза воркеров (`claimTasks`). Подзадачи полностью инкапсулированы внутри родительской задачи `Job`; родительское задание переходит в терминальный статус (`COMPLETED`/`FAILED`) строго после закрытия всех сабтасок с накоплением счетчиков объектов (`total_objects`, `transferred_objects`, `skipped_objects`, `failed_objects`), объемов и средней скорости.
- **Репликация Hive Metastore (HMS Replication)**:
  - Выделенный **Раздел «HMS Replication»** для межкластерной синхронизации метаданных баз и таблиц (HDP 3.1 ➔ Apache Hive 3.1.3).
  - **Изоляция подзадач**: задачи переноса HDFS для партиций создаются со статусом `job_type = 'HMS_SUBJOB'` и полностью скрыты из регламентного раздела «HDFS Replication».
  - **Non-ACID Gate**: реплицируются External таблицы и Managed Non-Transactional таблицы (`MANAGED_TABLE`, `transactional != true`); ACID-таблицы безопасно фильтруются (`SKIPPED_ACID`).
  - **HDFS Federation**: динамический парсинг NameService в `sd.location` партиций и маршрутизация по таблице соответствия `federation-mappings` с сохранением кластерных квот Token Bucket.
  - **Полный Bootstrap и потоковый CDC**: первичный экспорт структуры таблиц/партиций с автоматическим переходом в режим потокового чтения `NOTIFICATION_LOG`.
  - **Безопасное удаление**: операции `DROP` выполняются в целевом HMS строго с параметром `deleteData = false`, сохраняя файлы в HDFS.
- **Нативная Java 21 экосистема исполнения**:
  - **Java 21 / Spring Boot 3 Orchestrator (`backend/replicator/orchestrator`)**: высокопроизводительный нативный оркестратор с интеграцией `common-security-starter`, Spring Data JPA, потокобезопасным `TokenBucketThrottler`, SSRF-защищенным `AgentRegistry`, cron-шедулингом и раздачей собранного Svelte 5 SPA.
  - **Нативный Java 21 Agent (`backend/replicator/agent`)**: высокоскоростной полнодуплексный воркер для DataNode и контейнеров Apache Hadoop YARN.
  - **Единый мультимодульный Maven-проект (`backend/replicator/pom.xml`)**: связывает `agent` и `orchestrator` с общим циклом компиляции и тестирования (`make test-replicator`).

### 5.6 Архитектура и оптимизация Frontend (Svelte 5 & Tailwind 4)
Клиентская часть всех приложений построена на базе Svelte 5 с использованием системы реактивности Runes (`$state`, `$derived`, `$effect`):
- **Виртуализация списков (Virtual Windowing)**:
  - Компонент `FileList.svelte` в HDFS Explorer реализует легковесную виртуализацию с высотой строки `ROW_HEIGHT = 37px` и запасом рендеринга `OVERSCAN = 12`.
  - Динамический расчет диапазона видимости `[startIndex, endIndex]` на основе `scrollTop` контейнера и `topSpacerHeight` / `bottomSpacerHeight` обеспечивает плавный скроллинг и мгновенную работу с каталогами, содержащими десятки тысяч файлов (O(1) DOM-узлов).
  - Sticky-позиционирование шапки таблицы (`sticky top-0 z-10`) и автосброс скролла в 0 при переходе по директориям.
- **Lazy Loading модальных окон (Code-Splitting)**:
  - Все тяжелые модальные окна, мастера настроек и выдвижные панели (Drawers) загружаются асинхронно по требованию через `{#await import(...) then { default: Component }}`.
  - Это минимизирует первоначальный размер JavaScript-бандла (Time-to-Interactive) и ускоряет первую отрисовку страниц.
- **Единый пакет интерфейсных компонентов (`@hadoop-explorer/common`)**:
  - `Header.svelte` — централизованная шапка с профилем пользователя, отображением LDAP-групп/ролей, селектором кластеров и кнопкой выхода (`Logout`) для всех SPA-приложений платформы.
  - `LoginModal.svelte` — стандартизированный диалог аутентификации с поддержкой Kerberos SPNEGO SSO, входа по учетной записи LDAP и быстрого переключения mock-пользователей.
  - `Modal.svelte` — базовый переиспользуемый компонент диалогового окна с backdrop-blur, закрытием по Escape/клику вне окна и доступностью.
  - `sqlSplitter.ts` — общий парсер и анализатор SQL-скриптов с поддержкой строковых литералов, комментариев и выполнения запроса под курсором (`getStatementAtCursor`).
  - `useResizable.svelte.ts` — Svelte 5 runes хелперы для плавного Drag & Drop изменения размеров сплиттеров (сайдбары и редакторы кода).
- **Унифицированный API-клиент (`BaseApiClient`)**:
  - Все клиенты приложений (`YarnApiClient`, `hdfs/client.ts`, `ApiClient` в SQL, `SparkApiClient`) унаследованы от общего `BaseApiClient` из `@hadoop-explorer/common`.
  - Централизованная обработка HTTP 401 с прозрачной попыткой Kerberos SSO (`/auth/sso`), защита от CSRF (`X-Requested-With`), `credentials: 'include'` и поддержка Sliding Sessions.
- **Строгая типизация TypeScript (`frontend/common/types/`)**:
  - Строгие TypeScript-интерфейсы синхронизированы с DTO-моделями Java 21 сервисов платформы (`yarn`, `hdfs`, `sql`, `spark`, `replicator`).
  - Это исключает расхождения контрактов данных (Data Drift) между Java Record/Class бэкенда и фронтенд-клиентом.

---

### 3.10 Асимметричные JWT (RS256/ES256), ротация ключей и JWKS (RFC 7517)
В `backend/common-security-starter` реализован `JwtKeyManager`:
- Поддержка асимметричной подписи токенов RSA (`RS256`) и ECDSA (`ES256`) с автоматическим добавлением идентификатора ключа `kid` в JWT header.
- Бесшовная ротация ключей: сохранение истории публичных ключей для непрерывной валидации ранее выпущенных токенов при выпуске нового активного ключа подписи.
- Эндпоинты `GET /api/v1/auth/jwks.json` и `GET /.well-known/jwks.json`, экспортирующие набор открытых ключей в стандартном формате RFC 7517 для интеграции с внешними API Gateway, Service Mesh и OAuth2/OIDC сервисами.

---

## 4. Оптимизации производительности и потоковой передачи данных

### 4.1 Высокопроизводительный пул соединений с поддержкой HTTP/2
Инфраструктура HTTP-клиентов на базе нативного Java 21 `HttpClient` и пулов соединений:
- Поддержка мультиплексирования HTTP/2 с автоматическим graceful fallback на HTTP/1.1 при неподдерживаемых бэкендах.
- Пул соединений с тонкой настройкой таймаутов подключения, чтения и keepalive-сессий.
- Снижение latency и накладных расходов на TLS/TCP handshakes при частых опросах кластерных API (YARN RM, WebHDFS, Livy).

### 4.2 SSE-стриминг состояния сессий и запросов
В сервисах SQL и Spark реализована потоковая доставка обновлений по протоколу Server-Sent Events:
- `GET /api/v1/sessions/{session_id}/stream` (Spark) и `GET /api/v1/queries/{query_id}/stream` (SQL).
- Устраняет необходимость агрессивного HTTP-поллинга со стороны фронтенда, снижая нагрузку на сеть и серверные ресурсы.

### 4.3 Zero-Copy частичное чтение и превью Parquet / ORC файлов
Сервис HDFS Explorer реализует потоковый адаптер `_SeekableFooterStream`:
- Извлекает метаданные схемы и статистику из хвостовой части файла (Footer) за один запрос без скачивания всего объема данных.
- При запросе сэмпла строк для усеченных файлов выполняет прямое чтение `Row Group 0` (Parquet) или `Stripe 0` (ORC), возвращая репрезентативную выборку данных за миллисекунды даже для файлов размером в сотни гигабайт.

### 4.4 Пакетные операции HDFS Explorer
Для оптимизации массовых файловых манипуляций добавлены пакетные API:
- `POST /api/v1/clusters/{cluster_id}/files/batch-delete`: параллельное удаление множества файлов и директорий с детализированным отчетом об успехах и ошибках.
- `POST /api/v1/clusters/{cluster_id}/files/batch-download`: потоковая упаковка выбранного набора файлов и каталогов в единый ZIP-архив на лету без предварительного сохранения на диск сервера.

### 4.5 Условное ETag-кэширование (HTTP 304 Not Modified)
Spring Boot фильтр `ShallowEtagHeaderFilter`:
- Автоматически рассчитывает слабые ETag-хэши (`W/"..."`) для всех безопасных GET/HEAD ответов API.
- Обрабатывает входящий заголовок `If-None-Match`, возвращая легковесный `HTTP 304 Not Modified` без тела ответа при неизменности данных (каталоги метаданных Hive, неизменные очереди YARN, списки кластеров).

### 4.6 Визуальный конструктор и планировщик Spark DAG-пайплайнов
В Spark Explorer интегрирован движок и UI визуального конструирования пайплайнов (`backend.spark.app.models.pipeline`, `PipelineBuilder.svelte`):
- Поддержка гетерогенных узлов графа: PySpark скрипты, Spark SQL запросы, задачи Spark Submit.
- Строгая топологическая валидация DAG по алгоритму Кана (Kahn's Algorithm) для гарантированного исключения циклических зависимостей (ошибка `422 Unprocessable Content`).
- Асинхронное исполнение узлов и пошаговый мониторинг статусов выполнения в реальном времени.

### 4.7 Дисковый кэш аналитических результатов и автоматическая ротация по TTL (`data/results/`)
Для снижения нагрузки на кластерные движки (Trino, Hive, Livy) результаты выполнения запросов сохраняются в сжатом виде (gzip JSON: `{id}.json.gz`) в единой корневой директории `data/results/`:
- **SQL Explorer**: кэширует выборки строк и схемы колонок для мгновенной пагинации, переключения вкладок редактора и экспорта в CSV/JSON.
- **Spark Explorer**: кэширует спарсенные табличные результаты вычислений statements в сессиях Livy.
- **Автоматическая ротация (TTL Cleanup)**:
  - Оба сервиса реализуют метод `cleanup_expired_results()`, анализирующий время последней модификации файлов (`mtime`).
  - Файлы старше `RESULTS_TTL_SECONDS` (по умолчанию 7 дней / `604800` с) автоматически удаляются.
  - Очистка выполняется как при старте сервисов, так и в периодических фоновых воркерах с настраиваемым интервалом `RESULTS_CLEANUP_INTERVAL_SECONDS` (по умолчанию 1 час / `3600` с).

---

## 5. Наблюдаемость и мониторинг (Observability)

### 5.1 Распределенная трассировка OpenTelemetry & W3C Trace Context
Модуль `backend.common.core.tracing`:
- Полноценная поддержка спецификации W3C Trace Context (`traceparent` формата `00-<trace_id>-<span_id>-01`).
- `OpenTelemetryMiddleware` автоматически связывает спаны с контекстным `X-Request-ID` и пробрасывает заголовки трассировки клиентам и дочерним микросервисам платформы.
- Контекстный менеджер `tracer.span(...)` для профилирования межсервисных вызовов к кластерам Hadoop, YARN, Trino и Livy.

### 5.2 Специализированные Prometheus метрики и Grafana дашборды
В дополнение к базовым HTTP Golden Signals реализован расширенный сбор метрик в `MetricsRegistry`:
- **YARN**: `yarn_queues_active_gauge`, `yarn_change_requests_total`.
- **Spark**: `spark_sessions_active_gauge`, `spark_statements_total`, `spark_statement_duration_seconds`.
- **SQL**: `sql_queries_total`, `sql_query_duration_seconds`.
- **HDFS**: `hdfs_operations_total`, `hdfs_bytes_transferred_total`.
- **Replicator**: `replication_bytes_total`, `active_workers`, `replication_jobs_total`, `throttling_delay_seconds_total`, `replication_file_size_bytes`.
- Готовые provisioning-дашборды Grafana в `monitoring/grafana/dashboards/`:
  - `hadoop_explorer_overview.json` — сводный дашборд здоровья и Golden Signals всех 5 сервисов платформы.
  - `hadoop_replicator_overview.json` — межкластерная репликация, скорость WAN, иерархический шейпинг Token Bucket Throttler и статусы задач.
  - `hdfs_explorer_operations.json` — файловые операции WebHDFS, сетевой обмен и мониторинг отказов.
  - `yarn_explorer_queues.json` — планировщик Capacity Scheduler, утилизация очередей и статус Circuit Breaker.
  - `spark_sql_explorer_analytics.json` — интерактивная аналитика Livy Spark и запросов Trino/Hive.

---

## 6. Модель персистентности данных (Storage Layer)

Платформа поддерживает три варианта персистентности:
1. **SQLite (JDBC)**: режим по умолчанию для автономного запуска и локальных демо-стендов (`jdbc:sqlite:data/<service>.db`).
2. **PostgreSQL (JDBC / HikariCP)**: рекомендуемый промышленный стандарт для продакшн-окружений (`jdbc:postgresql://...`). Обеспечивает единое хранилище сессий, воркспейсов и заявок YARN при горизонтальном масштабировании подов с поддержкой блокировок на уровне строк (`SELECT FOR UPDATE`).
3. **H2 Database (In-Memory)**: режим для быстрого прогона модульных и интеграционных тестов (`jdbc:h2:mem:...`).

### 6.1 Инициализация и версионирование схемы БД (`schema.sql` / JPA)
Управление схемой реляционной базы данных автоматизировано средствами Spring Boot и JPA:
- Автоматическая инициализация DDL-схемы для таблиц сессий (`sessions`), воркспейсов (`workspaces`), сохраненных запросов (`saved_queries`), истории вычислений и аудита.
- Автоматическая поддержка H2, SQLite и PostgreSQL без расхождения схемы данных.

---

## 7. Модель развертывания и надежность (Reliability)

1. **Docker образы на базе Eclipse Temurin 21 JRE**:
   - Минимальный защищенный runtime-образ `eclipse-temurin:21-jre-jammy` с системными утилитами Kerberos (`krb5-user`) и `curl` для healthcheck.
   - Упаковка скомпилированного Spring Boot Fat JAR (`backend/<service>/target/*.jar` или `backend/replicator/*/target/*.jar`) и статических бандлов Svelte 5 SPA.
   - Запуск под непривилегированным пользователем `appuser (UID 10001)`.
   - Оптимизированные параметры памяти JVM (`-Xms256m -Xmx1024m`).
2. **Структурированное JSON-логирование в продакшне**:
   - Настройка Logback с JSON/Logstash-энкодером для стандартизированного вывода логов в формате JSON (совместимость с ELK, Vector, FluentBit, Grafana Loki).
   - Автоматическое включение `timestamp`, `level`, `logger`, `requestId`, `message` и контекстных MDC-метаданных.
3. **Kubernetes (Helm)**:
   - **Umbrella Chart (`helm/hadoop-explorer`)**: единая декларативная установка всех сервисов с Ingress-маршрутизацией.
   - **Автономные чарты (`helm/charts/*`)**: независимое развертывание компонентов в различных неймспейсах.
   - **PodDisruptionBudget (`pdb.yaml`)**: защита от случайного одновременного удаления реплик при drain и обслуживании узлов кластера Kubernetes.
   - **Liveness & Readiness Probes (`/healthz`, `/readyz`)**: периодическая диагностика доступности базы данных и сессионного хранилища со статусом HTTP 503 при деградации хранилища.
   - **Prometheus Metrics (`/metrics`)**: экспорт состояния и статистики вызовов Circuit Breaker в формате Prometheus.
   - **Корректное завершение (Graceful Shutdown)**: перехват SIGTERM и штатная остановка пулов задач без обрыва пользовательских операций.
4. **Демо-стенды (`demo/`)**:
   - Раздельные стенды под каждый сервис и единый комплексный стенд `demo/all` с общими контейнерами OpenLDAP, MIT Kerberos KDC, Prometheus и Grafana.

---

## 8. Пользовательский интерфейс и дизайн-система

### 8.1 Тёмная тема (Dark Mode)
- Модуль `themeStore` (`frontend/common/stores/theme.svelte.ts`) на реактивных рунах Svelte 5 управляет темами (`light`, `dark`, `system`) с персистентностью в `localStorage` и поддержкой `prefers-color-scheme`.
- Переключатель темы (Sun / Moon) встроен в `Header.svelte` всех 4 фронтенд-приложений.
- Дизайн адаптирован на базе семантических утилит Tailwind CSS (`dark:bg-slate-950`, `dark:border-slate-800`, `dark:text-slate-100`).

---

## 9. Обеспечение качества и тестовая инфраструктура (Quality Assurance & Testing)

Платформа следует подходу пирамиды тестирования с разделением на изолированные модульные, компонентные, интеграционные и сквозные E2E-тесты:

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

### 9.1 Метрики тестового покрытия платформы (Java 21 + Svelte 5 Vitest)

| Сервис / Уровень | Стек и покрытие | Ключевые аспекты покрытия |
|---|---|---|
| **Common Security Starter** | **20 тестов (Java 21)** | Spring Boot 3 AutoConfiguration, SPNEGO Kerberos GSS-API, LDAP(S) аутентификация, SessionStore L1 (Caffeine) / L2 (JDBC H2/Postgres), CSRF Guard, Bucket4j Rate Limiter, SimpleCircuitBreaker, AOP JSON-аудит |
| **YARN Explorer** | **13 тестов (Java 21)** | Capacity Scheduler валидация, балансировка долей веток 100%, Draft Diff, XML Generation и санитизация, RM HA failover (`STANDBY` → `ACTIVE`), метрики кластера, Change Requests (Four-Eyes Principle), AWX REST клиент, аудит, L1 кэш токенов, Circuit Breaker |
| **HDFS Explorer** | **16 тестов (Java 21)** | NameNode HA Failover при `StandbyException`, Kerberos Proxy User doAs имперсонация, ContentSummary квоты, ACL, API, Parquet/ORC Preview со schema footer reader, MockHdfsClient, Circuit Breaker + Prometheus metrics, Rate Limiter |
| **SQL Explorer** | **10 тестов (Java 21)** | Catalog API валидация и эндпоинты, Trino/Hive движки, MockStorage, AI ассистент, токены, CSRF, ролевой доступ к кластерам, User Workspace |
| **Spark Explorer** | **12 тестов (Java 21)** | REST клиент Apache Livy, DAG валидация циклов (алгоритм Кана), MockSparkEngine, User Workspace, ролевой доступ, сессии и выполнение statements |
| **Hadoop gRPC Replicator** | **13 тестов (Java 21)** | Hierarchical Token Bucket (Global, DC-DC, HDFS-HDFS bottleneck), gRPC контракт Worker Agent, Orchestrator API, Prometheus метрики, Cron Scheduler, JobRun история |
| **Frontend UI Suite** | **92 теста (Vitest)** | Компонентное тестирование Svelte 5 на базе Vitest и `@testing-library/svelte` во всех 5 SPA и общем ядре: HDFS, YARN, SQL, Spark, Replicator, Common (Header, LoginModal, Modal, StatusBadge, NotificationToast), строгая проверка типов `svelte-check` |

### 9.2 Тестирование отказоустойчивости (Resilience Testing)
1. **Circuit Breaker State Machine**:
   - Верификация переходов состояний: `CLOSED` → регистрация серии сбоев → `OPEN` (мгновенный Fast-Fail без нагрузки на упавший кластер) → ожидание `recovery_timeout` → `HALF_OPEN` → успешные пробные вызовы → возврат в `CLOSED`.
2. **High Availability Failover**:
   - Автоматическое обнаружение и переключение на активные узлы при возврате `StandbyException` от NameNode или `haState: STANDBY` от ResourceManager.
   - Полноценная обработка сетевых сбоев (таймауты, сбои связи) с переключением на резервные URL.

### 9.3 Тестирование безопасности (Security Assurance)
1. **CSRF & Origin Verification**:
   - Проверка Fail-Closed режима для cookie-сессий, валидация заголовков `Origin`, `Referer`, `Host` и `X-Requested-With`.
2. **SSRF & CWE-200 Protection**:
   - Валидация хостов NameNode и DataNode перед передачей чувствительных `hadoop.auth` cookie.
3. **SQL & Identifier Injection**:
   - Строгая валидация идентификаторов в SQL Explorer для предотвращения разрыва SQL-команд в Trino и Hive.
4. **Принцип Four-Eyes**:
   - Запрет согласования заявок YARN их автором.

### 9.4 Автоматизация проверок (CI/CD Quality Gates)
- `make test` — запуск всех тестов платформы (`./scripts/run-tests.sh all`).
- `make test-java` — прогон всех Java тестов (`mvn test`).
- `make test-ui` — прогон тестов фронтенда (`npm run test:ui`).

---

## 10. Документация по промышленному развертыванию (DevOps Runbooks)

Для каждого микросервиса платформы разработано детальное руководство администратора по установке в режимах **Standalone (systemd)**, **Docker & Docker Compose** и **Kubernetes (Helm)**:

- 🚀 [Единый DevOps Hub платформы (admin-guide.md)](admin-guide.md)
- ⚙️ [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- 📁 [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- 🔍 [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- ⚡ [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- 🔄 [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)



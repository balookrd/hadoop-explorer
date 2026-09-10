# 🏛️ Архитектура платформы Hadoop Explorer Platform

**Hadoop Explorer Platform** — это масштабируемая корпоративная веб-платформа для интерактивной работы, мониторинга и администрирования экосистемы Apache Hadoop, объединяющая в рамках согласованного монорепозитория четыре ключевых инструмента:
1. **YARN Explorer** — консоль администрирования очередей и планирования ресурсов Capacity Scheduler.
2. **HDFS Explorer** — распределенный файловый менеджер.
3. **SQL Explorer** — редактор распределенных аналитических запросов к Trino и Hive с поддержкой ИИ.
4. **Spark Explorer** — веб-студия аналитики и интерактивных вычислений (PySpark, Scala Spark, Spark SQL).

---

## 1. Концепция и принципы архитектуры

- **Монорепозиторий с независимой поставкой**: единая кодовая база с разделением на независимые легковесные микросервисы. Любое приложение может быть собрано в отдельный Docker-контейнер или развернуто автономным Helm-чартом без зависимостей от других частей платформы.
- **Единое ядро безопасности и отказоустойчивости (`backend/common`, `frontend/common`)**: централизованная реализация аутентификации (Kerberos SPNEGO + LDAPS), фабрика валидации сессий (`make_get_current_user`), персистентное сессионное хранилище, защита от CSRF, аудит, контроль частоты запросов, Circuit Breaker, распределенные блокировки (Distributed Lock) и Graceful Shutdown.
- **Cookie-First и Zero LocalStorage**: токены авторизации передаются исключительно через защищенные `HttpOnly`, `SameSite=Lax`, `Secure` Cookie. В `localStorage` браузера не сохраняются JWT-токены или чувствительные учетные данные (защита от XSS/CWE-312).
- **Изоляция контекстов и персистентность (`User Workspace`)**: состояние вкладок, написанный код и результаты выполнения изолируются по пользователям и сохраняются в персистентную БД (SQLite WAL / PostgreSQL).
- **Высокая доступность и самовосстановление (High Availability & Resilience)**: встроенная поддержка отказоустойчивости для всех кластерных служб (Hadoop NameNode HA, YARN ResourceManager HA, Hive Metastore HA, Kerberos KDC) с защитой Fast-Fail через Circuit Breaker и автоматическим Failover.

---

## 2. Диаграмма архитектуры платформы

```
                      ┌──────────────────────────────────────────────┐
                      │             Веб-браузер клиента              │
                      │  (YARN :8001 / HDFS :8002 / SQL :8003 /      │
                      │               Spark :8004)                   │
                      └──────────────────────┬───────────────────────┘
                                             │ HTTP / SPNEGO / Cookies
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │    Reverse Proxy / Ingress / API Gateway     │
                      └──────┬──────────┬──────────┬──────────┬──────┘
                             │          │          │          │
         ┌───────────────────┘          │          │          └───────────────────┐
         ▼                              ▼          ▼                              ▼
┌──────────────────┐           ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  YARN Explorer   │           │  HDFS Explorer   │  │   SQL Explorer   │  │  Spark Explorer  │
│ (FastAPI: 8000)  │           │ (FastAPI: 8000)  │  │ (FastAPI: 8000)  │  │ (FastAPI: 8000)  │
└────────┬─────────┘           └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                              │                     │                     │
         └──────────────────────────────┼─────────────────────┼─────────────────────┘
                                        │
                                        ▼
                   ┌──────────────────────────────────────────────┐
                   │       Ядро платформы (backend/common)        │
                   │  - SessionStore & L1/L2 Token Cache          │
                   │  - Security Middleware (make_get_current_user)│
                   │  - Circuit Breaker (Fast-Fail & HA Failover) │
                   │  - Distributed Lock (Redis / In-Memory DB)   │
                   │  - Graceful Shutdown Manager                 │
                   │  - CommonLdapAuthService / Kerberos SPNEGO   │
                   │  - CSRF Guard & Rate Limiter (Sliding Window)│
                   │  - Structured Audit Logger                   │
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

## 3. Подсистема безопасности (`backend/common`)

### 3.1 Аутентификация: Kerberos SPNEGO SSO, LDAPS и единый `create_auth_router`
1. **Централизованный Auth APIRouter (`backend.common.api.auth_router`)**:
   - Фабрика `create_auth_router` генерирует стандартизированный набор эндпоинтов аутентификации (`POST /api/v1/auth/login`, `GET /api/v1/auth/sso`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me`) для всех микросервисов платформы.
   - Поддерживает скользящее продление сессий (Sliding Session), установку безопасных `HttpOnly` Cookie, работу с mock-пользователями и аутентификацию по LDAP/Kerberos.
2. **Единый KerberosManager (`backend.common.core.kerberos`)**:
   - Потокобезопасный класс для управления тикетами Kerberos (инициализация `kinit -kt`, проверка валидности через `klist`, генерация SPNEGO-заголовков `Authorization: Negotiate` для внутренних клиентов WebHDFS, YARN RM и Livy).
   - Валидация Kerberos SPNEGO билетов браузера через GSSAPI и извлечение принципала пользователя (`user@REALM`).
3. **LDAPS / Active Directory**: безопасная проверка пароля через `CommonLdapAuthService` с экранированием фильтров (защита от LDAP Injection, CWE-90) и извлечением групп (`memberOf`).
4. **Mock-режим**: предназначен исключительно для разработки и демонстрационных стендов (`auth.mode: "mock"`). В режиме `debug: false` запуск mock-провайдера категорически блокируется.
5. **Унифицированный Security Middleware**: фабрика `make_get_current_user` гарантирует единообразную валидацию JWT-токенов, проверку серверного отзыва (Revocation check) и преобразование в `CommonUserSession` во всех микросервисах.

### 3.2 Персистентное сессионное хранилище (`SessionStore`)
- Защита от использования отозванных токенов (CWE-613).
- Неблокирующий асинхронный I/O: методы `save_session_async`, `get_session_async`, `is_token_revoked_async`, `check_and_record_rate_limit_async` выполняются через пул рабочих потоков во избежание блокировки FastAPI Event Loop.
- Активные сессии сохраняются в таблице `active_sessions` реляционной БД.
- Двухуровневый черный список отозванных токенов:
  - **L1**: сверхбыстрый In-Memory LRU кэш (`L1RevokedTokenCache`) с автоматической очисткой по TTL.
  - **L2**: база данных (PostgreSQL / SQLite WAL) или Redis.
- Защита от **Fail-Open**: при временной недоступности базы данных невалидные токены не пропускаются.
- **Диагностика доступности (`ping` / `ping_async`)**: поддержка проверки доступности хранилища для Kubernetes Readiness Probes.

### 3.3 Защита от CSRF и атак на транспорт
- Полная блокировка межсайтовых запросов: анализ заголовка `Sec-Fetch-Site: cross-site`.
- Валидация заголовков `Origin` и `Referer` по строгому белому списку разрешенных доменов (`server.cors_origins`). Устранено невалидированное доверие заголовку `Host` (Host Header Injection).
- Обязательное требование заголовка `X-Requested-With` для асинхронных API вызовов.

### 3.4 Rate Limiting и аудит
- Алгоритм скользящего окна (Sliding Window) с неблокирующей асинхронной проверкой и поддержкой распределенных хранилищ (Redis, Postgres, SQLite).
- **Оптимизированный путь исполнения**: проверка и регистрация запросов выполняются через быстрые `SELECT COUNT()` / `INSERT` без тяжелых `DELETE`-операций на каждый входящий запрос, что исключает блокировки (write-lock contention) в БД. Очистка устаревших записей вынесена в плановый метод `cleanup_expired`.
- Определение реального IP-клиента с защитой от спуфинга заголовка `X-Forwarded-For` через список доверенных прокси (`trusted_proxies`).
- Структурированное JSON-логирование критических событий безопасности (`AUDIT_LOGIN_SUCCESS`, `AUDIT_LOGIN_FAILURE`, `AUDIT_TOKEN_REVOKED`, `AUDIT_CSRF_REJECT`).

### 3.5 Content-Security-Policy (CSP) и защитные HTTP-заголовки
Централизованный модуль `backend.common.core.security` предоставляет функцию `apply_security_headers`, гарантирующую соблюдение современных стандартов защиты веб-клиента:
- **Content-Security-Policy (CSP)**:
  - **Базовая строгая политика (`CSP_DEFAULT_DIRECTIVES`)**: применяется для YARN и HDFS Explorer (`default-src 'self'`, `script-src 'self'`, `style-src 'self' 'unsafe-inline'`, `img-src 'self' data:`, `font-src 'self'`, `connect-src 'self'`, `frame-ancestors 'none'`, `object-src 'none'`, `base-uri 'self'`).
  - **Политика для редакторов кода (`CSP_CODE_EDITOR_DIRECTIVES`)**: применяется для SQL и Spark Explorer для безопасного функционирования Monaco Editor и Web Workers (`worker-src 'self' blob:`, `script-src 'self' 'unsafe-eval' blob:`, `connect-src 'self' ws: wss: http: https:`, `img-src 'self' data: blob:`).
  - Дублирование CSP в `index.html` через `<meta http-equiv="Content-Security-Policy">` для защиты статических файлов при независимой раздаче.
- **Защитные заголовки**:
  - `X-Frame-Options: DENY` — абсолютная защита от Clickjacking.
  - `X-Content-Type-Options: nosniff` — блокировка MIME-sniffing атак.
  - `Referrer-Policy: strict-origin-when-cross-origin` — ограничение передачи заголовка Referer на сторонние ресурсы.
  - `Strict-Transport-Security: max-age=31536000; includeSubDomains` (HSTS) — принудительный переход на HTTPS при включенной опции `secure_cookies`.

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
- **Состояния**: `CLOSED` (нормальная работа) $\rightarrow$ `OPEN` (кластер недоступен, вызовы сразу отклоняются с `CircuitBreakerOpenException` без ожидания сетевых таймаутов) $\rightarrow$ `HALF_OPEN` (пробная отправка запроса для проверки восстановления кластера).
- **Фильтрация ошибок**: клиентские HTTP-ошибки (4xx) считаются легитимными ответами и не приводят к срабатыванию автомата; учитываются только сетевые ошибки, сбои подключения, таймауты и серверные 5xx.
- **HA Failover**: автоматическое переключение на резервный узел (Standby NameNode / Standby ResourceManager) при фиксации сбоя на Active-узле.
- **Интеграция с Kubernetes Readiness Probe (`/readyz`)**: если все внешние коннекторы кластера переходят в статус `OPEN`, эндпоинт `/readyz` возвращает `503 Service Unavailable` (`status: degraded`), исключая отправку клиентского трафика на деградировавший под.

### 4.3 Модуль повторных попыток с экспоненциальным Backoff (`backend.common.core.retry`)
Для обработки кратковременных транзиентных сбоев сети и сокетов:
- Асинхронная функция `retry_async` и декоратор `@with_retry`.
- Экспоненциальный рост интервала задержки (`backoff_factor`) с добавлением случайного джиттера (`jitter=True`) для исключения эффекта «громоподобного стада» (Thundering Herd).
- Настраиваемый список повторяемых сетевых исключений (`httpx.ConnectError`, `httpx.TimeoutException`) и исключение критических ошибок бизнес-логики.

### 4.4 Централизованные обработчики исключений (`backend.common.api.error_handlers`)
- Защита от раскрытия чувствительной информации и структуры бэкенда при 500-х ошибках (CWE-209).
- Функция `setup_global_exception_handlers(app)` перехватывает необработанные ошибки, генерирует уникальный `incident_id`, детально логирует инцидент внутри сервера и возвращает клиенту безопасный ответ.
- Автоматическая трансляция состояния `CircuitBreakerOpenException` в HTTP 503 с заголовком `Retry-After`.

### 4.5 Distributed Lock (`backend.common.core.lock`)
Для предотвращения состояний гонки (Race Conditions) при параллельных операциях:
- **Трехуровневая стратегия блокировок**:
  1. **Redis**: атомарный `SET key owner_id NX PX` с безопасным освобождением через Lua-скрипт (`REDIS_RELEASE_LUA`).
  2. **DB-backed блокировка на уровне строк (PostgreSQL / SQLite)**: таблица `distributed_locks` в `SessionStore` с транзакционным `SELECT ... FOR UPDATE` (PostgreSQL) и атомарными операциями `INSERT`/`UPDATE` с проверкой `expires_at` и `owner_id`. Обеспечивает межпроцессную и межподовую синхронизацию даже при отсутствии Redis.
  3. **In-Memory Fallback**: локальный потокобезопасный словарь с TTL для сред разработки.
- Применяется в YARN Explorer для защиты согласования и отклонения заявок на изменение конфигурации очередей (Change Requests).

### 4.6 Graceful Shutdown (`backend.common.core.shutdown`)
Все микросервисы платформы интегрированы с `GracefulShutdownManager` в FastAPI lifespan:
- Перехват сигналов завершения подов Kubernetes (`SIGTERM`, `SIGINT`).
- Корректная остановка фоновых пулов потоков (`ThreadPoolExecutor.shutdown(wait=True)`).
- Закрытие активных HTTP-сессий, клиентов баз данных и сброс логов аудита до остановки процесса.


---

## 5. Архитектура сервисов платформы

### 5.1 YARN Explorer
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
- **Интеграция**: подключение к WebHDFS / HttpFS с поддержкой NameNode High Availability, защитой через Circuit Breaker и повторными попытками с экспоненциальным backoff (`retry_async`).
- **Имперсонация (`doAs`)**: выполнение файловых операций от имени аутентифицированного пользователя при наличии привилегий у сервисного аккаунта.
- **Неблокирующая архивация (Non-blocking ZIP)**: упаковка и распаковка директорий в ZIP-архивы с выносом ресурсоемких операций сжатия и чтения в пул рабочих потоков (`asyncio.to_thread`), защита Event Loop от DoS/OOM и ликвидация N+1 задержек.
- **Предпросмотр данных**: потоковое чтение и конвертация форматов Apache Parquet и Apache ORC в структурированный JSON прямо в памяти сервера без выгрузки на диск хоста. Для крупномасштабных Parquet и ORC файлов, превышающих лимит полного чтения, реализовано извлечение схемы колонок, типов и метаданных через чтение футера файла по диапазону (Range Reading с использованием адаптера `_SeekableFooterStream`).
- **Кросс-кластерное копирование**: асинхронная передача файлов между независимыми кластерами HDFS.


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

### 5.5 Архитектура и оптимизация Frontend (Svelte 5 & Tailwind 4)
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
- **Автоматическая кодогенерация TypeScript-типов из OpenAPI**:
  - Команда `make generate-types` запускает скрипт [`scripts/generate-types.sh`](../scripts/generate-types.sh), который автоматически извлекает актуальные OpenAPI JSON схемы бэкенд-сервисов (`yarn`, `hdfs`, `sql`, `spark`) и генерирует строгие TypeScript-интерфейсы в `frontend/common/types/generated/`.
  - Это исключает расхождения контрактов данных (Data Drift) между Pydantic-моделями бэкенда и фронтенд-клиентом.

---

### 3.10 Асимметричные JWT (RS256/ES256), ротация ключей и JWKS (RFC 7517)
В `backend.common.core.jwt_keys` реализован `JWTKeyManager`:
- Поддержка асимметричной подписи токенов RSA (`RS256`) и ECDSA (`ES256`) с автоматическим добавлением идентификатора ключа `kid` в JWT header.
- Бесшовная ротация ключей: сохранение истории публичных ключей для непрерывной валидации ранее выпущенных токенов при выпуске нового активного ключа подписи.
- Эндпоинты `GET /api/v1/auth/jwks.json` и `GET /.well-known/jwks.json`, экспортирующие набор открытых ключей в стандартном формате RFC 7517 для интеграции с внешними API Gateway, Service Mesh и OAuth2/OIDC сервисами.

---

## 4. Оптимизации производительности и потоковой передачи данных

### 4.1 Высокопроизводительный пул соединений с поддержкой HTTP/2
Фабрика `create_async_http_client` (`backend.common.core.http_client`) предоставляет оптимизированный асинхронный HTTP-клиент:
- Поддержка мультиплексирования HTTP/2 с автоматическим graceful fallback на HTTP/1.1 при неподдерживаемых бэкендах.
- Тонко настроенный пул соединений (`httpx.Limits(max_keepalive_connections=50, max_connections=200, keepalive_expiry=30.0)`).
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

### 4.5 ETag Middleware и условное кэширование (HTTP 304 Not Modified)
Модуль `backend.common.api.etag_middleware.ETagMiddleware`:
- Автоматически рассчитывает слабые ETag-хэши (`W/"..."`) для всех безопасных GET/HEAD ответов API.
- Обрабатывает входящий заголовок `If-None-Match`, возвращая легковесный `HTTP 304 Not Modified` без тела ответа при неизменности данных (каталоги метаданных Hive, неизменные очереди YARN, списки кластеров).

### 4.6 Визуальный конструктор и планировщик Spark DAG-пайплайнов
В Spark Explorer интегрирован движок и UI визуального конструирования пайплайнов (`backend.spark.app.models.pipeline`, `PipelineBuilder.svelte`):
- Поддержка гетерогенных узлов графа: PySpark скрипты, Spark SQL запросы, задачи Spark Submit.
- Строгая топологическая валидация DAG по алгоритму Кана (Kahn's Algorithm) для гарантированного исключения циклических зависимостей (ошибка `422 Unprocessable Content`).
- Асинхронное исполнение узлов и пошаговый мониторинг статусов выполнения в реальном времени.

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
- Готовые provisioning-дашборды Grafana в `monitoring/grafana/dashboards/`:
  - `hadoop_explorer_overview.json` — обзорный дашборд здоровья платформы.
  - `yarn_explorer_queues.json` — планировщик Capacity Scheduler, утилизация очередей и статус Circuit Breaker.
  - `spark_sql_explorer_analytics.json` — интерактивная аналитика Livy Spark и запросов Trino/Hive.

---

## 6. Модель персистентности данных (Storage Layer)

Платформа поддерживает три варианта персистентности:
1. **SQLite (WAL Mode)**: режим по умолчанию для автономного запуска и локальных демо-стендов (`sqlite+aiosqlite:///...`). Включение режима Write-Ahead Logging обеспечивает высокую конкурентность чтения и записи.
2. **PostgreSQL**: рекомендуемый промышленный стандарт для продакшн-окружений (`postgresql+asyncpg://...`). Обеспечивает единое хранилище сессий, воркспейсов и заявок YARN при горизонтальном масштабировании подов с поддержкой блокировок на уровне строк (`SELECT FOR UPDATE`).
3. **Redis**: опциональный слой для распределенного Rate Limiting, централизованного черного списка токенов и распределенных блокировок (`DistributedLock`).

### 6.1 Инфраструктура миграций БД (Alembic)
Управление схемой реляционной базы данных автоматизировано через Alembic (`alembic.ini`, `migrations/`):
- Декларативные миграции для таблиц сессий (`sessions`), воркспейсов (`workspaces`), сохраненных запросов (`saved_queries`), истории вычислений и аудита.
- Автоматическая поддержка как SQLite, так и PostgreSQL без расхождения DDL.

---

## 7. Модель развертывания и надежность (Reliability)

1. **Docker Multi-Stage Build & оптимизация кэширования слоев**:
   - Stage 1: сборка frontend SPA на Node.js 22 (Svelte 5 + Vite).
   - Stage 2: сборка зависимостей Python с отдельным слоем для сторонних пакетов, что исключает повторную загрузку и компиляцию библиотек при изменении кода.
   - Stage 3: минимальный runtime образ Python 3.12-slim с системными библиотеками Kerberos, SASL, OpenLDAP, запуском под непривилегированным пользователем `appuser (UID 10001)`.
   - Конфигурирование количества Uvicorn воркеров через переменную окружения `WEB_CONCURRENCY` / `WORKERS` (по умолчанию `2`).
2. **Структурированное JSON-логирование в продакшне**:
   - Реализован `JSONFormatter` (`backend.common.core.logging_config`) для стандартизированного вывода логов в формате JSON (совместимость с ELK, Vector, FluentBit, Grafana Loki).
   - Автоматическое включение `timestamp`, `level`, `logger`, `request_id`, `message` и дополнительных метаданных.
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
                  │ FastAPI REST API / IT │
                  └───────────┬───────────┘
                              │
                  ┌───────────┴───────────┐
                  │ Python Core Unit Test │
                  └───────────────────────┘
```

### 9.1 Метрики тестового покрытия платформы (308 тестов)

| Сервис / Уровень | Количество тестов | Ключевые аспекты покрытия |
|---|---|---|
| **YARN Explorer** | **65 тестов** | Capacity Scheduler валидация, балансировка долей, Draft Diff, XML Generation, RM HA failover (`STANDBY` → `ACTIVE`), сбор метрик кластера, Change Requests, аудит, L1 кэш токенов, Readiness / Healthz, Distributed Lock, Circuit Breaker |
| **HDFS Explorer** | **86 тестов** | NameNode HA Failover при `StandbyException`, WebHDFS exception mapping, ContentSummary квоты, ACL, API, Readiness / Healthz, Security (CWE-200, CSP, CSRF), Common Modules, Parquet/ORC Preview со schema footer reader, Cross-Cluster Copy, Circuit Breaker + Prometheus metrics, Retry с backoff, Global Exception Handlers, Distributed Lock на БД, Rate Limiter |
| **SQL Explorer** | **42 теста** | Catalog API валидация и эндпоинты, Trino/Hive движки с отменой запросов и стримингом, TTL-кэширование метаданных, AI сервис, токены, CSRF, ACL кластеров, Crash Recovery, Readiness / Healthz, SqlUserWorkspace |
| **Spark Explorer** | **24 теста** | Livy клиент полного цикла с отменой statement и логами, интерактивные сессии, автоостановка сессий при logout, Pydantic валидаторы, MockSparkEngine, User Workspace, TTL-кэширование метаданных, Crash Recovery, Readiness / Healthz, Circuit Breaker |
| **Frontend UI Suite** | **91 тест** | Полное компонентное тестирование Svelte 5 на базе Vitest и `@testing-library/svelte` во всех 4 SPA и общем ядре: HDFS (хлебные крошки, тулбар действий, мультивыбор, список файлов с сортировкой, модалки создания/переименования/удаления), YARN (метрики ресурсов кластера, селектор партиций, балансировка квот, дифф конфигураций, модалки очередей и XML), SQL (тулбар запуска, таблица результатов, очередь фоновых задач, ИИ-ассистент), Spark (тулбар сессий и языков, результаты и логи, модалка конфигурации), Common (Header, LoginModal, Modal, StatusBadge, NotificationToast), полифиллы jsdom (ResizeObserver, IntersectionObserver, clipboard), строгая проверка типов `svelte-check` (0 ошибок) и E2E сценарии Playwright |

### 9.2 Тестирование отказоустойчивости (Resilience Testing)
1. **Circuit Breaker State Machine**:
   - Верификация переходов состояний: `CLOSED` → регистрация серии сбоев → `OPEN` (мгновенный Fast-Fail без нагрузки на упавший кластер) → ожидание `recovery_timeout` → `HALF_OPEN` → успешные пробные вызовы → возврат в `CLOSED`.
2. **High Availability Failover**:
   - Автоматическое обнаружение и переключение на активные узлы при возврате `StandbyException` от NameNode или `haState: STANDBY` от ResourceManager.
   - Полноценная обработка сетевых сбоев (`ConnectError`, таймауты) с переключением на резервные URL.
3. **Retry с экспоненциальным Backoff и джиттером**:
   - Автоматический повтор сбойных вызовов (`retry_async`) для временных сетевых ошибок с защитой от шторма повторов (jitter).

### 9.3 Тестирование безопасности (Security Assurance)
1. **CSRF & Origin Verification**:
   - Проверка Fail-Closed режима для cookie-сессий, валидация заголовков `Origin`, `Referer`, `Host` и `X-Requested-With`.
2. **SSRF & CWE-200 Protection**:
   - Проверка `validate_webhdfs_location` на блокировку приватных IP-адресов, метаданных облачных провайдеров (169.254.169.254) и нестандартных схем.
   - Верификация `is_trusted_redirect_host` перед передачей чувствительных `hadoop.auth` cookie на DataNode.
3. **SQL & Identifier Injection**:
   - Строгая валидация идентификаторов `validate_identifier` в SQL Explorer для предотвращения разрыва SQL-команд в Trino и Hive.
4. **Принцип Four-Eyes**:
   - Запрет согласования заявок YARN их автором.

### 9.4 Автоматизация проверок (CI/CD Quality Gates)
- `make sync` — синхронизация единого окружения `uv workspaces`.
- `make lint` — проверка кодовой базы линтером Ruff.
- `make format` — форматирование исходного кода.
- `make test` — запуск всех 240 тестов платформы.



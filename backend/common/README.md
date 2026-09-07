# Hadoop Explorer Common (`hadoop-explorer-common`)

Пакет общих компонентов безопасности, моделей данных, отказоустойчивости и сервисов хранения для платформы **Hadoop Explorer**.

## Содержимое
- **`backend.common.core.security`** — фабрика `make_get_current_user`, модель `CommonUserSession`, генерация и валидация JWT, строгая CSRF-защита (блокировка cross-site запросов, валидация Origin/Referer по строгому белому списку без доверия Host Header, требование заголовка `X-Requested-With`), безопасная валидация `secret_key` (fail-fast в продакшне, автогенерация безопасного временного ключа в dev-режиме).
- **`backend.common.core.circuit_breaker`** — автомат состояний `CircuitBreaker` (`CLOSED`, `OPEN`, `HALF_OPEN`) для защиты вызовов к распределенным сервисам (WebHDFS NameNode, YARN ResourceManager, Apache Livy). Реализует Fast-Fail при сбоях кластера, автоматический Failover на Standby-узлы и исключает клиентские ошибки (4xx) из подсчета сбоев.
- **`backend.common.core.shutdown`** — `GracefulShutdownManager` для корректного освобождения ресурсов при остановке подов Kubernetes (SIGTERM/SIGINT): завершение пулов потоков `ThreadPoolExecutor`, отмена фоновых задач, закрытие HTTP-сессий и соединений с базами данных.
- **`backend.common.core.lock`** — `DistributedLock` для защиты критических секций (согласование Change Requests, конкурентные модификации очередей) на базе Redis с автоматическим fallback на In-Memory/DB при локальном запуске.
- **`backend.common.core.cache`** — `L1RevokedTokenCache`: потокобезопасный In-Memory LRU кэш с автоматической очисткой по TTL для мгновенной валидации отозванных JWT-токенов без лишней нагрузки на L2 (базу данных или Redis).
- **`backend.common.core.session_store`** — сохранение активных сессий пользователей в реляционной базе данных (`SQLite WAL`, `PostgreSQL`) для полной устойчивости к перезапуску бэкендов. Включает персистентную валидацию сессий через таблицу `active_sessions` (с автоматической конвертацией TTL), двухуровневый черный список токенов `revoked_tokens` (L1 In-Memory LRU + L2 DB), асинхронные неблокирующие методы (`save_session_async`, `get_session_async`, `is_token_revoked_async`, `check_and_record_rate_limit_async`) и метод проверки доступности `ping` / `ping_async` для Kubernetes Readiness Probes.
- **`backend.common.core.ldap_auth`** — универсальный `CommonLdapAuthService` для LDAPS / Active Directory / OpenLDAP с защитой от LDAP Injection, извлечением групп и mock-провайдером с PBKDF2 хэшированием.
- **`backend.common.core.rate_limiter`** — скользящее окно (sliding window) rate limiting с неблокирующей асинхронной проверкой, поддержкой SQLite WAL, PostgreSQL и Redis, а также безопасным извлечением клиентского IP (`is_trusted_proxy`).
- **`backend.common.core.audit`** — структурированное JSON-логирование событий безопасности (`AuditEventType`) в кольцевой буфер и файл.
- **`backend.common.db.storage`** — базовый `BaseStorageService` для отзыва токенов и ограничения частоты запросов.
- **`backend.common.models.auth`** — общие Pydantic-модели ролей (`Role`), сессий (`UserSession`, `CommonUserSession`) и аутентификации.

# Hadoop Explorer Common (`hadoop-explorer-common`)

Пакет общих компонентов безопасности, моделей данных и сервисов хранения для платформы **Hadoop Explorer**.

## Содержимое
- **`backend.common.core.session_store`** — сохранение активных сессий пользователей в реляционной базе данных (`SQLite WAL`, `PostgreSQL`) для полной устойчивости к перезапуску бэкендов. Включает персистентную валидацию сессий через таблицу `active_sessions` (с автоматической конвертацией TTL), двухуровневый черный список токенов `revoked_tokens` (L1 In-Memory LRU + L2 DB), асинхронные неблокирующие методы (`save_session_async`, `get_session_async`, `is_token_revoked_async`, `check_and_record_rate_limit_async`) и метод проверки доступности `ping` / `ping_async` для Kubernetes Readiness Probes.
- **`backend.common.core.security`** — генерация и валидация JWT, строгая CSRF-защита (блокировка cross-site запросов, валидация Origin/Referer по строгому белому списку без доверия Host Header, требование заголовка `X-Requested-With`), проверка отзыва токенов с двухуровневым кэшированием.
- **`backend.common.core.ldap_auth`** — универсальный `CommonLdapAuthService` для LDAPS / Active Directory / OpenLDAP с защитой от LDAP Injection, извлечением групп и mock-провайдером с PBKDF2 хэшированием.
- **`backend.common.core.rate_limiter`** — скользящее окно (sliding window) rate limiting с неблокирующей асинхронной проверкой, поддержкой SQLite WAL, PostgreSQL и Redis, а также безопасным извлечением клиентского IP (`is_trusted_proxy`).
- **`backend.common.core.audit`** — структурированное JSON-логирование событий безопасности (`AuditEventType`) в кольцевой буфер и файл.
- **`backend.common.db.storage`** — базовый `BaseStorageService` для отзыва токенов и ограничения частоты запросов.
- **`backend.common.models.auth`** — общие Pydantic-модели ролей (`Role`), сессий (`UserSession`) и аутентификации.


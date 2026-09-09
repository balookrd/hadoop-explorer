# Backend: YARN Queue Explorer

Бэкенд-сервис приложения **YARN Queue Explorer**, реализованный на базе **FastAPI (Python 3.12/3.14)**. Сервис обеспечивает взаимодействие с кластерами Apache Hadoop YARN через Kerberos SPNEGO с защитой **Circuit Breaker** и автоматическим **HA Failover**, корпоративную аутентификацию пользователей через OpenLDAP / Active Directory или Mock/Local провайдеры, персистентное хранение заявок на согласование в SQLite/PostgreSQL с защитой от гонок через **Distributed Lock**, безопасную генерацию конфигурации `capacity-scheduler.xml` и Graceful Shutdown.

---

## 🏛 Архитектура компонентов бэкенда

```
backend/yarn/
├── app/
│   ├── api/                     # REST API контроллеры (v1)
│   │   ├── auth.py              # Аутентификация (фабрика create_auth_router из backend.common)
│   │   ├── clusters.py          # Список кластеров (/api/v1/clusters)
│   │   ├── queues.py            # Очереди, валидация, diff, XML (/api/v1/clusters/{cluster_id}/...)
│   │   └── change_requests.py   # Управление заявками (/api/v1/change-requests) с DistributedLock
│   ├── core/                    # Ядро сервиса
│   │   ├── acl.py               # Проверка ACL (check_ui_access, resolve_cluster_role, check_cluster_permission)
│   │   ├── audit.py             # Структурированный аудит безопасности и изменений очередей
│   │   ├── config.py            # Pydantic Settings, загрузка config.yaml
│   │   ├── ldap_auth.py         # LdapService с защитой от LDAP-инъекций и валидацией TLS
│   │   ├── rate_limiter.py      # Rate Limiting (Sliding Window через StorageService)
│   │   └── security.py          # JWT-токены, make_get_current_user с валидацией UI ACL и resolve_system_role
│   ├── models/                  # Pydantic-модели и схемы данных
│   │   ├── cluster.py           # ClusterConfig, ClusterAcl, ClusterResources
│   │   ├── yarn.py              # QueueNode, QueueDraftItem (с regex-валидацией), PartitionResourceConfig
│   │   └── change_requests.py   # ChangeRequestCreate, ChangeRequestReview, ChangeRequestResponse
│   ├── services/                # Бизнес-логика
│   │   ├── capacity_scheduler.py# Алгоритмы проверки баланса очередей
│   │   ├── mock_yarn.py         # Mock данные для dev режима
│   │   ├── storage.py           # Конфигурация хранилища на базе SessionStore
│   │   ├── xml_generator.py     # Точечная модификация capacity-scheduler.xml с санитизацией
│   │   └── yarn_client.py       # REST API клиент YARN RM с KerberosManager, HA и Circuit Breaker
│   ├── docker-entrypoint.sh     # Инициализация Kerberos (kinit) и запуск uvicorn
│   └── main.py                  # Входная точка FastAPI, CORS, Security Headers, Graceful Shutdown, /healthz
├── tests/                       # Автоматические тесты (pytest - 43 теста)
│   ├── conftest.py              # Автосброс rate limits в тестах
│   ├── test_capacity_scheduler.py # Тесты балансировки и генерации XML
│   ├── test_change_requests.py   # Тесты CRUD хранилища заявок и DistributedLock
│   └── test_security.py          # Тесты безопасности (инъекции, BOLA, ACL, валидация)
├── pyproject.toml               # Конфигурация пакета hadoop-explorer-yarn
└── requirements.txt             # Зависимости Python
```

---

## 🛡️ Безопасность и отказоустойчивость (Security & Resilience)

В сервисе реализован комплекс защитных мер для соответствия лучшим практикам информационной безопасности (OWASP Top 10) и высокой доступности:

1. **Строгая защита от CSRF**:
   - Валидация источников через `urllib.parse.urlparse` со строгим сопоставлением с `server.cors_origins` и `Host` заголовком. Режим Fail-Closed отклоняет мутирующие cookie-запросы без валидных источников.
2. **Отказоустойчивость вызовов YARN RM (Circuit Breaker & HA Failover)**:
   - Встроенный `CircuitBreaker` в `YARNClient`: мгновенный отказ (Fast-Fail) при недоступности RM без блокировки пулов потоков.
   - Автоматический failover на Standby ResourceManager при падении Active RM. 4xx клиентские ошибки игнорируются автоматом.
   - Экспорт метрик состояний автоматов защиты в Prometheus формате на эндпоинтах `/metrics` и `/api/v1/metrics`.
3. **Защита от состояний гонки при согласовании (Distributed Lock)**:
   - Согласование и отклонение заявок (`/approve`, `/reject`) защищено `DistributedLock` (Redis с fallback на In-Memory), гарантируя атомарность и исключая двойное одобрение.
4. **Защита от инъекций и XXE**:
   - **XXE & DoS Protection**: Парсинг XML через `defusedxml.ElementTree` с блокировкой entity expansion, DTD и billion laughs атак.
   - **LDAP Filter Injection**: Входные данные экранируются через `ldap3.utils.conv.escape_filter_chars` перед передачей в фильтры поиска каталогов.
   - **XML Comment / Configuration Injection**: Поля `comment` и `generated_by` экранируются функцией `_sanitize_xml_comment`, исключающей разрыв XML-комментариев (`-->`) и внедрение недопустимых свойств в `capacity-scheduler.xml`.
5. **Защита от BOLA / IDOR и принцип Four-Eyes**:
   - Доступ к деталям заявки (`GET /api/v1/change-requests/{id}`) строго ограничен правами пользователя в соответствующем кластере.
   - Запрещено самостоятельное одобрение автором своей собственной заявки (`Four-Eyes Principle`).
6. **Двухуровневый контроль доступа (RBAC & UI ACL)**:
   - `check_ui_access`: проверка права доступа пользователя к интерфейсу и API на основе глобальных политик `acl.ui_access`.
   - `resolve_cluster_role` & `check_cluster_permission`: гранулярное разделение прав по каждому кластеру (ADMIN, WRITER, READER).
   - Обогащение LDAP-группами при Kerberos SPNEGO SSO (`/api/v1/auth/sso`) для корректного назначения ролей.
7. **Потокобезопасность сессий и пулов**:
   - Изоляция сетевых клиентов на асинхронные вызовы к YARN RM, исключающая гонки данных и утечки сессий.
8. **Серверная инвалидация токенов и персистентность (SQLAlchemy Core)**:
   - Хранилище заявок (Change Requests) и черного списка токенов на базе `SQLAlchemy Core` с поддержкой как `SQLite` (WAL), так и `PostgreSQL`.
9. **Защита от брутфорса и IP-спуфинга (Rate Limiting)**:
   - Эндпоинты аутентификации защищены ограничителем частоты запросов с защитой от IP-спуфинга (доверяет `X-Forwarded-For` только от доверенных прокси).
10. **Безопасные сессии (HttpOnly Cookies) и CSP**:
    - Токены принимаются через `Authorization: Bearer` или `HttpOnly`, `SameSite=Lax`, `Path=/` (и `Secure` в продакшне) Cookie. Токены в query-параметрах заблокированы.
    - Защитные заголовки Content-Security-Policy: `default-src 'self'`, `object-src 'none'`, `base-uri 'self'`, `form-action 'self'`, `frame-ancestors 'none'`.
11. **Централизованная обработка исключений (CWE-209)**:
    - Интеграция `setup_global_exception_handlers` с генерацией `incident_id` и скрытием внутренних трассировок при 500 ошибках.
12. **Graceful Shutdown**:
    - Интеграция с `GracefulShutdownManager` в lifespan приложения.

---

## 🌐 Спецификация REST API

### Аутентификация (`/api/v1/auth`)
- `POST /api/v1/auth/login` — аутентификация по логину и паролю (LDAP / Mock / Hybrid). Выставляет `HttpOnly` cookie `access_token` и возвращает JWT токен. Защищено Rate Limiter (10 запросов в минуту).
- `POST /api/v1/auth/sso` — аутентификация Kerberos SPNEGO SSO через заголовок `Authorization: Negotiate <token>`. Защищено Rate Limiter.
- `GET /api/v1/auth/me` — получение профиля текущего пользователя и его роли.
- `POST /api/v1/auth/logout` — завершение сессии, серверный отзыв токена (blacklist) и удаление сессионной cookie.

### Кластеры (`/api/v1/clusters`)
- `GET /api/v1/clusters` — список доступных пользователю YARN-кластеров с ролями и метаданными.

### Очереди и моделирование (`/api/v1/clusters/{cluster_id}`)
- `GET /api/v1/clusters/{cluster_id}/queues` — получение дерева очередей и метрик утилизации кластера. Доступно: `READER`, `WRITER`, `ADMIN`.
- `POST /api/v1/clusters/{cluster_id}/validate` — валидация баланса ресурсов веток очередей (RAM / vCPU). Доступно: `WRITER`, `ADMIN`.
- `POST /api/v1/clusters/{cluster_id}/diff` — расчет дельты изменений между live и draft состоянием. Доступно: `WRITER`, `ADMIN`.
- `POST /api/v1/clusters/{cluster_id}/generate-xml` — генерация `capacity-scheduler.xml`. Доступно: только `ADMIN`.
- `POST /api/v1/clusters/{cluster_id}/deploy-xml` — прямое горячее развертывание и применение `capacity-scheduler.xml` на кластере через Ansible AWX. Доступно: только `ADMIN`.

### Заявки на согласование (`/api/v1/change-requests`)
- `GET /api/v1/change-requests` — список заявок с фильтрацией по кластеру и статусу (только для разрешенных кластеров).
- `GET /api/v1/change-requests/pending-count` — количество заявок в статусе `SUBMITTED`, доступных пользователю.
- `GET /api/v1/change-requests/{cr_id}` — детальная информация о заявке (требуются права `READER` в кластере заявки).
- `POST /api/v1/change-requests` — создание заявки на изменение очередей. Доступно: `WRITER`, `ADMIN`.
- `POST /api/v1/change-requests/{cr_id}/approve` — согласование заявки и генерация XML (защищено `DistributedLock`). Доступно: только `ADMIN`.
- `POST /api/v1/change-requests/{cr_id}/deploy` — запуск задачи автоматизированной доставки и применения конфигурации через **Ansible AWX**. Доступно: только `ADMIN`.
- `GET /api/v1/change-requests/{cr_id}/deploy-status` — получение актуального статуса исполнения задачи деплоя в AWX и консольного вывода (stdout).
- `POST /api/v1/change-requests/{cr_id}/reject` — отклонение заявки (защищено `DistributedLock`). Доступно: только `ADMIN`.
- `POST /api/v1/change-requests/{cr_id}/cancel` — отзыв заявки (доступно автору заявки или `ADMIN`).

Подробное руководство по архитектуре, настройке и запуску AWX деплоя описано в [docs/awx-yarn-deployment.md](../../docs/awx-yarn-deployment.md).

### Системные эндпоинты
- `GET /healthz` — проверка жизнеспособности сервиса (`{"status": "ok"}`) для Kubernetes Liveness/Readiness probes (без авторизации).
- `GET /metrics` и `GET /api/v1/metrics` — экспорт метрик Circuit Breaker и состояния очередей в формате Prometheus для мониторинга.

---

## 🧪 Запуск тестов

```bash
make test-yarn
# либо
uv run pytest backend/yarn/tests -v
```

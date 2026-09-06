# Backend: YARN Queue Explorer

Бэкенд-сервис приложения **YARN Queue Explorer**, реализованный на базе **FastAPI (Python 3.12)**. Сервис обеспечивает взаимодействие с кластерами Apache Hadoop YARN через Kerberos SPNEGO, корпоративную аутентификацию пользователей через OpenLDAP / Active Directory или Mock/Local провайдеры, персистентное хранение заявок на согласование в SQLite и безопасную генерацию конфигурации `capacity-scheduler.xml`.

---

## 🏛 Архитектура компонентов бэкенда

```
backend/
├── app/
│   ├── api/                     # REST API контроллеры (v1)
│   │   ├── auth.py              # Аутентификация (/api/v1/auth/login, /sso, /me, /logout)
│   │   ├── clusters.py          # Список кластеров (/api/v1/clusters)
│   │   ├── queues.py            # Очереди, валидация, diff, XML (/api/v1/clusters/{cluster_id}/...)
│   │   └── change_requests.py   # Управление заявками (/api/v1/change-requests)
│   ├── core/                    # Ядро сервиса
│   │   ├── acl.py               # Проверка ACL (check_ui_access, resolve_cluster_role, check_cluster_permission)
│   │   ├── audit.py             # Структурированный аудит безопасности и изменений очередей
│   │   ├── config.py            # Pydantic Settings, загрузка config.yaml
│   │   ├── kerberos.py          # KerberosManager (kinit, SPNEGO)
│   │   ├── ldap_auth.py         # LdapService с защитой от LDAP-инъекций и валидацией TLS
│   │   ├── rate_limiter.py      # Rate Limiting (Sliding Window через StorageService)
│   │   └── security.py          # JWT-токены, get_current_user с валидацией UI ACL
│   ├── models/                  # Pydantic-модели и схемы данных
│   │   ├── auth.py              # UserSession, Role, TokenResponse, LoginRequest
│   │   ├── cluster.py           # ClusterConfig, ClusterAcl, ClusterResources
│   │   ├── yarn.py              # QueueNode, QueueDraftItem (с regex-валидацией), PartitionResourceConfig
│   │   └── change_requests.py   # ChangeRequestCreate, ChangeRequestReview, ChangeRequestResponse
│   ├── services/                # Бизнес-логика
│   │   ├── capacity_scheduler.py# Алгоритмы проверки баланса очередей
│   │   ├── mock_yarn.py         # Mock данные для dev режима
│   │   ├── storage.py           # Tri-Storage: Redis, PostgreSQL, SQLite
│   │   ├── xml_generator.py     # Точечная модификация capacity-scheduler.xml с санитизацией
│   │   └── yarn_client.py       # REST API клиент YARN RM с поддержкой Kerberos SPNEGO и HA
│   └── main.py                  # Входная точка FastAPI, безопасный CORS, Security Headers, /healthz
├── tests/                       # Автоматические тесты на pytest
│   ├── conftest.py              # Автосброс rate limits в тестах
│   ├── test_capacity_scheduler.py # Тесты балансировки и генерации XML
│   ├── test_change_requests.py   # Тесты CRUD хранилища заявок
│   └── test_security.py          # Тесты безопасности (инъекции, BOLA, ACL, валидация)
└── requirements.txt             # Зависимости Python
```

---

## 🛡️ Безопасность (Security Architecture)

В сервисе реализован комплекс защитных мер для соответствия лучшим практикам информационной безопасности (OWASP Top 10):

1. **Строгая защита от CSRF**:
   - Валидация источников через `urllib.parse.urlparse` со строгим сопоставлением с `server.cors_origins` и `Host` заголовком. Режим Fail-Closed отклоняет мутирующие cookie-запросы без валидных источников.
2. **Защита от инъекций и XXE**:
   - **XXE & DoS Protection**: Парсинг XML через `defusedxml.ElementTree` с блокировкой entity expansion, DTD и billion laughs атак.
   - **LDAP Filter Injection**: Входные данные экранируются через `ldap3.utils.conv.escape_filter_chars` перед передачей в фильтры поиска каталогов.
   - **XML Comment / Configuration Injection**: Поля `comment` и `generated_by` экранируются функцией `_sanitize_xml_comment`, исключающей разрыв XML-комментариев (`-->`) и внедрение недопустимых свойств в `capacity-scheduler.xml`.
3. **Защита от BOLA / IDOR и принцип Four-Eyes**:
   - Доступ к деталям заявки (`GET /api/v1/change-requests/{id}`) строго ограничен правами пользователя в соответствующем кластере.
   - Запрещено самостоятельное одобрение автором своей собственной заявки (`Four-Eyes Principle`).
4. **Двухуровневый контроль доступа (RBAC & UI ACL)**:
   - `check_ui_access`: проверка права доступа пользователя к интерфейсу и API на основе глобальных политик `acl.ui_access`.
   - `resolve_cluster_role` & `check_cluster_permission`: гранулярное разделение прав по каждому кластеру (ADMIN, WRITER, READER).
   - Обогащение LDAP-группами при Kerberos SPNEGO SSO (`/api/v1/auth/sso`) для корректного назначения ролей.
5. **Потокобезопасность сессий**:
   - Изоляция сессий `requests.Session` на каждый асинхронный вызов к YARN RM, исключающая гонки данных и утечки сессий.
6. **Серверная инвалидация токенов и персистентность (SQLAlchemy Core)**:
   - Хранилище заявок (Change Requests) и черного списка токенов на базе `SQLAlchemy Core` с поддержкой как `SQLite` (WAL), так и `PostgreSQL`.
7. **Защита от брутфорса и IP-спуфинга (Rate Limiting)**:
   - Эндпоинты аутентификации защищены ограничителем частоты запросов с защитой от IP-спуфинга (доверяет `X-Forwarded-For` только от доверенных прокси).
8. **Безопасные сессии (HttpOnly Cookies) и CSP**:
   - Токены принимаются через `Authorization: Bearer` или `HttpOnly`, `SameSite=Lax`, `Path=/` (и `Secure` в продакшне) Cookie. Токены в query-параметрах заблокированы.
   - Защитные заголовки Content-Security-Policy: `default-src 'self'`, `object-src 'none'`, `base-uri 'self'`, `form-action 'self'`, `frame-ancestors 'none'`.
9. **Строгая изоляция Mock-пользователей**:
   - Локальные mock-пользователи разрешены исключительно при режиме `auth.mode: "mock"`.
10. **Управление секретами через Kubernetes Secret**:
    - Чувствительные параметры (`JWT_SECRET_KEY`, `LDAP_BIND_PASSWORD`, `DATABASE_PASSWORD`, `Keytab`) монтируются через Kubernetes `Secret`.

---

## 🌐 Спецификация REST API

### Аутентификация (`/api/auth`)
- `POST /api/auth/login` — аутентификация по логину и паролю (LDAP / Mock / Hybrid). Выставляет `HttpOnly` cookie `access_token` и возвращает JWT токен. Защищено Rate Limiter (10 запросов в минуту).
- `POST /api/auth/spnego` — аутентификация Kerberos SPNEGO SSO через заголовок `Authorization: Negotiate <token>`. Защищено Rate Limiter.
- `GET /api/auth/me` — получение профиля текущего пользователя и его роли.
- `POST /api/auth/logout` — завершение сессии, серверный отзыв токена (blacklist) и удаление сессионной cookie.

### Кластеры (`/api/clusters`)
- `GET /api/clusters` — список доступных пользователю YARN-кластеров с ролями и метаданными.

### Очереди и моделирование (`/api/clusters/{cluster_id}`)
- `GET /api/clusters/{cluster_id}/queues` — получение дерева очередей и метрик утилизации кластера. Доступно: `READER`, `WRITER`, `ADMIN`.
- `POST /api/clusters/{cluster_id}/validate` — валидация баланса ресурсов веток очередей (RAM / vCPU). Доступно: `WRITER`, `ADMIN`.
- `POST /api/clusters/{cluster_id}/diff` — расчет дельты изменений между live и draft состоянием. Доступно: `WRITER`, `ADMIN`.
- `POST /api/clusters/{cluster_id}/generate-xml` — генерация `capacity-scheduler.xml`. Доступно: только `ADMIN`.

### Заявки на согласование (`/api/change-requests`)
- `GET /api/change-requests` — список заявок с фильтрацией по кластеру и статусу (только для разрешенных кластеров).
- `GET /api/change-requests/pending-count` — количество заявок в статусе `SUBMITTED`, доступных пользователю.
- `GET /api/change-requests/{cr_id}` — детальная информация о заявке (требуются права `READER` в кластере заявки).
- `POST /api/change-requests` — создание заявки на изменение очередей. Доступно: `WRITER`, `ADMIN`.
- `POST /api/change-requests/{cr_id}/approve` — согласование заявки и генерация XML. Доступно: только `ADMIN`.
- `POST /api/change-requests/{cr_id}/reject` — отклонение заявки. Доступно: только `ADMIN`.
- `POST /api/change-requests/{cr_id}/cancel` — отзыв заявки (доступно автору заявки или `ADMIN`).

### Системные эндпоинты
- `GET /health` — проверка жизнеспособности сервиса (`{"status": "ok"}`) для Kubernetes Liveness/Readiness probes (без авторизации).

---

## 💻 Локальный запуск и разработка

### 1. Подготовка окружения
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Переменные окружения
| Переменная | Описание | Значение по умолчанию |
|---|---|---|
| `CONFIG_PATH` | Путь к файлу конфигурации `config.yaml` | `config/config.yaml` |
| `DB_PATH` | Путь к файлу SQLite базы данных | `data/yarn_explorer.db` |
| `JWT_SECRET_KEY` | Секретный ключ подписи JWT (переопределяет `auth.jwt.secret_key`) | Из `config.yaml` |
| `LDAP_BIND_PASSWORD` | Пароль сервисной учетной записи LDAP (переопределяет `auth.ldap.bind_password`) | Из `config.yaml` |
| `SERVER_DEBUG` | Переопределение режима отладки (`true` / `false`) | `false` |
| `CORS_ORIGINS` | Разрешенные origins через запятую (например, `http://localhost:8080,http://localhost:5173`) | Из `config.yaml` |
| `KRB5_CONFIG` | Путь к файлу `krb5.conf` | `/etc/krb5.conf` |
| `PYTHONUNBUFFERED` | Отключение буферизации вывода логов | `1` |

### 3. Запуск сервера разработки
```bash
export CONFIG_PATH=../demo/config.yaml
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

---

## 🧪 Тестирование

Запуск полного набора автоматических тестов с помощью `pytest`:
```bash
pytest backend/tests -v
```

Тестирование покрывает:
- Балансировку емкости очередей и контроль инварианта 100%.
- Генерацию `capacity-scheduler.xml` с сохранением сторонних параметров.
- Защиту от инъекций (XML Comment Injection, LDAP Filter Injection).
- Защиту от BOLA / IDOR в API заявок Change Requests.
- Проверку политик доступа UI ACL.
- Защиту от Timing Attacks при сравнении учетных данных.
- Проверку наличия защитных HTTP-заголовков и политик CORS.


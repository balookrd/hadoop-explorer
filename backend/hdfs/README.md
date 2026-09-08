# Backend: HDFS Web Explorer

Бэкенд-сервис веб-портала **HDFS Web Explorer**, реализованный на базе **FastAPI (Python 3.12/3.14)**. Сервис обеспечивает взаимодействие с кластерами Apache Hadoop HDFS через протоколы WebHDFS и HttpFS с поддержкой Kerberos SPNEGO, корпоративную аутентификацию (LDAP / Active Directory), потоковый просмотр больших файлов (Parquet, ORC, Avro, CSV, JSON), аудит операций, защиту от сбоев через Circuit Breaker и разграничение прав доступа.

---

## 🏛 Архитектура компонентов бэкенда

```
backend/hdfs/
├── app/
│   ├── api/                     # REST API контроллеры (v1)
│   │   ├── auth.py              # Аутентификация (фабрика create_auth_router из backend.common)
│   │   ├── clusters.py          # Доступные кластеры HDFS (/api/v1/clusters, /cross-copy)
│   │   └── files.py             # Операции с файлами и директориями (/api/v1/clusters/{id}/files)
│   ├── core/                    # Ядро сервиса
│   │   ├── acl.py               # Проверка прав доступа и роли пользователя
│   │   ├── audit.py             # Структурированный аудит операций с HDFS
│   │   ├── config.py            # Pydantic Settings, загрузка config.yaml
│   │   ├── ldap_auth.py         # Безопасная аутентификация через LDAP/AD
│   │   ├── rate_limiter.py      # Rate Limiting (Sliding Window через StorageService)
│   │   └── security.py          # PyJWT, HttpOnly Cookie, CSRF-защита, make_get_current_user, resolve_system_role
│   ├── models/                  # Pydantic-схемы данных
│   │   ├── cluster.py           # Конфигурация кластеров
│   │   └── hdfs.py              # Статусы файлов, операции чтения/записи
│   ├── services/                # Бизнес-логика
│   │   ├── hdfs_client.py       # Клиент WebHDFS/HttpFS с поддержкой KerberosManager и Circuit Breaker
│   │   ├── mock_hdfs.py         # Mock данные для dev-режима
│   │   ├── preview.py           # Потоковый просмотр файлов Parquet/ORC (PyArrow)
│   │   └── storage.py           # Конфигурация хранилища на базе SessionStore
│   ├── docker-entrypoint.sh     # Инициализация Kerberos (kinit) и запуск uvicorn
│   └── main.py                  # Входная точка FastAPI, CORS, Security Headers, Graceful Shutdown, /healthz
├── tests/                       # Автоматические тесты (pytest - 51 тест)
│   ├── conftest.py              # Автосброс rate limits в тестах
│   ├── test_acl.py              # Тесты проверки прав доступа
│   ├── test_api.py              # Тесты основных API эндпоинтов
│   ├── test_common_modules.py   # Тесты интеграции с backend/common (Circuit Breaker, SessionStore)
│   ├── test_cross_cluster_copy.py # Тесты надежности межкластерного копирования
│   ├── test_parquet_orc_preview.py # Тесты предпросмотра Parquet и ORC
│   └── test_security.py         # Тесты CSRF, Security Headers, Rate Limit, Auth
├── pyproject.toml               # Конфигурация пакета hadoop-explorer-hdfs
└── requirements.txt             # Зависимости Python
```

---

## 🛡️ Безопасность и отказоустойчивость (Security & Resilience)

1. **Строгая защита от CSRF**:
   - Валидация источников через `urllib.parse.urlparse` с точным сравнением схемы, хоста и порта с `server.cors_origins` и `Host` заголовком.
   - Режим Fail-Closed: обязательное отклонение (HTTP 403) для всех мутирующих запросов (`POST`, `PUT`, `DELETE`, `PATCH`) при Cookie-сессии в случае отсутствия или несовпадения источников.
2. **Отказоустойчивость вызовов WebHDFS (Circuit Breaker & HA)**:
   - Встроенный `CircuitBreaker` на уровне клиента `HDFSClient`: мгновенный отказ (Fast-Fail) при недоступности NameNode без зависания пулов потоков.
   - Автоматический failover на standby NameNode при сбоях active узла. Игнорирование 4xx клиентских ошибок.
3. **Защита от SSRF (Server-Side Request Forgery)**:
   - Функция `validate_webhdfs_location` для проверки редиректов NameNode -> DataNode (307 Redirects). Блокировка Cloud Metadata (AWS `169.254.169.254`, GCP `metadata.google.internal`, Alibaba `100.100.100.200`) и нелегитимных хостов.
4. **Безопасность JWT и сессий**:
   - Библиотека `PyJWT >= 2.9.0` с защитой от алгоритмических атак.
   - Серверный отзыв токенов при выходе (`/api/v1/auth/logout`) через универсальный Tri-Storage (`StorageService`: Redis, PostgreSQL, SQLite) и L1 In-Memory кэш.
   - Токены принимаются исключительно через `Authorization: Bearer` или `HttpOnly` Cookie.
5. **Контроль частоты запросов (Rate Limiting)**:
   - Хранилище лимитов на базе `StorageService` с защитой от IP Spoofing: доверие `X-Forwarded-For` только от доверенных прокси, для прямых подключений используется реальный IP сокета.
6. **Аутентификация и каталог**:
   - Kerberos SPNEGO SSO с безопасным fallback прав.
   - Строгая проверка TLS-сертификатов LDAPS (`verify_cert: true`).
   - Изоляция тестовых пользователей: `mock_users` активны исключительно при `auth.mode: "mock"`.
7. **Защита от DoS / OOM при файловых операциях**:
   - Потоковая передача чанками при кросс-кластерном копировании и сборке/распаковке ZIP-архивов (Zero-Memory Streaming).
8. **Защитные заголовки Content-Security-Policy (CSP)**:
   - Директивы `default-src 'self'`, `object-src 'none'`, `base-uri 'self'`, `form-action 'self'` и `frame-ancestors 'none'`.
9. **Аудит безопасности**:
   - Все операции создания, изменения, удаления и скачивания файлов логируются с указанием инициатора, реального IP-адреса и результата.
10. **Graceful Shutdown**:
    - Интеграция с `GracefulShutdownManager` для корректного освобождения сетевых сессий и закрытия пулов потоков при остановке пода в Kubernetes.
11. **Ролевая модель и разграничение доступа (RBAC)**:
    - Интеграция с централизованной функцией `resolve_system_role`: автоматическое назначение системных ролей `ADMIN` (полный доступ к операциям и квотам), `WRITER` (запись/модификация в разрешенных директориях), `READER` (только чтение/листинг).

---

## 🧪 Запуск тестов

```bash
make test-hdfs
# либо
uv run pytest backend/hdfs/tests -v
```

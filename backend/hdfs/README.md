# Backend: HDFS Web Explorer

Бэкенд-сервис веб-портала **HDFS Web Explorer**, реализованный на базе **FastAPI (Python 3.12/3.14)**. Сервис обеспечивает взаимодействие с кластерами Apache Hadoop HDFS через протоколы WebHDFS и HttpFS с поддержкой Kerberos SPNEGO, корпоративную аутентификацию (LDAP / Active Directory), потоковый просмотр больших файлов (Parquet, ORC, Avro, CSV, JSON), аудит операций и разграничение прав доступа.

---

## 🏛 Архитектура компонентов бэкенда

```
backend/
├── app/
│   ├── api/                     # REST API контроллеры (v1)
│   │   ├── auth.py              # Аутентификация (/api/v1/auth/login, /sso, /me, /logout)
│   │   ├── clusters.py          # Доступные кластеры HDFS (/api/v1/clusters, /cross-copy)
│   │   └── files.py             # Операции с файлами и директориями (/api/v1/clusters/{id}/files)
│   ├── core/                    # Ядро сервиса
│   │   ├── acl.py               # Проверка прав доступа и роли пользователя
│   │   ├── audit.py             # Структурированный аудит операций с HDFS
│   │   ├── config.py            # Pydantic Settings, загрузка config.yaml
│   │   ├── kerberos.py          # KerberosClient (kinit, SPNEGO)
│   │   ├── ldap_auth.py         # Безопасная аутентификация через LDAP/AD
│   │   ├── rate_limiter.py      # Rate Limiting (Sliding Window через StorageService)
│   │   └── security.py          # PyJWT, HttpOnly Cookie, CSRF-защита
│   ├── models/                  # Pydantic-схемы данных
│   │   ├── auth.py              # Схемы аутентификации и пользователей
│   │   ├── cluster.py           # Конфигурация кластеров
│   │   └── hdfs.py              # Статусы файлов, операции чтения/записи
│   ├── services/                # Бизнес-логика
│   │   ├── hdfs_client.py       # Клиент WebHDFS/HttpFS с поддержкой Kerberos
│   │   ├── mock_hdfs.py         # Mock данные для dev-режима
│   │   ├── preview.py           # Потоковый просмотр файлов Parquet/ORC (PyArrow)
│   │   └── storage.py           # Tri-Storage: Redis, PostgreSQL, SQLite
│   ├── docker-entrypoint.sh     # Инициализация Kerberos (kinit) и запуск uvicorn
│   └── main.py                  # Входная точка FastAPI, CORS, Security Headers, /healthz
├── tests/                       # Автоматические тесты (pytest - 39 тестов)
│   ├── conftest.py              # Автосброс rate limits в тестах
│   ├── test_acl.py              # Тесты проверки прав доступа
│   ├── test_api.py              # Тесты основных API эндпоинтов
│   ├── test_common_modules.py   # Тесты интеграции с backend/common
│   ├── test_cross_cluster_copy.py # Тесты надежности межкластерного копирования
│   ├── test_parquet_orc_preview.py # Тесты предпросмотра Parquet и ORC
│   └── test_security.py         # Тесты CSRF, Security Headers, Rate Limit, Auth
├── pyproject.toml               # Конфигурация пакета hadoop-explorer-hdfs
└── requirements.txt             # Зависимости Python
```

---

## 🛡️ Безопасность (Security Architecture)

1. **Строгая защита от CSRF**:
   - Валидация источников через `urllib.parse.urlparse` с точным сравнением схемы, хоста и порта с `server.cors_origins` и `Host` заголовком.
   - Режим Fail-Closed: обязательное отклонение (HTTP 403) для всех мутирующих запросов (`POST`, `PUT`, `DELETE`, `PATCH`) при Cookie-сессии в случае отсутствия или несовпадения источников.
2. **Защита от SSRF (Server-Side Request Forgery)**:
   - Функция `validate_webhdfs_location` для проверки редиректов NameNode -> DataNode (307 Redirects). Блокировка Cloud Metadata (AWS `169.254.169.254`, GCP `metadata.google.internal`, Alibaba `100.100.100.200`) и нелегитимных хостов.
3. **Безопасность JWT и сессий**:
   - Библиотека `PyJWT >= 2.9.0` с защитой от алгоритмических атак.
   - Серверный отзыв токенов при выходе (`/api/v1/auth/logout`) через универсальный Tri-Storage (`StorageService`: Redis, PostgreSQL, SQLite).
   - Токены принимаются исключительно через `Authorization: Bearer` или `HttpOnly` Cookie.
4. **Контроль частоты запросов (Rate Limiting)**:
   - Хранилище лимитов на базе `StorageService` с защитой от IP Spoofing: доверие `X-Forwarded-For` только от доверенных прокси, для прямых подключений используется реальный IP сокета.
5. **Аутентификация и каталог**:
   - Kerberos SPNEGO SSO с безопасным fallback прав.
   - Строгая проверка TLS-сертификатов LDAPS (`verify_cert: true`).
   - Изоляция тестовых пользователей: `mock_users` активны исключительно при `auth.mode: "mock"`.
6. **Защита от DoS / OOM при файловых операциях**:
   - Потоковая передача чанками при кросс-кластерном копировании и сборке/распаковке ZIP-архивов (Zero-Memory Streaming).
7. **Защитные заголовки Content-Security-Policy (CSP)**:
   - Директивы `default-src 'self'`, `object-src 'none'`, `base-uri 'self'`, `form-action 'self'` и `frame-ancestors 'none'`.
8. **Аудит безопасности**:
   - Все операции создания, изменения, удаления и скачивания файлов логируются с указанием инициатора, реального IP-адреса и результата.

---

## 🧪 Запуск тестов

```bash
PYTHONPATH=. pytest
```

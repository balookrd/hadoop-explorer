# Backend: Spark Web Explorer

Бэкенд-сервис портала аналитических вычислений **Spark Explorer (PySpark, Scala Spark, Spark SQL)**, реализованный на базе **FastAPI (Python 3.12/3.14)** в рамках платформы **Hadoop Explorer**.

Сервис предоставляет единый высокопроизводительный API для управления интерактивными сессиями Spark на кластерах **Apache Hadoop YARN** и **Kubernetes** через **Apache Livy REST API**, обеспечивает централизованную аутентификацию (LDAPS / Kerberos SPNEGO), аудит безопасности, отказоустойчивость вызовов к Livy через **Circuit Breaker**, кэширование метаданных каталога Hive Metastore / Iceberg, персистентное сохранение рабочих пространств пользователей и изоляцию контекстов.

---

## 🏛️ Архитектура компонентов бэкенда

```
backend/spark/
├── app/
│   ├── api/                     # REST API контроллеры (v1)
│   │   ├── auth.py              # Аутентификация (фабрика create_auth_router из backend.common)
│   │   ├── catalog.py           # Метаданные Hive/Iceberg (/api/v1/metastore/{cluster_id}/databases, tables)
│   │   ├── clusters.py          # Доступные кластеры и конфигурации (/api/v1/clusters)
│   │   ├── sessions.py          # Интерактивные сессии Livy (/api/v1/sessions/create, list, delete, status)
│   │   ├── statements.py        # Выполнение кода (/api/v1/statements/execute, cancel, results)
│   │   └── workspace.py         # Личное рабочее пространство пользователя (/api/v1/workspace)
│   ├── core/                    # Безопасность и интеграция с common-модулями
│   │   ├── config.py            # Pydantic Settings конфигурации кластеров, очередей и Livy
│   │   └── security.py          # Интеграция с common.core.security, CSRF, JWT, SessionStore
│   ├── models/                  # Pydantic и SQLAlchemy модели
│   │   ├── models.py            # Pydantic схемы сессий, стейтментов, каталога и UserWorkspace
│   │   └── db_models.py         # SQLAlchemy модель SparkUserWorkspace (SQLite/PostgreSQL)
│   ├── services/                # Бизнес-логика и клиенты внешних систем
│   │   ├── livy_client.py       # Асинхронный HTTP-клиент к Apache Livy с поддержкой SPNEGO Kerberos и Circuit Breaker
│   │   └── mock_spark.py        # Демонстрационный движок MockSparkEngine для автономного режима
│   ├── docker-entrypoint.sh     # Инициализация kinit и запуск сервиса uvicorn
│   └── main.py                  # Точка входа FastAPI, CORS, Security Headers, Graceful Shutdown, Healthcheck (/healthz)
├── config/
│   └── config.yaml              # Конфигурационный файл по умолчанию
├── tests/                       # Автоматические тесты (pytest - 16 тестов)
│   ├── test_spark.py            # Тесты Livy клиента, валидаторов, MockSparkEngine, UserWorkspace, Logout cleanup
│   └── test_spark_circuit_breaker.py # Тесты Circuit Breaker для Livy вызовов
└── pyproject.toml               # Конфигурация пакета hadoop-explorer-spark
```

---

## 🛡️ Безопасность и архитектурные решения

1. **Двухуровневая аутентификация, SSO и автоочистка сессий Livy**:
   - Поддержка Kerberos SPNEGO Negotiate (`Authorization: Negotiate <ticket>`) и защищённого входа по логину/паролю через корпоративный LDAPS / Active Directory.
   - Выпуск защищённых `HttpOnly`, `SameSite=Lax`, `Secure` Cookie-токенов.
   - Изоляция сессий в персистентном `SessionStore` (PostgreSQL / SQLite WAL) с мгновенным отзывом при выходе.
   - При логауте пользователя (`POST /api/v1/auth/logout`) все связанные интерактивные сессии Livy автоматически закрываются и освобождают ресурсы YARN кластера.
   - Поддержка системных ролей `ADMIN`, `WRITER`, `READER` через централизованную `resolve_system_role`.

2. **Отказоустойчивость сетевых вызовов (Circuit Breaker & Prometheus)**:
   - Защита вызовов к Apache Livy через автомат состояний `CircuitBreaker`. При отказе Livy бэкенд мгновенно возвращает понятную ошибку (Fast-Fail) без блокировки пула потоков.
   - Клиентские ошибки (4xx) исключены из счетчика отказов.
   - Экспорт метрик состояний Circuit Breaker в формате Prometheus на эндпоинтах `/metrics` и `/api/v1/metrics`.

3. **Защита от межсайтовой подделки запросов (CSRF)**:
   - Проверка заголовков `Origin` и `Referer` против белого списка доверенных доменов.
   - Блокировка междоменных запросов (`Sec-Fetch-Site: cross-site`).
   - Требование заголовка `X-Requested-With` для API вызовов.

4. **Разграничение доступа к очередям YARN и ресурсам кластера (ACL)**:
   - Сопоставление LDAP-групп пользователя со списком разрешённых очередей YARN (`default_queue`, `available_queues`).
   - Лимиты на максимальные ресурсы драйвера и исполнителей (Cores, Memory, Max Executors).

5. **Изоляция и персистентность контекстов пользователей (`User Workspace`)**:
   - Эндпоинт `/api/v1/workspace` обеспечивает сохранение открытых вкладок, кода по всем трем языкам (`codeBuffers`) и последних результатов вычислений (`resultBuffers`).
   - Изоляция в БД по уникальному логину пользователя: при входе другого пользователя история и код предыдущего пользователя не отображаются.

6. **Гибкая интеграция с Apache Livy**:
   - Поддержка интерактивных сессий `pyspark`, `spark` (Scala), `sparkr` и `sql`.
   - Проброс Kerberos-билетов для доступа Livy к защищённым сервисам HDFS и YARN.
   - Автоматический сбор и парсинг логов выполнения, статусов `idle`, `busy`, `dead`, `success`, `error`.
   - Режим `MockSparkEngine` для локальной разработки и непрерывного CI-тестирования без развертывания реального кластера Hadoop.

7. **Централизованная обработка исключений и Graceful Shutdown**:
   - Централизованная обработка непредвиденных исключений `setup_global_exception_handlers` с маскированием и логированием `incident_id`.
   - Регистрация в `GracefulShutdownManager` для корректного освобождения фоновых задач и сессий при остановке контейнера.

---

## ⚙️ Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SPARK_CONFIG_PATH` | `config/config.yaml` | Путь к конфигурационному YAML-файлу |
| `DATABASE_URL` | `sqlite+aiosqlite:///data/spark_explorer.db` | Строка подключения к БД для сессий и воркспейсов |
| `LIVY_URL` | `http://spark-demo-livy:8998` | URL-адрес сервера Apache Livy |
| `HIVE_METASTORE_URI` | `thrift://sql-demo-hive-metastore:9083` | Адрес сервиса Apache Hive Metastore |
| `KRB5_CONFIG` | `/etc/krb5.conf` | Путь к конфигурации Kerberos |
| `KRB5_KEYTAB` | `/etc/security/keytabs/spark.keytab` | Путь к сервисному keytab файлу |

---

## 🧪 Запуск автоматических тестов

```bash
# Из корня монорепозитория:
make test-spark

# Либо напрямую через uv / pytest:
uv run pytest backend/spark/tests -v
```

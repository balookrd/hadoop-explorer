# Backend: HDFS Web Explorer (Java 21 / Spring Boot 3)

Бэкенд-сервис веб-портала **HDFS Web Explorer**, реализованный на базе **Java 21 LTS** и **Spring Boot 3.3.4**. Сервис обеспечивает высокопроизводительное прямое взаимодействие с кластерами Apache Hadoop HDFS через официальный клиент (`org.apache.hadoop:hadoop-hdfs-client:3.3.6`), автоматический **HA Failover NameNode**, поддержку Kerberos Proxy User doAs-имперсонации, корпоративную аутентификацию (LDAP / Kerberos SPNEGO SSO через `common-security-starter`), потоковый предпросмотр больших данных (Parquet, ORC, CSV, JSON), управление ACL и квотами.

---

## 🏛 Архитектура компонентов бэкенда

```
backend/hdfs/
├── hdfs-java/                    # Нативный сервис на Java 21 / Spring Boot 3
│   ├── pom.xml                   # Maven проект (org.apache.hadoop.explorer:hdfs-explorer-java:1.0.0)
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/             # Контроллеры, Сервисы, Клиенты, Сущности, Модели
│   │   │   └── resources/        # application.yml, статика фронтенда SPA (Svelte 5)
│   │   └── test/                 # Юнит- и интеграционные тесты (Spring Boot Test, MockMvc)
│   └── README.md                 # Подробная документация сервиса
└── README.md                     # Документация модуля
```

---

## 🛡️ Безопасность и отказоустойчивость (Security & Resilience)

1. **High Availability NameNode Failover**:
   - Автоматическая маршрутизация запросов к активной NameNode с автоопределением `dfs.nameservices` и `ConfiguredFailoverProxyProvider`.
   - Прозрачное переключение при `StandbyException`.
2. **Имперсонация пользователей (Kerberos Proxy User `doAs`)**:
   - Выполнение файловых операций от имени аутентифицированного пользователя при наличии привилегий у сервисного аккаунта в `core-site.xml` (`hadoop.proxyuser.*`).
   - Сохранение прозрачности для аудита Apache Ranger и HDFS Audit Logs.
3. **Отказоустойчивость и Circuit Breaker**:
   - Защита исходящих вызовов к кластерам HDFS через `SimpleCircuitBreaker` (`common-security-starter`). Мгновенный отказ (Fast-Fail) при падении NameNode без блокировки потоков.
   - Экспорт метрик состояний автоматов защиты в формате Prometheus.
4. **Безопасный предпросмотр Parquet и ORC**:
   - Чтение метаданных схемы и первых N строк без полной загрузки файла в память с использованием нативных Java библиотек (`parquet-hadoop`, `orc-core`).
5. **Общее ядро безопасности `common-security-starter`**:
   - Корпоративная аутентификация: Kerberos SPNEGO SSO, LDAP/Active Directory.
   - Двухуровневое хранилище сессий: L1 In-Memory (Caffeine) + L2 JDBC.
   - Защита от CSRF, Bucket4j Rate Limiter и AOP-аудит `@Audited`.

---

## 🌐 Спецификация REST API

### Аутентификация (`/api/v1/auth`)
- `POST /api/v1/auth/login` — аутентификация по логину и паролю.
- `POST /api/v1/auth/sso` — аутентификация Kerberos SPNEGO SSO.
- `GET /api/v1/auth/me` — профиль текущего пользователя и его роли.
- `POST /api/v1/auth/logout` — завершение сессии и отзыв токена.

### Кластеры (`/api/v1/clusters`)
- `GET /api/v1/clusters` — список доступных пользователю HDFS-кластеров.

### Файловые операции (`/api/v1/clusters/{cluster_id}/files`)
- `GET /api/v1/clusters/{cluster_id}/files?path=/path` — листинг директории (имена, размеры, репликация, владелец, права, mtime).
- `GET /api/v1/clusters/{cluster_id}/files/content?path=/path` — скачивание содержимого файла.
- `GET /api/v1/clusters/{cluster_id}/files/preview?path=/path&limit=100` — структурированный предпросмотр (Parquet, ORC, CSV, JSON).
- `POST /api/v1/clusters/{cluster_id}/files/upload` — загрузка файла в HDFS.
- `POST /api/v1/clusters/{cluster_id}/files/mkdir` — создание директории.
- `DELETE /api/v1/clusters/{cluster_id}/files?path=/path&recursive=true` — удаление файла или директории.
- `GET /api/v1/clusters/{cluster_id}/files/acl?path=/path` — просмотр ACL прав доступа.
- `GET /api/v1/clusters/{cluster_id}/files/quota?path=/path` — просмотр квот пространства и namespace (ContentSummary).

### Системные эндпоинты
- `GET /health` / `GET /actuator/health` — проверка состояния сервиса.
- `GET /metrics` / `GET /actuator/prometheus` — экспорт метрик Prometheus.

---

## 🧪 Запуск тестов и сборка

```bash
# Запуск тестов HDFS Explorer (16 тестов)
make test-hdfs
# либо
make test-hdfs-java

# Сборка исполняемого Spring Boot fat JAR (со встроенной фронтенд-статикой)
make build-hdfs
# либо
make build-hdfs-java
# Результат: backend/hdfs/hdfs-java/target/hdfs-explorer-java-1.0.0.jar
```

# HDFS Explorer API (Java 21 / Spring Boot 3)

Высокопроизводительный бэкенд файлового менеджера Apache HDFS на стеке **Java 21 LTS** и **Spring Boot 3.3.4**, построенный на базе нативного клиента Apache Hadoop (`hadoop-client`), с поддержкой High Availability (HA) NameNode failover, Kerberos doAs имперсонации и общей безопасности `common-security-starter`.

---

## 1. Архитектура и функциональные возможности

Сервис обеспечивает полный цикл работы с распределенной файловой системой HDFS для веб-интерфейса HDFS Explorer:

### Структура проекта
```
backend/hdfs/
├── pom.xml                   # Maven проект (org.apache.hadoop.explorer:hdfs-explorer-java:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, Клиенты, Сущности, Модели
│   │   └── resources/        # application.yml, статика фронтенда SPA (Svelte 5)
│   └── test/                 # Юнит- и интеграционные тесты (Spring Boot Test, MockMvc)
└── README.md                 # Документация модуля
```

### Ключевые компоненты

1. **Нативный Hadoop FileSystem Client (`NativeHdfsClient`)**:
   - Работает напрямую через официальные библиотеки `org.apache.hadoop:hadoop-common` и `org.apache.hadoop:hadoop-hdfs-client` (версия 3.3.6).
   - **High Availability (HA) NameNode Failover**: автоматическая конфигурация `dfs.nameservices`, `dfs.ha.namenodes`, `dfs.namenode.rpc-address` и `ConfiguredFailoverProxyProvider` при наличии нескольких адресов NameNode.
   - **Kerberos & doAs имперсонация**: вход сервиса в KDC по keytab (`UserGroupInformation.loginUserFromKeytabAndReturnUGI`) и безопасное выполнение операций от имени конкретного пользователя через `ugi.doAs(...)` (полная прозрачность для Apache Ranger ACL и Ranger Audit Log).
   - Локализация ошибок (`HdfsErrorTranslator`): преобразование `AccessControlException`, `FileNotFoundException`, `FileAlreadyExistsException`, `PathIsNotEmptyDirectoryException`, `SafeModeException`, `StandbyException`, `QuotaExceededException` в понятные сообщения на русском языке.

2. **Эмулятор файловой системы (`MockHdfsClient`)**:
   - Потокобезопасная in-memory файловая система для локальной разработки, быстрого демо и юнит-тестирования без необходимости запуска живого Hadoop-кластера.
   - Автоматическое создание домашних директорий пользователей (`/user/{username}`).

3. **Интеграция с ядром безопасности `common-security-starter`**:
   - Централизованная аутентификация через JWT Cookies (`access_token`, `hdfs_explorer_session`, `hadoop_explorer_session`).
   - Поддержка Kerberos SPNEGO SSO, корпоративного каталога LDAP / Active Directory и Mock-профилей.
   - Строгая CSRF-защита мутирующих HTTP-запросов.
   - Аудит критических операций `@Audited` (создание директорий, загрузка, переименование, удаление, межкластерное копирование).
   - Защита от перегрузок (Rate Limiting via Bucket4j).
   - Отказоустойчивость: защита вызовов через `SimpleCircuitBreaker` с экспортом метрик в Prometheus.

4. **Сервис предпросмотра файлов (`FilePreviewService`)**:
   - Табличный парсинг и структурированный вывод CSV/TSV файлов (`columns`, `rows`, `rowCount`).
   - Текстовый предпросмотр (JSON, XML, YAML, SQL, Markdown, Shell, Log, Properties) с контролем лимита байт (`preview_max_bytes`) и флагом `truncated`.
   - Распознавание форматов Apache Parquet и Apache ORC с выводом сводной информации без полной загрузки файла в память (`parquet-hadoop`, `orc-core`).

5. **Пакетные и расширенные операции (`HdfsFileOperationService`)**:
   - Обычная и многокомпонентная (chunked) загрузка файлов с отслеживанием статуса сборки.
   - Загрузка и потоковая распаковка ZIP-архивов непосредственно в HDFS.
   - Потоковое скачивание файлов и автоматическая упаковка целых директорий в ZIP на лету.
   - Пакетное удаление (`batch-delete`) и пакетное скачивание (`batch-download`).
   - Межкластерное копирование (`cross-cluster-copy`) между кластерами HDFS.

---

## 2. Конфигурация (`application.yml`)

```yaml
server:
  port: 8001

hadoop:
  security:
    auth:
      mode: mock # mock, ldap, kerberos
      admin-groups:
        - "hadoop-admins"
        - "superusers"

  hdfs:
    clusters:
      - id: "prod-datalake"
        name: "Production DataLake"
        hdfs-rpc-urls:
          - "hdfs://nn01.prod.example.com:8020"
          - "hdfs://nn02.prod.example.com:8020"
        auth-type: "kerberos"
        service-principal: "hdfs-explorer/hdfs-explorer.prod.example.com@EXAMPLE.COM"
        keytab-path: "/etc/security/keytabs/hdfs-explorer.keytab"
        timeout-seconds: 30
        preview-max-bytes: 1048576
        default-path: "/user/{username}"
        mock-storage: false
        acl:
          allowed-groups: ["data-engineers", "analytics", "hadoop-admins"]
          read-only-groups: ["analytics"]
          admin-groups: ["hadoop-admins"]
```

---

## 3. Сборка и тестирование

```bash
# Модульное и интеграционное тестирование
make test-hdfs
# или
mvn test -f backend/hdfs/pom.xml

# Сборка fat JAR
make build-hdfs
# или
mvn clean package -DskipTests -f backend/hdfs/pom.xml

# Запуск всех Java тестов платформы
make test-java
```

---

## 4. REST API Эндпоинты

| Метод | Путь | Описание | Доступ |
|---|---|---|---|
| `GET` | `/health` / `/actuator/health` | Проверка работоспособности сервиса | Публичный |
| `GET` | `/metrics` / `/actuator/prometheus` | Экспорт метрик Prometheus | Публичный |
| `POST` | `/api/v1/auth/login` | Вход пользователя (получение JWT Cookie) | Публичный |
| `POST` | `/api/v1/auth/sso` | Kerberos SPNEGO SSO вход | Публичный |
| `GET` | `/api/v1/auth/me` | Профиль текущего пользователя и его роли | Аутентифицирован |
| `POST` | `/api/v1/auth/logout` | Завершение сессии и отзыв токена | Аутентифицирован |
| `GET` | `/api/v1/clusters` | Список доступных HDFS кластеров с правами | Аутентифицирован |
| `POST` | `/api/v1/clusters/cross-copy` | Межкластерное копирование файлов и папок | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/files` | Листинг файлов директории | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/files/preview` | Предпросмотр содержимого файла | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/files/download` | Потоковое скачивание файла/папки (ZIP) | Аутентифицирован |
| `POST` | `/api/v1/clusters/{id}/files/upload` | Загрузка файла (multipart/form-data) | Запись |
| `POST` | `/api/v1/clusters/{id}/files/upload-chunk` | Загрузка части файла (chunk) | Запись |
| `GET` | `/api/v1/clusters/{id}/files/upload-chunk/status`| Статус сборки чанков | Аутентифицирован |
| `POST` | `/api/v1/clusters/{id}/files/upload-archive`| Загрузка и распаковка ZIP-архива | Запись |
| `POST` | `/api/v1/clusters/{id}/files/mkdir` | Создание директории | Запись |
| `POST` | `/api/v1/clusters/{id}/files/rename` | Переименование или перемещение | Запись |
| `DELETE`| `/api/v1/clusters/{id}/files/delete` | Удаление файла или каталога | Запись |
| `POST` | `/api/v1/clusters/{id}/files/batch-delete` | Пакетное удаление списка путей | Запись |
| `POST` | `/api/v1/clusters/{id}/files/batch-download` | Пакетное скачивание в едином ZIP | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/files/acl` | Просмотр ACL прав доступа | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/files/quota` | Просмотр квот пространства и namespace | Аутентифицирован |

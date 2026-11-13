# Sklad API (Java 21 / Spring Boot 3)

Бэкенд Sklad на Java 21 LTS и Spring Boot 3.3.4. Работает с HDFS через нативный клиент Apache Hadoop (`hadoop-client`), переключается между NameNode в HA-конфигурации, выполняет операции от имени пользователя через Kerberos doAs и использует стартер безопасности `zastava`.

---

## 1. Устройство модуля

### Структура проекта
```
backend/sklad/
├── pom.xml                   # Maven-проект (io.github.balookrd.pristan:sklad:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, Клиенты, Сущности, Модели
│   │   └── resources/        # application.yml, статика фронтенда SPA (Svelte 5)
│   └── test/                 # Юнит- и интеграционные тесты (Spring Boot Test, MockMvc)
└── README.md                 # Документация модуля
```

### Компоненты

1. Клиент Hadoop FileSystem (`NativeHdfsClient`):
   - Использует библиотеки `org.apache.hadoop:hadoop-common` и `org.apache.hadoop:hadoop-hdfs-client` версии 3.3.6.
   - HA NameNode: если у кластера задано несколько адресов NameNode, клиент сам выставляет `dfs.nameservices`, `dfs.ha.namenodes`, `dfs.namenode.rpc-address` и `ConfiguredFailoverProxyProvider`.
   - Kerberos и doAs: сервис входит в KDC по keytab (`UserGroupInformation.loginUserFromKeytabAndReturnUGI`) и выполняет операции от имени пользователя через `ugi.doAs(...)`, поэтому политики Apache Ranger и Ranger Audit Log видят конечного пользователя, а не сервисную учетную запись.
   - Перевод ошибок (`HdfsErrorTranslator`): `AccessControlException`, `FileNotFoundException`, `FileAlreadyExistsException`, `PathIsNotEmptyDirectoryException`, `SafeModeException`, `StandbyException`, `QuotaExceededException` превращаются в сообщения на русском языке.

2. Эмулятор файловой системы (`MockHdfsClient`):
   - Потокобезопасная in-memory файловая система для локальной разработки, демо и юнит-тестов без Hadoop-кластера.
   - Сам создает домашние каталоги пользователей (`/user/{username}`).

3. Стартер безопасности `zastava`:
   - Аутентификация по JWT в cookie (`access_token`, `sklad_session`, `pristan_session`).
   - Режимы входа: Kerberos SPNEGO SSO, LDAP / Active Directory, mock-профили.
   - CSRF-проверка изменяющих HTTP-запросов.
   - Аудит через `@Audited`: создание каталогов, загрузка, переименование, удаление, межкластерное копирование.
   - Ограничение частоты запросов (Bucket4j).
   - `SimpleCircuitBreaker` на вызовах к кластеру с экспортом метрик в Prometheus.

4. Предпросмотр файлов (`FilePreviewService`):
   - CSV/TSV разбирается в таблицу (`columns`, `rows`, `rowCount`).
   - Текстовые форматы (JSON, XML, YAML, SQL, Markdown, Shell, Log, Properties) читаются не дальше лимита `preview_max_bytes`; если файл длиннее, в ответе выставляется `truncated`.
   - Parquet и ORC распознаются и выводятся сводкой без загрузки всего файла в память (`parquet-hadoop`, `orc-core`).

5. Файловые операции (`HdfsFileOperationService`):
   - Загрузка файла целиком и по частям (chunked) со статусом сборки.
   - Загрузка ZIP-архива с потоковой распаковкой прямо в HDFS.
   - Потоковое скачивание файла; каталог упаковывается в ZIP на лету.
   - Пакетное удаление (`batch-delete`) и пакетное скачивание (`batch-download`).
   - Межкластерное копирование (`cross-cluster-copy`).

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
        service-principal: "sklad/sklad.prod.example.com@EXAMPLE.COM"
        keytab-path: "/etc/security/keytabs/sklad.keytab"
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
make test-sklad
# или
mvn test -f backend/sklad/pom.xml

# Сборка fat-JAR
make build-sklad
# или
mvn clean package -DskipTests -f backend/sklad/pom.xml

# Запуск всех Java-тестов платформы
make test-java
```

---

## 4. REST API-эндпоинты

| Метод | Путь | Описание | Доступ |
|---|---|---|---|
| `GET` | `/health` / `/healthz` / `/actuator/health` | Проверка работоспособности сервиса | Публичный |
| `GET` | `/actuator/prometheus` | Метрики Prometheus (HTTP, операции и трафик HDFS) | Публичный |
| `POST` | `/api/v1/auth/login` | Вход пользователя (получение JWT-cookie) | Публичный |
| `GET` | `/api/v1/auth/sso` | Вход через Kerberos SPNEGO SSO | Публичный |
| `GET` | `/api/v1/auth/me` | Профиль текущего пользователя и его роли | Аутентифицирован |
| `POST` | `/api/v1/auth/logout` | Завершение сессии и отзыв токена | Аутентифицирован |
| `GET` | `/api/v1/clusters` | Список доступных HDFS-кластеров с правами | Аутентифицирован |
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

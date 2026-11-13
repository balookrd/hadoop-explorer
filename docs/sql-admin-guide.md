# Руководство администратора SQL Explorer

Руководство для инженеров DevOps, SRE и системных администраторов: установка, настройка и эксплуатация сервиса SQL Explorer в двух режимах:
1. Standalone — сервер или ВМ под управлением systemd.
2. Docker и Docker Compose.

---

## 1. Архитектура и требования

### 1.1 Функции сервиса
SQL Explorer выполняет SQL-запросы и показывает каталоги метаданных Data Lake:
- Распределенные аналитические запросы через Trino DB API, с отменой долгих запросов и стримингом результатов.
- Подключение к Apache Hive через HiveServer2 и прямой опрос Hive Metastore.
- Каталог баз данных, схем и таблиц с TTL-кэшированием метаданных.
- Рабочее пространство аналитика (`SqlUserWorkspace`), хранимое на сервере: вкладки редактора, сохраненные запросы, история выполнений с метриками.
- ИИ-ассистент для проверки, объяснения, оптимизации, исправления и генерации SQL: OpenAI-совместимая LLM (`sql.ai.provider: openai_compatible`) с откатом на встроенные правила либо только правила (`mock-builtin`, по умолчанию). Что при этом уходит в модель — [CONFIGURATION.md, раздел 5.3](CONFIGURATION.md#53-ии-ассистент-on-premise-llm--ollama--vllm).
- Проверка идентификаторов каталогов, схем и таблиц перед подстановкой в запрос (`CatalogController.validateIdentifier`) — защита от SQL-инъекций.

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8003` | HTTP/HTTPS | Inbound | Пользовательский веб-интерфейс SPA (Svelte 5 + Monaco Editor) и REST API |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Корпоративный сервер аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- Проверка доступности (healthcheck): `GET /healthz`.
- Readiness: `GET /actuator/health/readiness` — 200 `UP` после `ApplicationReadyEvent`, 503 `OUT_OF_SERVICE` с начала остановки контекста приложения.
- Метрики Prometheus: `GET /actuator/prometheus`, см. [4.1](#41-метрики-prometheus-get-actuatorprometheus).

### 1.3 Системные требования

#### Запуск готового JAR или Docker-образа

| Ресурс | Требование |
|---|---|
| ОС | Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12), macOS |
| Среда выполнения | Java 21 LTS JRE (Eclipse Temurin 21 JRE / `openjdk-21-jre-headless`) |
| Системные пакеты | `krb5-user` (`krb5-workstation`), `curl`, `ca-certificates` |
| CPU на реплику | минимум 1 ядро, рекомендуется 2 |
| RAM на реплику | минимум 1.5 ГБ, рекомендуется 2–4 ГБ |
| Диск | 5–10 ГБ |

При установке из готового релиза Git, Apache Maven, Node.js и npm на сервере не нужны.

#### Сборка из исходников
- JDK: `21 LTS`
- Maven: `3.9+`
- Node.js: `20+` / `22 LTS` и npm `10+`

### 1.4 Дистрибутивы
- GitHub Releases (`v${VERSION}`):
  - `sql-explorer-java-${VERSION}.jar` (~50 МБ) — Spring Boot 3 fat JAR, в который включены веб-интерфейс (Svelte 5 SPA, Monaco Editor), коннекторы Trino и Hive, ИИ-ассистент и драйверы БД;
  - `SHA256SUMS.txt` — контрольные суммы SHA-256 для проверки загруженных файлов.
- GitHub Container Registry (GHCR):
  - `ghcr.io/company/pristan/sql:${VERSION}`

---

## 2. Установка на сервер или ВМ (systemd)

### Шаг 1: Установка системных зависимостей

#### Запуск из готового релиза (только JRE)
```bash
# Ubuntu 22.04 / 24.04 LTS, Debian 12:
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless krb5-user ldap-utils curl ca-certificates

# RHEL 8/9, Rocky Linux 8/9, AlmaLinux:
sudo dnf install -y --setopt=install_weak_deps=False \
    java-21-openjdk-headless krb5-workstation openldap-clients curl ca-certificates
```

#### Сборка из исходников на сервере
```bash
# Ubuntu / Debian:
sudo apt-get install -y openjdk-21-jdk maven nodejs npm git
# RHEL / Rocky Linux:
sudo dnf install -y java-21-openjdk-devel maven nodejs npm git
```

### Шаг 2: Создание пользователя и структуры каталогов
```bash
# Создание непривилегированного пользователя appuser
sudo groupadd -g 10001 appuser
sudo useradd -u 10001 -g appuser -m -s /bin/bash appuser

# Создание каталогов сервиса
sudo mkdir -p /opt/pristan/sql/releases
sudo mkdir -p /opt/pristan/sql/bin
sudo mkdir -p /etc/pristan/sql
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/pristan/sql/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/pristan/sql
```

### Шаг 3: Получение JAR

#### Вариант А: загрузка из GitHub Releases

Рекомендуется для промышленной установки. Готовый JAR уже содержит собранный веб-интерфейс и все зависимости.

```bash
sudo -u appuser -i

VERSION="1.0.0"
GITHUB_REPO="company/hadoop-explorer"
RELEASE_DIR="/opt/pristan/sql/releases/v${VERSION}"
BIN_DIR="/opt/pristan/sql/bin"

mkdir -p "${RELEASE_DIR}" "${BIN_DIR}" && cd "${RELEASE_DIR}"

# 1. Загрузка через curl по прямой ссылке (или через `gh release download`):
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/sql-explorer-java-${VERSION}.jar"
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/SHA256SUMS.txt"

# 2. Проверка контрольной суммы:
sha256sum -c SHA256SUMS.txt --ignore-missing

# 3. Создание стабильной символической ссылки:
ln -sfn "${RELEASE_DIR}/sql-explorer-java-${VERSION}.jar" "${BIN_DIR}/sql-explorer.jar"
```

#### Вариант Б: сборка из исходников
```bash
sudo -u appuser -i
cd /tmp
git clone https://github.com/company/hadoop-explorer.git
cd hadoop-explorer

# Сборка фронтенда SQL Explorer (Monaco Editor)
cd frontend
npm ci --workspace=apps/sql --include-workspace-root
# Бандл собирается сразу в backend/sql/src/main/resources/static и попадает в JAR
npm run build --workspace=apps/sql
cd ..

# Сборка Java 21 бэкенда (Spring Boot Fat JAR)
mvn clean package -pl backend/common-security-starter,backend/sql -am -DskipTests

# Копирование собранного JAR в рабочий каталог
cp backend/sql/target/sql-explorer-java-*.jar /opt/pristan/sql/bin/sql-explorer.jar
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/pristan/sql/application.yml`. Юнит systemd на шаге 5 подключает его через `SPRING_CONFIG_ADDITIONAL_LOCATION` поверх `application.yml` из JAR, поэтому указывайте только ключи, которые меняете. Ключи берутся из `backend/sql/src/main/resources/application.yml`, `SqlProperties` (префикс `sql`) и `CommonSecurityProperties` (префикс `hadoop.security`).
```yaml
server:
  port: 8003

spring:
  datasource:
    # Встроенная H2 в файле. Для PostgreSQL: jdbc:postgresql://<host>:5432/<database> (драйвер есть в JAR).
    url: jdbc:h2:file:/var/lib/pristan/sql/data/sql_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL

hadoop:
  security:
    auth:
      mode: ldap # ldap | kerberos | hybrid (mock — только для стендов, требует debug: true)
      admin-groups: ["hadoop-admins"]
      writer-groups: ["data-engineers"]
    jwt:
      # Случайная строка не короче 32 символов; без нее сервис не стартует
      secret-key: "${JWT_SECRET_KEY}"
      expiration-minutes: 480
    cors:
      allowed-origins:
        - "https://sql.company.local"
    cookie:
      secure: true
    ldap:
      enabled: true
      url: "ldaps://ldap.company.local:636"
      manager-dn: "cn=svc_sql,ou=services,dc=company,dc=local"
      manager-password: "${LDAP_MANAGER_PASSWORD}"
      user-search-base: "ou=users,dc=company,dc=local"
      user-search-filter: "(sAMAccountName={0})"
      group-search-base: "ou=groups,dc=company,dc=local"
      group-search-filter: "(member={0})"
    kerberos:
      # Входящий SPNEGO (SSO в браузере)
      enabled: true
      keytab: "/etc/security/keytabs/sql-explorer.keytab"
      service-principal: "HTTP/sql.company.local@COMPANY.LOCAL"

sql:
  results-ttl-hours: 24
  max-rows-per-query: 10000
  default-page-size: 500
  clusters:
    - id: "trino-prod"
      name: "Trino Production Cluster"
      type: "trino" # trino | hive | mock
      host: "trino-coordinator.company.local"
      port: 8080
      catalog: "hive"
      schema: "default"
      impersonation:
        enabled: true
        method: "header" # header | doAs | session
      allowed-groups: ["data-engineers", "bi-analysts"]
    - id: "hive-prod"
      name: "Apache Hive Cluster"
      type: "hive"
      host: "hiveserver2.company.local"
      port: 10000
      catalog: "default"
      schema: "default"
      impersonation:
        enabled: true
        method: "doAs"
  ai:
    enabled: true
```

Ограничения текущей версии, которые видны в коде:
- При `mock-storage: false` `TrinoSqlEngine` и `HiveSqlEngine` подключаются к кластерам через JDBC; параметры (`jdbc.*`, `kerberos.*`) описаны в [CONFIGURATION.md, раздел 5.1](CONFIGURATION.md#51-аналитические-кластеры). Hadoop-клиент сервису не нужен: Kerberos выполняют JDBC-драйверы по keytab.
- При `hive.server2.enable.doAs=false` и аутентификаторе HiveServer2 по умолчанию `current_user()` возвращает служебного пользователя HiveServer2 (на демо-стенде `hive`), хотя сессия открыта от имени `hive.server2.proxy.user`. Пользователя сессии показывает журнал HiveServer2: `Login attempt is successful for user : <пользователь>` и `userName=<пользователь>` у `EXECUTE_STATEMENT`. Чтобы `current_user()` возвращал пользователя сессии, задайте в HiveServer2 `hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator`.
- Если у кластера пусты `allowed-users` и `allowed-groups`, он доступен всем пользователям.
- Строки результатов, отмена выполняющихся запросов и SSE-подписки хранятся в памяти экземпляра, выполняющего запрос. При двух и более экземплярах без привязки клиента к экземпляру на балансировщике пользователь получает пустой результат при `total > 0`, а отмена с другого экземпляра не останавливает запрос. Настройте привязку или запускайте один экземпляр: [production-database.md, раздел 3.3](production-database.md#33-привязка-клиента-к-экземпляру).

Установите права доступа:
```bash
sudo chmod 600 /etc/pristan/sql/application.yml
sudo chown appuser:appuser /etc/pristan/sql/application.yml
```

### Шаг 5: Создание systemd-сервиса
Создайте файл `/etc/systemd/system/sql-explorer.service`:
```ini
[Unit]
Description=Hadoop SQL Explorer Web Service
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/pristan/sql
Environment="SPRING_CONFIG_ADDITIONAL_LOCATION=file:/etc/pristan/sql/application.yml"
Environment="KRB5_CONFIG=/etc/krb5.conf"

ExecStart=/usr/bin/java -Xms512m -Xmx2048m \
    -jar /opt/pristan/sql/bin/sql-explorer.jar \
    --server.port=8003

Restart=always
RestartSec=5s
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Шаг 6: Запуск и проверка
```bash
sudo systemctl daemon-reload
sudo systemctl enable sql-explorer
sudo systemctl start sql-explorer

sudo systemctl status sql-explorer
curl -I http://127.0.0.1:8003/healthz
# HTTP/1.1 200 OK
```

### Шаг 7: Обновление версии

Загрузите новую версию из GitHub Releases, переключите символическую ссылку и перезапустите сервис. На время перезапуска (2–3 с) сервис недоступен.
```bash
sudo -u appuser -i
NEW_VER="1.0.1"
RELEASE_DIR="/opt/pristan/sql/releases/v${NEW_VER}"
mkdir -p "${RELEASE_DIR}" && cd "${RELEASE_DIR}"

curl -fsSL -O "https://github.com/company/pristan/releases/download/v${NEW_VER}/sql-explorer-java-${NEW_VER}.jar"
curl -fsSL -O "https://github.com/company/pristan/releases/download/v${NEW_VER}/SHA256SUMS.txt"
sha256sum -c SHA256SUMS.txt --ignore-missing

# Атомарное переключение симлинка:
ln -sfn "${RELEASE_DIR}/sql-explorer-java-${NEW_VER}.jar" /opt/pristan/sql/bin/sql-explorer.jar

# Перезапуск сервиса (занимает 2–3 сек):
sudo systemctl restart sql-explorer
```

---

## 3. Запуск в Docker и Docker Compose

### 3.1 Загрузка образа из GHCR
Рекомендуемый вариант. Готовые образы публикуются в GitHub Container Registry (`ghcr.io`):
```bash
# Авторизация (при необходимости):
echo "${GITHUB_TOKEN}" | docker login ghcr.io -u <github-username> --password-stdin

# Загрузка готового образа:
VERSION="1.0.0"
docker pull ghcr.io/company/pristan/sql:${VERSION}
```

### 3.2 Локальная сборка Docker-образа
```bash
docker build -t ghcr.io/company/pristan/sql:latest -f docker/Dockerfile.sql-java .
```

### 3.3 Запуск одного контейнера (`docker run`)
Контейнер читает тот же `application.yml`, что на шаге 4 раздела 2: путь к файлу передается в `SPRING_CONFIG_ADDITIONAL_LOCATION`. `SPRING_DATASOURCE_URL` переопределяет `spring.datasource.url` из файла, чтобы база H2 лежала на томе `/app/data`; keytab монтируется по пути из `hadoop.security.kerberos.keytab`. Переменные `CONFIG_PATH`, `LDAP_BIND_PASSWORD`, `KRB5_KEYTAB`, `KRB5_PRINCIPAL` сервис не читает.
```bash
# <jwt_secret> - вывод `openssl rand -hex 32`; задайте тот же ключ на всех контейнерах сервиса
docker run -d \
  --name sql-explorer \
  --restart unless-stopped \
  -p 8003:8003 \
  -e SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/application.yml \
  -e SPRING_DATASOURCE_URL="jdbc:h2:file:/app/data/sql_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL" \
  -e JWT_SECRET_KEY="<jwt_secret>" \
  -e LDAP_MANAGER_PASSWORD="SecretLdapPassword" \
  -v /opt/sql-explorer/application.yml:/app/config/application.yml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v sql-data:/app/data \
  ghcr.io/company/pristan/sql:1.0.0
```

### 3.4 Запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  sql-explorer:
    image: ghcr.io/company/pristan/sql:1.0.0
    container_name: sql-explorer
    restart: unless-stopped
    ports:
      - "8003:8003"
    environment:
      - SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/application.yml
      - SPRING_DATASOURCE_URL=jdbc:h2:file:/app/data/sql_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_MANAGER_PASSWORD=${LDAP_MANAGER_PASSWORD}
    volumes:
      - ./application.yml:/app/config/application.yml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs/sql-explorer.keytab:/etc/security/keytabs/sql-explorer.keytab:ro
      - sql-storage:/app/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8003/healthz"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 10s
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 3072M
        reservations:
          cpus: "0.5"
          memory: 512M
    networks:
      - hadoop-net

volumes:
  sql-storage:
    driver: local

networks:
  hadoop-net:
    driver: bridge
```

---

## 4. Мониторинг и диагностика

### 4.1 Метрики Prometheus (`GET /actuator/prometheus`)
Сервис отдает метрики в текстовом формате Prometheus на `GET /actuator/prometheus` (порт приложения `8003`). Эндпоинт открыт без аутентификации; остальные эндпоинты Actuator, кроме `/actuator/health`, по HTTP не опубликованы, а при публикации требуют роли ADMIN (`common-security-starter`). `GET /metrics` возвращает 404.

| Метрика | Тип | Лейблы | Что считает |
|---|---|---|---|
| `http_server_requests_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app="sql"`, `method`, `uri`, `status`, `outcome`, `exception`, `le` | Входящие HTTP-запросы; `uri` — шаблон маршрута Spring MVC; бакеты 50 мс – 30 с |
| `http_server_requests_active_seconds` (`_count`, `_sum`, `_max`) | LongTaskTimer (в Prometheus — `summary`) | `app="sql"`, `method`; `uri`, `status`, `outcome`, `exception` фиксируются в начале запроса (`uri="UNKNOWN"`, `status="200"`) и смысла не несут | Запросы, которые выполняются в момент опроса: `_count` — их число (включая сам запрос к `/actuator/prometheus`), `_sum` и `_max` — сколько секунд они уже выполняются |
| `sql_query_duration_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app`, `cluster`, `engine`, `status`, `le` | Время выполнения SQL-запроса от старта задачи до конечного статуса (без времени ожидания в очереди до старта задачи). `cluster` — `id` кластера; `engine` — `type` кластера (`trino`, `hive`) в нижнем регистре, в том числе для кластера с `mock-storage: true`. `status`: `FINISHED`, `CANCELLED`, `FAILED`. Бакеты 0,1 с – 5 мин |
| `hadoop_rate_limit_blocks_total` | Counter | `app`, `tier` | Запросы, отклоненные ограничителем частоты ответом 429. `tier`: `login` или `api` |

Тексты запросов и имена пользователей в лейблы не попадают. Кроме перечисленных публикуются стандартные метрики Spring Boot Actuator (`jvm_*`, `process_*`, `system_*` и др.).

Проверка на демо-стенде:

```bash
curl -s http://localhost:8003/actuator/prometheus | grep '^sql_query_duration_seconds_count'
```

После выполнения запроса в выводе есть строка вида `sql_query_duration_seconds_count{app="sql",cluster="<cluster_id>",engine="<type>",status="FINISHED"} 1.0`, где `<cluster_id>` — идентификатор кластера из `sql.clusters`.

Сбор метрик Prometheus и дашборд `spark_sql_explorer_analytics.json` описаны в [CONFIGURATION.md, раздел 9](CONFIGURATION.md#9-мониторинг-prometheus-и-дашборды-grafana).

### 4.2 Типовые ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `502` от `/api/v1/catalog/...`, `detail`: `<кластер>: ... Connection refused` | Trino недоступен по `host:port` | Проверьте с узла сервиса: `curl -sk https://<host>:<port>/v1/info` |
| `TLS/SSL is required for Kerberos authentication` | `kerberos.enabled: true` без `jdbc.ssl: true` | Включите `jdbc.ssl` и укажите порт HTTPS координатора |
| `Kerberos error for [trino@<host>]: ... Server not found in Kerberos database (7) - LOOKING_UP_SERVER` | JDK канонизирует `host` через обратный DNS (в Docker `trino-coordinator` превращается в `trino-coordinator.<сеть>`) и запрашивает билет для SPN `trino/<каноническое имя>@<REALM>`, которого нет в KDC | Добавьте `dns_canonicalize_hostname = false` в `[libdefaults]` файла `krb5.conf` процесса (`-Djava.security.krb5.conf`) или зарегистрируйте SPN на каноническое имя. `KerberosUseCanonicalHostname=false`, который сервис передает драйверу, на канонизацию в JDK не влияет |
| `Access Denied: User <служебный пользователь> cannot impersonate user <пользователь>` | В правилах Trino нет имперсонации служебного пользователя | Добавьте правило `impersonation` для `jdbc.service-user` или отключите `impersonation.enabled` |
| Статус `FAILED`, «Превышен лимит времени выполнения запроса» | Запрос дольше `jdbc.query-timeout-seconds` | Увеличьте лимит для кластера или оптимизируйте запрос |
| Сервис не стартует: `kerberos.krb5-conf=<путь> не совпадает с -Djava.security.krb5.conf=<путь>` или `Кластеры с Kerberos задают разные kerberos.krb5-conf` | Конфигурация Kerberos JDK одна на процесс: trino-jdbc записывает `kerberos.krb5-conf` в `java.security.krb5.conf`, драйвер Kyuubi путь не принимает и читает свойство JVM. Путь расходится с JVM (для Hive — также при незаданном `-Djava.security.krb5.conf`) или между кластерами Trino и Hive | Укажите один путь, равный `-Djava.security.krb5.conf`, или уберите `kerberos.krb5-conf` |
| `Refusing to set system property 'java.security.krb5.conf' to '<путь>', it is already set to '<путь>'` | Свойство JVM `java.security.krb5.conf` изменилось после старта (его установил другой компонент процесса) | Приведите `kerberos.krb5-conf` и `-Djava.security.krb5.conf` к одному пути и перезапустите сервис |
| Сервис не стартует: `при kerberos.enabled: true параметр kerberos.<имя> обязателен` | Не задан `kerberos.principal` или `kerberos.keytab`; для Trino — `kerberos.service-name`, для Hive — `kerberos.server-principal` | Задайте параметр в конфигурации кластера |
| Сервис не стартует: `при kerberos.enabled: true требуется jdbc.ssl: true` | Kerberos к Trino включен без TLS | Включите `jdbc.ssl` и укажите порт HTTPS координатора |
| Сервис не стартует: `kerberos.krb5-conf должен быть абсолютным путем` | Задан относительный путь: драйвер сравнивает абсолютный путь с `java.security.krb5.conf` и после первого соединения отклонял бы все остальные | Укажите абсолютный путь |
| Сервис не стартует: `kerberos.krb5-conf=<путь> задан при Kerberos-кластере Hive, а -Djava.security.krb5.conf=...` | `kerberos.krb5-conf` задан у Kerberos-кластера (обычно Trino), а свойство JVM не задано или другое. Драйвер Kyuubi читает только свойство JVM, и до первого соединения trino-jdbc Hive вошел бы с другим `krb5.conf` | Задайте `-Djava.security.krb5.conf=<тот же путь>` |
| Сервис не стартует: `параметр <имя> не должен содержать символы ';', '?' и '#'` | В `host`, `schema` или `kerberos.*` кластера Hive есть разделители JDBC URL HiveServer2 | Исправьте значение |
| Запрос к Hive: `java.net.SocketTimeoutException: Read timed out` | HiveServer2 не ответил за `jdbc.socket-timeout-seconds` (по умолчанию 300 с) на одно обращение: завис при подключении, `FetchResults` или чтении метаданных | Проверьте журнал HiveServer2; для медленных выборок увеличьте `jdbc.socket-timeout-seconds` (`0` — без ограничения) |
| Сервис не стартует: `jdbc.ssl для type: hive не поддерживается` | У кластера Hive задан `jdbc.ssl: true`; сервис подключается к HiveServer2 только по binary-транспорту без TLS | Уберите `jdbc.ssl` у кластера Hive |
| `Failed to validate proxy privilege of <svc> for <user>` | HiveServer2 не разрешает служебному пользователю имперсонацию | Добавьте `hadoop.proxyuser.<svc>.hosts` и `hadoop.proxyuser.<svc>.groups` в `core-site.xml` HiveServer2 и перезапустите его |
| `имя пользователя '...' недопустимо для hive.server2.proxy.user` | В имени пользователя есть символы вне `[A-Za-z0-9._@-]` | Используйте учетную запись без таких символов или отключите `impersonation.enabled` для кластера |
| `GSS initiate failed` при подключении к Hive | Неверный `kerberos.server-principal` или нет билета у keytab. Если в журнале HiveServer2 `Checksum failed`, JDK канонизировал `host` (`hive-server-2` → `hive-server-2.<сеть>`) и запросил билет для другого SPN, чем principal HiveServer2 | Сверьте principal HiveServer2 (`hive.server2.authentication.kerberos.principal`) и проверьте keytab: `klist -kt <keytab>`. При канонизации добавьте `dns_canonicalize_hostname = false` в `[libdefaults]` файла `krb5.conf` процесса |
| `Failed to open new session: Database <schema> does not exist` | В HiveServer2 нет базы из `schema` кластера: сессия открывается сразу в ней | Создайте базу или укажите существующую в `schema` |

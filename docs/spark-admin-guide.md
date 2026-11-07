# Руководство администратора Spark Explorer

Руководство для инженеров DevOps/SRE и системных администраторов: установка, настройка и эксплуатация Spark Explorer в двух режимах:
1. Standalone: bare-metal или ВМ под управлением systemd.
2. Docker и Docker Compose.

---

## 1. Архитектура и требования

### 1.1 Функции сервиса
Spark Explorer — веб-среда для запуска вычислений Apache Spark через Apache Livy:
- Жизненный цикл сессий через Apache Livy REST API: создание, отслеживание состояния, отмена statement, вывод результата.
- Языки PySpark и Spark Scala.
- Перевод сессий, простаивающих в статусе `idle` дольше `spark.session-ttl-minutes` (по умолчанию 120 минут), в статус `killed`.
- Профили ресурсов для запуска в очередях YARN (`driver_memory`, `executor_memory`, `executor_cores`, `num_executors`).
- Ссылки на Spark History Server для просмотра завершённых приложений.
- Рабочее пространство пользователя (`SparkUserWorkspace`), хранимое на сервере: блокноты, сохранённые пайплайны и история выполнения.

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8004` | HTTP/HTTPS | Inbound | Пользовательский веб-интерфейс SPA (Svelte 5 + Monaco Editor) и REST API |
| `8998` | HTTP/HTTPS | Outbound | Apache Livy Server REST API |
| `18080` | HTTP/HTTPS | Outbound | Spark History Server Web UI |
| `8088` | HTTP/HTTPS | Outbound | YARN ResourceManager REST API |
| `9083` | TCP (Thrift) | Outbound | Apache Hive Metastore (метаданные Spark SQL / Iceberg) |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Корпоративный сервер аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- Проверка доступности (healthcheck): `GET /healthz`
- Readiness: `GET /actuator/health/readiness` — 200 `UP` после `ApplicationReadyEvent`, 503 `OUT_OF_SERVICE` с начала остановки контекста приложения.
- Метрики Prometheus: `GET /actuator/prometheus`, см. [4.1](#41-метрики-prometheus-get-actuatorprometheus).

### 1.3 Системные требования

#### Запуск готового JAR или Docker-образа

| Компонент | Требование |
|---|---|
| ОС | Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12), macOS |
| Среда выполнения | Java 21 LTS JRE (Eclipse Temurin 21 JRE или `openjdk-21-jre-headless`) |
| Системные пакеты | `krb5-user` (`krb5-workstation`), `curl`, `ca-certificates` |
| Ресурсы на реплику | CPU: 1 ядро (рекомендуется 2), RAM: 1,5 ГБ (рекомендуется 2 ГБ), диск: 5–10 ГБ |

Для установки из готового релиза Git, Apache Maven, Node.js и npm на целевых серверах не нужны.

#### Сборка из исходников
- JDK: `21 LTS`
- Maven: `3.9+`
- Node.js: `20+` / `22 LTS` и npm `10+`

### 1.4 Дистрибутивы (GitHub Releases и GHCR)
- GitHub Releases (`v${VERSION}`):
  - `spark-explorer-java-${VERSION}.jar` (~50 МБ) — Spring Boot 3 fat JAR со встроенным веб-интерфейсом (Svelte 5 SPA), клиентом Livy/YARN, DAG-пайплайнами и драйверами БД;
  - `SHA256SUMS.txt` — контрольные суммы SHA-256 для проверки загруженных файлов.
- GitHub Container Registry (GHCR):
  - `ghcr.io/company/hadoop-explorer/spark:${VERSION}`

---

## 2. Режим 1: Standalone (bare-metal, ВМ, systemd)

Установка на выделенный сервер или ВМ с запуском через `systemd`.

### Шаг 1: Установка системных зависимостей

#### Установка из готового релиза (только JRE)
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
sudo mkdir -p /opt/hadoop-explorer/spark/releases
sudo mkdir -p /opt/hadoop-explorer/spark/bin
sudo mkdir -p /etc/hadoop-explorer/spark
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/spark/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/spark
```

### Шаг 3: Получение JAR

#### Способ А: загрузка из GitHub Releases

Готовый JAR уже содержит собранный веб-интерфейс (Svelte 5 SPA) и все зависимости. Этот способ рекомендуется для production.

```bash
sudo -u appuser -i

VERSION="1.0.0"
GITHUB_REPO="company/hadoop-explorer"
RELEASE_DIR="/opt/hadoop-explorer/spark/releases/v${VERSION}"
BIN_DIR="/opt/hadoop-explorer/spark/bin"

mkdir -p "${RELEASE_DIR}" "${BIN_DIR}" && cd "${RELEASE_DIR}"

# 1. Загрузка через curl по прямой ссылке (или через `gh release download`):
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/spark-explorer-java-${VERSION}.jar"
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/SHA256SUMS.txt"

# 2. Проверка контрольной суммы:
sha256sum -c SHA256SUMS.txt --ignore-missing

# 3. Создание стабильной символической ссылки:
ln -sfn "${RELEASE_DIR}/spark-explorer-java-${VERSION}.jar" "${BIN_DIR}/spark-explorer.jar"
```

#### Способ Б: сборка из исходников (для разработчиков)
```bash
sudo -u appuser -i
cd /tmp
git clone https://github.com/company/hadoop-explorer.git
cd hadoop-explorer

# Сборка фронтенда Spark Explorer
cd frontend
npm ci --workspace=apps/spark --include-workspace-root
# Бандл собирается сразу в backend/spark/src/main/resources/static и попадает в JAR
npm run build --workspace=apps/spark
cd ..

# Сборка Java 21 бэкенда (Spring Boot Fat JAR)
mvn clean package -pl backend/common-security-starter,backend/spark -am -DskipTests

# Копирование собранного JAR в рабочий каталог
cp backend/spark/target/spark-explorer-java-*.jar /opt/hadoop-explorer/spark/bin/spark-explorer.jar
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/hadoop-explorer/spark/application.yml`. Юнит systemd на шаге 5 подключает его через `SPRING_CONFIG_ADDITIONAL_LOCATION` поверх `application.yml` из JAR, поэтому указывайте только ключи, которые меняете. Ключи берутся из `backend/spark/src/main/resources/application.yml`, `SparkProperties` (префикс `spark`) и `CommonSecurityProperties` (префикс `hadoop.security`).

```yaml
server:
  port: 8004

spring:
  datasource:
    # Встроенная H2 в файле. Для PostgreSQL: jdbc:postgresql://<host>:5432/<database> (драйвер есть в JAR).
    url: jdbc:h2:file:/var/lib/hadoop-explorer/spark/data/spark_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL

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
        - "https://spark.company.local"
    cookie:
      secure: true
    ldap:
      enabled: true
      url: "ldaps://ldap.company.local:636"
      manager-dn: "cn=svc_spark,ou=services,dc=company,dc=local"
      manager-password: "${LDAP_MANAGER_PASSWORD}"
      user-search-base: "ou=users,dc=company,dc=local"
      user-search-filter: "(sAMAccountName={0})"
      group-search-base: "ou=groups,dc=company,dc=local"
      group-search-filter: "(member={0})"
    kerberos:
      # Входящий SPNEGO (SSO в браузере)
      enabled: true
      keytab: "/etc/security/keytabs/spark-explorer.keytab"
      service-principal: "HTTP/spark.company.local@COMPANY.LOCAL"

spark:
  # Сессии в статусе idle, простаивающие дольше этого срока, переводятся в killed (проверка раз в минуту)
  session-ttl-minutes: 120
  results-ttl-hours: 24
  clusters:
    - id: "spark-prod"
      name: "Production Spark on YARN"
      type: "livy"
      livy-url: "http://livy-server.company.local:8998"
      yarn-cluster-id: "yarn-prod"
      yarn-queues: ["root.default", "root.analytics"]
      default-queue: "root.analytics"
      allowed-groups: ["data-engineers", "bi-analysts"]
      spark-versions:
        - id: "spark-3.5.1"
          name: "Apache Spark 3.5.1"
          default-version: true
          python-versions:
            - id: "py310"
              name: "Python 3.10"
              default-env: true
      metastores:
        - id: "hive-prod"
          name: "Production Hive Metastore"
          default-metastore: true
      # Пресеты выделения ресурсов для пользователей
      resource-profiles:
        - id: "small"
          name: "Small (2 executors, 4G)"
          driver-memory: "2g"
          driver-cores: 1
          executor-memory: "4g"
          executor-cores: 2
          num-executors: 2
        - id: "large"
          name: "Large (10 executors, 16G)"
          driver-memory: "8g"
          driver-cores: 2
          executor-memory: "16g"
          executor-cores: 4
          num-executors: 10
```

Ограничения текущей версии, которые видны в коде:
- `LivyClient.createSession` не обращается к Livy: идентификатор сессии Livy и YARN application ID генерируются случайно, код выполняет `MockSparkEngine`. `livy-url` только записывается в лог.
- Строки результатов, отмена выполняющегося кода, SSE-подписки и пайплайны (`/api/v1/pipelines`) хранятся в памяти экземпляра. При двух и более экземплярах без привязки клиента к экземпляру на балансировщике пользователь получает пустой результат при `total > 0`, отмена с другого экземпляра не останавливает выполнение, а пайплайн, созданный на одном экземпляре, не виден на другом. Настройте привязку или запускайте один экземпляр: [production-database.md, раздел 3.3](production-database.md#33-привязка-клиента-к-экземпляру).
- Если у кластера пусты `allowed-users` и `allowed-groups`, он доступен всем пользователям.

Установите права доступа:
```bash
sudo chmod 600 /etc/hadoop-explorer/spark/application.yml
sudo chown appuser:appuser /etc/hadoop-explorer/spark/application.yml
```

### Шаг 5: Создание systemd-сервиса
Создайте файл `/etc/systemd/system/spark-explorer.service`:
```ini
[Unit]
Description=Hadoop Spark Explorer Web Service
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/hadoop-explorer/spark
Environment="SPRING_CONFIG_ADDITIONAL_LOCATION=file:/etc/hadoop-explorer/spark/application.yml"
Environment="KRB5_CONFIG=/etc/krb5.conf"

ExecStart=/usr/bin/java -Xms512m -Xmx2048m \
    -jar /opt/hadoop-explorer/spark/bin/spark-explorer.jar \
    --server.port=8004

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
sudo systemctl enable spark-explorer
sudo systemctl start spark-explorer

sudo systemctl status spark-explorer
curl -I http://127.0.0.1:8004/healthz
# HTTP/1.1 200 OK
```

### Шаг 7: Обновление версии

Загрузите новый релиз, переключите символическую ссылку и перезапустите сервис. На время перезапуска сервис недоступен.
```bash
sudo -u appuser -i
NEW_VER="1.0.1"
RELEASE_DIR="/opt/hadoop-explorer/spark/releases/v${NEW_VER}"
mkdir -p "${RELEASE_DIR}" && cd "${RELEASE_DIR}"

curl -fsSL -O "https://github.com/company/hadoop-explorer/releases/download/v${NEW_VER}/spark-explorer-java-${NEW_VER}.jar"
curl -fsSL -O "https://github.com/company/hadoop-explorer/releases/download/v${NEW_VER}/SHA256SUMS.txt"
sha256sum -c SHA256SUMS.txt --ignore-missing

# Атомарное переключение симлинка:
ln -sfn "${RELEASE_DIR}/spark-explorer-java-${NEW_VER}.jar" /opt/hadoop-explorer/spark/bin/spark-explorer.jar

# Перезапуск сервиса (занимает 2–3 сек):
sudo systemctl restart spark-explorer
```

---

## 3. Режим 2: Docker и Docker Compose

### 3.1 Загрузка образа из GHCR
Готовые образы публикуются в GitHub Container Registry (`ghcr.io`). Этот способ рекомендуется для production.
```bash
# Авторизация (при необходимости):
echo "${GITHUB_TOKEN}" | docker login ghcr.io -u <github-username> --password-stdin

# Загрузка готового образа:
VERSION="1.0.0"
docker pull ghcr.io/company/hadoop-explorer/spark:${VERSION}
```

### 3.2 Локальная сборка образа (для разработки)
```bash
docker build -t ghcr.io/company/hadoop-explorer/spark:latest -f docker/Dockerfile.spark-java .
```

### 3.3 Запуск одного контейнера (`docker run`)
Контейнер читает тот же `application.yml`, что на шаге 4 раздела 2: путь к файлу передается в `SPRING_CONFIG_ADDITIONAL_LOCATION`. `SPRING_DATASOURCE_URL` переопределяет `spring.datasource.url` из файла, чтобы база H2 лежала на томе `/app/data`; keytab монтируется по пути из `hadoop.security.kerberos.keytab`. Переменные `CONFIG_PATH`, `LDAP_BIND_PASSWORD`, `KRB5_KEYTAB`, `KRB5_PRINCIPAL` сервис не читает.
```bash
# <jwt_secret> - вывод `openssl rand -hex 32`; задайте тот же ключ на всех контейнерах сервиса
docker run -d \
  --name spark-explorer \
  --restart unless-stopped \
  -p 8004:8004 \
  -e SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/application.yml \
  -e SPRING_DATASOURCE_URL="jdbc:h2:file:/app/data/spark_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL" \
  -e JWT_SECRET_KEY="<jwt_secret>" \
  -e LDAP_MANAGER_PASSWORD="SecretLdapPassword" \
  -v /opt/spark-explorer/application.yml:/app/config/application.yml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v spark-data:/app/data \
  ghcr.io/company/hadoop-explorer/spark:1.0.0
```

### 3.4 Запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  spark-explorer:
    image: ghcr.io/company/hadoop-explorer/spark:1.0.0
    container_name: spark-explorer
    restart: unless-stopped
    ports:
      - "8004:8004"
    environment:
      - SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/application.yml
      - SPRING_DATASOURCE_URL=jdbc:h2:file:/app/data/spark_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_MANAGER_PASSWORD=${LDAP_MANAGER_PASSWORD}
    volumes:
      - ./application.yml:/app/config/application.yml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs/spark-explorer.keytab:/etc/security/keytabs/spark-explorer.keytab:ro
      - spark-storage:/app/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8004/healthz"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 10s
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 2048M
        reservations:
          cpus: "0.5"
          memory: 512M
    networks:
      - hadoop-net

volumes:
  spark-storage:
    driver: local

networks:
  hadoop-net:
    driver: bridge
```

---

## 4. Мониторинг и диагностика

### 4.1 Метрики Prometheus (`GET /actuator/prometheus`)
Сервис отдает метрики в текстовом формате Prometheus на `GET /actuator/prometheus` (порт приложения `8004`). Эндпоинт открыт без аутентификации; остальные эндпоинты Actuator, кроме `/actuator/health`, по HTTP не опубликованы, а при публикации требуют роли ADMIN (`common-security-starter`). `GET /metrics` возвращает 404.

| Метрика | Тип | Лейблы | Что считает |
|---|---|---|---|
| `http_server_requests_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app="spark"`, `method`, `uri`, `status`, `outcome`, `exception`, `le` | Входящие HTTP-запросы; `uri` — шаблон маршрута Spring MVC; бакеты 50 мс – 30 с |
| `http_server_requests_active_seconds` (`_count`, `_sum`, `_max`) | LongTaskTimer (в Prometheus — `summary`) | `app="spark"`, `method`; `uri`, `status`, `outcome`, `exception` фиксируются в начале запроса (`uri="UNKNOWN"`, `status="200"`) и смысла не несут | Запросы, которые выполняются в момент опроса: `_count` — их число (включая сам запрос к `/actuator/prometheus`), `_sum` и `_max` — сколько секунд они уже выполняются |
| `spark_statement_duration_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app`, `cluster`, `language`, `status`, `le` | Время выполнения инструкции от старта задачи до конечного статуса (без времени ожидания в очереди до старта задачи). `cluster` — `id` кластера сессии; `language` — язык инструкции, а если он не указан, вид сессии (`pyspark`, `spark`, `sql`). `status`: `FINISHED`, `CANCELLED`, `FAILED`. Бакеты 0,1 с – 5 мин |
| `spark_sessions_active` | Gauge | `app`, `cluster`, `kind` | Сессии в статусе, отличном от `dead` и `killed`, по записям в БД сервиса. Значение обновляется раз в 30 с одним запросом `GROUP BY`, поэтому отстает от интерфейса до 30 с; ряды кластеров и видов без сессий исчезают из вывода. Значение общее для всех экземпляров на одной БД: при N экземплярах каждый отдает полное число, поэтому агрегируйте через `max by (cluster, kind)`, а не `sum` |
| `hadoop_rate_limit_blocks_total` | Counter | `app`, `tier` | Запросы, отклоненные ограничителем частоты ответом 429. `tier`: `login` или `api` |

Внимание: инструкции выполняет `LivyClient.getMockEngine()` (`SparkSessionManager.runExecutionTask`), поэтому `spark_statement_duration_seconds` показывает время mock-движка, а не Livy.

Код инструкции, имена пользователей и идентификаторы сессий в лейблы не попадают. Значения `language` и `kind` приводятся к нижнему регистру и ограничены списком `pyspark`, `spark`, `scalaspark`, `scala`, `sparkr`, `sql`: пустое значение превращается в `unknown`, любое другое — в `other` (для `spark_sessions_active` серии `other` суммируются). Кроме перечисленных публикуются стандартные метрики Spring Boot Actuator (`jvm_*`, `process_*`, `system_*` и др.).

Проверка на демо-стенде:

```bash
curl -s http://localhost:8004/actuator/prometheus | grep -E '^spark_(sessions_active|statement_duration_seconds_count)'
```

После выполнения инструкции в выводе есть строка вида `spark_statement_duration_seconds_count{app="spark",cluster="<cluster_id>",language="pyspark",status="FINISHED"} 1.0`. Через 30 с после создания сессии появляется `spark_sessions_active{app="spark",cluster="<cluster_id>",kind="pyspark"} 1.0`; `<cluster_id>` — идентификатор кластера из конфигурации Spark Explorer.

Сбор метрик Prometheus и дашборд `spark_sql_explorer_analytics.json` описаны в [CONFIGURATION.md, раздел 9](CONFIGURATION.md#9-мониторинг-prometheus-и-дашборды-grafana).

### 4.2 Типовые ошибки

| Ошибка | Причина | Решение |
|---|---|---|
| `LivySessionCreationError: YARN queue limit exceeded` | В целевой очереди YARN нет доступной памяти | Выберите профиль с меньшим потреблением ресурсов (`small`) либо смените очередь YARN. |
| `Cannot connect to Livy server http://...:8998` | Служба Apache Livy не запущена | Проверьте статус службы Livy: `systemctl status livy-server` или `curl http://livy:8998/sessions`. |
| `Session Dead: Driver exit with code 1` | Ошибка инициализации драйвера (память, несовместимость версий jar) | Перейдите в Spark History Server или проверьте логи контейнера драйвера в YARN: `yarn logs -applicationId <app_id>`. |

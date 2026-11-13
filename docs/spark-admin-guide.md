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
- Закрытие сессий: кнопка остановки, простой в статусе `idle` дольше `spark.session-ttl-minutes` (по умолчанию 120 минут, проверка раз в минуту) и выход пользователя (`spark.close-sessions-on-logout`, по умолчанию `true`) удаляют сессию в Livy (`DELETE /sessions/{id}`) и переводят запись в `killed`. Если Livy недоступен, запись все равно переводится в `killed`, а сессию закроет Livy по `livy.server.session.timeout`.
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
  - `ghcr.io/company/pristan/spark:${VERSION}`

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
sudo mkdir -p /opt/pristan/spark/releases
sudo mkdir -p /opt/pristan/spark/bin
sudo mkdir -p /etc/pristan/spark
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/pristan/spark/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/pristan/spark
```

### Шаг 3: Получение JAR

#### Способ А: загрузка из GitHub Releases

Готовый JAR уже содержит собранный веб-интерфейс (Svelte 5 SPA) и все зависимости. Этот способ рекомендуется для production.

```bash
sudo -u appuser -i

VERSION="1.0.0"
GITHUB_REPO="company/hadoop-explorer"
RELEASE_DIR="/opt/pristan/spark/releases/v${VERSION}"
BIN_DIR="/opt/pristan/spark/bin"

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
mvn clean package -pl backend/zastava,backend/spark -am -DskipTests

# Копирование собранного JAR в рабочий каталог
cp backend/spark/target/spark-explorer-java-*.jar /opt/pristan/spark/bin/spark-explorer.jar
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/pristan/spark/application.yml`. Юнит systemd на шаге 5 подключает его через `SPRING_CONFIG_ADDITIONAL_LOCATION` поверх `application.yml` из JAR, поэтому указывайте только ключи, которые меняете. Ключи берутся из `backend/spark/src/main/resources/application.yml`, `SparkProperties` (префикс `spark`) и `ZastavaProperties` (префикс `hadoop.security`).

```yaml
server:
  port: 8004

spring:
  datasource:
    # Встроенная H2 в файле. Для PostgreSQL: jdbc:postgresql://<host>:5432/<database> (драйвер есть в JAR).
    url: jdbc:h2:file:/var/lib/pristan/spark/data/spark_explorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL

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
      mock-storage: false          # значение по умолчанию: сессии создаются в Apache Livy
      livy:
        auth: "none"
        proxy-user: true
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
- При `mock-storage: false` и `type: livy` сессии создаются в Apache Livy (`POST /sessions` с `proxyUser`, очередью и ресурсами профиля), код выполняется как statement (`POST /sessions/{id}/statements`). Состояние сессии (`starting`, `idle`, `busy`, `dead`) и YARN application ID обновляет фоновая синхронизация: раз в `spark.session-sync-interval-ms` (по умолчанию 5000 мс, отсчет от конца предыдущего прохода) сервис делает один `GET /sessions` на кластер, кластеры опрашиваются параллельно. API списка и карточки сессии читает только БД, поэтому статус в UI отстает от Livy на время до одного интервала. После любой ошибки Livy (недоступен, таймаут, отказ SPNEGO) кластер не опрашивается `spark.session-sync-pause-seconds` (по умолчанию 30 с), статусы остаются сохраненными, в лог пишется одно предупреждение за паузу. Запрос — `GET /sessions?from=0&size=1000`: по документации REST API Livy объект сессии содержит поле `log` со строками лога, поэтому размер ответа растет с числом сессий на Livy [проверить на Livy 0.8: сколько строк лога сессии входит в ответ списка]. Ответ больше 4 МиБ сервис отбрасывает (HTTP 502): синхронизация кластера встает на паузу `spark.session-sync-pause-seconds`, в логе — `Livy <livy_url> GET /sessions: ответ больше 4194304 байт`. Сессия, которой нет в ответе Livy (Livy перезапущен без восстановления сессий), переводится в `dead`. Livy без восстановления после перезапуска выдает идентификаторы с 0, поэтому сохраненный идентификатор может принадлежать чужой сессии: если имя сессии в Livy отсутствует или не начинается с `hexp-<пользователь>-`, либо `proxyUser` не совпадает с владельцем записи, запись переводится в `dead`, а сессия в Livy не удаляется и код в нее не отправляется. При `livy.proxy-user: true` (по умолчанию) `proxyUser` обязателен: если Livy работает с `livy.impersonation.enabled = false` и возвращает `"proxyUser": null`, каждая сессия сразу получает `dead`, а запуск кода завершается ошибкой `Сессия Livy #<N> не принадлежит пользователю <user> ... livy.impersonation.enabled = false`. Каждый экземпляр сервиса синхронизирует сессии сам: при N экземплярах Livy получает N запросов `GET /sessions` за интервал, записи защищены условными `UPDATE`. Statement, который застал сессию в `dead`, `error` или `killed`, завершается `FAILED`, а запись сессии переводится в `dead`: следующий запуск кода сразу просит запустить новую сессию. Остановка записи в статусе `dead` или `killed` в Livy не обращается. Аутентификация к Livy — `livy.auth: none` или `spnego` (keytab сервиса, см. [CONFIGURATION.md, раздел 6.1](CONFIGURATION.md#61-кластеры-и-интеграция-с-apache-livy)). Очередь сессии должна входить в `yarn-queues` кластера (при непустом списке). Параметры `livy.*` описаны в [CONFIGURATION.md, раздел 6.1](CONFIGURATION.md#61-кластеры-и-интеграция-с-apache-livy).
- Внимание: при `livy.impersonation.enabled = false` Livy молча игнорирует `proxyUser`: сессии и доступ к HDFS и Hive идут от учетки Livy. Проверка: `curl -s http://<livy_host>:8998/sessions/<livy_session_id>` показывает `"proxyUser": null`, в ResourceManager владелец приложения — учетка Livy. На демо-стенде `demo/spark` имперсонация включена (`livy.impersonation.enabled = true`), `proxyUser` в ответе Livy — имя вошедшего пользователя. Подробнее — [CONFIGURATION.md, раздел 6](CONFIGURATION.md#6-настройка-spark-explorer-livy-pyspark-scala-metastore).
- Внимание: `proxyUser` изолирует пользователей только при `livy.spark.master = yarn` и `livy.spark.deploy-mode = cluster` в `livy.conf`. При `local[*]` или `client` драйвер и код пользователя выполняются на хосте Livy под ОС-учеткой `livy`: код любого пользователя может прочитать keytab Livy, а эта учетка входит в `livy.superusers` и `hadoop.proxyuser.livy.*` и с ее keytab можно имперсонировать любого пользователя. Признак такого режима — `"appId": null` у сессий в состоянии `idle` в ответе `curl -s http://<livy_host>:8998/sessions`. Для многопользовательской установки переведите Livy на `yarn` + `cluster` и используйте `LinuxContainerExecutor` на NodeManager: с `DefaultContainerExecutor` контейнеры всех пользователей работают под ОС-учеткой `yarn`. Подробнее — [CONFIGURATION.md, раздел 6](CONFIGURATION.md#6-настройка-spark-explorer-livy-pyspark-scala-metastore).
- Каталог на кластере Livy выполняет `SHOW`/`DESCRIBE` в последней по активности сессии пользователя в статусе `idle` или `busy`. В `busy`-сессии Livy ставит запрос каталога в очередь за выполняющимся кодом, дерево загружается после него. Запросы каталога не обновляют время активности сессии: сессия, в которой только просматривали каталог, закрывается по `spark.session-ttl-minutes`.
- Остановка сервиса прерывает выполняющиеся statement (пул ждет до 5 с, затем прерывает потоки): если прерывание застало опрос statement, движок отменяет его в Livy (`POST /sessions/{id}/statements/{sid}/cancel`). Если прерывание пришлось на HTTP-запрос к Livy, отмена не отправляется и statement дорабатывает в Livy. Статус выполнения записывается, только если БД еще доступна; оставшиеся `QUEUED`/`RUNNING` записи после перезапуска не меняются. Сами сессии в Livy остаются и после перезапуска сервиса продолжают работать по записям в `spark_sessions`; закрываются они по TTL, остановке или выходу пользователя.
- Выход пользователя закрывает все его Spark-сессии, в том числе открытые в других вкладках и браузерах: сервис не различает браузерные сессии одного пользователя. Сессии закрывает экземпляр, принявший `POST /api/v1/auth/logout`: он читает сессии пользователя из `spark_sessions` и удаляет их в Livy в фоновом потоке, ответ на logout этого не ждет. Отключение — `spark.close-sessions-on-logout: false`.
- Если Livy создал сессию, но ответ на `POST /sessions` не пришел за `livy.read-timeout-seconds` (HTTP 504), записи в `spark_sessions` нет и сервис эту сессию не закроет: она живет до `livy.server.session.timeout` Livy. Найти такие сессии: `curl -s http://<livy_host>:8998/sessions` — имя сессии `hexp-<пользователь>-<8 символов>`, удаление — `curl -X DELETE -H 'X-Requested-By: admin' http://<livy_host>:8998/sessions/<livy_session_id>`.
- Строки результатов, отмена выполняющегося кода, SSE-подписки и пайплайны (`/api/v1/pipelines`) хранятся в памяти экземпляра. При двух и более экземплярах без привязки клиента к экземпляру на балансировщике пользователь получает пустой результат при `total > 0`, отмена с другого экземпляра не останавливает выполнение, а пайплайн, созданный на одном экземпляре, не виден на другом. Настройте привязку или запускайте один экземпляр: [production-database.md, раздел 3.3](production-database.md#33-привязка-клиента-к-экземпляру).
- Если у кластера пусты `allowed-users` и `allowed-groups`, он доступен всем пользователям.

Установите права доступа:
```bash
sudo chmod 600 /etc/pristan/spark/application.yml
sudo chown appuser:appuser /etc/pristan/spark/application.yml
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
WorkingDirectory=/opt/pristan/spark
Environment="SPRING_CONFIG_ADDITIONAL_LOCATION=file:/etc/pristan/spark/application.yml"
Environment="KRB5_CONFIG=/etc/krb5.conf"

ExecStart=/usr/bin/java -Xms512m -Xmx2048m \
    -jar /opt/pristan/spark/bin/spark-explorer.jar \
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
RELEASE_DIR="/opt/pristan/spark/releases/v${NEW_VER}"
mkdir -p "${RELEASE_DIR}" && cd "${RELEASE_DIR}"

curl -fsSL -O "https://github.com/company/pristan/releases/download/v${NEW_VER}/spark-explorer-java-${NEW_VER}.jar"
curl -fsSL -O "https://github.com/company/pristan/releases/download/v${NEW_VER}/SHA256SUMS.txt"
sha256sum -c SHA256SUMS.txt --ignore-missing

# Атомарное переключение симлинка:
ln -sfn "${RELEASE_DIR}/spark-explorer-java-${NEW_VER}.jar" /opt/pristan/spark/bin/spark-explorer.jar

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
docker pull ghcr.io/company/pristan/spark:${VERSION}
```

### 3.2 Локальная сборка образа (для разработки)
```bash
docker build -t ghcr.io/company/pristan/spark:latest -f docker/Dockerfile.spark-java .
```

### 3.3 Запуск одного контейнера (`docker run`)
Контейнер читает тот же `application.yml`, что на шаге 4 раздела 2: путь к файлу передается в `SPRING_CONFIG_ADDITIONAL_LOCATION`. `SPRING_DATASOURCE_URL` переопределяет `spring.datasource.url` из файла, чтобы база H2 лежала на томе `/app/data`; keytab монтируется по пути из `zastava.kerberos.keytab`. Переменные `CONFIG_PATH`, `LDAP_BIND_PASSWORD`, `KRB5_KEYTAB`, `KRB5_PRINCIPAL` сервис не читает.
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
  ghcr.io/company/pristan/spark:1.0.0
```

### 3.4 Запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  spark-explorer:
    image: ghcr.io/company/pristan/spark:1.0.0
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
Сервис отдает метрики в текстовом формате Prometheus на `GET /actuator/prometheus` (порт приложения `8004`). Эндпоинт открыт без аутентификации; остальные эндпоинты Actuator, кроме `/actuator/health`, по HTTP не опубликованы, а при публикации требуют роли ADMIN (`zastava`). `GET /metrics` возвращает 404.

| Метрика | Тип | Лейблы | Что считает |
|---|---|---|---|
| `http_server_requests_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app="spark"`, `method`, `uri`, `status`, `outcome`, `exception`, `le` | Входящие HTTP-запросы; `uri` — шаблон маршрута Spring MVC; бакеты 50 мс – 30 с |
| `http_server_requests_active_seconds` (`_count`, `_sum`, `_max`) | LongTaskTimer (в Prometheus — `summary`) | `app="spark"`, `method`; `uri`, `status`, `outcome`, `exception` фиксируются в начале запроса (`uri="UNKNOWN"`, `status="200"`) и смысла не несут | Запросы, которые выполняются в момент опроса: `_count` — их число (включая сам запрос к `/actuator/prometheus`), `_sum` и `_max` — сколько секунд они уже выполняются |
| `spark_statement_duration_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app`, `cluster`, `language`, `status`, `le` | Время выполнения инструкции от старта задачи до конечного статуса (без времени ожидания в очереди до старта задачи). `cluster` — `id` кластера сессии; `language` — язык инструкции, а если он не указан, вид сессии (`pyspark`, `spark`, `sql`). `status`: `FINISHED`, `CANCELLED`, `FAILED`. Бакеты 0,1 с – 5 мин |
| `spark_sessions_active` | Gauge | `app`, `cluster`, `kind` | Сессии в статусе, отличном от `dead` и `killed`, по записям в БД сервиса. Значение обновляется раз в 30 с одним запросом `GROUP BY`, поэтому отстает от интерфейса до 30 с; ряды кластеров и видов без сессий исчезают из вывода. Значение общее для всех экземпляров на одной БД: при N экземплярах каждый отдает полное число, поэтому агрегируйте через `max by (cluster, kind)`, а не `sum` |
| `hadoop_rate_limit_blocks_total` | Counter | `app`, `tier` | Запросы, отклоненные ограничителем частоты ответом 429. `tier`: `login` или `api` |

Внимание: для кластеров с `mock-storage: true` инструкции выполняет `MockSparkEngine`, и `spark_statement_duration_seconds` показывает время mock-движка. Для кластеров Livy время включает ожидание перехода сессии в `idle` (до `livy.session-start-timeout-seconds`). Отмена, принятая до записи итога в `spark_execution_history`, записывается как `status="CANCELLED"`, даже если движок успел вернуть результат.

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
| Сервис не стартует: `spark.clusters[<id>].livy-url не задан: ...` | У кластера `mock-storage: false` (значение по умолчанию), но нет `livy-url` | Задайте `livy-url` или `mock-storage: true`. |
| HTTP 502 на создание сессии: `Livy http://<livy_host>:8998 недоступен (POST /sessions): ConnectException` | Служба Apache Livy не запущена или порт закрыт | Проверьте статус службы Livy: `systemctl status livy-server` или `curl http://<livy_host>:8998/sessions`. |
| HTTP 502: `SPNEGO: не удалось получить билет для HTTP/<host> (livy.principal): ...` | Нет principal `HTTP/<host>` в KDC или `livy-url` указывает на алиас, а не на имя хоста из SPN | Укажите в `livy-url` имя хоста, на которое выписан `HTTP/...` principal Livy. Principal сервиса в ответ API не выводится, он есть в логе сервиса в строке `SPNEGO: не удалось получить билет для HTTP/<host> от имени <principal>`. |
| HTTP 502: `Kerberos-вход сервиса по keytab не выполнен (livy.principal, livy.keytab): ...` | Keytab не читается сервисом или не содержит `livy.principal` | Principal и путь к keytab — в логе сервиса (`Kerberos-вход <principal> по keytab <keytab> не выполнен`), в ответ API и историю выполнений они не попадают. Проверьте keytab от пользователя сервиса: `klist -kt <keytab>`; права файла `0400`. |
| HTTP 504: `Livy http://<livy_host>:8998 не ответил за 60000 мс (...)` | Livy не ответил за `livy.read-timeout-seconds` | Проверьте нагрузку на Livy; при медленной сети увеличьте `livy.read-timeout-seconds`. |
| HTTP 400: `Параметр spark_conf запрещен администратором: <ключ>` | Ключ попадает под `spark.denied-spark-conf-prefixes` или всегда запрещенные префиксы | Уберите ключ из `spark_conf` сессии. |
| HTTP 400: `Параметр <ключ>: путь <путь> должен быть в удаленной файловой системе (...)` | Локальный путь (`file:`, `local:`, без схемы) в `spark.files`, `spark.jars`, `spark.submit.pyFiles`, `spark.archives`, `jars`, `py_files` или `custom_python_archive` | Загрузите файл в HDFS и укажите `hdfs:///...`. |
| HTTP 400: `Очередь YARN <queue> не входит в список очередей кластера <id>: ...` | Очередь не указана в `yarn-queues` кластера | Выберите очередь из списка или добавьте ее в `yarn-queues`. |
| HTTP 409 от `/api/v1/catalog/{clusterId}/databases` (`tables`, `columns`) | На реальном кластере каталог читается через сессию пользователя, а активной (`idle`/`busy`) сессии нет | Запустите сессию на этом кластере и обновите дерево каталога. |
| `Сессия Livy #<N> в состоянии dead, запустите новую сессию` | Драйвер сессии завершился: ошибка инициализации (память, несовместимость версий jar) или `livy.server.session.timeout` | Перейдите в Spark History Server или проверьте логи контейнера драйвера в YARN: `yarn logs -applicationId <app_id>`. |

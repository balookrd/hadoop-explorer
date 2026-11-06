# Руководство администратора: HDFS Explorer

Руководство для DevOps-инженеров, SRE и системных администраторов: установка, настройка и эксплуатация HDFS Explorer в трех режимах:
1. Standalone: bare-metal или виртуальная машина под управлением systemd.
2. Docker и Docker Compose.
3. Kubernetes через Helm-чарт.

---

## 1. Архитектура и требования

### 1.1 Функции сервиса
HDFS Explorer — веб-менеджер файлов Apache Hadoop HDFS:
- Навигация по каталогам, загрузка, скачивание, удаление и перемещение файлов через нативный клиент Hadoop FileSystem (RPC); WebHDFS используется как резервный канал (fallback).
- Переключение на другую NameNode в HA-конфигурации при получении `StandbyException`.
- Вход через Kerberos SPNEGO SSO и выполнение операций от имени пользователя (`doAs`).
- Предпросмотр Parquet, ORC, CSV, TSV, JSON, XML, YAML, SQL и логов.
- Отображение POSIX-прав (`rwxr-xr-x`) и владельца (`owner:group`).
- Потоковое копирование между кластерами (Cross-Cluster Copy).

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8001` (или `8000`, `8002`) | HTTP/HTTPS | Inbound | Пользовательский веб-интерфейс SPA (Svelte 5) и REST API |
| `8020` / `9000` | TCP (Hadoop IPC) | Outbound | NameNode RPC-протокол (`fs.defaultFS`, метаданные, Active/Standby HA) |
| `9866` / `50010` | TCP (DataTransfer) | Outbound | сокетный протокол передачи блоков данных DataNode (чтение/запись) |
| `9870` / `50070` | HTTP/HTTPS | Outbound | WebHDFS REST API NameNode (опциональный fallback) |
| `9864` / `50075` | HTTP/HTTPS | Outbound | DataNode WebHDFS streaming (опциональный fallback) |
| `14000` | HTTP/HTTPS | Outbound | HttpFS Gateway (альтернатива WebHDFS) |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Сервер каталогов аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- Проверка доступности (healthcheck): `GET /health` или `GET /healthz`.
- Метрики Spring Boot: `GET /actuator/prometheus` (при включенном Actuator).

### 1.3 Системные требования

#### Запуск готового JAR или Docker-образа
| Ресурс | Требование |
|---|---|
| ОС | Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12), macOS |
| Среда выполнения | Java 21 LTS JRE (Eclipse Temurin 21 JRE / `openjdk-21-jre-headless`) |
| Системные пакеты | `krb5-user` (`krb5-workstation`), `curl`, `ca-certificates` |
| CPU на реплику | 1 ядро; 2 ядра при частом чтении Parquet/ORC |
| RAM на реплику | 1 ГБ, рекомендуется 2 ГБ |
| Диск | 5–10 ГБ |

Git, Apache Maven, Node.js и npm на целевых серверах при установке из готового релиза не нужны.

#### Сборка из исходников
- JDK: `21 LTS`
- Maven: `3.9+`
- Node.js: `20+` / `22 LTS` и npm `10+`

### 1.4 Дистрибутивы (GitHub Releases и GHCR)
- GitHub Releases (`v${VERSION}`):
  - `hdfs-explorer-java-${VERSION}.jar` (~50 МБ) — Spring Boot 3 fat-JAR с веб-интерфейсом Svelte 5 SPA, WebHDFS-клиентом, просмотрщиком Parquet/ORC и драйверами БД;
  - `SHA256SUMS.txt` — контрольные суммы SHA-256 для проверки загруженных файлов.
- GitHub Container Registry (GHCR):
  - `ghcr.io/company/hadoop-explorer/hdfs:${VERSION}`

---

## 2. Режим 1: Standalone (Bare-Metal / VM / systemd)

Установка на выделенный сервер или ВМ с `systemd`.

### Шаг 1: установка системных зависимостей

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

### Шаг 2: создание пользователя и каталогов
```bash
# Создание непривилегированного пользователя appuser
sudo groupadd -g 10001 appuser
sudo useradd -u 10001 -g appuser -m -s /bin/bash appuser

# Создание каталогов сервиса
sudo mkdir -p /opt/hadoop-explorer/hdfs/releases
sudo mkdir -p /opt/hadoop-explorer/hdfs/bin
sudo mkdir -p /etc/hadoop-explorer/hdfs
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/hdfs/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/hdfs
```

### Шаг 3: получение артефакта

#### Способ А: загрузка из GitHub Releases (для production)

JAR из релиза уже содержит собранный веб-интерфейс Svelte 5 SPA и все зависимости:

```bash
sudo -u appuser -i

VERSION="1.0.0"
GITHUB_REPO="company/hadoop-explorer"
RELEASE_DIR="/opt/hadoop-explorer/hdfs/releases/v${VERSION}"
BIN_DIR="/opt/hadoop-explorer/hdfs/bin"

mkdir -p "${RELEASE_DIR}" "${BIN_DIR}" && cd "${RELEASE_DIR}"

# 1. Загрузка через curl по прямой ссылке (или через `gh release download`):
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/hdfs-explorer-java-${VERSION}.jar"
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/SHA256SUMS.txt"

# 2. Проверка контрольной суммы:
sha256sum -c SHA256SUMS.txt --ignore-missing

# 3. Создание стабильной символической ссылки:
ln -sfn "${RELEASE_DIR}/hdfs-explorer-java-${VERSION}.jar" "${BIN_DIR}/hdfs-explorer.jar"
```

#### Способ Б: сборка из исходников (для разработчиков)
```bash
sudo -u appuser -i
cd /tmp
git clone https://github.com/company/hadoop-explorer.git
cd hadoop-explorer

# Сборка фронтенда HDFS (Svelte 5)
cd frontend
npm ci --workspace=apps/hdfs --include-workspace-root
# Бандл собирается сразу в backend/hdfs/src/main/resources/static и попадает в JAR
npm run build --workspace=apps/hdfs
cd ..

# Сборка исполняемого Spring Boot fat-JAR
mvn clean package -DskipTests -f backend/hdfs/pom.xml

# Копирование собранного JAR в рабочий каталог
cp backend/hdfs/target/hdfs-explorer-java-*.jar /opt/hadoop-explorer/hdfs/bin/hdfs-explorer.jar
```

### Шаг 4: конфигурационный файл
Создайте файл `/etc/hadoop-explorer/hdfs/application.yml`:
```yaml
server:
  port: 8000

spring:
  application:
    name: hadoop-hdfs-explorer
  datasource:
    url: jdbc:h2:file:/var/lib/hadoop-explorer/hdfs/data/hdfs_explorer;DB_CLOSE_DELAY=-1
    driver-class-name: org.h2.Driver
    # Для PostgreSQL-кластера:
    # url: jdbc:postgresql://pg.company.local:5432/hdfs_explorer
    # username: hdfs_user
    # password: DbPassword123

# Общие параметры безопасности платформы
hadoop:
  security:
    auth:
      mode: ldap # mock | ldap | kerberos
      admin-groups:
        - "hadoop-admins"
        - "platform-admins"
      writer-groups:
        - "data-engineers"
        - "developers"
    jwt:
      secret-key: "CHANGE_TO_SUPER_SECURE_RANDOM_KEY_MIN_32_CHARS"
      expiration-minutes: 480
    ldap:
      enabled: true
      server-uri: "ldaps://ldap.company.local:636"
      bind-dn: "cn=svc_hdfs,ou=services,dc=company,dc=local"
      bind-password: "ServiceLdapPassword"
      user-search-base: "ou=users,dc=company,dc=local"
      user-search-filter: "(sAMAccountName={0})"
      group-search-base: "ou=groups,dc=company,dc=local"
      group-search-filter: "(member={0})"
    kerberos:
      enabled: true
      keytab-path: "/etc/security/keytabs/hdfs-explorer.keytab"
      principal: "HTTP/hdfs.company.local@COMPANY.LOCAL"

  # Настройка подключения к кластерам HDFS
  hdfs:
    clusters:
      - id: "prod-datalake"
        name: "Production DataLake"
        description: "Основной аналитический кластер HDFS"
        webhdfs-urls:
          - "http://nn01.prod.company.local:9870/webhdfs/v1"
          - "http://nn02.prod.company.local:9870/webhdfs/v1"
        hdfs-rpc-urls:
          - "hdfs://nn01.prod.company.local:8020"
          - "hdfs://nn02.prod.company.local:8020"
        auth-type: "kerberos"
        service-principal: "hdfs-client@COMPANY.LOCAL"
        keytab-path: "/etc/security/keytabs/hdfs-client.keytab"
        timeout-seconds: 15
        preview-max-bytes: 10485760
        default-path: "/user/{username}"
        mock-storage: false
        acl:
          allowed-groups:
            - "data-engineers"
            - "analytics"
            - "hadoop-admins"
          admin-groups:
            - "hadoop-admins"
```

Ограничьте доступ к файлу: в нем пароль LDAP и JWT-ключ.
```bash
sudo chmod 600 /etc/hadoop-explorer/hdfs/application.yml
sudo chown appuser:appuser /etc/hadoop-explorer/hdfs/application.yml
```

### Шаг 5: systemd-юнит
Создайте файл `/etc/systemd/system/hdfs-explorer.service`:
```ini
[Unit]
Description=Hadoop HDFS Explorer Java Service (Spring Boot 3)
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/hadoop-explorer/hdfs
Environment="SPRING_CONFIG_LOCATION=/etc/hadoop-explorer/hdfs/application.yml"
Environment="KRB5_CONFIG=/etc/krb5.conf"

ExecStart=/usr/bin/java \
    -Xms512m \
    -Xmx2048m \
    -Dspring.config.location=/etc/hadoop-explorer/hdfs/application.yml \
    -jar /opt/hadoop-explorer/hdfs/bin/hdfs-explorer.jar

Restart=always
RestartSec=5s
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Шаг 6: запуск и проверка
```bash
sudo systemctl daemon-reload
sudo systemctl enable hdfs-explorer
sudo systemctl start hdfs-explorer

# Проверка статуса
sudo systemctl status hdfs-explorer
curl -I http://127.0.0.1:8000/healthz
# HTTP/1.1 200 OK
```

### Шаг 7: обновление версии

Загрузите новый релиз, переключите симлинк и перезапустите сервис. Во время перезапуска сервис недоступен:
```bash
sudo -u appuser -i
NEW_VER="1.0.1"
RELEASE_DIR="/opt/hadoop-explorer/hdfs/releases/v${NEW_VER}"
mkdir -p "${RELEASE_DIR}" && cd "${RELEASE_DIR}"

curl -fsSL -O "https://github.com/company/hadoop-explorer/releases/download/v${NEW_VER}/hdfs-explorer-java-${NEW_VER}.jar"
curl -fsSL -O "https://github.com/company/hadoop-explorer/releases/download/v${NEW_VER}/SHA256SUMS.txt"
sha256sum -c SHA256SUMS.txt --ignore-missing

# Атомарное переключение символической ссылки (симлинка):
ln -sfn "${RELEASE_DIR}/hdfs-explorer-java-${NEW_VER}.jar" /opt/hadoop-explorer/hdfs/bin/hdfs-explorer.jar

# Перезапуск сервиса (занимает 2–3 с):
sudo systemctl restart hdfs-explorer
```

---

## 3. Режим 2: Docker & Docker Compose

### 3.1 Образ из GHCR (рекомендуется)
Образы публикуются в GitHub Container Registry (`ghcr.io`):
```bash
# Авторизация (при необходимости):
echo "${GITHUB_TOKEN}" | docker login ghcr.io -u <github-username> --password-stdin

# Загрузка готового образа:
VERSION="1.0.0"
docker pull ghcr.io/company/hadoop-explorer/hdfs:${VERSION}
```

### 3.2 Локальная сборка образа (для разработки)
```bash
docker build -t ghcr.io/company/hadoop-explorer/hdfs:latest -f docker/Dockerfile.hdfs-java .
```

### 3.3 Запуск одного контейнера (`docker run`)
```bash
docker run -d \
  --name hdfs-explorer \
  --restart unless-stopped \
  -p 8000:8000 \
  -e SPRING_CONFIG_LOCATION=/app/config/application.yml \
  -e KRB5_CONFIG=/etc/krb5.conf \
  -v /opt/hdfs-explorer/application.yml:/app/config/application.yml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v hdfs-data:/app/data \
  ghcr.io/company/hadoop-explorer/hdfs:1.0.0
```

### 3.4 Запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  hdfs-explorer:
    image: ghcr.io/company/hadoop-explorer/hdfs:1.0.0
    container_name: hdfs-explorer
    restart: unless-stopped
    ports:
      - "8002:8000"
    environment:
      - SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/application.yml
      - KRB5_CONFIG=/etc/krb5.conf
      - JAVA_TOOL_OPTIONS=-Djava.security.krb5.conf=/etc/krb5.conf
    volumes:
      - ./application.yml:/app/config/application.yml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs:/etc/security/keytabs:ro
      - hdfs-storage:/app/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/healthz"]
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
  hdfs-storage:
    driver: local

networks:
  hadoop-net:
    driver: bridge
```

Запуск:
```bash
docker compose up -d
docker compose logs -f hdfs-explorer
```

---

## 4. Режим 3: Kubernetes (Helm)

### 4.1 Установка Helm-чарта
Чарт лежит в [`helm/charts/hdfs-explorer`](../helm/charts/hdfs-explorer).

#### Шаг 1: секреты Kubernetes
```bash
kubectl create namespace hadoop-explorer

kubectl create secret generic hdfs-explorer-secrets \
  --namespace hadoop-explorer \
  --from-literal=jwt-secret-key="Min32CharSecretKeyForHdfsExplorerJwt" \
  --from-literal=ldap-bind-password="SecretLdapPassword"

# Создание секрета для скачивания образов из GHCR
kubectl create secret docker-registry ghcr-secret \
  --namespace hadoop-explorer \
  --docker-server=ghcr.io \
  --docker-username="<github-username>" \
  --docker-password="<github-token-with-read:packages>"

kubectl create secret generic hdfs-kerberos-keytab \
  --namespace hadoop-explorer \
  --from-file=hdfs-explorer.keytab=/etc/security/keytabs/hdfs-explorer.keytab

kubectl create configmap krb5-config \
  --namespace hadoop-explorer \
  --from-file=krb5.conf=/etc/krb5.conf
```

#### Шаг 2: параметры `custom-values.yaml`
```yaml
replicaCount: 2

image:
  repository: ghcr.io/company/hadoop-explorer/hdfs
  tag: "1.0.0"
  pullPolicy: IfNotPresent

imagePullSecrets:
  - name: ghcr-secret

extraEnv:
  - name: JWT_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: hdfs-explorer-secrets
        key: jwt-secret-key
  - name: LDAP_BIND_PASSWORD
    valueFrom:
      secretKeyRef:
        name: hdfs-explorer-secrets
        key: ldap-bind-password

service:
  type: ClusterIP
  port: 80
  targetPort: 8000

ingress:
  enabled: true
  className: "nginx"
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-corp"
    nginx.ingress.kubernetes.io/proxy-body-size: "1024m" # Для загрузки больших файлов
    nginx.ingress.kubernetes.io/proxy-read-timeout: "300"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "300"
  hosts:
    - host: hdfs.company.local
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: hdfs-tls-cert
      hosts:
        - hdfs.company.local

resources:
  limits:
    cpu: "2"
    memory: "2Gi"
  requests:
    cpu: "500m"
    memory: "512Mi"

extraVolumes:
  - name: keytab-vol
    secret:
      secretName: hdfs-kerberos-keytab
      defaultMode: 0400
  - name: krb5-vol
    configMap:
      name: krb5-config

extraVolumeMounts:
  - name: keytab-vol
    mountPath: /etc/security/keytabs
    readOnly: true
  - name: krb5-vol
    mountPath: /etc/krb5.conf
    subPath: krb5.conf
    readOnly: true

config:
  database:
    url: "jdbc:postgresql://postgres-ha.database.svc.cluster.local:5432/hdfs_explorer"

  hdfs:
    clusters:
      - id: "prod-hdfs"
        name: "Production Cluster"
        hdfs-rpc-urls:
          - "hdfs://nn01.hadoop.company.local:8020"
          - "hdfs://nn02.hadoop.company.local:8020"
        webhdfs-urls:
          - "http://nn01.hadoop.company.local:9870/webhdfs/v1"
        auth-type: "kerberos"
        service-principal: "hdfs/hdfs.company.local@COMPANY.LOCAL"

podDisruptionBudget:
  minAvailable: 1
```

#### Шаг 3: установка через Helm
```bash
helm upgrade --install hdfs-explorer ./helm/charts/hdfs-explorer \
  --namespace hadoop-explorer \
  -f custom-values.yaml

kubectl get pods -n hadoop-explorer -l app.kubernetes.io/name=hdfs-explorer
```

---

## 5. Мониторинг и диагностика

### 5.1 Метрики Prometheus (`GET /metrics`)
Сервис не экспортирует метрики Prometheus: в `backend/hdfs` и `common-security-starter` нет зависимости `micrometer-registry-prometheus`, а Spring Boot Actuator открывает по HTTP только `/actuator/health` (с группами `liveness` и `readiness`). `GET /metrics` возвращает 404. Задание `hadoop-explorer` из `monitoring/prometheus/prometheus.yml` (`metrics_path: '/metrics'`) и дашборд `monitoring/grafana/dashboards/hdfs_explorer_operations.json` данных от сервиса не получат.

### 5.2 Известные проблемы

| Ошибка / Симптом | Причина | Решение |
|---|---|---|
| `Payload Too Large (HTTP 413)` | Ограничение Ingress на размер загрузки | Увеличьте значение аннотации `nginx.ingress.kubernetes.io/proxy-body-size: "1024m"`. |

---

## 6. Бэкенд на Java 21 / Spring Boot 3

Бэкенд (`backend/hdfs`) написан на Java 21 LTS и Spring Boot 3.3.4 и использует библиотеку Apache Hadoop Client (`org.apache.hadoop:hadoop-hdfs-client`).

### 6.1 Компоненты бэкенда
1. Клиент Hadoop FileSystem: обращается к NameNode напрямую по RPC (`hdfs://`) или HTTP (`webhdfs://`), без промежуточных шлюзов.
2. HA NameNode: переключение между Active и Standby NameNode через `ConfiguredFailoverProxyProvider`.
3. Kerberos GSS-API и doAs: операции выполняются от имени пользователя (`UserGroupInformation.createProxyUser`), поэтому политики Apache Ranger применяются к нему, а не к сервисной учетной записи.
4. Стартер `common-security-starter`: сессии (L1 Caffeine + L2 JDBC), CSRF Guard, ограничение частоты запросов Bucket4j и аудит через AOP `@Audited`.

### 6.2 Сборка и тестирование

```bash
# Модульное и интеграционное тестирование
make test-hdfs
# или
mvn test -f backend/hdfs/pom.xml

# Сборка исполняемого fat-JAR
make build-hdfs
# Результат: backend/hdfs/target/hdfs-explorer-java-1.0.0.jar

# Полная валидация всех Java-компонентов платформы
make test-java
```

### 6.3 Запуск JAR с параметрами в командной строке

```bash
java -jar -Dspring.profiles.active=prod \
  -Dserver.port=8001 \
  -Dhadoop.security.auth.mode=kerberos \
  -Dhadoop.security.jwt.secret-key="production-super-secret-key-min-32-chars!" \
  -Dhadoop.hdfs.clusters-config-path=/etc/hadoop-explorer/clusters.yaml \
  backend/hdfs/target/hdfs-explorer-java-1.0.0.jar
```


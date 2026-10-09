# 🛠️ Руководство администратора: YARN Explorer

Данный документ содержит полное руководство для инженеров **DevOps / SRE / системных администраторов** по установке, настройке, промышленному развертыванию и эксплуатации сервиса **YARN Explorer** в трех режимах:
1. **Standalone** (Bare-Metal / Виртуальные машины под управлением systemd)
2. **Docker & Docker Compose** (Контейнеризированный запуск)
3. **Kubernetes** (Промышленное развертывание через Helm-чарт и сырые манифесты)

---

## 1. Архитектура и требования

### 1.1 Назначение сервиса
**YARN Explorer** — компонент платформы Hadoop Explorer, обеспечивающий:
- Интерактивное визуальное управление очередями **Capacity Scheduler** (балансировка долей гарантированных и максимальных ресурсов, дочерние очереди).
- Многокластерный мониторинг приложений YARN в режиме реального времени.
- Автоматический **Failover при High Availability (RM HA)** между активным и резервным ResourceManager.
- Процесс согласования изменений **Change Requests (принцип Four-Eyes)** с аудитом.
- Автоматизированную доставку и применение `capacity-scheduler.xml` на кластер через **Ansible AWX / Tower API**.
- Корпоративную аутентификацию по протоколам **LDAP / Active Directory** и **Kerberos SPNEGO SSO**.

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8000` (или `8001`) | HTTP/HTTPS | Inbound | Веб-интерфейс SPA (Svelte 5) и REST API |
| `8088` / `8090` | HTTP/HTTPS | Outbound | YARN ResourceManager REST API (Active/Standby) |
| `80` / `443` | HTTP/HTTPS | Outbound | Ansible AWX / Tower REST API |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Сервер каталогов корпоративной аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- **Диагностика доступности (Healthcheck)**: `GET /healthz` (возвращает статус базы данных и компонентов).
- **Метрики Prometheus**: `GET /metrics` (экспорт золотых сигналов HTTP, Circuit Breaker и счетчиков).

### 1.3 Системные требования

#### Для промышленной эксплуатации (Runtime — запуск готового JAR / Docker):
- **ОС**: Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12, macOS).
- **Среда выполнения**: **Java 21 LTS JRE** (Eclipse Temurin 21 JRE / `openjdk-21-jre-headless`).
- **Системные библиотеки**: `krb5-user` (`krb5-workstation`), `curl`, `ca-certificates`.
- **Ресурсы на реплику**: CPU: `1 ядро` (рек. `2 ядра`), RAM: `1 ГБ` (рек. `2 ГБ`), Диск: `5–10 ГБ`.
- > [!IMPORTANT]
  > При установке из готовых релизов Git, Apache Maven, Node.js и npm на целевых серверах **НЕ требуются**.

#### Только для сборки из исходников (Build Environment):
- JDK: `21 LTS`
- Maven: `3.9+`
- Node.js: `20+` / `22 LTS` и npm `10+`

### 1.4 Официальные дистрибутивы (GitHub Releases & GHCR)
- **GitHub Releases (`v${VERSION}`)**:
  - `yarn-explorer-java-${VERSION}.jar` (~50 МБ) — Spring Boot 3 Fat JAR со встроенным веб-интерфейсом Svelte 5 SPA, REST API, интеграцией с AWX и драйверами БД;
  - `SHA256SUMS.txt` — контрольные суммы SHA-256 для верификации целостности файлов.
- **GitHub Container Registry (GHCR)**:
  - `ghcr.io/company/hadoop-explorer/yarn:${VERSION}`

---

## 2. Режим 1: Standalone (Bare-Metal / VM / systemd)

Развертывание на выделенном сервере или ВМ под управлением `systemd`.

### Шаг 1: Установка системных зависимостей

#### Вариант для Production (Запуск из готового релиза — только JRE):
```bash
# Ubuntu 22.04 / 24.04 LTS, Debian 12:
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless krb5-user ldap-utils curl ca-certificates

# RHEL 8/9, Rocky Linux 8/9, AlmaLinux:
sudo dnf install -y --setopt=install_weak_deps=False \
    java-21-openjdk-headless krb5-workstation openldap-clients curl ca-certificates
```

#### Альтернативный вариант (Только если требуется сборка из исходников на сервере):
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
sudo mkdir -p /opt/hadoop-explorer/yarn/releases
sudo mkdir -p /opt/hadoop-explorer/yarn/bin
sudo mkdir -p /etc/hadoop-explorer/yarn
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/yarn/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/yarn
```

### Шаг 3: Получение артефакта (Два варианта)

#### Способ А (Рекомендуемый для Production): Загрузка из GitHub Releases (No-Build)

В готовый бинарный JAR уже вшиты скомпилированный веб-интерфейс Svelte 5 SPA и все зависимости:

```bash
sudo -u appuser -i

VERSION="1.0.0"
GITHUB_REPO="company/hadoop-explorer"
RELEASE_DIR="/opt/hadoop-explorer/yarn/releases/v${VERSION}"
BIN_DIR="/opt/hadoop-explorer/yarn/bin"

mkdir -p "${RELEASE_DIR}" "${BIN_DIR}" && cd "${RELEASE_DIR}"

# 1. Загрузка через curl по прямой ссылке (или через `gh release download`):
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/yarn-explorer-java-${VERSION}.jar"
curl -fsSL -O "https://github.com/${GITHUB_REPO}/releases/download/v${VERSION}/SHA256SUMS.txt"

# 2. Проверка контрольной суммы:
sha256sum -c SHA256SUMS.txt --ignore-missing

# 3. Создание стабильной символической ссылки:
ln -sfn "${RELEASE_DIR}/yarn-explorer-java-${VERSION}.jar" "${BIN_DIR}/yarn-explorer.jar"
```

#### Способ Б: Сборка из исходных кодов (для разработчиков)
```bash
sudo -u appuser -i
cd /tmp
git clone https://github.com/company/hadoop-explorer.git
cd hadoop-explorer

# Сборка фронтенда (Svelte 5)
cd frontend
npm ci --workspace=apps/yarn --include-workspace-root
npm run build --workspace=apps/yarn
cd ..

# Копирование статики фронтенда в ресурсы Spring Boot
cp -r frontend/apps/yarn/dist/* backend/yarn/src/main/resources/static/

# Сборка исполняемого Spring Boot fat JAR
mvn clean package -DskipTests -f backend/yarn/pom.xml

# Копирование собранного JAR в рабочий каталог
cp backend/yarn/target/yarn-explorer-java-*.jar /opt/hadoop-explorer/yarn/bin/yarn-explorer.jar
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/hadoop-explorer/yarn/application.yml`:
```yaml
server:
  port: 8000

spring:
  application:
    name: yarn-explorer-java
  datasource:
    # Для автономной работы используется embedded H2:
    url: jdbc:h2:file:/var/lib/hadoop-explorer/yarn/data/yarn_explorer;DB_CLOSE_DELAY=-1
    driverClassName: org.h2.Driver
    # Для промышленного кластера рекомендуется PostgreSQL:
    # url: jdbc:postgresql://pg-cluster.company.local:5432/yarn_explorer
    # username: yarn_user
    # password: StrongPass123
  jpa:
    hibernate:
      ddl-auto: update

# Общие параметры безопасности платформы
hadoop:
  security:
    auth-mode: ldap # mock | ldap | kerberos
    jwt:
      secret: "GENERATE_SECURE_KEY_AT_LEAST_32_CHARACTERS_LONG"
      expiration-minutes: 480
    ldap:
      enabled: true
      server-uri: "ldaps://ldap.company.local:636"
      bind-dn: "cn=svc_hadoop,ou=services,dc=company,dc=local"
      bind-password: "SecureServicePassword"
      user-search-base: "ou=users,dc=company,dc=local"
      user-search-filter: "(sAMAccountName={0})"
      group-search-base: "ou=groups,dc=company,dc=local"
      group-search-filter: "(member={0})"
    kerberos:
      enabled: true
      keytab-path: "/etc/security/keytabs/yarn-explorer.keytab"
      principal: "HTTP/yarn.company.local@COMPANY.LOCAL"

# Модуль управления очередями YARN
yarn:
  acl:
    enforce-four-eyes: true
  awx:
    enabled: true
    base-url: "https://awx.company.local"
    token: "AWX_OAUTH2_APPLICATION_TOKEN"
    job-template-id: 42
    verify-ssl: true
  clusters:
    - id: "prod-cluster"
      name: "Production DataLake"
      resource-manager-urls:
        - "http://rm01.prod.company.local:8088"
        - "http://rm02.prod.company.local:8088"
      kerberos-enabled: true
      kerberos-principal: "yarn/rm01.prod.company.local@COMPANY.LOCAL"
      impersonation-enabled: true
      default-partition: "DEFAULT"
      partitions: ["DEFAULT", "GPU", "HIGH_MEM"]
      resource-mode: "percentage"
      total-resources:
        memory-mb: 2097152
        vcores: 1024
      acl:
        allowed-users: ["*"]
        allowed-groups: ["*"]
        roles:
          admin:
            groups: ["hadoop-admins"]
          writer:
            groups: ["yarn-operators"]
          reader:
            groups: ["*"]
```

Установите строгие права доступа к файлу:
```bash
sudo chmod 600 /etc/hadoop-explorer/yarn/application.yml
sudo chown appuser:appuser /etc/hadoop-explorer/yarn/application.yml
```

### Шаг 5: Создание systemd сервиса
Создайте файл `/etc/systemd/system/yarn-explorer.service`:
```ini
[Unit]
Description=Hadoop YARN Explorer Java Service (Spring Boot 3)
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/hadoop-explorer/yarn
Environment="SPRING_CONFIG_LOCATION=/etc/hadoop-explorer/yarn/application.yml"
Environment="KRB5_CONFIG=/etc/krb5.conf"

ExecStart=/usr/bin/java \
    -Xms512m \
    -Xmx2048m \
    -Dspring.config.location=/etc/hadoop-explorer/yarn/application.yml \
    -jar /opt/hadoop-explorer/yarn/bin/yarn-explorer.jar

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
sudo systemctl enable yarn-explorer
sudo systemctl start yarn-explorer

# Проверка статуса
sudo systemctl status yarn-explorer

# Проверка healthcheck эндпоинта
curl -I http://127.0.0.1:8000/healthz
# HTTP/1.1 200 OK

# Просмотр логов в реальном времени
journalctl -u yarn-explorer -f
```

### Шаг 7: Бесшовное обновление версий (Rolling Upgrade)

При выходе новой версии на GitHub Releases:
```bash
sudo -u appuser -i
NEW_VER="1.0.1"
RELEASE_DIR="/opt/hadoop-explorer/yarn/releases/v${NEW_VER}"
mkdir -p "${RELEASE_DIR}" && cd "${RELEASE_DIR}"

curl -fsSL -O "https://github.com/company/hadoop-explorer/releases/download/v${NEW_VER}/yarn-explorer-java-${NEW_VER}.jar"
curl -fsSL -O "https://github.com/company/hadoop-explorer/releases/download/v${NEW_VER}/SHA256SUMS.txt"
sha256sum -c SHA256SUMS.txt --ignore-missing

# Атомарное переключение симлинка:
ln -sfn "${RELEASE_DIR}/yarn-explorer-java-${NEW_VER}.jar" /opt/hadoop-explorer/yarn/bin/yarn-explorer.jar

# Перезапуск сервиса (занимает 2–3 сек):
sudo systemctl restart yarn-explorer
```

---

## 3. Режим 2: Docker & Docker Compose

### 3.1 Запуск из предсобранного образа GHCR (Рекомендуется)
Готовые образы публикуются в GitHub Container Registry (`ghcr.io`):
```bash
# Авторизация (при необходимости):
echo "${GITHUB_TOKEN}" | docker login ghcr.io -u <github-username> --password-stdin

# Загрузка готового образа:
VERSION="1.0.0"
docker pull ghcr.io/company/hadoop-explorer/yarn:${VERSION}
```

### 3.2 Альтернатива: Локальная сборка Docker-образа (для разработки)
```bash
docker build -t ghcr.io/company/hadoop-explorer/yarn:latest -f docker/Dockerfile.yarn-java .
```

### 3.3 Автономный запуск одного контейнера (`docker run`)
```bash
docker run -d \
  --name yarn-explorer \
  --restart unless-stopped \
  -p 8000:8000 \
  -e SPRING_CONFIG_LOCATION=/app/config/application.yml \
  -e KRB5_CONFIG=/etc/krb5.conf \
  -v /opt/yarn-explorer/application.yml:/app/config/application.yml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v yarn-data:/app/data \
  ghcr.io/company/hadoop-explorer/yarn:1.0.0
```

### 3.4 Промышленный запуск через `docker-compose.yml`
Создайте директорию `/opt/yarn-docker/` со следующим `docker-compose.yml`:
```yaml
version: "3.8"

services:
  yarn-explorer:
    image: ghcr.io/company/hadoop-explorer/yarn:1.0.0
    container_name: yarn-explorer
    restart: unless-stopped
    ports:
      - "8001:8000"
    environment:
      - APP_NAME=yarn
      - WEB_CONCURRENCY=2
      - CONFIG_PATH=/app/config/config.yaml
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_BIND_PASSWORD=${LDAP_BIND_PASSWORD}
      - AWX_TOKEN=${AWX_TOKEN}
      - KRB5_KEYTAB=/etc/security/keytabs/yarn-explorer.keytab
      - KRB5_PRINCIPAL=HTTP/yarn.company.local@COMPANY.LOCAL
    volumes:
      - ./config.yaml:/app/config/config.yaml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs/yarn-explorer.keytab:/etc/security/keytabs/yarn-explorer.keytab:ro
      - yarn-storage:/app/data
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
  yarn-storage:
    driver: local

networks:
  hadoop-net:
    driver: bridge
```

Файл окружения `.env`:
```bash
JWT_SECRET_KEY=9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b
LDAP_BIND_PASSWORD=SuperSecretLdapPass
AWX_TOKEN=VeryLongSecretAwxOAuthToken
```

Запуск и мониторинг:
```bash
docker compose up -d
docker compose ps
docker compose logs -f yarn-explorer
```

---

## 4. Режим 3: Kubernetes (Helm & Raw Manifests)

### 4.1 Развертывание через официальный Helm-чарт
В репозитории подготовлен чарт [`helm/charts/yarn-explorer`](../helm/charts/yarn-explorer).

#### Шаг 1: Подготовка секретов Kubernetes
```bash
kubectl create namespace hadoop-explorer

# Создание секрета с паролями и токенами
kubectl create secret generic yarn-explorer-secrets \
  --namespace hadoop-explorer \
  --from-literal=jwt-secret-key="Min32CharSecretKeyForJWTTokens12345" \
  --from-literal=ldap-bind-password="SecretLdapPassword" \
  --from-literal=awx-token="SecretAwxOAuthToken"

# Создание секрета для скачивания образов из GHCR
kubectl create secret docker-registry ghcr-secret \
  --namespace hadoop-explorer \
  --docker-server=ghcr.io \
  --docker-username="<github-username>" \
  --docker-password="<github-token-with-read:packages>"

# Создание секрета с Kerberos Keytab
kubectl create secret generic yarn-kerberos-keytab \
  --namespace hadoop-explorer \
  --from-file=yarn-explorer.keytab=/etc/security/keytabs/yarn-explorer.keytab

# Создание ConfigMap с krb5.conf
kubectl create configmap krb5-config \
  --namespace hadoop-explorer \
  --from-file=krb5.conf=/etc/krb5.conf
```

#### Шаг 2: Файл параметров `custom-values.yaml`
```yaml
# Количество реплик. При внешней PostgreSQL БД можно масштабировать до 2-5 реплик.
replicaCount: 2

image:
  repository: ghcr.io/company/hadoop-explorer/yarn
  tag: "1.0.0"
  pullPolicy: IfNotPresent

imagePullSecrets:
  - name: ghcr-secret

extraEnv:
  - name: JWT_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: yarn-explorer-secrets
        key: jwt-secret-key
  - name: LDAP_BIND_PASSWORD
    valueFrom:
      secretKeyRef:
        name: yarn-explorer-secrets
        key: ldap-bind-password
  - name: AWX_TOKEN
    valueFrom:
      secretKeyRef:
        name: yarn-explorer-secrets
        key: awx-token

service:
  type: ClusterIP
  port: 80
  targetPort: 8000

ingress:
  enabled: true
  className: "nginx"
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-corp"
    nginx.ingress.kubernetes.io/proxy-body-size: "64m"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "120"
  hosts:
    - host: yarn.company.local
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: yarn-tls-cert
      hosts:
        - yarn.company.local

resources:
  limits:
    cpu: "2"
    memory: "2Gi"
  requests:
    cpu: "500m"
    memory: "512Mi"

# Монтирование Kerberos Keytab и krb5.conf
extraVolumes:
  - name: keytab-vol
    secret:
      secretName: yarn-kerberos-keytab
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

# Внешняя БД PostgreSQL для сессий и заявок в режиме High Availability
config:
  database:
    url: "postgresql://yarn_user:DbPass123@postgres-ha.database.svc.cluster.local:5432/yarn_explorer"

  yarn:
    clusters:
      - id: "prod-yarn"
        name: "Hadoop Production"
        active_rm_url: "http://rm01.hadoop.company.local:8088"
        standby_rm_url: "http://rm02.hadoop.company.local:8088"
        kerberos_enabled: true
        principal: "yarn/yarn.company.local@COMPANY.LOCAL"

podDisruptionBudget:
  minAvailable: 1
```

#### Шаг 3: Установка и обновление через Helm
```bash
# Установка чарта
helm upgrade --install yarn-explorer ./helm/charts/yarn-explorer \
  --namespace hadoop-explorer \
  -f custom-values.yaml

# Проверка статуса подов
kubectl get pods -n hadoop-explorer -l app.kubernetes.io/name=yarn-explorer

# Просмотр логов
kubectl logs -n hadoop-explorer -l app.kubernetes.io/name=yarn-explorer -f
```

---

## 5. Мониторинг, Аудит и Troubleshooting

### 5.1 Метрики Prometheus
Эндпоинт `/metrics` экспортирует следующие метрики:
- `http_requests_total{app="yarn", method="GET", status="200"}`
- `http_request_duration_seconds{app="yarn", ...}`
- `hadoop_circuit_breaker_state{name="yarn_rm"}` (0=CLOSED, 1=HALF_OPEN, 2=OPEN)
- `hadoop_circuit_breaker_calls_total{name="yarn_rm", status="success|failed"}`
- `yarn_change_requests_total{status="pending|approved|rejected|applied"}`

**Пример Prometheus Scrape Config**:
```yaml
- job_name: 'yarn-explorer'
  metrics_path: '/metrics'
  static_configs:
    - targets: ['yarn.company.local:8000']
```

### 5.2 Решение типичных инцидентов (FAQ)

| Симптом / Ошибка | Возможная причина | Решение |
|---|---|---|
| `HTTP 502 Bad Gateway` при запросе к YARN RM | Упал активный ResourceManager | Проверить состояние RM HA: сервис автоматически переключится на standby. Проверьте `active_rm_url` и `standby_rm_url` в конфиге. |
| `GSSException: No valid credentials provided` | Истек или недоступен Kerberos keytab | Проверить права доступа (`chmod 400`), соответствие principal и наличие файла `kinit -kt /path/to.keytab principal@REALM`. |
| `LDAP InvalidCredentialsResult` | Ошибка учетных данных в `bind_password` | Проверить пароль сервисной учетной записи через `ldapsearch -H ldaps://... -D "..." -W`. |
| `Database is locked (sqlite3.OperationalError)` | Несколько воркеров/подов пишут в один SQLite | При масштабировании на 2+ реплики обязательно переключите `database.url` на **PostgreSQL**. |
| `AWX Job launch failed (HTTP 401/403)` | Невалидный или истекший OAuth-токен AWX | Перевыпустите токен в веб-интерфейсе AWX и обновите `AWX_TOKEN` в секретах. |

---

## 6. Резервное копирование и восстановление (Disaster Recovery)

- **Конфигурация**: Рекомендуется хранить все `values.yaml` и `config.yaml` в Git-репозитории инфраструктуры (GitOps / ArgoCD / Flux).
- **База данных PostgreSQL**:
  ```bash
  # Создание резервной копии
  pg_dump -h pg-host -U yarn_user yarn_explorer > yarn_explorer_backup_$(date +%F).sql

  # Восстановление
  psql -h pg-host -U yarn_user yarn_explorer < yarn_explorer_backup_2026-10-08.sql
  ```
- **Локальная SQLite (Standalone)**:
  ```bash
  sqlite3 /var/lib/hadoop-explorer/yarn/data/yarn_explorer.db ".backup '/backup/yarn_explorer_$(date +%F).db'"
  ```

---

## 7. Развертывание и эксплуатация бэкенда на Java 21 / Spring Boot 3

Бэкенд **YARN Explorer полностью функционирует на высокопроизводительном нативном стеке Java 21 LTS и Spring Boot 3.3.4** (`backend/yarn`), используя официальные библиотеки Apache Hadoop YARN Client (`org.apache.hadoop:hadoop-yarn-client`).

### 7.1 Преимущества Java 21 реализации
1. **Нативный YARN Client и RM HA Failover**: прямое подключение к REST API / RPC активного ResourceManager с автоматическим обнаружением и переключением на standby-узел при сбоях (`haState == "ACTIVE"`).
2. **Capacity Scheduler Engine**: встроенная валидация веток и правила 100% емкости, глубокий расчет diff для долей ресурсов и node labels, генерация и XXE-защищенная санитизация XML (`capacity-scheduler.xml`).
3. **Change Requests & Four-Eyes Principle**: встроенный жизненный цикл согласования изменений с запретом самосогласования заявок автором (HTTP 403 Forbidden).
4. **Интеграция с Ansible AWX**: автоматический триггер Job Template через REST API для Zero-Downtime обновления очередей (`yarn rmadmin -refreshQueues`).
5. **Общее ядро безопасности `common-security-starter`**: единая модель аутентификации (Kerberos SPNEGO SSO, LDAP), двухуровневое хранилище сессий (L1 Caffeine + L2 JDBC), защита от CSRF, Bucket4j Rate Limiter и AOP-аудит `@Audited`.

### 7.2 Сборка и тестирование

```bash
# Модульное и интеграционное тестирование
make test-yarn
# или
mvn test -f backend/yarn/pom.xml

# Сборка исполняемого Spring Boot fat JAR
make build-yarn
# Результат: backend/yarn/target/yarn-explorer-java-1.0.0.jar

# Полная валидация всех Java компонентов платформы
make test-java
```

### 7.3 Промышленный запуск

```bash
java -jar -Dspring.profiles.active=prod \
  -Dserver.port=8000 \
  -Dhadoop.security.auth.mode=kerberos \
  -Dhadoop.security.jwt.secret-key="production-super-secret-key-min-32-chars!" \
  -Dawx.base-url="https://awx.company.local" \
  -Dawx.token="secret-awx-token" \
  -Dawx.job-template-id=42 \
  backend/yarn/target/yarn-explorer-java-1.0.0.jar
```


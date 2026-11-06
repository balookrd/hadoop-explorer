# Руководство администратора YARN Explorer

Руководство описывает установку, настройку и эксплуатацию YARN Explorer. Оно рассчитано на DevOps- и SRE-инженеров и системных администраторов. Описаны три способа развертывания:
1. Standalone: сервер или виртуальная машина, процесс под управлением systemd.
2. Docker и Docker Compose.
3. Kubernetes через Helm-чарт.

---

## 1. Функции и требования

### 1.1 Функции сервиса
YARN Explorer — компонент платформы Hadoop Explorer. Функции:
- управление очередями Capacity Scheduler через веб-интерфейс: доли гарантированных и максимальных ресурсов, дочерние очереди;
- просмотр загрузки ресурсов кластера (vCores, RAM) и очередей;
- переключение между активным и резервным ResourceManager при отказе (RM HA);
- согласование изменений через заявки (Change Requests) по принципу четырех глаз (Four-Eyes), с аудитом;
- доставка и применение изменений `capacity-scheduler.xml` на кластер через Ansible AWX / Tower API;
- аутентификация через LDAP / Active Directory и Kerberos SPNEGO SSO.

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8000` (или `8001`) | HTTP/HTTPS | Inbound | Веб-интерфейс SPA (Svelte 5) и REST API |
| `8088` / `8090` | HTTP/HTTPS | Outbound | YARN ResourceManager REST API (Active/Standby) |
| `80` / `443` | HTTP/HTTPS | Outbound | Ansible AWX / Tower REST API |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Сервер каталогов корпоративной аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- Проверка доступности: `GET /healthz` (а также `GET /health`) возвращает `{"status":"ok"}`.
- Readiness: `GET /actuator/health/readiness` — 200 `UP` после `ApplicationReadyEvent`, 503 `OUT_OF_SERVICE` с начала остановки контекста приложения.
- Метрики Prometheus сервис не экспортирует, см. [5.1](#51-метрики-prometheus).

### 1.3 Системные требования

#### Запуск готового JAR или Docker-образа
- ОС: Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12) или macOS.
- Среда выполнения: Java 21 LTS JRE (Eclipse Temurin 21 JRE / `openjdk-21-jre-headless`).
- Системные пакеты: `krb5-user` (`krb5-workstation`), `curl`, `ca-certificates`.
- Ресурсы на реплику: CPU — 1 ядро (рекомендуется 2), RAM — 1 ГБ (рекомендуется 2 ГБ), диск — 5–10 ГБ.

Git, Apache Maven, Node.js и npm для запуска готового релиза не нужны.

#### Сборка из исходников
- JDK: `21 LTS`
- Maven: `3.9+`
- Node.js: `20+` / `22 LTS` и npm `10+`

### 1.4 Дистрибутивы
- GitHub Releases (`v${VERSION}`):
  - `yarn-explorer-java-${VERSION}.jar` (~50 МБ) — Spring Boot 3 Fat JAR: веб-интерфейс (Svelte 5 SPA), REST API, интеграция с AWX, драйверы БД;
  - `SHA256SUMS.txt` — контрольные суммы SHA-256 файлов релиза.
- GitHub Container Registry (GHCR):
  - `ghcr.io/company/hadoop-explorer/yarn:${VERSION}`

---

## 2. Standalone: сервер или ВМ под systemd

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
sudo mkdir -p /opt/hadoop-explorer/yarn/releases
sudo mkdir -p /opt/hadoop-explorer/yarn/bin
sudo mkdir -p /etc/hadoop-explorer/yarn
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/yarn/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/yarn
```

### Шаг 3: Получение JAR

#### Способ А: загрузка из GitHub Releases

Релизный JAR уже содержит собранный веб-интерфейс и все зависимости, сборка на сервере не нужна.

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

#### Способ Б: сборка из исходников
```bash
sudo -u appuser -i
cd /tmp
git clone https://github.com/company/hadoop-explorer.git
cd hadoop-explorer

# Сборка фронтенда (Svelte 5)
cd frontend
npm ci --workspace=apps/yarn --include-workspace-root
# Бандл собирается сразу в backend/yarn/src/main/resources/static и попадает в JAR
npm run build --workspace=apps/yarn
cd ..

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
    # PostgreSQL заработает только после добавления org.postgresql:postgresql
    # в backend/yarn/pom.xml: в JAR драйвер не входит.
    # url: jdbc:postgresql://pg-cluster.company.local:5432/yarn_explorer
    # username: yarn_user
    # password: StrongPass123
  jpa:
    hibernate:
      ddl-auto: update

# Общие параметры безопасности платформы
hadoop:
  security:
    auth:
      mode: ldap # ldap | kerberos (mock — только для стендов, требует debug: true)
    jwt:
      # Случайная строка не короче 32 символов; без нее сервис не стартует
      secret-key: "${JWT_SECRET_KEY}"
      expiration-minutes: 480
    ldap:
      enabled: true
      url: "ldaps://ldap.company.local:636"
      manager-dn: "cn=svc_hadoop,ou=services,dc=company,dc=local"
      manager-password: "${LDAP_MANAGER_PASSWORD}"
      user-search-base: "ou=users,dc=company,dc=local"
      user-search-filter: "(sAMAccountName={0})"
      group-search-base: "ou=groups,dc=company,dc=local"
      group-search-filter: "(member={0})"
    kerberos:
      # Входящий SPNEGO (SSO в браузере)
      enabled: true
      keytab: "/etc/security/keytabs/yarn-explorer.keytab"
      service-principal: "HTTP/yarn.company.local@COMPANY.LOCAL"

# Модуль управления очередями YARN
yarn:
  # native — ResourceManager REST API; mock — встроенное демо-дерево (только для стендов)
  client-mode: native
  acl:
    enforce-four-eyes: true
  # Учетная запись Explorer для исходящих SPNEGO-запросов к RM с kerberos-enabled: true.
  # Должна быть proxy user в core-site.xml кластера (hadoop.proxyuser.<user>.hosts/groups),
  # если включен impersonation-enabled.
  kerberos:
    principal: "yarn-explorer@COMPANY.LOCAL"
    keytab: "/etc/security/keytabs/yarn-explorer.keytab"
    krb5-conf: "/etc/krb5.conf"
  awx:
    enabled: true
    base-url: "https://awx.company.local"
    token: "${YARN_AWX_TOKEN}"
    default-job-template-id: 42
    verify-ssl: true
    poll-interval-seconds: 2
    timeout-seconds: 180
  clusters:
    - id: "prod-cluster"
      name: "Production DataLake"
      resource-manager-urls:
        - "http://rm01.prod.company.local:8088"
        - "http://rm02.prod.company.local:8088"
      kerberos-enabled: true
      impersonation-enabled: true
      default-partition: "DEFAULT"
      partitions: ["DEFAULT", "GPU", "HIGH_MEM"]
      resource-mode: "percentage"
      # Текущие маппинги очередей с кластера: RM не отдает их по REST,
      # поэтому UI показывает это значение как «текущее»
      queue-mappings: "u:%user:%user"
      queue-mappings-override: false
      total-resources:
        memory-mb: 2097152
        vcores: 1024
      acl:
        roles:
          admin:
            groups: ["hadoop-admins"]
          writer:
            groups: ["yarn-operators"]
          reader:
            groups: ["*"]
```

#### Проверки на старте

Сервис откажется запускаться, если конфигурация небезопасна:

- `hadoop.security.auth.mode` не задан или равен `mock` без `hadoop.security.debug: true`. По умолчанию действует именно mock: логин демо-учетками и фиктивные данные очередей. Раньше опечатка в ключе (например, `auth-mode` вместо `auth.mode`) молча включала этот режим;
- JWT-секрет не задан, короче 32 символов или совпадает со встроенным значением из исходников;
- `auth.mode: ldap` при `ldap.enabled: false`.

Ключи из старых версий этого руководства (`auth-mode`, `ldap.server-uri`, `bind-dn`, `bind-password`, `user-base-dn`, `user-filter`, `kerberos.keytab-path`, `kerberos.principal`) по-прежнему принимаются как синонимы. Для новых конфигураций используйте ключи из примера выше.

#### Как применяются изменения очередей

Explorer не генерирует `capacity-scheduler.xml` целиком. При файловом хранилище конфигурации ResourceManager не отдает этот файл по REST: `/conf` возвращает конфигурацию демона без единого ключа `yarn.scheduler.capacity.*`. Поэтому заявка превращается в список правок только тех свойств, которые она меняет. AWX-роль `yarn_capacity_scheduler` применяет эти правки к файлу на RM, выполняет `yarn rmadmin -refreshQueues` и при ошибке откатывает изменения. ACL очередей, глобальные лимиты, `resource-calculator` и прочие свойства, которых заявка не касается, остаются как есть. Подробности — в [awx-yarn-deployment.md](awx-yarn-deployment.md).

Заявка проверяется при создании и повторно при согласовании, уже против текущего состояния кластера. Проверяются: сумма емкостей дочерних очередей (100% для каждого раздела), существование родителя, удаление только остановленных (`STOPPED`) очередей, допустимость `ordering-policy` для листовых и родительских очередей.

Закройте файл от других пользователей: в нем могут быть пароли и токены.
```bash
sudo chmod 600 /etc/hadoop-explorer/yarn/application.yml
sudo chown appuser:appuser /etc/hadoop-explorer/yarn/application.yml
```

### Шаг 5: Создание systemd-сервиса
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

# Проверка healthcheck-эндпоинта
curl -I http://127.0.0.1:8000/healthz
# HTTP/1.1 200 OK

# Просмотр логов в реальном времени
journalctl -u yarn-explorer -f
```

### Шаг 7: Обновление версии

Скачайте новый релиз, проверьте контрольную сумму, переключите символическую ссылку и перезапустите сервис. На время перезапуска единственный экземпляр сервиса недоступен.
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

# Перезапуск сервиса (занимает 2–3 секунды):
sudo systemctl restart yarn-explorer
```

---

## 3. Docker и Docker Compose

### 3.1 Загрузка образа из GHCR
Образы публикуются в GitHub Container Registry (`ghcr.io`):
```bash
# Авторизация (при необходимости):
echo "${GITHUB_TOKEN}" | docker login ghcr.io -u <github-username> --password-stdin

# Загрузка готового образа:
VERSION="1.0.0"
docker pull ghcr.io/company/hadoop-explorer/yarn:${VERSION}
```

### 3.2 Локальная сборка образа
```bash
docker build -t ghcr.io/company/hadoop-explorer/yarn:latest -f docker/Dockerfile.yarn-java .
```

### 3.3 Запуск одного контейнера (`docker run`)
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

### 3.4 Запуск через Docker Compose
Создайте каталог `/opt/yarn-docker/` и в нем файл `docker-compose.yml`:

Переменные `JWT_SECRET_KEY`, `LDAP_MANAGER_PASSWORD` и `YARN_AWX_TOKEN` подставляются в `application.yml` из шага 4 (`${JWT_SECRET_KEY}` и т. д.). Principal и keytab для запросов к ResourceManager задаются в том же файле: `yarn.kerberos.principal`, `yarn.kerberos.keytab`.
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
      - SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/application.yml
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_MANAGER_PASSWORD=${LDAP_MANAGER_PASSWORD}
      - YARN_AWX_TOKEN=${YARN_AWX_TOKEN}
    volumes:
      - ./application.yml:/app/config/application.yml:ro
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

Создайте рядом файл `.env`:
```bash
JWT_SECRET_KEY=9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b
LDAP_MANAGER_PASSWORD=SuperSecretLdapPass
YARN_AWX_TOKEN=VeryLongSecretAwxOAuthToken
```

Запустите сервис и проверьте состояние:
```bash
docker compose up -d
docker compose ps
docker compose logs -f yarn-explorer
```

---

## 4. Kubernetes (Helm)

### 4.1 Развертывание Helm-чартом
Чарт лежит в [`helm/charts/yarn-explorer`](../helm/charts/yarn-explorer).

#### Шаг 1: Подготовка секретов Kubernetes
Чарт сам создает Secret `<fullname>-secrets` из `secrets.*` (при имени релиза `yarn-explorer` — `yarn-explorer-secrets`), поэтому не создавайте Secret с таким именем вручную: `helm install` остановится с ошибкой о существующем ресурсе.

```bash
kubectl create namespace hadoop-explorer

# Пароль LDAP и токен AWX; сервис читает их из LDAP_MANAGER_PASSWORD и YARN_AWX_TOKEN
kubectl create secret generic yarn-explorer-credentials \
  --namespace hadoop-explorer \
  --from-literal=ldap-manager-password="SecretLdapPassword" \
  --from-literal=awx-token="SecretAwxOAuthToken"

# Создание секрета для скачивания образов из GHCR
kubectl create secret docker-registry ghcr-secret \
  --namespace hadoop-explorer \
  --docker-server=ghcr.io \
  --docker-username="<github-username>" \
  --docker-password="<github-token-with-read:packages>"

# Keytab; ключ в Secret совпадает с kerberos.keytab.fileName (yarn-explorer.keytab)
kubectl create secret generic yarn-kerberos-keytab \
  --namespace hadoop-explorer \
  --from-file=yarn-explorer.keytab=/etc/security/keytabs/yarn-explorer.keytab
```

#### Шаг 2: Файл параметров `custom-values.yaml`

Чарт монтирует секцию `config` как `/app/config/config.yaml`, но сам не сообщает сервису путь к этому файлу. Путь передается через `extraEnv` в `SPRING_CONFIG_ADDITIONAL_LOCATION`, и тогда секция `config` читается как Spring-конфигурация с теми же ключами, что `application.yml` на шаге 4 раздела 2. Ключи из `values.yaml` чарта по умолчанию (`config.auth`, `config.acl`, `config.clusters` и т. п.) попадают в тот же файл, но сервис их не читает.

```yaml
# Один под: в JAR нет JDBC-драйвера PostgreSQL, а встроенную H2 на PVC
# (/app/data/yarn_explorer.mv.db) несколько подов использовать не могут.
replicaCount: 1

image:
  repository: ghcr.io/company/hadoop-explorer/yarn
  tag: "1.0.0"
  pullPolicy: IfNotPresent

imagePullSecrets:
  - name: ghcr-secret

# JWT_SECRET_KEY чарт передает сам из secrets.jwtSecret.
# Случайная строка не короче 32 символов: значение по умолчанию из values.yaml публично.
secrets:
  jwtSecret: "<jwt_secret>"

extraEnv:
  - name: SPRING_CONFIG_ADDITIONAL_LOCATION
    value: "file:/app/config/config.yaml"
  - name: LDAP_MANAGER_PASSWORD
    valueFrom:
      secretKeyRef:
        name: yarn-explorer-credentials
        key: ldap-manager-password
  - name: YARN_AWX_TOKEN
    valueFrom:
      secretKeyRef:
        name: yarn-explorer-credentials
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

# krb5.conf (kerberos.krb5Config) передается при установке через --set-file, см. шаг 3.
# Keytab монтируется из Secret yarn-kerberos-keytab в /etc/security/keytabs/yarn-explorer.keytab.
kerberos:
  enabled: true
  keytab:
    existingSecret: yarn-kerberos-keytab
    fileName: yarn-explorer.keytab
    mountPath: /etc/security/keytabs

# Spring-конфигурация сервиса (ключи как в application.yml)
config:
  hadoop:
    security:
      auth:
        mode: ldap
      jwt:
        secret-key: "${JWT_SECRET_KEY}"
      ldap:
        enabled: true
        url: "ldaps://ldap.company.local:636"
        manager-dn: "cn=svc_hadoop,ou=services,dc=company,dc=local"
        manager-password: "${LDAP_MANAGER_PASSWORD}"
        user-search-base: "ou=users,dc=company,dc=local"
        user-search-filter: "(sAMAccountName={0})"
  yarn:
    client-mode: native
    kerberos:
      principal: "yarn-explorer@COMPANY.LOCAL"
      keytab: "/etc/security/keytabs/yarn-explorer.keytab"
      krb5-conf: "/etc/krb5.conf"
    awx:
      enabled: true
      base-url: "https://awx.company.local"
      token: "${YARN_AWX_TOKEN}"
    clusters:
      - id: "prod-yarn"
        name: "Hadoop Production"
        resource-manager-urls:
          - "http://rm01.hadoop.company.local:8088"
          - "http://rm02.hadoop.company.local:8088"
        kerberos-enabled: true
        kerberos-principal: "yarn/yarn.company.local@COMPANY.LOCAL"
```

Плейсхолдер `<jwt_secret>` — случайная строка, например вывод `openssl rand -hex 32`.

#### Шаг 3: Установка и обновление через Helm
```bash
# Установка чарта
helm upgrade --install yarn-explorer ./helm/charts/yarn-explorer \
  --namespace hadoop-explorer \
  -f custom-values.yaml \
  --set-file kerberos.krb5Config=/etc/krb5.conf

# Проверка статуса подов
kubectl get pods -n hadoop-explorer -l app.kubernetes.io/name=yarn-explorer

# Просмотр логов
kubectl logs -n hadoop-explorer -l app.kubernetes.io/name=yarn-explorer -f
```

---

## 5. Мониторинг и диагностика

### 5.1 Метрики Prometheus
Сервис не экспортирует метрики Prometheus: в `backend/yarn` и `common-security-starter` нет зависимости `micrometer-registry-prometheus`, а Spring Boot Actuator открывает по HTTP только `/actuator/health` (с группами `liveness` и `readiness`). `GET /metrics` возвращает 404, поэтому задание `hadoop-explorer` из `monitoring/prometheus/prometheus.yml` (`metrics_path: '/metrics'`) данных от сервиса не получит.

### 5.2 Типичные ошибки

| Симптом или ошибка | Причина | Что сделать |
|---|---|---|
| `HTTP 502 Bad Gateway` при запросе к YARN RM | Упал активный ResourceManager | Проверьте состояние RM HA: сервис автоматически переключится на standby-узел. Проверьте список `resource-manager-urls` в конфигурационном файле `application.yml`. |
| `GSSException: No valid credentials provided` | Истек или недоступен Kerberos keytab | Проверьте права доступа к keytab (`chmod 400`), корректность `principal` и наличие файла keytab, а также проверьте получение тикета командой `kinit -kt /path/to.keytab principal@REALM`. |
| `LDAP InvalidCredentialsResult` | Ошибка учетных данных в `bind_password` | Проверьте пароль сервисной учетной записи (параметр `bind-password`) через `ldapsearch -H ldaps://... -D "..." -W`. |
| Блокировка базы данных при конкурентном доступе | Несколько инстансов/подов обращаются к файловой embedded H2 | Запускайте одну реплику. Для 2 и более реплик нужен PostgreSQL в `spring.datasource.url`, но JDBC-драйвер PostgreSQL в JAR не входит: в `common-security-starter` зависимость `org.postgresql:postgresql` объявлена как `optional` и в сервис не попадает, поэтому без добавления ее в `pom.xml` сервиса подключение к PostgreSQL падает с ошибкой загрузки драйвера. |
| `AWX Job launch failed (HTTP 401/403)` | Невалидный или истекший OAuth-токен AWX | Перевыпустите токен в веб-интерфейсе AWX и обновите `YARN_AWX_TOKEN` в секретах. |

---

## 6. Резервное копирование и восстановление

- Конфигурация: храните `values.yaml` и `application.yml` в Git-репозитории инфраструктуры (GitOps: ArgoCD, Flux).
- База данных PostgreSQL:
  ```bash
  # Создание резервной копии
  pg_dump -h pg-host -U yarn_user yarn_explorer > yarn_explorer_backup_$(date +%F).sql

  # Восстановление
  psql -h pg-host -U yarn_user yarn_explorer < yarn_explorer_backup_2026-10-08.sql
  ```
- Встроенная H2 (Standalone):
  ```bash
  # Создание копии файла базы данных
  cp /var/lib/hadoop-explorer/yarn/data/yarn_explorer.mv.db /backup/yarn_explorer_$(date +%F).mv.db
  ```

---

## 7. Сборка и запуск бэкенда из исходников

Бэкенд (`backend/yarn`) написан на Java 21 LTS и Spring Boot 3.3.4 и использует клиентские библиотеки Apache Hadoop YARN (`org.apache.hadoop:hadoop-yarn-client`).

### 7.1 Состав бэкенда
1. Клиент ResourceManager и RM HA failover: запросы идут в REST API активного ResourceManager; активный узел определяется по `haState == "ACTIVE"`, при его отказе сервис переключается на standby-узел.
2. Capacity Scheduler Engine: валидация веток (правило 100% емкости), расчет diff по долям ресурсов и node labels, формирование правок `capacity-scheduler.xml`; XML разбирается с запретом DOCTYPE (защита от XXE).
3. Change Requests и принцип четырех глаз: жизненный цикл согласования; попытка автора согласовать свою заявку получает HTTP 403 Forbidden.
4. Интеграция с Ansible AWX: запуск Job Template через REST API AWX; очереди обновляются командой `yarn rmadmin -refreshQueues` без перезапуска RM.
5. Общий модуль безопасности `common-security-starter`: аутентификация (Kerberos SPNEGO SSO, LDAP), двухуровневое хранилище сессий (L1 Caffeine + L2 JDBC), защита от CSRF, Bucket4j Rate Limiter, AOP-аудит `@Audited`.

### 7.2 Сборка и тестирование

```bash
# Модульное и интеграционное тестирование
make test-yarn
# или
mvn test -f backend/yarn/pom.xml

# Сборка исполняемого Spring Boot fat JAR
make build-yarn
# Результат: backend/yarn/target/yarn-explorer-java-1.0.0.jar

# Полная валидация всех Java-компонентов платформы
make test-java
```

### 7.3 Запуск с параметрами в командной строке

```bash
java -jar -Dspring.profiles.active=prod \
  -Dserver.port=8000 \
  -Dhadoop.security.auth.mode=kerberos \
  -Dhadoop.security.jwt.secret-key="production-super-secret-key-min-32-chars!" \
  -Dyarn.awx.base-url="https://awx.company.local" \
  -Dyarn.awx.token="secret-awx-token" \
  -Dyarn.awx.default-job-template-id=42 \
  backend/yarn/target/yarn-explorer-java-1.0.0.jar
```


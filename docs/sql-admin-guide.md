# 🛠️ Руководство администратора: SQL Explorer

Данный документ содержит практическое руководство для инженеров **DevOps / SRE / системных администраторов** по установке, конфигурированию, промышленному развертыванию и эксплуатации сервиса **SQL Explorer** в трех режимах:
1. **Standalone** (Bare-Metal / Виртуальные машины под управлением systemd)
2. **Docker & Docker Compose** (Контейнеризированный запуск)
3. **Kubernetes** (Промышленное развертывание через Helm-чарт и манифесты)

---

## 1. Архитектура и требования

### 1.1 Назначение сервиса
**SQL Explorer** — корпоративная среда выполнения SQL-запросов и исследования каталогов метаданных Data Lake:
- Выполнение распределенных аналитических запросов через **Trino DB API** (с отменой долгих запросов и стримингом результатов).
- Подключение к **Apache Hive** (через HiveServer2 и прямой опрос Hive Metastore).
- Каталог баз данных, схем и таблиц с интеллектуальным TTL-кэшированием метаданных.
- Персистентное рабочее пространство аналитика (**SqlUserWorkspace**): вкладки редактора, сохраненные запросы, история выполнений с метриками.
- Встроенный контекстный **ИИ-ассистент генерации и оптимизации SQL** (интеграция с локальными On-Premise LLM через Ollama/vLLM или внешние API).
- Защита от внедрения SQL-инъекций и подмены идентификаторов (`validate_identifier`).

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8000` (или `8003`) | HTTP/HTTPS | Inbound | Пользовательский веб-интерфейс SPA (Svelte 5 + Monaco Editor) и REST API |
| `8080` / `8443` | HTTP/HTTPS | Outbound | Trino Coordinator REST API |
| `9083` | TCP (Thrift) | Outbound | Apache Hive Metastore Thrift Service |
| `10000` | TCP (Thrift) | Outbound | Apache HiveServer2 (PyHive / JDBC) |
| `11434` / `8000` | HTTP | Outbound | Сервер локальной нейросети (Ollama / vLLM / OpenAI API) |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Корпоративный сервер аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- **Диагностика доступности (Healthcheck)**: `GET /healthz`
- **Метрики Prometheus**: `GET /metrics`

### 1.3 Системные требования
- **ОС**: Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12).
- **Среда выполнения**: Java `21 LTS` (Eclipse Temurin / OpenJDK), Maven `3.9+`, Node.js `20+` / `22 LTS`.
- **Ресурсы (на 1 реплику)**:
  - CPU: `1 ядро` (рек. `2 ядра`)
  - RAM: `1.5 ГБ` (рек. `2-4 ГБ` при активной буферизации больших выборок данных)
  - Диск: `10 ГБ` для локальной БД SQLite и кэша метаданных.

---

## 2. Режим 1: Standalone (Bare-Metal / VM / systemd)

### Шаг 1: Установка системных зависимостей

**Ubuntu / Debian**:
```bash
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    openjdk-21-jdk maven \
    krb5-user ldap-utils curl git
```

**RHEL / Rocky Linux**:
```bash
sudo dnf install -y java-21-openjdk-devel maven \
    krb5-workstation openldap-clients curl git
```

### Шаг 2: Создание пользователя и структуры каталогов
```bash
sudo groupadd -g 10001 appuser
sudo useradd -u 10001 -g appuser -m -s /bin/bash appuser

sudo mkdir -p /opt/hadoop-explorer/sql
sudo mkdir -p /etc/hadoop-explorer/sql
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/sql/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/sql
```

### Шаг 3: Размещение кода и сборка
```bash
sudo -u appuser -i
cd /opt/hadoop-explorer/sql
git clone https://github.com/company/hadoop-explorer.git .

# Сборка фронтенда SQL Explorer (Monaco Editor)
cd frontend
npm ci --workspace=apps/sql --include-workspace-root
npm run build --workspace=apps/sql
cd ..

# Сборка Java 21 бэкенда (Spring Boot Fat JAR)
mvn clean package -pl backend/common-security-starter,backend/sql -am -DskipTests
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/hadoop-explorer/sql/config.yaml`:
```yaml
server:
  host: "0.0.0.0"
  port: 8000
  debug: false
  secure_cookies: true
  cors_origins:
    - "https://sql.company.local"

security:
  secret_key: "CHANGE_TO_SUPER_SECURE_RANDOM_KEY_MIN_32_CHARS"
  algorithm: "HS256"
  access_token_expire_minutes: 480
  cookie_name: "sql_explorer_session"

database:
  url: "sqlite:////var/lib/hadoop-explorer/sql/data/sql_explorer.db"
  # Для HA кластера:
  # url: "postgresql://sql_user:DbPass123@pg.company.local:5432/sql_explorer"

auth:
  ldap:
    enabled: true
    server_uri: "ldaps://ldap.company.local:636"
    bind_dn: "cn=svc_sql,ou=services,dc=company,dc=local"
    bind_password: "ServiceLdapPassword"
    user_search_base: "ou=users,dc=company,dc=local"
    user_search_filter: "(sAMAccountName={username})"
    group_search_base: "ou=groups,dc=company,dc=local"
    group_search_filter: "(member={user_dn})"
    admin_group: "cn=hadoop-admins,ou=groups,dc=company,dc=local"

  kerberos:
    enabled: true
    keytab_path: "/etc/security/keytabs/sql-explorer.keytab"
    service_principal: "HTTP/sql.company.local@COMPANY.LOCAL"

# Настройка аналитических движков выполнения запросов
sql:
  engines:
    - id: "trino-prod"
      name: "Trino Production Cluster"
      type: "trino"
      host: "trino-coordinator.company.local"
      port: 8080
      http_scheme: "http"
      catalog: "hive"
      schema: "default"
      user: "sql_explorer"
      timeout_seconds: 300
      max_rows: 10000

    - id: "hive-prod"
      name: "Apache Hive Cluster"
      type: "hive"
      host: "hiveserver2.company.local"
      port: 10000
      metastore_uri: "thrift://hive-metastore.company.local:9083"
      auth_mechanism: "KERBEROS"
      kerberos_service_name: "hive"
      timeout_seconds: 600
      max_rows: 5000

# Интеграция с ИИ-ассистентом для генерации SQL
ai:
  enabled: true
  provider: "ollama" # ollama | openai | vllm
  base_url: "http://ollama.company.local:11434"
  model: "qwen2.5-coder:7b"
  timeout_seconds: 30
```

Установите права доступа:
```bash
sudo chmod 600 /etc/hadoop-explorer/sql/config.yaml
sudo chown appuser:appuser /etc/hadoop-explorer/sql/config.yaml
```

### Шаг 5: Создание systemd сервиса
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
WorkingDirectory=/opt/hadoop-explorer/sql
Environment="SPRING_CONFIG_ADDITIONAL_LOCATION=file:/etc/hadoop-explorer/sql/application.yml"
Environment="KRB5_CONFIG=/etc/krb5.conf"

ExecStart=/usr/bin/java -Xms512m -Xmx2048m \
    -jar /opt/hadoop-explorer/sql/backend/sql/target/sql-explorer.jar \
    --server.port=8003

Restart=always
RestartSec=5s
LimitNOFILE=65536
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### Шаг 6: Запуск и валидация
```bash
sudo systemctl daemon-reload
sudo systemctl enable sql-explorer
sudo systemctl start sql-explorer

sudo systemctl status sql-explorer
curl -I http://127.0.0.1:8000/healthz
# HTTP/1.1 200 OK
```

---

## 3. Режим 2: Docker & Docker Compose

### 3.1 Сборка Docker-образа
```bash
docker build -t hadoop-explorer/sql:latest -f docker/Dockerfile.sql .
```

### 3.2 Автономный запуск одного контейнера (`docker run`)
```bash
docker run -d \
  --name sql-explorer \
  --restart unless-stopped \
  -p 8003:8000 \
  -e CONFIG_PATH=/app/config/config.yaml \
  -e JWT_SECRET_KEY="SecureJwtTokenSecretMin32CharsLength" \
  -e KRB5_CONFIG=/etc/krb5.conf \
  -v /opt/sql-explorer/config.yaml:/app/config/config.yaml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v sql-data:/app/data \
  hadoop-explorer/sql:latest
```

### 3.3 Промышленный запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  sql-explorer:
    image: hadoop-explorer/sql:latest
    container_name: sql-explorer
    restart: unless-stopped
    ports:
      - "8003:8000"
    environment:
      - APP_NAME=sql
      - WEB_CONCURRENCY=2
      - CONFIG_PATH=/app/config/config.yaml
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_BIND_PASSWORD=${LDAP_BIND_PASSWORD}
      - KRB5_KEYTAB=/etc/security/keytabs/sql-explorer.keytab
      - KRB5_PRINCIPAL=HTTP/sql.company.local@COMPANY.LOCAL
    volumes:
      - ./config.yaml:/app/config/config.yaml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs/sql-explorer.keytab:/etc/security/keytabs/sql-explorer.keytab:ro
      - sql-storage:/app/data
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

## 4. Режим 3: Kubernetes (Helm & Raw Manifests)

### 4.1 Развертывание через официальный Helm-чарт
В репозитории подготовлен чарт [`helm/charts/sql-explorer`](file:///Users/mvmalykh/IdeaProjects/hadoop-explorer/helm/charts/sql-explorer).

#### Шаг 1: Подготовка секретов Kubernetes
```bash
kubectl create namespace hadoop-explorer

kubectl create secret generic sql-explorer-secrets \
  --namespace hadoop-explorer \
  --from-literal=jwt-secret-key="Min32CharSecretKeyForSqlExplorerJwt" \
  --from-literal=ldap-bind-password="SecretLdapPassword"

kubectl create secret generic sql-kerberos-keytab \
  --namespace hadoop-explorer \
  --from-file=sql-explorer.keytab=/etc/security/keytabs/sql-explorer.keytab

kubectl create configmap krb5-config \
  --namespace hadoop-explorer \
  --from-file=krb5.conf=/etc/krb5.conf
```

#### Шаг 2: Параметры `custom-values.yaml`
```yaml
replicaCount: 2

image:
  repository: registry.company.local/hadoop-explorer/sql
  tag: "1.0.0"
  pullPolicy: IfNotPresent

extraEnv:
  - name: JWT_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: sql-explorer-secrets
        key: jwt-secret-key
  - name: LDAP_BIND_PASSWORD
    valueFrom:
      secretKeyRef:
        name: sql-explorer-secrets
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
    nginx.ingress.kubernetes.io/proxy-read-timeout: "600" # Длительные запросы Trino
    nginx.ingress.kubernetes.io/proxy-send-timeout: "600"
  hosts:
    - host: sql.company.local
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: sql-tls-cert
      hosts:
        - sql.company.local

resources:
  limits:
    cpu: "2"
    memory: "3Gi"
  requests:
    cpu: "500m"
    memory: "1Gi"

extraVolumes:
  - name: keytab-vol
    secret:
      secretName: sql-kerberos-keytab
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
    url: "postgresql://sql_user:DbPass123@postgres-ha.database.svc.cluster.local:5432/sql_explorer"

podDisruptionBudget:
  minAvailable: 1
```

#### Шаг 3: Установка через Helm
```bash
helm upgrade --install sql-explorer ./helm/charts/sql-explorer \
  --namespace hadoop-explorer \
  -f custom-values.yaml

kubectl get pods -n hadoop-explorer -l app.kubernetes.io/name=sql-explorer
```

---

## 5. Мониторинг и Troubleshooting

### 5.1 Метрики Prometheus (`GET /metrics`)
- `http_requests_total{app="sql", ...}`
- `sql_queries_total{engine="trino|hive", status="success|failed|cancelled"}`
- `sql_query_duration_seconds{engine="trino|hive", ...}`
- `hadoop_circuit_breaker_state{name="trino_coordinator"}`

### 5.2 Решение инцидентов (FAQ)

| Ошибка | Причина | Решение |
|---|---|---|
| `TrinoQueryError: Query exceeded maximum execution time` | Слишком тяжелый запрос превысил тайм-аут | Увеличьте `timeout_seconds` в `config.yaml` или используйте партиционирование в предложении `WHERE`. |
| `Cannot connect to Hive Metastore thrift://...:9083` | Служба Hive Metastore недоступна или заблокирован сетевой порт | Проверьте статус службы `systemctl status hive-metastore` и сетевой доступ `nc -zv hive-metastore 9083`. |
| `AI Service Unavailable` | Локальный сервер Ollama/vLLM не отвечает | Проверьте доступность контейнера Ollama: `curl http://ollama:11434/api/tags`. |

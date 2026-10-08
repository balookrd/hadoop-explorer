# 🛠️ Руководство администратора: HDFS Explorer

Данный документ содержит практическое руководство для инженеров **DevOps / SRE / системных администраторов** по установке, конфигурированию, промышленному развертыванию и эксплуатации сервиса **HDFS Explorer** в трех режимах:
1. **Standalone** (Bare-Metal / Виртуальные машины под управлением systemd)
2. **Docker & Docker Compose** (Контейнеризированный запуск)
3. **Kubernetes** (Промышленное развертывание через Helm-чарт и манифесты)

---

## 1. Архитектура и требования

### 1.1 Назначение сервиса
**HDFS Explorer** — высокопроизводительный веб-менеджер файловой системы Apache Hadoop (HDFS), обеспечивающий:
- Навигацию по каталогам, загрузку, выгрузку, удаление и перемещение файлов через **WebHDFS / HttpFS REST API**.
- Автоматический **Failover при NameNode High Availability (HA)** с перехватом `StandbyException`.
- Корпоративную сквозную аутентификацию **Kerberos SPNEGO SSO** с поддержкой Impersonation (`doAs`).
- Интерактивный предпросмотр структурированных форматов данных (**Parquet, ORC, Avro, CSV, TSV, JSON**).
- Управление HDFS квотами (`Name Quota`, `Space Quota`, `Storage Type Quota`).
- Межкластерное копирование (**Cross-Cluster Copy**).
- Управление правами доступа **POSIX Permissions** и расширенными **HDFS ACL**.

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8000` (или `8002`) | HTTP/HTTPS | Inbound | Пользовательский веб-интерфейс SPA (Svelte 5) и REST API |
| `9870` / `50070` | HTTP/HTTPS | Outbound | WebHDFS REST API NameNode (Active/Standby) |
| `9864` / `50075` | HTTP/HTTPS | Outbound | DataNode WebHDFS streaming (при операциях чтения/записи) |
| `14000` | HTTP/HTTPS | Outbound | HttpFS Gateway (альтернатива WebHDFS) |
| `9083` | TCP (Thrift) | Outbound | Hive Metastore (опционально, для схемы таблиц) |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Сервер каталогов аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- **Диагностика доступности (Healthcheck)**: `GET /healthz`
- **Метрики Prometheus**: `GET /metrics`

### 1.3 Системные требования
- **ОС**: Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12).
- **Среда выполнения**: Python `3.11` – `3.14`, Node.js `20+` / `22 LTS` (для локальной сборки).
- **Системные библиотеки**: `libkrb5-dev`, `libsasl2-dev`, `krb5-user`, `curl`.
- **Ресурсы (на 1 реплику)**:
  - CPU: `1 ядро` (рек. `2 ядра` при частом чтении Parquet/ORC)
  - RAM: `1 ГБ` (рек. `2 ГБ`)
  - Диск: `10 ГБ` для кэша и локальной БД SQLite.

---

## 2. Режим 1: Standalone (Bare-Metal / VM / systemd)

### Шаг 1: Установка системных зависимостей

**Ubuntu / Debian**:
```bash
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    build-essential python3 python3-venv python3-dev \
    libkrb5-dev libsasl2-dev krb5-user ldap-utils curl git
```

**RHEL / Rocky Linux**:
```bash
sudo dnf install -y gcc python3 python3-devel \
    krb5-devel cyrus-sasl-devel krb5-workstation openldap-clients curl git
```

### Шаг 2: Создание пользователя и структуры каталогов
```bash
sudo groupadd -g 10001 appuser
sudo useradd -u 10001 -g appuser -m -s /bin/bash appuser

sudo mkdir -p /opt/hadoop-explorer/hdfs
sudo mkdir -p /etc/hadoop-explorer/hdfs
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/hdfs/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/hdfs
```

### Шаг 3: Размещение кода и виртуального окружения
```bash
sudo -u appuser -i
cd /opt/hadoop-explorer/hdfs
git clone https://github.com/company/hadoop-explorer.git .

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e backend/common -e backend/hdfs

# Сборка фронтенда HDFS
cd frontend
npm ci --workspace=apps/hdfs --include-workspace-root
npm run build --workspace=apps/hdfs
cd ..
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/hadoop-explorer/hdfs/config.yaml`:
```yaml
server:
  host: "0.0.0.0"
  port: 8000
  debug: false
  secure_cookies: true
  cors_origins:
    - "https://hdfs.company.local"

security:
  secret_key: "CHANGE_TO_SUPER_SECURE_RANDOM_KEY_MIN_32_CHARS"
  algorithm: "HS256"
  access_token_expire_minutes: 480
  cookie_name: "hdfs_explorer_session"

database:
  url: "sqlite:////var/lib/hadoop-explorer/hdfs/data/hdfs_explorer.db"
  # Для HA кластера:
  # url: "postgresql://hdfs_user:DbPass123@pg.company.local:5432/hdfs_explorer"

auth:
  ldap:
    enabled: true
    server_uri: "ldaps://ldap.company.local:636"
    bind_dn: "cn=svc_hdfs,ou=services,dc=company,dc=local"
    bind_password: "ServiceLdapPassword"
    user_search_base: "ou=users,dc=company,dc=local"
    user_search_filter: "(sAMAccountName={username})"
    group_search_base: "ou=groups,dc=company,dc=local"
    group_search_filter: "(member={user_dn})"
    admin_group: "cn=hadoop-admins,ou=groups,dc=company,dc=local"

  kerberos:
    enabled: true
    keytab_path: "/etc/security/keytabs/hdfs-explorer.keytab"
    service_principal: "HTTP/hdfs.company.local@COMPANY.LOCAL"

# Настройка подключения к кластерам HDFS
hdfs:
  clusters:
    - id: "prod-datalake"
      name: "Production DataLake"
      active_namenode_url: "http://nn01.prod.company.local:9870"
      standby_namenode_url: "http://nn02.prod.company.local:9870"
      auth_type: "kerberos" # kerberos | simple
      keytab_path: "/etc/security/keytabs/hdfs-client.keytab"
      principal: "hdfs-client@COMPANY.LOCAL"
      impersonation_enabled: true # doAs={username}
      timeout_seconds: 10
      preview:
        max_rows: 100
        max_file_size_mb: 200

    - id: "archive-datalake"
      name: "Cold Archive DataLake"
      active_namenode_url: "http://nn01.archive.company.local:9870"
      standby_namenode_url: "http://nn02.archive.company.local:9870"
      auth_type: "kerberos"
      keytab_path: "/etc/security/keytabs/hdfs-client.keytab"
      principal: "hdfs-client@COMPANY.LOCAL"
      impersonation_enabled: true
```

Установите права доступа:
```bash
sudo chmod 600 /etc/hadoop-explorer/hdfs/config.yaml
sudo chown appuser:appuser /etc/hadoop-explorer/hdfs/config.yaml
```

### Шаг 5: Создание systemd сервиса
Создайте файл `/etc/systemd/system/hdfs-explorer.service`:
```ini
[Unit]
Description=Hadoop HDFS Explorer Web Service
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/hadoop-explorer/hdfs
Environment="PYTHONPATH=/opt/hadoop-explorer/hdfs:/opt/hadoop-explorer/hdfs/backend/common:/opt/hadoop-explorer/hdfs/backend/hdfs"
Environment="CONFIG_PATH=/etc/hadoop-explorer/hdfs/config.yaml"
Environment="FRONTEND_DIST=/opt/hadoop-explorer/hdfs/frontend/apps/hdfs/dist"
Environment="KRB5_CONFIG=/etc/krb5.conf"
Environment="WEB_CONCURRENCY=2"

ExecStart=/opt/hadoop-explorer/hdfs/.venv/bin/uvicorn app.main:app \
    --app-dir backend/hdfs \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 2 \
    --log-level info

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
sudo systemctl enable hdfs-explorer
sudo systemctl start hdfs-explorer

# Проверка статуса
sudo systemctl status hdfs-explorer
curl -I http://127.0.0.1:8000/healthz
# HTTP/1.1 200 OK
```

---

## 3. Режим 2: Docker & Docker Compose

### 3.1 Сборка Docker-образа
```bash
docker build -t hadoop-explorer/hdfs:latest -f docker/Dockerfile.hdfs .
```

### 3.2 Автономный запуск одного контейнера (`docker run`)
```bash
docker run -d \
  --name hdfs-explorer \
  --restart unless-stopped \
  -p 8002:8000 \
  -e CONFIG_PATH=/app/config/config.yaml \
  -e JWT_SECRET_KEY="SecureJwtTokenSecretMin32CharsLength" \
  -e KRB5_CONFIG=/etc/krb5.conf \
  -v /opt/hdfs-explorer/config.yaml:/app/config/config.yaml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v hdfs-data:/app/data \
  hadoop-explorer/hdfs:latest
```

### 3.3 Промышленный запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  hdfs-explorer:
    image: hadoop-explorer/hdfs:latest
    container_name: hdfs-explorer
    restart: unless-stopped
    ports:
      - "8002:8000"
    environment:
      - APP_NAME=hdfs
      - WEB_CONCURRENCY=2
      - CONFIG_PATH=/app/config/config.yaml
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_BIND_PASSWORD=${LDAP_BIND_PASSWORD}
      - KRB5_KEYTAB=/etc/security/keytabs/hdfs-explorer.keytab
      - KRB5_PRINCIPAL=HTTP/hdfs.company.local@COMPANY.LOCAL
    volumes:
      - ./config.yaml:/app/config/config.yaml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs/hdfs-explorer.keytab:/etc/security/keytabs/hdfs-explorer.keytab:ro
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

## 4. Режим 3: Kubernetes (Helm & Raw Manifests)

### 4.1 Развертывание через официальный Helm-чарт
В репозитории подготовлен чарт [`helm/charts/hdfs-explorer`](file:///Users/mvmalykh/IdeaProjects/hadoop-explorer/helm/charts/hdfs-explorer).

#### Шаг 1: Подготовка секретов Kubernetes
```bash
kubectl create namespace hadoop-explorer

kubectl create secret generic hdfs-explorer-secrets \
  --namespace hadoop-explorer \
  --from-literal=jwt-secret-key="Min32CharSecretKeyForHdfsExplorerJwt" \
  --from-literal=ldap-bind-password="SecretLdapPassword"

kubectl create secret generic hdfs-kerberos-keytab \
  --namespace hadoop-explorer \
  --from-file=hdfs-explorer.keytab=/etc/security/keytabs/hdfs-explorer.keytab

kubectl create configmap krb5-config \
  --namespace hadoop-explorer \
  --from-file=krb5.conf=/etc/krb5.conf
```

#### Шаг 2: Параметры `custom-values.yaml`
```yaml
replicaCount: 2

image:
  repository: registry.company.local/hadoop-explorer/hdfs
  tag: "1.0.0"
  pullPolicy: IfNotPresent

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
    url: "postgresql://hdfs_user:DbPass123@postgres-ha.database.svc.cluster.local:5432/hdfs_explorer"

  hdfs:
    clusters:
      - id: "prod-hdfs"
        name: "Production Cluster"
        active_namenode_url: "http://nn01.hadoop.company.local:9870"
        standby_namenode_url: "http://nn02.hadoop.company.local:9870"
        auth_type: "kerberos"
        principal: "hdfs/hdfs.company.local@COMPANY.LOCAL"
        impersonation_enabled: true

podDisruptionBudget:
  minAvailable: 1
```

#### Шаг 3: Установка через Helm
```bash
helm upgrade --install hdfs-explorer ./helm/charts/hdfs-explorer \
  --namespace hadoop-explorer \
  -f custom-values.yaml

kubectl get pods -n hadoop-explorer -l app.kubernetes.io/name=hdfs-explorer
```

---

## 5. Мониторинг и Troubleshooting

### 5.1 Метрики Prometheus (`GET /metrics`)
- `http_requests_total{app="hdfs", ...}`
- `hadoop_circuit_breaker_state{name="hdfs_namenode"}`
- `hadoop_circuit_breaker_calls_total{name="hdfs_namenode", status="success|failed"}`
- `hdfs_operations_total{op="list|read|write|delete|quota"}`

### 5.2 Решение инцидентов (FAQ)

| Ошибка | Причина | Решение |
|---|---|---|
| `StandbyException: Operation category READ is not supported in state standby` | NameNode переключилась в Standby | HDFS Explorer автоматически опрашивает второй URL из `standby_namenode_url`. Убедитесь, что оба URL корректно заданы в `config.yaml`. |
| `SecurityException: User appuser is not allowed to impersonate alice` | Не настроен `hadoop.proxyuser` в `core-site.xml` кластера | Добавьте в `core-site.xml` NameNode: `hadoop.proxyuser.<principal>.hosts=*` и `hadoop.proxyuser.<principal>.groups=*`. |
| `ConnectError: Failed to connect to DataNode port 9864` | Поды или сервер не имеют прямого сетевого доступа к DataNode | При использовании WebHDFS клиент выполняет redirect на DataNode. Проверьте сетевую связность до DataNode или используйте HttpFS. |
| `Payload Too Large (HTTP 413)` | Ограничение Ingress на размер загрузки | Увеличьте аннотацию `nginx.ingress.kubernetes.io/proxy-body-size: "1024m"`. |

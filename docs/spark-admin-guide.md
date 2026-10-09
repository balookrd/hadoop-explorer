# 🛠️ Руководство администратора: Spark Explorer

Данный документ содержит практическое руководство для инженеров **DevOps / SRE / системных администраторов** по установке, конфигурированию, промышленному развертыванию и эксплуатации сервиса **Spark Explorer** в трех режимах:
1. **Standalone** (Bare-Metal / Виртуальные машины под управлением systemd)
2. **Docker & Docker Compose** (Контейнеризированный запуск)
3. **Kubernetes** (Промышленное развертывание через Helm-чарт и манифесты)

---

## 1. Архитектура и требования

### 1.1 Назначение сервиса
**Spark Explorer** — интерактивная веб-среда запуска и управления вычислениями Apache Spark в Data Lake:
- Управление жизненным циклом сессий **Apache Livy REST API** (создание, мониторинг состояния, отмена statement, логирование вывода).
- Поддержка языков **PySpark** и **Spark Scala**.
- Автоматическая остановка простаивающих сессий (`idle_timeout`) и остановка при завершении сеанса пользователя (`auto_stop_on_logout`).
- Профилирование и выделение ресурсов в YARN очередях (`driver_memory`, `executor_memory`, `executor_cores`, `num_executors`).
- Интеграция с **Spark History Server** для аудита завершенных вычислений.
- Персистентное рабочее пространство пользователя (**SparkUserWorkspace**): интерактивные блокноты, сохраненные пайплайны и история выполнения.

### 1.2 Сетевые порты и эндпоинты
| Порт | Протокол | Направление | Назначение |
|---|---|---|---|
| `8000` (или `8004`) | HTTP/HTTPS | Inbound | Пользовательский веб-интерфейс SPA (Svelte 5 + Monaco Editor) и REST API |
| `8998` | HTTP/HTTPS | Outbound | Apache Livy Server REST API |
| `18080` | HTTP/HTTPS | Outbound | Spark History Server Web UI |
| `8088` | HTTP/HTTPS | Outbound | YARN ResourceManager REST API |
| `9083` | TCP (Thrift) | Outbound | Apache Hive Metastore (метаданные Spark SQL / Iceberg) |
| `389` / `636` | TCP (LDAP/LDAPS) | Outbound | Корпоративный сервер аутентификации |
| `88` | TCP/UDP | Outbound | Kerberos Key Distribution Center (KDC) |

- **Диагностика доступности (Healthcheck)**: `GET /healthz`
- **Метрики Prometheus**: `GET /metrics`

### 1.3 Системные требования
- **ОС**: Linux (RHEL 8/9, Rocky Linux 8/9, Ubuntu 22.04/24.04 LTS, Debian 12).
- **Среда выполнения**: Java `21 LTS` (Eclipse Temurin / OpenJDK), Maven `3.9+`, Node.js `20+` / `22 LTS`.
- **Ресурсы (на 1 реплику)**:
  - CPU: `1 ядро` (рек. `2 ядра`)
  - RAM: `1.5 ГБ` (рек. `2 ГБ`)
  - Диск: `10 ГБ` для SQLite и кэша сессий.

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

sudo mkdir -p /opt/hadoop-explorer/spark
sudo mkdir -p /etc/hadoop-explorer/spark
sudo mkdir -p /var/log/hadoop-explorer
sudo mkdir -p /var/lib/hadoop-explorer/spark/data
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/spark
```

### Шаг 3: Размещение кода и сборка
```bash
sudo -u appuser -i
cd /opt/hadoop-explorer/spark
git clone https://github.com/company/hadoop-explorer.git .

# Сборка фронтенда Spark Explorer
cd frontend
npm ci --workspace=apps/spark --include-workspace-root
npm run build --workspace=apps/spark
cd ..

# Сборка Java 21 бэкенда (Spring Boot Fat JAR)
mvn clean package -pl backend/common-security-starter,backend/spark -am -DskipTests
```

### Шаг 4: Настройка конфигурационного файла
Создайте файл `/etc/hadoop-explorer/spark/config.yaml`:
```yaml
server:
  host: "0.0.0.0"
  port: 8000
  debug: false
  secure_cookies: true
  cors_origins:
    - "https://spark.company.local"

security:
  secret_key: "CHANGE_TO_SUPER_SECURE_RANDOM_KEY_MIN_32_CHARS"
  algorithm: "HS256"
  access_token_expire_minutes: 480
  cookie_name: "spark_explorer_session"

database:
  url: "sqlite:////var/lib/hadoop-explorer/spark/data/spark_explorer.db"
  # Для HA кластера:
  # url: "postgresql://spark_user:DbPass123@pg.company.local:5432/spark_explorer"

auth:
  ldap:
    enabled: true
    server_uri: "ldaps://ldap.company.local:636"
    bind_dn: "cn=svc_spark,ou=services,dc=company,dc=local"
    bind_password: "ServiceLdapPassword"
    user_search_base: "ou=users,dc=company,dc=local"
    user_search_filter: "(sAMAccountName={username})"
    group_search_base: "ou=groups,dc=company,dc=local"
    group_search_filter: "(member={user_dn})"
    admin_group: "cn=hadoop-admins,ou=groups,dc=company,dc=local"

  kerberos:
    enabled: true
    keytab_path: "/etc/security/keytabs/spark-explorer.keytab"
    service_principal: "HTTP/spark.company.local@COMPANY.LOCAL"

# Настройка подключения к кластерам Apache Livy и Spark
spark:
  clusters:
    - id: "spark-prod"
      name: "Production Spark on YARN"
      livy_url: "http://livy-server.company.local:8998"
      history_server_url: "http://history-server.company.local:18080"
      yarn_rm_url: "http://rm01.company.local:8088"
      default_queue: "analytics"
      timeout_seconds: 60
      session_idle_timeout_minutes: 60
      auto_stop_on_logout: true

      # Пресеты выделения ресурсов для пользователей
      resource_profiles:
        small:
          driver_memory: "2g"
          executor_memory: "4g"
          executor_cores: 2
          num_executors: 2
        medium:
          driver_memory: "4g"
          executor_memory: "8g"
          executor_cores: 4
          num_executors: 5
        large:
          driver_memory: "8g"
          executor_memory: "16g"
          executor_cores: 4
          num_executors: 10
```

Установите права доступа:
```bash
sudo chmod 600 /etc/hadoop-explorer/spark/config.yaml
sudo chown appuser:appuser /etc/hadoop-explorer/spark/config.yaml
```

### Шаг 5: Создание systemd сервиса
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
    -jar /opt/hadoop-explorer/spark/backend/spark/target/spark-explorer.jar \
    --server.port=8004

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
sudo systemctl enable spark-explorer
sudo systemctl start spark-explorer

sudo systemctl status spark-explorer
curl -I http://127.0.0.1:8000/healthz
# HTTP/1.1 200 OK
```

---

## 3. Режим 2: Docker & Docker Compose

### 3.1 Сборка Docker-образа
```bash
docker build -t hadoop-explorer/spark:latest -f docker/Dockerfile.spark .
```

### 3.2 Автономный запуск одного контейнера (`docker run`)
```bash
docker run -d \
  --name spark-explorer \
  --restart unless-stopped \
  -p 8004:8000 \
  -e CONFIG_PATH=/app/config/config.yaml \
  -e JWT_SECRET_KEY="SecureJwtTokenSecretMin32CharsLength" \
  -e KRB5_CONFIG=/etc/krb5.conf \
  -v /opt/spark-explorer/config.yaml:/app/config/config.yaml:ro \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/security/keytabs:/etc/security/keytabs:ro \
  -v spark-data:/app/data \
  hadoop-explorer/spark:latest
```

### 3.3 Промышленный запуск через `docker-compose.yml`
```yaml
version: "3.8"

services:
  spark-explorer:
    image: hadoop-explorer/spark:latest
    container_name: spark-explorer
    restart: unless-stopped
    ports:
      - "8004:8000"
    environment:
      - APP_NAME=spark
      - WEB_CONCURRENCY=2
      - CONFIG_PATH=/app/config/config.yaml
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
      - LDAP_BIND_PASSWORD=${LDAP_BIND_PASSWORD}
      - KRB5_KEYTAB=/etc/security/keytabs/spark-explorer.keytab
      - KRB5_PRINCIPAL=HTTP/spark.company.local@COMPANY.LOCAL
    volumes:
      - ./config.yaml:/app/config/config.yaml:ro
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs/spark-explorer.keytab:/etc/security/keytabs/spark-explorer.keytab:ro
      - spark-storage:/app/data
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
  spark-storage:
    driver: local

networks:
  hadoop-net:
    driver: bridge
```

---

## 4. Режим 3: Kubernetes (Helm & Raw Manifests)

### 4.1 Развертывание через официальный Helm-чарт
В репозитории подготовлен чарт [`helm/charts/spark-explorer`](../helm/charts/spark-explorer).

#### Шаг 1: Подготовка секретов Kubernetes
```bash
kubectl create namespace hadoop-explorer

kubectl create secret generic spark-explorer-secrets \
  --namespace hadoop-explorer \
  --from-literal=jwt-secret-key="Min32CharSecretKeyForSparkExplorerJwt" \
  --from-literal=ldap-bind-password="SecretLdapPassword"

kubectl create secret generic spark-kerberos-keytab \
  --namespace hadoop-explorer \
  --from-file=spark-explorer.keytab=/etc/security/keytabs/spark-explorer.keytab

kubectl create configmap krb5-config \
  --namespace hadoop-explorer \
  --from-file=krb5.conf=/etc/krb5.conf
```

#### Шаг 2: Параметры `custom-values.yaml`
```yaml
replicaCount: 2

image:
  repository: registry.company.local/hadoop-explorer/spark
  tag: "1.0.0"
  pullPolicy: IfNotPresent

extraEnv:
  - name: JWT_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: spark-explorer-secrets
        key: jwt-secret-key
  - name: LDAP_BIND_PASSWORD
    valueFrom:
      secretKeyRef:
        name: spark-explorer-secrets
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
    nginx.ingress.kubernetes.io/proxy-read-timeout: "300"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "300"
  hosts:
    - host: spark.company.local
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: spark-tls-cert
      hosts:
        - spark.company.local

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
      secretName: spark-kerberos-keytab
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
    url: "postgresql://spark_user:DbPass123@postgres-ha.database.svc.cluster.local:5432/spark_explorer"

podDisruptionBudget:
  minAvailable: 1
```

#### Шаг 3: Установка через Helm
```bash
helm upgrade --install spark-explorer ./helm/charts/spark-explorer \
  --namespace hadoop-explorer \
  -f custom-values.yaml

kubectl get pods -n hadoop-explorer -l app.kubernetes.io/name=spark-explorer
```

---

## 5. Мониторинг и Troubleshooting

### 5.1 Метрики Prometheus (`GET /metrics`)
- `http_requests_total{app="spark", ...}`
- `spark_sessions_total{status="starting|idle|busy|dead|killed"}`
- `spark_statements_total{status="waiting|running|available|error|cancelled"}`
- `hadoop_circuit_breaker_state{name="livy_server"}`

### 5.2 Решение инцидентов (FAQ)

| Ошибка | Причина | Решение |
|---|---|---|
| `LivySessionCreationError: YARN queue limit exceeded` | В целевой очереди YARN нет доступной памяти | Выберите профиль с меньшим потреблением ресурсов (`small`) либо смените очередь YARN. |
| `Cannot connect to Livy server http://...:8998` | Служба Apache Livy не запущена | Проверьте статус службы Livy: `systemctl status livy-server` или `curl http://livy:8998/sessions`. |
| `Session Dead: Driver exit with code 1` | Ошибка инициализации драйвера (память, несовместимость версий jar) | Перейдите в Spark History Server или проверьте логи контейнера драйвера в YARN: `yarn logs -applicationId <app_id>`. |

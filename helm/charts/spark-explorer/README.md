# Helm Chart: Spark Web Explorer

Helm-чарт для установки Spark Explorer (PySpark, Scala Spark, Spark SQL) в Kubernetes.

## Что входит в чарт

- Подключение к Apache Livy для интерактивных вычислений на YARN или Kubernetes.
- Аутентификация и доступ:
  - Kerberos SPNEGO SSO и аутентификация через LDAPS.
  - Получение сервисного билета Kerberos (`kinit`) из keytab, смонтированного из Secret с правами `0400`.
  - Передача имени пользователя в Livy (`proxyUser`).
  - Правила RBAC/ACL для очередей YARN и UI.
- Хранение сессий и рабочих пространств: внешний PostgreSQL или встроенный SQLite на PersistentVolumeClaim.

## Установка

Команды выполняются из корня репозитория.

### 1. Установка со значениями по умолчанию

```bash
# Установка с дефолтными значениями
helm install my-spark-explorer helm/charts/spark-explorer -n analytics --create-namespace
```

### 2. Установка с Kerberos keytab и внешним PostgreSQL

Создайте файл `custom-values.yaml`:

```yaml
image:
  repository: registry.company.local/analytics/spark-explorer
  tag: "0.1.0"

ingress:
  enabled: true
  hosts:
    - host: spark-explorer.company.local
      paths:
        - path: /
          pathType: Prefix

secrets:
  jwtSecret: "SuperSecretLongRandomKeyForProductionJWTToken12345"
  ldapBindPassword: "SecretServiceAccountPassword"
  databasePassword: "SecretPostgresPassword123"
  # base64 -w 0 /path/to/spark-explorer.keytab
  kerberosKeytabBase64: "BQIAAAA4AA..."

config:
  server:
    cors_origins:
      - "https://spark-explorer.company.local"
    secure_cookies: true
  database:
    url: "postgresql+asyncpg://spark_user:spark_pass@postgres.analytics.svc:5432/spark_explorer"
  auth:
    mode: "hybrid"
    ldap:
      server_uri: "ldaps://dc.company.local:636"
      bind_dn: "cn=svc_spark_explorer,ou=services,dc=company,dc=local"
      user_base_dn: "ou=users,dc=company,dc=local"
      group_base_dn: "ou=groups,dc=company,dc=local"
      allow_insecure_ssl: false
    kerberos:
      service_principal: "HTTP/spark-explorer.company.local@COMPANY.LOCAL"

kerberos:
  enabled: true
  krb5Conf: |
    [libdefaults]
        default_realm = COMPANY.LOCAL
        dns_lookup_realm = false
        dns_lookup_kdc = true
        ticket_lifetime = 24h
        renew_lifetime = 7d
        forwardable = true

    [realms]
        COMPANY.LOCAL = {
            kdc = kdc.company.local:88
            admin_server = kdc.company.local:749
        }

    [domain_realm]
        .company.local = COMPANY.LOCAL
        company.local = COMPANY.LOCAL
```

Установите чарт с этим файлом:

```bash
helm upgrade --install spark-explorer helm/charts/spark-explorer \
  -n analytics \
  -f custom-values.yaml
```

## Проверка статуса

```bash
kubectl get pods -n analytics -l app.kubernetes.io/name=spark-explorer
kubectl logs -n analytics -l app.kubernetes.io/name=spark-explorer -c spark-explorer
```

---

## Настройка безопасности для production

### 1. Ingress: ограничение частоты запросов и заголовки
Чтобы ограничить подбор паролей Active Directory через `POST /api/v1/auth/login` и число запросов от одного клиента, задайте аннотации Ingress Controller.

```yaml
ingress:
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    # Ограничение частоты запросов для защиты от Brute-Force
    nginx.ingress.kubernetes.io/limit-rps: "5"
    nginx.ingress.kubernetes.io/limit-connections: "10"
    # Защитные HTTP-заголовки
    nginx.ingress.kubernetes.io/configuration-snippet: |
      more_set_headers "X-Frame-Options: DENY";
      more_set_headers "X-Content-Type-Options: nosniff";
      more_set_headers "Strict-Transport-Security: max-age=31536000; includeSubDomains";
```

### 2. Запуск от непривилегированного пользователя
Контейнер запускается от пользователя `appuser` (UID 10001, GID 10001). Значения `podSecurityContext` и `securityContext` в `values.yaml` соответствуют профилю Pod Security Standards `Restricted`:
```yaml
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  fsGroup: 10001

securityContext:
  allowPrivilegeEscalation: false
  capabilities:
    drop:
      - ALL
```

### 3. Несколько реплик и отзыв токенов
При `replicaCount` 2 и больше нужен внешний PostgreSQL. JDBC-драйвер PostgreSQL в JAR не входит: в `common-security-starter` зависимость `org.postgresql:postgresql` объявлена как `optional` и в сервис не попадает, поэтому без добавления ее в `pom.xml` сервиса подключение к PostgreSQL падает с ошибкой загрузки драйвера (`backend/spark/pom.xml`). Кроме того, `config.database.url` чарта сервис не читает: он берет `spring.datasource.url` из Spring-конфигурации. Реестр отозванных JWT-токенов (`revoked_tokens`) хранится в общей базе данных с двумя уровнями кэша: L1 в памяти и L2 в PostgreSQL. После выхода пользователя (`POST /api/v1/auth/logout`) токен отклоняется всеми репликами.

### 4. Сбор событий аудита (SIEM/SOC)
Приложение пишет события аудита через логгер `security.audit` в формате JSON в `stdout` пода:
```json
{"timestamp": "2026-09-06T12:00:00Z", "event_type": "AUTH_LOGIN_SUCCESS", "username": "analyst_user", "client_ip": "10.244.0.1", "status": "SUCCESS", "details": {"auth_method": "ldap", "groups": ["bi-analysts"]}}
```
Для отправки в централизованное хранилище (OpenSearch / ELK / Splunk) используйте DaemonSet с FluentBit, Promtail или Vector, настроенный на фильтрацию логов с логгером `security.audit`.


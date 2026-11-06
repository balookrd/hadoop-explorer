# Helm-чарт SQL Web Explorer

Helm-чарт для установки SQL Web Explorer (веб-интерфейс запросов к Trino и Hive) в Kubernetes.

## Состав чарта

- Подключение к Trino и Hive (HiveServer2).
- Безопасность:
  - аутентификация через Kerberos SPNEGO SSO и LDAPS;
  - получение сервисного билета Kerberos (`kinit`) из keytab, смонтированного из Secret с правами `0400`;
  - запросы выполняются от имени пользователя (`X-Trino-User` для Trino, `doAs` для Hive);
  - правила доступа RBAC / ACL на уровне кластеров и UI.
- Стриминг результатов: в аннотациях Ingress по умолчанию отключена буферизация Nginx (`proxy-buffering: "off"`) и увеличены таймауты (`proxy-read-timeout` и `proxy-send-timeout` — `1800`), чтобы Server-Sent Events доходили до браузера без задержек и обрывов.
- Хранение состояния: внешний PostgreSQL или встроенный SQLite на PersistentVolumeClaim.

## Установка

### 1. Установка со значениями по умолчанию

Команды выполняются из корня репозитория.

```bash
# Установка с дефолтными значениями
helm install my-sql-explorer helm/charts/sql-explorer -n analytics --create-namespace
```

### 2. Установка с Kerberos Keytab и внешним PostgreSQL

Создайте файл `custom-values.yaml`:

```yaml
image:
  repository: registry.company.local/analytics/sql-explorer
  tag: "0.1.0"

ingress:
  enabled: true
  hosts:
    - host: sql-explorer.company.local
      paths:
        - path: /
          pathType: Prefix

secrets:
  jwtSecret: "SuperSecretLongRandomKeyForProductionJWTToken12345"
  ldapBindPassword: "SecretServiceAccountPassword"
  databasePassword: "SecretPostgresPassword123"
  # base64 -w 0 /path/to/sql-explorer.keytab
  kerberosKeytabBase64: "BQIAAAA4AA..."

config:
  server:
    cors_origins:
      - "https://sql-explorer.company.local"
    secure_cookies: true
  database:
    url: "postgresql+asyncpg://sql_user:sql_pass@postgres.analytics.svc:5432/sql_explorer"
  auth:
    mode: "hybrid"
    ldap:
      server_uri: "ldaps://dc.company.local:636"
      bind_dn: "cn=svc_sql_explorer,ou=services,dc=company,dc=local"
      user_base_dn: "ou=users,dc=company,dc=local"
      group_base_dn: "ou=groups,dc=company,dc=local"
      allow_insecure_ssl: false
    kerberos:
      service_principal: "HTTP/sql-explorer.company.local@COMPANY.LOCAL"

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

Установите чарт:

```bash
helm upgrade --install sql-explorer helm/charts/sql-explorer \
  -n analytics \
  -f custom-values.yaml
```

## Проверка статуса

```bash
kubectl get pods -n analytics -l app.kubernetes.io/name=sql-explorer
kubectl logs -n analytics -l app.kubernetes.io/name=sql-explorer -c sql-explorer
```

---

## Настройка безопасности для промышленной установки

### 1. Ограничение частоты запросов и заголовки в Ingress
Чтобы ограничить подбор паролей к Active Directory через `/api/auth/login`, добавьте аннотации Ingress Controller:

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
Контейнер запускается от пользователя `appuser` (UID 10001, GID 10001). Значения `podSecurityContext` и `securityContext` в `values.yaml` соответствуют профилю Pod Security Restricted:
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
При `replicaCount` 2 и больше подключите внешний PostgreSQL (`config.database.url`). Реестр отозванных JWT-токенов (`revoked_tokens`) хранится в общей базе с двухуровневым кэшем (L1 — память процесса, L2 — PostgreSQL). После выхода пользователя (`POST /api/auth/logout`) токен отклоняется всеми репликами.

### 4. Сбор событий аудита (SIEM / SOC)
Приложение пишет события аудита в логгер `security.audit` в формате JSON, вывод идет в `stdout` пода:
```json
{"timestamp": "2026-09-06T12:00:00Z", "event_type": "AUTH_LOGIN_SUCCESS", "username": "analyst_user", "client_ip": "10.244.0.1", "status": "SUCCESS", "details": {"auth_method": "ldap", "groups": ["bi-analysts"]}}
```
Для отправки в централизованное хранилище (OpenSearch / ELK / Splunk) используйте DaemonSet с FluentBit, Promtail или Vector, настроенный на фильтрацию логов с логгером `security.audit`.


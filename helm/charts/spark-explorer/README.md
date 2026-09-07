# Helm Chart: SQL Web Explorer

Production Helm chart для развертывания веб-портала аналитических запросов **SQL Web Explorer (Trino & Hive)** в Kubernetes.

## Возможности

- 🚀 **Trino & Hive (HS2)**: готовая интеграция с аналитическими кластерами компании.
- 🔐 **Безопасность**:
  - Kerberos SPNEGO SSO и LDAPS аутентификация.
  - Автоматическая инициализация сервисного билета Kerberos (`kinit`) из Secret keytab с правами `0400`.
  - Проброс реального пользователя (`X-Trino-User` для Trino, `doAs` для Hive).
  - RBAC / ACL правила доступа на уровне кластеров и UI.
- ⚡ **Стриминг результатов**: конфигурация Ingress оптимизирована под Server-Sent Events (отключена буферизация Nginx, увеличены таймауты).
- 💾 **Хранение состояния**: поддержка внешнего PostgreSQL или встроенного SQLite через PersistentVolumeClaim.

## Быстрый старт

### 1. Добавление или локальная установка чарта

```bash
# Установка с дефолтными значениями
helm install my-sql-explorer ./helm/sql-explorer -n analytics --create-namespace
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

Примените установку:

```bash
helm upgrade --install sql-explorer ./helm/sql-explorer \
  -n analytics \
  -f custom-values.yaml
```

## Проверка статуса

```bash
kubectl get pods -n analytics -l app.kubernetes.io/name=sql-explorer
kubectl logs -n analytics -l app.kubernetes.io/name=sql-explorer -c sql-explorer
```

---

## Рекомендации по безопасности для Production

### 1. Ingress, Rate Limiting и Заголовки
Для защиты от подбора паролей к Active Directory на `/api/auth/login` и стабилизации сетевой нагрузки рекомендуется настроить аннотации Ingress Controller:

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

### 2. Запуск от непривилегированного пользователя (Non-Root)
Контейнер собран для выполнения от имени пользователя `appuser` (UID: 10001, GID: 10001). Значения `podSecurityContext` и `securityContext` в `values.yaml` настроены для соблюдения профиля безопасности **Restricted**:
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

### 3. Горизонтальное масштабирование и персистентный отзыв токенов (High Availability)
При масштабировании приложения (`replicaCount: 2+`) подключите внешний PostgreSQL (`config.database.url`). Реестр отозванных JWT-токенов (`revoked_tokens`) хранится в общей базе данных с двухуровневым кэшированием (L1 Memory Cache + L2 PostgreSQL). При выходе пользователя (`POST /api/auth/logout`) токен немедленно блокируется во всех репликах приложения.

### 4. Сбор и мониторинг событий аудита (SIEM / SOC)
Приложение генерирует события аудита информационной безопасности через логгер `security.audit` в формате структурированного JSON. Логи выводятся в стандартный поток вывода (`stdout`) пода:
```json
{"timestamp": "2026-09-06T12:00:00Z", "event_type": "AUTH_LOGIN_SUCCESS", "username": "analyst_user", "client_ip": "10.244.0.1", "status": "SUCCESS", "details": {"auth_method": "ldap", "groups": ["bi-analysts"]}}
```
Для отправки в централизованное хранилище (OpenSearch / ELK / Splunk) используйте DaemonSet с FluentBit, Promtail или Vector, настроенный на фильтрацию логов с логгером `security.audit`.


# Helm Chart: HDFS Web Explorer

Production-ready Helm-чарт для развертывания веб-портала **HDFS Web Explorer** (WebHDFS & HttpFS) в кластере Kubernetes.

## 🚀 Возможности чарта

- **Безопасность (Non-root)**: запуск контейнера от непривилегированного пользователя (`UID/GID 10001`, `appuser`), `readOnlyRootFilesystem: true`, capabilities сброшены (`drop: [ALL]`).
- **Сетевая изоляция (NetworkPolicy)**: декларативная политика, разрешающая входящий трафик только на целевой порт сервиса (8000), а исходящий трафик — строго к необходимым сервисам: DNS (53), Kerberos KDC (88, 749), LDAP/LDAPS (389, 636) и WebHDFS / DataNodes (9870, 9864, 50070, 50075).
- **Интеграция с Kerberos / SPNEGO**:
  - Монтирование пользовательского файла конфигурации `krb5.conf` через ConfigMap.
  - Поддержка создания K8s Secret с base64-кодированным keytab или использование существующего секрета (`existingSecret`).
  - Автоматическая инициализация Kerberos-билета (`kinit`) через `docker-entrypoint.sh`.
- **Хранение данных (PostgreSQL / SQLite)**: поддержка внешней базы данных PostgreSQL (`config.database.url`) или локального SQLite через `PersistentVolumeClaim` (`/app/data/hdfs_explorer.db`). При использовании SQLite количество реплик автоматически ограничивается `replicas: 1` для предотвращения блокировок файлов.
- **Корпоративная аутентификация**: поддержка LDAP/Active Directory и сопоставления групп с правами доступа.
- **Ingress & TLS**: оптимизация под потоковую передачу данных большого объема (отключена буферизация Nginx `proxy-buffering: off`, увеличены таймауты) и автоматическая TLS-терминация.
- **Health Probes**: Liveness и Readiness пробы по эндпоинту `/health`.

---

## 📦 Установка чарта

### 1. Локальная установка
```bash
helm install hdfs-explorer ./helm/hdfs-explorer \
  --namespace hdfs-system \
  --create-namespace
```

### 2. Установка с пользовательскими параметрами (`custom-values.yaml`)
```bash
helm upgrade --install hdfs-explorer ./helm/hdfs-explorer \
  -f custom-values.yaml \
  --namespace hdfs-system \
  --create-namespace
```

---

## ⚙️ Конфигурация секретов и Keytab

### Вариант А: Передача параметров через Values
```yaml
ingress:
  enabled: true
  hosts:
    - host: hdfs.corp.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: hdfs-explorer-tls
      hosts:
        - hdfs.corp.example.com

secrets:
  jwtSecret: "e8b23c91d4e7a8f1029384756cba0192e8b23c91d4e7a8f1029384756cba0192"
  ldapBindPassword: "ComplexServicePassword123"
  databasePassword: "ComplexDatabasePassword123"
  kerberosKeytabBase64: "ВСТАВИТЬ_СЮДА_KEYTAB_B64"

config:
  database:
    url: "postgresql://hdfs_user:hdfs_pass@postgres.hdfs-system.svc:5432/hdfs_explorer"
  ldap:
    server_uri: "ldaps://dc01.corp.example.com:636"
    bind_dn: "cn=svc-hdfs,ou=Services,dc=corp,dc=example,dc=com"
    user_search_base: "ou=Users,dc=corp,dc=example,dc=com"
    group_search_base: "ou=Groups,dc=corp,dc=example,dc=com"

clusters:
  - id: "prod-cluster"
    name: "Production DataLake"
    webhdfs_urls:
      - "http://nn01.corp.example.com:9870/webhdfs/v1"
      - "http://nn02.corp.example.com:9870/webhdfs/v1"
    auth_type: "kerberos"
    service_principal: "hdfs-explorer/hdfs.corp.example.com@CORP.EXAMPLE.COM"
    keytab_path: "/etc/security/keytabs/hdfs-explorer.keytab"
    acl:
      allowed_groups:
        - "data-platform"
        - "analysts"
      read_only_groups:
        - "analysts"

networkPolicy:
  enabled: true
  ingress:
    from: []
  egress:
    allowDns: true
    allowKerberos: true
    allowLdap: true
    allowHdfs: true
```

### Вариант Б: Использование существующих Kubernetes Secrets / Vault
Если вы используете **External Secrets Operator** или **SealedSecrets**:
```bash
kubectl create secret generic hdfs-explorer-secrets \
  --from-literal=jwt-secret="super-secret-jwt-key" \
  --from-literal=ldap-password="ldap-bind-password" \
  --from-file=hdfs-explorer.keytab=/path/to/hdfs-explorer.keytab \
  -n hdfs-system
```
И укажите в `values.yaml`:
```yaml
existingSecret: "hdfs-explorer-secrets"
```

---

## 🔒 Доверенные сертификаты LDAPS
Если Active Directory использует корпоративный самоподписанный центр сертификации (Enterprise Root CA):
```yaml
customLdapCaCert: |
  -----BEGIN CERTIFICATE-----
  MIIE...
  ...
  -----END CERTIFICATE-----
```
Helm-чарт автоматически смонтирует его в `/etc/ssl/certs/ldap-ca.crt` и включит безопасную проверку `verify_cert: true`.

# Helm Chart: HDFS Web Explorer

Helm-чарт для развертывания HDFS Web Explorer (WebHDFS и HttpFS) в Kubernetes.

## Что разворачивает чарт

- Контейнер без root: пользователь `appuser` (`UID/GID 10001`), `readOnlyRootFilesystem: true`, capabilities сброшены (`drop: [ALL]`).
- NetworkPolicy: входящий трафик разрешен только на порт сервиса 8000, исходящий — только к DNS (53), Kerberos KDC (88, 749), LDAP/LDAPS (389, 636) и WebHDFS / DataNode (9870, 9864, 50070, 50075).
- Kerberos / SPNEGO:
  - `krb5.conf` монтируется из ConfigMap.
  - Keytab берется из Secret, который чарт создает из base64-строки `secrets.kerberosKeytabBase64` (ключ `hdfs-explorer.keytab`), и монтируется в `/etc/security/keytabs`. Если `secrets.kerberosKeytabBase64` пуст, keytab не монтируется.
  - `kinit` в контейнере не запускается: образ стартует `java -jar` напрямую. Для кластера с `auth-type: kerberos` в `hadoop.hdfs.clusters` сервис сам получает билет через `UserGroupInformation.loginUserFromKeytabAndReturnUGI` с `service-principal` и `keytab-path` этого кластера (`NativeHdfsClient`).
- Хранилище: внешний PostgreSQL (`config.database.url`) или SQLite на `PersistentVolumeClaim` (по умолчанию `sqlite:////app/data/hdfs_tokens.db`). Если `config.database.url` пуст, начинается с `sqlite` или `jdbc:sqlite` либо указывает на встроенную H2 (`jdbc:h2:file:`, `jdbc:h2:mem:`), Deployment создается с `replicas: 1` независимо от `replicaCount`: несколько подов не могут писать в один файл базы. При `autoscaling.enabled: true` поле `replicas` не выставляется и ограничение не действует.
- Аутентификация через LDAP / Active Directory с сопоставлением групп и прав доступа.
- Ingress и TLS: для потоковой передачи больших файлов отключена буферизация Nginx (`proxy-buffering: off`, `proxy-request-buffering: off`), таймауты `proxy-read-timeout` и `proxy-send-timeout` подняты до 600 с, `proxy-body-size` — 500m. Сертификат выпускает cert-manager (аннотация `cert-manager.io/cluster-issuer`).
- Пробы на порту 8000: liveness — `GET /healthz`, readiness — `GET /actuator/health/readiness` (группа readiness Spring Boot Actuator: 200 `UP` после `ApplicationReadyEvent`, 503 `OUT_OF_SERVICE` с начала остановки контекста приложения).

---

## Установка чарта

Команды выполняются из корня репозитория.

### 1. Локальная установка
```bash
helm install hdfs-explorer helm/charts/hdfs-explorer \
  --namespace hdfs-system \
  --create-namespace
```

### 2. Установка с пользовательскими параметрами (`custom-values.yaml`)
```bash
helm upgrade --install hdfs-explorer helm/charts/hdfs-explorer \
  -f custom-values.yaml \
  --namespace hdfs-system \
  --create-namespace
```

---

## Секреты и keytab

### Вариант А: параметры в values
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

### Вариант Б: существующий Secret (External Secrets Operator, SealedSecrets, Vault)
Создайте Secret заранее:
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

## CA-сертификат для LDAPS
Если сертификат Active Directory выпущен внутренним центром сертификации (Enterprise Root CA), передайте корневой сертификат:
```yaml
customLdapCaCert: |
  -----BEGIN CERTIFICATE-----
  MIIE...
  ...
  -----END CERTIFICATE-----
```
Чарт смонтирует его в `/etc/ssl/certs/ldap-ca.crt`. Проверку сертификата LDAP включает `config.ldap.verify_cert` (в `values.yaml` по умолчанию `true`).

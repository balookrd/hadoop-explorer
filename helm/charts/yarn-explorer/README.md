# Helm Chart: YARN Queue Explorer

Helm-чарт для развертывания YARN Queue Explorer (веб-интерфейс Apache Hadoop Capacity Scheduler) в Kubernetes. Версия чарта `0.1.0`, `appVersion` `0.1.0` (`Chart.yaml`).

## Что разворачивает чарт

- Контейнер запускается от непривилегированного пользователя (UID 10001), `allowPrivilegeEscalation: false`, все capabilities сброшены; параметры в `securityContext`.
- Kerberos / SPNEGO:
  - `krb5.conf` из `kerberos.krb5Config` монтируется через ConfigMap в `/etc/krb5.conf`.
  - Keytab берется из существующего Secret (`kerberos.keytab.existingSecret`) или чарт создает Secret из base64-строки (`kerberos.keytab.keytabBase64`).
- Хранение заявок: внешний PostgreSQL (`config.database.url`) или SQLite на `PersistentVolumeClaim` (`/app/data/yarn_explorer.db`). Если `config.database.url` пуст, начинается с `sqlite` или `jdbc:sqlite` либо указывает на встроенную H2 (`jdbc:h2:file:`, `jdbc:h2:mem:`), Deployment создается с `replicas: 1` независимо от `replicaCount`: несколько подов не могут писать в один файл базы. При `autoscaling.enabled: true` поле `replicas` не выставляется и ограничение не действует.
- Аутентификация через LDAP / Active Directory и сопоставление групп с ролями ADMIN, WRITER, READER (секция `config.acl`).
- Ingress с TLS через `ingress.tls` для ingress-контроллеров (`ingress-nginx`, Traefik и др.).
- Пробы: liveness — `GET /healthz`, readiness — `GET /actuator/health/readiness` (группа readiness Spring Boot Actuator: 200 `UP` после `ApplicationReadyEvent`, 503 `OUT_OF_SERVICE` с начала остановки контекста приложения).
- Аннотации `checksum/config` и `checksum/krb5` (вторая — только при `kerberos.enabled: true`) содержат sha256 ConfigMap, поэтому `helm upgrade` с измененной конфигурацией перезапускает поды.

## Установка чарта

### 1. Добавление репозитория или локальная установка
```bash
# Клонирование репозитория
git clone https://github.com/balookrd/hadoop-explorer.git
cd hadoop-explorer

# Установка чарта в namespace yarn-system
helm install yarn-explorer helm/charts/yarn-explorer \
  --namespace yarn-system \
  --create-namespace
```

### 2. Установка с пользовательскими параметрами (`custom-values.yaml`)
```bash
helm upgrade --install yarn-explorer helm/charts/yarn-explorer \
  -f custom-values.yaml \
  --namespace yarn-system \
  --create-namespace
```

### 3. Удаление релиза
```bash
helm uninstall yarn-explorer --namespace yarn-system
```

## Параметры `values.yaml`

### Основные параметры

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `replicaCount` | `1` | Количество реплик Deployment |
| `image.repository` | `balookrd/yarn-explorer` | Docker-образ приложения |
| `image.tag` | `""` (берется `appVersion` из `Chart.yaml`) | Тег Docker-образа |
| `image.pullPolicy` | `IfNotPresent` | Политика скачивания образа |
| `imagePullSecrets` | `[]` | Секреты для скачивания из приватных реестров |
| `nameOverride` | `""` | Переопределение короткого имени релиза |
| `fullnameOverride` | `""` | Переопределение полного имени релиза |

### Сеть и Ingress

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `service.type` | `ClusterIP` | Тип Kubernetes Service |
| `service.port` | `80` | Внешний порт сервиса |
| `service.targetPort` | `8000` | Порт контейнера |
| `ingress.enabled` | `false` | Включение создания Ingress |
| `ingress.className` | `""` | Имя IngressClass (например `nginx`) |
| `ingress.annotations` | `{}` | Дополнительные аннотации Ingress |
| `ingress.hosts` | `[{host: "yarn-explorer.local", paths: [{path: "/", pathType: "ImplementationSpecific"}]}]` | Список виртуальных хостов и путей |
| `ingress.tls` | `[]` | Настройки TLS сертификатов |

### Безопасность и переменные окружения

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `securityContext.runAsNonRoot` | `true` | Запуск от непривилегированного пользователя |
| `securityContext.runAsUser` | `10001` | UID пользователя процесса |
| `securityContext.runAsGroup` | `10001` | GID пользователя процесса |
| `extraEnv` | `[]` | Список дополнительных env-переменных (включая secretKeyRef) |
| `envFrom` | `[]` | Источники envFrom (например, secretRef с секретами) |

### Ресурсы и пробы

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `resources.limits.cpu` | `1000m` | Лимит CPU |
| `resources.limits.memory` | `1Gi` | Лимит оперативной памяти |
| `resources.requests.cpu` | `100m` | Запрос CPU (requests) |
| `resources.requests.memory` | `256Mi` | Запрос памяти (requests) |
| `livenessProbe.httpGet.path` | `/healthz` | Путь liveness-пробы |
| `readinessProbe.httpGet.path` | `/actuator/health/readiness` | Путь readiness-пробы |

### Постоянное хранилище (SQLite)

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `persistence.enabled` | `true` | Включение PersistentVolumeClaim |
| `persistence.storageClass` | `""` | StorageClass тома: пусто — StorageClass кластера по умолчанию, `"-"` — `storageClassName: ""` |
| `persistence.accessMode` | `ReadWriteOnce` | Режим доступа тома |
| `persistence.size` | `1Gi` | Размер постоянного тома |
| `persistence.mountPath` | `/app/data` | Точка монтирования тома в контейнере |
| `persistence.existingClaim` | `""` | Имя существующего PVC; если задано, чарт не создает свой PVC |

### Kerberos / SPNEGO

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `kerberos.enabled` | `false` | Включение монтирования Kerberos конфигурации |
| `kerberos.keytab.keytabBase64` | `""` | Base64-строка keytab файла (создаст K8s Secret) |
| `kerberos.keytab.existingSecret` | `""` | Имя уже созданного K8s Secret с keytab |
| `kerberos.keytab.fileName` | `yarn-explorer.keytab` | Ключ в Secret и имя файла keytab |
| `kerberos.keytab.mountPath` | `/etc/security/keytabs` | Директория монтирования keytab в контейнере |
| `kerberos.krb5Config` | пример для realm `COMPANY.LOCAL` | Текстовое содержимое файла `krb5.conf` |

## Пример `custom-values.yaml` для production

[проверить: пример расходится с `values.yaml`: ключ `kerberos.krb5Conf` в чарте называется `kerberos.krb5Config`, `persistence.storageClassName` — `persistence.storageClass`, `config.server.port` по умолчанию `8000`; блок `secrets:` вставлен внутрь секции `config.auth`, из-за чего YAML невалиден]

```yaml
replicaCount: 1

image:
  repository: my-registry.company.local/bigdata/yarn-explorer
  tag: "1.0.0"
  pullPolicy: IfNotPresent

ingress:
  enabled: true
  className: "nginx"
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/proxy-body-size: "32m"
  hosts:
    - host: yarn-explorer.company.local
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: yarn-explorer-tls
      hosts:
        - yarn-explorer.company.local

persistence:
  enabled: true
  storageClassName: "fast-rbd"
  size: 10Gi

kerberos:
  enabled: true
  keytab:
    keytabBase64: "BQIAAABUAAI..."
  krb5Conf: |
    [libdefaults]
      default_realm = COMPANY.LOCAL
      dns_lookup_realm = false
      dns_lookup_kdc = false
      ticket_lifetime = 24h
      renew_lifetime = 7d
      forwardable = true
    [realms]
      COMPANY.LOCAL = {
        kdc = kdc1.company.local:88
        kdc = kdc2.company.local:88
        admin_server = kdc1.company.local:749
      }

config:
  server:
    host: "0.0.0.0"
    port: 8080
    debug: false
    cors_origins:
      - "https://yarn-explorer.company.local"
  database:
    url: "postgresql://yarn_user:yarn_pass@postgres.yarn-system.svc:5432/yarn_explorer"
  auth:
    mode: "hybrid"
    jwt:
      secret_key: "super-secure-production-jwt-token-key-2026"

secrets:
  databasePassword: "ComplexDatabasePassword123"
      expire_minutes: 720
    ldap:
      enabled: true
      server_uri: "ldaps://corp-ad.company.local:636"
      use_ssl: true
      bind_dn: "CN=svc-yarn-explorer,OU=ServiceAccounts,DC=company,DC=local"
      bind_password: "ProdServicePasswordHere"
      user_base_dn: "OU=Users,DC=company,DC=local"
      user_filter: "(&(objectClass=user)(sAMAccountName={username}))"
  acl:
    ui_access:
      allowed_users: ["*"]
      allowed_groups: ["*"]
    roles:
      admin:
        groups: ["Hadoop-Admins"]
      writer:
        groups: ["Hadoop-Operators"]
      reader:
        groups: ["*"]
  clusters:
    - id: "prod-yarn"
      name: "Production Hadoop"
      description: "Hadoop 3.3 Production YARN Cluster"
      resource_manager_urls:
        - "http://rm1.company.local:8088"
        - "http://rm2.company.local:8088"
      kerberos_enabled: true
      default_partition: "DEFAULT"
```

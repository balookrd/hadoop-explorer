# 🛠️ Руководство администратора: Hadoop gRPC Replicator

Данный документ содержит полное руководство для инженеров **DevOps / SRE / сетевых администраторов** по установке, конфигурированию, промышленному развертыванию и эксплуатации распределенной системы межкластерной репликации **Hadoop gRPC Replicator** в трех режимах:
1. **Standalone** (Bare-Metal / Виртуальные машины под управлением systemd)
2. **Docker & Docker Compose** (Контейнеризированный запуск компонентов)
3. **Kubernetes** (Промышленное развертывание в K8s)

---

## 1. Архитектура и сетевая топология

#### 1.1 Архитектура и компоненты системы
Hadoop gRPC Replicator построен на базе симметричных универсальных агентов **Replicator Agent (`Full-Duplex`)**:
Каждый узел в ЦОД1 и ЦОД2 запускает агент `backend.replicator.agent`, который одновременно принимает входящие gRPC-потоки (:50051) и передает исходящие задачи из очереди Оркестратора. Это обеспечивает полноценную двунаправленную репликацию (`DC1 ⇄ DC2`, DR failback) в рамках единого сервиса.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                             ЦОД 1 (Москва / Primary)                            │
│                                                                                 │
│   ┌────────────────────────┐                    ┌───────────────────────────┐   │
│   │    HDFS DataLake 1     │ ◄──(чтение/запись)─► │   Replicator Agent DC1    │   │
│   │(demo/analytics-cluster)│                    │  (:50051, Mode: Full-Duplex)│ │
│   └────────────────────────┘                    └─────────────▲─────────────┘   │
└───────────────────────────────────────────────────────────────│─────────────────┘
                                                                │
                     ┌──────────────────────────────────────────┴───────────────┐
                     │ Двунаправленный gRPC стриминг (:50051 ⇄ :50051 / :50052) │
                     │ [Многоуровневый шейпинг WAN: Global + DC-DC + HDFS-HDFS] │
                     ▼                                                          │
┌───────────────────────────────────────────────────────────────────────────────┴─┐
│                     ЦОД 2 (Санкт-Петербург / Disaster Recovery)                 │
│                                                                                 │
│   ┌────────────────────────┐                    ┌───────────────────────────┐   │
│   │    HDFS DataLake 2     │ ◄──(чтение/запись)─► │   Replicator Agent DC2    │   │
│   │    (backup-cluster)    │                    │  (:50051, Mode: Full-Duplex)│ │
│   └────────────────────────┘                    └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                ▲                                ▲
             (запрос квот)      │                                │ (оркестрация)
                                │                                │
┌──────────────────────────────┴────────────────────────────────┴─────────────────┐
│                     ЦОД Управления / Центральный сегмент                        │
│                                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                    Replicator Orchestrator (:8005)                      │   │
│   │  - Web UI SPA (Svelte 5) & REST API                                     │   │
│   │  - Hierarchical Token Bucket Throttler (многоуровневый шейпинг полосы)  │   │
│   │  - Cron Scheduler (планировщик периодических задач со статусом SCHEDULED│   │
│   │  - Job Runs History & Retention (журнал запусков со статистикой)        │   │
│   │  - Реестр топологии кластеров и автоматический резолвинг target_address │   │
│   │  - Каноническая база данных SQLite: /app/data/replicator.db             │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Сетевые порты и протоколы
| Сервис | Порт | Протокол | Направление | Назначение |
|---|---|---|---|---|
| **Orchestrator** | `8005` | HTTP/HTTPS | Inbound | Web UI консоль, REST API, выдача токенов шейпера, `/health`, `/metrics` |
| **Replicator Agent (DC1 / DC2)** | `50051` | gRPC (HTTP/2) | Inbound/Outbound | Полный дуплекс: прием входящих чанков и отправка исходящих реплик |
| **NameNodes** | `9870`/`8020` | HTTP / RPC | Outbound | Чтение из исходного HDFS и запись в целевой HDFS |
| **KDC** | `88` | TCP/UDP | Outbound | Выпуск билетов Kerberos для системной техучетки |

### 1.3 Динамическая регистрация агентов и разрешение адресов (Dynamic Keepalive Service Discovery)

В платформе реализован режим **динамической саморегистрации агентов с keepalive-мониторингом**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│             Динамический жизненный цикл агента (Keepalive Lifecycle)        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
               [1. Старт агента]      ▼
          POST /api/v1/agents/register (agent_id, cluster_id, grpc_address)
                                      │
                                      ▼
               ┌──────────────────────────────────────────────┐
               │ Статус: ONLINE                               │
               │ Оркестратор привязывает gRPC-адрес к кластеру│
               └──────────────────────┬───────────────────────┘
                                      │
     ┌────────────────────────────────┼────────────────────────────────┐
     │ Каждые 3 сек:                  │ При остановке агента (SIGTERM):│
     ▼                                ▼                                ▼
POST /api/v1/agents/heartbeat    Таймаут > 15 сек            POST /api/v1/agents/unregister
(active_transfers: N)            (нет heartbeat)                       │
     │                                │                                ▼
     ▼                                ▼                       Статус: OFFLINE
Обновление TTL                   Статус: STALE                 (исключен из пула)
(счетчик активных задач)         (исключен из маршрутизации)
```

#### Приоритет разрешения целевого адреса (get_target_address):
1. **Динамический реестр живых агентов (AgentRegistry keepalive) — основной механизм**:
   - Агент при старте автоматически сообщает свой внешний адрес `advertised_grpc_address` (например, `agent-dc1:50051`).
   - Оркестратор динамически регистрирует этот адрес за целевым HDFS-кластером.
   - Если один кластер обслуживают несколько агентов (горизонтальное масштабирование), Оркестратор выполняет балансировку нагрузки, выбирая узел с наименьшим числом текущих задач (`active_transfers`).
2. **Статическая топология Оркестратора (`config.yaml`)**:
   - Значения по умолчанию из файла конфигурации используются как резерв, если живой агент еще не зарегистрировался.
3. **Локальный оверрайд переменной окружения `AGENT_TARGET_<CLUSTER_ID>`**:
   - Применяется в нестандартных изолированных сетях (NAT, Ingress-шлюзы, раздельные DMZ).
4. **Резервный адрес по умолчанию (Fallback)**:
   - Значение переменной `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS` (по умолчанию `localhost:50051`).

### 1.3.1 Безопасность динамической регистрации (Security & Zero-Trust)

Динамическая регистрация защищена комплексной многоуровневой системой безопасности:

1. **Аутентификация агентов через Agent Secret (PSK / Token)**:
   - На Оркестраторе и всех доверенных агентах задается общий секретный ключ `REPLICATOR_AGENT_SECRET` (минимум 32 символа).
   - Агент автоматически передает этот токен в HTTP-заголовке `X-Agent-Secret` (или `Authorization: Bearer <token>`) при вызовах `/api/v1/agents/register`, `/api/v1/agents/heartbeat`, `/api/v1/agents/unregister` и запросах сетевых квот `/api/v1/tokens/request`.
   - Запросы без валидного токена отбрасываются с кодом `401 Unauthorized`. В боевом режиме (`ENVIRONMENT=production`) отсутствие переменной блокирует старт сервиса.
2. **Белый список кластеров (Cluster Whitelisting)**:
   - Переменная `REPLICATOR_ENFORCE_CLUSTER_WHITELIST=true` (по умолчанию активна в боевом режиме).
   - Агенту разрешено регистрироваться только для тех кластеров, которые явно объявлены администратором в `config.yaml` (`topology.clusters`).
   - Попытка зарегистрировать неавторизованный кластер отклоняется с ошибкой `403 Forbidden`, защищая топологию от отравления (Topology Poisoning).
3. **Защита от SSRF и валидация сетевых адресов**:
   - Оркестратор валидирует структуру анонсируемого gRPC-адреса `host:port` и диапазон порта (1–65535).
   - Автоматически блокируются попытки передать эндпоинты облачных метаданных (`169.254.169.254`, `metadata.google.internal`, `instance-data`) и адреса link-local (`169.254.0.0/16`, `fe80::/10`).

---

### 1.4 Справочник переменных окружения (Environment Variables Reference)

#### 1.4.1 Переменные Replicator Orchestrator
| Переменная | Описание | Значение по умолчанию | Обязательность |
|---|---|---|---|
| `CONFIG_PATH` / `REPLICATOR_CONFIG_PATH` | Путь к файлу конфигурации топологии и лимитов | `backend/replicator/config/config.yaml` | Опционально |
| `REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC` | Глобальный лимит пропускной способности WAN (байт/сек) | `52428800` (50 МБ/с) | Опционально |
| `REPLICATOR_DATABASE_URL` | Строка подключения к базе данных SQLite/PostgreSQL | `sqlite:////app/data/replicator.db` | Опционально |
| `REPLICATOR_SERVICE_PRINCIPAL` | Kerberos-принципал системной техучетки для репликации | `hdfs-replicator@REALM.LOCAL` | Опционально |
| `REPLICATOR_AGENT_SECRET` | Общий секретный ключ аутентификации агентов репликации (заголовок `X-Agent-Secret`) | — (в dev опционален, в prod обязателен) | Обязательно в prod |
| `REPLICATOR_ENFORCE_CLUSTER_WHITELIST` | Принудительная блокировка регистрации неизвестных кластеров (`true`/`false`) | `false` (в prod автоматически `true`) | Опционально |
| `JWT_SECRET_KEY` | Секретный ключ подписи JWT-токенов сессий Web UI (мин. 32 симв.) | В dev автогенерируется, в prod обязателен | Обязательно в prod |

#### 1.4.2 Переменные Replicator Agent (Full-Duplex)
| Переменная | Описание | Значение по умолчанию | Обязательность |
|---|---|---|---|
| `AGENT_ID` | Уникальный строковый идентификатор агента в платформе | `agent-01` (или `$WORKER_ID`) | Рекомендуется |
| `AGENT_CLUSTER_ID` | Идентификатор HDFS-кластера, который обслуживает данный агент | `demo-cluster` | Рекомендуется |
| `REPLICATOR_AGENT_SECRET` | Секретный ключ для аутентификации на Оркестраторе (передается в `X-Agent-Secret`) | — (должен совпадать с Оркестратором) | Обязательно в prod |
| `AGENT_MODE` | Режим работы агента: `all` (дуплекс: прием + передача), `sender` (только передача), `receiver` (только прием) | `all` | Опционально |
| `ORCHESTRATOR_URL` | URL Replicator Orchestrator для саморегистрации, задач и квот | `http://localhost:8005` | Обязательно |
| `RECEIVER_HOST` | Сетевой интерфейс для прослушивания входящего gRPC-сервера | `0.0.0.0` | Опционально |
| `RECEIVER_PORT` | Порт входящего gRPC-сервера для приема чанков данных | `50051` | Опционально |
| `AGENT_ADVERTISED_ADDRESS` | Внешний gRPC-адрес для саморегистрации в Оркестраторе (например, `agent-dc1:50051`) | Вычисляется автоматически (`hostname:50051`) | Опционально |
| `AGENT_ENABLE_REGISTRATION` | Включение режима динамической саморегистрации с keepalive | `true` | Опционально |
| `REPLICATOR_STAGING_DIR` | Локальная директория буфера для приема чанков перед коммитом | `/tmp/staging` | Опционально |
| `REPLICATOR_HDFS_NAMENODE` | Хост целевого NameNode для прямого HDFS-коммита через PyArrow | Не задано (локальный commit) | Опционально |
| `POLL_INTERVAL_SEC` | Интервал опроса очереди задач и keepalive-пингов (секунды) | `3.0` | Опционально |
| `AGENT_MAX_BANDWIDTH_MB_S` | Локальный лимит пропускной способности агента (МБ/с, Token Bucket). Предотвращает перегрузку сетевой карты и дисков при установке непосредственно на ноду Hadoop (DataNode / Edge Gateway). `0` или пусто — без ограничений | `0.0` (без ограничений) | Опционально |
| `AGENT_TARGET_<CLUSTER_ID>` | Ручной оверрайд сетевого gRPC-адреса для целевого кластера (для NAT/DMZ) | Динамический реестр Оркестратора | Опционально |
| `RECEIVER_ADDRESS` / `FALLBACK_TARGET_ADDRESS` | Резервный gRPC-адрес назначения (fallback при отсутствии агентов) | `localhost:50051` | Опционально |
| `REPLICATOR_KEYTAB_PATH` | Путь к Keytab-файлу системной техучетки для Kerberos аутентификации | `/etc/security/keytabs/replicator.keytab` | Рекомендуется |

### 1.5 Настройка Hadoop Impersonation (Proxy User) для Apache Ranger

Для обеспечения прозрачного аудита безопасности и применения политик доступа в **Apache Ranger** платформа использует протокол **Hadoop Proxy User (doAs имперсонация)**:
1. Replicator Agent аутентифицируется в Kerberos KDC по системному keytab техучетки (`hdfs-replicator@REALM.LOCAL`).
2. При вызове HDFS (`pyarrow.fs.HadoopFileSystem`) агент передает имя конечного пользователя (`user=alice`, извлеченное из `execution_principal` или сессии).
3. NameNode и Apache Ranger проверяют политики доступа непосредственно для `alice`, а в Ranger Audit Log регистрируется запись вида:
   ```
   UGI: alice (auth:PROXY via hdfs-replicator@REALM.LOCAL)
   Resource: /data/production/events/...
   Action: WRITE / READ
   Access Result: ALLOWED / DENIED
   ```

Для работы данного механизма в файле конфигурации `core-site.xml` на NameNode должна быть разрешена имперсонация для сервисной техучетки:
```xml
<property>
    <name>hadoop.proxyuser.hdfs-replicator.hosts</name>
    <value>*</value>
</property>
<property>
    <name>hadoop.proxyuser.hdfs-replicator.groups</name>
    <value>*</value>
</property>
```

---

## 2. Режим 1: Standalone (Bare-Metal / VM / systemd)

Развертывание трех компонентов на Linux-серверах под управлением `systemd`.

### Шаг 1: Системные пакеты ОС
Выполняется на серверах Orchestrator, Receiver и Worker:
```bash
# Ubuntu / Debian:
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    build-essential python3 python3-venv python3-dev \
    libkrb5-dev libkrb5-3 krb5-user curl git

# RHEL / Rocky Linux:
sudo dnf install -y gcc python3 python3-devel krb5-devel krb5-workstation curl git
```

### Шаг 2: Пользователь и каталоги
```bash
sudo groupadd -g 10001 appuser
sudo useradd -u 10001 -g appuser -m -s /bin/bash appuser

sudo mkdir -p /opt/hadoop-explorer/replicator
sudo mkdir -p /etc/hadoop-explorer/replicator
sudo mkdir -p /var/log/hadoop-explorer/replicator
sudo mkdir -p /var/lib/hadoop-explorer/replicator/data
sudo mkdir -p /var/lib/hadoop-explorer/replicator/staging
sudo mkdir -p /etc/security/keytabs

sudo chown -R appuser:appuser /opt/hadoop-explorer /etc/hadoop-explorer /var/log/hadoop-explorer /var/lib/hadoop-explorer/replicator
```

### Шаг 3: Размещение кода и виртуального окружения
```bash
sudo -u appuser -i
cd /opt/hadoop-explorer/replicator
git clone https://github.com/company/hadoop-explorer.git .

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

# Установка зависимостей
pip install -e backend/common -e backend/replicator
pip install grpcio>=1.62.0 grpcio-tools>=1.62.0 protobuf>=4.25.0

# Компиляция Protobuf контрактов
python -m grpc_tools.protoc \
    -I backend/replicator/proto \
    --python_out=backend/replicator/generated \
    --grpc_python_out=backend/replicator/generated \
    backend/replicator/proto/replicator.proto

sed -i -e 's/^import replicator_pb2 as/from . import replicator_pb2 as/g' \
    backend/replicator/generated/replicator_pb2_grpc.py

# Сборка фронтенда Replicator SPA
cd frontend
npm ci --workspace=apps/replicator --include-workspace-root
npm run build:replicator
cd ..
```

### Шаг 4: Конфигурация компонентов (`/etc/hadoop-explorer/replicator/config.yaml`)
```yaml
server:
  host: "0.0.0.0"
  port: 8005
  debug: false
  secure_cookies: true
  cors_origins:
    - "https://replicator.company.local"

security:
  secret_key: "CHANGE_TO_SUPER_SECURE_KEY_AT_LEAST_32_CHARS"
  algorithm: "HS256"
  access_token_expire_minutes: 480
  cookie_name: "replicator_session"

database:
  url: "sqlite:////var/lib/hadoop-explorer/replicator/data/replicator.db"
  # Для промышленного кластера:
  # url: "postgresql://replicator_user:DbPass123@pg.company.local:5432/replicator"

# Настройка топологии ЦОД и иерархического шейпинга
replicator:
  global_limit_mb_per_sec: 100 # Глобальный пул полосы пропускания WAN

  datacenters:
    - id: "dc1"
      name: "Дата-Центр Москва (DC1)"
      location: "Moscow"
    - id: "dc2"
      name: "Дата-Центр Санкт-Петербург (DC2)"
      location: "Saint-Petersburg"

  clusters:
    - id: "demo-cluster"
      name: "HDFS Primary (Prod)"
      dc_id: "dc1"
      default_path: "/data/production"
    - id: "backup-cluster"
      name: "HDFS DR (Backup)"
      dc_id: "dc2"
      default_path: "/backup/mirror"

  # Ограничения каналов между дата-центрами (DC-DC)
  dc_limits:
    - source_dc: "dc1"
      target_dc: "dc2"
      limit_mb_per_sec: 80

  # Ограничения каналов между кластерами HDFS (HDFS-HDFS)
  hdfs_limits:
    - source_cluster: "demo-cluster"
      target_cluster: "backup-cluster"
      limit_mb_per_sec: 60
```

> [!NOTE]
> **Важно**: В актуальной архитектуре файл `config.yaml` Оркестратора управляет **исключительно сетевой топологией и лимитами полосы WAN**. Секции `receiver` и `worker` более не используются — функции приема и передачи объединены в единый универсальный **Replicator Agent**, который настраивается переменными окружения и регистрируется на Оркестраторе динамически.


### Шаг 5: Создание systemd units

#### 1. Сервис Оркестратора (`/etc/systemd/system/replicator-orchestrator.service`):
```ini
[Unit]
Description=Hadoop gRPC Replicator Orchestrator
After=network.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/hadoop-explorer/replicator
Environment="PYTHONPATH=/opt/hadoop-explorer/replicator:/opt/hadoop-explorer/replicator/backend:/opt/hadoop-explorer/replicator/backend/replicator"
Environment="CONFIG_PATH=/etc/hadoop-explorer/replicator/config.yaml"
Environment="WEB_CONCURRENCY=2"

ExecStart=/opt/hadoop-explorer/replicator/.venv/bin/uvicorn backend.replicator.orchestrator.main:app \
    --host 0.0.0.0 \
    --port 8005 \
    --workers 2 \
    --log-level info

Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

#### 2. Рекомендуемый сервис: Универсальный Агент (`/etc/systemd/system/replicator-agent.service`):
Устанавливается на серверах в каждом ЦОД (DC1 и DC2) для обеспечения полной двунаправленной репликации (`DC1 ⇄ DC2`):
```ini
[Unit]
Description=Hadoop gRPC Replicator Full-Duplex Agent
After=network.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/hadoop-explorer/replicator
Environment="PYTHONPATH=/opt/hadoop-explorer/replicator:/opt/hadoop-explorer/replicator/backend:/opt/hadoop-explorer/replicator/backend/replicator"
Environment="AGENT_ID=agent-dc1-node-01"
Environment="AGENT_CLUSTER_ID=demo-cluster"
Environment="AGENT_MODE=all"
Environment="ORCHESTRATOR_URL=http://orchestrator.company.local:8005"
Environment="RECEIVER_HOST=0.0.0.0"
Environment="RECEIVER_PORT=50051"
Environment="REPLICATOR_STAGING_DIR=/var/lib/hadoop-explorer/replicator/staging"
Environment="POLL_INTERVAL_SEC=2.0"

ExecStart=/opt/hadoop-explorer/replicator/.venv/bin/python -m backend.replicator.agent

Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### Шаг 6: Запуск и валидация
```bash
sudo systemctl daemon-reload

# На сервере Оркестратора:
sudo systemctl enable --now replicator-orchestrator
curl -s http://127.0.0.1:8005/health
# {"status":"ok","timestamp":"..."}

# На серверах Агентов (DC1 и DC2):
sudo systemctl enable --now replicator-agent
```

### 2.1 Запуск нескольких агентов для одного кластера (Горизонтальное масштабирование и HA)

Оркестратор из коробки поддерживает параллельную работу **произвольного числа агентов**, обслуживающих один и тот же HDFS-кластер.

#### Механизм распределения нагрузки:
- **Параллельная передача (Sender Scale-out)**: Все запущенные агенты с одинаковым `AGENT_CLUSTER_ID` независимо опрашивают очередь задач Оркестратора (`POLL_INTERVAL_SEC=2.0`). Задачи забираются свободными воркерами параллельно, линейно увеличивая общую скорость репликации кластера.
- **Интеллектуальная балансировка приема (Least-Connections Receiver)**: Когда удаленный дата-центр запрашивает адрес для передачи файлов в данный кластер, Оркестратор выбирает онлайн-агент с **наименьшим числом активных потоков** (`min(active_transfers)`).
- **Автоматический Failover**: При аварии узла агент перестает слать Keepalive; через 15 секунд его статус переходит в `STALE`, и он исключается из маршрутизации входящих потоков.

#### Вариант 1: Запуск нескольких агентов в systemd (Шаблонные Units)
Создайте шаблонный юнит `/etc/systemd/system/replicator-agent@.service`:
```ini
[Unit]
Description=Hadoop Replicator Agent Instance %i
After=network.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/opt/hadoop-explorer/replicator
Environment="PYTHONPATH=/opt/hadoop-explorer/replicator"
Environment="AGENT_ID=agent-dc1-%i"
Environment="AGENT_CLUSTER_ID=demo-cluster"
Environment="RECEIVER_PORT=5005%i"
Environment="AGENT_ADVERTISED_ADDRESS=node1.dc1.company.local:5005%i"
Environment="ORCHESTRATOR_URL=http://orchestrator.company.local:8005"
ExecStart=/opt/hadoop-explorer/replicator/.venv/bin/python -m backend.replicator.agent
Restart=always

[Install]
WantedBy=multi-user.target
```
Запуск 2 инстансов агента на одной ноде:
```bash
sudo systemctl enable --now replicator-agent@1
sudo systemctl enable --now replicator-agent@2
```

#### Вариант 2: Запуск нескольких агентов в Docker Compose
```yaml
services:
  agent-dc1-01:
    image: hadoop-explorer/replicator:latest
    environment:
      AGENT_ID: agent-dc1-01
      AGENT_CLUSTER_ID: demo-cluster
      ORCHESTRATOR_URL: http://orchestrator:8005
      RECEIVER_PORT: 50051

  agent-dc1-02:
    image: hadoop-explorer/replicator:latest
    environment:
      AGENT_ID: agent-dc1-02
      AGENT_CLUSTER_ID: demo-cluster
      ORCHESTRATOR_URL: http://orchestrator:8005
      RECEIVER_PORT: 50051
```

#### Вариант 3: Запуск в Kubernetes (Deployment с replicas > 1)
В Kubernetes достаточно указать `replicas: 3` в Deployment агента. Имя пода автоматически передается в `AGENT_ID`, а сетевой адрес — через Pod IP или Headless Service DNS.

### 2.2 Совместное развертывание на нодах Hadoop (DataNode Co-location) и локальный шейпинг полосы (`AGENT_MAX_BANDWIDTH_MB_S`)

#### Зачем устанавливать агент непосредственно на DataNode / Edge Gateway?
В высоконагруженных enterprise-кластерах выделение отдельных серверов-шлюзов под репликацию может создавать бутылочное горлышко. Установка Replicator Agent непосредственно на узлы Hadoop (DataNode или Edge Node) обеспечивает:
1. **Локальный ввод-вывод (Zero WAN-Hop внутри кластера)**: агент читает блоки данных непосредственно с локальных дисковых массивов или обращается к локальному DataNode демону, минуя дополнительную пересылку по локальной сети кластера.
2. **Линейное масштабирование**: при добавлении новых DataNode пропускная способность кластера по репликации растет пропорционально.

#### Проблема и риски совместного размещения:
Если агент не ограничен по ресурсам, при репликации объемного датасета он может занять 100% пропускной способности сетевой карты ноды (1GbE / 10GbE / 25GbE) или вызвать дисковую перегрузку (I/O saturation). В результате боевые задачи Spark, YARN-контейнеры и запросы HDFS DataNode начнут испытывать задержки и падать по Heartbeat Timeout.

#### Решение: Локальный шейпер `AGENT_MAX_BANDWIDTH_MB_S`
Для изоляции ресурсов каждого агента предусмотрена переменная окружения `AGENT_MAX_BANDWIDTH_MB_S`:
```ini
# /etc/systemd/system/replicator-agent.service.d/override.conf
[Service]
# Ограничить утилизацию сетевого интерфейса и дисков этой DataNode до 40 МБ/с
Environment="AGENT_MAX_BANDWIDTH_MB_S=40.0"
```

#### Принцип работы локального шейпера (Token Bucket):
1. **Исходящий трафик (Sender Throttling)**: Перед передачей каждого потокового чанка данных воркер запрашивает токены у встроенного `LocalBandwidthLimiter`. Если лимит исчерпан, передача приостанавливается на рассчитанный интервал времени. Затем воркер дополнительно согласовывает квоту с Оркестратором (глобальные ограничения DC-DC и HDFS-HDFS).
2. **Входящий трафик (Receiver Backpressure Throttling)**: При приеме чанков на стороне сервера Receiver агент сдерживает чтение блоков через `LocalBandwidthLimiter`. Благодаря протоколу HTTP/2 и окнам приема TCP (TCP Window Flow Control), задержка в чтении буфера автоматически передается удаленному передающему узлу через WAN, плавно снижая скорость отправки без переполнения оперативной памяти и без потерь пакетов.
3. **Мониторинг и наблюдаемость**: Заданное значение `max_bandwidth_mb_s` автоматически передается при динамической регистрации на Оркестраторе и отображается в карточке агента в веб-консоли (бейдж с точным ограничением либо статус «без ограничений»).

### 2.3 Развертывание нативного Java 17 Replicator Agent (`agent-java`)

Для кластеров Hadoop, где требуется максимальная производительность, нативная работа с `org.apache.hadoop.fs.FileSystem`, использование Hadoop Delegation Tokens и запуск внутри **Apache Hadoop YARN**, разработан нативный **Java 17 Replicator Agent**.

#### 2.3.1 Сборка JAR-пакета
```bash
# Из корня репозитория
make build-replicator-agent-java
# Формируется автономный Shaded JAR:
# backend/replicator/agent-java/target/replicator-agent-java-1.0.0-all.jar
```

#### 2.3.2 Запуск на нодах Hadoop (DataNode / Edge Nodes) через systemd
Создайте файл `/etc/systemd/system/replicator-agent-java.service`:
```ini
[Unit]
Description=Hadoop gRPC Replicator Java Agent
After=network.target hadoop-hdfs-datanode.service

[Service]
Type=simple
User=hdfs
Group=hadoop
WorkingDirectory=/opt/hadoop-explorer/replicator-java
Environment="JAVA_HOME=/usr/lib/jvm/java-17-openjdk"
Environment="HADOOP_CONF_DIR=/etc/hadoop/conf"
Environment="AGENT_ID=agent-dn-01"
Environment="AGENT_CLUSTER_ID=demo-cluster"
Environment="AGENT_MODE=all"
Environment="ORCHESTRATOR_URL=http://orchestrator.company.local:8005"
Environment="RECEIVER_PORT=50051"
Environment="AGENT_MAX_BANDWIDTH_MB_S=60.0"
Environment="KRB5_KEYTAB=/etc/security/keytabs/hdfs.keytab"
Environment="KRB5_PRINCIPAL=hdfs/_HOST@REALM.LOCAL"

ExecStart=/opt/hadoop-explorer/replicator-java/bin/replicator-agent.sh

Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

#### 2.3.3 Запуск агентов в кластере Apache Hadoop YARN
Replicator Agent включает встроенные `ReplicatorYarnClient` и `ReplicatorApplicationMaster`. При подаче заявки в YARN ApplicationMaster запрашивает у ResourceManager нужное количество контейнеров и запускает репликационные воркеры на узлах NodeManager:

```bash
# Отправка приложения в YARN:
./backend/replicator/agent-java/bin/submit-yarn.sh \
    --cluster_id demo-cluster \
    --orchestrator http://orchestrator.company.local:8005 \
    --num_containers 4 \
    --memory 2048 \
    --vcores 1 \
    --queue default

# Либо стандартным вызовом hadoop jar:
hadoop jar backend/replicator/agent-java/target/replicator-agent-java-1.0.0-all.jar \
    org.apache.hadoop.explorer.replicator.yarn.ReplicatorYarnClient \
    --jar backend/replicator/agent-java/target/replicator-agent-java-1.0.0-all.jar \
    --cluster_id demo-cluster \
    --orchestrator http://orchestrator.company.local:8005 \
    --num_containers 4 \
    --memory 2048 \
    --vcores 1 \
    --queue default
```

**Особенности работы в YARN**:
- Автоматическое распределение контейнеров по разным NodeManager в кластере.
- Автоматическое наследование Hadoop Delegation Tokens для безопасной авторизации в Kerberos HDFS.
- Автоматическая динамическая саморегистрация каждого YARN-контейнера в Оркестраторе (`agent-id: yarn-<container_id>`).
- При падении контейнера ApplicationMaster автоматически запрашивает новый у ResourceManager.

---


## 3. Режим 2: Docker & Docker Compose

### 3.1 Сборка единого Docker-образа
Образ собирается из корня репозитория:
```bash
docker build -t hadoop-explorer/replicator:latest -f docker/Dockerfile.replicator .
```

### 3.2 Полный `docker-compose.yml` (Двунаправленная репликация DC1 ⇄ DC2)
Готовый compose-файл находится в `demo/replicator/docker-compose.yml`:
```yaml
version: "3.8"

services:
  # 1. Orchestrator (Web UI, API, Token Bucket, Scheduler, Metrics)
  orchestrator:
    image: hadoop-explorer/replicator:latest
    container_name: replicator-orchestrator
    restart: unless-stopped
    command: ["uvicorn", "backend.replicator.orchestrator.main:app", "--host", "0.0.0.0", "--port", "8005", "--workers", "2"]
    environment:
      - REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC=104857600 # 100 МБ/с
      - REPLICATOR_DATABASE_URL=sqlite:////app/data/replicator.db
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
    volumes:
      - replicator-data:/app/data
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs:/etc/security/keytabs:ro
    ports:
      - "8005:8005"
    networks:
      - replicator-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8005/health"]
      interval: 15s
      timeout: 5s
      retries: 3

  # 2. Агент DC1 (Primary ЦОД: Москва) — Полный дуплекс (Sender + Receiver)
  agent-dc1:
    image: hadoop-explorer/replicator:latest
    container_name: replicator-agent-dc1
    hostname: agent-dc1
    restart: unless-stopped
    command: ["python", "-m", "backend.replicator.agent"]
    environment:
      - AGENT_ID=agent-dc1
      - AGENT_CLUSTER_ID=demo-cluster
      - AGENT_MODE=all
      - ORCHESTRATOR_URL=http://orchestrator:8005
      - RECEIVER_HOST=0.0.0.0
      - RECEIVER_PORT=50051
      - REPLICATOR_STAGING_DIR=/tmp/staging
      - POLL_INTERVAL_SEC=2.0
    volumes:
      - agent-dc1-staging:/tmp/staging
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs:/etc/security/keytabs:ro
    ports:
      - "50051:50051"
    depends_on:
      - orchestrator
    networks:
      - replicator-net

  # 3. Агент DC2 (DR ЦОД: Санкт-Петербург) — Полный дуплекс (Sender + Receiver)
  agent-dc2:
    image: hadoop-explorer/replicator:latest
    container_name: replicator-agent-dc2
    hostname: agent-dc2
    restart: unless-stopped
    command: ["python", "-m", "backend.replicator.agent"]
    environment:
      - AGENT_ID=agent-dc2
      - AGENT_CLUSTER_ID=backup-cluster
      - AGENT_MODE=all
      - ORCHESTRATOR_URL=http://orchestrator:8005
      - RECEIVER_HOST=0.0.0.0
      - RECEIVER_PORT=50051
      - REPLICATOR_STAGING_DIR=/tmp/staging
      - POLL_INTERVAL_SEC=2.0
    volumes:
      - agent-dc2-staging:/tmp/staging
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/keytabs:/etc/security/keytabs:ro
    ports:
      - "50052:50051"
    depends_on:
      - orchestrator
    networks:
      - replicator-net

volumes:
  replicator-data:
  agent-dc1-staging:
  agent-dc2-staging:

networks:
  replicator-net:
    driver: bridge
```

Запуск:
```bash
docker compose up -d
docker compose ps
docker compose logs -f orchestrator
```

---

## 4. Режим 3: Kubernetes (K8s Manifests & Helm)

### 4.1 Манифесты Kubernetes (Raw Manifests)

#### 1. Namespace, Secret и ConfigMap
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: hadoop-replicator
---
apiVersion: v1
kind: Secret
metadata:
  name: replicator-secrets
  namespace: hadoop-replicator
type: Opaque
stringData:
  jwt-secret-key: "Min32CharSecretKeyForReplicatorTokens"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: replicator-config
  namespace: hadoop-replicator
data:
  REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC: "104857600"
  ORCHESTRATOR_URL: "http://replicator-orchestrator:8005"
  # Опциональный резервный адрес (fallback при отсутствии кластера в топологии):
  FALLBACK_TARGET_ADDRESS: "replicator-agent:50051"
```

#### 2. Deployment и Service Оркестратора
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: replicator-orchestrator
  namespace: hadoop-replicator
spec:
  replicas: 1 # При PostgreSQL можно масштабировать до 2+
  selector:
    matchLabels:
      app: replicator-orchestrator
  template:
    metadata:
      labels:
        app: replicator-orchestrator
    spec:
      containers:
        - name: orchestrator
          image: registry.company.local/hadoop-explorer/replicator:latest
          command: ["uvicorn", "backend.replicator.orchestrator.main:app", "--host", "0.0.0.0", "--port", "8005"]
          envFrom:
            - configMapRef:
                name: replicator-config
          env:
            - name: JWT_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: replicator-secrets
                  key: jwt-secret-key
          ports:
            - containerPort: 8005
          livenessProbe:
            httpGet:
              path: /health
              port: 8005
            initialDelaySeconds: 15
            periodSeconds: 20
          readinessProbe:
            httpGet:
              path: /health
              port: 8005
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "2"
              memory: "2Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: replicator-orchestrator
  namespace: hadoop-replicator
spec:
  type: ClusterIP
  selector:
    app: replicator-orchestrator
  ports:
    - port: 8005
      targetPort: 8005
```

#### 3. Deployment и Service Replicator Agent (Full-Duplex)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: replicator-agent
  namespace: hadoop-replicator
spec:
  replicas: 2 # Горизонтальное масштабирование дуплексных агентов
  selector:
    matchLabels:
      app: replicator-agent
  template:
    metadata:
      labels:
        app: replicator-agent
    spec:
      containers:
        - name: agent
          image: registry.company.local/hadoop-explorer/replicator:latest
          command: ["python", "-m", "backend.replicator.agent"]
          envFrom:
            - configMapRef:
                name: replicator-config
          env:
            - name: AGENT_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: AGENT_MODE
              value: "all"
            - name: RECEIVER_HOST
              value: "0.0.0.0"
            - name: RECEIVER_PORT
              value: "50051"
            - name: REPLICATOR_STAGING_DIR
              value: "/tmp/staging"
          ports:
            - containerPort: 50051
              name: grpc
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
            limits:
              cpu: "4"
              memory: "4Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: replicator-agent
  namespace: hadoop-replicator
spec:
  type: ClusterIP # Либо LoadBalancer при межкластерном K8s-to-K8s доступе
  selector:
    app: replicator-agent
  ports:
    - port: 50051
      targetPort: 50051
      name: grpc
```

---

## 5. Мониторинг и Troubleshooting

### 5.1 Метрики Prometheus (`GET http://orchestrator:8005/metrics`)
- `replication_bytes_total{status="COMPLETED"}` — суммарный объем успешно переданных байт данных.
- `active_workers` — количество зарегистрированных активных воркеров в пуле.
- `replication_jobs_total{status="SCHEDULED|QUEUED|RUNNING|COMPLETED|FAILED|CANCELLED"}` — количество задач репликации по статусам.
- `throttling_delay_seconds_total` — суммарное время принудительной задержки передачи из-за исчерпания сетевых токенов (шейпинг).

### 5.2 Дашборд Grafana (`hadoop_replicator_overview.json`)
Для системы репликации подготовлен официальный дашборд Grafana [`monitoring/grafana/dashboards/hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json):
- 🌊 **Throughput во времени**: мгновенная и средняя скорость передачи чанков по gRPC (MB/s).
- ⚖️ **Token Bucket Throttling**: задержки воркеров из-за исчерпания сетевых квот WAN/ЦОД.
- 👥 **Флот воркеров**: число активных передающих агентов (`active_workers`).
- 📋 **Статусы задач**: распределение RUNNING, QUEUED, SCHEDULED, COMPLETED, FAILED.
- 📦 **Размеры файлов**: гистограмма размеров реплицируемых файлов.
- 🩺 **Ресурсы процесса**: использование CPU и памяти (RSS) оркестратором.

### 5.3 Решение инцидентов (FAQ)

| Ошибка / Симптом | Возможная причина | Решение |
|---|---|---|
| Скорость репликации зафиксирована на низком значении | Срабатывает один из уровней Token Bucket шейпера | Проверьте значения `global_limit_mb_per_sec`, `dc_limits` и `hdfs_limits` в веб-интерфейсе Оркестратора на вкладке «Топология ЦОД и Полоса». Увеличьте лимит или включите чекбокс «Без лимита» (0 МБ/с). |
| `gRPC Unavailable: failed to connect to agent:50051` | Сетевая блокировка между ЦОД или агент не запущен | Проверьте статус пода/сервиса агента репликации и откройте порт `50051` TCP в межсетевом экране (Firewall) между ЦОД. |
| Периодическая задача зависла в статусе `SCHEDULED` | Время следующего запуска `next_run_at` еще не наступило | Статус `SCHEDULED` является штатным режимом ожидания между циклами расписания. Чтобы запустить задачу немедленно, нажмите кнопку `▶` (Старт) в строке таблицы. |
| Переполнение каталога Staging (`No space left on device`) | Большой поток файлов при медленной финализации в HDFS | Увеличьте размер буфера Staging или смонтируйте быстрый NVMe диск в `/var/lib/hadoop-explorer/replicator/staging`. |
| Рост базы данных SQLite при частых cron-запусках | Настроена слишком большая глубина истории | Уменьшите лимит `history_retention_runs` (например, до 10–20 запусков). Старые записи автоматически удаляются функцией прунинга. |

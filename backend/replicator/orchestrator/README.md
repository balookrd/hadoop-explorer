# Hadoop gRPC Replicator :: Orchestrator (Java 21 / Spring Boot 3)

Оркестратор межкластерной репликации данных Apache HDFS на Java 21 LTS и Spring Boot 3.3.4. Аутентификацию, сессии и аудит берет из общего стартера платформы `common-security-starter`.

---

## Компоненты оркестратора

Оркестратор управляет топологией дата-центров, пулом агентов передачи данных (`agent`), распределением сетевых квот (Hierarchical Token Bucket) и жизненным циклом задач репликации.

### Шейпинг трафика (`TokenBucketThrottler`)

- Алгоритм Hierarchical Token Bucket (HTB) ограничивает трафик HDFS-репликации по каналам между ЦОД (агенты запрашивают токены через `RequestTokens`); Kafka-задачи ограничиваются собственным лимитом полосы.
- Три уровня лимитов:
  - Global WAN Pool — общий лимит на весь трафик репликации;
  - DC-to-DC Pool — лимит канала между парой дата-центров (например, `dc1 → dc2`);
  - HDFS-to-HDFS Pool — полоса между парой кластеров.
- Учет на `AtomicLong`; время ожидания — наибольшее из `max(wait_global, wait_dc, wait_hdfs)`.

### Реестр агентов и gRPC Control Plane (`AgentRegistry`, `OrchestratorControlPlaneService`)

- Управляющий gRPC-интерфейс на порту `50050` (HTTP/2).
- Регистрация агентов (`RegisterAgent`), контроль доступности по heartbeat (`Heartbeat`) и graceful-дерегистрация (`UnregisterAgent`).
- Write-behind кэш для heartbeat (`flushDirtyAgentsToDatabase`): heartbeat обновляет состояние агента в памяти, а в БД состояния пишутся пакетом раз в 10 секунд. Так при 300+ агентах каждый heartbeat не превращается в отдельную синхронную транзакцию.
- Агенты, не приславшие heartbeat за таймаут, исключаются из пула.
- Защита от SSRF (Server-Side Request Forgery): запрещена регистрация агентов на loopback, link-local и cloud-metadata адресах (`169.254.169.254`).
- Вызовы агентов аутентифицируются на уровне gRPC через интерцептор заголовка `X-Agent-Secret`.

### Планировщик и жизненный цикл задач (`ReplicationScheduler`, `JobService`, `TaskService`)

- Запуск по расписанию: пресеты интервалов (`@every_5m`, `@every_15m`, `@hourly`, `@daily`) и cron-выражения (`UNIX` / `Spring`).
- История запусков (`JobRunEntity`) и пофайловые задачи (`TaskEntity`): обычные файлы (`FILE`) и бандлы мелких файлов (`BUNDLE_TAR`).
- Распределенная очередь (`claimTasks`): метод `findPendingTasksForClusterSkipLocked` с составным индексом `(status, updated_at)` и блокировкой `SKIP LOCKED`; агенты одного кластера не ждут, пока разберут задачи другого (head-of-line blocking).
- Fast-path в памяти для `claimTasks`: для пустой очереди действует адаптивный кулдаун, который сбрасывается при появлении новых задач (`notifyNewTasksAvailable`). Пока кулдаун действует, пустой ответ агенту возвращается из памяти за $0.001\text{ мс}$ без обращения к СУБД.
- Максимальный номер запуска считается агрегатным запросом (`findMaxRunNumberByJobId`), история запусков в память не загружается ($O(N)$).
- Задачи задания удаляются одним JPQL/SQL-запросом (`deleteByJobId`), без поэлементного удаления.
- Снижение нагрузки на CPU в простое: дебаунс алертов о доступности стримеров (`StreamingLeaseCoordinator`), пропуск SQL-транзакций при пустом буфере прогресса (`TaskService.flushPendingProgress`) и увеличенные интервалы фоновых проверок failover (`TaskService`, `HmsCoordinatorService`).
- Автоматическая сверка (self-healing reconciliation) в `TaskService.flushPendingProgress`: если все объекты задания уже скопированы, задание переводится в `COMPLETED`. Это закрывает гонку параллельных транзакций, из-за которой задание оставалось в `RUNNING`.
- Дедупликация подзадач (`findReusableSubjob` в `JobService` и `createHdfsSubjob` в `OrchestratorControlPlaneService`): при повторе CDC-события используется активная или недавно завершенная подзадача, новая не создается.
- Распределенная блокировка `DistributedLockService.runWithLock("orphan_task_failover_lock", ...)` при проверке и переносе осиротевших задач аварийных агентов (`checkAndFailoverOrphanedTasks`).
- При завершении задач учитывается число файлов в бандле (`fileCount`) в `transferredObjects` / `totalObjects`.
- Очистка устаревших запусков (`cleanupRetentionDays`).
- Метрики задания: процент выполнения, средняя скорость передачи (`MB/s`), время старта и завершения.

### Безопасность через `common-security-starter`

- Аутентификация через JWT в cookie и Kerberos SPNEGO SSO.
- Ролевая модель RBAC: `ADMIN` управляет всеми задачами организации, `USER` / `WRITER` видят только свои задачи.
- Защита от BOLA/IDOR при чтении задачи (`GET /api/v1/jobs/{jobId}`): доступ есть у создателя задачи, администратора и агента кластера (`checkJobReadAccess`).
- Защита от CSRF для изменяющих запросов API.
- Аудит операций через `@Audited`. Ограничение частоты запросов (`RateLimitFilter` стартера, Token Bucket): вход (`/api/v1/auth/login`, `/api/v1/auth/sso`) считается по IP клиента, остальные запросы к API — по пользователю, анонимные — по IP. Сверх лимита — HTTP 429 с заголовком `Retry-After`; параметры — `docs/CONFIGURATION.md`, раздел 2.5.

### Метрики Prometheus и Micrometer (`ReplicatorMetricsService`)

- Эндпоинт `/metrics` для Prometheus.
- Сжатие трафика: `replicator_compression_raw_bytes_total`, `replicator_compression_compressed_bytes_total`, `replicator_compression_ratio`.
- Гистограмма задержек передачи gRPC-чанков с SLA-бакетами от 1 мс до 5 с: `replicator_grpc_chunk_transfer_latency_seconds_bucket`, `_sum`, `_count`, а также `replicator_grpc_chunk_transfer_latency_avg_ms` и `max_ms`.
- Очередь и буфер staging на целевых кластерах: `replicator_staging_queue_size`, `replicator_staging_active_files`, `replicator_staging_active_bytes`, `replicator_staging_orphans_cleaned_total`.

### Создание топиков Kafka (`KafkaTopicProvisioner`, `KafkaReplicationOrchestratorService`)

- Перед запуском репликации проверяется наличие топиков на целевом кластере Kafka.
- Отсутствующие топики создаются с числом партиций (`numPartitions`) и фактором репликации (`replicationFactor`) исходного топика.
- Если топик уже есть, но партиций в нем меньше, на целевом брокере добавляются партиции (`createPartitions`), чтобы ключи попадали в партиции с теми же номерами.
- Двунаправленная (`BIDIRECTIONAL`) и однонаправленная (`UNIDIRECTIONAL`) репликация.
- `KafkaAdminClientFactory` использует изолированный in-memory JAAS-контекст Kerberos и graceful fallback при временной недоступности брокера.

---

## Конфигурация (`application.yml`)

```yaml
server:
  port: 8005

hadoop:
  security:
    auth:
      mode: mock # mock, ldap, kerberos
      admin-groups:
        - "hadoop-admins"
        - "superusers"
    jwt:
      secret-key: "replicator-orchestrator-secret-key-32-chars-minimum!"
      expiration-minutes: 480

  replicator:
    grpc:
      port: 50050 # gRPC Control Plane порт
      enabled: true
      tls-enabled: false
    global-limit-bytes-per-sec: 104857600 # 100 MB/s
    agent-secret: "replicator-secure-agent-secret-key-12345"
    agent-heartbeat-timeout-seconds: 15
    agent-offline-timeout-seconds: 45
    seed-demo-data: false # Сидирование демонстрационных задач при старте (по умолчанию false)
    small-file-threshold-bytes: 1048576 # 1 MB: порог мелких файлов
    bundle-target-size-bytes: 16777216 # 16 MB: целевой размер бандла
    max-bundle-files: 500 # макс. файлов в одном бандле
    bundle-commit-concurrency: 8 # параллельные потоки записи в HDFS
    streaming:
      enabled: true
      max-pending-tasks-threshold: 5000 # порог неразобранных задач для Backpressure
      inotify-queue-capacity: 10000 # емкость очереди Inotify-задач
      inotify-high-watermark: 0.8 # верхний порог гистерезиса (80%)
      inotify-low-watermark: 0.3 # нижний порог гистерезиса (30%)
      inotify-max-batch-size: 500 # макс. размер микробатча
      inotify-flush-interval-ms: 1000 # интервал периодического сброса (мс)
      inotify-backpressure-max-delay-ms: 5000 # макс. задержка опроса при Backpressure (мс)
    datacenters:
      - id: "dc1"
        name: "Дата-Центр 1 (Primary DC)"
        network-zone: "zone-a"
      - id: "dc2"
        name: "Дата-Центр 2 (Disaster Recovery)"
        network-zone: "zone-b"
    clusters:
      - id: "dc1"
        name: "HDFS DC1 Production"
        dc-id: "dc1"
        hdfs-rpc-address: "hdfs://namenode-dc1:8020"
      - id: "dc2"
        name: "HDFS DC2 Standby"
        dc-id: "dc2"
        hdfs-rpc-address: "hdfs://namenode-dc2:8020"
```

---

## Сборка и тесты

### Требования
- JDK 21 LTS. На другой версии JDK сборку модулей репликатора останавливает `maven-enforcer-plugin`.
- Apache Maven 3.9+

### Сборка и тесты модуля
```bash
# Тестирование модуля оркестратора
mvn test -f backend/replicator/pom.xml -pl orchestrator -am

# Сборка исполняемого Spring Boot JAR
mvn clean package -DskipTests -f backend/replicator/pom.xml -pl orchestrator -am
```

Запускайте Maven только через реактор `backend/replicator/pom.xml` с `-pl orchestrator -am`. При запуске с `-f backend/replicator/orchestrator/pom.xml` общие модули `proto` и `model` берутся из локального Maven-репозитория и могут быть устаревшими. Цели `make test-replicator-orchestrator` и `make build-replicator-orchestrator` вызывают Maven именно так.

### Цели Makefile
```bash
# Тесты только оркестратора
make test-replicator-orchestrator

# Тесты всего Replicator (Agent + Orchestrator)
make test-replicator

# Полный прогон всех Java тестов платформы
make test-java
```

---

## gRPC Control Plane API (`orchestrator_control_plane.proto`)

Протокол обмена агентов и стримеров с оркестратором: порт `50050`, HTTP/2, аутентификация заголовком `X-Agent-Secret`.

| Категория | RPC-метод | Запрос / ответ | Назначение |
|---|---|---|---|
| Агенты | `RegisterAgent` | `AgentRegisterRequest` / `AgentRegisterResponse` | Регистрация агента и его сетевых возможностей |
| | `Heartbeat` | `AgentHeartbeatRequest` / `AgentHeartbeatResponse` | Продление аренды, отправка телеметрии и загрузки |
| | `UnregisterAgent` | `AgentUnregisterRequest` / `AgentUnregisterResponse` | Вывод агента из пула (graceful shutdown) |
| Шейпинг WAN | `RequestTokens` | `TokenRequest` / `TokenResponse` | Запрос квоты полосы пропускания (Hierarchical Token Bucket) |
| Задачи HDFS | `ClaimTasks` | `ClaimTasksRequest` / `ClaimTasksResponse` | Забор пачки доступных пофайловых задач свободным воркером |
| | `CompleteTask` | `CompleteTaskRequest` / `CompleteTaskResponse` | Подтверждение передачи файла или бандла |
| | `FailTask` | `FailTaskRequest` / `FailTaskResponse` | Фиксация сбоя исполнения задачи для перераспределения |
| | `BatchCreateTasks`| `BatchCreateTasksRequest` / `BatchCreateTasksResponse` | Пакетная регистрация найденных файлов на репликацию |
| Inotify CDC | `RenewStreamingLease` | `StreamingLeaseRenewRequest` / `StreamingLeaseRenewResponse` | Продление лизинга лидерства стримера Inotify |
| | `ReportInotifyProgress` | `InotifyProgressReportRequest` / `InotifyProgressReportResponse` | Фиксация последнего обработанного `txid` NameNode |
| Hive Metastore | `ClaimHmsTasks` | `ClaimHmsTasksRequest` / `ClaimHmsTasksResponse` | Забор подзадач репликации метаданных HMS |
| | `ReportHmsProgress` | `HmsProgressReportRequest` / `HmsProgressReportResponse` | Промежуточный отчет о прогрессе синхронизации схем |
| | `ReportHmsTaskResult` | `HmsTaskResultRequest` / `HmsTaskResultResponse` | Фиксация результата обработки метаданных таблицы/партиции |
| | `AcquireHmsJobLease` | `AcquireHmsJobLeaseRequest` / `AcquireHmsJobLeaseResponse` | Захват эксклюзивного лизинга на репликацию схемы |
| | `RenewHmsJobLease` | `RenewHmsJobLeaseRequest` / `RenewHmsJobLeaseResponse` | Продление эксклюзивного лизинга схемы HMS |

---

## REST API (веб-интерфейс и администрирование)

| Метод | Путь | Назначение | Доступ |
|---|---|---|---|
| `GET` | `/health`, `/healthz` | Проверка жизнеспособности сервиса | Публичный |
| `GET` | `/api/v1/health` | То же, что `/health` | Любой аутентифицированный пользователь или агент (`X-Agent-Secret`) |
| `GET` | `/metrics` | Метрики Prometheus (Replicator + Micrometer) | Любой аутентифицированный пользователь или агент (`X-Agent-Secret`) |
| `GET` | `/actuator/prometheus`, `/actuator/health`, `/actuator/info` | Эндпоинты Spring Boot Actuator | Публичный |
| `GET` | `/actuator/metrics` | Системные метрики Spring Boot Actuator. В профиле `prod` не публикуется (`management.endpoints.web.exposure.include: health,info,prometheus`) | Роль `ADMIN` |
| `POST` | `/api/v1/auth/login` | Аутентификация пользователя (JWT Cookie) | Публичный |
| `GET` | `/api/v1/agents` | Список активных агентов репликации | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/jobs` | Список задач репликации | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/jobs` | Создание новой задачи репликации | JWT (`ADMIN` / `WRITER`) |
| `GET` | `/api/v1/jobs/{id}` | Детальная информация по задаче | JWT (`USER` / `ADMIN`) |
| `PUT` | `/api/v1/jobs/{id}` | Редактирование параметров задачи | JWT (`ADMIN` / `WRITER`) |
| `PATCH` | `/api/v1/jobs/{id}` | Обновление статуса и прогресса | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/jobs/{id}/start` | Запуск задачи репликации | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/jobs/{id}/stop` | Остановка выполнения задачи | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/jobs/{id}/cancel` | Отмена выполнения задачи | JWT (`ADMIN` / `WRITER`) |
| `DELETE` | `/api/v1/jobs/{id}` | Удаление задачи | JWT (`ADMIN`) |
| `GET` | `/api/v1/jobs/{id}/runs` | История запусков задачи | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/jobs/{id}/tasks` | Постраничный просмотр пофайловых задач | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/tasks/queue-status` | Проверка насыщения очереди пула задач (Backpressure) | `X-Agent-Secret` / JWT |
| `GET` | `/api/v1/streaming/lease/status` | Статус распределенного лидерства потокового слушателя Inotify | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/dr/status` | Телеметрия Disaster Recovery и лаг непереданных данных | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/dr/emergency-stop` | Экстренная остановка (kill switch) и сетевое ограждение (fencing) | JWT (`ADMIN`) |
| `POST` | `/api/v1/dr/emergency-stop/rollback` | Откат аварийной изоляции и снятие ограждения | JWT (`ADMIN`) |
| `POST` | `/api/v1/dr/reverse` | Генерация зеркальных задач обратной репликации (`DC2 → DC1`) | JWT (`ADMIN`) |
| `POST` | `/api/v1/dr/jobs/{id}/undo-reverse` | Отзыв обратного зеркала и возобновление штатного цикла | JWT (`ADMIN`) |
| `GET` | `/api/v1/hms/jobs` | Список задач репликации Hive Metastore | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/hms/jobs` | Создание новой задачи репликации схемы HMS | JWT (`ADMIN` / `WRITER`) |
| `GET` | `/api/v1/topology` | Топология дата-центров, кластеров и активные лимиты | JWT (`USER` / `ADMIN`) |
| `PUT` | `/api/v1/tokens/limit` | Изменение лимитов полосы пропускания на лету | JWT (`ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/jobs` | Список задач межкластерной репликации Kafka | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/replicator/kafka/jobs` | Регистрация задачи репликации Kafka с автосозданием топиков | JWT (`ADMIN` / `WRITER`) |
| `GET` | `/api/v1/replicator/kafka/jobs/{id}` | Детали задачи репликации Kafka | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/provision-topics` | Предварительная проверка и автосоздание/расширение топиков | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/pause` | Приостановка репликации Kafka | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/resume` | Возобновление репликации Kafka | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/failover` | Запуск failover консьюмеров | JWT (`ADMIN`) |
| `DELETE` | `/api/v1/replicator/kafka/jobs/{id}` | Удаление задачи репликации Kafka | JWT (`ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/discovery/clusters` | Обнаружение доступных кластеров Kafka в ЦОД | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/discovery/topics` | Обнаружение топиков для автокомплита в UI | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/discovery/groups` | Обнаружение консьюмер-групп в ЦОД | JWT (`USER` / `ADMIN`) |

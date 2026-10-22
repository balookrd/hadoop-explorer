# Hadoop gRPC Replicator :: Orchestrator (Java 21 / Spring Boot 3)

Высокопроизводительный оркестратор межкластерной репликации данных Apache HDFS на стеке **Java 21 LTS** и **Spring Boot 3.3.4**, полностью интегрированный с общим ядром безопасности платформы `common-security-starter`.

---

## 1. Архитектура и функциональные возможности

Оркестратор репликации управляет топологией дата-центров, пулом агентов передачи данных (`agent`), распределением сетевых квот (Hierarchical Token Bucket) и жизненным циклом задач репликации.

### Основные компоненты

1. **Многоуровневый шейпинг сетевого трафика (`TokenBucketThrottler`)**:
   - Реализует алгоритм **Hierarchical Token Bucket (HTB)** для предотвращения перегрузки каналов передачи данных.
   - Трехуровневая иерархия лимитов:
     - **Global WAN Pool**: общий глобальный лимит всей инфраструктуры репликации;
     - **DC-to-DC Pool**: ограничение магистральных каналов между парами дата-центров (например, `dc1 ➔ dc2`);
     - **HDFS-to-HDFS Pool**: выделенная полоса между парами кластеров.
   - Потокобезопасный учет на базе `AtomicLong` с расчетом узкого горлышка `max(wait_global, wait_dc, wait_hdfs)`.

2. **Реестр агентов и gRPC Control Plane (`AgentRegistry`, `OrchestratorControlPlaneService`)**:
   - Высокопроизводительный управляющий gRPC интерфейс (порт `50050`, HTTP/2).
   - Автоматическая регистрация агентов (`RegisterAgent`), мониторинг доступности по heartbeat (`Heartbeat`) и graceful дерегистрация (`UnregisterAgent`).
   - Write-Behind Cache для heartbeat (`flushDirtyAgentsToDatabase`): периодический keepalive обновляет состояние агентов в оперативной памяти за микросекунды, а сохранение в БД выполняется пакетно раз в 10 секунд, исключая шторм синхронных транзакций при 300+ агентах.
   - Автоматическое исключение офлайн-агентов по таймауту.
   - Защита от SSRF (Server-Side Request Forgery): блокировка регистрации агентов на loopback, link-local и cloud-metadata адреса (`169.254.169.254`).
   - Аутентификация вызовов агентов на уровне gRPC L7 через интерцептор заголовка `X-Agent-Secret`.

3. **Планировщик и жизненный цикл задач (`ReplicationScheduler`, `JobService`, `TaskService`)**:
   - Поддержка запуска по расписанию: интервальные пресеты (`@every_5m`, `@every_15m`, `@hourly`, `@daily`) и Cron-выражения (`UNIX` / `Spring`).
   - Ведение истории запусков (`JobRunEntity`) и детализация пофайловых задач (`TaskEntity`), включая обычные файлы (`FILE`) и бандлы упакованных мелких файлов (`BUNDLE_TAR`).
   - Оптимизация распределенной очереди (`claimTasks`): метод `findPendingTasksForClusterSkipLocked` с составным индексом `(status, updated_at)` и блокировкой `SKIP LOCKED`, предотвращающий Head-of-Line межкластерное голодание агентов.
   - In-Memory Fast-Path для распределенной очереди (`claimTasks`): адаптивный кулдаун пустых очередей с немедленной инвалидацией при появлении новых задач (`notifyNewTasksAvailable`), возвращающий пустые ответы агентам прямо из оперативной памяти за $0.001\text{ мс}$ без блокировок СУБД.
   - Агрегатное вычисление максимального номера запуска (`findMaxRunNumberByJobId`), исключающее выгрузку истории запусков $O(N)$ в память.
   - Пакетное атомарное удаление задач в БД (`deleteByJobId`) единым JPQL/SQL запросом без поэлементного сканирования.
   - Энергоэффективность и фоновая оптимизация: дебаунс алертов высокой доступности стримеров (`StreamingLeaseCoordinator`), ленивая реконсиляция прогресса без паразитных SQL-транзакций при пустом буфере (`TaskService.flushPendingProgress`) и оптимизированные интервалы фоновых проверок failover (`TaskService`, `HmsCoordinatorService`), снижающие нагрузку на CPU на холостом ходу.
   - Самовосстанавливающаяся авто-реконсиляция (Self-Healing Reconciliation) в `TaskService.flushPendingProgress`: периодическая фоновая сверка и гарантированная финализация заданий в статус `COMPLETED`, когда все объекты уже скопированы пулом воркеров, устраняющая race condition параллельных транзакций.
   - Дедупликация и защита от шторма подзадач (`findReusableSubjob` в `JobService` и `createHdfsSubjob` в `OrchestratorControlPlaneService`): повторное использование активных или недавно завершенных подзадач при ретраях CDC-событий, исключающее лавинообразное пложение одинаковых задач при таймаутах.
   - Распределенный замок `DistributedLockService.runWithLock("orphan_task_failover_lock", ...)` при проверке и миграции осиротевших задач аварийных агентов (`checkAndFailoverOrphanedTasks`).
   - Корректный учет числа упакованных объектов (`fileCount`) в бандлах при завершении задач (`transferredObjects` / `totalObjects`).
   - Автоматическая очистка устаревших запусков (`cleanupRetentionDays`).
   - Расчет метрик: процент выполнения, средняя скорость передачи (`MB/s`), время старта и завершения.

4. **Интеграция с `common-security-starter`**:
   - Централизованная аутентификация через cookie JWT и Kerberos SPNEGO SSO.
   - Ролевая модель RBAC (`ADMIN` — полный контроль задач организации, `USER` / `WRITER` — изоляция задач конкретного пользователя).
   - Защита от BOLA / IDOR в чтении задач (`GET /api/v1/jobs/{jobId}`): проверка прав доступа создателя задачи, администратора или агента кластера (`checkJobReadAccess`).
   - Защита от CSRF-атак на мутирующие запросы API.
   - Встроенный аудит критических операций `@Audited` и Rate Limiter (Token Bucket per IP).

5. **Enterprise-мониторинг Prometheus и Micrometer (`ReplicatorMetricsService`)**:
   - Единый гармонизированный эндпоинт `/metrics` для сбора метрик скрейпером Prometheus.
   - Метрики эффективности сжатия на лету: `replicator_compression_raw_bytes_total`, `replicator_compression_compressed_bytes_total`, `replicator_compression_ratio`.
   - Гистограмма задержек gRPC передачи чанков с SLA-бакетами от 1 мс до 5 с: `replicator_grpc_chunk_transfer_latency_seconds_bucket`, `_sum`, `_count`, а также `replicator_grpc_chunk_transfer_latency_avg_ms` и `max_ms`.
   - Мониторинг очереди и буфера Staging на целевых кластерах: `replicator_staging_queue_size`, `replicator_staging_active_files`, `replicator_staging_active_bytes`, `replicator_staging_orphans_cleaned_total`.

6. **Оркестрация и автосоздание Kafka-топиков (`KafkaTopicProvisioner`, `KafkaReplicationOrchestratorService`)**:
   - Автоматическая предварительная проверка наличия топиков на целевом кластере Kafka перед запуском репликации.
   - Создание отсутствующих топиков с полным сохранением топологии партиционирования исходного топика (`numPartitions`, фактор репликации `replicationFactor`).
   - Автоматическое расширение партиций (`createPartitions`) на целевом брокере, если топик уже существовал, но имел меньшее число партиций, исключая рассинхронизацию ключей и партиций.
   - Поддержка двунаправленной (`BIDIRECTIONAL`) и однонаправленной (`UNIDIRECTIONAL`) репликации.
   - Безопасная работа через `KafkaAdminClientFactory` с изолированным in-memory JAAS Kerberos контекстом и graceful fallback при временной недоступности брокера.

---

## 2. Конфигурация (`application.yml`)

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

## 3. Сборка и тестирование

### Требования
- JDK 21+
- Apache Maven 3.9+

### Сборка и тесты модуля
```bash
# Тестирование модуля оркестратора
mvn test -f backend/replicator/orchestrator/pom.xml

# Сборка исполняемого Spring Boot JAR
mvn clean package -DskipTests -f backend/replicator/orchestrator/pom.xml
```

### Использование Makefile
```bash
# Тесты только оркестратора
make test-replicator-orchestrator

# Тесты всего Replicator (Agent + Orchestrator)
make test-replicator

# Полный прогон всех Java тестов платформы
make test-java
```

---

## 4. gRPC Control Plane API (`orchestrator_control_plane.proto`)

Протокол взаимодействия Агентов и Стримеров с Оркестром (порт `50050`, HTTP/2, заголовок `X-Agent-Secret`).

| Категория | RPC Метод | Запрос / Ответ | Назначение |
|---|---|---|---|
| **Агенты** | `RegisterAgent` | `AgentRegisterRequest` / `AgentRegisterResponse` | Регистрация агента и его сетевых возможностей |
| | `Heartbeat` | `AgentHeartbeatRequest` / `AgentHeartbeatResponse` | Продление аренды, отправка телеметрии и загрузки |
| | `UnregisterAgent` | `AgentUnregisterRequest` / `AgentUnregisterResponse` | Корректный вывод агента из пула (graceful shutdown) |
| **Шейпинг WAN** | `RequestTokens` | `TokenRequest` / `TokenResponse` | Запрос квоты полосы пропускания (Hierarchical Token Bucket) |
| **Задачи HDFS** | `ClaimTasks` | `ClaimTasksRequest` / `ClaimTasksResponse` | Забор пачки доступных пофайловых задач свободным воркером |
| | `CompleteTask` | `CompleteTaskRequest` / `CompleteTaskResponse` | Подтверждение успешной передачи файла/бандла |
| | `FailTask` | `FailTaskRequest` / `FailTaskResponse` | Фиксация сбоя исполнения задачи для перераспределения |
| | `BatchCreateTasks`| `BatchCreateTasksRequest` / `BatchCreateTasksResponse` | Пакетная регистрация выявленных файлов на репликацию |
| **Inotify CDC** | `RenewStreamingLease` | `StreamingLeaseRenewRequest` / `StreamingLeaseRenewResponse` | Продление лизинга лидерства стримера Inotify |
| | `ReportInotifyProgress` | `InotifyProgressReportRequest` / `InotifyProgressReportResponse` | Фиксация последнего обработанного `txid` NameNode |
| **Hive Metastore** | `ClaimHmsTasks` | `ClaimHmsTasksRequest` / `ClaimHmsTasksResponse` | Забор подзадач репликации метаданных HMS |
| | `ReportHmsProgress` | `HmsProgressReportRequest` / `HmsProgressReportResponse` | Промежуточный отчет о прогрессе синхронизации схем |
| | `ReportHmsTaskResult` | `HmsTaskResultRequest` / `HmsTaskResultResponse` | Фиксация результата обработки метаданных таблицы/партиции |
| | `AcquireHmsJobLease` | `AcquireHmsJobLeaseRequest` / `AcquireHmsJobLeaseResponse` | Захват эксклюзивного лизинга на репликацию схемы |
| | `RenewHmsJobLease` | `RenewHmsJobLeaseRequest` / `RenewHmsJobLeaseResponse` | Продление эксклюзивного лизинга схемы HMS |

---

## 5. REST API Эндпоинты (Web UI и Администрирование)

| Метод | Путь | Описание | Доступ |
|---|---|---|---|
| `GET` | `/health`, `/api/v1/health` | Проверка жизнеспособности сервиса | Публичный |
| `GET` | `/metrics` | Метрики Prometheus (Replicator + Micrometer) | Публичный |
| `GET` | `/actuator/metrics` | Системные метрики Spring Boot Actuator | Публичный |
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
| `POST` | `/api/v1/dr/emergency-stop` | Экстренная остановка (Kill-Switch) и сетевое ограждение (Fencing) | JWT (`ADMIN`) |
| `POST` | `/api/v1/dr/emergency-stop/rollback` | Откат аварийной изоляции и безопасное снятие ограждения | JWT (`ADMIN`) |
| `POST` | `/api/v1/dr/reverse` | Генерация зеркальных задач обратной репликации (`DC2 ➔ DC1`) | JWT (`ADMIN`) |
| `POST` | `/api/v1/dr/jobs/{id}/undo-reverse` | Отзыв обратного зеркала и возобновление штатного цикла | JWT (`ADMIN`) |
| `GET` | `/api/v1/hms/jobs` | Список задач репликации Hive Metastore | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/hms/jobs` | Создание новой задачи репликации схемы HMS | JWT (`ADMIN` / `WRITER`) |
| `GET` | `/api/v1/topology` | Топология дата-центров, кластеров и активные лимиты | JWT (`USER` / `ADMIN`) |
| `PUT` | `/api/v1/tokens/limit` | Динамическое изменение лимитов полосы пропускания | JWT (`ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/jobs` | Список задач межкластерной репликации Kafka | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/replicator/kafka/jobs` | Регистрация задачи репликации Kafka с автосозданием топиков | JWT (`ADMIN` / `WRITER`) |
| `GET` | `/api/v1/replicator/kafka/jobs/{id}` | Детали задачи репликации Kafka | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/provision-topics` | Предварительная проверка и автосоздание/расширение топиков | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/pause` | Приостановка репликации Kafka | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/resume` | Возобновление репликации Kafka | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/replicator/kafka/jobs/{id}/failover` | Инициирование Failover консьюмеров | JWT (`ADMIN`) |
| `DELETE` | `/api/v1/replicator/kafka/jobs/{id}` | Удаление задачи репликации Kafka | JWT (`ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/discovery/clusters` | Обнаружение доступных кластеров Kafka в ЦОД | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/discovery/topics` | Обнаружение топиков для автокомплита в UI | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/replicator/kafka/discovery/groups` | Обнаружение консьюмер-групп в ЦОД | JWT (`USER` / `ADMIN`) |

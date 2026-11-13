# Hadoop gRPC Replicator

Сервис межкластерной репликации данных HDFS (DC1 → DC2 → DC3) через WAN с многоуровневым контролем полосы пропускания (Hierarchical Token Bucket), gRPC-транспортом, изоляцией Kerberos-контекстов и веб-интерфейсом в дизайн-системе HDFS Explorer.

---

## Устройство и функции сервиса

### Топология дата-центров и кластеров HDFS

- Привязка каждого кластера HDFS к конкретному ЦОД (на стенде: 2 ЦОД — в DC1 два кластера HDFS, в DC2 один кластер HDFS).
- Настройка каналов и квот:
  - DC-DC limits: лимит межЦОДного канала (`DC1 (Москва) → DC2 (СПб)`: 100 МБ/с).
  - HDFS-HDFS limits: полосы между парами кластеров (`demo-cluster → backup-cluster`: 60 МБ/с, `analytics-cluster → backup-cluster`: 40 МБ/с, внутри DC1: 80 МБ/с).
  - Global WAN Pool: общий лимит на весь трафик репликации.

### Иерархический шейпинг трафика (Hierarchical Token Bucket)

- При передаче чанка воркер запрашивает сетевую квоту. Оркестратор берет наибольшее ожидание `max(wait_global, wait_dc_dc, wait_hdfs_hdfs)` и одновременно списывает токены во всех задействованных корзинах.

### Аутентификация и ролевой доступ (RBAC)

- Вход через Kerberos SPNEGO SSO, LDAP и тестовые профили (`admin_user`, `de_user`, `analyst_user`).
- Администратор (`ADMIN` / `admin_user`): видит все задачи репликации всех пользователей и управляет ими, может указать произвольного пользователя для doAs-имперсонации (`execution_principal`) в разделах HDFS и HMS.
- Пользователь (`USER` / `de_user`, `analyst_user`): видит только свои задачи и системные задачи технической учетной записи; поле пользователя имперсонации заполнено логином создателя и не редактируется.

### Планировщик задач (cron)

- Периодическая синхронизация по расписанию (`@every_5m`, `@hourly`, `@daily`, custom cron).
- Фоновый демон рассчитывает `next_run_at` и ставит задачу в очередь.

### Kerberos proxy user и doAs-имперсонация (Apache Ranger)

- Аутентификация в KDC по keytab системной технической учетной записи (`hdfs-replicator@REALM.LOCAL`).
- doAs-имперсонация конечного пользователя (`impersonate_user` / `execution_principal`) через `org.apache.hadoop.security.UserGroupInformation`.
- Управление пользователем имперсонации в UI и REST API:
  - В разделе HDFS (создание и редактирование задач) и в разделе HMS (создание репликации схем) администраторам доступно редактирование имени пользователя (`username` или Kerberos principal).
  - Для непривилегированных пользователей поле заблокировано (read-only) и всегда показывает автора задачи (`session.username()`); сервер отклоняет запрос с подмененным значением.
  - При репликации HMS схем указанный пользователь передается во все вложенные HDFS-подзадачи переноса данных (`HMS_SUBJOB`).
- Политики Apache Ranger применяются к инициатору задачи, в Ranger Audit Log операция записывается как `ugi: user (auth:PROXY via hdfs-replicator)`.

### Веб-консоль в стиле HDFS Explorer

- Единая палитра `bg-slate-950` / `bg-slate-900` / `border-slate-800` / `text-slate-100`.
- Окно входа повторяет `LoginModal.svelte`.
- Селектор HDFS кластеров в хедере и форме создания задач с отображением ЦОД.

### Полнодуплексный агент (Java 21)

- Сервис на Java 21 LTS (`agent`) объединяет функции Sender и Receiver.
- Поддерживает двунаправленную репликацию (`DC1 ⇄ DC2`), failback после аварии и топологии из нескольких ЦОД.
- Режимы работы (`AGENT_MODE`):
  - `all` (по умолчанию) — полный дуплекс: параллельный прием входящих файлов по gRPC (:50051) и отправка исходящих задач из очереди оркестратора;
  - `sender` — только отправка исходящих файлов (совместимость с воркером);
  - `receiver` — только прием входящих файлов (совместимость с ресивером).
- Целевой gRPC-узел (`target_address`) выбирается по топологии кластеров оркестратора.

### Хранилище состояния и метаданных

- База данных оркестратора (H2 или PostgreSQL) через Spring Data JPA.
- Хранит задачи, топологию и историю всех запусков (`JobRun`).

### Локальное ограничение полосы агента (`AGENT_MAX_BANDWIDTH_MB_S`)

- Ограничивает трафик агента, установленного на DataNode или edge-узле, чтобы он не вытеснял трафик рабочих задач Spark и YARN.
- На отправке (Sender) — асинхронный Token Bucket, на приеме (Receiver) — backpressure через TCP flow control.
- Фактический лимит узлов показывается в веб-консоли оркестратора.

### Агент на Java 21 (`agent`)

- Запускается на узлах Hadoop (DataNode, edge-узел) и в контейнерах YARN.
- Работает с HDFS через `org.apache.hadoop.fs.FileSystem` и Kerberos UGI / YARN Delegation Tokens.
- `ReplicatorYarnClient` и `ReplicatorApplicationMaster` развертывают пула агентов в кластере YARN (`yarn jar replicator-agent-1.0.0-all.jar ...`).
- Подробная документация: [`agent/README.md`](agent/README.md).

### Оркестратор на Java 21 и Spring Boot 3 (`orchestrator`)

- Оркестратор репликации на Java 21 LTS и Spring Boot 3.3.4.
- Мультимодульный Maven-проект (`pom.xml`) объединяет общие контракты (`proto`, `model`), агент (`agent`) и оркестратор (`orchestrator`).
- Использует общий стартер безопасности `zastava` (SPNEGO SSO, LDAP, JWT, CSRF, Rate Limiting, Audit).
- Шейпинг Hierarchical Token Bucket, реестр агентов с защитой от SSRF и планировщик периодических репликаций.
- Раздает собранный SPA на Svelte 5.
- Подробная документация: [`orchestrator/README.md`](orchestrator/README.md).

### Упаковка мелких файлов в TAR-поток (small files bundling)

- Для каталогов с миллионами мелких файлов (5–50 КБ): стриминговые логи, CDC, витрины. Сокращает операции ввода-вывода на локальных дисках и число транзакций NameNode.
- Файлы размером меньше `smallFileThresholdBytes` (по умолчанию 1 МБ) группируются в виртуальные бандлы `BUNDLE_TAR` размером около 16 МБ (до 500 файлов в одной задаче).
- Без промежуточного сохранения: поток упаковывается на лету в виртуальный TAR (`TarArchiveOutputStream`) и передается через gRPC (`TransferTarStream`) без записи на локальный диск агента.
- Приемник распаковывает поток (`TarArchiveInputStream`) и параллельно пишет файлы в целевой HDFS пулом потоков (`bundleCommitConcurrency`, по умолчанию 8 потоков) под Kerberos UGI `doAs`.
- Коммит через HDFS-staging и двухуровневый кэш родительских каталогов (PERF-04): при потоковой распаковке бандла `DataTransferServiceImpl` пишет каждый файл в `targetPath._staging_<jobId>` (`HadoopFsManager.writeDirect`, `fs.create(path, true)`), затем переносит его в целевой путь `HadoopFsManager.renameFile`. На HDFS перенос выполняется одним вызовом `DistributedFileSystem.rename(src, dst, Options.Rename.OVERWRITE)`. Перед записью `ensureParentDirectoryExists` проверяет родительский каталог сначала по кэшу бандла (`sessionVerifiedDirs`), затем по глобальному LRU-кэшу на 5000 путей (`verifiedDirectoriesGlobalCache`); `fs.exists`/`fs.mkdirs` к NameNode вызываются только для каталога, которого нет ни в одном кэше.
- Для крупных одиночных файлов (`FILE`) локальный staging также заменен на прямой HDFS-staging (`targetPath + "._staging_" + jobId"`) с последующим атомарным `fs.rename()`; локальный диск агента не используется.

### Очистка staging-файлов на HDFS (staging reaper)

- Удаляет брошенные временные файлы на целевой HDFS при обрывах gRPC сессий, WAN-сбоях или внезапной гибели процессов агентов (`kill -9`, OOM).
- Реактивная очистка: удаление незавершенного `stagingPath` или всех файлов бандла в `cleanup()` при получении `onError` или исключениях коммита.
- Предстартовая очистка: удаление незавершенных staging файлов предыдущей попытки перед повторным запуском задачи.
- Фоновый TTL Reaper: демон `hdfs-staging-cleaner` периодически (по умолчанию каждые 15 минут) удаляет осиротевшие staging-файлы старше 30 минут по всем известным целевым директориям.
- Фильтрация в манифестах: staging-файлы исключены из манифестов каталогов и in-memory diff, поэтому не реплицируются повторно.

### Сжатие трафика в gRPC-канале (Zstandard, LZ4)

- Сжатие чанков файлов (`TransferFile`) и виртуальных TAR-стримов (`TransferTarStream`) алгоритмами Zstandard (`zstd`) и LZ4 (`lz4`).
- На текстовых логах, JSON, CSV и дампах БД экономит 40–80% полосы межЦОДного WAN.
- Пропуск сжатых форматов: для файлов Parquet, ORC, Snappy, Gz, Zip, Avro сжатие отключается.
- Fallback: данные отправляются в несжатом виде (`COMPRESSION_NONE`), если сжатие конкретного чанка не уменьшает его размер.
- Учет WAN-полосы: токены Hierarchical Token Bucket списываются по фактически переданным сжатым байтам, а SHA-256 считается по исходному файлу.

### Отказоустойчивость оркестратора и агентов

- Резервирование оркестратора (Active-Active Stateless API + ShedLock + HA PostgreSQL Patroni).
- Клиентская балансировка и Failover без внешнего L7 LB (`ORCHESTRATOR_GRPC_ADDRESSES`): агенты принимают список адресов инстансов оркестратора через запятую (`orch-1:50050,orch-2:50050,orch-3:50050`). При старте агент перемешивает адреса в случайном порядке (client-side random shuffle), и агенты распределяются по Active-Active инстансам равномерно. При отказе узла (`UNAVAILABLE`, обрыв соединения) клиент переключается на следующий доступный узел оркестратора; логика репликации этого не замечает.
- Персистентный реестр агентов в БД (`replication_agents`): синхронизация регистрации, heartbeat и дерегистрации агентов с базой данных через JPA-сущность `AgentEntity` и репозиторий `AgentRepository`. При старте инстанс оркестратора восстанавливает реестр из БД (`loadAgentsFromDatabase`), поэтому все узлы Active-Active пула видят одинаковое состояние агентов.
- Потоковый слушатель HDFS Inotify (Active-Standby пул с распределенным лизингом и Fencing).
- Изоляция привилегий (`AGENT_MODE=streamer` без прав исполнения задач).
- Подробная документация и схемы: [`docs/replicator-fault-tolerance.md`](../../docs/replicator-fault-tolerance.md).

### Backpressure при всплеске событий Inotify

- Защищает память агента и БД оркестратора от всплесков транзакций NameNode (массовое удаление или генерация миллионов файлов).
- Локальная очередь `InotifyBatchProcessor` с емкостью `inotifyQueueCapacity` (10 000) и микробатчингом (до 500 задач или 1 сек) с дедупликацией путей.
- Гистерезис насыщения: защитный режим включается при заполнении очереди $\ge 80\%$ и выключается только при снижении до $\le 30\%$, поэтому режим не переключается на каждом колебании.
- Обратная связь от оркестратора (`queueSaturated` при превышении порога неразобранных задач в пуле `maxPendingTasksThreshold`, по умолчанию 5 000).
- Задержка опроса `stream.poll()` агентом растет от 100 мс до 5 000 мс, накопившиеся события сбрасываются принудительным `flush()`.

### Метрики Micrometer и Prometheus

- Сжатие трафика: счетчики исходных (`replicator_compression_raw_bytes_total`) и сжатых сетевых байт (`replicator_compression_compressed_bytes_total`), фактический коэффициент сжатия (`replicator_compression_ratio`).
- Задержка передачи gRPC-чанков: таймер Micrometer с SLA-бакетами от 1 мс до 5 с (`replicator_grpc_chunk_transfer_latency_seconds_bucket`, `_sum`, `_count`), средняя (`replicator_grpc_chunk_transfer_latency_avg_ms`) и пиковая задержки (`replicator_grpc_chunk_transfer_latency_max_ms`).
- Мониторинг очереди и буфера Staging на стороне приемника: количество активных незавершенных файлов (`replicator_staging_queue_size`, `replicator_staging_active_files`), совокупный объем в байтах (`replicator_staging_active_bytes`) и число удаленных сборщиком мусора файлов-сирот (`replicator_staging_orphans_cleaned_total`).

### Безопасность и целостность данных

- Защита от fail-open при аутентификации агентов (`enforceAgentSecret`): в `AgentRegistry` и `ReplicatorProperties` есть режим `enforce-agent-secret`. При включении в `@PostConstruct` блокируется старт сервиса с пустым или коротким (< 16 символов) ключом, а `verifySecret` отклоняет запросы без валидного секрета.
- Защита от IDOR и разграничение прав в `PATCH /api/v1/jobs/{jobId}`: в `JobController.updateJobProgress` выполняется проверка `checkJobAccess(jobId, auth)`. Читателям (`READER`) и сторонним пользователям запрещено изменение прогресса и статуса чужих задач; обновление разрешено только авторизованным системным агентам (`system_agent`) и администраторам (`ADMIN`), либо владельцу задачи при наличии роли `WRITER`.
- Устранение BOLA / IDOR в инспекции задач и истории запусков: в `JobController.getJobRuns` (`GET /api/v1/jobs/{jobId}/runs`) и `TaskController.getTasks` (`GET /api/v1/jobs/{jobId}/tasks`) выполняется проверка `checkJobReadAccess(jobId, auth, agentSecret)`. Непривилегированным пользователям запрещен просмотр структуры файлов, путей, контрольных сумм, токенов аттестации и истории запусков чужих задач.
- Изоляция служебных эндпоинтов воркеров (`TaskController`): вызовы взятия задач в работу (`claimTasks`), фиксации успеха (`completeTask`), обработки сбоя (`failTask`) и создания пакетов подзадач (`batchCreateTasks`) защищены методом `requireAgentOrAdmin(auth)`. Обычные пользователи не могут вызывать эти методы.
- Защита управления квотами трафика (`TokenController`): эндпоинт ручного обновления лимитов пропускной способности (`PUT /api/v1/tokens/limit`) защищен проверкой `requireAdmin(auth)`.
- gRPC L7 аутентификация агентов (`AgentGrpcAuthInterceptor`): прямые вызовы gRPC сервисов (`DataTransferService`, `HmsTransferService`) защищены проверкой метаданных `X-Agent-Secret` в `ServerInterceptor` с constant-time сравнением (`MessageDigest.isEqual`). Исходящие каналы `ReplicationSender` и `HmsPipelineSender` передают секрет через клиентский интерцептор.
- Защита API и устранение Auth Bypass: все служебные и управляющие REST-эндпоинты требуют аутентификации (`anyRequest().authenticated()`). Фоновые демоны агентов аутентифицируются через `AgentSecretAuthenticationFilter` по заголовку `X-Agent-Secret`.
- Защита от Timing Attack: сравнение секретов агентов (`AgentRegistry.verifySecret`, `AgentSecretAuthenticationFilter`, `AgentGrpcAuthInterceptor`) переведено на constant-time (`MessageDigest.isEqual`).
- Защита от эскалации привилегий doAs (`execution_principal` и `runAsServiceAccount`): в `JobController` непривилегированным пользователям запрещено менять целевого Kerberos-пользователя и флаг запуска от системной техучетки (`runAsServiceAccount`); оба параметра берутся из автора запроса.
- Нет fallback на локальную ФС хоста: в `HadoopFsManager` для путей HDFS отключен fallback на локальный диск сервера при сбоях NameNode. Все методы записи (`commitFile`, `writeDirect`, `openOutputStreamForWrite`, `renameFile`) выбрасывают `IOException`, поэтому данные не попадают на диск хоста в обход прав Kerberos.
- Защита от Zip Slip, Tar Bomb и Path Traversal: нормализация путей в `combinePaths` и распаковщике TAR-стримов с запретом выхода за пределы целевого каталога (`..`). Блокировка удаления корневой директории `/` в HDFS. В потоковой распаковке `transferTarStream` действует лимит максимального размера записи (32 МБ) для защиты от Tar Bomb.
- Защита от Decompression Bomb (CWE-400): ограничение максимального размера распаковываемого чанка (`MAX_UNCOMPRESSED_CHUNK_SIZE = 64MB`) не дает исчерпать память (OOM) при декомпрессии.
- Защита файловой системы хоста: проверка `validatePathSafety` запрещает доступ к системным путям ОС (`/etc`, `/proc`, `/sys`, `/dev`, `/root`, `/var/run`, `/boot`).
- Доставка событий Inotify: обработка переведена на транзакционный ack с backpressure, прогресс `txid` не теряется при сетевых сбоях.

### Производительность и конкурентность (P1)

- Неблокирующий Data Plane в Kafka Replicator: в `KafkaBatchProducer` отправка записей в локальный брокер вынесена в виртуальные потоки Java 21 (`Executors.newVirtualThreadPerTaskExecutor()`). потоки Netty EventLoop gRPC не блокируются при переполнении буфера продюсера или сбоях сети Kafka.
- Координация in-flight пакетов и перехват отмен gRPC в Kafka Transfer: в `KafkaDataTransferServiceImpl.streamBatches` ведется учет активных батчей (`AtomicInteger inFlightBatches`) и обработка `setOnCancelHandler`. Завершение `responseObserver.onCompleted()` вызывается только после подтверждения всех батчей клиенту.
- Шардированный Token Bucket без глобального contention: в `TokenBucketThrottler` убрана глобальная блокировка `throttlerLock` на весь оркестратор. В `SingleBucket` добавлен потокобезопасный метод `calculateWaitAndConsume`, выполняющий расчет интервала задержки и списание сетевых токенов под изолированным `ReentrantLock` конкретного бакета. Потоки передачи между разными кластерами масштабируются независимо без ожидания в общей очереди.
- Кэширование потоковых задач в Inotify Streamer: в `HdfsInotifyListener` добавлен кэш активных задач `cachedStreamingJobs` с интервалом инвалидации 5 секунд. До этого стример отправлял сотни синхронных HTTP GET запросов к оркестратору при чтении каждого батча EditLog NameNode.
- Изолированный забор задач без блокировки родительской таблицы (`FOR UPDATE SKIP LOCKED`): в `TaskRepository.findPendingTasksForClusterSkipLocked` выборка задач переведена на подзапрос `WHERE t.jobId IN (SELECT j.id FROM JobEntity j ...)` вместо `JOIN`: пессимистическая блокировка `FOR UPDATE SKIP LOCKED` накладывается только на строки подзадач `replication_tasks`, а родительские строки `replication_jobs` не блокируются.
- Ликвидация N+1 при подсчете запусков (`countByJobIdIn`): в `JobRunRepository` и `JobService` N+1 запросов `countByJobId` при листинге задач заменены одним агрегирующим SQL-запросом с группировкой по списку идентификаторов.
- Лизинг стримера без полных сканов БД: в `StreamingLeaseCoordinator` вызов `jobRepository.findAll()` в `renewLease` и `getLeaseStatus` заменен на JPQL-запрос `findMaxProcessedTxidByClusterId`. В `DisasterRecoveryService` полные сканы таблиц `JobEntity` и `HmsReplicationJobEntity` при аварийном останове (`doEmergencyStop`) и восстановлении (`resumeCluster`) заменены на индексные запросы `findByNormalizedSourceClusterId(clusterId)`.
- Изолированный пул виртуальных потоков распаковки TAR-стримов (`tarUnpackExecutor`): в `DataTransferServiceImpl` потоковая распаковка архивов (`TarArchiveInputStream`) переведена из общего пула `ForkJoinPool.commonPool()` в отдельный именованный `ExecutorService` на базе виртуальных потоков Java 21 (`Thread.ofVirtual().name("tar-stream-unpacker-", 0).factory()`) и не конкурирует с системными задачами JVM.
- Схема как миграции Flyway (`db/migration`): схему создают и меняют миграции, `ddl-auto` стоит в `validate` во всех профилях. Прежний эталонный скрипт `schema-postgresql.sql` не подключался ни к одному окружению и успел разойтись с моделью: в нем не было таблиц журнала событий HMS и его DLQ, снимки DR и аренда стримера назывались иначе, а блокировки кластера были описаны дважды. Базовая миграция выгружена из самой модели JPA, поэтому схема и сущности совпадают колонка в колонку. Секционирование `replication_tasks` и `job_runs` из прежнего скрипта не перенесено: ключ секционирования обязан входить в первичный ключ, то есть PK стал бы составным (`id, updated_at`) при `updated_at`, допускающем `NULL` — это отдельная задача со сменой семантики модели.
- Трассировка и health-пробы gRPC: все вызовы gRPC проходят через перехватчики трассировки (`GrpcTracing`): клиент кладет в метаданные W3C `traceparent`, сервер продолжает тот же trace и выставляет `trace_id` в MDC, поэтому операция собирается из логов разных процессов. Экспорт спанов включается заданием адреса OTLP-коллектора (`MANAGEMENT_OTLP_TRACING_ENDPOINT` у оркестратора, `OTEL_EXPORTER_OTLP_ENDPOINT` у агента); без него API работает как no-op. Control Plane, приемник агента и приемник Kafka-батчей отвечают по стандартному протоколу `grpc.health.v1.Health` со статусом по подсистемам, а HEALTHCHECK контейнера агента выполняет `GrpcHealthProbeMain` вместо проверки открытого TCP-порта.
- МежЦОДный шейпинг (`TokenBucketThrottler`): лимиты дата-центров извлекаются через идентификатор ЦОД (`dcId`), поэтому межЦОДные квоты соблюдаются, даже если имена кластеров отличаются от имен ЦОД.
- Сохранение сетевого ограждения (DR fencing) при перезапуске: при перезапуске оркестратора аварийные снимки `EmergencySnapshot` из БД не только восстанавливаются в кэш, но и повторно активируют нулевой лимит пропускной способности (`setDcLimit(..., 0L)`), предотвращая Split-Brain до снятия аварии.
- Пул gRPC соединений (`ManagedChannel` pool): в `ReplicationSender` используется постоянный пул gRPC-каналов с поддержкой HTTP/2 keepalive (`keepAliveTime: 30s`, `keepAliveTimeout: 10s`), graceful shutdown и эвикцией сбойных каналов при сетевых ошибках (`evictChannel`); TCP- и TLS-соединение не устанавливается заново на каждую передачу.
- Пакетирование сетевых токенов (Batch Token Leasing): воркеры запрашивают сетевые токены у оркестратора порциями по 2 МБ на маршрут (`source -> target`), что сокращает число HTTP-запросов до 97% при передаче файлов и TAR-бандлов.
- Устранение N+1 SQL-запросов: в `TaskRepository` добавлен JPQL-запрос `countPendingTasksByClusterId` вместо сканирования всей таблицы `JobEntity` в `StreamingLeaseCoordinator` и `TaskService`. В забор задач (`claimTasks`) добавлена пагинация `PageRequest` и локальный кэш родительских задач `JobEntity`.
- Виртуальные потоки (Virtual Threads / Project Loom): в оркестраторе включен режим `spring.threads.virtual.enabled: true`. В `TokenBucketThrottler` примитивы `synchronized` заменены на `ReentrantLock`, чтобы виртуальный поток не закреплялся за carrier-потоком (carrier thread pinning) на Java 21 LTS. В gRPC-сервере приема агента (`ReplicatorAgent`) используется пул виртуальных потоков (`Executors.newVirtualThreadPerTaskExecutor()`), поэтому EventLoop-потоки Netty не блокируются при распаковке виртуальных TAR-стримов.
- Пагинация задач по умолчанию: в `TaskController.getTasks` действует пагинация (`PageRequest.of(0, 100)`, макс. 1000) при отсутствии явных параметров `page` и `size`; без нее просмотр заданий с миллионами файлов приводил к OutOfMemoryError и полным сканам таблицы.
- Хранение снимков Disaster Recovery в БД: активные снимки аварийного останова (`EmergencySnapshot`) перенесены из оперативной памяти в базу данных (`DrSnapshotEntity`, `DrSnapshotRepository`) с автоматическим восстановлением при старте сервиса (`@PostConstruct`).
- Перехват отмен gRPC и очистка Staging: в `DataTransferServiceImpl` зарегистрирован `serverCallStreamObserver.setOnCancelHandler(...)`, который удаляет staging-файлы в HDFS и освобождает семафоры при клиентской отмене или разрыве соединения.

### Хранение задач, кэш Schema Registry и декомпозиция фронтенда (P2)

- Retention Policy и автоочистка таблицы задач (`TaskEntity`): периодический шедулер (по умолчанию раз в час) удаляет подзадачи в терминальных статусах (`COMPLETED`, `FAILED`, `SKIPPED`) старше `task-retention-days` (по умолчанию 7 дней), чтобы таблица `replication_tasks` не росла при репликации миллионов файлов. Удаление идет пакетами по `task-retention-batch-size` с отдельной транзакцией на пакет и ограничением `task-retention-max-batches-per-run` на проход: одна транзакция на все окно удержания держала бы блокировки и раздувала WAL, а прерывание откатывало бы всю работу. Активные задачи (`QUEUED`, `RUNNING`) не удаляются ни при каком возрасте, а в active-active очистку выполняет один узел под распределенной блокировкой.
- Lock-free кэш Schema Registry и single-flight разрешение схем: в `CachedSchemaRegistryResolver` реализован `BoundedLruCache` (до 10 000 записей) с lock-free чтением через `ConcurrentHashMap` и обновлением LRU-порядка через `tryLock()`; чтения не конкурируют за блокировку при нагрузке 100 000+ RPS. Single-flight (`CompletableFuture` in-flight promise) объединяет параллельные запросы к одной новой схеме в один HTTP-запрос к Schema Registry (защита от thundering herd). В `ZeroCopyWireTranscoder` аллокации `HeapByteBuffer` заменены побитовыми операциями (транскодинг без аллокаций).
- Пакетная вставка и обновление в БД (Hibernate JDBC Batching): в конфигурацию `application.yml` оркестратора добавлены параметры `hibernate.jdbc.batch_size: 500`, `order_inserts: true`, `order_updates: true` и `batch_versioned_data: true`; вставки и обновления подзадач уходят в СУБД пакетами до 500 строк.
- Модульная декомпозиция SPA фронтенда (`App.svelte`): монолитный компонент `App.svelte` (~2560 строк) разделен на компоненты в директории `frontend/apps/replicator/src/components/`:
  - `JobsTableView.svelte`: таблица HDFS-задач, карточки статистики, динамические фильтры статуса/автора, полнотекстовый поиск и управление жизненным циклом (Play/Stop/Edit/Delete/History).
  - `TopologyView.svelte`: визуализация дата-центров, привязка кластеров, троттлинг WAN и Keepalive телеметрия агентов (latency, compression ratio, staging buffers).
  - `CreateJobModal.svelte`: модальное окно создания задач с валидацией путей, выбором кластеров, режимов синхронизации (Manual/Scheduled/Inotify) и глубины хранения.
  - `EditJobModal.svelte`: модальное окно редактирования параметров активных и периодических задач.
  - `JobHistoryModal.svelte`: модальное окно с детальной историей всех запусков (`JobRun`), статистикой переданных объемов и настройкой лимита retention.

### Остановка gRPC-каналов, метрики отмен и разграничение Actuator (P3)

- Graceful-остановка каналов gRPC-пула: `ReplicatorAgent.stop()` вызывает `sender.close()`, который закрывает все каналы пула `channelPool` и освобождает сокеты до остановки демона.
- Мониторинг отмен и обрывов gRPC-стримов: в `AgentMetrics` добавлен счетчик Micrometer `replicator.grpc.transfers.cancelled` (`cancelledTransfersCounter`). Счетчик увеличивается в обработчиках клиентской отмены (`serverCallStreamObserver.setOnCancelHandler(...)`) при передаче одиночных файлов (`transferFile`) и потоковых TAR-архивов (`transferTarStream`).
- Сегментация Spring Boot Actuator: эндпоинты Actuator оркестратора разделены по уровню доступа:
  - Публичные (`permitAll()`): эндпоинты проверки жизнеспособности (`/actuator/health/**`), базовой информации (`/actuator/info`) и Prometheus-скрейпинга метрик (`/actuator/prometheus`).
  - Защищенные (`hasRole("ADMIN")`): административные и диагностические эндпоинты (`/actuator/metrics/**`, `/actuator/env`, `/actuator/beans`, `/actuator/mappings` и др.).

### Fencing, блокировки и изменение лимитов на лету (P4)

- Fencing прямых задач при инверсии (Disaster Recovery Reverse Failback): в `DisasterRecoveryService.reverseReplication` выполняется остановка прямых задач HDFS (`directJobs`) и схем HMS (`directHms`) со статусов `RUNNING`, `QUEUED`, `ACTIVE` и отключение их расписания (`isScheduled = false`) перед запуском обратных репликаций. Так исключается встречная запись и зацикливание репликации в одни и те же каталоги HDFS.
- Точное сопоставление кластеров: во всех сервисах оркестратора и агента (`DisasterRecoveryService`, `StreamingLeaseCoordinator`, `HdfsInotifyListener`) нестрогий поиск по подстроке (`contains`) заменен на точное совпадение нормализованных идентификаторов кластеров (`c1n.equals(c2n)`); иначе `dc1` совпадал бы с `dc10`.
- Динамическое обновление лимитов Token Bucket на лету: в `TokenBucketThrottler.SingleBucket` добавлен потокобезопасный метод `updateLimit(...)`. При обновлении лимитов каналов в рантайме через UI/API (`setDcLimit`, `setHdfsLimit`) существующие в кэше бакеты обновляют параметры емкости и скорости генерации токенов без перезапуска сервиса.
- Self-Fencing стримера Inotify при потере связи: в `HdfsInotifyListener` добавлено самоограждение: если демон стримера теряет связь с оркестратором на интервал, превышающий окно безопасности аренды (`LEASE_EXPIRATION_SAFETY_TIMEOUT_MS = 10s`), агент сбрасывает статус (`active.set(false)`) и переходит в STANDBY, чтобы при перевыборах лидера не появилось два активных стримера (split-brain).
- Heartbeat Renewal в распределенных блокировках: в `DistributedLockService.runWithLock` запускается виртуальный поток автопродления блокировки (Virtual Thread Java 21 `lock-renew-*`), продлевающий аренду каждые `duration / 2` до завершения действия; длительная задача планировщика не теряет блокировку при задержках или GC-паузах.

### Настройка БД и пула соединений под 200+ агентов

- Пул HikariCP: в `application.yml` оркестратора настроен пул соединений `ReplicatorHikariPool` (`maximum-pool-size: 60`, `minimum-idle: 20`, `connection-timeout: 20s`, `leak-detection-threshold: 30s`). Размер рассчитан на сотни воркеров, опрашивающих оркестратор каждые 1–2 секунды.
- Без конкуренции за строку и взаимных блокировок при завершении задач: когда разные агенты одновременно фиксируют завершение тысяч файлов, родительская строка `JobEntity` не блокируется через ORM. В `JobRepository` используется атомарный SQL UPDATE (`incrementProgress`) с `clearAutomatically = true, flushAutomatically = true`.
- Индексная проверка завершения задач (LIMIT 1 за 0.05 мс): периодический подсчет `COUNT(*)` по многомиллионной таблице подзадач заменен на метод `taskRepository.existsByJobIdAndStatusIn(jobId, ['QUEUED', 'RUNNING'])`. Пока задачи выполняются, транзакция завершается без сканирования таблицы, а финализация задания и вычисление метрик производятся единожды при завершении последней задачи.
- Пакетное подтверждение задач (`completeTasksBatch`): добавлен эндпоинт `POST /api/v1/tasks/complete-batch` и метод сервиса `completeTasksBatch`, через который воркер отправляет отчет о завершении 5–50 файлов одной транзакцией.
- Таргетированный мониторинг зависших задач: планировщик `checkAndFailoverOrphanedTasks` больше не сканирует все тысяч выполняющихся задач (`findByStatus("RUNNING")`). Поиск ограничен только аварийными агентами со статусом `OFFLINE` из памяти `AgentRegistry`, задачами без назначенного воркера и задачами, чье обновление превысило таймаут сердцебиения (`findByStatusAndUpdatedAtBefore`). В нормальном режиме фоновый опрос занимает 0.1 мс и возвращает 0 строк.
- Частичные индексы (partial indexes): в миграции `db/migration/postgresql/V2__postgresql_partial_indexes.sql` созданы индексы (`idx_tasks_queued_active`, `idx_tasks_running_stale`, `idx_tasks_active_by_job`, `idx_tasks_assigned_running`), по активным задачам (`QUEUED`, `RUNNING`) без завершенных записей, поэтому индексы небольшие и помещаются в кэш СУБД в памяти.
- Хранение PostgreSQL для частых UPDATE (HOT-updates): для таблицы `replication_tasks` выставлены параметры `WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, fillfactor = 90)`. 10% свободного места на страницах таблицы позволяют записать обновленную версию строки на ту же страницу (HOT) без обновления индексов.
- Пагинация при инспекции задач: эндпоинт `GET /api/v1/jobs/{jobId}/tasks` дополнен поддержкой параметров `page` и `size` (по умолчанию 100, максимум 1000); без нее просмотр заданий с миллионами файлов приводил к OutOfMemoryError.

### gRPC Control Plane для агентов вместо REST

- Переход на HTTP/2 и gRPC: управляющий протокол агентов и оркестратора (Control Plane) переведен с REST на gRPC (порт `50050`, HTTP/2).
- Protobuf-контракт (`orchestrator_control_plane.proto`) включает 15 RPC-методов для регистрации (`RegisterAgent`, `Heartbeat`, `UnregisterAgent`), квотирования трафика (`RequestTokens`), диспетчеризации задач (`ClaimTasks`, `CompleteTask`, `FailTask`, `BatchCreateTasks`), лизинга стримеров Inotify (`RenewStreamingLease`, `ReportInotifyProgress`) и координации репликации схем Hive Metastore (`ClaimHmsTasks`, `ReportHmsProgress`, `ReportHmsTaskResult`, `AcquireHmsJobLease`, `RenewHmsJobLease`).
- Мультиплексирование: постоянные HTTP/2-соединения с keepalive вместо сотен короткоживущих HTTP/1.1-запросов; TCP-соединение не устанавливается на каждый вызов, бинарный Protobuf сериализуется быстрее JSON.
- Безопасность L7 и TLS: запросы агентов аутентифицируются через интерцептор заголовка `X-Agent-Secret` с constant-time верификацией (`AgentSecretServerInterceptor`), с поддержкой шифрования TLS/mTLS.
- Разделение интерфейсов: в REST API оркестратора нет служебных эндпоинтов агентов, он используется только для веб-интерфейса Svelte 5 и внешнего администрирования.

### Очередь задач, параллельный воркер и проверки безопасности

- Устранение межкластерного голодания (Head-of-Line Blocking) в очереди задач: в `TaskService.claimTasks` используется метод `findPendingTasksForClusterSkipLocked`: выборка ограничена кластером (`clusterId`), и агенты одного кластера не ждут блокировок задач другого. Поддержан забор задач в статусе `STREAMING`.
- Параллельный пул воркера (Concurrent Sender Task Pool): в `ReplicatorAgent` последовательный забор и передача задач из пачки переведены на асинхронный пул потоков (`workerTaskPool = Executors.newFixedThreadPool(workerConcurrency)`). Передачи `transferTask` исполняются параллельно с ограничением слотов `activeTransfers`.
- Предотвращение O(N) выгрузки истории запусков: в `JobRunRepository` добавлен агрегатный запрос `findMaxRunNumberByJobId`вместо загрузки всей истории запусков через `jobRunRepository.findByJobIdOrderByRunNumberDesc` в `ReplicationScheduler` и `JobService`.
- Пакетное удаление задач в БД (`deleteByJobId`): поэлементное удаление через `taskRepository.deleteAll(tasks)` заменено одним запросом `@Modifying @Query("DELETE FROM TaskEntity t WHERE t.jobId = :jobId")` с индексной поддержкой `(status, updated_at)`.
- Устранение BOLA / IDOR в чтении задач (`GET /api/v1/jobs/{jobId}`): в `JobController` добавлен метод `checkJobReadAccess`, запрещающий просмотр конфигураций и путей чужих задач пользователями с ролью `WRITER` сторонних проектов.
- Проверка принципалов (SEC-1): в `HadoopFsManager` добавлен метод `validatePrincipalSafety` с проверкой регулярным выражением (`SAFE_PRINCIPAL_PATTERN`) и блокировкой системных учеток ОС (`root`, `bin`, `daemon`, `sys`, `adm`, `operator` и др.) и инъекций (`..`, `;`, `\0`, `\n`) при doAs-имперсонации.
- Защита от MITM при gRPC TLS (SEC-2): в `OrchestratorGrpcClient` убран безусловный `InsecureTrustManagerFactory`. По умолчанию сертификаты оркестратора валидируются через доверенные корневые CA системы (TrustManager JVM).
- Распределенная блокировка на failover осиротевших задач: в `TaskService.checkAndFailoverOrphanedTasks` вызывается `DistributedLockService.runWithLock("orphan_task_failover_lock", ...)`, чтобы одни и те же задачи не переносили несколько узлов оркестратора одновременно.

### HMS Replicator: пул соединений, перенос схемы, CDC

- Пул соединений Hive Metastore (Apache Commons-Pool2): в `ThriftHmsClient` используется `GenericObjectPool<IMetaStoreClient>`, потому что `HiveMetaStoreClient` не потокобезопасен. При `MetaException` соединение инвалидируется, а операция повторяется.
- Полный перенос схемы таблиц Hive: расширены Protobuf-контракты (`HmsFieldSchemaProto`), DTO и конвертеры (`HmsConversionHelper`). Колонки больше не захардкожены (`id`/`data`): переносятся поля таблицы, типы колонок, комментарии, `partition_key_fields` и параметры сериализации/десериализации (`serde_parameters`).
- Дельта-синхронизация CDC без O(N): обработчик события `ADD_PARTITION` извлекает дельту партиций (`extractPartitionValuesFromEvent`) непосредственно из полезной нагрузки JSON события Hive Metastore. Все 100 000+ партиций таблицы из источника не загружаются.
- Обработка ошибок gRPC: добавлены проверки `response.getSuccess()` при вызовах `applyTableSchema` и `applyPartitionBatch` в `HmsTaskExecutor`.
- Защита от несанкционированного доступа (IDOR Protection): в `HmsReplicationController` добавлен метод `checkHmsJobAccess`, запрещающий изменение, остановку и удаление задач другими пользователями (HTTP 403 Forbidden).
- Устранение N+1 запросов в JPA/SQL: в `JobRepository` и `JobService` реализован пакетный запрос `listSubjobsByParentIds(parentJobIds)`, а в `HmsCoordinatorService` используется массовый `saveAll` и агрегация метрик сабтасков без циклических обращений к базе данных.
- Каскадная очистка при Disaster Recovery Failback: в `DisasterRecoveryService` удаление прямых задач HMS переведено на каскадный метод `deleteReplicationJob` с очисткой истории событий и вложенных HDFS подзадач.
- Сопоставление путей: в `HmsPathRewriter` используются границы слов (`\b`) при сопоставлении authority; имена хостов больше не искажаются.
- Инициализация пустых списков партиций (`partitionKeys`): в `ThriftHmsClient.toThrift` поле `t.partitionKeys` инициализируется не-null списком (`new ArrayList<>()`) для непартиционированных таблиц; без этого `alter_table` падает в Hive Metastore 4.0 с ошибкой `InvalidOperationException: partition keys can not be changed.`.
- Диагностические сообщения CDC-пачек: в `HmsTaskExecutor` статусное сообщение при сбоях пачки теперь выводит точный ID сбойного события и конкретную причину ошибки вместо общего «Приостановка CDC на событии из-за ошибки».
- Фиксация нулевого лага CDC (`event_lag = 0`): в `OrchestratorControlPlaneServiceImpl` устранено ошибочное приведение `request.getEventLag() != 0L ? ... : null`, из-за которого сервер отбрасывал валидный лаг `0L` и оставлял в БД старое значение. В `HmsTaskExecutor` добавлена автосинхронизация нулевого отставания при исчерпании очереди событий.

### Отказоустойчивость и целостность данных при анализе и стриминге

- Атомарный захват задания на анализ (`claimJobForAnalysis`): устранен split-brain при параллельной работе агентов в режиме `all`. Перевод задания `QUEUED -> ANALYZING` защищен условным SQL CAS-обновлением. Анализ каталогов и создание подзадач выполняет ровно один агент, поэтому очередь не пересоздается повторно.
- Устранение зацикливания при `MissingEventsException`: при отставании стримера и ротации NameNode Edits журналов монотонный курсор `currentTxid` подтягивается до актуального `e.getActualTxid()`; без этого запросы повторялись бесконечно.
- Reconciliation scan: при выпадении событий Inotify запускается обход директорий задач потоковой репликации и найденные файлы ставятся в очередь дельт.
- Переполнение буфера Inotify: при заполнении очереди вместо молчаливого сброса событий выбрасывается `InotifyQueueOverflowException`, что приостанавливает опрос NameNode без смещения `currentTxid` до освобождения буфера.
- Отмена gRPC-вызовов и прерывание фоновых коммитов: в `DataTransferServiceImpl` (`TransferFileObserver` и `TarStreamObserver`) есть атомарный флаг отмены `cancelled` и принудительное прерывание пула воркеров (`commitFutures.cancel(true)`). Так не остаются недописанные файлы и нет гонки между удалением staging-файлов и параллельным `renameFile`.

### Защита периметра: fail-closed gRPC, SSRF, белый список кластеров

- Перевод `AgentGrpcAuthInterceptor` в режим Fail-Closed по умолчанию: при отсутствии или некорректной конфигурации секрета `X-Agent-Secret` неавторизованные входящие gRPC-вызовы отклоняются со статусом `Status.UNAUTHENTICATED`. Для изолированных тестовых окружений есть флаг явного разрешения анонимного доступа `allowAnonymous = true`.
- Защита от SSRF в реестре агентов (`AgentRegistry`): метод валидации адреса узла `validateGrpcAddress` проверяет адрес через `InetAddress` и регулярные выражения. Запрещен анонс Link-Local (`169.254.0.0/16`, `fe80::/10`), IPv4-mapped IPv6, Multicast, Wildcard/AnyLocal адресов и облачных сервисов метаданных (AWS, GCP `metadata.google.internal`, Azure, Alibaba `100.100.100.200`, OpenStack): через них можно получить IAM-токены или сканировать внутреннюю сеть.
- Предотвращение захвата кластеров (Cluster Hijacking Prevention): в `AgentRegistry` добавлен метод `validateClusterAssignment`. Зарегистрированному агенту запрещена динамическая смена идентификатора кластера (`clusterId`) как при повторной регистрации, так и в периодических heartbeat без предварительной дерегистрации.
- Принудительное соблюдение белого списка кластеров (`enforce-cluster-whitelist`): при включении политики агенты могут регистрироваться только в кластерах, заданных в топологии `TopologyRegistry`.

### Control Plane и flow control конвейера Kafka Replicator (P1)

- Устранение Stop-and-Wait задержек в gRPC Data Plane (P1-1): в `KafkaDataTransferServiceImpl` используется скользящее окно упреждающих запросов пакетов (`pipelineWindow = 16`, `serverCallStreamObserver.request(N)`). По завершении обработки каждого входящего пакета вызывается `request(1)`; канал не простаивает в ожидании RTT.
- Динамический Control Plane диспетчер задач Kafka на агентах (P1-2): в `orchestrator_control_plane.proto` и `OrchestratorControlPlaneService` реализован контракт `GetActiveKafkaJobs(dc_id)`. Агент (`KafkaReplicatorAgentMain`) в цикле heartbeat опрашивает оркестратор и получает новые задачи.
- Версионированная потокобезопасная переподписка без ребалансировок: в `KafkaBatchConsumerPool` реализована версионная синхронизация топиков (`AtomicLong topicsVersion`); ложных пробуждений и лишних rebalance нет. Воркеры подхватывают новые топики на лету без перезапуска CLI-демона.

### Zero-copy транскодинг и Kerberos в Java 21 (P2, P3)

- Zero-copy транскодинг wire-форматов схем (P2-2): в `ZeroCopyWireTranscoder` убраны аллокации `payload.clone()`. При сопоставлении схем Confluent/Hortonworks замена ID схемы выполняется in-place в буфере заголовка, без аллокаций в куче на пакет.
- Изоляция конфигурации Kerberos и защита krb5.conf (P2-1): в `KafkaSecurityConfigBuilder` и `KerberosSpnegoAuthenticator` добавлена защита от взаимной перезаписи `java.security.krb5.conf`. В JAAS-шаблон Krb5LoginModule добавлены директивы `doNotPrompt=true` и `refreshKrb5Config=true`.
- Переход на стандарт безопасности Java 21 (P3): в `KerberosSpnegoAuthenticator` помеченный к удалению (`@Deprecated(forRemoval=true)`) API `Subject.doAs(...)` заменен на `Subject.callAs(...)` из Java 21 LTS с обработкой `CompletionException`.

### Исходящий backpressure, zero-copy и кэш маршрутов (Phase 3)

- Исходящий Flow Control и Backpressure в gRPC клиенте (P1-1): в `ReplicationSender` отправка потоковых чанков одиночных файлов (`transferSingleFileWithSync`), виртуальных TAR-стримов (`TarChunkingOutputStream`) и прямых стримов файлов переведена на gRPC `io.grpc.stub.ClientResponseObserver`. В методе `beforeStart()` регистрируется `setOnReadyHandler()`. При заполнении исходящего буфера сокета Netty отправка с диска HDFS приостанавливается через `awaitStreamReady()`; на медленных WAN-каналах данные не копятся в куче JVM.
- Zero-copy и меньше нагрузки на сборщик мусора (P1-2): в `DataTransferServiceImpl` для несжатых чанков (`COMPRESSION_NONE` / `UNRECOGNIZED`) исключено промежуточное копирование `data.toByteArray()`. Прямой сброс в целевой HDFS поток `data.writeTo(outputStream)` и вычисление SHA-256 через `data.asReadOnlyByteBufferList()` пишут байты без аллокаций в куче JVM. Аналогично для TAR-потока `transferTarStream` реализован прямой сброс `data.writeTo(pipedOut)`. В `WireCompressor` добавлен метод `decompress(ByteString, CompressionCodec, int uncompressedSize)`.
- Lock-Free Fast-Path и кэширование маршрутов в Throttler (P1-3): в `TokenBucketThrottler` поле `limitBytesPerSec` переведено на `volatile double`, а в `SingleBucket.updateLimit` добавлен lock-free fast-path (`Math.abs(limit - newLimit) < 1e-6`); при неизменном лимите `ReentrantLock` не захватывается. Добавлен кэш разрешенных маршрутов `routeCache` (`ConcurrentHashMap<String, RouteBinding>`); он убрал повторные обращения к топологии и аллокации строк на каждый пакет токенов.
- Single-Flight Token Lease Refill (P1-4): в `ReplicationSender` для запросов сетевых квот по маршрутам используется single-flight с `routeRefillLocks`. При исчерпании пачки токенов ровно один поток отправляет RPC-запрос к оркестратору на пополнение квоты (2 МБ); остальные потоки ждут на блокировке маршрута и списывают токены из локального пула без собственных RPC (защита от thundering herd).

### Безопасность и отказоустойчивость (Phase 4)

- [SEC-03] Санитизация путей HDFS и ограничение staging reaper: шаблон `Pattern.compile(".*\\._staging_[a-zA-Z0-9_-]+$")` и валидация путей `HadoopFsManager.validatePathSafety` с проверкой схем `file:///` и `hdfs://` против Path Traversal и удаления пользовательских файлов reaper-ом.
- [SEC-02] Fail-closed gRPC auth: интерцептор закрыт по умолчанию, `AgentGrpcAuthInterceptor` (`allowAnonymous = false` при отсутствии секрета).
- [PERF-01] Memory-Bounded Flow Control при TAR-Streaming: семафор памяти (лимит `16 МБ`) и списки коммитов с `$O(1)$` в `DataTransferServiceImpl` против OOM при обработке миллионов мелких файлов.
- [RES-04] Целостность структуры TAR-потока: проверка существования файла `fsManager.exists` перенесена до записи заголовка TAR-архива, иначе поток повреждался и распаковка обрывалась.
- [SEC-04] Валидация URL Schema Registry и SSRF: добавлен `SchemaRegistryUrlValidator` с блокировкой облачных метаданных (`169.254.169.254`, `metadata.google.internal`) и link-local диапазонов, а также предупреждением о незашифрованной передаче Kerberos SPNEGO тикетов.
- [RES-05] Устранение Carrier Thread Pinning: в `SafetyCommitManager` критическая секция синхронизации переведена с монитора `synchronized` на `ReentrantLock`, чтобы виртуальные потоки Java 21 (Project Loom) не закреплялись за carrier-потоками.
- [SEC-06] doAs-имперсонация в Hive Metastore: расширены gRPC контракты (`replicator.proto`) полем `execution_principal`, в `ThriftHmsClient` добавлен `executeAs` с валидацией имени принципала и `UserGroupInformation.createProxyUser`. Все DDL- и snapshot-операции выполняются с правами инициатора задачи под контролем Apache Ranger.
- [SEC-05 & SEC-08] Защита топологии Kafka и изоляция Discovery: не-администраторам запрещено указание произвольных keytab путей в `KafkaReplicationController`, заблокирован SSRF в Kafka Discovery при разрешении bootstrap-серверов.
- [RES-02] Epoch fencing стримера: добавлен монотонный номер эпохи стримера (`epoch`) в `orchestrator_control_plane.proto` и `TaskService`. Отклоняются устаревшие пакеты событий от отгороженных стримеров при смене лидера. Проверка выполняется только для заданий `STREAMING_INOTIFY`: пакет `BatchCreateTasks` отклоняется, если его `epoch` меньше эпохи записи аренды (`StreamingLeaseEntity`) кластера-источника. Если записи аренды нет или стример не передал `epoch` (значение `0`), пакет принимается без проверки эпохи.
- [RES-03] Защита активных передач от Staging Reaper: проверка `DistributedFileSystem.isFileClosed` перед удалением staging-файлов; reaper не удаляет файлы, передача которых идет дольше 30 минут.
- [SEC-07] Защита от Route Hijacking и Whitelist топологии: в `TopologyRegistry` запрещена перезапись постоянных адресов кластеров динамическими воркерами, валидирован список кластеров `enforce-cluster-whitelist`.

### Аттестация задач HMAC-токенами (SEC-01)

- Ресивер `DataTransferServiceImpl` не доверяет параметрам передачи (`execution_principal`, `run_as_service_account`, `target_path`, `task_id`, `job_id`) без проверки токена. При выдаче задачи из очереди `claimTasks` оркестратор подписывает канонические метаданные задачи алгоритмом HMAC-SHA256 (`v1|taskId|jobId|targetPath|executionPrincipal|runAsServiceAccount|expiresAt`).
- Сквозная передача через gRPC: поле `task_token` добавлено в Protobuf-сообщения `TaskItemProto`, `FileMetadata` и `TarStreamMetadata`. Воркер передает токен ресиверу при инициализации потока.
- Проверка на стороне ресивера: при валидации токена проверяется подлинность криптографической подписи (`MessageDigest.isEqual`), совпадение целевого пути, принципала Kerberos, флага системной техучетки и срок действия токена (TTL). При подмене прав или пути ресивер отклоняет запрос с ошибкой `Status.PERMISSION_DENIED`.
- Обратная совместимость: переход включается флагом `enforceTaskAttestation` (по умолчанию `false` для тестов обратной совместимости и `true` при включении `REPLICATOR_ENFORCE_TASK_ATTESTATION=true`).

### Песочница целевых путей и защита системных файлов хоста (SEC-10)

- Изоляция локальной директории (chroot jail): при локальной репликации (`file:///` или автономный режим) агент ограничивает файловые операции (`transferFile`, `transferTarStream`, `renamePath`, `deletePath`, чтение метаданных) внутри настроенной директории песочницы `REPLICATOR_LOCAL_SANDBOX` (конфиг `localSandboxDir`).
- Защита от Symlink Traversal: проверка канонических путей (`File.getCanonicalFile().toPath().startsWith(sandboxRoot)`) не дает обойти песочницу через символические ссылки на внешние каталоги хоста.
- Защита от Null Byte Injection и Path Traversal: блокируются любые байты `\0` и относительные переходы (`..`).
- Черный список системных каталогов ОС: запрещен доступ к `/bin`, `/sbin`, `/usr`, `/lib`, `/lib64`, `/opt`, `/boot`, `/etc`, `/proc`, `/sys`, `/dev`, `/root`, `/System`, `/Library`, `/Applications`, системным директориям `/var` (`/var/run`, `/var/spool`, `/var/log`, `/var/lib`, `/var/mail`, `/var/root`, `/var/db`, `/var/cron`), а также чувствительным конфигурациям пользователей хоста (`/.ssh`, `/.gnupg`, `/.bashrc`, `/.zshrc`, `/.profile`, `/.bash_profile`, `/.bash_history`, `/.zsh_history`, `/.env`, `crontab`).

### Установка `krb5.conf` без перезаписи и взаимная аутентификация SPNEGO (SEC-11, SEC-15)

- Без перезаписи системного свойства JVM (SEC-11): в `KafkaSecurityConfigBuilder.initializeKrb5ConfSafely` реализована потокобезопасная установка `java.security.krb5.conf`. Если свойство уже задано в JVM (например, UGI/HDFS, аргументами JVM или другим кластером); повторная перезапись блокируется с предупреждением в логе. Иначе в мультикластерной среде сбрасывались бы настройки KDC и рвались соединения с другими Kerberos realm.
- Проверка путей Kerberos (`validateKerberosPathSafety`): разрешает пути конфигурации (`/etc/krb5.conf`, `/etc/security/keytabs/...`, `/var/run/secrets/...`), но блокирует попытки Path Traversal (`..`), Null Byte Injection (`\0`), системные псевдо-ФС ядра (`/proc`, `/sys`, `/dev`) и чувствительные файлы/ключи (`/etc/shadow`, `/etc/passwd`, `/etc/sudoers`, `/.ssh`, `/.gnupg`, `/.env`).
- Настраиваемая взаимная аутентификация SPNEGO (SEC-15): в `KafkaKerberosConfig` и `KerberosSpnegoAuthenticator` добавлена поддержка флага `srMutualAuth` (`context.requestMutualAuth(config.isSrMutualAuth())`); клиент проверяет подлинность сервиса Schema Registry.

### Секрет агента в gRPC Control Plane и защита от SSRF через loopback (SEC-12, SEC-13, SEC-16)

- Секрет агента в gRPC Context (SEC-16): в `AgentGrpcAuthInterceptor` реализован `AGENT_SECRET_CONTEXT_KEY`. Токен из входящего заголовка `x-agent-secret` помещается в gRPC-контекст текущего вызова (`Contexts.interceptCall`). В `OrchestratorControlPlaneServiceImpl` вызовы `registerAgent`, `heartbeat` и `unregisterAgent` извлекают секрет через `AgentGrpcAuthInterceptor.getAuthenticatedSecret()` и передают в `agentRegistry`; интерцептор и реестр проверяют один и тот же секрет.
- Хардкод секрета и его enforcement (SEC-12): в `ReplicatorProperties` константа `DEFAULT_AGENT_SECRET` переопределяется переменной окружения `REPLICATOR_AGENT_SECRET` (`getAgentSecret()`). При `enforceAgentSecret = true` или наличии вшитого ключа по умолчанию в журнал выводится `SECURITY ALERT` с требованием задать свой секрет в продуктивной инсталляции. При выключенном enforcement локальные тестовые среды работают с ключом по умолчанию.
- Защита от SSRF Loopback и валидация FQDN (SEC-13): в `AgentRegistry.validateGrpcAddress` добавлен контроль loopback-адресов (`allowLoopbackAgents`), блокирующий регистрацию агентов на `127.0.0.1`, `::1`, `localhost` в строгом режиме. Если имя хоста не разрешается в DNS, выполняется синтаксическая проверка имени на соответствие спецификации FQDN и фиктивные IP-адреса отклоняются.

### Снижение числа RPC к NameNode и адаптивный лизинг токенов (Phase 5)

- Кэширование родительских каталогов HDFS (PERF-04): при распаковке TAR-бандлов `HadoopFsManager.ensureParentDirectoryExists` проверяет родительский каталог по сессионному кэшу бандла (`sessionVerifiedDirs`) и глобальному потокобезопасному LRU-кэшу проверенных каталогов (`verifiedDirectoriesGlobalCache`, емкость 5000 путей, без TTL). `fs.exists`/`fs.mkdirs` к NameNode выполняются только для каталога, которого нет в кэшах. Сам файл по-прежнему пишется в `targetPath._staging_<jobId>` и переносится в целевой путь `renameFile` (см. пункт [P2] о двухфазном staging-коммите).
- Адаптивный динамический лизинг сетевых токенов (PERF-05): в `ReplicationSender` и `ReplicatorAgentConfig` фиксированный размер лизинга токенов (2 МБ) заменен на адаптивный по алгоритму AIMD (Additive Increase / Multiplicative Decrease). Размер батча токенов масштабируется от `REPLICATOR_TOKEN_BATCH_MIN_BYTES` (2 МБ по умолчанию) до `REPLICATOR_TOKEN_BATCH_MAX_BYTES` (16 МБ по умолчанию, настраивается до 32–64 МБ). Если передача идет без задержек, размер батча удваивается (2 МБ → 4 МБ → 8 МБ → 16 МБ); на каналах WAN 10 GbE это сокращает число gRPC-запросов к оркестратору в 8 раз. При троттлинге (`waitSec > 0.05`) размер батча сбрасывается к минимуму, чтобы один агент не занимал весь канал.
- Fencing Tokens и защита от Split-Brain в распределенных блокировках (RES-07): в `ClusterLockEntity` и `ClusterLockRepository` добавлен монотонно возрастающий счетчик эпохи `epoch` (Fencing Token), инкрементируемый атомарно при каждой смене владельца замка (`CASE WHEN l.lockedBy = :lockedBy THEN l.epoch ELSE (l.epoch + 1) END`). В `DistributedLockService` реализованы `LockHandle(acquired, epoch, lockName, nodeId)` и метод валидации владения блокировкой `isLockHeld(lockName, epoch)`. В `runWithLock` фоновый поток автопродления (`lock-renew-*`) при обнаружении рассинхронизации эпохи или потери аренды генерирует событие `DistributedLock Fencing Alert` и прекращает исполнение, чтобы при длительных GC-паузах или сетевом разделении действие не выполняли два узла (split-brain).

### Распределенные блокировки для операций Disaster Recovery (RES-08, RES-09)

- Взаимоисключение DR-операций: в `DisasterRecoveryService` операции аварийного останова (`emergencyStop`), отката аварии (`rollbackEmergencyStop`) и инверсии репликации (`reverseReplication`) защищены распределенными блокировками на базе `DistributedLockService` (`dr_emergency_<clusterId>`, `dr_reverse_<from>_<to>`) с таймаутами 15–20 секунд.
- Active-Active пул оркестратора: конкурирующий вызов (оператора или внешней автоматизации) на другом узле отклоняется с сообщением об ошибке (`DrActionResponse.error(...)`). Это предотвращает дублирование инвертированных задач, встречную циклическую передачу данных в HDFS и рассинхронизацию метаданных Hive Metastore.

### Деление квот Token Bucket между узлами оркестратора (RES-06)

- Реестр узлов Orchestrator (`replication_orchestrator_nodes`): в `OrchestratorClusterCoordinator`, `OrchestratorNodeEntity` и `OrchestratorNodeRepository` активные инстансы оркестратора отслеживаются через базу данных. Узлы отправляют heartbeat каждые 5 секунд и исключают сбойные узлы (`cleanStaleNodes` при `last_heartbeat < now - 15s`).
- Пропорциональное деление полосы WAN: в `TokenBucketThrottler` добавлен расчет локального лимита генерации сетевых токенов $\text{localLimit} = \frac{\text{configuredLimit}}{\max(1, N)}$, где $N$ — число активных инстансов. При $N$ узлах за L4/L7 балансировщиком суммарный трафик репликации не превышает общий лимит, а не умножается на $N$. При падении узлов оставшиеся инстансы увеличивают локальную квоту до 100%.
- Кросс-узловая синхронизация лимитов (`replication_bandwidth_limits`): динамические изменения лимитов (включая аварийный сброс в 0 при Disaster Recovery и ручное обновление через `TokenController`) сохраняются в БД через `BandwidthLimitEntity` и синхронизируются между всеми узлами оркестратора.

### Агрегация прогресса в памяти и пакетное подтверждение подзадач (PERF-06)

- Без конкуренции за блокировку строки (`JobProgressBuffer`): при фиксации сотен и тысяч мелких файлов в секунду одиночные вызовы `jobRepository.incrementProgress(...)` заменены буферизацией дельт в памяти на неблокирующих `LongAdder`. Так нет конкуренции потоков за один счетчик и строчных блокировок (`SELECT FOR UPDATE` / row-level lock) строк `replication_jobs` в СУБД.
- Периодический сброс и сброс при остановке (`flushPendingProgress`): накопленные дельты прогресса (байты и число переданных файлов) агрегируются и сбрасываются в БД периодическим таймером раз в 1 секунду (`hadoop.replicator.progress-flush-interval-ms`); это не больше 1 транзакции в секунду на активное задание. При остановке JVM буфер тоже сбрасывается (`@PreDestroy shutdown()`).
- Сброс при финализации (`finalizeJobCompletion`): перед проверкой завершения и обновлением статуса задания буфер принудительно очищается через `drainJob(jobId)`; итоговые счетчики `transferredObjects` и `copiedBytes` учитывают все дельты.
- Пакетный коммит (`completeTasksBatch`): метод переведен на массовую выборку `findAllById` и коммит `saveAll`; вместо $O(N)$ обращений к БД проверка активности задания выполняется 1 раз для каждого затронутого `jobId`.

### Изоляция потоков, пагинация и безопасность (P0, P1, P2)

- [P0] Устранение BOLA/IDOR в инспекции задач и истории запусков: в `JobController.getJobRuns` (`GET /api/v1/jobs/{jobId}/runs`) и `TaskController.getTasks` (`GET /api/v1/jobs/{jobId}/tasks`) выполняется проверка `checkJobReadAccess(jobId, auth, agentSecret)`. Непривилегированным пользователям запрещен просмотр структуры файлов, путей, контрольных сумм, токенов аттестации и истории запусков чужих задач.
- [P0] Виртуальные потоки gRPC Receiver (`ReplicatorAgent`): сервер `NettyServerBuilder` gRPC-приемника агента переведен на виртуальные потоки Java 21 (`Executors.newVirtualThreadPerTaskExecutor()`). Блокирующие вызовы распаковки TAR-стримов и файлового ввода-вывода не блокируют EventLoop-потоки Netty, поэтому heartbeat и прием новых соединений не останавливаются.
- [P0] Принудительная пагинация инспекции задач (`TaskController`): эндпоинт `GET /api/v1/jobs/{jobId}/tasks` дополнен пагинацией по умолчанию (`page=0, size=100`, макс. 1000); без нее создавались сотни тысяч JPA-сущностей и возникал OutOfMemoryError при просмотре заданий с миллионами файлов.
- [P1] Устранение полных сканов (`findAll`) в координаторах: обходы `jobRepository.findAll()` и `hmsReplicationJobRepository.findAll()` заменены на индексные JPQL-запросы `findMaxProcessedTxidByClusterId` в `StreamingLeaseCoordinator` и `findByNormalizedSourceClusterId` в `DisasterRecoveryService`.
- [P1] Изоляция распаковки TAR-стримов (`tarUnpackExecutor`): процесс потоковой распаковки входящих виртуальных TAR-стримов в `DataTransferServiceImpl` вынесен в выделенный пул виртуальных потоков Java 21 (`tar-stream-unpacker-*`); системный пул `ForkJoinPool.commonPool()` JVM для этого не используется.
- [P2] Двухфазный Staging-коммит для Tar-бандлов мелких файлов: в `DataTransferServiceImpl` распаковка файлов бандла переведена на запись во временные staging-файлы с суффиксом `._staging_<jobId>` с последующим атомарным `renameFile`. При аварийном завершении процесса воркера (`kill -9`, kernel OOM) недописанные файлы удаляет фоновым сборщиком `HdfsStagingReaper`.
- [P2] Транскодинг wire-форматов Kafka без мутации входного массива: в `ZeroCopyWireTranscoder` метод `transcode(byte[], int)` переведен на защитную копию массива `.clone()`; данные в общих кэшах и retry-обработчиках не меняются на месте. Для сценариев, где важны аллокации, есть метод `transcodeInPlace(...)`.
- [P2] Декомпозиция валидации токенов аттестации (`TaskAttestationValidator`): проверка HMAC-SHA256 токенов аттестации задач, проверки сроков TTL и валидации Kerberos принципалов вынесена из `DataTransferServiceImpl` в отдельный компонент `TaskAttestationValidator`.

### Масштабирование выше 40 Гбит/с и модель полосы агента

- Динамический параллелизм Kafka Consumer Pool: число потоков вычитки считается по формуле $\text{targetTotalThreads} = \max(64, \lceil \text{bandwidthMbS} / 30.0 \rceil)$ с масштабированием микробатчей от 10k до 50k сообщений (до 16 МБ) для сетей 100G/200G/400G+. В одной JVM агента не больше 32 потоков, чтобы не исчерпать сокеты и не перегрузить CPU.
- Модель полосы как в HDFS Replicator: оркестратор не моделирует сетевую конфигурацию хостов (4x25G, L3-бондинг, маршрутизацию интерфейсов), а согласует WAN HTB квоту и явно заданный оператором лимит процесса `--max-bandwidth-mb-s` (`AGENT_MAX_BANDWIDTH_MB_S`). Доступная полоса пула агентов рассчитывается как $\min(\text{wanQuotaMbS}, \sum \text{agentConfiguredBandwidthMbS})$.
- Изоляция брокерного ISR-трафика: репликация ISR между брокерами Kafka внутри дата-центра идет через локальные сетевые интерфейсы (2x25G) и не учитывается в расчете межЦОДного WAN HTB шейпинга.
- Без констант по умолчанию в API и фронтенде: убран хардкод `defaultValue = "DC1"/"DC2"`, параметр `clusterId` стал обязательным с валидацией 400 Bad Request, адреса брокеров Kafka динамически разрешаются через `TopologyRegistry`. В UI снят потолок 5000 МБ/с и обновлена терминология («Суммарный лимит подключенных агентов» и «Лимит процесса агента»).

### Гонка завершения задач и дедупликация подзадач HMS

- Автоматическая сверка заданий (self-healing reconciliation): выполняется в фоновом сбросе прогресса `TaskService.flushPendingProgress`. Устранено зависание заданий в статусе `RUNNING` из-за конкурентной гонки транзакций (`READ COMMITTED`) при параллельном завершении пофайловых задач пулом воркеров. Задания, у которых переданы все объекты, переводятся в `COMPLETED` в течение 1 секунды.
- Пустые каталоги: задания с 0 объектов больше не зависают в `batchCreateTasks` — директории без файлов сразу финализируются в `COMPLETED` с понятным статусным сообщением.
- Идемпотентность и дедупликация подзадач HMS: в `JobService.findReusableSubjob` и gRPC `createHdfsSubjob` проверяется наличие активных или недавно завершенных HDFS-подзадач для той же таблицы/партиции. При ретраях CDC-событий агент повторно использует существующую подзадачу вместо создания десятков дубликатов.
- Таймауты Data-First Barrier: устранена причина 60-секундных таймаутов ожидания данных при репликации схемы Hive Metastore; конвейер CDC больше не блокируется, ложные ошибки не попадают в Dead-Letter Queue (DLQ).

### Бесконечный реплей CDC, pinning виртуальных потоков и загрузка CPU в простое (P0)

- Устранение бесконечного цикла CDC Hive Metastore: в Protobuf-сообщение `HmsPendingJobItemProto` добавлены недостающие поля `int64 last_processed_event_id`, `bool drop_extraneous_tables`, `bool drop_extraneous_partitions` и `string assigned_agent_id`. Убрано захардкоженное значение `0L` в `OrchestratorGrpcClient.getPendingHmsJobs`, из-за которого агент на каждом такте повторно вычитывал одни и те же CDC-события с начала журнала Hive Metastore и отправлял оркестратору повторные отчеты.
- Устранение Carrier Thread Pinning в gRPC Control Plane: в `OrchestratorGrpcServer` экзекутор Netty переведен с `Executors.newVirtualThreadPerTaskExecutor()` на управляемый пул платформенных потоков `ThreadPoolExecutor` с очередью `LinkedBlockingQueue` и graceful shutdown. Carrier-потоки `ForkJoinPool` больше не блокируются при транзакциях JPA/JDBC/H2, их число не растет, busy-spin в `awaitWork()` устранен.
- Адаптивный Idle Backoff в цикле HMS репликатора: в `ReplicatorAgent.runHmsLoop` добавлен экспоненциальный backoff с джиттером $\pm 20\%$ (базовый интервал 2.0с, максимальный 15.0с) при отсутствии новых CDC-событий; раньше опрос шел каждые 200 мс. Загрузка CPU оркестратора в режиме покоя снизилась с 81.6%–114% до 0.1%–5%, а дисковый Block I/O сокращен с 13.5 GB до 150 KB.

### Черный список consumer group Kafka

- Служебные и инфраструктурные группы: в `ConsumerGroupBlacklistFilter` задан шаблон исключения `^(kafka-replicator-.*|replicator-.*|hadoop-replicator-.*|connect-.*|schema-registry-.*|console-consumer-.*|kafka-exporter-.*|__.*)$`.
- Защита на этапе сканирования (`ConsumerGroupOffsetScanner`): при синхронизации смещений служебные группы отсекаются до запроса оффсетов у Kafka AdminClient; оффсеты локальных консьюмеров агента не перезаписываются.
- Защитный гейт на этапе коммита (`SafetyCommitManager`): при попытке применить оффсеты служебных групп в целевом ДЦ запрос отклоняется с кодом ошибки.
- Фильтрация в Live Discovery и создании задач (`KafkaReplicationOrchestratorService`): из списков обнаружения `/api/v1/kafka/consumer-groups` и при создании заданий служебные группы отфильтровываются.

### Устранение carrier thread pinning в Data Plane и flow control

- Неблокирующий Flow Control в TarStreamPipeline: ожидание готовности сокета Netty `stream.isReady()` переведено с монитора `synchronized (readyLock)` с `wait/notify` на `ReentrantLock` и `Condition.await(100, TimeUnit.MILLISECONDS)`; при перегрузке сокета carrier-потоки ForkJoinPool не блокируются.
- Потокобезопасный gRPC Ack в KafkaDataTransferServiceImpl: отправка подтверждений `responseObserver.onNext(ack)` из пула виртуальных потоков продюсера изолирована отдельным `ReentrantLock responseLock`; монитор StreamObserver в виртуальном потоке не захватывается.
- Локальный шейпер `LocalBandwidthLimiter`: критическая секция вычисления дефицита токенов переведена с `synchronized (lock)` на `ReentrantLock lock`.

---

## Запуск стенда, сборка и тесты

```bash
# Запуск стенда (Orchestrator: 8005, Agent DC1: 50051, Agent DC2: 50052)
make demo-replicator

# Открыть веб-интерфейс
open http://localhost:8005

# Сборка и тесты Java Replicator (Agent + Orchestrator)
make build-replicator
make test-replicator

# Тестирование отдельных модулей (через реактор родителя: общие proto/model
# пересобираются вместе с модулем, а не берутся из локального репозитория)
make test-replicator-agent
make test-replicator-orchestrator
```

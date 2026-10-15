# Hadoop gRPC Replicator

Сервис высокопроизводительной межкластерной репликации данных HDFS (DC1 ➔ DC2 ➔ DC3) через WAN с многоуровневым контролем полосы пропускания (Hierarchical Token Bucket), gRPC-транспортом, изоляцией Kerberos-контекстов и встроенным веб-интерфейсом в дизайн-системе HDFS Explorer.

---

## 1. Архитектурные возможности

1. **Топология дата-центров (DC) и кластеров HDFS**:
   - Привязка каждого кластера HDFS к конкретному ЦОД (на стенде: 2 ЦОД — в DC1 два кластера HDFS, в DC2 один кластер HDFS).
   - Настройка каналов и квот:
     - **DC-DC limits**: ограничение межЦОДных магистралей (`DC1 (Москва) ➔ DC2 (СПб)`: 100 МБ/с).
     - **HDFS-HDFS limits**: выделенные полосы между парами кластеров (`demo-cluster ➔ backup-cluster`: 60 МБ/с, `analytics-cluster ➔ backup-cluster`: 40 МБ/с, внутри DC1: 80 МБ/с).
     - **Global WAN Pool**: общий лимит для всей инфраструктуры репликации.

2. **Многоуровневый шейпинг сетевого трафика (Hierarchical Token Bucket)**:
   - При передаче чанка воркер запрашивает сетевую квоту. Оркестратор проверяет узкое горлышко `max(wait_global, wait_dc_dc, wait_hdfs_hdfs)` и одновременно списывает токены во всех задействованных корзинах.

3. **Аутентификация и ролевой доступ (RBAC)**:
   - Поддержка Kerberos SPNEGO SSO, LDAP и тестовых профилей (`admin_user`, `de_user`, `analyst_user`).
   - **Администратор (`ADMIN` / `admin_user`)**: видит и управляет всеми задачами репликации всех пользователей организации, имеет право указывать произвольного пользователя для doAs-имперсонации (`execution_principal`) в разделах HDFS и HMS.
   - **Пользователь (`USER` / `de_user`, `analyst_user`)**: видит только свои персональные задачи и системные задачи техучетки, поле пользователя имперсонации зафиксировано за текущим логином создателя.

4. **Шедулер задач (Cron Scheduler)**:
   - Запуск периодической синхронизации по расписанию (`@every_5m`, `@hourly`, `@daily`, custom cron).
   - Автоматический расчет `next_run_at` и постановка в очередь фоновым демоном.

5. **Kerberos Proxy User и doAs имперсонация (Apache Ranger Audit & Policy Enforcement)**:
   - Аутентификация в KDC по системному keytab техучетки (`hdfs-replicator@REALM.LOCAL`).
   - Нативная Hadoop doAs-имперсонация конечного пользователя (`impersonate_user` / `execution_principal`) через `org.apache.hadoop.security.UserGroupInformation`.
   - Управление пользователем имперсонации в UI и REST API:
     - В разделе **HDFS** (создание и редактирование задач) и в разделе **HMS** (создание репликации схем) администраторам доступно редактирование имени пользователя (`username` или Kerberos principal).
     - Для непривилегированных пользователей поле заблокировано (read-only) и всегда отображает автора задачи (`session.username()`), с серверным контролем подделки запросов.
     - При репликации HMS схем указанный пользователь автоматически транслируется во все вложенные HDFS-подзадачи переноса данных (`HMS_SUBJOB`).
   - Гарантирует применение политик Apache Ranger на уровне инициатора задачи и корректную фиксацию в Ranger Audit Log (`ugi: user (auth:PROXY via hdfs-replicator)`).

6. **UI Консоль в стиле HDFS Explorer**:
   - Единая палитра `bg-slate-950` / `bg-slate-900` / `border-slate-800` / `text-slate-100`.
   - Модальное окно авторизации точно как `LoginModal.svelte`.
   - Селектор HDFS кластеров в хедере и форме создания задач с отображением ЦОД.

7. **Универсальный полнодуплексный агент (Replicator Agent Java 21)**:
   - Нативный сервис на Java 21 LTS (`agent`) объединяет функции Sender и Receiver.
   - Поддерживает двунаправленную репликацию (`DC1 ⇄ DC2`), катастрофоустойчивый failback и multi-DC топологии.
   - Режимы работы (`AGENT_MODE`):
     - `all` (по умолчанию) — полный дуплекс: параллельный прием входящих файлов по gRPC (:50051) и отправка исходящих задач из очереди оркестратора;
     - `sender` — только отправка исходящих файлов (совместимость с воркером);
     - `receiver` — только прием входящих файлов (совместимость с ресивером).
   - Динамический выбор целевого gRPC узла (`target_address`) на основе топологии кластеров Оркестратора.

8. **Хранилище состояния и метаданных**:
   - Реляционная база данных (H2/PostgreSQL) оркестратора со встроенным Spring Data JPA.
   - Персистентное хранение задач, топологии и истории всех запусков (`JobRun`).

9. **Локальный шейпинг полосы агента (`AGENT_MAX_BANDWIDTH_MB_S`)**:
   - Позволяет безопасно устанавливать агенты непосредственно на DataNode и Edge Nodes без риска вытеснения трафика боевых задач Spark/YARN.
   - Асинхронный Token Bucket троттлинг на отправку (Sender) и TCP Flow Control Backpressure на прием (Receiver).
   - Отображение фактического лимита узлов в UI консоли Оркестратора.

10. **Нативный Java 21 Replicator Agent (`agent`)**:
    - Запуск непосредственно на нодах Hadoop (DataNode, Edge Node) и в контейнерах Apache Hadoop YARN.
    - Прямая работа с HDFS через нативный `org.apache.hadoop.fs.FileSystem` и Kerberos UGI / YARN Delegation Tokens.
    - Встроенные `ReplicatorYarnClient` и `ReplicatorApplicationMaster` для развертывания пула агентов в кластере YARN (`yarn jar replicator-agent-1.0.0-all.jar ...`).
    - Подробная документация: [`agent/README.md`](agent/README.md).

11. **Высокопроизводительный Java 21 / Spring Boot 3 Оркестратор (`orchestrator`)**:
    - Полная реализация оркестратора репликации на Java 21 LTS и Spring Boot 3.3.4.
    - Единое мультимодульное Maven-дерево (`pom.xml`) объединяет агент (`agent`) и оркестратор (`orchestrator`).
    - Бесшовная интеграция с общим ядром безопасности `common-security-starter` (SPNEGO SSO, LDAP, JWT, CSRF, Rate Limiting, Audit).
    - Иерархический Token Bucket шейпинг, динамический реестр агентов с SSRF-защитой и планировщик периодических репликаций.
    - Встроенная раздача собранного Svelte 5 SPA фронтенда.
    - Подробная документация: [`orchestrator/README.md`](orchestrator/README.md).

12. **Батчинг и потоковая упаковка мелких файлов на лету (Small Files Bundling / Tar-Streaming)**:
    - Эффективное решение проблемы «миллионов мелких файлов» (5–50 КБ) в стриминговых логах, CDC и витринах: устраняет колоссальный оверхед по IOPS локальных дисков и транзакциям NameNode.
    - Автоматическая группировка файлов размером меньше `smallFileThresholdBytes` (по умолчанию 1 МБ) в виртуальные бандлы `BUNDLE_TAR` целевого размера ~16 МБ (до 500 файлов в одной задаче).
    - **Zero-Staging архитектура**: упаковка потока на лету в виртуальный TAR (`TarArchiveOutputStream`) и прямая потоковая передача через gRPC (`TransferTarStream`) без сохранения на локальный диск агента.
    - Конвейерная распаковка на приемнике (`TarArchiveInputStream`) и параллельная запись файлов напрямую в целевой HDFS пулом рабочих потоков (`bundleCommitConcurrency`, по умолчанию 8 потоков) с соблюдением Kerberos UGI `doAs`.
    - Для крупных одиночных файлов (`FILE`) локальный staging также заменен на прямой HDFS-staging (`targetPath + "._staging_" + jobId"`) с последующим атомарным `fs.rename()`, полностью исключая нагрузку на локальную дисковую подсистему агентов.
    - Ускорение репликации директорий с миллионами мелких файлов в 5–10 раз.

13. **Многоуровневая автоматическая очистка Staging-файлов на HDFS (HDFS Garbage Collection / Reaper)**:
    - Предотвращение накопления брошенных временных файлов на целевой HDFS при обрывах gRPC сессий, WAN-сбоях или внезапной гибели процессов агентов (`kill -9`, OOM).
    - **Реактивная очистка**: немедленное удаление незавершенного `stagingPath` или всех файлов бандла в `cleanup()` при получении `onError` или исключениях коммита.
    - **Предстартовая очистка**: удаление незавершенных staging файлов предыдущей попытки перед повторным запуском задачи.
    - **Фоновый TTL Reaper**: демон `hdfs-staging-cleaner` периодически (по умолчанию каждые 15 минут) удаляет осиротевшие staging-файлы старше 30 минут по всем известным целевым директориям.
    - **Фильтрация в манифестах**: исключение staging-паттернов из манифестов каталогов и in-memory diff исключает циклическую или ошибочную повторную репликацию мусора.

14. **Сжатие трафика на лету в gRPC-канале (Wire Compression: Zstandard / LZ4)**:
    - Прозрачное сжатие чанков файлов (`TransferFile`) и виртуальных TAR-стримов (`TransferTarStream`) на базе алгоритмов Zstandard (`zstd`) и LZ4 (`lz4`).
    - **Экономия от 40% до 80% полосы межЦОДного WAN** на текстовых логах, JSON, CSV и дампах БД при минимальной нагрузке на процессор агента.
    - **Интеллектуальный пропуск предварительно сжатых форматов**: автоматическое отключение сжатия для файлов Parquet, ORC, Snappy, Gz, Zip, Avro, что защищает процессор от лишней работы.
    - **Адаптивный fallback**: автоматическая отправка данных в несжатом виде (`COMPRESSION_NONE`), если сжатие конкретного чанка не уменьшает его размер.
    - **Точный шейпинг WAN-полосы**: лимиты пропускной способности и токены Hierarchical Token Bucket списываются по фактически переданным сжатым байтам, сохраняя при этом сквозной расчет SHA-256 контрольной суммы по исходному файлу.

15. **Комплексная архитектура отказоустойчивости (High Availability & Zero SPoF)**:
    - Резервирование Оркестратора (Active-Active Stateless API + ShedLock + HA PostgreSQL Patroni).
    - Отказоустойчивый потоковый слушатель HDFS Inotify (Active-Standby пул с распределенным лизингом и Fencing).
    - Строгая изоляция привилегий безопасности (`AGENT_MODE=streamer` без прав исполнения задач).
    - Подробная документация и схемы: [`docs/replicator-fault-tolerance.md`](../../docs/replicator-fault-tolerance.md).

16. **Адаптивный Backpressure при шторме Inotify-событий (Flow Control & Hysteresis)**:
    - Защита памяти агента и БД Оркестратора от лавинообразных всплесков транзакций NameNode (массовое удаление или генерация миллионов файлов).
    - Локальная очередь `InotifyBatchProcessor` с емкостью `inotifyQueueCapacity` (10 000) и микробатчингом (до 500 задач или 1 сек) с дедупликацией путей.
    - Двухуровневый гистерезис насыщения: переход в защитный режим при заполнении очереди $\ge 80\%$ и возврат к штатному опросу только при снижении $\le 30\%$ (без дребезга).
    - Обратная связь от Оркестратора (`queueSaturated` при превышении порога неразобранных задач в пуле `maxPendingTasksThreshold`, по умолчанию 5 000).
    - Прогрессивное регулирование задержки опроса `stream.poll()` агентом от 100 мс до 5 000 мс с принудительным `flush()` накопившихся событий.

17. **Enterprise-мониторинг Micrometer и Prometheus**:
    - Комплексный учет эффективности сжатия трафика: счетчики исходных (`replicator_compression_raw_bytes_total`) и сжатых сетевых байт (`replicator_compression_compressed_bytes_total`), вычисление реального коэффициента сжатия (`replicator_compression_ratio`).
    - Высокоточный замер задержек передачи gRPC-чанков: таймер Micrometer с SLA-бакетами от 1 мс до 5 с (`replicator_grpc_chunk_transfer_latency_seconds_bucket`, `_sum`, `_count`), средняя (`replicator_grpc_chunk_transfer_latency_avg_ms`) и пиковая задержки (`replicator_grpc_chunk_transfer_latency_max_ms`).
    - Мониторинг очереди и буфера Staging на стороне приемника: количество активных незавершенных файлов (`replicator_staging_queue_size`, `replicator_staging_active_files`), совокупный объем в байтах (`replicator_staging_active_bytes`) и число удаленных сборщиком мусора файлов-сирот (`replicator_staging_orphans_cleaned_total`).

18. **Безопасность и целостность данных (Security Hardening)**:
    - **Защита API и устранение Auth Bypass**: все служебные и управляющие REST эндпоинты защищены обязательной аутентификацией (`anyRequest().authenticated()`). Фоновые демоны агентов аутентифицируются через `AgentSecretAuthenticationFilter` по заголовку `X-Agent-Secret`.
    - **Защита от Timing Attack**: сравнение секретов агентов (`AgentRegistry.verifySecret`, `AgentSecretAuthenticationFilter`) переведено на constant-time (`MessageDigest.isEqual`).
    - **Защита от эскалации привилегий doAs (`execution_principal`)**: непривилегированные пользователи не могут подменить целевого Kerberos-пользователя; `execution_principal` принудительно фиксируется за автором запроса.
    - **Защита от Zip Slip и Path Traversal**: строгая нормализация путей в `combinePaths` и распаковщике TAR-стримов с блокировкой любых выходов за пределы целевого каталога (`..`). Блокировка удаления корневой директории `/` в HDFS.
    - **Защита от Decompression Bomb (CWE-400)**: ограничение максимального размера распаковываемого чанка (`MAX_UNCOMPRESSED_CHUNK_SIZE = 64MB`) предотвращает исчерпание памяти (OOM) при декомпрессии.
    - **Защита хостовой файловой системы**: в `HadoopFsManager` удален небезопасный откат на чтение локальной ФС хоста и внедрен шлюз `validatePathSafety`, блокирующий доступ к системным путям ОС (`/etc`, `/proc`, `/sys`, `/dev`, `/root`, `/var/run`, `/boot`).
    - **Гарантированная доставка событий Inotify**: обработка событий переведена на транзакционный ack с backpressure, предотвращая потерю прогресса `txid` при сетевых сбоях.

19. **Высокая производительность и масштабируемость (P1 Performance & Concurrency)**:
    - **Пул gRPC соединений (`ManagedChannel` pool)**: в `ReplicationSender` внедрен постоянный пул gRPC каналов с поддержкой HTTP/2 keepalive (`keepAliveTime: 30s`, `keepAliveTimeout: 10s`), graceful shutdown и эвикцией сбойных каналов при сетевых ошибках (`evictChannel`), устраняя постоянные накладные расходы на TCP и TLS рукопожатия.
    - **Пакетирование сетевых токенов (Batch Token Leasing)**: воркеры запрашивают сетевые токены у оркестратора порциями по 2 МБ на маршрут (`source -> target`), экономя до 97% HTTP round-trip запросов при передаче файлов и TAR-бандлов.
    - **Ликвидация N+1 SQL-запросов**: в `TaskRepository` добавлен прямой JPQL-запрос `countPendingTasksByClusterId`, устранивший сканирование всей таблицы `JobEntity` в `StreamingLeaseCoordinator` и `TaskService`. В забор задач (`claimTasks`) внедрена пагинация `PageRequest` и локальный кэш родительских задач `JobEntity`.
    - **Виртуальные потоки (Virtual Threads / Project Loom)**: в оркестраторе активирован режим `spring.threads.virtual.enabled: true`. В `TokenBucketThrottler` примитивы `synchronized` заменены на `ReentrantLock`, предотвращая блокировку несущих потоков (Carrier Thread Pinning) на Java 21 LTS.
    - **Надежность Disaster Recovery**: активные снимки аварийного останова (`EmergencySnapshot`) перенесены из оперативной памяти в базу данных (`DrSnapshotEntity`, `DrSnapshotRepository`) с автоматическим восстановлением при старте сервиса (`@PostConstruct`).
    - **Перехват отмен gRPC и очистка Staging**: в `DataTransferServiceImpl` зарегистрирован `serverCallStreamObserver.setOnCancelHandler(...)`, выполняющий мгновенную очистку staging-файлов в HDFS и освобождение семафоров при клиентской отмене или разрыве соединения.

20. **Качество кода, безопасность памяти и декомпозиция фронтенда (P2 Code Quality & Architecture)**:
    - **Ограниченный LRU-кэш схем (`BoundedLruCache`)**: в `CachedSchemaRegistryResolver` все неограниченные in-memory кэши (`ConcurrentHashMap`) заменены на потокобезопасные `BoundedLruCache` с лимитом 10 000 записей и политикой LRU-вытеснения. Сетевые I/O запросы в удаленный Schema Registry вынесены из-под блокировок бакетов карты, что устранило риски исчерпания памяти и взаимоблокировок (Deadlock) при параллельной регистрации схем.
    - **Пакетная вставка и обновление в БД (Hibernate JDBC Batching)**: в конфигурацию `application.yml` оркестратора добавлены параметры `hibernate.jdbc.batch_size: 500`, `order_inserts: true`, `order_updates: true` и `batch_versioned_data: true`, сокращающие число сетевых обращений к СУБД при создании и обновлении сотен подзадач репликации в сотни раз.
    - **Модульная декомпозиция SPA фронтенда (`App.svelte`)**: монолитный компонент `App.svelte` (~2560 строк) декомпозирован на независимые, легко сопровождаемые модули в директории `frontend/apps/replicator/src/components/`:
      - `JobsTableView.svelte`: таблица HDFS-задач, карточки статистики, динамические фильтры статуса/автора, полнотекстовый поиск и управление жизненным циклом (Play/Stop/Edit/Delete/History).
      - `TopologyView.svelte`: визуализация дата-центров, привязка кластеров, троттлинг WAN и Keepalive телеметрия агентов (latency, compression ratio, staging buffers).
      - `CreateJobModal.svelte`: модальное окно создания задач с валидацией путей, выбором кластеров, режимов синхронизации (Manual/Scheduled/Inotify) и глубины хранения.
      - `EditJobModal.svelte`: модальное окно редактирования параметров активных и периодических задач.
      - `JobHistoryModal.svelte`: модальное окно с детальной историей всех запусков (`JobRun`), статистикой переданных объемов и настройкой лимита retention.

21. **Тонкая настройка надежности, метрики отмен gRPC и сегментация Actuator (P3 Reliability & Security Segmentation)**:
    - **Graceful остановка gRPC каналов пула**: в методе жизненного цикла `ReplicatorAgent.stop()` внедрен явный вызов `sender.close()`, гарантирующий корректное закрытие всех активных каналов пула `channelPool` и высвобождение сокетов и сетевых ресурсов до остановки фонового демона.
    - **Мониторинг отмен и обрывов gRPC-стримов**: в `AgentMetrics` добавлен счетчик Micrometer `replicator.grpc.transfers.cancelled` (`cancelledTransfersCounter`). Вызовы интегрированы в обработчики клиентской отмены (`serverCallStreamObserver.setOnCancelHandler(...)`) при передаче одиночных файлов (`transferFile`) и потоковых TAR-архивов (`transferTarStream`), обеспечивая прозрачную телеметрию нестабильных WAN-каналов.
    - **Сегментация Spring Boot Actuator**: эндпоинты Actuator оркестратора строго разграничены по уровню доступа:
      - Публичные (`permitAll()`): эндпоинты проверки жизнеспособности (`/actuator/health/**`), базовой информации (`/actuator/info`) и Prometheus-скрейпинга метрик (`/actuator/prometheus`).
      - Защищенные (`hasRole("ADMIN")`): административные и диагностические эндпоинты (`/actuator/metrics/**`, `/actuator/env`, `/actuator/beans`, `/actuator/mappings` и др.), блокирующие несанкционированный доступ к внутренней структуре и метаданным оркестратора.

22. **Комплексный Fencing, надежность кластера и динамический троттлинг (P4 Advanced Reliability & Dynamic QoS)**:
    - **Fencing прямых задач при инверсии (Disaster Recovery Reverse Failback)**: в `DisasterRecoveryService.reverseReplication` внедрена принудительная остановка прямых задач HDFS (`directJobs`) и схем HMS (`directHms`) со статусов `RUNNING`, `QUEUED`, `ACTIVE` и отключение их расписания (`isScheduled = false`) перед запуском обратных репликаций. Это полностью исключает встречную конкурентную запись и зацикливание репликации в одни и те же каталоги HDFS.
    - **Строгое детерминированное сопоставление кластеров**: во всех сервисах оркестратора и агента (`DisasterRecoveryService`, `StreamingLeaseCoordinator`, `HdfsInotifyListener`) нестрогий поиск по подстроке (`contains`) заменен на строгое совпадение нормализованных идентификаторов кластеров (`c1n.equals(c2n)`), предотвращая ошибочные пересечения между похожими кластерами (например, `dc1` и `dc10`).
    - **Динамическое обновление лимитов Token Bucket на лету**: в `TokenBucketThrottler.SingleBucket` добавлен потокобезопасный метод `updateLimit(...)`. При обновлении лимитов каналов в рантайме через UI/API (`setDcLimit`, `setHdfsLimit`) существующие в кэше экземпляры бакетов мгновенно обновляют свои параметры емкости и скорости генерации токенов без перезапуска сервиса.
    - **Self-Fencing стримера Inotify при потере связи**: в `HdfsInotifyListener` внедрен механизм аварийного самоограждения: если демон стримера теряет связь с оркестратором на интервал, превышающий окно безопасности аренды (`LEASE_EXPIRATION_SAFETY_TIMEOUT_MS = 10s`), агент немедленно сбрасывает статус `active.set(false)` и переходит в STANDBY, исключая возникновение Split-Brain и зомби-генерации задач при перевыборах лидера.
    - **Heartbeat Renewal в распределенных блокировках**: в `DistributedLockService.runWithLock` запущен легковесный виртуальный поток автопродления блокировки (Virtual Thread Java 21 `lock-renew-*`), продлевающий аренду каждые `duration / 2` до фактического завершения целевого действия, защищая длительные шедулер-задачи от истечения срока лока при задержках или GC-паузах.

---

## 2. Быстрый запуск

```bash
# Запуск стенда (Orchestrator: 8005, Agent DC1: 50051, Agent DC2: 50052)
make demo-replicator

# Открыть веб-интерфейс
open http://localhost:8005

# Сборка и тесты Java Replicator (Agent + Orchestrator)
make build-replicator
make test-replicator

# Тестирование отдельных модулей
make test-replicator-agent
make test-replicator-orchestrator
```

# Parom Agent (Java 21)

Агент репликации данных Hadoop на Java 21. Варианты запуска:
1. На узлах Hadoop (DataNode, edge-узлы, master-узлы) как фоновый демон (standalone, systemd).
2. В кластере Apache Hadoop YARN как распределенное приложение (YARN Client, ApplicationMaster, контейнеры YARN).

---

## Сравнение с Python-версией агента

| Характеристика | Python Replicator Agent | Java Replicator Agent (LTS 21) |
|---|---|---|
| Интеграция с HDFS | PyArrow / WebHDFS (требует C++ бинарники и glibc) | `org.apache.hadoop.fs.FileSystem` и `HdfsDataInputStream` |
| Kerberos и delegation tokens | Изоляция только через окружение `KRB5CCNAME` | `UserGroupInformation` и YARN delegation tokens |
| Запуск в YARN | Требует упаковки Python venv или conda в архив | Запуск через `yarn jar` или `ParomYarnClient` |
| Шейпинг трафика | Локальный Token Bucket + Orchestrator API | Потокобезопасный Token Bucket + Orchestrator API |
| Транспорт gRPC | Python asyncio / grpcio | Netty Shaded HTTP/2 transport с нулевым копированием буферов |

---

## Компоненты агента

| Класс | Назначение |
|---|---|
| `DataTransferServiceImpl` | gRPC-сервер контракта `parom.proto` (streaming RPC `TransferFile` и `TransferTarStream`). Принимает данные блоками, считает SHA-256 по потоку, распаковывает виртуальные TAR-стримы (`TarArchiveInputStream`) на лету и параллельно коммитит файлы в HDFS пулом потоков (`bundleCommitConcurrency`). Распаковка TAR-стримов идет в отдельном пуле виртуальных потоков Java 21 `tarUnpackExecutor` (`tar-stream-unpacker-*`), а не в `ForkJoinPool.commonPool()`. Файл бандла пишется во временный staging-файл `targetPath + "._staging_" + jobId` и затем атомарно переименовывается через `fsManager.renameFile`. Память распаковщика ограничивают справедливый `Semaphore` (`maxConcurrentTarStreams`), размер pipe-буфера (`tarPipeBufferBytes`) и лимит параллельных коммитов файлов бандла (`fileCommitLimiter`). При обрыве потока (`onError`) `cleanup` удаляет недописанные staging-файлы. |
| `TaskAttestationValidator` | Проверка токенов аттестации задач (HMAC-SHA256): подпись (constant-time сравнение), целевой путь HDFS, принципал Kerberos (`executionPrincipal`), флаг системной учетной записи (`runAsServiceAccount`) и срок действия (TTL). Приемник не выполняет запись, если хоть одна проверка не прошла. |
| `ZeroCopyWireTranscoder` | Транскодер заголовков wire-форматов Confluent Schema Registry и Hortonworks Schema Registry. По умолчанию работает с защитной копией массива байт; `transcodeInPlace(...)` меняет ID схемы на месте без аллокаций. |
| `WireCompressor` | Сжатие и декомпрессия чанков на лету: Zstandard (zstd, уровень по умолчанию 3) и LZ4. На текстовых данных, логах, JSON, CSV и дампах экономит 40–80% полосы WAN. Для сжатых форматов (Parquet, ORC, Snappy, Gz, Zip, Avro) сжатие отключается, несжимаемый чанк передается как есть. |
| `ReplicationSender` | gRPC-клиент с многоуровневым шейпингом полосы и пулом gRPC-каналов. Координирует передачу файлов и бандлов; построение пулов задач делегирует `DirectoryDiffEngine`, потоковую передачу архивов — `TarStreamPipeline`. |
| `DirectoryDiffEngine` | Рекурсивное сканирование директорий HDFS, сравнение манифестов по сети (`DirectoryManifestRequest`), in-memory diff и создание задач в оркестраторе с группировкой мелких файлов в виртуальные бандлы `BUNDLE_TAR`. |
| `TarStreamPipeline` | Сборка, сжатие (`WireCompressor`) и передача виртуальных TAR-бандлов на лету через `TarArchiveOutputStream` с backpressure по flow control gRPC. |
| `HdfsStagingReaper` | Поиск и удаление осиротевших staging-файлов (`*._staging_*`) в HDFS и локальной ФС с фильтром по идентификатору задания (`jobId`) и возрасту (`olderThanMillis`). Удаляет временные файлы одиночных передач и TAR-бандлов после падения воркера (`kill -9`, kernel OOM) или разрыва сети. |
| `KerberosUgiProvider` | Контексты безопасности Kerberos `UserGroupInformation` и doAs-имперсонация. Проверяет принципалы (`validatePrincipalSafety`), запрещает системные учетные записи (`root`, `bin`, `daemon`), содержит fallback для новых JVM. |
| `AgentMetrics` | Метрики Micrometer (`SimpleMeterRegistry`): объемы и коэффициент сжатия (`compression_ratio`), таймер задержки передачи gRPC-чанков (`replicator.grpc.chunk.transfer.latency`, SLA-бакеты от 1 мс до 5 с), очередь и байты буфера staging (`staging.queue.size`, `staging.active.bytes`, `staging.orphans.cleaned`). Агрегаты передаются оркестратору в heartbeat. |
| `ParomAgent` | Цикл воркера и отправителя задач. Пул `workerTaskPool` (`Executors.newFixedThreadPool(workerConcurrency)`) параллельно выполняет задачи передачи (`transferTask`) с лимитом одновременных передач (`activeTransfers`) и graceful shutdown. gRPC-сервер приема работает на виртуальных потоках Java 21 (`Executors.newVirtualThreadPerTaskExecutor()`), поэтому долгая распаковка TAR-стримов не блокирует EventLoop-потоки Netty. В циклах `runSenderLoop` и `runHmsLoop` при отсутствии новых CDC-событий интервал опроса растет экспоненциально с 2 до 15 секунд со случайным разбросом $\pm 20\%$, чтобы сотни агентов не опрашивали очередь одновременно. |
| `OrchestratorGrpcClient` | Клиент gRPC Control Plane оркестратора (`orchestrator_control_plane.proto`) через постоянный мультиплексированный HTTP/2-канал: регистрация агента, heartbeat с телеметрией, взятие пофайловых задач из очереди (`claimTasks`), подтверждение завершения (`completeTask`), запрос квот Token Bucket, лизинг Inotify-стримеров и координация репликации схем HMS (с `last_processed_event_id`, флагами `dropExtraneousTables`/`dropExtraneousPartitions` и назначенным агентом). Если в `ORCHESTRATOR_GRPC_ADDRESSES` передан список адресов, клиент перемешивает их при старте (client-side random load balancing) и при сбое узла (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, обрыв сокета) повторяет вызов на следующем узле; внешний L7-балансировщик не нужен. TLS/mTLS проверяет сертификаты по системным доверенным CA (TrustManager JVM); `insecureSkipVerify` — только для локальной отладки. |
| `HadoopFsManager` | Работа с HDFS и локальной ФС; Kerberos UGI делегирует `KerberosUgiProvider`, очистку staging — `HdfsStagingReaper`. |
| `ParomYarnClient`, `ParomApplicationMaster` | Развертывание пула агентов в контейнерах YARN по запросу. |

---

## Сборка

Требования:
- JDK 21 LTS. На другой версии JDK сборку модулей репликатора останавливает `maven-enforcer-plugin`; на JDK 24+ Hadoop 3.3.6 теряет имперсонацию (`Subject.getSubject` бросает `UnsupportedOperationException`).
- Apache Maven 3.8+

```bash
# Из корня репозитория:
make build-parom-agent

# Либо напрямую через Maven, через реактор родителя:
mvn clean package -DskipTests -f backend/parom/pom.xml -pl agent -am
```

Собирайте модуль только через реактор `backend/parom/pom.xml` с `-pl agent -am`. При запуске с `-f backend/parom/agent/pom.xml` общие модули `proto` и `model` берутся из локального Maven-репозитория и могут быть устаревшими.

Результат сборки — исполняемый fat JAR:
`target/parom-agent-1.0.0-all.jar` (~72 МБ, включает все зависимости Netty, gRPC, Hadoop Client, YARN Client, Jackson).

---

## Запуск на узлах Hadoop (standalone)

### Запуск через скрипт

```bash
./backend/parom/agent/bin/parom-agent.sh \
    --agent-id agent-dn-01 \
    --cluster-id demo-cluster \
    --orchestrator http://orchestrator-host:8005 \
    --port 50051 \
    --bandwidth 100 \
    --staging-dir /tmp/parom-staging
```

### Запуск через java с classpath Hadoop

```bash
# С использованием системного hadoop classpath:
export HADOOP_CLASSPATH=$(hadoop classpath)
java -Xms1g -Xmx4g -cp "target/parom-agent-1.0.0-all.jar:${HADOOP_CLASSPATH}" \
    io.github.balookrd.pristan.parom.agent.ParomAgentMain \
    --agent-id agent-node-01 \
    --cluster-id demo-cluster \
    --orchestrator http://localhost:8005 \
    --port 50051 \
    --mode all
```

### Переменные окружения

| Переменная | По умолчанию | Что задает |
|---|---|---|
| `AGENT_ID` | `agent-<uuid>` | Уникальный ID агента |
| `AGENT_CLUSTER_ID` | `null` | Идентификатор обслуживаемого HDFS-кластера |
| `AGENT_MODE` | `all` | Режим работы: `all` (дуплекс), `sender`, `receiver` |
| `ORCHESTRATOR_GRPC_ADDRESSES` | `${ORCHESTRATOR_GRPC_ADDRESS}` | Адреса gRPC Control Plane пула оркестраторов через запятую (например, `orch-1:50050,orch-2:50050`). Агент перемешивает их при старте (client-side random LB) и при сбое узла переключается на следующий |
| `ORCHESTRATOR_GRPC_ADDRESS` | `localhost:50050` (`orchestrator:50050` в docker) | Адрес gRPC Control Plane оркестратора (`host:port`): одиночный адрес или значение по умолчанию для `ORCHESTRATOR_GRPC_ADDRESSES` |
| `RECEIVER_HOST` | `0.0.0.0` | Адрес, на котором gRPC Data Plane принимает входящие соединения |
| `RECEIVER_PORT` | `50051` | Порт gRPC сервиса приема файлов |
| `AGENT_MAX_BANDWIDTH_MB_S` | `0.0` (без ограничений) | Локальный лимит скорости репликации в МБ/с |
| `PAROM_STAGING_DIR` | `/tmp/staging` | Директория временных файлов для локальной ФС (для HDFS staging-файл `._staging_<jobId>` создается в самом HDFS) |
| `PAROM_SMALL_FILE_THRESHOLD_BYTES` | `1048576` (1 МБ) | Порог размера файла для группировки в виртуальные TAR-бандлы |
| `PAROM_BUNDLE_TARGET_SIZE_BYTES` | `16777216` (16 МБ) | Целевой суммарный объем файлов в одном бандле `BUNDLE_TAR` |
| `PAROM_MAX_BUNDLE_FILES` | `500` | Максимальное количество файлов в одном бандле `BUNDLE_TAR` |
| `PAROM_BUNDLE_COMMIT_CONCURRENCY` | `8` | Число параллельных потоков записи распаковываемых файлов в HDFS |
| `PAROM_MAX_CONCURRENT_TAR_STREAMS` | `16` | Максимальное число параллельных потоков TAR-распаковки на приемнике (семафор ограничивает расход памяти) |
| `PAROM_TAR_PIPE_BUFFER_BYTES` | `2097152` (2 МБ) | Размер буфера PipedInputStream для виртуального TAR-стрима; ограничивает память на один стрим |
| `PAROM_STAGING_CLEANUP_ENABLED` | `true` | Включает фоновую очистку осиротевших staging-файлов |
| `PAROM_STAGING_CLEANUP_INTERVAL_MINUTES` | `15` | Интервал периодического сканирования и очистки staging-файлов (минуты) |
| `PAROM_STAGING_TTL_MINUTES` | `30` | Время жизни (TTL) staging-файлов, после которого они считаются осиротевшими и удаляются |
| `HDFS_DEFAULT_FS` | `core-site.xml` | URI HDFS NameNode (например, `hdfs://namenode:8020`) |
| `PAROM_AGENT_SECRET` | `null` | Секретный токен авторизации (`X-Agent-Secret`) |
| `PAROM_GRPC_TLS_ENABLED` | `false` | Включает TLS для gRPC |
| `PAROM_GRPC_CERT_CHAIN_PATH` | `null` | Путь к сертификату сервера/клиента gRPC (X.509 PEM) |
| `PAROM_GRPC_PRIVATE_KEY_PATH` | `null` | Путь к приватному ключу gRPC (PKCS8 PEM) |
| `PAROM_GRPC_TRUST_CERT_COLLECTION_PATH` | `null` | Путь к доверенным CA сертификатам для проверки пиров |
| `PAROM_GRPC_CLIENT_AUTH` | `NONE` | Режим взаимной аутентификации (mTLS): `NONE`, `OPTIONAL`, `REQUIRE` |
| `PAROM_GRPC_INSECURE_SKIP_VERIFY` | `false` | Отключение проверки TLS сертификатов для gRPC (тестовые стенды) |
| `ORCHESTRATOR_TLS_INSECURE_SKIP_VERIFY` | `false` | Отключение проверки HTTPS-сертификатов оркестратора |
| `KRB5_KEYTAB` | `null` | Путь к keytab-файлу Kerberos |
| `KRB5_PRINCIPAL` | `null` | Kerberos Principal (например, `parom@REALM`) |
| `PAROM_WIRE_COMPRESSION` | `zstd` | Кодек потокового сжатия трафика на лету: `zstd` (Zstandard), `lz4` (LZ4), `none` (отключено) |
| `PAROM_WIRE_COMPRESSION_LEVEL` | `3` | Уровень сжатия для Zstandard (1–22); выше уровень — сильнее сжатие и больше нагрузка на CPU |
| `INOTIFY_QUEUE_CAPACITY` | `10000` | Максимальная емкость локальной очереди `InotifyBatchProcessor` |
| `INOTIFY_HIGH_WATERMARK` | `0.8` | Верхний порог заполнения очереди для включения backpressure ($\ge 80\%$) |
| `INOTIFY_LOW_WATERMARK` | `0.3` | Нижний порог заполнения очереди для снятия backpressure ($\le 30\%$) |
| `INOTIFY_MAX_BATCH_SIZE` | `500` | Максимальный размер микробатча Inotify-задач к отправке в оркестратор |
| `INOTIFY_FLUSH_INTERVAL_MS` | `1000` | Интервал периодического сброса накопившегося батча (мс) |
| `INOTIFY_BACKPRESSURE_MAX_DELAY_MS` | `5000` | Максимальная задержка цикла опроса `stream.poll()` при насыщении (мс) |

---

### Сжатие трафика в канале gRPC (Zstandard, LZ4)

Агент сжимает чанки перед отправкой по WAN (wire compression). Кодек задает `PAROM_WIRE_COMPRESSION`:
- Zstandard (`zstd`) — кодек по умолчанию; на текстовых логах, JSON, CSV и дампах БД экономит 40–80% полосы WAN.
- LZ4 (`lz4`) — распаковка 2–4 ГБ/с на ядро и быстрее, для каналов WAN 10/40/100 Гбит/с.
- Для файлов, сжатых на уровне формата (`.parquet`, `.orc`, `.snappy`, `.gz`, `.gzip`, `.zst`, `.lz4`, `.bz2`, `.zip`, `.avro`), сжатие отключается (`COMPRESSION_NONE`): повторное сжатие таких блоков тратит CPU и не уменьшает размер.
- Если сжатие чанка не уменьшило размер (`compressedSize >= rawSize`), чанк передается несжатым (`COMPRESSION_NONE`).
- SHA-256 считается по исходному содержимому файла, а токены Token Bucket списываются по фактически переданным сжатым байтам.

## Запуск в кластере Apache Hadoop YARN

В агент входят YARN Client и ApplicationMaster: пул агентов запускается в контейнерах NodeManager с заданными памятью и числом ядер CPU.

### Запуск через скрипт

```bash
./backend/parom/agent/bin/submit-yarn.sh \
    --cluster_id demo-cluster \
    --orchestrator http://orchestrator-host:8005 \
    --num_containers 2 \
    --memory 2048 \
    --vcores 1 \
    --queue default
```

### Запуск через `hadoop jar`

```bash
hadoop jar target/parom-agent-1.0.0-all.jar \
    io.github.balookrd.pristan.parom.yarn.ParomYarnClient \
    --jar target/parom-agent-1.0.0-all.jar \
    --cluster_id demo-cluster \
    --orchestrator http://orchestrator-host:8005 \
    --num_containers 2 \
    --memory 2048 \
    --vcores 1 \
    --queue default
```

Что происходит после запуска:
1. `ParomYarnClient` загружает JAR в HDFS staging директорию `.parom-staging/<appId>`.
2. YARN ResourceManager выделяет контейнер для `ParomApplicationMaster`.
3. ApplicationMaster регистрируется в RM и запрашивает указанное количество контейнеров (`num_containers`).
4. На выделенных узлах запускаются контейнеры с `ParomAgentMain`, которые регистрируются в оркестраторе вызовом gRPC `RegisterAgent` сервиса Control Plane (`orchestrator_control_plane.proto`) и начинают обрабатывать задачи репликации из очереди.
5. При остановке приложения в YARN (`yarn application -kill <appId>`) агенты снимаются с регистрации (статус OFFLINE) и освобождают ресурсы.

---

## Запуск тестов

```bash
# Запуск JUnit 5 тестов (LocalBandwidthLimiter, InProcess gRPC, ParomAgent):
make test-parom-agent
```

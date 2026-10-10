# 🚀 Hadoop gRPC Replicator Agent (Java 21)

Высокопроизводительный нативный Java 21 агент репликации данных для экосистемы Hadoop, разработанный для запуска:
1. **Непосредственно на нодах Hadoop (DataNode, Edge Nodes, Master Nodes)** как автономный фоновый демон (Standalone Daemon / systemd).
2. **В кластере Apache Hadoop YARN** в виде распределенного приложения (YARN Client + ApplicationMaster + YARN Containers).

---

## 1. Преимущества Java-версии агента

| Характеристика | Python Replicator Agent | Java Replicator Agent (LTS 21) |
|---|---|---|
| **Интеграция с HDFS** | PyArrow / WebHDFS (требует C++ бинарники и glibc) | Нативный `org.apache.hadoop.fs.FileSystem` и `HdfsDataInputStream` |
| **Kerberos & Delegation Tokens** | Ограниченная изоляция окружения `KRB5CCNAME` | Полная поддержка `UserGroupInformation` и YARN Delegation Tokens |
| **Запуск в YARN** | Требует упаковку Python venv / conda в архив | Нативный запуск через `yarn jar` или `ReplicatorYarnClient` |
| **Шейпинг трафика** | Локальный Token Bucket + Orchestrator API | Нативный потокобезопасный Token Bucket + Orchestrator API |
| **Производительность gRPC** | Python asyncio / grpcio | Netty Shaded HTTP/2 transport с нулевым копированием буферов |

---

## 2. Архитектура и компоненты

- **`DataTransferServiceImpl`**: gRPC-сервер, реализующий контракт `replicator.proto` (`TransferFile` и `TransferTarStream` streaming RPC). Поддерживает потоковый прием данных блоками, потоковый расчет SHA-256 контрольной суммы, потоковую распаковку виртуальных TAR-стримов (`TarArchiveInputStream`) на лету и прямой параллельный коммит файлов в HDFS пулом потоков (`bundleCommitConcurrency`) без промежуточной записи на локальный диск (**Zero-Staging архитектура**). Включает реактивную очистку (`cleanup`) недописанных staging-файлов при обрыве потока (`onError`).
- **`ReplicationSender`**: gRPC-клиент с поддержкой многоуровневого шейпинга пропускной способности. Читает файлы из HDFS и потоково передает их в удаленный gRPC Receiver (одиночные файлы через `TransferFile`, сгруппированные мелкие файлы через виртуальный `TarArchiveOutputStream` и RPC `TransferTarStream`). Выполняет pre-flight очистку старых staging-файлов задания перед началом репликации.
- **`HdfsStagingCleaner` / TTL Reaper**: Фоновый сборщик мусора агента, периодически сканирующий зарегистрированные целевые директории и удаляющий осиротевшие staging-файлы (`*._staging_*`), оставшиеся от аварийно упавших агентов (по порогу TTL).
- **`LocalBandwidthLimiter`**: Локальный Token Bucket ограничитель полосы пропускания для предотвращения вытеснения рабочего трафика Spark/YARN на DataNode.
- **`OrchestratorClient`**: Взаимодействие с REST API Оркестратора через `java.net.http.HttpClient` (регистрация в реестре, keepalive heartbeat, опрос очереди задач, запрос квот Token Bucket, обновление прогресса).
- **`ReplicatorYarnClient` & `ReplicatorApplicationMaster`**: Автономная интеграция с Apache Hadoop YARN для развертывания пула агентов в контейнерах YARN по запросу.

---

## 3. Сборка проекта

Требования:
- Java Development Kit (JDK 21 LTS или новее)
- Apache Maven 3.8+

```bash
# Из корня репозитория:
make build-replicator-agent

# Либо напрямую через Maven:
mvn clean package -DskipTests -f backend/replicator/agent/pom.xml
```

В результате сборки формируется единый исполняемый Fat JAR:
`target/replicator-agent-1.0.0-all.jar` (~72 МБ, включает все зависимости Netty, gRPC, Hadoop Client, YARN Client, Jackson).

---

## 4. Запуск на нодах Hadoop (Standalone Daemon)

### 4.1. Быстрый запуск через скрипт

```bash
./backend/replicator/agent/bin/replicator-agent.sh \
    --agent-id agent-dn-01 \
    --cluster-id demo-cluster \
    --orchestrator http://orchestrator-host:8005 \
    --port 50051 \
    --bandwidth 100 \
    --staging-dir /tmp/replicator-staging
```

### 4.2. Прямой запуск через Java / Hadoop CLI

```bash
# С использованием системного hadoop classpath:
export HADOOP_CLASSPATH=$(hadoop classpath)
java -Xms1g -Xmx4g -cp "target/replicator-agent-1.0.0-all.jar:${HADOOP_CLASSPATH}" \
    org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentMain \
    --agent-id agent-node-01 \
    --cluster-id demo-cluster \
    --orchestrator http://localhost:8005 \
    --port 50051 \
    --mode all
```

### 4.3. Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `AGENT_ID` | `agent-<uuid>` | Уникальный ID агента в системе |
| `AGENT_CLUSTER_ID` | `null` | Идентификатор обслуживаемого HDFS-кластера |
| `AGENT_MODE` | `all` | Режим работы: `all` (дуплекс), `sender`, `receiver` |
| `ORCHESTRATOR_URL` | `http://localhost:8005` | URL REST API Оркестратора |
| `RECEIVER_HOST` | `0.0.0.0` | Сетевой адрес для прослушивания gRPC |
| `RECEIVER_PORT` | `50051` | Порт gRPC сервиса приема файлов |
| `AGENT_MAX_BANDWIDTH_MB_S` | `0.0` (без ограничений) | Локальный лимит скорости репликации в МБ/с |
| `REPLICATOR_STAGING_DIR` | `/tmp/staging` | Директория временных файлов для локальной ФС (при работе с HDFS используется прямой Zero-Staging `._staging_<jobId>` в HDFS) |
| `REPLICATOR_SMALL_FILE_THRESHOLD_BYTES` | `1048576` (1 МБ) | Порог размера файла для группировки в виртуальные TAR-бандлы |
| `REPLICATOR_BUNDLE_TARGET_SIZE_BYTES` | `16777216` (16 МБ) | Целевой совокупный объем файлов в одном бандле `BUNDLE_TAR` |
| `REPLICATOR_MAX_BUNDLE_FILES` | `500` | Максимальное количество файлов в одном бандле `BUNDLE_TAR` |
| `REPLICATOR_BUNDLE_COMMIT_CONCURRENCY` | `8` | Число параллельных потоков прямой записи распаковываемых файлов в HDFS |
| `REPLICATOR_STAGING_CLEANUP_ENABLED` | `true` | Включение фонового сборщика мусора осиротевших staging-файлов |
| `REPLICATOR_STAGING_CLEANUP_INTERVAL_MINUTES` | `15` | Интервал периодического сканирования и очистки staging-файлов (минуты) |
| `REPLICATOR_STAGING_TTL_MINUTES` | `30` | Время жизни (TTL) staging-файлов, после которого они считаются осиротевшими и удаляются |
| `HDFS_DEFAULT_FS` | `core-site.xml` | URI HDFS NameNode (например, `hdfs://namenode:8020`) |
| `REPLICATOR_AGENT_SECRET` | `null` | Секретный токен авторизации (`X-Agent-Secret`) |
| `REPLICATOR_GRPC_TLS_ENABLED` | `false` | Включение защищенного TLS канала для gRPC |
| `REPLICATOR_GRPC_CERT_CHAIN_PATH` | `null` | Путь к сертификату сервера/клиента gRPC (X.509 PEM) |
| `REPLICATOR_GRPC_PRIVATE_KEY_PATH` | `null` | Путь к приватному ключу gRPC (PKCS8 PEM) |
| `REPLICATOR_GRPC_TRUST_CERT_COLLECTION_PATH` | `null` | Путь к доверенным CA сертификатам для проверки пиров |
| `REPLICATOR_GRPC_CLIENT_AUTH` | `NONE` | Режим взаимной аутентификации (mTLS): `NONE`, `OPTIONAL`, `REQUIRE` |
| `REPLICATOR_GRPC_INSECURE_SKIP_VERIFY` | `false` | Отключение проверки TLS сертификатов для gRPC (тестовые стенды) |
| `ORCHESTRATOR_TLS_INSECURE_SKIP_VERIFY` | `false` | Отключение проверки HTTPS сертификатов Оркестратора |
| `KRB5_KEYTAB` | `null` | Путь к Kerberos Keytab файлу |
| `KRB5_PRINCIPAL` | `null` | Kerberos Principal (например, `hdfs-replicator@REALM`) |

---

## 5. Запуск в кластере Apache Hadoop YARN

Replicator Agent предоставляет встроенный YARN Client и ApplicationMaster, что позволяет запускать пул агентов в изолированных контейнерах NodeManager с выделенными ресурсами памяти и ядер CPU.

### 5.1. Запуск через вспомогательный скрипт

```bash
./backend/replicator/agent/bin/submit-yarn.sh \
    --cluster_id demo-cluster \
    --orchestrator http://orchestrator-host:8005 \
    --num_containers 2 \
    --memory 2048 \
    --vcores 1 \
    --queue default
```

### 5.2. Запуск через стандартную команду `hadoop jar`

```bash
hadoop jar target/replicator-agent-1.0.0-all.jar \
    org.apache.hadoop.explorer.replicator.yarn.ReplicatorYarnClient \
    --jar target/replicator-agent-1.0.0-all.jar \
    --cluster_id demo-cluster \
    --orchestrator http://orchestrator-host:8005 \
    --num_containers 2 \
    --memory 2048 \
    --vcores 1 \
    --queue default
```

При этом:
1. `ReplicatorYarnClient` загрузит JAR в HDFS staging директорию `.replicator-staging/<appId>`.
2. YARN ResourceManager выделит контейнер для `ReplicatorApplicationMaster`.
3. ApplicationMaster зарегистрируется в RM и запросит указанное количество контейнеров (`num_containers`).
4. На выделенных нодах запустятся контейнеры с `ReplicatorAgentMain`, которые автоматически зарегистрируются в Оркестраторе (`POST /api/v1/agents/register`) и начнут обрабатывать задачи репликации из очереди.
5. При остановке приложения в YARN (`yarn application -kill <appId>`) агенты корректно снимутся с регистрации (OFFLINE) и освободят ресурсы.

---

## 6. Тестирование

```bash
# Запуск JUnit 5 тестов (LocalBandwidthLimiter, InProcess gRPC, ReplicatorAgent):
make test-replicator-agent
```

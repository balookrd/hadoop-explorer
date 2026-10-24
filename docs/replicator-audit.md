# Аудит Hadoop gRPC Replicator — 24 октября 2026

Объём: `backend/replicator` — 31 510 строк main-кода (agent 404 файла / orchestrator), 15 893 строк тестов,
77 тест-классов, **450 тестов, 0 падений** (прогон `mvn test -f backend/replicator/pom.xml` выполнен в рамках аудита).

Метод: AST-индекс (`scripts/java-index-query.sh`) для навигации без чтения файлов целиком, точечное чтение
горячих участков, прогон тестов, `ast-grep scan`, разбор конфигурации и развёртывания. Предыдущие аудиты
из памяти Gemini (`~/.gemini/antigravity/brain/*`, 28 документов по replicator) использованы как **список
гипотез для перепроверки**, а не как факты: часть заявленных там исправлений не подтвердилась (раздел 7).

---

## 1. Сводная оценка

| Направление | Оценка | Ключевой вывод |
|---|:---:|---|
| Архитектура и дизайн | **6 / 10** | Разделение control/data plane продумано, но оркестратор зависит от агента (compile scope), критичное состояние (лимиты, DR-снимки, реестр агентов) живёт в памяти узла вопреки заявленному active-active, контракты proto продублированы, DDL-схема разошлась с JPA-моделью |
| Производительность | **5.5 / 10** | Много грамотных механизмов (SKIP LOCKED-очередь, AIMD-лизинг, HRW-балансировка, bounded-LRU), но открытый `/metrics` грузит всю таблицу задач, `merge()` вместо `persist` обесценивает batching, `CallerRunsPolicy` на gRPC-executor, нет flow-control на пофайловом пути, N+1 к HMS, троттлинг внутри poll-цикла Kafka |
| Отказоустойчивость | **4 / 10** | Систематическая «тихая потеря данных»: позиция/оффсет продвигается независимо от фактического успеха — одна и та же ошибка в трёх подсистемах (HDFS inotify, Kafka, HMS). Плюс DR-ограждение фактически не ограждает, эпоха fencing-токена немонотонна, блокировка берётся внутри бизнес-транзакции |
| Качество кода | **6.5 / 10** | Единый стиль, развитая тестовая база, но 252 широких `catch (Exception)`, God-классы ~1000 строк, несколько «мёртвых» функций, выдающих успех |
| Безопасность | **5.5 / 10** | Защитные механизмы реализованы качественно (HMAC-аттестация, chroot, constant-time сравнение), но **по умолчанию почти все выключены**, а control plane fail-open |

**Главный вывод.** Это не «сырой» код: механизмы защиты и отказоустойчивости здесь продуманнее, чем
в среднем проекте такого класса. Проблема в другом — в систематическом разрыве между *декларацией*
и *фактическим поведением*: зелёные тесты не покрывают продовую конфигурацию, заявленные в README
исправления местами не работают, а защита по умолчанию отключена. Четыре сквозных паттерна ниже
объясняют большинство найденных дефектов.

---

## 2. Четыре сквозных паттерна (корень большинства дефектов)

### 2.1. Fail-open в обработке ошибок: ошибка превращается в «успех»
Единый антипаттерн — `catch (Exception) { log; return <дефолт> }`, где дефолт неотличим от нормального результата:

| Место | Возврат при ошибке | Чем это становится |
|---|---|---|
| `fs/HadoopFsManager.java:328` `exists()` | `false` | файл «не существует» → пропущен в бандле, задача «успешна» |
| `fs/HadoopFsManager.java:316` `getFileSize()` | `0` | нулевой размер в манифесте |
| `client/OrchestratorGrpcClient.java:386` `requestNetworkTokens()` | `0.0` | квота WAN не ограничивает |
| `client/OrchestratorGrpcClient.java:673` `renewStreamingLease()` | `null` | self-fencing не срабатывает (см. P0-7) |
| `hms/client/ThriftHmsClient.java:250` `getCurrentNotificationEventId()` | `0` | позиция CDC обнуляется |
| `hms/client/ThriftHmsClient.java` `getNextNotifications()` | пустой список | «лаг 0, всё синхронизировано» |
| `hms/client/HttpRemoteHmsClient.java:170-273` | без проверки HTTP-кода | 401/500 = «DDL применён» |
| `kafka/schema/CachedSchemaRegistryResolver.java:197` | исходный schema ID | запись с чужой схемой в целевом кластере |
| `kafka/offset/SafetyCommitManager.java:152` | `false` («группы нет») | перезапись оффсетов живой consumer-группы |

Всего в модуле **252 блока `catch (Exception|Throwable)`** на 31.5k строк.

### 2.2. Позиция прогресса продвигается независимо от фактического успеха
Одна и та же архитектурная ошибка в трёх независимых подсистемах:

- **Kafka**: `pendingCommittedOffsets.merge(tp, maxOffset+1, Math::max)` — watermark берёт максимум по всем
  батчам партиции, при `ack.success == false` только пишется лог. Успешный поздний батч коммитит оффсет за
  упавший ранний → записи между ними потеряны безвозвратно.
- **HMS**: `lastProcessedEventId` принимает любое значение из отчёта агента без проверки монотонности и
  без проверки владельца lease; «ядовитое» событие уходит в DLQ, а позиция двигается вперёд.
- **HDFS inotify**: `currentTxid` выставляется **до** обработки батча; при переполнении очереди поток
  переоткрывается с `currentTxid + 1`, необработанные события теряются.

Во всех трёх случаях внешне задача остаётся зелёной — расхождение обнаруживается только сверкой данных.

### 2.3. Защита реализована, но по умолчанию выключена
| Параметр | Дефолт | Последствие |
|---|---|---|
| `hadoop.security.auth.mode` | `mock` + 5 пользователей с `password123` в `application.yml` | прод стартует с mock-аутентификацией, если не переопределить |
| `agent-secret` | хардкод `replicator-secure-agent-secret-key-12345` (в коде и в yml) | известный публичный секрет |
| `enforce-agent-secret` | `false` | регистрация агента без секрета (`AgentRegistry.register`: секрет проверяется только если заголовок прислан) |
| `enforce-cluster-whitelist` | `false` | агент регистрируется в произвольном `clusterId` |
| `REPLICATOR_ENFORCE_TASK_ATTESTATION` | `false` | HMAC-аттестация задач (защита от privilege escalation) не требуется |
| `grpc.tls-enabled` | `false` | control plane и data plane в plaintext |
| `jwt.secret-key` | хардкод в `application.yml` | подделка токенов |
| `cookie.secure` | `false` | — |
| `spring.datasource` | `jdbc:h2:mem:` + `ddl-auto: update` | состояние задач теряется при рестарте; active-active невозможен |
| Dockerfile (оба) | нет `USER` | контейнеры работают от root |

### 2.4. «Исправлено» ≠ работает: мёртвые механизмы, рапортующие успех
- `inotify/InotifyBatchProcessor.java:260` `enqueueDelete()` — только `log.debug`; при включённом
  `syncDeletes` удаления не реплицируются никогда.
- Self-fencing стримера (>10 с без оркестратора) — недостижим (см. P0-7).
- `HmsCoordinatorService.scanSchemaDrift()` — всегда `noDrift`; рабочий `HmsSchemaDriftDetector`
  вызывается только из теста. `POST /{id}/sync` возвращает `success`, ничего не делая.
- Транскодинг схем Kafka — в реальном агенте создаётся passthrough-резолвер, флаг
  `schemaTranscodingEnabled` ни на что не влияет.
- Offset translation (`ConsumerGroupOffsetScanner`, `syncOffsets`) — нет вызовов в продакшн-коде;
  при failover consumer-группы в целевом ДЦ окажутся на `earliest`/`latest`.
- `HmsPathRewriter.PathRewriteResult.unmapped` — нигде не читается; при отсутствии маппинга федерации
  таблица создаётся с location исходного кластера.
- `schema-postgresql.sql` (18 индексов, партиционирование, частичные индексы) **не подключён**
  (`spring.sql.init` отсутствует, Flyway/Liquibase нет) — схему строит `ddl-auto: update`.
- Сетевое ограждение DR: `setDcLimit(..., 0L)` означает в троттлере «без лимита», то есть Kill-Switch
  с `fenceNetwork=true` ничего не перекрывает, но рисует `isFenced=true` (P0-17).
- Механизм персистентности лимитов между узлами (`persistAndApplyLimit`) написан, но DR и
  `TopologyController` его не вызывают — пишут в локальную память.

---

## 3. P0 — критические дефекты

### Потеря данных

**P0-1 | HDFS-приёмник: откат удаляет уже закоммиченные файлы**
`receiver/DataTransferServiceImpl.java:842-852` — `cleanup()` в `finally` удаляет пути из
`activeWrittenPaths`, то есть **финальные целевые файлы**, успешно закоммиченные через rename.
Набор очищается только при полностью успешном завершении (`:790`).
*Сценарий*: бандл из 500 файлов, один упал → удаляются 499 корректных. То же при отмене клиентом
(таймаут 120 с в `sender/TarStreamPipeline.java:199`) и при таймауте распаковки.
Вместе с P0-2 это уничтожает и предыдущую версию целевых файлов.
*Фикс*: откатывать только `activeStagingPaths`; закоммиченное не удалять (повторная передача идемпотентна).

**P0-2 | Неатомарный «атомарный» коммит в HDFS**
`fs/HadoopFsManager.java:953-956` — `if (fs.exists(dst)) fs.delete(dst,false); return fs.rename(src,dst);`
Если после `delete` rename не проходит (пропал staging, failover NameNode, AccessControlException),
прежний целевой файл утрачен. Плюс 3 RPC к NameNode на файл.
*Фикс*: `FileContext.rename(src, dst, Options.Rename.OVERWRITE)` — единственный атомарный overwrite-rename.

**P0-3 | Kafka: коммит оффсета за упавшим батчем**
`kafka/consumer/KafkaBatchConsumerPool.java:219-238` — watermark двигается через `Math::max` независимо
от судьбы предыдущих батчей; при `ack.success == false` только `log.error`, ни retry, ни DLQ.
*Сценарий*: B1 (offsets 100-199) падает, B2 (200-299) проходит → коммитится 300. 100 записей потеряны.
*Фикс*: коммитить непрерывный префикс (минимальный неподтверждённый оффсет на партицию); retry/DLQ при `success=false`.

**P0-4 | HMS: тихая потеря формата партиций**
`hms/client/ThriftHmsClient.java:601-607` — в `toThrift(HmsPartitionDto)` `inputFormat`/`outputFormat`/`serdeLib`
захардкожены в Text/LazySimpleSerDe; в `HmsPartitionDto` этих полей вообще нет.
*Сценарий*: Parquet/ORC-таблица с партициями → SD таблицы корректен, каждая партиция объявлена как
TextInputFormat → Hive читает партиции по их SD → запросы падают или возвращают мусор. Задача зелёная.
*Фикс*: добавить поля в DTO и proto, заполнять из `Partition.getSd()`, при отсутствии наследовать от таблицы.

**P0-5 | HMS: частичный батч партиций репортится как успех**
`hms/client/ThriftHmsClient.java:415-428` — `add_partitions` на весь батч обёрнут в
`catch (AlreadyExistsException)` с возвратом успеха. HMS при коллизии хотя бы одной партиции не добавляет
**ни одной** из батча.
*Сценарий*: ретрай частично применённого батча → остальные 999 партиций не доедут никогда;
`ALTER_PARTITION` (смена location) обрабатывается тем же путём → изменение молча отбрасывается.
*Фикс*: `add_partitions(..., ifNotExists=true)` / upsert через `alter_partitions`; не репортить успех вслепую.

**P0-6 | HMS в распределённом режиме: HTTP-ошибки игнорируются**
`hms/client/HttpRemoteHmsClient.java:170,196,221,242,255,273` — все мутирующие методы вызывают
`httpClient.send(...)` **без проверки `statusCode()`**, исключения проглатываются в `log.error`
(в отличие от read-методов, которые проверяют 200).
*Сценарий*: координатор вернул 401/500 → операция «успешна» → агент репортит APPLIED → оркестратор
продвигает `lastProcessedEventId` → события потеряны безвозвратно. Это основной путь записи в Docker/K8s-режиме.
*Фикс*: проверять 2xx, иначе бросать исключение.

### Отказоустойчивость / split-brain

**P0-7 | Self-fencing стримера inotify не срабатывает (мёртвый код)**
`client/OrchestratorGrpcClient.java:673-688` перехватывает любое исключение и возвращает `null`.
В `inotify/HdfsInotifyListener.java:200` ветка `resp != null` не выполняется, а `handleLeaseCheckFailure()`
вызывается **только из catch** — исключение уже проглочено клиентом. Агент остаётся ACTIVE бесконечно.
Дополнительно `modelResp.setEpoch(1L)` — fencing-token захардкожен и нигде не проверяется.
*Сценарий*: сетевая изоляция стримера → split-brain, два активных стримера дублируют задачи.
Тест `InotifyProcessingTest` проверяет fencing прямым вызовом `handleLeaseCheckFailure`, обходя
ровно тот путь, который в проде не вызывается.
*Фикс*: вызывать `handleLeaseCheckFailure()` и при `resp == null`; передавать реальную эпоху в `batchCreateTasks`.

**P0-8 | HMS: зомби-агент возвращает себе задачу и откатывает позицию**
`orchestrator/hms/service/HmsCoordinatorService.java:337-420` — fencing только по статусу `STOPPED`;
нет сверки `report.agentId()` с владельцем lease, нет монотонности `lastProcessedEventId`,
а `setAssignedAgentId(report.agentId())` + продление lease выполняются безусловно.
*Сценарий*: после failover «подвисший» агент досылает отчёт → снова становится владельцем и
перезаписывает позицию устаревшим значением → два агента крутят CDC одной БД.
*Фикс*: отклонять отчёт не-владельца; `lastProcessedEventId` только вверх; lease продлевать только владельцу.

**P0-9 | Kafka: воркер навсегда уходит в busy-wait после обрыва стрима**
`kafka/consumer/KafkaBatchConsumerPool.java:241-246` — в `onError` снимается только стрим из `activeStreams`;
`inFlightBatches`/`inFlightCount` не очищаются, а backpressure-петля `while (inFlightCount >= max) sleep(10)`
(`:273-281`) ждёт их вечно.
*Сценарий*: целевой кластер недоступен 30 с → счётчик залипает → шард не реплицируется до рестарта агента
даже после восстановления цели; параллельно утечка памяти.
*Фикс*: в `onError` снимать все батчи стрима (decrement + re-queue), таймаут на in-flight батч, watchdog.

**P0-10 | Kafka: нарушение порядка записей в партиции**
`kafka/service/KafkaDataTransferServiceImpl.java:90,105` + `kafka/producer/KafkaBatchProducer.java:76`
— приёмник держит окно 16 батчей и отдаёт каждый в `newVirtualThreadPerTaskExecutor()`, поэтому
`producer.send()` для батчей одной партиции идут параллельно. Идемпотентный producer порядок не спасает.
*Сценарий*: в целевом топике offsets 200-299 записаны раньше 100-199 → для compacted-топиков и CDC
это испорченное состояние.
*Фикс*: сериализовать запись по партиции (per-partition очередь) либо stop-and-wait.

**P0-11 | Kafka: эндпоинты и топики всех задач смешиваются в один пул**
`kafka/agent/KafkaReplicatorAgentMain.java:362-394` — `flatMap` по всем активным задачам ДЦ:
объединённый список топиков и объединённый список `targetEndpoints` отдаются одному пулу и одной
consumer-группе.
*Сценарий*: job A (DC1→DC2) и реверс BIDIRECTIONAL job B (DC1→DC3) → HRW раскидывает партиции одного
топика между DC2 и DC3; часть данных уходит в чужой кластер. Один топик в двух задачах читается один раз.
*Фикс*: отдельная consumer-группа и отдельный `KafkaReplicationClient` на (jobId, направление).

**P0-12 | Kafka: гонка при динамическом переключении топологии**
`kafka/client/KafkaReplicationClient.java:118-146` — `updateEndpoints` под `synchronized` делает
`channels.clear(); asyncStubs.clear()` и заново наполняет обычные `ArrayList`, которые воркеры читают
**без синхронизации**.
*Сценарий*: heartbeat (раз в 3 с) перестраивает пул → воркер получает `asyncStubs.size()-1 == -1` →
`IndexOutOfBoundsException` → `catch (Exception)` в `run()` убивает воркер навсегда; in-flight батчи
закрытых каналов теряются.
*Фикс*: иммутабельный снапшот пула (copy-on-write, `volatile record`), версионирование, дренирование in-flight.

### Производительность

**P0-17 | DR: сетевое ограждение не ограждает (0 = «без лимита»)**
`service/DisasterRecoveryService.java:553-559` ставит `topologyRegistry.setDcLimit(cluster, dc, 0L)`, но в
`throttler/TokenBucketThrottler.java:103,142` условие `limitBytesPerSec <= 0` трактуется как «лимита нет»:
`calculateWaitAndConsume` сразу возвращает `0.0`.
*Сценарий*: оператор нажимает Kill-Switch с `fenceNetwork=true` — агенты продолжают качать данные через WAN
на полной скорости и вообще без шейпинга, а UI показывает `isFenced=true`. Split-brain не предотвращён.
*Фикс*: явное состояние `blocked` (или трактовать `0` как запрет: `granted=false`).

**P0-18 | DR: откат Kill-Switch снимает ограждение произвольного кластера**
`service/DisasterRecoveryService.java:621-631` — если снимка для запрошенного кластера нет, код берёт
**первый из мапы**: `activeSnapshots.entrySet().iterator().next()`, удаляет его и строку в БД и восстанавливает
лимиты из него. Ошибки удаления глушатся `catch (Exception ignored) {}`.
*Сценарий*: ограждён DC2 (реальная авария), оператор откатывает Kill-Switch по DC1 (опечатка или повтор) →
снимается ограждение DC2, лимиты возвращаются к 100 МБ/с, репликация в упавший ДЦ оживает.
*Фикс*: нет снимка по ключу — возвращать ошибку, без fallback.

**P0-19 | Открытый `/metrics` грузит всю таблицу задач в heap**
`metrics/ReplicatorMetricsService.java:186` — `taskRepository.findAll()` по `replication_tasks`
(миллионы пофайловых задач), плюс ещё 7 полных `findAll()` (453, 491, 519, 530, 574, 589) на один скрейп.
Эндпоинт `/metrics` стоит в `permitAll` (`ReplicatorOrchestratorApplication.java:64`).
*Сценарий*: Prometheus со стандартным интервалом 15 с (или любой, кто дотянулся до порта 8005) →
OOM/остановка GC-паузами, без аутентификации.
*Фикс*: агрегатные `@Query` (`count/sum ... GROUP BY`) или Micrometer-гистограммы; закрыть эндпоинт.

**P0-13 | TAR-приёмник нерабочий в продовой конфигурации (Piped* + virtual threads)**
`receiver/DataTransferServiceImpl.java:543-544,743` + `agent/ReplicatorAgent.java:234` — поток склеивается
через `PipedOutputStream`/`PipedInputStream`, а gRPC-сервер сконфигурирован
`.executor(Executors.newVirtualThreadPerTaskExecutor())`. `PipedInputStream` запоминает `writeSide` и бросает
`IOException("Write end dead")`, если этот поток мёртв, а каждая порция сообщений приходит в новом
виртуальном потоке.
*Сценарий*: пауза в WAN между чанками → распаковщик дренировал буфер → `writeSide.isAlive() == false` →
весь бандл (путь BUNDLE_TAR, то есть все мелкие файлы) падает; ретрай падает так же.
Тесты не ловят: все gRPC-тесты на `InProcess + directExecutor()`.
*Фикс*: заменить Piped* на `BlockingQueue<byte[]>` + адаптер `InputStream`.

**P0-14 | Утечка DFSClient при doAs: OOM за один job**
`security/KerberosUgiProvider.java:167` — `UserGroupInformation.createProxyUser(...)` вызывается на
**каждую** операцию (`exists`, `openInputStream`, `writeDirect`, `renameFile`, `deletePath`), создавая новый
`Subject`. `FileSystem.Cache.Key` включает UGI, `UGI.equals` сравнивает subject по ссылке, кэш FileSystem
не отключён → на каждую операцию создаётся и навсегда кэшируется новый `DistributedFileSystem`/`DFSClient`
(сокеты, lease-renewer-потоки).
*Сценарий*: синхронизация 100k файлов в режиме doAs → 100k+ DFSClient → исчерпание FD и OOM.
*Фикс*: кэшировать proxy-UGI по имени пользователя.

### Безопасность

**P0-15 | Control plane оркестратора fail-open при пустом секрете**
`orchestrator/grpc/OrchestratorGrpcServer.java:82-87` — интерцептор аутентификации добавляется
**только если** секрет непустой; иначе gRPC control plane (register, claim tasks, report progress)
полностью анонимен. Та же схема в `kafka/agent/KafkaReplicatorAgentMain.java:215` — приём Kafka-батчей
без аутентификации, то есть запись произвольных записей в целевые топики и коммит оффсетов.
Важно: `demo/replicator/docker-compose.yml:18,89,137` задаёт `AGENT_SECRET: ""` — в демо-режиме
control plane открыт. При этом HDFS-приёмник (`agent/ReplicatorAgent.java:247`) реализован правильно,
fail-closed — поведение несогласованно между компонентами.
*Фикс*: fail-closed везде (как в `ReplicatorAgent`): при отсутствии секрета отклонять вызовы или не стартовать.

**P0-16 | HMS REST: отчёт о прогрессе и захват lease без авторизации**
`orchestrator/controller/HmsReplicationController.java:136-164` — `POST /{id}/progress` проверяет только
`session != null` (ни роли, ни владельца, ни идентичности агента); `GET /{id}/pending` принимает произвольный
`agentId` и при этом **мутирует состояние** — захватывает/продлевает lease. `listJobs`, `/{id}/events`,
`/{id}/subtasks`, `/{id}/dlq` — без `Authentication` вовсе.
*Сценарий*: пользователь с ролью READER одним POST выставляет `lastProcessedEventId = 10^9` → все будущие
события считаются обработанными, репликация тихо встаёт; GET с чужим `agentId` отбирает lease у работающего агента.
*Фикс*: аутентификация агента (X-Agent-Secret/gRPC) + сверка `agentId` с владельцем lease; `/pending` сделать POST.

---

## 4. P1 — высокий приоритет (выборка; полный список по подсистемам ниже)

**Отказоустойчивость и целостность**
- `fs/HadoopFsManager.java:328` — `exists()` глотает любое исключение → при истёкшем Kerberos-тикете или
  NameNode в safemode бандл уходит пустым, приёмник отвечает `success=true` на 0 файлах, задача «выполнена».
- `receiver/DataTransferServiceImpl.java:397-439` — нет сверки `bytesWritten == totalBytes`, флаг
  `isLastChunk` игнорируется, SHA-256 считается, но ни с чем не сверяется → усечённый файл переименовывается
  поверх валидного.
- `inotify/HdfsInotifyListener.java:331,361` — `currentTxid` продвигается до обработки; при переполнении
  очереди события теряются; при недоступности оркестратора (`getStreamingJobs()` вернул пустой кэш)
  события молча отбрасываются, а txid уже продвинут.
- `sender/TarStreamPipeline.java:182-189` — `tarEntry.setSize()` из манифеста при фактическом чтении данных:
  файл дописали между анализом и передачей → `IOException` на весь бандл (до 500 файлов).
- `kafka/consumer/KafkaBatchConsumerPool.java:288,464` — `subscribe()` без `ConsumerRebalanceListener`, а
  `drainAndCommitPendingOffsets()` **никогда не очищает** `pendingCommittedOffsets` → после ребаланса
  бывший владелец вечно коммитит устаревший оффсет → дубли после рестарта.
- `kafka/consumer/KafkaBatchConsumerPool.java:451-461,527` — на shutdown только `commitAsync` перед
  `consumer.close()` → подтверждённые оффсеты теряются при каждом деплое.
- `kafka/consumer/KafkaBatchConsumerPool.java:337-348` — если все записи партиции отфильтрованы
  loop-фильтром, оффсет не коммитится никогда → в active-active лаг растёт монотонно, при рестарте топик
  перечитывается целиком.
- `kafka/offset/SafetyCommitManager.java:152-175` — `isGroupActiveInTarget` при любой ошибке возвращает
  `false` → перезапись оффсетов живой consumer-группы; монотонность не проверяется.
- `hms/service/HmsTaskExecutor.java:583-700` — в CDC обрабатываются только 5 типов событий;
  `ALTER_TABLE` (ADD COLUMNS, RENAME, смена location), `INSERT`, `*_DATABASE` помечаются обработанными
  (`eventSuccess = true`) → схема на приёмнике навсегда расходится, INSERT-данные не копируются.
- `hms/service/HmsTaskExecutor.java:708-730` — «ядовитое» событие уходит в DLQ, позиция продвигается,
  статус задачи остаётся `ACTIVE`; `replayDeadLetterEvent` только помечает запись `REPLAYED`
  (payload сохраняется как `null`) — переигрывания нет.

**Безопасность**
- `receiver/DataTransferServiceImpl.java:195-277` — `checkFile` и `getDirectoryManifest` не вызывают
  `validateSandboxAccess`/`validatePathSafety`; `getDirectoryManifest` рекурсивно листит произвольный путь.
- `fs/HadoopFsManager.java:1022` — `isLocalPath()` возвращает `false` для `file:///...`, если `defaultFsUri`
  начинается с `hdfs://` → проверка границ chroot полностью пропускается, а запись реально идёт на локальный диск.
- `hms/client/ThriftHmsClient.java:113-190` — соединения создаются один раз в пуле под keytab агента,
  а `doAs` оборачивает вызов по **уже открытому** SASL-соединению: HMS/Ranger видит принципала агента,
  не пользователя; пул общий для всех `executionPrincipal`. Имперсонация де-факто не работает.
- `hms/client/HmsClient.java:49` — `executeAs` объявлен default-методом, `HttpRemoteHmsClient` его не
  переопределяет: в распределённом режиме логируется `doAs=user@REALM`, а DDL идёт от имени агента.
- `client/OrchestratorGrpcClient.java:187-189` — при ошибке настройки TLS (нет файла сертификата, битый PEM)
  код **молча падает в `usePlaintext()`** → control-plane трафик вместе с `x-agent-secret` уходит открытым.
- `kafka/schema/client/ConfluentRegistryClient.java:74` — `subject` подставляется в URL без URL-encoding,
  приходит из gRPC-запроса → обращение к произвольным путям API реестра.
- `orchestrator/registry/AgentRegistry.java:291` — секрет проверяется только если заголовок прислан
  (`isEnforceAgentSecret() || secretHeader != null`) → при дефолтных настройках регистрация анонимна.
- Dockerfile оркестратора и агента: `appuser` создаётся, но `USER` не выставлен — оба контейнера от root.

**Производительность**
- `orchestrator/grpc/OrchestratorGrpcServer.java:57-72` — gRPC-executor оркестратора: `ThreadPoolExecutor`
  с очередью 1000 и **`CallerRunsPolicy`**; при перегрузке вызовы выполняются в Netty event-loop потоке →
  блокировка транспорта всего control plane. Виртуальные потоки не используются (в отличие от агента).
- `sender/ReplicationSender.java:406-446,648-696` — пофайловая передача вызывает `onNext()` без проверки
  `isReady()`; flow-control реализован только в TAR-конвейере → неограниченный рост исходящей очереди
  Netty и OOM отправителя на большом файле при узком WAN.
- `receiver/DataTransferServiceImpl.java:550-595` — оценка памяти TAR-записи занижена до 128× (64 КБ против
  фактических 8 МБ) → при `maxConcurrentTarStreams=16` реальный расход ~2 ГБ heap вместо учтённого.
- `compression/WireCompressor.java:168-190` — лимит распаковки 64 МБ при фактическом чанке 64 КБ
  (в 1000× больше необходимого); для zstd нет сверки фактической длины с объявленной.
- `receiver/DataTransferServiceImpl.java:43,125` — `knownTargetDirectories` неограничен и никогда не чистится;
  планировщик рекурсивно обходит каждый накопленный каталог → шторм RPC к NameNode каждые N минут.
- `kafka/consumer/KafkaBatchConsumerPool.java:273,404,422` — backpressure, `!isReady()` и
  `bandwidthLimiter.throttle()` (`Thread.sleep`) выполняются в том же потоке, что `poll()`; при этом
  `max.poll.records`, `max.poll.interval.ms`, `session.timeout.ms`, `linger.ms`, `batch.size` не заданы
  нигде → при деградации WAN брокер выкидывает консьюмера → ребаланс-шторм. Плюс `maxBatchSize=2000`
  недостижим при дефолтном `max.poll.records=500`.
- `kafka/schema/CachedSchemaRegistryResolver.java` — нет TTL и негативного кэша: при недоступности реестра
  HTTP-запрос с 5-секундным таймаутом делается на каждую запись внутри poll-цикла.
- `hms/client/ThriftHmsClient.java:331` — `listPartitions(db, tbl, (short) -1)` материализует все партиции;
  для таблицы на 50-100k партиций это сотни МБ × `bootstrapConcurrency`; в CDC — N+1 (`getPartition` на
  каждое значение); `HmsPipelineSender` (а значит новый gRPC-канал и TLS-handshake) создаётся заново
  в каждом `pollAndSyncCdc`.

**Оркестратор: active-active, транзакции, блокировки**
- *Состояние живёт в памяти узла.* Механизм распространения лимитов между узлами существует
  (`persistAndApplyLimit` + `BandwidthLimitRepository` + `syncLimitsFromDatabase` раз в 5 с), но ни
  `DisasterRecoveryService`, ни `TopologyController` им не пользуются — пишут прямо в `ConcurrentHashMap`
  (`topology/TopologyRegistry.java:93`, `service/DisasterRecoveryService.java:173,556,645`).
  `activeSnapshots` читается из БД только в `@PostConstruct`. Итог: за балансировщиком Kill-Switch,
  ограждение и смена полосы применяются только на принявшем запрос узле; остальные продолжают выдавать
  токены и показывают `killSwitchActive=false`.
- *Реестр агентов тоже локальный* (`registry/AgentRegistry.java:122`, write-behind в БД раз в 10 с,
  чтение из БД только на старте). Heartbeat приходит на один узел, а failover-шедулер под распределённой
  блокировкой может выполниться на другом, где `lastHeartbeat` устарел на часы → `refreshHealth()`
  помечает живые агенты OFFLINE → массовый requeue активных задач и дубли передач.
- *Self-invocation ломает `@Transactional`*: `lockService.runWithLock(..., this::executeCheckAndFailover…)`
  (`service/TaskService.java:688`, `hms/service/HmsCoordinatorService.java:427,661`) — ссылка на метод
  берётся у `this`, не у прокси, аннотация не применяется. Failover идёт вне транзакции, каждый `save()`
  сам по себе; `job.setFailedObjects(getFailedObjects()+1)` — read-modify-write на detached-сущности.
  В `scheduler/ReplicationScheduler.java:85` это сделано правильно, через `TransactionTemplate`.
- *`@PreDestroy` flush прогресса не работает*: `service/TaskService.java:888` вызывает
  `flushPendingProgress()` напрямую → транзакции нет → `@Modifying incrementProgress` падает с
  `TransactionRequiredException`, исключение проглатывается. При каждом rolling update накопленный
  прогресс теряется, `copied_bytes`/`transferred_objects` занижены навсегда.
- *Распределённая блокировка берётся внутри бизнес-транзакции*: `emergencyStop`/`rollback`/
  `reverseReplication` помечены `@Transactional` и внутри вызывают `runWithLock` (PROPAGATION_REQUIRED) →
  строка блокировки не коммитится до конца DR-операции. Второй узел не «проскакивает», а блокируется на
  row lock на всё время операции; поток-продленец аренды встаёт на той же строке навсегда (JDBC не
  прерывается по `interrupt()`), удерживая соединение Hikari, — то есть продление аренды де-факто не работает.
- *Эпоха fencing-токена немонотонна*: `releaseLock` — это `DELETE`
  (`repository/ClusterLockRepository.java:42-44`), а следующий захват вставляет строку заново с
  `epoch = 1L` (`service/DistributedLockService.java:85`). Узел после GC-паузы вставляет строку с epoch=1,
  проверка `renewed.epoch() != lockedEpoch` проходит — потеря аренды не детектируется.
  *Фикс*: освобождать через `lockedUntil = now`, эпоху только инкрементировать.
- *Нечёткое сравнение ДЦ*: `matchesDc` (`service/DisasterRecoveryService.java:1023`) сравнивает подстрокой →
  при ограждении `dc1` датацентр `dc10` считается «тем же» и не ограждается; в `getDrStatus` агенты `dc10-*`
  засчитываются в `dc1`. Такая же нечёткость в `HmsCoordinatorService.matchesCluster:781` — агент кластера
  `prod-dc1-backup` может забрать задачу кластера `dc1` и применить DDL в чужой ДЦ.
- *Точечный реверс без ограждения*: `reverseSingleHdfsJob` (`:876-920`), в отличие от `doReverseReplication`,
  не берёт блокировку, не останавливает прямую задачу и не проверяет существующий реверс → A→B и B→A
  работают одновременно по тем же путям (встречная перезапись, пинг-понг дельт). Плюс `matchesReversePath`
  сравнивает пути по `endsWith` → ложное «реверс уже существует» и обратная задача не создаётся вовсе.
- *`StreamingLeaseController` без аутентификации агента* (`controller/StreamingLeaseController.java:21-36`):
  любой аутентифицированный пользователь (даже READER) постит `/api/v1/streaming/lease/renew` с чужим
  `agentId` → инкремент epoch → легитимный стример отгораживается, inotify-репликация молча встаёт.
- *Heartbeat Kafka-агента в hot path*: `service/KafkaReplicationOrchestratorService.java:641-666` на
  **каждый** heartbeat делает `findAll()` всех Kafka-задач + `save()` на каждую подходящую. 50 агентов ×
  5 с × 20 задач ≈ 200 UPDATE/с по одним строкам. Метрики при этом неверны: `Math.max` вместо суммы по
  агентам, после рестарта агента счётчик «залипает», при `BIDIRECTIONAL` цифры дублируются на оба направления.
- *Внешний вызов внутри транзакции*: `createJob`/`provisionTopicsForJob` (`:95,259-304`) помечены
  `@Transactional` и внутри выполняют блокирующий Kafka AdminClient к обоим брокерам с Kerberos-логином →
  недоступный брокер держит транзакцию и соединение Hikari весь таймаут; несколько параллельных созданий
  выбирают пул (60) и роняют control plane.
- *`TaskEntity` не реализует `Persistable`* при присвоенном строковом `@Id` → `saveAll(new entities)` идёт
  через `em.merge()`, то есть **SELECT на каждую строку** перед вставкой. `batchCreateTasks` на 20 000 файлов =
  20 000 одиночных SELECT; заявленный `batch_size: 500` ускоряет только сам INSERT.
- *`refreshHealth()` вызывается из каждого геттера* `AgentRegistry` (полный проход с мутацией статусов),
  а в цикле `claimTasks` на каждого кандидата вызывается `hasOtherOnlineAgentsForCluster`:
  300 агентов × 60 кандидатов = 18 000 итераций на один claim. Поля `AgentEntry` при этом мутируются из
  heartbeat-потоков без синхронизации и без `volatile`.
- *Нет оптимистичной блокировки нигде* (`@Version` отсутствует у всех сущностей), при том что `status`,
  `failedObjects`, `message`, `activeRunId` обновляются read-modify-write одновременно из gRPC агентов,
  шедулеров и REST, с нескольких узлов → потерянные обновления счётчиков и затирание статусов
  (`COMPLETED` поверх `FAILED`).
- *Unpaged `findAll()` в hot path*: `getDrStatus` (`:191,333`) — `findAll()` по job и hms + вложенный поиск
  зеркала на каждую задачу (**O(n²)**, вызывается дважды подряд из контроллера); `JobService.listJobs:223`
  — `findAll()` + фильтрация в Java, причём это путь gRPC `getJobs` агентов; `getActiveJobsForDc` —
  `findAll()` на каждый опрос агента; `getAllLeaseStatuses` — N+1; `pruneJobRuns` — удаление по одной записи.
- `JobEntity` **не имеет ни одного `@Index`** (в отличие от `TaskEntity`/`HmsReplicationJobEntity`), а
  SQL-скрипт с индексами не подключён → seq scan по `replication_jobs` в тике планировщика каждые 5 с.

**Архитектура**
- `orchestrator/pom.xml:41-44` — оркестратор зависит от артефакта `replicator-agent` в compile scope и
  тянет весь стек агента: `hadoop-common`, `hadoop-hdfs-client`, `hadoop-yarn-client`,
  `hive-standalone-metastore-common`, `kafka-clients`, zstd, lz4. Нет выделенного модуля
  `replicator-proto`/`replicator-model`. Следствия: раздутый classpath control plane, конфликты
  транзитивных зависимостей, расширенная attack surface, невозможность независимой эволюции.
  Дополнительно `orchestrator/hms/client/HmsClientPool` использует Thrift-клиент агента напрямую —
  оркестратор сам ходит в HMS, вопреки заявленной модели «только агент касается кластера».
- Контракты proto продублированы: `backend/replicator/proto/*.proto` (устаревшая копия, без `task_token`
  и `execution_principal`, `int64 target_offset` вместо `optional`) и
  `agent/src/main/proto/*.proto` (компилируется только она — `protoSourceRoot` в `agent/pom.xml:261`).
  Первая — мёртвый код, вводящий в заблуждение.
- Схема БД разошлась с JPA-моделью: `schema-postgresql.sql` создаёт `dr_emergency_snapshots`,
  `replicator_streaming_lease`, `replicator_cluster_locks`, тогда как сущности объявляют
  `replicator_dr_snapshots`, `replicator_streaming_leases`, `cluster_locks`; таблиц `hms_event_logs`
  и `hms_dead_letter_events` в скрипте нет вовсе. Скрипт нигде не подключён, миграций нет,
  партиции заданы статически только до `2027_01` без автосоздания.
- Трассировки нет совсем (ни OpenTelemetry, ни micrometer-tracing) — для системы «оркестратор + N агентов
  × 3 подсистемы» это основной пробел наблюдаемости. gRPC health-service (`HealthStatusManager`) тоже нет,
  у контейнера агента нет HEALTHCHECK.
- Фронтенд: монолит не устранён, а перемещён — `App.svelte` сократился до 508 строк, но появились
  `HmsReplicationView.svelte` (2312) и `KafkaReplicationView.svelte` (1909).

---

## 5. Что сделано хорошо

1. **Data-First Consistency Barrier** (`hms/service/HmsTaskExecutor.java:371-378,460-472`) — перед применением
   DDL ожидается завершение HDFS-подзадачи, с таймаутом и отменой DDL при неуспехе. Это корректный ответ на
   самый трудный вопрос репликации метаданных.
2. **Flow-control в TAR-конвейере** (`sender/TarStreamPipeline.java:95-128,348-369`) — `ClientResponseObserver`
   + `setOnReadyHandler` + `ReentrantLock/Condition`: образцовый Loom-friendly паттерн. Его осталось
   распространить на пофайловый путь.
3. **Криптография без типичных ошибок** — HMAC-SHA256 по каноническому payload с TTL и допуском на clock
   skew, сравнение через `MessageDigest.isEqual`, fail-closed в `AgentGrpcAuthInterceptor`,
   защита от Zip Slip через `Path.normalize()` + `startsWith` (не регэкспом), проверка нулевого байта,
   whitelist-паттерн принципалов, чёрный список системных учёток (`hdfs`, `root`).
4. **Partition-pinned HRW-балансировка Kafka** (`KafkaTargetBalancer`) с локализованным failover 1/M без
   каскадного reshuffle и карантином узлов.
5. **Адаптивный AIMD-лизинг токенов** (`sender/ReplicationSender.java:728-794`) с эскалацией 2→16 МБ и
   мгновенным сбросом при троттлинге; idle-backoff с jitter во всех циклах агента.
6. **Пул Thrift-соединений** с `testOnBorrow`, повтором на `TException` и корректной инвалидацией;
   **двухуровневый bounded-LRU кэш** проверенных каталогов HDFS; **single-flight** при промахе кэша схем.
7. **Безопасный Bootstrap HMS**: high-water mark снимается до переноса, reconciliation удаляет лишние
   объекты только с `deleteData=false`; защита оффсетов consumer-групп от перемотки (blacklist служебных
   групп, отказ подставлять timestamp отстающей группе).
8. **Схема PostgreSQL** (если её применить) проработана: 18 индексов, частичные индексы на активные задачи,
   партиционирование по месяцам.
9. **Очередь задач сделана честно**: `PESSIMISTIC_WRITE` + `lock.timeout = -2` (SKIP LOCKED) + `Pageable`
   в `TaskRepository:31-52` — конкурентный забор без двойной выдачи; прогресс агрегируется
   `LongAdder`-буфером (`JobProgressBuffer`) с атомарным `incrementProgress`, то есть row lock contention
   на `replication_jobs` действительно снят.
10. **Epoch fencing стримеров через БД** (`StreamingLeaseRepository.tryAcquireOrRenewLease` + проверка в
   `TaskService:89-97`) — правильный паттерн с корректным SQL-условием (в отличие от эпохи кластерных
   блокировок). **Валидация анонсируемых адресов агентов** (`AgentRegistry.validateGrpcAddress`) —
   защита от SSRF на metadata-эндпоинты, link-local и multicast, плюс блокировка смены кластера.
11. **Loop prevention для active-active Kafka реально подключён** в продакшн-пути агента
   (`LoopPreventionFilter` в `KafkaBatchConsumerPool:346`) — циклического эха на уровне данных нет
   (при корректно заданном peer-DC, см. P1).
12. **Периметр API оркестратора** настроен правильно: `anyRequest().authenticated()`, сегментация actuator
   (`/actuator/**` → `hasRole("ADMIN")`), CSRF-валидатор для cookie-аутентификации, constant-time сверка
   секрета агента.

---

## 6. Тесты: почему 450 зелёных тестов не защищают

Покрытие «листовых» компонентов хорошее (70-80%): компрессор, аттестация, фильтры, балансировщик,
резолвер схем, path-rewriter. Провал — ровно там, где живут все P0:

- **Все** gRPC-тесты используют `InProcessServerBuilder` + `directExecutor()`
  (`TarStreamReplicationTest.java:91`, `DataTransferServiceGrpcTest.java:52`, chaos- и stress-тесты).
  Продовая конфигурация `NettyServerBuilder.executor(newVirtualThreadPerTaskExecutor())` не покрыта
  ни одним тестом — поэтому P0-13 (мёртвый `writeSide` у Piped*) и отсутствие `isReady()` проходят зелёными.
  `directExecutor()` вдобавок маскирует P0-10 (параллелизм батчей Kafka).
- **Kafka**: все «smoke/E2E» тесты построены на `mock(KafkaProducer)`/`mock(KafkaConsumer)`;
  Testcontainers/embedded Kafka не используется. `KafkaBatchConsumerPool` (554 строки, ядро) покрыт
  4 happy-path тестами. Ни одна из пяти P0-находок не была бы замечена.
- **HMS**: тесты идут через `MockHmsClient`, который не воспроизводит ни `AlreadyExistsException`,
  ни SD партиций, ни таймауты Thrift. `HttpRemoteHmsClient` — основной путь записи в распределённом
  режиме — не покрыт ни одним тестом.
- **Fencing стримера** проверяется прямым вызовом `handleLeaseCheckFailure()`, то есть тест обходит
  именно тот путь, который в проде недостижим.
- **Оркестратор** покрыт лучше остальных (26 классов / 156 тестов): троттлер под конкурентным штормом,
  DR-сценарии, очередь задач, прогресс-буфер, HMS-диспетчеризация, авторизация, chaos-тесты. Но пробелы
  точно совпадают с находками: нет теста на троттлер при лимите 0, на откат Kill-Switch с отсутствующим
  снимком, на транзакционность шедулеров и `@PreDestroy`-flush, на монотонность epoch после release,
  на `matchesDc`, на нагрузку `/metrics`. **Active-active не тестируется вообще** — ни одного теста с двумя
  инстансами оркестратора, `OrchestratorNodeRepository` и `BandwidthLimitRepository` не покрыты.

Минимальный набор тестов, который поймал бы большинство P0:
1. gRPC на `NettyServerBuilder` + виртуальные потоки, с искусственными паузами между чанками (P0-13).
2. «Бандл из 3 файлов, второй падает» → первый и третий остались на месте (P0-1).
3. Kafka: ack `success=false` между двумя успешными батчами → коммит не уходит вперёд (P0-3).
4. Kafka: `onError` стрима → счётчик in-flight обнуляется, воркер продолжает работу (P0-9).
5. Kafka: ребаланс через `MockConsumer.rebalance()` → коммит и очистка состояния; `close()` → `commitSync`.
6. Kafka: конкурентный `updateEndpoints` + отправка батчей, 100 итераций без исключений (P0-12).
7. HMS: `add_partitions` бросает `AlreadyExists` в середине батча → задача не репортит успех (P0-5).
8. HMS: отчёт о прогрессе от агента, не являющегося владельцем lease → отклонён (P0-8).
9. HMS: репликация Parquet-таблицы с партициями → проверка `inputFormat` на приёмнике (P0-4).
10. Порядок на реальном Kafka (Testcontainers) при `pipelineWindow > 1` (P0-10).

---

## 7. Что из прошлых аудитов не подтвердилось

Предыдущие отчёты (Gemini) выставляли 9.9/10 по всем направлениям после «внедрения P0-P4». Проверка по коду:

| Заявлено исправленным | Факт |
|---|---|
| Пул `ManagedChannel` + keepalive | ✅ подтверждено |
| JDBC batching, частичные индексы, партиционирование | ⚠️ в SQL-скрипте есть, но скрипт не подключён; схему строит `ddl-auto: update` |
| `synchronized` → `ReentrantLock` для Loom | ⚠️ частично: 19 `synchronized` осталось, в том числе вокруг I/O (`channelPool`, `ensureLoggedIn`, `flush()`) |
| `validatePathSafety` / chroot-песочница | ⚠️ обходится схемой `file://`; `checkFile`/`getDirectoryManifest` вообще без валидации |
| `MAX_UNCOMPRESSED_CHUNK_SIZE = 64 MB` | ⚠️ есть, но в 1000× больше фактического чанка |
| Транзакционный ack и backpressure в inotify | ❌ `currentTxid` продвигается до обработки → потеря событий |
| Self-fencing стримера >10 с | ❌ мёртвый код, не вызывается |
| Полная аутентификация API | ⚠️ REST-периметр да; gRPC control plane и Kafka-приёмник fail-open; HMS `/progress` и `/pending` без авторизации |
| Хардкод секрета устранён (SEC-12) | ❌ `DEFAULT_AGENT_SECRET` в коде и в `application.yml`, `enforce-agent-secret: false` |
| doAs в Thrift HMS-клиенте | ⚠️ код есть, но имперсонация не доезжает до HMS (SASL-личность фиксируется при открытии соединения) |
| `BoundedLruCache` схем, I/O вне локов | ✅ подтверждено (без TTL и негативного кэша) |
| Декомпозиция фронтенда | ⚠️ монолит перемещён в `HmsReplicationView.svelte` (2312 строк) |
| Транскодинг схем, offset-sync | ❌ в проде не подключены (passthrough-резолвер, нет вызовов `syncOffsets`) |
| JPQL `countPendingTasksByClusterId` вместо N+1 | ✅ подтверждено, используется |
| Пагинация вместо unpaged `findAll()` | ⚠️ в очереди задач — да; в метриках, DR, `JobService.listJobs`, Kafka — нет |
| Монотонные fencing tokens (epoch) | ❌ эпоха сбрасывается в 1 из-за `DELETE` при release |
| Персистентность DR-снимков (`DrSnapshotEntity`) | ⚠️ запись/чтение есть, но между узлами не синхронизируется, лимиты не персистятся |
| Динамический `updateLimit` в `SingleBucket` | ⚠️ работает, но `0` = unlimited → DR-ограждение бесполезно |
| Fencing прямых задач при DR-реверсе | ⚠️ только в `doReverseReplication`; `reverseSingleHdfsJob` без fencing |
| Автопродление распределённой блокировки (heartbeat) | ❌ продленец встаёт на row lock той же транзакции — не работает |

Это главный методологический вывод: отчёты, написанные тем же инструментом, что вносил правки,
систематически завышают результат. Проверять нужно по коду и тестам, воспроизводящим продовую конфигурацию.

---

## 8. Предлагаемый порядок работ

**Шаг 1 — остановить потерю данных (без этого остальное не имеет смысла)**
1. P0-1: откатывать только staging, не удалять закоммиченное.
2. P0-2: `FileContext.rename(..., OVERWRITE)`.
3. P0-3: коммит непрерывного префикса оффсетов + retry/DLQ при `success=false`.
4. P0-6: проверка HTTP-кодов в `HttpRemoteHmsClient`.
5. P0-4, P0-5: формат партиций в DTO/proto; `ifNotExists` + upsert для `add_partitions`.
6. Запретить «успех по умолчанию»: `exists()`, `getFileSize()`, `getNextNotifications()`,
   `getCurrentNotificationEventId()`, `requestNetworkTokens()` должны различать «нет» и «не знаю».

7. P0-17: `0` в троттлере не должен означать «без лимита» — иначе Kill-Switch не работает.
8. P0-18: убрать fallback на «первый снимок из мапы» в откате Kill-Switch.
9. P0-19: агрегатные запросы в `/metrics` + закрыть эндпоинт (сейчас это и DoS, и утечка).

**Шаг 2 — вернуть работоспособность заявленным механизмам**
10. P0-13: убрать Piped* из TAR-приёмника (иначе путь мелких файлов не работает в проде).
11. P0-7: вызывать `handleLeaseCheckFailure()` при `resp == null`, передавать реальную эпоху.
12. P0-8: fencing отчётов HMS по владельцу lease + монотонность позиции.
13. P0-9, P0-12: очистка in-flight в `onError`, иммутабельный снапшот пула эндпоинтов.
14. Эпоху кластерных блокировок сделать монотонной (release без `DELETE`); блокировку брать **до**
    бизнес-транзакции (`REQUIRES_NEW`), продление — отдельной транзакцией.
15. Исправить self-invocation в шедулерах и `@PreDestroy`-flush (`TransactionTemplate` или self-прокси).
16. Либо подключить `enqueueDelete`, `scanSchemaDrift`, транскодинг схем, offset-sync — либо честно
    отключить флаги и убрать из документации.

**Шаг 3 — безопасность по умолчанию**
17. P0-15: fail-closed для gRPC control plane и Kafka-приёмника.
18. P0-16: авторизация `/progress`, `/pending`, read-эндпоинтов HMS и `StreamingLeaseController`.
19. Убрать хардкод секретов; `enforce-agent-secret`, `enforce-cluster-whitelist`,
    `enforce-task-attestation` → `true` по умолчанию, fail-fast при дефолтных значениях вне dev-профиля.
20. Профиль `prod`: PostgreSQL, `ddl-auto: validate`, TLS, `auth.mode` ≠ mock, `USER appuser` в Dockerfile.
21. Исправить silent downgrade TLS → plaintext в `OrchestratorGrpcClient`.

**Шаг 4 — active-active, производительность, архитектура**
22. Вынести лимиты, DR-снимки и статус агентов в БД как единственный источник истины (механизм уже есть,
    его надо начать вызывать); failover читает статус агентов из БД.
23. Строгое сравнение кластеров и ДЦ (`matchesDc`, `matchesCluster`, `matchesReversePath`) вместо `contains`/`endsWith`.
24. P0-10, P0-11: порядок записей по партиции; изоляция задач по consumer-группам и эндпоинтам.
25. P0-14: кэш proxy-UGI; `Persistable.isNew()` для `TaskEntity`; `@Version` на job-сущности;
    `refreshHealth` в `@Scheduled`; heartbeat Kafka — точечный запрос вместо `findAll()` + `save()`.
26. `CallerRunsPolicy` → ограниченная очередь с отказом или виртуальные потоки в control plane;
    `isReady()`/flow-control на пофайловом пути; явные `max.poll.*`, `linger.ms`, `batch.size` для Kafka;
    провижининг топиков — за пределы транзакции.
27. Выделить модуль `replicator-proto`/`replicator-model`, убрать зависимость оркестратора от агента
    и дубль `backend/replicator/proto`.
28. Миграции (Flyway/Liquibase) как единственный источник схемы; согласовать имена таблиц; индексы на
    `JobEntity`; автосоздание партиций (сейчас статически до `2027_01`).
29. OpenTelemetry/micrometer-tracing сквозь оркестратор и агенты; gRPC health-service; HEALTHCHECK агента.

**Шаг 5 — тесты, закрепляющие исправления**: 10 сценариев из раздела 6.

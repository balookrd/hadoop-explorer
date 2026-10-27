# Демонстрационный стенд Hadoop gRPC Replicator

В данной папке находятся конфигурации и скрипты полностью автономного демонстрационного стенда сервиса межкластерной репликации **Hadoop gRPC Replicator**.

---

## 🚀 Быстрый запуск и остановка

### Через корневой Makefile:
```bash
# Запуск стенда (сборка контейнеров и старт в фоне)
make demo-replicator

# Остановка и очистка контейнеров
make demo-replicator-stop
```

### Через Docker Compose:
```bash
# Запуск
docker compose -f demo/replicator/docker-compose.yml up -d --build

# Остановка
docker compose -f demo/replicator/docker-compose.yml down -v
```

---

## 🌐 Сетевые интерфейсы и компоненты стенда

| Сервис | Порт | Технология | Описание | URL / Доступ |
|---|---|---|---|---|
| **Replicator Orchestrator (Web / REST)** | `8005` | Java 21 LTS (Spring Boot 3 + Svelte 5) | Веб-интерфейс, REST API для UI, аутентификация | [http://localhost:8005](http://localhost:8005) |
| **Orchestrator Control Plane (gRPC)** | `50050` | Java 21 LTS (gRPC Netty HTTP/2) | Высокопроизводительный Control Plane для агентов | `localhost:50050` |
| **Prometheus Metrics** | `8005` | Replicator Metrics Engine | Экспорт канонических метрик Prometheus (`/metrics`) | [http://localhost:8005/metrics](http://localhost:8005/metrics) |
| **Grafana Dashboards** | `3000` | Grafana 10.4 | Дашборды Replicator (Overview, Kafka, HMS, Agents) | [http://localhost:3000](http://localhost:3000) |
| **Agent DC1 (Primary ЦОД)** | `50051` | Java 21 LTS (gRPC) | Дуплексный агент кластера `dc1` (HDFS 1 Kerberos) | `localhost:50051` |
| **Agent DC2 (DR ЦОД)** | `50052` | Java 21 LTS (gRPC) | Дуплексный агент кластера `dc2` (HDFS 2 Kerberos) | `localhost:50052` |
| **Primary HDFS (Cluster 1)** | `9870` | Apache Hadoop 3.3 (WebHDFS) | Исходный DataLake кластер (DC1) | [http://localhost:9870](http://localhost:9870) |
| **DR Backup HDFS (Cluster 2)** | `9872` | Apache Hadoop 3.3 (WebHDFS) | Резервный DataLake кластер (DC2) | [http://localhost:9872](http://localhost:9872) |
| **Hive Metastore 1 (DC1)** | `9083` | Apache Hive 4.0.0 (Thrift) | Метастор схем первичного кластера (DC1) | `thrift://localhost:9083` |
| **Hive Metastore 2 (DC2)** | `9084` | Apache Hive 4.0.0 (Thrift) | Метастор схем резервного кластера (DC2) | `thrift://localhost:9084` |
| **MIT Kerberos KDC** | `88` | Kerberos 5 KDC Daemon | Единый KDC аутентификации (Realm `COMPANY.LOCAL`) | `localhost:88` |

---

## 🧪 Запуск демонстрационных задач распределенной репликации

Стенд демонстрирует полный цикл работы изолированных ЦОД с распределенным пулом пофайловых задач:

```bash
# 1. Запуск стенда (поднимает KDC, 2x HDFS, 2x HMS, Orchestrator и 2x Агента)
./start-demo.sh

# 2. Создание демо-задач (DC1 ➔ DC2 и DC2 ➔ DC1)
./create-test-jobs.sh
```

Скрипт автоматически:
1. Генерирует файлы в общем томе данных (`prod_sales_dc1.csv` 5 МБ, `events_stream_dc1.json` 2 МБ и `analytics_report_dc2.parquet` 3 МБ).
2. Ставит задачу репликации **Agent DC1 ➔ Agent DC2** (`prod_sales_dc1.csv`) от принципала `writer_user@REALM.LOCAL`.
3. Ставит задачу репликации **Agent DC2 ➔ Agent DC1** (`analytics_report_dc2.parquet`) от принципала `de_user@REALM.LOCAL`.
4. Ставит задачу потоковой передачи **Agent DC1 ➔ Agent DC2** (`events_stream_dc1.json`) от принципала `reader_user@REALM.LOCAL`.
5. Тестирует обработку ошибок при репликации несуществующего пути (задача переходит в `FAILED`).
6. Агенты выполняют анализ каталогов, формируют пул пофайловых подзадач, параллельные воркеры забирают задачи через `claimTasks` и передают потоком gRPC с валидацией контрольных сумм SHA-256.

---

## 🔬 Полноценные End-to-End Smoke-тесты репликации (Таблицы, Партиции, Дописывание, Удаление, Inotify/CDC)

В стенд включен расширенный автоматический smoke-тест полного жизненного цикла репликации схемы и данных, поддерживающий режимы работы:
* **`all`** (по умолчанию) — сквозное тестирование всех фаз (стандартная репликация, две предметные схемы HMS CDC и HDFS Inotify streaming);
* **`standard`** — проверка переноса файлов через воркеры без использования Inotify и без CDC;
* **`cdc`** — проверка репликации метаданных Hive Metastore (схемы, партиции, дописывание, удаление);
* **`inotify`** — проверка HDFS Inotify стриминга (Active/Standby лизинг, фильтрация staging, коммит по RenameEvent).

### Проверяемые сценарии и бизнес-витрины:
1. **Проверка доступности инфраструктуры**: KDC, 2x HDFS NameNode (WebHDFS), 2x Hive Metastore (Thrift), HiveServer2 1 & 2, воркеры DC1/DC2 (gRPC), стримеры DC1/DC2, Оркестратор.
2. **Перенос таблицы (`sales_transactions`)**: создание схемы `retail_analytics_dw` и таблицы транзакций в DC1, Bootstrap-перенос на DC2, чтение через HiveServer2 2.
3. **Дописывание файла в существующую таблицу**: добавление файла дельты (`extra_transactions_delta.csv`) в каталог существующей таблицы в DC1, синхронизация дельты и верификация через HiveServer2 2.
4. **Перенос партиционированной таблицы (`customer_orders_partitioned`)**: добавление партиции `order_date=2026-10-10`, перенос схемы и данных партиции в DC2, проверка через `SHOW PARTITIONS`.
5. **Дописывание файла в существующую партицию**: добавление 2-го файла (`orders_batch_02_appended.csv`) в ту же существующую партицию `order_date=2026-10-10`, репликация дельты и валидация присутствия обоих файлов на DC2.
6. **Удаление партиции**: создание временной таблицы `staging_events_temporary`, удаление партиции `batch_id=temp_batch_99` в DC1, применение CDC события `DROP_PARTITION` и верификация удаления в HMS DC2 (основная партиция витрины сохраняется).
7. **Удаление таблицы**: удаление временной таблицы `staging_events_temporary` в DC1, применение CDC события `DROP_TABLE` и верификация удаления таблицы в HMS DC2.
8. **Вторая бизнес-схема (`inventory_logistics_dw`)**: создание отдельной витрины складской логистики с партиционированной таблицей `warehouse_stock_partitioned` (партиции `WH-CENTRAL-01` и `WH-NORTH-02`), полная репликация для наглядной визуализации двух разных схем в UI.
9. **HDFS Inotify Streaming HA**: опрос распределенного лизинга (`/api/v1/streaming/lease/all`), проверка ролей `ACTIVE` и `STANDBY`, проверка изоляции привилегий (HTTP 403 Forbidden при попытке забора задач воркера стримером).
10. **Распознавание коммита (RenameEvent)**: фильтрация временных путей (`.staging`, `_temporary`), фиксация коммита при `rename` и автоматическое порождение задач воркерам без ручного триггера.
11. **Disaster Recovery Kill-Switch & Rollback**: аварийный останов (Kill-Switch) с фиксацией снимка состояния (Snapshot), перекрытием канала шейпера (0 МБ/с) и однокликовым откатом (Unfence) с восстановлением лимитов (100 МБ/с) и реактивацией задач.

### Запуск smoke-тестов:
```bash
# Вариант 1: Полный smoke-тест (все сценарии)
./demo/replicator/run-smoke-tests.sh all

# Вариант 2: Запуск конкретного режима (standard | cdc | inotify)
./demo/replicator/run-smoke-tests.sh cdc
./demo/replicator/run-smoke-tests.sh standard
./demo/replicator/run-smoke-tests.sh inotify

# Вариант 3: Запуск внутри Docker-контейнера через Docker Compose
docker compose -f demo/replicator/docker-compose.yml --profile test run --rm smoke-test
```

### ⚡ Запуск E2E Smoke-тестов Kafka Replicator (Kerberos SASL & SPNEGO SR):
```bash
# 1. Запуск керберизованных кластеров Kafka и Schema Registry (DC1 и DC2)
docker compose -f demo/infra/docker-compose.infra.yml up -d kdc ldap
docker compose -f demo/replicator/docker-compose.kafka.yml up -d

# 2. Выполнение полного E2E Smoke-теста Data Plane & Control Plane:
make demo-kafka-smoke
# Либо напрямую:
./demo/replicator/run-kafka-smoke-tests.sh
```

Smoke-тест Kafka проверяет:
1. **Kerberos SASL GSSAPI**: аутентификация клиентов и межброкерного взаимодействия через KDC (`COMPANY.LOCAL`).
2. **Schema Registry SPNEGO Negotiate**: HTTP-авторизация в Hortonworks и Confluent реестрах схем по билетам Kerberos.
3. **Zero-Copy Wire Transcoding**: горячее преобразование 9-байтового HWX-заголовка в 5-байтовый Confluent-заголовок без десериализации Avro payload.
4. **Passthrough для Raw JSON/Text**: прямой байтовый проброс сообщений без схемы.
5. **Поддержка Tombstone (`null payload`)**: корректное удаление ключей в compact-топиках.
6. **Защита от циклов (Loop Prevention)**: фильтрация дубликатов по заголовку `X-Repl-Origin`.
7. **Защитный гейт синхронизации смещений**: блокировка перезаписи оффсетов для активных Consumer Groups в целевом ДЦ.


---

## 📈 Нагрузочный прогон HMS-репликации (200k партиций, Kerberos)

Стенд проверяет перенос схемы с таблицей на 200 000 партиций Parquet между двумя керберизированными метасторами (Hive 4.0.0):

```bash
# Нужны свежие JAR-файлы: make build-java
./run-hms-load-test.sh                    # 200 000 партиций
PARTITIONS=20000 ./run-hms-load-test.sh   # короткий прогон
SKIP_UP=1 ./run-hms-load-test.sh          # стенд уже поднят
```

Сценарий:

1. Поднимает KDC, оба HDFS, оба метастора, оркестратор и два агента с наложенным `docker-compose.hms-load.yml`.
2. Генерирует таблицу `loadtest.big_table` (`HmsTool generate-partitions`, пакеты по 1000).
3. Создает задачу HMS-репликации и ждет конца bootstrap, снимая `docker stats` агентов и оркестратора.
4. Сверяет приемник: число партиций и формат хранения (`HmsTool count-partitions`).
5. Проверяет жизненный цикл через API: пауза, повторная пауза, возобновление, возобновление задачи не на паузе (ожидается 409), постраничный список подзадач.

Что заложено в наложение и зачем:

| Настройка | Зачем |
|---|---|
| Куча агента `-Xmx512m`, `ExitOnOutOfMemoryError` | Партиции читаются порциями; агент, державший все 200k в памяти, на этом пределе падал бы |
| TGT на 5 минут (`load/krb5-load.conf`) | Bootstrap дольше срока билета: без перелогина по keytab новые соединения с метастором падают |
| `REPLICATOR_HMS_DATA_BARRIER_ENABLED=false` | Прогон меряет метаданные; копирование 200k пустых каталогов HDFS к этому ничего не добавляет |
| Оркестратор без HiveServer2 | Для репликации схем он не нужен, а память стенда уходит на метасторы |

Ограничения стенда: метасторы работают на встроенном Derby, а не на PostgreSQL/MySQL, как в бою. Агентов два, а не 200+: раздачу задач между агентами покрывают интеграционные тесты оркестратора (`HmsDispatchAtScaleTest`).

## 🐝 Масштабный прогон: 200k партиций, HMS 3.1, рой из 200 агентов

```bash
./run-hms-scale-test.sh                                  # 200 000 партиций, 200 агентов, 100 задач роя
PARTITIONS=5000 SWARM_AGENTS=50 SWARM_JOBS=20 SWARM_SECONDS=300 ./run-hms-scale-test.sh
```

Стенд поднимается на метасторах Hive 3.1.3 с включенным барьером «данные раньше DDL» (`LOAD_DATA_BARRIER=true`): DDL партиций ждет настоящих HDFS-подзадач копирования. Пока два настоящих агента переносят таблицу, рой эмулированных агентов (`AgentSwarm`) нагружает оркестратор по настоящему протоколу: регистрация, heartbeat раз в 5 с, опрос задач раз в 2–3 с и отчеты о прогрессе по полученным задачам. Рой живет в кластерах `swarm-src`/`swarm-dst` и не трогает задачи стенда (whitelist кластеров на стенде снят).

В конце рой печатает задержки p50/p95/p99 по каждому вызову, ошибки и распределение задач по агентам. Прогон считается пройденным, если таблица перенесена целиком, у роя нет ошибок, а каждая задача роя досталась какому-то агенту.

Рой можно запустить и отдельно, против любого оркестратора:

```bash
docker exec replicator-agent-dc1 java -cp /app/replicator-agent.jar \
  org.apache.hadoop.explorer.replicator.tool.AgentSwarm orchestrator:50050 <agent-secret> 200 600
```

---

### Живая проверка событий INSERT

```bash
./run-hms-insert-test.sh                          # на поднятом стенде с метасторами 3.1 и барьером
WORKLOAD=insert SKIP_UP=1 ./run-hms-scale-test.sh # то же под нагрузкой роя из 200 агентов
```

Событие `INSERT` метастор пишет, когда DML-клиент после записи файлов вызывает `fire_listener_event` (HiveServer2 делает это в `Hive.fireInsertEvent` при `hive.metastore.dml.events=true`). Сценарий делает то же самое: кладет настоящие файлы в каталог партиции на HDFS-источнике и вызывает `fire_listener_event` (`HmsTool fire-insert`), и метастор 3.1 пишет настоящее событие в формате `json-0.2`. Проверяется, что bootstrap с барьером переносит файлы партиции, `INSERT INTO` доносит новый файл свежей подзадачей, а `INSERT OVERWRITE` убирает на приемнике устаревшие файлы.

HiveServer2 3.1.3 из образа `apache/hive` для этого не используется: на Hadoop 3 внутри образа он не исполняет запросы локально (MapReduce падает на `NoClassDefFound metrics/Updater`, Tez в локальном режиме не проходит Kerberos).

---

## 🧩 Совместимость с Hive Metastore 3.1 (HDP 3.1)

Агент работает с клиентом метастора Hive 3.1. Он нативно говорит на Thrift-контракте 3.1, а с метастором 4.x работает через сохраненные там методы 3.x. Проверка на метасторах `apache/hive:3.1.3` с Kerberos:

```bash
./run-hms31-compat-test.sh
```

Сценарий поднимает стенд с наложением `docker-compose.hms31.yml`, переносит таблицу на 5000 партиций Parquet и проверяет CDC в формате событий Hive 3 (`json-0.2`): `ADD_PARTITION`, `DROP_PARTITION`, `CREATE_TABLE`, `DROP_TABLE`. В конце проверяет, что в логах агентов нет ошибок протокола (`Invalid method name`).

Entrypoint образа 3.1.3 инициализирует схему Derby на каждом старте и падает, если она уже есть. Поэтому повторный старт метасторов делается с `HMS31_IS_RESUME=true`.

---

## 🛡️ Отказы, CDC под нагрузкой и HDFS-стриминг (inotify)

Сценарии запускаются на поднятом стенде с метасторами 3.1 и барьером данных. Каждый добавляет к нему свои слои:

| Слой | Что меняет |
|---|---|
| `docker-compose.ha.yml` | оркестратор на PostgreSQL и второй узел (`localhost:8006`); агенты знают оба узла |
| `docker-compose.hms-pg.yml` | метастор-источник на PostgreSQL вместо Derby |
| `docker-compose.inotify.yml` | стримеры источника; принципал `hdfs` включается в группу суперпользователей HDFS |

### Отказ узла оркестратора и агента посреди bootstrap

```bash
./run-hms-ha-test.sh
PARTITIONS=10000 ./run-hms-ha-test.sh
```

Проверяется, что оба узла видят агентов, остановка узла 1 не прерывает перенос, его возврат не сбрасывает bootstrap, а перезапуск агента-источника продолжает перенос с чекпоинта.

### CDC на нагруженном метасторе

```bash
./run-hms-cdc-load-test.sh                                           # 30 баз, 16 потоков, 5 минут
DBS=50 THREADS=16 LOAD_SECONDS=600 PARTITIONS_PER_EVENT=20 ./run-hms-cdc-load-test.sh
```

30 задач (по базе на задачу) в состоянии `ACTIVE`, а генератор (`HmsTool churn`) пишет в журнал метастора поток `ADD_PARTITION` (по `PARTITIONS_PER_EVENT` партиций в событии) и `DROP_PARTITION`. Сценарий следит за отставанием самой медленной задачи, ждет, пока все догонят журнал, и сверяет число партиций в каждой базе на приемнике, журналы задач и логи оркестраторов.

Прогон делается только на метасторе с настоящей СУБД (`docker-compose.hms-pg.yml`): на Derby метастор не блокирует счетчик событий, при параллельных DDL-клиентах в журнале появляются одинаковые номера, и клиент Hive 3.1 видит в этом пропуск журнала.

Итог прогона на стенде (6 ядер, метастор и все сервисы в одном docker):

| Показатель | Значение |
|---|---|
| Нагрузка | 5648 событий, 56 480 партиций за 5 минут (≈20 событий/с, потолок метастора стенда) |
| Отставание к концу нагрузки | 6205 событий |
| Догоняние после нагрузки | 28 минут (с барьером: подзадача копирования на каждую новую партицию) |
| Расхождения, FAILED-события, ERROR, ошибки аренды | нет |

Пока генератор нагружает метастор (CPU 300%), задачи упираются в него же: каждое событие требует чтения партиций. Без конкуренции за метастор конвейер разбирает 40–80 барьерных подзадач в секунду на агента-источника.

### HDFS-стриминг для HMS-задачи

```bash
./run-hms-inotify-test.sh
```

Задача с `enable_hdfs_streaming=true` и флажком «Удалять лишние партиции». Проверяется:

1. компаньон `STREAMING_INOTIFY` создан, стример источника в сети;
2. новый файл в существующей партиции без события `INSERT` доезжает до приемника;
3. перезапись партиции в стиле Spark (файл из `.spark-staging`, старые удалены) — на приемнике остаются только новые файлы;
4. пауза задачи останавливает перенос, а после возобновления доезжают и файл, записанный на паузе, и удаление, сделанное на паузе;
5. удаление задачи удаляет и компаньон;
6. задача без флажков сверки дозапись переносит, но файл, удаленный на источнике, на приемнике остается.

Сценарий пересоздает NameNode источника с новой группой суперпользователей; данные HDFS стенда при этом теряются (анонимный том).

---

## 🗺️ Топология стенда
- **ЦОД 1 (Москва / Primary)**: содержит 2 кластера HDFS (`demo-cluster` — Prod DataLake и `analytics-cluster` — Secondary DataLake).
- **ЦОД 2 (Санкт-Петербург / Disaster Recovery)**: содержит 1 кластер HDFS (`backup-cluster` — DR Mirror DataLake).
- **Лимиты каналов**:
  - Магистраль **DC1 ➔ DC2**: 100 МБ/с.
  - Канал **Prod ➔ Backup**: 60 МБ/с.
  - Канал **Analytics ➔ Backup**: 40 МБ/с.
  - Локальный обмен **Prod ➔ Analytics (DC1)**: 80 МБ/с.
  - Глобальный пул полосы: 120 МБ/с.

---

## 👥 Тестовые учетные записи (RBAC)

Для проверки разграничения прав доступа в окне авторизации доступны три преднастроенных пользователя:

1. **👑 `admin_user` / `password123` (Александр Админов)**:
   - Роль: `ADMIN`.
   - Права: видит **все задачи всех пользователей**, имеет право менять сетевые лимиты DC-DC и HDFS-HDFS в реальном времени.
2. **⚙️ `de_user` / `password123` (Иван Датаинженеров)**:
   - Роль: `WRITER`.
   - Права: видит **только свои персональные задачи** репликации.
3. **📊 `analyst_user` / `password123` (Анна Аналитикова)**:
   - Роль: `READER`.
   - Права: видит **только свои задачи** в режиме чтения.

---

## 📖 Документация
- Подробное руководство пользователя: [docs/replicator-user-guide.md](../../docs/replicator-user-guide.md)
- Руководство по конфигурации: [docs/CONFIGURATION.md](../../docs/CONFIGURATION.md)
- Архитектурный документ: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)

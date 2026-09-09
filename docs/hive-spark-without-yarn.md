# Архитектура и работа Apache Hive и Apache Spark без кластера YARN

В данном документе описываются архитектурные принципы, схемы взаимодействия и особенности конфигурации, позволяющие запускать **Apache Hive** и **Apache Spark** на тестовых, локальных и демонстрационных стендах **без развертывания распределенного менеджера ресурсов Apache Hadoop YARN** (ResourceManager и NodeManagers).

---

## 📑 Содержание

- [1. Общий обзор концепции](#1-общий-обзор-концепции)
- [2. Apache Spark без YARN](#2-apache-spark-без-yarn)
  - [2.1 Режим `local[*]`](#21-режим-local)
  - [2.2 Интеграция с Apache Livy](#22-интеграция-с-apache-livy)
  - [2.3 Альтернативные бескластерные и автономные режимы](#23-альтернативные-бескластерные-и-автономные-режимы)
- [3. Apache Hive без YARN](#3-apache-hive-без-yarn)
  - [3.1 Hive Metastore (HMS) как независимый сервис](#31-hive-metastore-hms-как-независимый-сервис)
  - [3.2 HiveServer2 (HS2) в локальном режиме](#32-hiveserver2-hs2-в-локальном-режиме)
  - [3.3 Делегирование вычислений современным движкам (Trino и Spark)](#33-делегирование-вычислений-современным-движкам-trino-и-spark)
- [4. Схема взаимодействия компонентов](#4-схема-взаимодействия-компонентов)
- [5. Примеры конфигурации стенда](#5-примеры-конфигурации-стенда)
  - [5.1 Конфигурация Livy и Spark (`livy.conf`, `spark-defaults.conf`)](#51-конфигурация-livy-и-spark-livyconf-spark-defaultsconf)
  - [5.2 Конфигурация HiveServer2 (`hive-site.xml`)](#52-конфигурация-hiveserver2-hive-sitexml)
- [6. Сравнение режимов работы: YARN vs Standalone/Local](#6-сравнение-режимов-работы-yarn-vs-standalonelocal)

---

## 1. Общий обзор концепции

В полноценном продакшн-кластере Hadoop сервис **YARN** отвечает за централизованное распределение ресурсов CPU и RAM, планирование очередей (*Capacity Scheduler*, *Fair Scheduler*) и изоляцию процессов через cgroups в NodeManager-контейнерах.

Однако для локальной разработки, CI/CD пайплайнов и демонстрационных стендов стек YARN создает избыточный оверхед:
- Высокое фоновое потребление оперативной памяти демонами RM, NM, JobHistoryServer (от 4–8 GB RAM).
- Дополнительные задержки (10–30 секунд) на согласование контейнеров, запуск `ApplicationMaster` и регистрацию исполнителей.
- Усложнение конфигурации Kerberos-тикетов для NodeManager.

Исключение YARN на стенде позволяет запускать все компоненты за **1–3 секунды** с минимальным потреблением ресурсов (до 1–2 GB RAM на весь стек).

---

## 2. Apache Spark без YARN

### 2.1 Режим `local[*]`

В режиме `local[*]` Spark не обращается к внешним кластерным менеджерам:
- **Единый процесс JVM**: Spark Driver и вычислительные потоки (*Executors*) выполняются внутри одного и того же Java-процесса.
- **Многопоточность**: Параметр `*` указывает Spark задействовать все логические ядра процессора хоста/контейнера (или конкретное число, например `local[4]`).
- **Прямой доступ к данным**: Spark считывает и записывает файлы в HDFS (`hdfs://namenode:9000`) по прямому протоколу Hadoop RPC / WebHDFS или в S3/MinIO по протоколу S3A, используя встроенные Hadoop Client библиотеки.

### 2.2 Интеграция с Apache Livy

**Apache Livy** предоставляет REST API для интерактивного создания сессий и отправки пакетных заданий (PySpark, Spark SQL, Scala Spark):
1. Веб-клиент (*Spark Explorer*) отправляет POST-запрос на создание сессии в Livy.
2. Livy стартует локальный процесс `spark-submit` в режиме `deploy-mode = client` с параметром `master = local[*]`.
3. Сессия сразу переходит в статус `idle` и готова принимать код на исполнение, не ожидая аллокации ресурсов от YARN.
4. Ограничение ресурсов регулируется напрямую параметрами JVM: `spark.driver.memory` и `spark.executor.memory`.

### 2.3 Альтернативные бескластерные и автономные режимы

- **Spark Standalone Mode**: встроенный легковесный мастер Spark (`spark://master:7077`), не требующий компонентов Hadoop. Поднимается один контейнер `spark-master` и один или несколько `spark-worker`.
- **Spark on Kubernetes**: Driver обращается напрямую к Kubernetes API и динамически создает Pod'ы для каждого исполнителя.

---

## 3. Apache Hive без YARN

Архитектурно Apache Hive разделен на уровень хранения метаданных и уровень выполнения запросов.

### 3.1 Hive Metastore (HMS) как независимый сервис

**Hive Metastore** вообще не зависит от YARN:
- HMS — это самостоятельный Thrift-сервис (порт `9083`), управляющий каталогом метаданных (схемы таблиц, типы данных колонок, партиции, пути хранения файлов в HDFS/S3, статистика).
- Данные метастора сохраняются в реляционной СУБД (PostgreSQL, MySQL, MariaDB или локальная встраиваемая Apache Derby).
- Любые внешние движки (Trino, Spark, HiveServer2, Presto) обращаются к HMS напрямую по протоколу Thrift.

### 3.2 HiveServer2 (HS2) в локальном режиме

**HiveServer2** принимает SQL-запросы по протоколу JDBC/ODBC (Thrift порт `10000`):
- **Fetch Task**: DDL-команды (`CREATE TABLE`, `ALTER`, `DROP`, `SHOW TABLES`, `DESCRIBE`) и простые выборки (`SELECT * FROM table LIMIT 50`) выполняются самим процессом HS2 без генерации задач MapReduce/Tez.
- **Local Mode**: Для аналитических запросов с агрегацией и фильтрацией при отсутствии YARN движок переключается в локальный режим (Local MapReduce или Local Tez), исполняя вычисления в пуле потоков внутри JVM HiveServer2.

### 3.3 Делегирование вычислений современным движкам (Trino и Spark)

На стендах тяжелые MapReduce/Tez задачи часто полностью заменяются MPP-движком **Trino** или **Spark SQL**:
- **Trino Coordinator** подключается к Hive Metastore только для получения схемы и метаданных каталога, после чего параллельно считывает паркет/ORC файлы из HDFS и обрабатывает их на своем собственном in-memory распределенном движке.
- **Spark Explorer** использует `spark.hadoop.hive.metastore.uris` и обращается к метастору напрямую через `HiveContext` / `SparkSession.builder().enableHiveSupport()`.

---

## 4. Схема взаимодействия компонентов

```mermaid
flowchart TD
    subgraph Clients["Клиенты и Web UI"]
        UI_SQL["SQL Explorer\n(:8003)"]
        UI_SPARK["Spark Explorer\n(:8004)"]
        Beeline["Beeline / JDBC Client"]
    end

    subgraph Engines["Вычислительные движки (Без YARN)"]
        TRINO["Trino Coordinator\n(Встроенный MPP движок, :8080)"]
        HS2["HiveServer2\n(Local Execution, :10000)"]
        LIVY["Apache Livy Server\n(SPARK_MASTER = local[*], :8998)"]
    end

    subgraph StorageMeta["Слой метаданных и хранения"]
        HMS["Hive Metastore\n(Thrift API, :9083)"]
        RDBMS[("PostgreSQL / Derby\n(Схемы и таблицы)")]
        HDFS["HDFS NameNode + DataNode\n(Hadoop RPC :9000 / WebHDFS :9870)"]
    end

    UI_SQL -->|JDBC / HTTP| TRINO
    UI_SQL -->|Thrift / JDBC| HS2
    UI_SPARK -->|REST API| LIVY
    Beeline -->|Thrift / Kerberos| HS2

    TRINO -->|1. Запрос метаданных (Thrift)| HMS
    TRINO -->|2. Прямое чтение файлов| HDFS
    
    HS2 -->|1. Запрос метаданных (Thrift)| HMS
    HS2 -->|2. Локальное чтение данных| HDFS
    
    LIVY -->|1. Каталог таблиц (Thrift)| HMS
    LIVY -->|2. Чтение/Запись DataFrame| HDFS
    
    HMS -->|SQL запросы к схемам| RDBMS
```

---

## 5. Примеры конфигурации стенда

### 5.1 Конфигурация Livy и Spark (`livy.conf`, `spark-defaults.conf`)

Файл `livy.conf`:
```properties
livy.server.port = 8998
livy.server.host = 0.0.0.0
# Указание локального мастера Spark
livy.spark.master = local[*]
livy.spark.deploy-mode = client
livy.repl.enable-hive-context = true
livy.impersonation.enabled = false
```

Файл `spark-defaults.conf`:
```properties
# Локальный мастер
spark.master = local[*]
spark.app.name = LivySparkApp

# Прямое подключение к файловой системе HDFS
spark.hadoop.fs.defaultFS = hdfs://hdfs-cluster-1:9000

# Прямое подключение к метастору Hive
spark.hadoop.hive.metastore.uris = thrift://hive-metastore-1:9083
spark.sql.catalogImplementation = hive
spark.sql.warehouse.dir = hdfs://hdfs-cluster-1:9000/warehouse

# Лимиты памяти локальной JVM
spark.driver.memory = 1g
spark.executor.memory = 1g
```

### 5.2 Конфигурация HiveServer2 (`hive-site.xml`)

```xml
<configuration>
  <!-- Адрес Hive Metastore сервиса -->
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore-1:9083</value>
  </property>

  <!-- Путь к хранилищу таблиц в HDFS -->
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://hdfs-cluster-1:9000/warehouse</value>
  </property>

  <!-- Порт и хост HiveServer2 -->
  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>0.0.0.0</value>
  </property>

  <!-- Отключение тяжелого кэширования для легкого стенда -->
  <property>
    <name>hive.query.results.cache.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

---

## 6. Сравнение режимов работы: YARN vs Standalone/Local

| Характеристика | Стенд без YARN (`local[*]`) | Кластер с Apache Hadoop YARN |
|---|---|---|
| **Назначение** | Разработка, демонстрационные стенды, CI/CD тесты, локальная отладка | Продакшн-эксплуатация, многопользовательская аналитика больших данных |
| **Потребление ресурсов** | Минимальное (~1–2 GB RAM на весь стек сервисов) | Высокое (от 8–16 GB RAM на служебные демоны и контейнеры) |
| **Время запуска сессии Spark** | **1–3 секунды** (in-process JVM) | **15–45 секунд** (согласование с RM, запуск AM и контейнеров) |
| **Управление очередями** | Отсутствует (FIFO в рамках потоков процесса) | Иерархические очереди *Capacity Scheduler* с весами и квотами |
| **Изоляция вычислений** | На уровне потоков единой JVM | На уровне cgroups и операционной системы контейнеров |
| **Горизонтальное масштабирование** | Ограничено ресурсами одного хоста/контейнера | Распределение на десятки и сотни физических серверов |
| **Сложность инфраструктуры** | Минимальная (легковесные Docker-контейнеры) | Высокая (требуется мониторинг нод, Healthcheck, ZooKeeper, HA) |

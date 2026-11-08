# Объединенный демо-стенд платформы (demo/all)

Окружение Docker Compose со всеми 5 сервисами Hadoop Explorer Platform и общей инфраструктурой: Kerberos KDC, OpenLDAP, HDFS, Hive Metastore, YARN, Trino, Livy, Replicator Orchestrator и агенты.

---

## Контейнеры стенда

| Контейнер | Назначение | Внутренний порт | Внешний порт хоста |
|---|---|---|---|
| `kdc` | MIT Kerberos KDC (`COMPANY.LOCAL`) | `88/tcp`, `88/udp` | `88` |
| `ldap` | OpenLDAP (`dc=company,dc=local`) | `389` | `389` |
| `hdfs-cluster-1` | Hadoop NameNode + DataNode + WebHDFS (Cluster 1) | `9870`, `9000` | `9870`, `9864` |
| `hdfs-cluster-2` | Hadoop NameNode + DataNode + WebHDFS (Cluster 2 - Archive) | `9870`, `9000` | `9872`, `9865` |
| `yarn-rm-1` | YARN ResourceManager 1 (Capacity Scheduler) | `8088` | `8088` |
| `yarn-rm-2` | YARN ResourceManager 2 (Ad-hoc Cluster) | `8088` | `8089` |
| `hive-metastore-1` | Apache Hive Metastore 1 (Thrift, Cluster 1) | `9083` | `9083` |
| `hive-metastore-2` | Apache Hive Metastore 2 (Thrift, Cluster 2) | `9083` | `9084` |
| `hive-server-1` | Apache HiveServer2 1 (Kerberized Thrift, Cluster 1) | `10000`, `10002` | `10000`, `10002` |
| `hive-server-2` | Apache HiveServer2 2 (Kerberized Thrift, Cluster 2) | `10001`, `10002` | `10001`, `10003` |
| `trino-coordinator` | Trino Coordinator с Hive/TPCH коннекторами | `8080` | `8080` |
| `spark-livy-1` | Apache Livy Server 1 REST API для Spark (Cluster 1) | `8998` | `8998` |
| `spark-livy-2` | Apache Livy Server 2 REST API для Spark (Cluster 2) | `8999` | `8999` |
| `spark-yarn-nm-1` | YARN NodeManager для сессий Livy 1 (RM `yarn-rm-1`) | `45454`, `8042` | — |
| `spark-yarn-nm-2` | YARN NodeManager для сессий Livy 2 (RM `yarn-rm-2`) | `45454`, `8042` | — |
| `orchestrator` | Replicator Orchestrator и веб-интерфейс (Control Plane) | `8005`, `50050` | `8005`, `50050` |
| `agent-dc1` | Replicator gRPC Data Worker (DC1 Sender/Receiver) | `50051` | `50051` |
| `agent-dc2` | Replicator gRPC Data Worker (DC2 Sender/Receiver) | `50051` | `50052` |
| `prometheus` | Prometheus Server (метрики всех 5 сервисов) | `9090` | `9090` |
| `grafana` | Grafana (дашборды платформы) | `3000` | `3000` |
| `yarn-explorer` | YARN Explorer (UI и бэкенд) | `8000` | `8001` |
| `hdfs-explorer` | HDFS Explorer (UI и бэкенд) | `8000` | `8002` |
| `sql-explorer` | SQL Explorer (UI и бэкенд) | `8003` | `8003` |
| `spark-explorer` | Spark Explorer (UI и бэкенд) | `8004` | `8004` |

---

## Требования к памяти Docker

На Docker Desktop с 11,67 ГБ памяти (6 CPU, arm64) стенд в простое занимает около 10,3 ГБ по `docker stats`, из них около 0,6 ГБ — NodeManager'ы `spark-yarn-nm-1` и `spark-yarn-nm-2`. Каждая сессия Spark профиля `small` добавляет JVM драйвера и executor'а в контейнере NodeManager (около 1,4 ГБ). При нехватке памяти OOM-killer ядра VM Docker убивает процессы контейнеров: `trino-coordinator` и `yarn-rm-1` перезапускаются (`RestartCount` в `docker inspect`), сессии Livy уходят в `dead`. Проверка: `docker run --rm --privileged alpine:3.20 dmesg | grep "Killed process"`. На 11,67 ГБ это происходило еще до запуска первой сессии Spark. Для `demo/all` выделите Docker не меньше 16 ГБ [проверить: минимальный объем, при котором стенд с сессией Spark работает без OOM]; для проверки одного сервиса используйте его стенд (`demo/spark`, `demo/sql` и т. д.).

## Запуск стенда

```bash
# Из корневой директории:
make demo-all

# Либо напрямую из каталога demo/all:
cd demo/all && ./start-all-demo.sh
```

Скрипт `start-all-demo.sh`:
1. Проверяет доступность Docker и Docker Compose.
2. Поднимает инфраструктурные контейнеры (`kdc`, `ldap`).
3. Генерирует Kerberos keytab-файлы и инициализирует пользователей OpenLDAP.
4. Запускает кластерные службы HDFS, YARN, Hive Metastore, HiveServer2, Trino и Livy.
5. Инициализирует демо-таблицы в Hive Metastore: `demo/sql/hive/init-demo-data.sh` через HiveServer2 (`customers`, `transactions`, `orders`, `events_log`, `daily_metrics`), затем `demo/spark/init-demo-tables.sh` через Livy в YARN создает отсутствующие `customers`, `transactions` (кластер 1) и `quarterly_reports` (кластер 2).
6. Запускает 5 веб-сервисов Hadoop Explorer Platform, включая Replicator Orchestrator и агенты.

### Веб-интерфейсы
- YARN Explorer: [http://localhost:8001](http://localhost:8001)
- HDFS Explorer: [http://localhost:8002](http://localhost:8002)
- SQL Explorer: [http://localhost:8003](http://localhost:8003)
- Spark Explorer: [http://localhost:8004](http://localhost:8004)
- Hadoop gRPC Replicator: [http://localhost:8005](http://localhost:8005)
- Grafana: [http://localhost:3000](http://localhost:3000) (Дашборды YARN, HDFS, Replicator, Kafka, HMS DR)
- Prometheus: [http://localhost:9090](http://localhost:9090)

---

## Остановка стенда

```bash
# Из корневой директории:
make demo-all-stop

# Либо напрямую:
cd demo/all && ./stop-all-demo.sh
```

Остановка с удалением томов Docker (данные стенда будут потеряны):
```bash
docker compose -f demo/all/docker-compose.all.yml down -v
```

---

## Учетные записи и роли

Учетные записи заведены в общем OpenLDAP и Kerberos KDC.
Пароль всех пользователей: `password123`.

| Логин | Роль (бейдж в UI) | Группы LDAP | Права в YARN | Права в HDFS | Права в SQL | Права в Spark |
|---|---|---|---|---|---|---|
| `admin` / `admin_user` | `ADMIN` (`ADM`) | `admins`, `hadoop-admins` | Утверждение Change Requests | Полный доступ (R/W, квоты, ACL) | Все кластеры и каталоги | Все очереди и кластеры |
| `engineer` / `engineer_user` | `WRITER` (`RW`) | `engineers`, `data-engineers` | Создание Change Requests | Запись в `/data`, `/tmp` | Выполнение DDL и DML | Очереди `root.etl`, `root.adhoc` |
| `analyst` / `analyst_user` | `READER` (`RO`) | `analytics`, `analysts` | Только просмотр очередей | Чтение `/data` | Только SELECT запросы | Очередь `root.analytics` |

---

## Сценарии сквозной проверки

### YARN Explorer (`:8001`)
- Войдите как `engineer` и создайте заявку на перераспределение весов очередей `root.etl` и `root.analytics`.
- Войдите как `admin` и согласуйте заявку кнопкой **Одобрить и сгенерировать XML**.

### HDFS Explorer (`:8002`)
- Войдите как `engineer` (`password123`).
- Перейдите в `/data` и просмотрите директории таблиц Hive.
- Откройте предпросмотр файлов Parquet или текстовых логов.

### SQL Explorer (`:8003`)
- Войдите как `analyst` (`password123`).
- В дереве каталога выберите `hive -> default -> customers`.
- Выполните запрос:
  ```sql
  SELECT country, count(*) as cnt 
  FROM hive.default.customers 
  GROUP BY country 
  ORDER BY cnt DESC;
  ```
- Убедитесь, что вкладки и история запросов сохраняются в БД при обновлении страницы.

### Spark Explorer (`:8004`)
- Войдите как `engineer` (`password123`).
- Создайте интерактивную сессию PySpark в очереди `root.prod.spark`. Сессия запускается в YARN cluster mode и переходит в `idle` через 30–90 секунд (подробности — `demo/spark/README.md`, раздел «Запуск сессий в YARN»).
- Выполните код:
  ```python
  df = spark.table("customers")
  df.groupBy("city").count().show()
  ```
- Переключитесь на вкладку Spark SQL и выполните запрос. Результаты PySpark и Spark SQL хранятся в отдельных буферах: запуск одного движка не затирает вывод другого.

# 🌐 Объединенный демонстрационный стенд платформы (demo/all)

Единое демонстрационное окружение Docker Compose, развертывающее полный стек **Hadoop Explorer Platform** со всеми 4 сервисами и общей инфраструктурой безопасности (Kerberos KDC, OpenLDAP, HDFS, Hive Metastore, YARN, Trino, Livy).

---

## 🏗️ Архитектура сервисов стенда

| Контейнер | Назначение | Внутренний порт | Внешний порт хоста |
|---|---|---|---|
| **`kdc`** | MIT Kerberos KDC (`COMPANY.LOCAL`) | `88/tcp`, `88/udp` | `88` |
| **`ldap`** | OpenLDAP (`dc=company,dc=local`) | `389` | `389` |
| **`hdfs-cluster-1`** | Hadoop NameNode + DataNode + WebHDFS (Cluster 1) | `9870`, `9000` | `9870`, `9864` |
| **`hdfs-cluster-2`** | Hadoop NameNode + DataNode + WebHDFS (Cluster 2 - Archive) | `9870`, `9000` | `9872`, `9865` |
| **`yarn-rm-1`** | YARN ResourceManager 1 (Capacity Scheduler) | `8088` | `8088` |
| **`yarn-rm-2`** | YARN ResourceManager 2 (Ad-hoc Cluster) | `8088` | `8089` |
| **`hive-metastore-1`**| Apache Hive Metastore 1 (Thrift, Cluster 1) | `9083` | `9083` |
| **`hive-metastore-2`**| Apache Hive Metastore 2 (Thrift, Cluster 2) | `9083` | `9084` |
| **`hive-server-1`** | Apache HiveServer2 1 (Kerberized Thrift, Cluster 1) | `10000`, `10002` | `10000`, `10002` |
| **`hive-server-2`** | Apache HiveServer2 2 (Kerberized Thrift, Cluster 2) | `10001`, `10002` | `10001`, `10003` |
| **`trino-coordinator`** | Trino Coordinator с Hive/TPCH коннекторами | `8080` | `8080` |
| **`spark-livy-1`** | Apache Livy Server 1 REST API для Spark (Cluster 1) | `8998` | `8998` |
| **`spark-livy-2`** | Apache Livy Server 2 REST API для Spark (Cluster 2) | `8999` | `8999` |
| **`prometheus`** | Prometheus Server (Сбор метрик) | `9090` | `9090` |
| **`grafana`** | Grafana Dashboard (Дашборды мониторинга) | `3000` | `3000` |
| **`yarn-explorer`** | **YARN Web Explorer UI & Backend** | `8000` | **`8001`** |
| **`hdfs-explorer`** | **HDFS Web Explorer UI & Backend** | `8000` | **`8002`** |
| **`sql-explorer`** | **SQL Web Explorer UI & Backend** | `8000` | **`8003`** |
| **`spark-explorer`** | **Spark Web Explorer UI & Backend** | `8000` | **`8004`** |

---

## ⚡ Быстрый запуск

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
5. Инициализирует демо-таблицы в Hive Metastore (`customers`, `transactions`, `orders`, `events_log`, `daily_metrics`) через Livy.
6. Запускает 4 веб-сервиса Hadoop Explorer.

### Веб-интерфейсы приложений:
- 🎛️ **YARN Explorer**: [http://localhost:8001](http://localhost:8001)
- 📁 **HDFS Explorer**: [http://localhost:8002](http://localhost:8002)
- 📊 **SQL Web Explorer**: [http://localhost:8003](http://localhost:8003)
- ⚡ **Spark Explorer**: [http://localhost:8004](http://localhost:8004)

---

## 🛑 Остановка стенда

```bash
# Из корневой директории:
make demo-all-stop

# Либо напрямую:
cd demo/all && ./stop-all-demo.sh
```

Для полной очистки томов Docker:
```bash
docker compose -f demo/all/docker-compose.all.yml down -v
```

---

## 🔑 Учетные записи и роли

Все учетные записи синхронизированы через единый OpenLDAP и Kerberos KDC.
Пароль для всех пользователей: **`password123`**

| Логин | Роль (UI-бейдж) | Группы LDAP | Права в YARN | Права в HDFS | Права в SQL | Права в Spark |
|---|---|---|---|---|---|---|
| **`admin`** / `admin_user` | `ADMIN` (`ADM`) | `admins`, `hadoop-admins` | Утверждение Change Requests | Полный доступ (R/W, квоты, ACL) | Все кластеры и каталоги | Все очереди и кластеры |
| **`engineer`** / `engineer_user` | `WRITER` (`RW`) | `engineers`, `data-engineers` | Создание Change Requests | Запись в `/data`, `/tmp` | Выполнение DDL и DML | Очереди `root.etl`, `root.adhoc` |
| **`analyst`** / `analyst_user` | `READER` (`RO`) | `analytics`, `analysts` | Только просмотр очередей | Чтение `/data` | Только SELECT запросы | Очередь `root.analytics` |

---

## 🧪 Сценарии сквозного тестирования

### 1. YARN Explorer (`:8001`)
- Авторизуйтесь как `engineer` и создайте заявку на перераспределение весов очередей `root.etl` и `root.analytics`.
- Войдите как `admin` и согласуйте заявку (Approve & Apply).

### 2. HDFS Explorer (`:8002`)
- Авторизуйтесь как `engineer` (`password123`).
- Перейдите в `/data` и просмотрите директории таблиц Hive.
- Откройте предпросмотр файлов Parquet или текстовых логов.

### 3. SQL Explorer (`:8003`)
- Авторизуйтесь как `analyst` (`password123`).
- В дереве каталога выберите `hive -> default -> customers`.
- Выполните аналитический запрос:
  ```sql
  SELECT country, count(*) as cnt 
  FROM hive.default.customers 
  GROUP BY country 
  ORDER BY cnt DESC;
  ```
- Убедитесь, что вкладки и история запросов сохраняются в БД при обновлении страницы.

### 4. Spark Explorer (`:8004`)
- Авторизуйтесь как `engineer` (`password123`).
- Создайте интерактивную сессию PySpark в очереди `root.etl`.
- Выполните код:
  ```python
  df = spark.table("customers")
  df.groupBy("country").count().show()
  ```
- Переключитесь на вкладку Spark SQL и выполните запрос. Обратите внимание на независимость буферов результатов между движками.

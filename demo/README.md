# Демонстрационные стенды Hadoop Explorer

Каталог содержит локальные стенды на Docker Compose для демонстрации и интеграционного тестирования сервисов платформы:
- `demo/infra` — общая инфраструктура безопасности: MIT Kerberos KDC и OpenLDAP с заранее созданными пользователями и группами, генерация keytab-файлов.
- `demo/yarn` — стенд YARN Explorer (:8001) с двумя кластерами YARN ResourceManager и Capacity Scheduler.
- `demo/hdfs` — стенд HDFS Explorer (:8002) с двумя кластерами DataLake (WebHDFS + Kerberos).
- `demo/sql` — стенд SQL Explorer (:8003) с PostgreSQL, Apache Hive (Metastore + HiveServer2) и Trino.
- `demo/spark` — стенд Spark Explorer (:8004) с Apache Livy, Hive Metastore, YARN и HDFS.
- `demo/replicator` — стенд Hadoop gRPC Replicator (:8005) с межкластерной репликацией HDFS и Hive Metastore (2 HMS, 2 HDFS, 2 агента, KDC).
- `demo/monitoring` — Prometheus (:9090) и Grafana (:3000) с подключенным дашбордом.
- `demo/all` — все 5 сервисов, KDC, LDAP, Prometheus и Grafana в одном Compose-проекте; общие контейнеры запускаются один раз.

---

## Запуск стендов

### Все сервисы платформы
```bash
make demo-all
# Либо cd demo/all && ./start-all-demo.sh
```

Сервисы доступны по адресам:
- YARN Explorer: [http://localhost:8001](http://localhost:8001)
- HDFS Explorer: [http://localhost:8002](http://localhost:8002)
- SQL Explorer: [http://localhost:8003](http://localhost:8003)
- Spark Explorer: [http://localhost:8004](http://localhost:8004)
- Hadoop gRPC Replicator: [http://localhost:8005](http://localhost:8005)
- Grafana: [http://localhost:3000](http://localhost:3000) (анонимный вход с ролью admin)
- Prometheus: [http://localhost:9090](http://localhost:9090)

Остановка всех сервисов:
```bash
make demo-all-stop
```

### Отдельные стенды
- YARN Explorer: `make demo-yarn` (остановка: `make demo-yarn-stop`)
- HDFS Explorer: `make demo-hdfs` (остановка: `make demo-hdfs-stop`)
- SQL Explorer: `make demo-sql` (остановка: `make demo-sql-stop`)
- Spark Explorer: `make demo-spark` (остановка: `make demo-spark-stop`)
- Replicator: `make demo-replicator` (остановка: `make demo-replicator-stop`)
- Стек мониторинга: `make demo-monitoring` (остановка: `make demo-monitoring-stop`)

---

## Тестовые учетные записи (LDAP, Kerberos)

Пароль всех пользователей: `password123`.

| Пользователь | Отображаемое имя | Роль (бейдж в UI) | Группы LDAP | Права |
|---|---|---|---|---|
| `admin` / `admin_user` | Главный Администратор | `ADMIN` (`ADM`) | `admins`, `engineers`, `analytics`, `hadoop-admins` | Полный доступ (R/W во всех HDFS кластерах, утверждение Change Requests в YARN, управление сессиями Spark, админ SQL) |
| `engineer` / `engineer_user` / `de_user` | Инженер данных | `WRITER` (`RW`) | `engineers`, `data-engineers` | Чтение и запись в рабочие директории HDFS, создание заявок Change Requests в YARN, запуск расчетов в Livy Spark |
| `analyst` / `analyst_user` / `reader_user` | Аналитик данных | `READER` (`RO`) | `analytics`, `analysts`, `bi-analysts` | Доступ только для чтения к данным HDFS, выполнение SELECT-запросов в SQL Explorer, мониторинг очередей YARN и статусов сессий Spark |

---

## Инфраструктура безопасности (`demo/infra`)

- OpenLDAP: порт `389` (база `dc=company,dc=local`, файл инициализации `demo/infra/ldap/bootstrap.ldif`).
- MIT Kerberos KDC: порт `88` (realm `COMPANY.LOCAL`).
- Сервисные учетные записи и keytab-файлы монтируются в `/etc/security/keytabs` каждого контейнера для SPNEGO-аутентификации.

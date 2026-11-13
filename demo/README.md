# Демонстрационные стенды Pristan

Каталог содержит локальные стенды на Docker Compose для демонстрации и интеграционного тестирования сервисов платформы:
- `demo/infra` — общая инфраструктура безопасности: MIT Kerberos KDC и OpenLDAP с заранее созданными пользователями и группами, генерация keytab-файлов.
- `demo/artel` — стенд Artel (:8001) с двумя кластерами YARN ResourceManager и Capacity Scheduler.
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
- Artel: [http://localhost:8001](http://localhost:8001)
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
- Artel: `make demo-artel` (остановка: `make demo-artel-stop`)
- HDFS Explorer: `make demo-hdfs` (остановка: `make demo-hdfs-stop`)
- SQL Explorer: `make demo-sql` (остановка: `make demo-sql-stop`)
- Spark Explorer: `make demo-spark` (остановка: `make demo-spark-stop`)
- Replicator: `make demo-replicator` (остановка: `make demo-replicator-stop`)
- Стек мониторинга: `make demo-monitoring` (остановка: `make demo-monitoring-stop`)

---

## Тестовые учетные записи (LDAP, Kerberos)

Пароль всех пользователей: `password123`.

Вход на всех стендах — через OpenLDAP (`auth_method: ldap`); группы берутся поиском `groupOfNames` по
`member` (`ldap.group-search-filter`). Роль определяют `admin-groups`/`writer-groups` сервиса и роли кластеров
в `application-demo.yml`; в таблице — роль при настройках стендов.

| Пользователь | Отображаемое имя | Группы LDAP | Роль |
|---|---|---|---|
| `admin` | Главный Администратор | `hadoop-admins`, `data-platform-admins`, `platform-admins`, `superusers`, `data-engineers`, `artel-operators`, `bi-analysts`, `analytics` | `ADMIN` |
| `admin_user` | Александр Админов | `hadoop-admins`, `data-platform-admins`, `platform-admins`, `superusers`, `platform-team` | `ADMIN` |
| `hdfs` | Hadoop Superuser | `hadoop-admins` | `ADMIN` |
| `writer_user` | Владимир Инженеров | `data-engineers`, `artel-operators`, `dev-leads`, `etl-developers` | `WRITER` |
| `de_user` | Иван Датаинженеров | `data-engineers`, `artel-operators`, `bi-analysts`, `spark-users` | `WRITER` |
| `engineer` | Data Engineer | `data-engineers`, `artel-operators`, `bi-analysts`, `developers` | `WRITER` |
| `analyst_user` | Анна Аналитикова | `bi-analysts`, `analytics` | `READER` |
| `analyst` | BI Data Analyst | `bi-analysts`, `analytics` | `READER` |
| `reader_user` | Екатерина Наблюдателева | `bi-analysts`, `analytics`, `junior-devs` | `READER` |

Каталог создается из `demo/infra/ldap/bootstrap.ldif` при сборке образа `ldap` и первом старте контейнера.
После правки файла пересоздайте контейнер: `docker compose -f demo/infra/docker-compose.infra.yml up -d --build --force-recreate ldap`.

---

## Инфраструктура безопасности (`demo/infra`)

- OpenLDAP: порт `389` (база `dc=company,dc=local`, файл инициализации `demo/infra/ldap/bootstrap.ldif`).
- MIT Kerberos KDC: порт `88` (realm `COMPANY.LOCAL`).
- Сервисные учетные записи и keytab-файлы монтируются в `/etc/security/keytabs` каждого контейнера для SPNEGO-аутентификации.

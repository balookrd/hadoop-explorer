# 🚀 Демонстрационные стенды Hadoop Explorer

В данном каталоге представлены конфигурации локальных стендов в Docker Compose для демонстрации и интеграционного тестирования всех компонентов платформы:
- **`demo/infra`** — Единый инфраструктурный стек безопасности (MIT Kerberos KDC и OpenLDAP) с преднастроенными пользователями, группами и генерацией keytab-файлов.
- **`demo/yarn`** — Стенд YARN Explorer (:8001) с двумя кластерами YARN ResourceManager и Capacity Scheduler.
- **`demo/hdfs`** — Стенд HDFS Explorer (:8002) с двумя кластерами DataLake (WebHDFS + Kerberos).
- **`demo/sql`** — Стенд SQL Web Explorer (:8003) с PostgreSQL, Apache Hive (Metastore + HiveServer2) и Trino.
- **`demo/spark`** — Стенд Spark Explorer (:8004) с Apache Livy, Hive Metastore, YARN и HDFS.
- **`demo/monitoring`** — Стек наблюдаемости (Prometheus :9090 + Grafana :3000) с преднастроенным дашбордом.
- **`demo/all`** — Объединенный запуск всех 4 компонентов + KDC + LDAP + Prometheus + Grafana без дублирования сервисов.

---

## ⚡ Быстрый старт

### 1. Единый запуск всех сервисов платформы
```bash
make demo-all
# Либо cd demo/all && ./start-all-demo.sh
```

Сервисы доступны по адресам:
- 🎛️ **YARN Explorer**: [http://localhost:8001](http://localhost:8001)
- 📁 **HDFS Explorer**: [http://localhost:8002](http://localhost:8002)
- 📊 **SQL Web Explorer**: [http://localhost:8003](http://localhost:8003)
- ⚡ **Spark Explorer**: [http://localhost:8004](http://localhost:8004)
- 📈 **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (anonymous admin)
- 🎯 **Prometheus UI**: [http://localhost:9090](http://localhost:9090)

Остановка всех сервисов:
```bash
make demo-all-stop
```

### 2. Запуск раздельных стендов
- YARN Explorer: `make demo-yarn` (остановка: `make demo-yarn-stop`)
- HDFS Explorer: `make demo-hdfs` (остановка: `make demo-hdfs-stop`)
- SQL Explorer: `make demo-sql` (остановка: `make demo-sql-stop`)
- Spark Explorer: `make demo-spark` (остановка: `make demo-spark-stop`)
- Стек мониторинга: `make demo-monitoring` (остановка: `make demo-monitoring-stop`)

---

## 🔑 Учетные записи для тестирования (LDAP / Kerberos)

Все пароли для пользователей: **`password123`**

| Пользователь | Отображаемое имя | Роль (UI-бейдж) | Группы LDAP | Права и возможности |
|---|---|---|---|---|
| **`admin`** / **`admin_user`** | Главный Администратор | `ADMIN` (`ADM`) | `admins`, `engineers`, `analytics`, `hadoop-admins` | Полный доступ (R/W во всех HDFS кластерах, утверждение Change Requests в YARN, управление сессиями Spark, админ SQL) |
| **`engineer`** / **`engineer_user`** / **`de_user`** | Инженер данных | `WRITER` (`RW`) | `engineers`, `data-engineers` | Чтение и запись в рабочие директории HDFS, создание заявок Change Requests в YARN, запуск расчетов в Livy Spark |
| **`analyst`** / **`analyst_user`** / **`reader_user`** | Аналитик данных | `READER` (`RO`) | `analytics`, `analysts`, `bi-analysts` | Доступ только для чтения к данным HDFS, выполнение SELECT-запросов в SQL Explorer, мониторинг очередей YARN и статусов сессий Spark |

---

## 🏛️ Инфраструктура безопасности (demo/infra)

- **OpenLDAP**: Порт `389` (база `dc=company,dc=local`, файл инициализации `demo/infra/ldap/bootstrap.ldif`).
- **MIT Kerberos KDC**: Порт `88` (realm `COMPANY.LOCAL`).
- Сервисные учетные записи и keytabs монтируются в `/etc/security/keytabs` каждого контейнера для SPNEGO аутентификации.

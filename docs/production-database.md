# Переход со SQLite на PostgreSQL для High Availability (HA)

Документ описывает перевод сервисов Hadoop Explorer (`hdfs-explorer`, `sql-explorer`, `spark-explorer`, `yarn-explorer`) с локальной SQLite на PostgreSQL. По умолчанию сервисы хранят в SQLite сессии, отозванные токены, счетчики rate limiting и историю запросов.

> [!WARNING]
> С SQLite сервис запускается только в одном экземпляре.
> Для двух и более экземпляров за балансировщиком подключите внешний PostgreSQL.

---

## 1. Подготовка базы данных PostgreSQL

Создайте пользователя и по базе данных на каждый сервис (или одну базу с отдельными схемами и пользователями):

```sql
CREATE USER hadoop_explorer WITH PASSWORD 'your_strong_password';

CREATE DATABASE hdfs_explorer OWNER hadoop_explorer;
CREATE DATABASE sql_explorer OWNER hadoop_explorer;
CREATE DATABASE spark_explorer OWNER hadoop_explorer;
CREATE DATABASE yarn_explorer OWNER hadoop_explorer;
```

---

## 2. Настройка через переменные окружения (Standalone / Docker)

Задайте подключение переменными окружения:

| Переменная | Пример значения | Описание |
|---|---|---|
| `SPRING_DATASOURCE_URL` (или `DATABASE_URL`) | `jdbc:postgresql://host:5432/db` | Строка подключения JDBC (HikariCP) |
| `SPRING_DATASOURCE_USERNAME` | `hadoop_explorer` | Пользователь БД |
| `SPRING_DATASOURCE_PASSWORD` | `your_strong_password` | Пароль пользователя БД |

Пример запуска контейнера:
```bash
docker run -d \
  -e SPRING_DATASOURCE_URL="jdbc:postgresql://postgres:5432/sql_explorer" \
  -e SPRING_DATASOURCE_USERNAME="hadoop_explorer" \
  -e SPRING_DATASOURCE_PASSWORD="secret" \
  -e JWT_SECRET_KEY="your-production-secret-key" \
  -p 8003:8003 \
  hadoop-explorer/sql:latest
```

---

## 3. Проверка перед запуском в production

- [ ] `SPRING_DATASOURCE_URL` указывает на `jdbc:postgresql://...`.
- [ ] Пароль базы данных не хранится в открытом виде в `application.yml` или `docker-compose.yml`, а передается через переменную окружения из хранилища секретов (например, HashiCorp Vault).
- [ ] Проверка доступности на балансировщике смотрит на `/actuator/health/readiness`. По умолчанию группа readiness проверяет только состояние приложения, не PostgreSQL. Чтобы экземпляр выводился из балансировки при недоступной базе, добавьте в конфигурацию сервиса `management.endpoint.health.group.readiness.include: readinessState,db`; при сбое PostgreSQL из балансировки тогда выйдут все экземпляры сразу.

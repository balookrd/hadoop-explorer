# Переход со SQLite на PostgreSQL для High Availability (HA)

Документ описывает перевод сервисов Hadoop Explorer (`hdfs-explorer`, `sql-explorer`, `spark-explorer`, `yarn-explorer`) с локальной SQLite на PostgreSQL. По умолчанию сервисы хранят в SQLite сессии, отозванные токены, счетчики rate limiting и историю запросов.

> [!WARNING]
> С SQLite сервис запускается только в одной реплике (`replicaCount: 1`).
> Для двух и более реплик и автомасштабирования (HPA) подключите внешний PostgreSQL.

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

## 2. Конфигурация через Helm (Kubernetes)

Параметры подключения передаются через `values.yaml` или флаги `--set`.

### 2.1 Настройка `values.yaml` для подчарта

Пример для `sql-explorer`:

```yaml
replicaCount: 2

config:
  database:
    # URL для JDBC драйвера (PostgreSQL HikariCP)
    url: "jdbc:postgresql://postgres-cluster.database.svc.cluster.local:5432/sql_explorer?user=hadoop_explorer"

secrets:
  # Пароль базы данных, автоматически внедряемый в Deployment
  databasePassword: "your_strong_password"

# Включение PodDisruptionBudget (включается автоматически при replicaCount > 1)
podDisruptionBudget:
  enabled: true
  maxUnavailable: 1

# Автомасштабирование (HPA)
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  targetCPUUtilizationPercentage: 80
```

Шаблоны Deployment всех четырех чартов (`helm/charts/*/templates/deployment.yaml`) при `autoscaling.enabled: false` ставят `replicas: 1`, если `config.database.url` пуст, начинается с `sqlite` или `jdbc:sqlite` либо указывает на встроенную H2 (`jdbc:h2:file:`, `jdbc:h2:mem:`), и молча игнорируют больший `replicaCount`. С URL `jdbc:postgresql://...` используется значение `replicaCount`.

---

## 3. Настройка через переменные окружения (Standalone / Docker)

Вне Kubernetes задайте подключение переменными окружения:

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

## 4. Проверка перед запуском в production

- [ ] В `values.yaml` задан URL базы данных `jdbc:postgresql://...`.
- [ ] Пароль базы данных вынесен в Kubernetes Secret (`secrets.databasePassword` или SealedSecrets / HashiCorp Vault).
- [ ] Параметр `replicaCount >= 2`.
- [ ] Включен `PodDisruptionBudget` (`maxUnavailable: 1`).
- [ ] Включена `NetworkPolicy` (`networkPolicy.enabled: true`).
- [ ] Readiness-проба чарта смотрит на `/actuator/health/readiness`. По умолчанию группа readiness проверяет только состояние приложения, не PostgreSQL. Чтобы под выходил из Service при недоступной базе, добавьте в конфигурацию сервиса `management.endpoint.health.group.readiness.include: readinessState,db`; при сбое PostgreSQL из Service тогда выйдут все поды сразу.

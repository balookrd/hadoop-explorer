# Переход со SQLite на PostgreSQL для High Availability (HA)

По умолчанию микросервисы платформы **Hadoop Explorer** (`hdfs-explorer`, `sql-explorer`, `spark-explorer`, `yarn-explorer`) сконфигурированы для работы с локальной базой данных **SQLite** (хранение сессий, отзывов токенов, rate limits, истории запросов).

> [!WARNING]
> SQLite допускает запуск **только одной реплики** сервиса (`replicaCount: 1`).
> Для обеспечения высокой доступности (HA), запуска 2+ реплик и использования автоматического масштабирования (HPA) обязательно требуется подключение к внешней реляционной базе данных **PostgreSQL**.

---

## 1. Подготовка базы данных PostgreSQL

Создайте базы данных и пользователей для каждого микросервиса (или единую БД с разделением схем/пользователей):

```sql
CREATE USER hadoop_explorer WITH PASSWORD 'your_strong_password';

CREATE DATABASE hdfs_explorer OWNER hadoop_explorer;
CREATE DATABASE sql_explorer OWNER hadoop_explorer;
CREATE DATABASE spark_explorer OWNER hadoop_explorer;
CREATE DATABASE yarn_explorer OWNER hadoop_explorer;
```

---

## 2. Конфигурация через Helm (Kubernetes)

В production-окружении параметры подключения передаются через `values.yaml` или флаги `--set`:

### 2.1 Настройка `values.yaml` для подчарта

Пример для `sql-explorer`:

```yaml
replicaCount: 2

config:
  database:
    # URL для JDBC драйвера (PostgreSQL HikariCP)
    url: "jdbc:postgresql://postgres-cluster.database.svc.cluster.local:5432/sql_explorer?user=hadoop_explorer"

secrets:
  # Пароль базы данных, автоматически инжектируемый в Deployment
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

> [!NOTE]
> При указании строки подключения `jdbc:postgresql://...` Helm автоматически снимает ограничение на `replicaCount = 1`.

---

## 3. Настройка через переменные окружения (Standalone / Docker)

Если сервисы развертываются вне Kubernetes, переопределите переменные окружения:

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
  balookrd/sql-explorer:latest
```

---

## 4. Чек-лист проверки готовности к Production

- [ ] В `values.yaml` задан URL базы данных `jdbc:postgresql://...`.
- [ ] Пароль базы данных вынесен в Kubernetes Secret (`secrets.databasePassword` или SealedSecrets / HashiCorp Vault).
- [ ] Параметр `replicaCount >= 2`.
- [ ] Включен `PodDisruptionBudget` (`maxUnavailable: 1`).
- [ ] Включена `NetworkPolicy` (`networkPolicy.enabled: true`).
- [ ] Настроен эндпоинт готовности `/readyz` (проверяет доступность PostgreSQL).

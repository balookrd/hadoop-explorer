# Hadoop gRPC Replicator :: Orchestrator (Java 21 / Spring Boot 3)

Высокопроизводительный оркестратор межкластерной репликации данных Apache HDFS на стеке **Java 21 LTS** и **Spring Boot 3.3.4**, полностью интегрированный с общим ядром безопасности платформы `common-security-starter`.

---

## 1. Архитектура и функциональные возможности

Оркестратор репликации управляет топологией дата-центров, пулом агентов передачи данных (`agent-java`), распределением сетевых квот (Hierarchical Token Bucket) и жизненным циклом задач репликации.

### Основные компоненты

1. **Многоуровневый шейпинг сетевого трафика (`TokenBucketThrottler`)**:
   - Реализует алгоритм **Hierarchical Token Bucket (HTB)** для предотвращения перегрузки каналов передачи данных.
   - Трехуровневая иерархия лимитов:
     - **Global WAN Pool**: общий глобальный лимит всей инфраструктуры репликации;
     - **DC-to-DC Pool**: ограничение магистральных каналов между парами дата-центров (например, `dc1 ➔ dc2`);
     - **HDFS-to-HDFS Pool**: выделенная полоса между парами кластеров.
   - Потокобезопасный учет на базе `AtomicLong` с расчетом узкого горлышка `max(wait_global, wait_dc, wait_hdfs)`.

2. **Реестр агентов и обнаружение топологии (`AgentRegistry`)**:
   - Автоматическая регистрация агентов (`POST /api/v1/agents/register`) и мониторинг доступности по heartbeat (`POST /api/v1/agents/heartbeat`).
   - Автоматическое исключение оффлайн-агентов по таймауту.
   - Защита от SSRF (Server-Side Request Forgery): блокировка регистрации агентов на loopback, link-local и cloud-metadata адреса (`169.254.169.254`).
   - Аутентификация вызовов агентов через защищенный заголовок `X-Agent-Secret`.

3. **Планировщик и жизненный цикл задач (`ReplicationScheduler`, `JobService`)**:
   - Поддержка запуска по расписанию: интервальные пресеты (`@every_5m`, `@every_15m`, `@hourly`, `@daily`) и Cron-выражения (`UNIX` / `Spring`).
   - Ведение истории запусков (`JobRunEntity`) и детализация файлов (`TaskEntity`).
   - Автоматическая очистка устаревших запусков (`cleanupRetentionDays`).
   - Расчет метрик: процент выполнения, средняя скорость передачи (`MB/s`), время старта и завершения.

4. **Интеграция с `common-security-starter`**:
   - Централизованная аутентификация через JWT Cookies и Kerberos SPNEGO SSO.
   - Ролевая модель RBAC (`ADMIN` — полный контроль задач организации, `USER` — изоляция задач конкретного пользователя).
   - Защита от CSRF-атак на мутирующие запросы API.
   - Встроенный аудит критических операций `@Audited` и Rate Limiter (Token Bucket per IP).

---

## 2. Конфигурация (`application.yml`)

```yaml
server:
  port: 8005

hadoop:
  security:
    auth:
      mode: mock # mock, ldap, kerberos
      admin-groups:
        - "hadoop-admins"
        - "superusers"
    jwt:
      secret-key: "replicator-orchestrator-secret-key-32-chars-minimum!"
      expiration-minutes: 480

  replicator:
    global-limit-bytes-per-sec: 104857600 # 100 MB/s
    agent-secret: "replicator-secure-agent-secret-key-12345"
    agent-heartbeat-timeout-seconds: 15
    agent-offline-timeout-seconds: 45
    datacenters:
      - id: "dc1"
        name: "Дата-Центр 1 (Primary DC)"
        network-zone: "zone-a"
      - id: "dc2"
        name: "Дата-Центр 2 (Disaster Recovery)"
        network-zone: "zone-b"
    clusters:
      - id: "dc1"
        name: "HDFS DC1 Production"
        dc-id: "dc1"
        hdfs-rpc-address: "hdfs://namenode-dc1:8020"
      - id: "dc2"
        name: "HDFS DC2 Standby"
        dc-id: "dc2"
        hdfs-rpc-address: "hdfs://namenode-dc2:8020"
```

---

## 3. Сборка и тестирование

### Требования
- JDK 21+
- Apache Maven 3.9+

### Сборка и тесты модуля
```bash
# Тестирование модуля оркестратора
mvn test -f backend/replicator/orchestrator-java/pom.xml

# Сборка исполняемого Spring Boot JAR
mvn clean package -DskipTests -f backend/replicator/orchestrator-java/pom.xml
```

### Использование Makefile
```bash
# Тесты только оркестратора
make test-replicator-orchestrator-java

# Тесты всего Replicator (Agent + Orchestrator)
make test-replicator-java

# Полный прогон всех Java тестов платформы
make test-java
```

---

## 4. REST API Эндпоинты

| Метод | Путь | Описание | Доступ |
|---|---|---|---|
| `GET` | `/health` | Проверка жизнеспособности сервиса | Публичный |
| `GET` | `/actuator/metrics` | Системные метрики Spring Boot Actuator | Публичный |
| `POST` | `/api/v1/auth/login` | Аутентификация пользователя (JWT Cookie) | Публичный |
| `POST` | `/api/v1/agents/register` | Регистрация gRPC агента | `X-Agent-Secret` |
| `POST` | `/api/v1/agents/heartbeat` | Heartbeat агента и метрики загрузки | `X-Agent-Secret` |
| `GET` | `/api/v1/agents` | Список активных агентов репликации | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/tokens/request` | Запрос квоты полосы пропускания (HTB) | `X-Agent-Secret` |
| `GET` | `/api/v1/jobs` | Список задач репликации | JWT (`USER` / `ADMIN`) |
| `POST` | `/api/v1/jobs` | Создание новой задачи репликации | JWT (`ADMIN` / `WRITER`) |
| `GET` | `/api/v1/jobs/{id}` | Детальная информация по задаче | JWT (`USER` / `ADMIN`) |
| `PATCH` | `/api/v1/jobs/{id}` | Обновление статуса и прогресса | JWT (`ADMIN` / `WRITER`) |
| `POST` | `/api/v1/jobs/{id}/cancel` | Отмена выполнения задачи | JWT (`ADMIN` / `WRITER`) |
| `DELETE` | `/api/v1/jobs/{id}` | Удаление задачи | JWT (`ADMIN`) |
| `GET` | `/api/v1/jobs/{id}/runs` | История запусков задачи | JWT (`USER` / `ADMIN`) |
| `GET` | `/api/v1/topology` | Топология дата-центров и кластеров | JWT (`USER` / `ADMIN`) |

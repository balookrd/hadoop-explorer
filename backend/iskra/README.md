# Iskra (Spark) API (Java 21 / Spring Boot 3)

Бэкенд Iskra: интерактивные сессии PySpark, Scala и Spark SQL на Hadoop-кластерах. Java 21 LTS, Spring Boot 3.3.4.

---

## Структура модуля

```
backend/iskra/
├── pom.xml                   # Maven проект (io.github.balookrd.pristan:iskra:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, SparkEngine и SparkEngineRegistry, DAG Engine, DTO
│   │   └── resources/        # application.yml, schema.sql, статика SPA
│   └── test/                 # Интеграционные тесты MockMvc, MockSparkEngine
└── README.md                 # Документация модуля
```

- Безопасность: стартер `zastava` (Java 21) — LDAPS, Kerberos SPNEGO, JWT в cookie, защита от CSRF и проверка прав доступа к очередям YARN.
- Движки исполнения: интерфейс `SparkEngine`, движок кластера выбирает `SparkEngineRegistry`. Кластер с `mock-storage: true` обслуживает `MockSparkEngine` (эмуляция сессий и кода на всех трёх языках), кластер `type: livy` с `mock-storage: false` — `LivySparkEngine` (пакет `service.engine.livy`: REST-клиент Apache Livy, сессии с `proxyUser`, statements, отмена, таймауты). Тесты клиента выполняются против WireMock.
- Жизненный цикл сессий Livy (`SparkSessionManager`): статус и YARN application ID обновляет фоновая синхронизация (`syncSessions`, один `GET /sessions` на кластер раз в `iskra.session-sync-interval-ms`), API сессий читает только БД; остановка, простой дольше `iskra.session-ttl-minutes` и выход пользователя (`UserLoggedOutEvent` стартера, `iskra.close-sessions-on-logout`) удаляют сессию в Livy. Каталог (`SparkCatalogService`) на кластере Livy выполняет `SHOW DATABASES`, `SHOW TABLES IN` и `DESCRIBE` в активной сессии пользователя, без нее отвечает 409. Тесты — `LivyLifecycleIntegrationTest` (WireMock).
- Движок DAG-пайплайнов: бэкенд визуального конструктора ETL. Проверяет граф на циклы (алгоритм Кана), выполняет узлы по шагам и пишет лог каждого узла.
- Рабочее пространство и история: состояние сессий, блокноты и результаты вычислений хранятся в H2 через JPA.
- Встроенный фронтенд: production-бандл собирается из `frontend/apps/iskra` в `src/main/resources/static` (в git не хранится, см. `make build-iskra`) и отдаётся через Spring Boot.

---

## Сборка и запуск

```bash
# Запуск модульных и интеграционных тестов
make test-iskra
# или
mvn clean test -f backend/iskra/pom.xml

# Сборка fat jar
make build-iskra
# или
mvn clean package -DskipTests -f backend/iskra/pom.xml

# Локальный запуск без кластера и каталога: профиль mock (демо-учетки, debug)
java -jar backend/iskra/target/iskra-1.0.0.jar --spring.profiles.active=mock

# Боевой запуск: без JWT_SECRET_KEY и LDAP/Kerberos сервис не стартует
JWT_SECRET_KEY=... java -jar backend/iskra/target/iskra-1.0.0.jar \
  --spring.config.additional-location=file:/etc/pristan/iskra/application.yml
```

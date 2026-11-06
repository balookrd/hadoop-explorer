# Spark Explorer API (Java 21 / Spring Boot 3)

Бэкенд Spark Explorer: интерактивные сессии PySpark, Scala и Spark SQL на Hadoop-кластерах. Java 21 LTS, Spring Boot 3.3.4.

---

## Структура модуля

```
backend/spark/
├── pom.xml                   # Maven проект (org.apache.hadoop.explorer:spark-explorer-java:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, LivyClient, DAG Engine, DTO
│   │   └── resources/        # application.yml, schema.sql, статика SPA
│   └── test/                 # Интеграционные тесты MockMvc, MockSparkEngine
└── README.md                 # Документация модуля
```

- Безопасность: стартер `common-security-starter` (Java 21) — LDAPS, Kerberos SPNEGO, JWT в cookie, защита от CSRF и проверка прав доступа к очередям YARN.
- Движки исполнения: клиент REST API Apache Livy и `MockSparkEngine`, который эмулирует запуск YARN-приложений и сессии на всех трёх языках.
- Движок DAG-пайплайнов: бэкенд визуального конструктора ETL. Проверяет граф на циклы (алгоритм Кана), выполняет узлы по шагам и пишет лог каждого узла.
- Рабочее пространство и история: состояние сессий, блокноты и результаты вычислений хранятся в H2 через JPA.
- Встроенный фронтенд: production-бандл собирается из `frontend/apps/spark` в `src/main/resources/static` (в git не хранится, см. `make build-spark`) и отдаётся через Spring Boot.

---

## Сборка и запуск

```bash
# Запуск модульных и интеграционных тестов
make test-spark
# или
mvn clean test -f backend/spark/pom.xml

# Сборка fat jar
make build-spark
# или
mvn clean package -DskipTests -f backend/spark/pom.xml

# Локальный запуск без кластера и каталога: профиль mock (демо-учетки, debug)
java -jar backend/spark/target/spark-explorer-java-1.0.0.jar --spring.profiles.active=mock

# Боевой запуск: без JWT_SECRET_KEY и LDAP/Kerberos сервис не стартует
JWT_SECRET_KEY=... java -jar backend/spark/target/spark-explorer-java-1.0.0.jar \
  --spring.config.additional-location=file:/etc/hadoop-explorer/spark/application.yml
```

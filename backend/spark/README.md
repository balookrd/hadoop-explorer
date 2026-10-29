# Spark Explorer API (Java 21 / Spring Boot 3)

Корпоративный бэкенд-сервис для интерактивной аналитики, сессий PySpark, Scala и Spark SQL на гетерогенных Hadoop-кластерах, реализованный на **Java 21 LTS** и **Spring Boot 3.3.4**.

---

## Архитектура и структура проекта

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

- **Единое ядро безопасности**: Интеграция со стартером `common-security-starter` (Java 21), обеспечивающим поддержку LDAPS, Kerberos SPNEGO, защищенных JWT Cookie, CSRF-защиту и проверку прав доступа к очередям YARN.
- **Движки исполнения**: Клиент REST API Apache Livy и полнофункциональный MockSparkEngine, эмулирующий запуск YARN-приложений и мультиязычные сессии.
- **Движок DAG-пайплайнов**: Бэкенд для визуального конструктора ETL с алгоритмической проверкой отсутствия циклов (алгоритм Кана), пошаговым выполнением и детальными логами узлов.
- **Воркспейс и история**: Персистентное хранение состояния сессий, блокнотов и результатов вычислений в H2 через JPA.
- **Встроенный SPA-фронтенд**: Продакшн-бандл собирается из `frontend/apps/spark` прямо в `src/main/resources/static` (в git не хранится, см. `make build-spark`) и раздается через Spring Boot.

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

# SQL Explorer API (Java 21 / Spring Boot 3)

Нативный корпоративный бэкенд-сервис для аналитических запросов к кластерам Trino и Apache Hive, реализованный на **Java 21 LTS** и **Spring Boot 3.3.4**.

---

## Архитектура и структура проекта

```
backend/sql/
├── pom.xml                   # Maven проект (org.apache.hadoop.explorer:sql-explorer-java:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, Движки (Trino, Hive), DTO
│   │   └── resources/        # application.yml, schema.sql, статика SPA
│   └── test/                 # Интеграционные тесты MockMvc, MockStorage
└── README.md                 # Документация модуля
```

- **Единое ядро безопасности**: Интеграция со стартером `common-security-starter` (Java 21), обеспечивающим поддержку LDAPS, Kerberos SPNEGO, JWT в защищенных HttpOnly Cookie, защиту от CSRF, Rate Limiting и разграничение доступа по ACL/RBAC.
- **Движки исполнения**: Trino REST клиент, Hive движок и MockSqlEngine с полнофункциональными каталогами данных (`tpch`, `analytics`).
- **API метаданных каталогов**: Многоуровневое кэширование (каталоги, схемы, таблицы, колонки) с поддержкой принудительного обновления.
- **Асинхронное исполнение и SSE**: Потоковая передача жизненного цикла запросов, стриминг строк, поддержка сигналов отмены и сохранение истории.
- **AI-ассистент**: Встроенный интеллектуальный помощник для форматирования, пошагового объяснения планов выполнения, оптимизации запросов, исправления ошибок и генерации Text-to-SQL.
- **Встроенный SPA-фронтенд**: Продакшн-бандл собирается из `frontend/apps/sql` прямо в `src/main/resources/static` (в git не хранится, см. `make build-sql`) и раздается через Spring Boot.

---

## Сборка и запуск

```bash
# Запуск модульных и интеграционных тестов
make test-sql
# или
mvn clean test -f backend/sql/pom.xml

# Сборка fat jar
make build-sql
# или
mvn clean package -DskipTests -f backend/sql/pom.xml

# Локальный запуск без кластера и каталога: профиль mock (демо-учетки, debug)
java -jar backend/sql/target/sql-explorer-java-1.0.0.jar --spring.profiles.active=mock

# Боевой запуск: без JWT_SECRET_KEY и LDAP/Kerberos сервис не стартует
JWT_SECRET_KEY=... java -jar backend/sql/target/sql-explorer-java-1.0.0.jar \
  --spring.config.additional-location=file:/etc/hadoop-explorer/sql/application.yml
```

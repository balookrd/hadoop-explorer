# SQL Explorer API (Java 21 / Spring Boot 3)

Бэкенд-сервис для аналитических запросов к кластерам Trino и Apache Hive на Java 21 LTS и Spring Boot 3.3.4.

---

## Структура модуля

```
backend/sql/
├── pom.xml                   # Maven проект (io.github.balookrd.pristan:sql-explorer-java:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, Движки (Trino, Hive), DTO
│   │   └── resources/        # application.yml, schema.sql, статика SPA
│   └── test/                 # Интеграционные тесты MockMvc, MockStorage
└── README.md                 # Документация модуля
```

## Компоненты

- Безопасность: стартер `common-security-starter` (Java 21) — LDAPS, Kerberos SPNEGO, JWT в HttpOnly cookie, защита от CSRF, rate limiting, разграничение доступа по ACL/RBAC.
- Движки исполнения: `TrinoSqlEngine` (`io.trino:trino-jdbc`), `HiveSqlEngine` (`org.apache.kyuubi:kyuubi-hive-jdbc-shaded`) и `MockSqlEngine` с тестовыми каталогами `tpch` и `analytics` для кластеров с `mock-storage: true`. Тесты `*ContainerTest` требуют Docker и без него пропускаются.
- API метаданных каталогов: кэш каталогов, схем, таблиц и колонок с принудительным обновлением.
- Асинхронное исполнение и SSE: поток событий о состоянии запроса, стриминг строк, отмена, сохранение истории.
- ИИ-ассистент: форматирование, пошаговое объяснение планов выполнения, оптимизация запросов, исправление ошибок, генерация SQL по тексту (Text-to-SQL).
- SPA-фронтенд: промышленный бандл собирается из `frontend/apps/sql` прямо в `src/main/resources/static` (в git не хранится, см. `make build-sql`) и раздается через Spring Boot.

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

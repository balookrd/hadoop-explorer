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
- **Встроенный SPA-фронтенд**: Продакшн-бандл из `frontend/apps/sql/dist` раздается напрямую через Spring Boot.

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

# Запуск сервиса
java -jar backend/sql/target/sql-explorer-java-1.0.0.jar
```

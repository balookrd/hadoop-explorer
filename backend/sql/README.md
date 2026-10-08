# SQL Explorer API (Java 21 / Spring Boot 3)

Native enterprise backend service for analytical queries across Trino and Apache Hive clusters, built with **Java 21 LTS** and **Spring Boot 3.3.4**.

## Architecture & Features

- **Single Security Core**: Built upon `common-security-starter` (Java 21), providing LDAPS, Kerberos SPNEGO, JWT HttpOnly cookies, CSRF protection, Sliding Window Rate Limiting, and RBAC/ACL.
- **Engines**: Trino REST client, Hive engine, and MockSqlEngine with full schema catalogs (`tpch`, `analytics`).
- **Catalog Metadata API**: Multilevel caching (catalogs, schemas, tables, columns) with on-demand refresh.
- **Asynchronous Execution & SSE**: Streaming query lifecycle, row streaming, cancel signal propagation, and persistent query history.
- **AI Assistant**: Built-in intelligent SQL assistant for formatting, explanation, optimization, automated bug fixing, and text-to-SQL generation.
- **Embedded Frontend SPA**: Static production bundle from `frontend/apps/sql/dist` served directly by Spring Boot.

## Build and Run

```bash
# Build & run tests
mvn clean test -f backend/sql/sql-java/pom.xml

# Package fat jar
mvn clean package -DskipTests -f backend/sql/sql-java/pom.xml

# Run application
java -jar backend/sql/sql-java/target/sql-explorer-java-1.0.0.jar
```

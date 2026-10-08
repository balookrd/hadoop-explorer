# Spark Explorer API (Java 21 / Spring Boot 3)

Enterprise backend service for interactive Spark workflows, PySpark, Scala, and Spark SQL execution on heterogeneous Hadoop clusters, built with **Java 21 LTS** and **Spring Boot 3.3.4**.

## Architecture & Features

- **Single Security Core**: Powered by `common-security-starter` (Java 21), supporting LDAPS, Kerberos SPNEGO, HttpOnly JWT cookies, CSRF protection, and YARN queue ACL checks.
- **Engines**: Apache Livy REST client and standalone MockSparkEngine simulating YARN applications and multi-language sessions.
- **DAG Pipeline Engine**: Interactive DAG visual editor backend with Kahn topological cycle validation, execution monitoring, and node-level logs.
- **Workspace & History**: Persistent state synchronization across sessions and statements via JPA and H2 database.
- **Embedded Frontend SPA**: Static production bundle from `frontend/apps/spark/dist` served directly by Spring Boot.

## Build and Run

```bash
# Run tests
mvn clean test -f backend/spark/spark-java/pom.xml

# Package fat jar
mvn clean package -DskipTests -f backend/spark/spark-java/pom.xml

# Run application
java -jar backend/spark/spark-java/target/spark-explorer-java-1.0.0.jar
```

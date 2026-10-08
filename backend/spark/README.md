# Spark Explorer API (Java 21 / Spring Boot 3)

Корпоративный бэкенд-сервис для интерактивной аналитики, сессий PySpark, Scala и Spark SQL на гетерогенных Hadoop-кластерах, реализованный на **Java 21 LTS** и **Spring Boot 3.3.4**.

## Архитектура и возможности

- **Единое ядро безопасности**: Интеграция со стартером `common-security-starter` (Java 21), обеспечивающим поддержку LDAPS, Kerberos SPNEGO, защищенных JWT Cookie, CSRF-защиту и проверку прав доступа к очередям YARN.
- **Движки исполнения**: Клиент REST API Apache Livy и полнофункциональный MockSparkEngine, эмулирующий запуск YARN-приложений и мультиязычные сессии.
- **Движок DAG-пайплайнов**: Бэкенд для визуального конструктора ETL с алгоритмической проверкой отсутствия циклов (алгоритм Кана), пошаговым выполнением и детальными логами узлов.
- **Воркспейс и история**: Персистентное хранение состояния сессий, блокнотов и результатов вычислений в H2 через JPA.
- **Встроенный SPA-фронтенд**: Продакшн-бандл из `frontend/apps/spark/dist` раздается напрямую через Spring Boot.

## Сборка и запуск

```bash
# Запуск модульных и интеграционных тестов
mvn clean test -f backend/spark/spark-java/pom.xml

# Сборка fat jar
mvn clean package -DskipTests -f backend/spark/spark-java/pom.xml

# Запуск сервиса
java -jar backend/spark/spark-java/target/spark-explorer-java-1.0.0.jar
```

# Демо-стенд Spark Explorer (PySpark & Scala Spark on YARN & HDFS)

Демонстрационный стенд сервиса **Spark Explorer** в составе платформы Hadoop Explorer.

## Компоненты стенда
1. **Spark Explorer UI & Backend** (порт `8004`):
   - Интерактивная веб-студия разработки на PySpark, Scala Spark и Spark SQL.
   - Управление сессиями в реальном времени, привязка к YARN Application ID.
   - Модальное окно конфигурации сессий: выбор версий Spark, окружений Python, очередей YARN, Hive Metastore и кастомных зависимостей (JARs, Maven, PyFiles).
2. **Apache Livy Server** (порт `8998`):
   - REST API для управления интерактивными сессиями Spark на YARN/HDFS.
3. **Apache Hadoop HDFS** (порт `9870` / `9000`):
   - Файловая система Data Lake (`hdfs-cluster-1`), сгенерированные таблицы и директории.
4. **Apache Hadoop YARN** (порт `8088`):
   - Resource Manager (`yarn-demo-rm-1`) с очередями `root.analytics`, `root.etl`, `root.adhoc`.
5. **Apache Hive Metastore** (порт `9083`):
   - Каталог метаданных таблиц с БД PostgreSQL.
6. **OpenLDAP & Kerberos KDC**:
   - Единая аутентификация пользователей домена `COMPANY.LOCAL`.

## Быстрый запуск
```bash
./start-demo.sh
```

После старта веб-интерфейс доступен по адресу:
👉 **http://localhost:8004**

Остановка:
```bash
./stop-demo.sh
```

# Демо-стенд Spark Explorer (PySpark & Scala Spark on YARN & HDFS)

Демо-стенд Spark Explorer в составе платформы Hadoop Explorer.

## Компоненты стенда
1. Spark Explorer UI и бэкенд (порт `8004`):
   - Веб-среда разработки на PySpark, Scala Spark и Spark SQL.
   - Компоновка UI: управление Livy-сессией в шапке (`SparkSessionWidget`), верхняя панель вкладок скриптов и ноутбуков, тулбар сессии вкладки (`SessionBar`).
   - Эффективная роль пользователя (`ADM`, `RW`, `RO`) в профиле.
   - Управление сессиями, привязка к YARN Application ID и остановка сессий при выходе пользователя.
   - Модальное окно конфигурации сессий: выбор версий Spark, окружений Python, очередей YARN, Hive Metastore и кастомных зависимостей (JARs, Maven, PyFiles).
2. Apache Livy Server (порт `8998`):
   - REST API для управления интерактивными сессиями Spark на YARN/HDFS.
3. Apache Hadoop HDFS (порты `9870` / `9000`):
   - Файловая система Data Lake (`hdfs-cluster-1`) со сгенерированными таблицами и каталогами.
4. Apache Hadoop YARN (порт `8088`):
   - Resource Manager (`yarn-demo-rm-1`) с очередями `root.analytics`, `root.etl`, `root.adhoc`.
5. Apache Hive Metastore (порт `9083`):
   - Каталог метаданных таблиц с БД PostgreSQL.
6. OpenLDAP и Kerberos KDC:
   - Аутентификация пользователей домена `COMPANY.LOCAL`.

## Запуск
Команды выполняются из каталога `demo/spark`:
```bash
./start-demo.sh
```

После старта веб-интерфейс доступен по адресу http://localhost:8004.

Остановка:
```bash
./stop-demo.sh
```

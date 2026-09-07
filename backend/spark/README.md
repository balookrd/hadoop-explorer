# Spark Explorer Backend

Сервис платформы Hadoop Explorer для интерактивной и пакетной работы с Apache Spark (PySpark и Scala Spark).

## Возможности
- Интерактивные сессии через Apache Livy REST API на YARN.
- Поддержка множества гетерогенных Hadoop-кластеров.
- Выбор версий Spark (2.4, 3.2, 3.5) и окружений Python/PySpark (Conda/Venv в HDFS).
- Подключение множественных Hive Metastore (HMS) и каталогов Iceberg.
- Управление кастомными зависимостями: JARs, Maven координаты, Python `.whl`/`.zip` архивы.
- Разграничение очередей YARN и профилей ресурсов по группам пользователей (ACL).
- MockSparkEngine для автономного запуска и тестирования без внешнего кластера.

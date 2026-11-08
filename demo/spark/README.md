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
   - Имперсонация включена (`livy.impersonation.enabled = true` в `livy/entrypoint.sh`): сессии создаются с `proxyUser` вошедшего пользователя. Аутентификация к Livy на стенде не включена (`livy.auth: "none"` в `application-demo.yml`).
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

## Проверка сессий Livy

После входа в http://localhost:8004 и запуска PySpark-сессии на `spark-cluster-1`:

```bash
curl -s http://localhost:8998/sessions | jq -c '.sessions[] | {id, name, proxyUser, state, appId}'
```

Ожидаемый вывод для `de_user` (идентификатор и суффикс имени другие):

```
{"id":0,"name":"hexp-de_user-be53a08e","proxyUser":"de_user","state":"idle","appId":null}
```

Statement `spark.sql("SELECT current_user()").show()` выводит `de_user`. После выхода из UI сессия пропадает из `GET /sessions` в течение нескольких секунд, запись в Spark Explorer получает статус `killed`. Сессия в `idle` без активности дольше `spark.session-ttl-minutes` (на стенде 120 минут) удаляется так же.

## Известные проблемы

- Hive Metastore недоступен из сессий с `proxyUser`: `SHOW DATABASES`, `spark.read.table(...)` и дерево каталога в UI завершаются ошибкой `Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient`, в логе сессии (`curl -s 'http://localhost:8998/sessions/<id>/log?size=3000'`) — `TTransportException: GSS initiate failed`. Причина: Livy запускает Spark с `livy.spark.master = local[*]`, а в локальном режиме Spark не получает делегационный токен Hive Metastore для `proxyUser`, у пользователя нет своего билета Kerberos. HDFS работает: Hadoop RPC выполняет вход от principal Livy (`hive/hive-server`) и действует от имени пользователя (`UserGroupInformation`: `de_user (auth:PROXY) via hive/hive-server@COMPANY.LOCAL`). Сессия без `proxyUser` (прямой `POST /sessions` в Livy) читает метастор. Примеры `spark.read.table("customers")` из вывода `start-demo.sh` на стенде поэтому не выполняются. На кластере, где Livy запускает Spark в YARN, Spark получает токен метастора для `proxyUser` сам [проверить на кластере с YARN].
- `yarn_application_id` сессии пустой, `appId` в Livy — `null`: Spark работает в контейнере Livy в режиме `local[*]`, приложения YARN нет.
- Группы пользователей LDAP при входе пустые (`"groups": []` в ответе `POST /api/v1/auth/login`), роль — `reader`. Кластеры стенда не ограничены `allowed-users`/`allowed-groups`, поэтому доступны всем вошедшим пользователям.


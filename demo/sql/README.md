# Демонстрационный стенд SQL Web Explorer

Конфигурации и скрипты демонстрационного стенда в Docker Compose: MIT Kerberos KDC, OpenLDAP, два кластера HDFS (`hdfs-cluster-1`, `hdfs-cluster-2`, из `../platform/docker-compose.hdfs.yml`), Apache Hive 4.0.0 (HiveServer2 + Metastore), Trino Coordinator и веб-интерфейс SQL Web Explorer. Внешние сервисы стенду не нужны.

## Запуск и остановка

- Запуск стенда:
  ```bash
  ./start-demo.sh
  ```
  (или `docker compose up -d`)

- Остановка стенда:
  ```bash
  ./stop-demo.sh
  ```
  (или `docker compose down -v`)

После запуска веб-интерфейс доступен по адресу http://localhost:8003.

## Демо-таблицы Hive

Последний шаг `start-demo.sh` запускает `hive/init-demo-data.sh`. Скрипт создает таблицы через `beeline` внутри контейнеров `hive-server-1` и `hive-server-2` под principal `hive/<host>@COMPANY.LOCAL` из `/etc/security/keytabs/hive.keytab`.

| Кластер | HiveServer2 | Таблицы |
|---|---|---|
| Cluster 1 | `hive-server:10000` | `default.customers`, `default.transactions`, `default.orders`, `default.events_log`, `default.daily_metrics`, `demo_db.sales` |
| Cluster 2 | `hive-server-2:10001` | `default.historical_orders`, `archive_db.quarterly_reports` |

Перед DDL скрипт ждет готовности каждого кластера: в `docker logs hdfs-cluster-N` есть строка `HDFS кластер hdfs-cluster-N готов к работе`, порт HiveServer2 принимает соединения, `beeline` открывает сессию. На хостах arm64 образы HDFS и Hive работают в эмуляции amd64, и HiveServer2 открывает порт через несколько минут после `docker compose up`. Ожидание ограничено 900 с на кластер, переменная `HIVE_INIT_TIMEOUT` (секунды) меняет предел.

Существующие таблицы скрипт не пересоздает: в `demo/all` таблицы `customers` и `transactions` в Metastore Cluster 1 может создать `demo/spark/init-demo-tables.sh`. Создаются и заполняются из CSV только отсутствующие таблицы, поэтому повторный запуск безопасен.

Успешный прогон заканчивается строкой `[hive-init] Демо-таблицы Hive созданы в обоих кластерах.`, код выхода 0. Если кластер не готов за отведенное время или DDL завершился ошибкой, скрипт пишет `[hive-init] ОШИБКА: ...` с этапом, на котором остановилось ожидание, и командами для диагностики, и выходит с кодом 1. `start-demo.sh` в этом случае выводит перед адресом веб-интерфейса `Демо-таблицы Hive: НЕ созданы`. Повторный запуск на работающем стенде:

```bash
./hive/init-demo-data.sh
```

Проверка из контейнера (`HOME=/tmp`: у пользователя `hive` нет домашнего каталога; JAAS-файл создает `init-demo-data.sh`):

```bash
docker exec -e HOME=/tmp -e HADOOP_CLIENT_OPTS="-Djava.security.auth.login.config=/tmp/jaas-beeline.conf -Djavax.security.auth.useSubjectCredsOnly=false" hive-server-1 \
  /opt/hive/bin/beeline -u "jdbc:hive2://hive-server:10000/default;principal=hive/hive-server@COMPANY.LOCAL" -e "SELECT * FROM demo_db.sales"
```

Кластер `trino-kerberized` выполняет запросы в контейнере `trino-coordinator` (Trino 484): SQL Explorer входит по keytab `svc_sql_explorer` (`/etc/security/keytabs/sql-explorer.keytab`) через HTTPS-порт `8443` и передает вошедшего пользователя в `sessionUser`. Проверка: под `analyst_user` выполните `SELECT current_user` — результат `analyst_user`. Hive-кластеры выполняют запросы в `hive-server-1` и `hive-server-2` (Hive 4.0.0) через Kerberos с `hive.server2.proxy.user`. `SELECT current_user()` на них возвращает `hive`: в `hive/hive-site.xml` задано `hive.server2.enable.doAs=false`, а аутентификатор по умолчанию отдает служебного пользователя HiveServer2. Пользователь сессии виден в `/tmp/hive/hive.log` контейнера: `docker exec hive-server-1 grep "Login attempt" /tmp/hive/hive.log` — `Login attempt is successful for user : analyst_user`.

ИИ-ассистент обращается к Ollama на хосте (`http://host.docker.internal:11434/v1`, модель `qwen2.5-coder:7b`; загрузка: `ollama pull qwen2.5-coder:7b`), `sql.ai.timeout-seconds: 60`. Текст SQL и описания для генерации уходят в эту модель. Без Ollama ассистент отвечает встроенными правилами: в заголовке окна ассистента — «Mock-заглушка (без LLM)», над результатами — баннер «Автономный Mock-режим: Локальный сервер LLM не обнаружен на http://host.docker.internal:11434/v1 ...».

## Известные проблемы

Кластеры стенда в `application-demo.yml` работают на встроенном эмуляторе (`mock-storage: true`): без доработок ниже реальные Trino и HiveServer2 стенда из SQL Explorer не работают. Реальные движки проверяют тесты `TrinoSqlEngineContainerTest` и `HiveSqlEngineContainerTest` (`mvn -B test -pl backend/sql -am`, нужен Docker). Чтобы направить запросы в контейнеры стенда, поставьте у нужного кластера `mock-storage: false` и примените обходы из списка.

- `GET /api/v1/auth/me` возвращает пользователям стенда `"groups": []`: группы берутся из атрибута `memberOf`, а OpenLDAP стенда его не заполняет. Кластеры с `allowed-groups` пользователю не видны, `POST /api/v1/queries/execute` отвечает `403`. Обход: добавьте пользователя в `allowed-users` кластера в `application-demo.yml`.
- Запрос к `trino-kerberized` падает с `Server not found in Kerberos database (7) - LOOKING_UP_SERVER`: JDK канонизирует `trino-coordinator` через обратный DNS Docker в `trino-coordinator.demo-platform-net`, а в KDC зарегистрирован только `trino/trino-coordinator@COMPANY.LOCAL`. Тот же механизм ломает `hive-archive-analytics`: `hive-server-2` канонизируется в `hive-server-2.demo-platform-net`, билет выдается для `hive/hive-server-2.demo-platform-net@COMPANY.LOCAL`, а HiveServer2 принимает только `hive/hive-server-2@COMPANY.LOCAL` — запрос падает с `GSS initiate failed`, в `/tmp/hive/hive.log` контейнера `hive-server-2` — `Checksum failed`. `hive-server` не канонизируется (обратный DNS дает `hive-server-1.demo-platform-net`, JDK принимает только имя, начинающееся с `hive-server.`). Обход для обоих кластеров (проверен): смонтируйте в `sql-explorer` копию `/etc/security/keytabs/krb5.conf` с `dns_canonicalize_hostname = false` в `[libdefaults]` и укажите ее путь в `JAVA_TOOL_OPTIONS` (`-Djava.security.krb5.conf=...`) и в `kerberos.krb5-conf` кластера `trino-kerberized` (у Hive-кластеров `kerberos.krb5-conf` не задан; если задать, путь должен совпадать с JVM).
- Пока `hive/init-demo-data.sh` не завершился успешно, баз `demo_db` и `archive_db` нет, и запросы к `hive-demo-analytics` и `hive-archive-analytics` падают с `Failed to open new session: Database demo_db does not exist`.
- `INSERT` на стенде падает с `return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask`. Поэтому `hive/init-demo-data.sh` заполняет таблицы через `LOAD DATA LOCAL INPATH`.

---

Сценарии демонстрации, учетные записи и архитектура стенда описаны в [README.md](../README.md).

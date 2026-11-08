# Демо-стенд Spark Explorer (PySpark & Scala Spark on YARN & HDFS)

Демо-стенд Spark Explorer в составе платформы Hadoop Explorer.

## Компоненты стенда
1. Spark Explorer UI и бэкенд (порт `8004`):
   - Веб-среда разработки на PySpark, Scala Spark и Spark SQL.
   - Компоновка UI: управление Livy-сессией в шапке (`SparkSessionWidget`), верхняя панель вкладок скриптов и ноутбуков, тулбар сессии вкладки (`SessionBar`).
   - Эффективная роль пользователя (`ADM`, `RW`, `RO`) в профиле.
   - Управление сессиями, привязка к YARN Application ID и остановка сессий при выходе пользователя.
   - Модальное окно конфигурации сессий: выбор версий Spark, окружений Python, очередей YARN, Hive Metastore и кастомных зависимостей (JARs, Maven, PyFiles).
2. Apache Livy Server: `spark-livy-1` (порт `8998`, кластер 1) и `spark-livy-2` (порт `8999`, кластер 2):
   - REST API для управления интерактивными сессиями Spark. Сессии запускаются в YARN cluster mode, см. раздел «Запуск сессий в YARN».
   - Имперсонация включена (`livy.impersonation.enabled = true` в `livy/entrypoint.sh`): сессии создаются с `proxyUser` вошедшего пользователя. Аутентификация к Livy на стенде не включена (`livy.auth: "none"` в `application-demo.yml`).
3. Apache Hadoop HDFS (порты `9870` / `9000`):
   - Файловая система Data Lake (`hdfs-cluster-1`, `hdfs-cluster-2`) с демо-каталогами.
4. Apache Hadoop YARN:
   - ResourceManager `yarn-rm-1` (порт `8088`) с очередями `root.prod.spark`, `root.dev`, `root.default` и `yarn-rm-2` (порт `8089`) с очередями `root.batch`, `root.interactive`, `root.streaming`.
   - NodeManager `spark-yarn-nm-1` и `spark-yarn-nm-2` (`yarn-nm/`): контейнеры драйверов и executor'ов сессий Livy.
5. Apache Hive Metastore (порт `9083`):
   - Каталог метаданных таблиц: `hive-metastore-1` и `hive-metastore-2` (порт `9084`), БД Derby (`DB_DRIVER: derby` в `demo/platform/docker-compose.hms.yml`).
6. OpenLDAP и Kerberos KDC:
   - Аутентификация пользователей домена `COMPANY.LOCAL`.

## Запуск
Команды выполняются из каталога `demo/spark`:
```bash
./start-demo.sh
```

После старта веб-интерфейс доступен по адресу http://localhost:8004.

`start-demo.sh` в конце запускает `init-demo-tables.sh`: скрипт создает в Hive Metastore таблицы `default.customers` и `default.transactions` (кластер 1) и `default.quarterly_reports` (кластер 2), если их нет. Таблицы создает сессия Livy без `proxyUser`, то есть от principal Livy (`hive`). Перед этим скрипт ждет регистрации `spark-yarn-nm-N` в ResourceManager и строку `HDFS кластер hdfs-cluster-N готов к работе` в логе `hdfs-cluster-N`: до нее каталоги `/tmp` и `/tmp/hive` в HDFS еще не получили права `1777`, и сессия падает с `Permission denied: user=hive, access=WRITE, inode="/tmp/hive"`. Вместе с ожиданием HDFS на это уходит 2–4 минуты. Повторный запуск скрипта существующие таблицы не меняет.

Остановка:
```bash
./stop-demo.sh
```

## Запуск сессий в YARN

Livy запускает каждую сессию через `spark-submit --master yarn --deploy-mode cluster --proxy-user <пользователь>`. Драйвер работает в контейнере NodeManager, а не на хосте Livy.

| Параметр | Значение на стенде | Где задан | Что делает |
|---|---|---|---|
| `livy.spark.master` | `yarn` | `SPARK_MASTER` в `docker-compose.yaml` → `livy/entrypoint.sh` | Отправка сессий в YARN |
| `livy.spark.deploy-mode` | `cluster` | `SPARK_DEPLOY_MODE` | Драйвер в контейнере YARN; Spark при отправке получает делегационные токены HDFS и Hive Metastore для `proxyUser` |
| `livy.impersonation.enabled` | `true` | `livy/entrypoint.sh` | `proxyUser` из запроса передается в `spark-submit --proxy-user` |
| `yarn.resourcemanager.hostname` / `yarn.resourcemanager.principal` | `yarn-rm-1.yarn-demo-net` / `yarn/yarn-rm-1.yarn-demo-net@COMPANY.LOCAL` (кластер 2 — `yarn-rm-2`) | `YARN_RM_HOST`, `YARN_RM_PRINCIPAL` | Адрес RM и его principal: renewer токена HDFS |
| `spark.yarn.jars` | `local:/opt/spark/jars/*` | `livy/entrypoint.sh` | jar'ы Spark берутся из образа NodeManager, а не загружаются в HDFS на каждую сессию |
| `yarn.nodemanager.container-executor.class` | `LinuxContainerExecutor` | `yarn-nm/entrypoint.sh` | Процессы контейнера работают под ОС-учеткой владельца приложения |

Что для этого настроено вне `demo/spark`:

- `demo/yarn/cluster-1/core-site.xml`, `demo/yarn/cluster-2/core-site.xml`: `hadoop.proxyuser.hive.*`. Livy входит в KDC как `hive/hive-server` (`hive/hive-server-2.demo-platform-net` для кластера 2), и RM без этого отклоняет отправку от имени пользователя.
- `demo/yarn/cluster-1/hdfs-site.xml`, `demo/yarn/cluster-2/hdfs-site.xml` (подключены в `demo/platform/docker-compose.yarn.yml`): principal NameNode. RM продлевает токен HDFS приложения при отправке и без principal отклоняет приложение с `Failed to specify server's Kerberos principal name`.
- `hadoop.proxyuser.hive.*` в HDFS (`demo/hdfs/hadoop/entrypoint.sh`) и в Hive Metastore (`demo/sql/hive/core-site*.xml`) уже были.

Принципалы и keytab'ы `demo/infra` не менялись. NodeManager входит под principal RM своего кластера (`yarn/yarn-rm-N.yarn-demo-net` из `yarn-cluster-N.keytab`).

### NodeManager и изоляция пользователей

- Образ `yarn-nm/Dockerfile`: `eclipse-temurin:11-jdk-jammy`, Hadoop 3.4.0 (jar'ы из `apache/hadoop:3.4.0`), Spark 3.5.1 и `python3` как в образе Livy. `container-executor` собирается из исходников Hadoop 3.4.0 при сборке образа (`archive.apache.org`, проверка SHA-512): бинарник из `apache/hadoop:3.4.0` есть только для amd64.
- `LinuxContainerExecutor` запускает контейнер от ОС-учетки с именем пользователя Hadoop. Учетки `de_user`, `admin_user`, `analyst_user`, `writer_user`, `reader_user`, `engineer`, `analyst`, `admin` и `hive` (uid 2001–2009) созданы в образе. Сессия пользователя, которого нет в этом списке, переходит в `dead`: `container-executor` отказывает с `User <пользователь> not found` (видно в `curl -s 'http://localhost:8998/sessions/<id>/log?size=400'`). Новую учетку добавьте в цикл `useradd` в `yarn-nm/Dockerfile` и пересоберите образ: `docker compose -f docker-compose.yaml up -d --build spark-yarn-nm-1 spark-yarn-nm-2`.
- Том `demo-shared-keytabs` смонтирован в `/root/shared-keytabs` (каталог `/root` с правами `700`). NodeManager копирует свой keytab в `/etc/hadoop-nm/nm.keytab` (владелец `yarn`, права `400`). Код пользователя не читает ни общий том, ни keytab NodeManager.
- NodeManager объявляет 16384 МБ и 16 vCores (`NM_MEMORY_MB`, `NM_VCORES`) при меньшей памяти контейнера; проверки pmem/vmem выключены. Причина: `yarn.scheduler.minimum-allocation-mb` = 1024 округляет драйвер и executor профиля `small` (1 ГБ + 384 МБ overhead) до 2048 МБ каждый, а `capacity-scheduler.xml` выдает пользователю долю кластера. Профиль `medium` (драйвер 2048 МБ и 2 executor'а по 3072 МБ, всего 8192 МБ) больше расчетного лимита пользователя в `root.prod.spark` (16384 × 0,6 × 0,4 × `user-limit-factor` 2 ≈ 7864 МБ), но на свободном кластере Capacity Scheduler выделяет все три контейнера: `yarn node -status spark-yarn-nm-1:45454` показывает `Containers : 3`, `Memory-Used : 8192MB`, `yarn queue -status spark` — `Current Capacity : 208.34%`. Когда в очереди работают другие пользователи, второй executor может ждать освобождения ресурсов.
- `local-dirs` и `log-dirs` NodeManager лежат на диске VM Docker вместе с образами и build cache. При заполнении выше 90% (порог по умолчанию) NodeManager переходит в `UNHEALTHY`, YARN не выделяет контейнеры, приложения висят в `ACCEPTED`, и Livy убивает сессию (`was killed by user hive`). На стенде порог поднят до 98% и задан минимум свободного места 1024 МБ (`NM_MAX_DISK_UTILIZATION_PERCENT`, `NM_MIN_FREE_SPACE_MB`). Проверка: `yarn node -status spark-yarn-nm-1:45454` в контейнере `yarn-rm-1` (после `kinit`, как в разделе «Проверка сессий») — строка `Health-Report`; заполнение диска — `docker exec spark-yarn-nm-1 df -h /`.
- Агрегация логов выключена. Логи контейнеров — в `/var/log/hadoop-yarn/containers/<application_id>/` контейнера `spark-yarn-nm-N`, удаляются через 600 секунд после завершения приложения (`yarn.nodemanager.delete.debug-delay-sec`).

## Проверка сессий

Сессия переходит в `idle` через 30–90 секунд после создания (замер на arm64): `spark-submit`, загрузка jar'ов Livy в staging, запуск контейнера драйвера. Верхняя граница — пока HDFS и ResourceManager заняты после старта стенда. ResourceManager работает в образе `apache/hadoop:3.4.0` только для amd64 и на хостах arm64 выполняется под эмуляцией.

После входа в http://localhost:8004 под `de_user` и запуска PySpark-сессии на `spark-cluster-1`:

```bash
curl -s http://localhost:8998/sessions | jq -c '.sessions[] | {id, name, proxyUser, state, appId}'
```

Ожидаемый вывод (идентификаторы и суффикс имени другие):

```
{"id":3,"name":"hexp-de_user-49f19eed","proxyUser":"de_user","state":"idle","appId":"application_1794119291865_0005"}
```

Владелец приложения в YARN:

```bash
docker exec yarn-rm-1 bash -c 'kinit -kt /etc/security/keytabs/yarn-cluster-1.keytab yarn/yarn-rm-1.yarn-demo-net@COMPANY.LOCAL && yarn application -status <appId>' 2>/dev/null | grep -E 'User :|Queue :|AM Host'
```

```
	User : de_user
	Queue : root.prod.spark
	AM Host : spark-yarn-nm-1
```

Проверки в сессии:

| Код | Ожидаемый результат |
|---|---|
| `spark.sql("SELECT current_user()").show()` | `de_user` |
| `spark.read.table("customers").show()` | 5 строк таблицы `customers` |
| `import subprocess; print(subprocess.check_output(["id"]).decode())` | `uid=2001(de_user) gid=2001(de_user) groups=2001(de_user)` |
| `open("/root/shared-keytabs/hive.keytab", "rb")` | `PermissionError` |

Дерево каталога в UI показывает базу `default` и ее таблицы. Те же данные отдает API: `GET /api/v1/catalog/spark-cluster-1/tables?metastore_id=hive-metastore-1&database=default` → `["customers","transactions"]`.

После выхода из UI сессия пропадает из `GET /sessions` в течение нескольких секунд, приложение YARN переходит в `FINISHED`, запись в Spark Explorer получает статус `killed`. Сессия в `idle` без активности дольше `spark.session-ttl-minutes` (на стенде 120 минут) удаляется так же.

Если сессия перешла в `dead`, причина — в выводе `spark-submit`: `curl -s 'http://localhost:8998/sessions/<id>/log?size=200' | jq -r '.log[]'`. Ошибки драйвера — в `stderr` контейнера: `docker exec spark-yarn-nm-1 find /var/log/hadoop-yarn/containers -name stderr`.

## Известные проблемы

- Первый `start-demo.sh` после `stop-demo.sh` занимает 5–8 минут (замеры на arm64: 5 мин 21 с, 6 мин 10 с, 7 мин 50 с). Большая часть уходит на инициализацию `hdfs-cluster-1` и `hdfs-cluster-2`: образ `apache/hadoop:3.3.6` только для amd64, каждая команда `hdfs dfs` в `demo/hdfs/hadoop/entrypoint.sh` запускает JVM под эмуляцией. До появления строки `HDFS кластер hdfs-cluster-N готов к работе` в `docker logs hdfs-cluster-N` каталоги `/tmp`, `/tmp/hive` и `/user` еще не получили права `1777`/`777`, и сессия, созданная из UI в это время, падает с `Permission denied` на `/tmp/hive` или `/user`.
- ResourceManager (`apache/hadoop:3.4.0`, amd64 под эмуляцией `qemu-x86_64` на arm64) изредка зависает при старте: в `docker logs yarn-rm-N` после `Starting YARN ResourceManager` нет ни одной строки, процессы java в контейнере не потребляют CPU, порт `8032` отказывает в соединении. `init-demo-tables.sh` ждет регистрации `spark-yarn-nm-N` в RM 300 секунд и затем выводит подсказку. Восстановление: `docker restart yarn-rm-N`, затем повторный запуск `./init-demo-tables.sh`. Livy ждет появления приложения в YARN 600 секунд (`livy.server.yarn.app-lookup-timeout`, по умолчанию `120s`), поэтому сессии, созданные во время медленного старта RM, не удаляются.
- Группы пользователей LDAP при входе пустые (`"groups": []` в ответе `POST /api/v1/auth/login`), роль — `reader`. Кластеры стенда не ограничены `allowed-users`/`allowed-groups`, поэтому доступны всем вошедшим пользователям.


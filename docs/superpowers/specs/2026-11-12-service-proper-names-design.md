# Переименование сервисов в имена собственные

Спецификация описывает замену шаблонных имён сервисов платформы Hadoop Explorer
(`<движок>-explorer`, `replicator`) на имена собственные во всех идентификаторах
репозитория. Документ предназначен для исполнителя переименования и для ревью.

Статус: проект, ожидает проверки. Дата: 2026-11-12.

## Принятые решения

| Вопрос | Решение |
|---|---|
| Что переименовывать | Всё: каталоги, артефакты сборки, Java-пакеты и классы, конфигурацию, внешние контракты, тексты |
| Совместимость со старыми именами | Нет. Старые имена удаляются без переходного кода; стенды пересоздаются с нуля |
| Имя платформы | Hadoop Explorer → Pristan («Пристань»), полная замена; см. раздел «Платформа» |
| groupId и корневой Java-пакет | `io.github.balookrd.pristan` |
| `common-security-starter` | Переименовывается в Zastava; см. раздел «Стартер безопасности» |

## Имена

| Было | Стало | Идентификатор | Смысл |
|---|---|---|---|
| YARN Explorer | Artel | `artel` | артель: делит работу и ресурсы между участниками |
| HDFS Explorer | Sklad | `sklad` | склад |
| SQL Explorer | Tolmach | `tolmach` | толмач: переводит вопрос в запрос |
| Spark Explorer | Iskra | `iskra` | искра |
| Replicator | Parom | `parom` | паром между дата-центрами |
| common-security-starter | Zastava | `zastava` | застава: пост на границе, проверяет каждого входящего |

Идентификатор пишется в нижнем регистре в путях, артефактах и ключах
(`artel`), в верхнем — в переменных окружения (`ARTEL_`), с заглавной — в
именах классов и в текстах (`Artel`).

В документации и UI при первом упоминании на странице пишется имя с движком в
скобках: «Artel (YARN)». Дальше — только «Artel».

## Платформа: Hadoop Explorer → Pristan

Имя платформы встречается в 850 отслеживаемых файлах (без `docs/superpowers`
и lock-файлов); основная часть — объявления `package` и `import` в Java.

| Было | Стало |
|---|---|
| отображаемое имя «Hadoop Explorer», «Hadoop Explorer Platform» | «Pristan»; при первом упоминании — «Pristan — платформа для работы с Hadoop» |
| artifactId `hadoop-explorer-parent` | `pristan-parent` |
| groupId `org.apache.hadoop.explorer` | `io.github.balookrd.pristan` |
| корневой Java-пакет `org.apache.hadoop.explorer`, каталоги `org/apache/hadoop/explorer` | `io.github.balookrd.pristan`, `io/github/balookrd/pristan` |
| образы `hadoop-explorer/<сервис>`, значение `REGISTRY` по умолчанию | `pristan/<сервис>`, `pristan` |
| образ `hadoop-explorer/hive-metastore-pg` | `pristan/hive-metastore-pg` |
| npm-область `@hadoop-explorer`, корневой пакет `hadoop-explorer-monorepo` | `@pristan`, `pristan-monorepo` |
| cookie `hadoop_explorer_session` | `pristan_session` |
| путь данных `/var/lib/hadoop-explorer` | `/var/lib/pristan` |
| учётка `svc_hadoop_explorer` | `svc_pristan` |
| `job_name: 'hadoop-explorer'` в Prometheus | `'pristan'` |
| дашборд `hadoop_explorer_overview` (файл и uid), группа алертов `hadoop_explorer_alerts` | `pristan_overview`, `pristan_alerts` |
| артефакты CI `hadoop-explorer-jars`, `hadoop-explorer-<tag>` | `pristan-jars`, `pristan-<tag>` |
| база и пользователь Postgres `explorer` в `ci.yml` | `pristan` |

Текущий корневой пакет `org.apache.hadoop.explorer` лежит в пространстве имён
проекта Apache Hadoop, к которому репозиторий не относится. Смена пакета
убирает это пересечение.

groupId `io.github.balookrd.pristan` привязан к учётной записи GitHub
`balookrd`, которой принадлежит репозиторий; для публикации в Maven Central
владение отдельным доменом не требуется.

Платформа переименовывается первым шагом, до сервисов: иначе каждый файл
Java правится дважды — сначала сегмент сервиса, затем корень пакета.

### Каталог проекта и репозиторий на GitHub

Репозиторий `balookrd/hadoop-explorer` переименовывается в `balookrd/pristan`,
каталог проекта `hadoop-explorer` — в `pristan`. Это последний шаг работ: он
выполняется после того, как переименование кода закоммичено и отправлено, и
только по отдельной команде владельца репозитория.

1. Переименуйте репозиторий на GitHub из каталога проекта:

   ```bash
   gh repo rename pristan --repo balookrd/hadoop-explorer
   ```

   GitHub перенаправляет старые URL (`git`, веб, API) на новые, пока имя
   `balookrd/hadoop-explorer` не занято другим репозиторием.

2. Обновите адрес `origin` в локальной копии:

   ```bash
   git remote set-url origin git@github.com:balookrd/pristan.git
   ```

   Проверка: `git remote -v` показывает новый адрес, `git fetch` проходит.

3. Закройте IDE и сессии Claude Code, работающие в каталоге, затем
   переименуйте каталог:

   ```bash
   mv ~/IdeaProjects/hadoop-explorer ~/IdeaProjects/pristan
   ```

До шага 1 в рабочем дереве заменяются ссылки на старый адрес репозитория. На
2026-11-12 они есть в пяти файлах `docs/*-admin-guide.md` (17 вхождений);
актуальный список даёт поиск:

```bash
git grep -n -E 'balookrd/hadoop-explorer|github\.com[:/][^ /]+/hadoop-explorer' -- . ':!docs/superpowers'
```

Что перестаёт работать после переименования каталога:

| Что | Признак | Действие |
|---|---|---|
| Память и история сессий Claude Code | каталог `~/.claude/projects/-Users-<user>-IdeaProjects-hadoop-explorer/` привязан к старому пути; новая сессия в `~/IdeaProjects/pristan` стартует без сохранённой памяти | перенести содержимое подкаталога `memory/` в каталог проекта для нового пути |
| Проект IDE | IntelliJ IDEA открывает старый путь из списка недавних и сообщает, что каталог не найден | открыть проект из нового каталога |
| Абсолютные пути в локальных файлах вне репозитория | hook `.git/hooks/pre-push`, локальные скрипты и алиасы оболочки ссылаются на старый путь | найти поиском `IdeaProjects/hadoop-explorer` и заменить |

## Стартер безопасности: common-security-starter → Zastava

| Было | Стало |
|---|---|
| каталог `backend/common-security-starter` | `backend/zastava` |
| artifactId `common-security-starter` | `zastava` |
| пакет `org.apache.hadoop.explorer.common` | `io.github.balookrd.pristan.zastava` |
| префикс конфигурации `hadoop.security.*` | `zastava.*` |
| `CommonSecurityAutoConfiguration`, `CommonSecurityProperties`, `CommonSecurityErrorHandlers` | `ZastavaAutoConfiguration`, `ZastavaProperties`, `ZastavaErrorHandlers` |
| цели Makefile `test-security-starter`, `build-security-starter` | `test-zastava`, `build-zastava` |

Вместе с пакетом меняется файл
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:
в нём записано полное имя класса автоконфигурации. При расхождении стартер
подключается без ошибок сборки, но бины безопасности не создаются, и сервис
останавливается на старте с `NoSuchBeanDefinitionException`.

Префикс `hadoop.security.*` совпадает с ключами самого Hadoop
(`hadoop.security.authentication`, `hadoop.security.auth_to_local`), которые
встречаются в `core-site.xml` стендов и не меняются. Замена префикса
выполняется только в `application*.yml` сервисов, в тестовых свойствах и в
аннотациях `@ConfigurationProperties`, `@Value`, `@ConditionalOnProperty`.

Остальные классы стартера (`JwtTokenService`, `LdapAuthService`,
`KerberosSpnegoService`, `CsrfProtectionValidator` и другие) имён не меняют.

Zastava переименовывается вместе с платформой, первым шагом: от неё зависят
все пять сервисов.

## Описание проекта

Новые имена и их смысл записываются в описание проекта в рамках тех же шагов,
что и переименование кода, чтобы текст не расходился с каталогами и
артефактами.

| Где | Что меняется |
|---|---|
| `README.md`, шапка | имя «Pristan», строка «Pristan — платформа для работы с Hadoop», `alt` логотипа |
| `README.md`, раздел «Состав платформы» | таблица: имя, идентификатор, движок, назначение, смысл имени — для Artel, Sklad, Tolmach, Iskra, Parom и Zastava |
| `README.md`, разделы «Структура монорепозитория», «Общие модули», «Адреса, health-check и образы сервисов», «Метрики Prometheus и дашборды Grafana», «Запуск демо-стендов» | пути, имена образов, метрик, целей `make` и заголовки стендов |
| `AGENTS.md` | карта репозитория, правило изоляции сервисов, команды сборки и тестов |
| `docs/ARCHITECTURE.md` и остальные файлы `docs/` | имена сервисов и платформы; при первом упоминании на странице — «Artel (YARN)» |
| `README.md` модулей: `backend/zastava`, `backend/sklad`, `backend/parom`, `demo/`, `ansible/` | то же |
| `<name>` и `<description>` во всех `pom.xml`, `description` в `package.json` | имя платформы и сервиса |
| `<title>` и видимые заголовки в `frontend/apps/*` | имя сервиса с движком: «Artel (YARN)» |
| `scripts/generate-presentation-*.py` | имена в тексте презентации и имена метрик |

Файлы логотипа в `images/` проверяются на наличие надписи со старым именем
платформы; замена изображения в эту работу не входит и отмечается отдельно.

Описание и темы репозитория на GitHub — внешнее действие; выполняется только
по отдельной команде.

## Граница: что считается именем продукта

Слова `yarn`, `hdfs`, `sql`, `spark` в репозитории обозначают и продукт, и
движок Hadoop. Переименовывается только продукт.

| Переименовывается (продукт) | Остаётся (движок или предметная область) |
|---|---|
| каталоги `backend/yarn`, `frontend/apps/yarn`, `demo/yarn` | `yarn-site.xml`, `yarn.scheduler.capacity.*`, `spark.yarn.*`, `YARN_RM_HOST` |
| префикс конфигурации `yarn.*` сервиса (`YarnProperties`) | ключи внутри него, называющие движок: `webhdfs-urls`, `hdfs-rpc-urls`, `hms-*`, `kafka-*` |
| класс `YarnExplorerApplication` | классы `YarnProperties`, `HdfsFileService`, `SparkMetrics`, `SqlQueryMetrics` и остальные, где слово называет движок |
| классы `Replicator*` (10 в main, 7 в тестах) | классы и таблицы `Replication*` / `replication_*`: это предметное слово «репликация» |
| контейнеры сервисов: `yarn-explorer`, `replicator-orchestrator` | контейнеры движков на стендах: `yarn-rm-1`, `spark-livy-1`, `sql-trino-coordinator`, `hdfs-cluster-*` |
| таблицы `replicator_dr_snapshots`, `replicator_streaming_leases` | таблицы `spark_sessions`, `spark_execution_history`, `spark_user_workspaces`, `sql_user_workspaces` |
| заголовки и ключи с именем продукта | заголовки Kafka `X-Repl-Origin`, `X-Repl-Hop` |

Следствие: замена по подстроке запрещена. Каждая замена выполняется по списку
точных токенов из таблиц ниже.

## Таблицы замены

Таблицы составлены по инвентарю от 2026-11-12. Инвентарь — отправная точка;
критерий готовности задаёт раздел «Проверка».

### Каталоги и сборка

| Было | Стало |
|---|---|
| `backend/{yarn,hdfs,sql,spark,replicator}` | `backend/{artel,sklad,tolmach,iskra,parom}` |
| `frontend/apps/{yarn,hdfs,sql,spark,replicator}` | `frontend/apps/{artel,sklad,tolmach,iskra,parom}` |
| `demo/{yarn,hdfs,sql,spark,replicator}` | `demo/{artel,sklad,tolmach,iskra,parom}` |
| `docs/images/<сервис>` | `docs/images/<новое имя>` |
| `docs/{yarn,hdfs,sql,spark,replicator}-admin-guide.md` | `docs/{artel,sklad,tolmach,iskra,parom}-admin-guide.md` |
| artifactId `yarn-explorer-java`, `hdfs-explorer-java`, `sql-explorer-java`, `spark-explorer-java` | `artel`, `sklad`, `tolmach`, `iskra` |
| artifactId `replicator-parent`, `replicator-proto`, `replicator-model`, `replicator-agent`, `replicator-orchestrator` | `parom-parent`, `parom-proto`, `parom-model`, `parom-agent`, `parom-orchestrator` |
| `docker/Dockerfile.{yarn,hdfs,sql,spark}-java` | `docker/Dockerfile.{artel,sklad,tolmach,iskra}` |
| `docker/Dockerfile.replicator-{agent,orchestrator}` | `docker/Dockerfile.parom-{agent,orchestrator}` |
| образы `hadoop-explorer/{yarn,hdfs,sql,spark}` | `pristan/{artel,sklad,tolmach,iskra}` |
| образы `hadoop-explorer/replicator-{agent,orchestrator}`, `hadoop-replicator-kafka-agent` | `pristan/parom-{agent,orchestrator}`, `pristan/parom-kafka-agent` |
| JAR в образе `/app/<сервис>-explorer.jar`, `/app/replicator-*.jar` | `/app/<новое имя>.jar`, `/app/parom-*.jar` |
| npm-пакеты `yarn-explorer-frontend`, `hdfs-explorer-frontend`, `frontend` (sql), `@hadoop-explorer/spark`, `replicator-explorer-frontend` | `@pristan/{artel,sklad,tolmach,iskra,parom}` |
| скрипты `build:<сервис>`, `dev:<сервис>` в `frontend/package.json` | `build:<новое имя>`, `dev:<новое имя>` |
| e2e `frontend/e2e/{yarn,hdfs,sql,spark}.spec.ts`, `capture_*` | по новым именам |
| цели Makefile `test-*`, `build-*`, `demo-*` и алиасы `*-java` | по новым именам: `test-artel`, `build-parom-agent`, `demo-sklad` |
| `backend/replicator/agent/bin/replicator-agent.sh` | `backend/parom/agent/bin/parom-agent.sh` |

Вместе с каталогами правятся пути в корневом `pom.xml`, `scripts/run-tests.sh`,
`scripts/demo.sh`, `scripts/build-containers.sh`, `scripts/check-russian-*.py`,
`.github/workflows/ci.yml`, `.github/workflows/_release-build.yml`,
`frontend/apps/*/vite.config.ts` (`outDir`), `frontend/playwright.config.ts` и
`.ast-grep/rules/no-synchronized-on-io-path.yml`. В последнем четыре абсолютных
пути; после переименования каталогов правило без правки перестаёт срабатывать и
не сообщает об этом.

### Java

| Было | Стало |
|---|---|
| пакеты `org.apache.hadoop.explorer.{yarn,hdfs,sql,spark,replicator}` | `io.github.balookrd.pristan.{artel,sklad,tolmach,iskra,parom}` |
| `YarnExplorerApplication`, `HdfsExplorerApplication`, `SqlExplorerApplication`, `SparkExplorerApplication` | `ArtelApplication`, `SkladApplication`, `TolmachApplication`, `IskraApplication` |
| `ReplicatorAgent`, `ReplicatorAgentMain`, `ReplicatorAgentConfig`, `ReplicatorApplicationMaster`, `ReplicatorYarnClient`, `ReplicatorOrchestratorApplication`, `ReplicatorProperties`, `ReplicatorMetricsService`, `ReplicatorMetricsController` | то же с префиксом `Parom` |
| `KafkaReplicatorAgentMain` | `ParomKafkaAgentMain` |

Полные имена классов встречаются строками вне Java и меняются вместе с
пакетами: `mainClass` в `pom.xml`, demo-скрипты (`HmsTool`, `AgentSwarm`),
compose-файлы, `Dockerfile.parom-agent` (`GrpcHealthProbeMain`),
`bin/submit-yarn.sh`, `logback.xml`, `application-test.yml`
(`statement_inspector`), `SchemaMigrationIntegrityTest`,
`@RestControllerAdvice(basePackages = …)`.

### Конфигурация

| Было | Стало |
|---|---|
| `spring.application.name`: `yarn-explorer-java`, `hadoop-hdfs-explorer`, `hadoop-sql-explorer`, `hadoop-spark-explorer`, `hadoop-grpc-replicator-orchestrator` | `artel`, `sklad`, `tolmach`, `iskra`, `parom-orchestrator` |
| поле `service` в ответе `/health` | те же новые значения |
| префиксы `yarn.*`, `hadoop.hdfs.*`, `sql.*`, `spark.*` | `artel.*`, `sklad.*`, `tolmach.*`, `iskra.*` |
| префиксы `hadoop.replicator.*`, `replicator.hms.*` | `parom.*`, `parom.hms.*` |
| системное свойство `-Dreplicator.schemaregistry.block-loopback` | `-Dparom.schemaregistry.block-loopback` |
| `management.metrics.tags.app`: `yarn`, `hdfs`, `sql`, `spark` | `artel`, `sklad`, `tolmach`, `iskra` |
| переменные `YARN_DB_*`, `YARN_KERBEROS_*`, `YARN_AWX_*` | `ARTEL_DB_*`, `ARTEL_KERBEROS_*`, `ARTEL_AWX_*` |
| переменные `REPLICATOR_*`, `HADOOP_REPLICATOR_*` | `PAROM_*` |
| `APP_NAME` в Dockerfile | новое имя сервиса |

Префикс `iskra.*` снимает совпадение ключей сервиса с ключами Spark
(`spark.master`, `spark.yarn.*`), которые передаются в Livy как литералы и не
меняются. Переменные движков (`HDFS_DEFAULT_FS`, `YARN_RM_HOST`, `SPARK_HOME`,
`PYSPARK_*`, `KAFKA_DC*_BOOTSTRAP`) не меняются.

### gRPC и protobuf

| Было | Стало |
|---|---|
| файлы `replicator.proto`, `kafka_replicator.proto` | `parom.proto`, `parom_kafka.proto` |
| `package replicator`, `replicator.kafka`, `replicator.control` | `parom`, `parom.kafka`, `parom.control` |
| `java_package org.apache.hadoop.explorer.replicator.*` | `io.github.balookrd.pristan.parom.*` |
| `ReplicatorProto`, `KafkaReplicatorProto` | `ParomProto`, `ParomKafkaProto` |

Имена сервисов (`DataTransferService`, `HmsTransferService`,
`KafkaDataTransferService`, `OrchestratorControlPlaneService`) и файл
`orchestrator_control_plane.proto` не меняются. Строки с полным именем сервиса
меняются: `"replicator.DataTransferService"` в health-регистрации, в
`Dockerfile.parom-agent` и в тестах, ручные дескрипторы
`"replicator.control.…/ClaimTasks"` в тестах.

Смена `package` меняет wire-имена методов. Агент и оркестратор разных версий
не соединяются; обновляются одновременно.

### Данные и состояние

| Было | Стало |
|---|---|
| H2-файлы `yarn_explorer`, `hdfs_explorer`, `sql_explorer`, `spark_explorer`, `replicator` | `artel`, `sklad`, `tolmach`, `iskra`, `parom` |
| H2 in-memory `sqlexplorer`, `sparkexplorer`, `hdfs`, `replicator` | `tolmach`, `iskra`, `sklad`, `parom` |
| база и пользователь Postgres `replicator`, хост `replicator-db` | `parom`, `parom-db` |
| таблицы `replicator_dr_snapshots`, `replicator_streaming_leases` | `parom_dr_snapshots`, `parom_streaming_leases` |
| переменная `REPLICATOR_PG_TEST_URL` | `PAROM_PG_TEST_URL` |
| cookie `hdfs_explorer_session`, `sql_explorer_session`, `spark_explorer_session` | `sklad_session`, `tolmach_session`, `iskra_session` |
| localStorage `spark_explorer_state_v2`, `spark_explorer_state_<user>`, `replicator_active_tab` | `iskra_state_v2`, `iskra_state_<user>`, `parom_active_tab` |
| HDFS-каталог `.replicator-staging/` | `.parom-staging/` |
| `TOKEN_SIGNATURE = "replicator-hms-token"` | `"parom-hms-token"` |
| имя YARN-приложения `Hadoop-Replicator-Agent-<cluster>` | `Parom-Agent-<cluster>` |
| consumer group `kafka-replicator-<dc>-<job>` | `parom-kafka-<dc>-<job>` |

Таблицы переименовываются новой миграцией Flyway (`ALTER TABLE … RENAME TO`)
в наборах `common`, `h2` и `postgresql`. Файл `V1__baseline_schema.sql` не
правится: изменение применённой миграции меняет её checksum, и Flyway
останавливает старт на validate.

Регулярное выражение в `ConsumerGroupBlacklistFilter` получает ветку
`parom-.*`. Ветки `kafka-replicator-.*`, `replicator-.*`,
`hadoop-replicator-.*` остаются: они исключают служебные группы старых версий
из синхронизации offsets на кластерах, где такие группы уже созданы.

### Kerberos, LDAP, API

| Было | Стало |
|---|---|
| учётки `svc_yarn_explorer`, `svc_hdfs_explorer`, `svc_sql_explorer`, `svc_spark_explorer` | `svc_artel`, `svc_sklad`, `svc_tolmach`, `svc_iskra` |
| principal `yarn-explorer@R`, `hdfs-explorer@R`, `HTTP/<сервис>-explorer@R` | `artel@R`, `sklad@R`, `HTTP/<новое имя>@R` |
| principal `kafka-replicator@R`, значение по умолчанию `hdfs-replicator@REALM.LOCAL` | `parom-kafka@R`, `parom@REALM.LOCAL` |
| keytab `<сервис>-explorer.keytab`, `kafka-replicator.keytab`, `replicator-dc1.keytab`, `/etc/replicator.keytab` | `<новое имя>.keytab`, `parom-kafka.keytab`, `parom-dc1.keytab`, `/etc/parom.keytab` |
| `hadoop.proxyuser.hdfs-explorer.*`, `hadoop.proxyuser.yarn-explorer.*`, `hadoop.proxyuser.svc_sql_explorer.*` | `hadoop.proxyuser.sklad.*`, `hadoop.proxyuser.artel.*`, `hadoop.proxyuser.svc_tolmach.*` |
| LDAP-группы `yarn-operators`, `spark-users`, `replicator-operators`, `replicator-agents` | `artel-operators`, `iskra-users`, `parom-operators`, `parom-agents` |
| API `/api/v1/replicator/kafka` | `/api/v1/parom/kafka` |

SPNEGO привязывает principal `HTTP/<host>` к имени хоста. Поэтому principal,
`container_name`/`hostname` в compose, строки `domain_realm` в `krb5.conf`,
proxyuser в `core-site.xml` и скрипт `demo/infra/kdc/entrypoint.sh` меняются в
одном шаге на сервис. При рассинхронизации вход по SPNEGO возвращает 401, а в
логе сервиса появляется `Server not found in Kerberos database`.

### Стенды и мониторинг

| Было | Стало |
|---|---|
| compose-проекты `<сервис>-explorer-demo`, `hadoop-replicator-demo`, `hadoop-replicator-kafka-demo` | `<новое имя>-demo`, `parom-demo`, `parom-kafka-demo` |
| сервисы и контейнеры `yarn-demo-explorer`/`yarn-explorer`, `hdfs-explorer`, `sql-explorer`, `spark-explorer` | `artel`, `sklad`, `tolmach`, `iskra` |
| контейнеры `replicator-*`, `kafka-replicator-agent-*` | `parom-*`, `parom-kafka-agent-*` |
| тома `sql-explorer-data`, `replicator-db-data`, `replicator-ha-pg-data` | `tolmach-data`, `parom-db-data`, `parom-ha-pg-data` |
| метрики `replicator_*`, `replicator.*` | `parom_*`, `parom.*` |
| метрики `yarn.change.requests`, `hdfs.operations`, `hdfs.bytes.transferred`, `sql.query.duration`, `spark.statement.duration`, `spark.sessions.active` | `artel.change.requests`, `sklad.operations`, `sklad.bytes.transferred`, `tolmach.query.duration`, `iskra.statement.duration`, `iskra.sessions.active` |
| OTel `service.name`: `replicator-agent`, `kafka-replicator-agent`; атрибут `replicator.dc_id` | `parom-agent`, `parom-kafka-agent`; `parom.dc_id` |
| `job_name: 'hadoop-replicator'`, метка `app: 'replicator'`, секрет `replicator-metrics-scrape-token` | `'parom'`, `'parom'`, `parom-metrics-scrape-token` |
| дашборды `hadoop_replicator_{agents,hms_dr,kafka,overview}.json` и uid `hadoop-replicator-*` | `parom_{agents,hms_dr,kafka,overview}.json`, uid `parom-*` |
| дашборды `yarn_explorer_queues`, `hdfs_explorer_operations`, `spark_sql_explorer_analytics` (файл и uid) | `artel_queues`, `sklad_operations`, `iskra_tolmach_analytics` |

Платформенные `job_name` и обзорный дашборд переименовываются по разделу
«Платформа»; внутри них меняются цели скрейпа, селекторы `app=…` и имена
метрик. Ссылки `/d/hadoop-replicator-agents` в четырёх дашбордах и проверка uid
в `demo/monitoring/run-monitoring-smoke-tests.sh` меняются вместе с uid.

## Что не меняется, хотя содержит старое имя

- Список известных секретов в `SecurityConfigurationValidator` и
  `LEGACY_DEFAULT_AGENT_SECRET` (после переименования класса — в
  `ParomProperties`). Это denylist: строки вида
  `yarn-explorer-…-secret-key-…` должны остаться, иначе валидатор перестаёт
  отклонять конфигурацию с опубликованным секретом.
- Ветки старых имён в `ConsumerGroupBlacklistFilter` (см. выше).
- История: `V1__baseline_schema.sql`, существующие записи CHANGELOG, файлы в
  `docs/superpowers/plans/`.
- Данные движков в тестовых фикстурах, например
  `backend/artel/src/test/resources/rm/*.json`.
- Метрика стартера `hadoop.rate.limit.blocks` (Prometheus
  `hadoop_rate_limit_blocks_total`): имя указывает на платформу Hadoop, к
  которой идёт доступ, а не на продукт.
- Переменная окружения `HADOOP_AUTH_MODE` (`zastava.auth.mode` в `application.yml`
  сервисов): то же основание.

## Изменение поведения: определение приложения в LoginModal

`frontend/common/components/LoginModal.svelte` определяет приложение по
подстроке в `<title>` (`'yarn'`, `'replicat'`). С новыми заголовками все
приложения попадают в ветку `universal`. Функция `detectApp` удаляется;
приложение передаёт свой идентификатор (`artel`, `sklad`, `tolmach`, `iskra`,
`parom`) явным свойством компонента. Ключи `APP_MOCK_USERS` и идентификаторы
вкладок меняются на те же значения.

## Порядок работ

После каждого шага, начиная со второго, сборка и тесты затронутых модулей
проходят, а дерево остаётся рабочим.

1. Проверить, заняты ли имена `pristan`, `artel`, `sklad`, `tolmach`, `iskra`, `parom` в
   Maven Central, npm и Docker Hub. Коллизия значима только для имён, которые
   публикуются вне пространства `pristan`.
2. Платформа: groupId, `pristan-parent`, корневой пакет `io.github.balookrd.pristan` во всех
   модулях; стартер `common-security-starter` → `zastava`; npm-область, образы, cookie,
   платформенный дашборд и `job_name`, артефакты CI, отображаемое имя.
3. Artel: каталоги, сборка, Java, конфигурация, стенд `demo/artel`, Kerberos и
   proxyuser стенда, дашборд, фронтенд, e2e.
4. Sklad: то же.
5. Tolmach: то же.
6. Iskra: то же, включая общий с Tolmach дашборд `iskra_tolmach_analytics`.
7. Parom: модули `proto`, `model`, `agent`, `orchestrator` одним шагом через
   реактор родителя, затем миграция таблиц, стенды `demo/parom`, дашборды,
   фронтенд.
8. Общее: `LoginModal`, корневые `pom.xml` и `package.json`, Makefile, скрипты,
   CI, `monitoring/`, `demo/all`, `demo/platform`, `docs/`, README, AGENTS.md.
9. Репозиторий и каталог проекта: по разделу «Каталог проекта и репозиторий на
   GitHub», по отдельной команде.

Каталоги переносятся командой `git mv`, чтобы сохранить историю файлов.

## Проверка

Сборка и тесты выполняются на JDK 21 через `make`.

1. `make test-java` — проходит.
2. `make test-ui` — проходит.
3. `make frontend-build` — статика собирается в `backend/<новое имя>/src/main/resources/static`.
4. `ast-grep scan` — правило `no-synchronized-on-io-path` находит те же файлы,
   что и до переименования (сравнить число совпадений до и после).
5. `make java-index` — индекс собирается.
6. Поиск остатков возвращает только строки из раздела «Что не меняется»:

   ```bash
   git grep -n -i -E '(yarn|hdfs|sql|spark|hadoop)[-_ ]?explorer|org[./]apache[./]hadoop[./]explorer|replicator' -- . ':!docs/superpowers' ':!CHANGELOG*'
   ```

7. Каталогов `backend/{yarn,hdfs,sql,spark,replicator}`,
   `frontend/apps/{yarn,hdfs,sql,spark,replicator}` и
   `demo/{yarn,hdfs,sql,spark,replicator}` нет.
8. Стенды: `make demo-all` поднимается, вход через OpenLDAP и SPNEGO работает
   в каждом сервисе, `make demo-monitoring-smoke` и `make demo-kafka-smoke`
   проходят.

## Несовместимые изменения для существующих установок

Установка со старыми именами после обновления требует ручных действий; кода
миграции нет.

| Что | Действие |
|---|---|
| Principal и keytab | создать новые principal в KDC, выпустить keytab с новыми именами файлов |
| proxyuser | заменить ключи `hadoop.proxyuser.*` в `core-site.xml` кластера, перезапустить NameNode и ResourceManager либо выполнить `hdfs dfsadmin -refreshSuperUserGroupsConfiguration` и `yarn rmadmin -refreshSuperUserGroupsConfiguration` |
| LDAP | переименовать или создать группы `artel-operators`, `iskra-users`, `parom-operators`, `parom-agents` |
| Конфигурация | переименовать ключи и переменные окружения по таблицам выше |
| H2 | переименовать файлы базы; иначе сервис стартует с пустой базой |
| Parom, Postgres | переименовать базу и пользователя либо задать прежние через `POSTGRES_DB` |
| Parom, Kafka | задания начинают читать с позиции по `auto.offset.reset`: новые consumer group не имеют закоммиченных offsets |
| Parom, HDFS | удалить каталоги `.replicator-staging/` в домашних каталогах сервисных пользователей |
| Parom, версии | обновить агенты и оркестратор одновременно |
| Мониторинг | заменить имена метрик и uid в собственных алертах и дашбордах |
| Пользователи UI | сессии сбрасываются, сохранённое состояние интерфейса теряется |

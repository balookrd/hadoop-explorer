# Демо-стенды без mock-ов: план

План убирает из стендов `demo/*` все пути, где сервис отвечает встроенным эмулятором вместо настоящего
компонента. Состояние кода на момент написания — `9c771aa5` (`main`).

Статус на 2026-11-08: задачи 1–6 выполнены (не закоммичены), 7–10 ждут решений Р1–Р3.

## Что эмулируется сейчас

| Стенд | Что эмулируется | Где включено | Следствие |
|---|---|---|---|
| yarn | вход | `demo/yarn/application-demo.yml`: `auth.mode: mock` + `mock-users` | LDAP стенда не используется, `auth_method: mock` |
| yarn | деплой заявок | `yarn.awx.simulate: true`, `base-url: http://mock-awx` | `AwxClientService.simulate()` отвечает `SUCCESS`, RM не меняется |
| hdfs | вход | `demo/hdfs/application-demo.yml`: `auth.mode: mock` | как у yarn |
| sql | движки | все 4 кластера: `mock-storage: true` | запросы выполняет `MockSqlEngine`, Trino 484 и HiveServer2 стенда простаивают |
| sql | ИИ-ассистент | `sql.ai.url: http://host.docker.internal:11434/v1` | без Ollama на хосте `AiService` отвечает правилами `mock-builtin` |
| sql, spark | группы | `auth.mode: hybrid`, вход через LDAP | `"groups": []`: `LdapAuthService` читает только `memberOf`, OpenLDAP стенда его не заполняет; `group-search-base`/`group-search-filter` нигде не используются |
| replicator | вход | `SPRING_PROFILES_ACTIVE: "mock"` в `docker-compose.yml` и `docker-compose.kafka.yml` (`ha`, `hms-load`, `kafka-scale` их расширяют) | `application-mock.yml`: `auth.mode: mock` |
| replicator (kafka) | HMS | `REPLICATOR_HMS_MOCK_CLUSTER_API_ENABLED: "true"` | поднимается `HmsClientPool` с `MockHmsClient` и `/api/v1/hms/clusters`; потребителей на стенде не найдено [проверить: `kafka-agent-*` не создают HMS-клиент] |
| replicator agent | HMS | `ReplicatorAgent`: ветка `else` | без `HMS_THRIFT_URIS` и без `orchestrator-url` агент молча берет `MockHmsClient` |
| replicator (kafka) | Schema Registry DC1 | `schema-registry-1`: образ `confluentinc/cp-schema-registry:7.5.0`, у кластера `schema-registry-type: HORTONWORKS` | Hortonworks Registry не запущен, его API не проверяется [проверить, какие вызовы агент делает к SR типа `HORTONWORKS`] |

Уже на настоящих компонентах: очереди YARN (`client-mode: native`, SPNEGO к RM), HDFS (`mock_storage` по
умолчанию `false`, Kerberos), Spark (`mock-storage: false`, Livy в YARN cluster mode), HMS в основном стенде
Replicator (`REPLICATOR_HMS_MODE: thrift`). Заметка в памяти о том, что `demo/yarn` целиком на моках,
устарела после `ad89e653`.

Не эмуляция, а упрощения; в план не входят, если не решено иначе (см. «Решения»): Livy без SPNEGO
(решение Р5 из `2026-11-07-sql-spark-real-engines.md`), H2 вместо PostgreSQL в Explorer-стендах, Kerberos SSO
выключен (`kerberos.enabled: false`). Быстрый выбор учетки в `LoginModal.svelte` только подставляет логин и
пароль в форму; пользователи из списка есть в `bootstrap.ldif` с тем же паролем `password123`.

## Задачи

Порядок важен: задачи 3–5 опираются на группы из задач 1–2.

### 1. Группы из LDAP через поиск групп

Файлы: `backend/common-security-starter/.../auth/LdapAuthService.java`, тесты стартера.

- Если у записи пользователя нет `memberOf`, искать группы в `ldap.group-search-base` фильтром
  `ldap.group-search-filter` с DN пользователя вместо `{0}` (DN экранировать по RFC 4515).
- Тест на стенд-подобном каталоге: пользователь без `memberOf`, состоящий в двух `groupOfNames`, получает обе
  группы и роль по `admin-groups`/`writer-groups` [проверить: чем стартер уже поднимает LDAP в тестах].
- Проверка: `make test-security-starter`.

Альтернатива — overlay `memberof` в `demo/infra/ldap/slapd.conf`. Не выбрана: ключи `group-search-*`
описаны в конфигурации и гайдах, но сейчас мертвые, и на продовом каталоге без `memberOf` будет та же ошибка.

### 2. Каталог LDAP под конфиги стендов

Файлы: `demo/infra/ldap/bootstrap.ldif`, `demo/README.md`.

- Добавить группы, которые используют конфиги стендов, но которых нет в LDIF: `platform-team`, `dev-leads`,
  `junior-devs`, `spark-users`, `platform-admins`, `developers`, `etl-developers`, `superusers`. Членство
  перенести из удаляемых `mock-users` (например, `writer_user` — `dev-leads`, `de_user` — `spark-users`).
- Привести таблицу учетных записей в `demo/README.md` к LDIF: сейчас там группы `admins`, `engineers`,
  `analysts` и пользователь `engineer_user`, которых в каталоге нет.

### 3. Вход через LDAP на всех стендах

Файлы: `demo/{yarn,hdfs,sql,spark}/application-demo.yml`, `demo/*/start-demo.sh`, README стендов.

- yarn, hdfs: `auth.mode: ldap`; удалить `mock-users`. В yarn сохранить `four-eyes`: заявку создает
  `writer_user`, утверждает `admin_user` (роль по `hadoop-admins`).
- sql, spark: оставить `hybrid`, удалить `mock-users`.
- Убрать «OpenLDAP / Mock» из вывода `start-demo.sh`.
- Проверка на каждом стенде: `POST /api/v1/auth/login` под `admin_user`, `writer_user`, `analyst_user` —
  `auth_method: ldap`, непустые `groups`, роль совпадает с таблицей из задачи 2.

### 4. Replicator без профиля mock

Файлы: `backend/replicator/orchestrator/src/main/resources/application-demo.yml` (новый),
`demo/replicator/docker-compose.yml`, `docker-compose.kafka.yml`, `docker-compose.kafka-scale.yml`.

- Профиль `demo`: `debug: true`, `auth.mode: ldap`, LDAP стенда (`ldap://ldap:389`, ключи `url`,
  `manager-dn`, `user-search-*`, `group-search-*`), группы ролей. `application-mock.yml` оставить для тестов
  [проверить: `grep -rn 'ActiveProfiles.*mock\|profiles.active=mock' backend/replicator`].
- `SPRING_PROFILES_ACTIVE: "demo"` во всех compose-файлах Replicator; `SPRING_LDAP_URLS` из
  `kafka-scale.yml` перенести в профиль — тогда health перестанет отдавать 503 и в `kafka.yml`.
- Удалить `REPLICATOR_HMS_MOCK_CLUSTER_API_ENABLED: "true"` из `docker-compose.kafka.yml`.
- Проверка: `run-smoke-tests.sh`, `run-kafka-smoke-tests.sh`, `create-test-jobs.sh` — логин `admin_user`/
  `password123` в скриптах подходит и для LDAP.

### 5. Агент без тихого перехода на MockHmsClient

Файлы: `backend/replicator/agent/.../agent/ReplicatorAgent.java`, `ReplicatorAgentConfig.java`, тесты агента.

- В ветке `else` (режим `all`/`hms`/`receiver`, нет Thrift URI и orchestrator URL) останавливать старт с
  сообщением, какой параметр задать. `hms-mode: mock` оставить только явным значением для unit-тестов.
- Проверка: `mvn test -f backend/replicator/pom.xml -pl agent -am` на JDK 21.

### 6. SQL Explorer на Trino и HiveServer2 стенда

Файлы: `demo/sql/application-demo.yml`, `demo/sql/docker-compose.yaml`, `demo/infra/kdc/entrypoint.sh` или
отдельный `krb5.conf` для `sql-explorer`, `demo/sql/README.md`.

- `mock-storage: false` у всех четырех кластеров.
- Канонизация имен: `krb5.conf` для `sql-explorer` с `dns_canonicalize_hostname = false`, путь в
  `JAVA_TOOL_OPTIONS` и в `kerberos.krb5-conf` кластера `trino-kerberized` (обход уже проверен и описан в
  «Известных проблемах» `demo/sql/README.md`). Без этого `trino-kerberized` падает с `LOOKING_UP_SERVER`,
  `hive-archive-analytics` — с `GSS initiate failed`.
- `allowed-groups` начнут работать после задачи 1; обход через `allowed-users` не нужен.
- `start-demo.sh` дожидается успешного `hive/init-demo-data.sh`, иначе первые запросы падают с
  `Database demo_db does not exist`.
- `INSERT` падает с `return code 2 ... MapRedTask`: [проверить] запуск MR в local mode
  (`mapreduce.framework.name=local` в `hive/hive-site.xml`) или отправку в YARN стенда; если не решается в
  рамках плана — оставить в известных проблемах.
- Проверка: под `analyst_user` `SELECT current_user` на `trino-kerberized` → `analyst_user`;
  `SELECT * FROM demo_db.sales` на `hive-demo-analytics`; `archive_db` на `hive-archive-analytics`.

### 7. Деплой заявок YARN без симуляции

Зависит от решения Р1. Файлы при варианте «RM REST»: `backend/yarn/.../service/` (новая стратегия деплоя
рядом с `AwxClientService`), `YarnProperties`, `demo/yarn/cluster-*/yarn-site.xml`.

### 8. ИИ-ассистент SQL на LLM

Зависит от решения Р2.

### 9. Schema Registry Hortonworks для DC1

Зависит от решения Р3.

### 10. Защита от возврата mock-ов и документация

- Скрипт или тест, падающий при `mode: mock`, `mock-storage: true`, `mock_storage: true`, `simulate: true`,
  `SPRING_PROFILES_ACTIVE: "mock"`, `MOCK_CLUSTER_API_ENABLED: "true"` в `demo/`; подключить к
  `./scripts/run-tests.sh all`.
- Обновить `demo/*/README.md`, `demo/all/README.md`, `docs/` (разделы про демо-стенды), удалить из известных
  проблем закрытые пункты.
- Финальная проверка: `demo/all/start-all-demo.sh`, вход под каждой учеткой во все пять UI.

## Решения

| № | Вопрос | Варианты | Рекомендация |
|---|---|---|---|
| Р1 | Чем заменить `yarn.awx.simulate` | а) настоящий AWX: с версии 18 ставится только через awx-operator в Kubernetes, на стенде — k3s в контейнере, несколько ГБ RAM; б) новая стратегия деплоя через RM REST `PUT /ws/v1/cluster/scheduler-conf` (нужен `yarn.scheduler.configuration.store.class` ≠ `file` на RM стенда), AWX остается для прода; в) ansible-runner в контейнере стенда с ролью `ansible/roles/yarn_capacity_scheduler` по SSH к RM | б): меняет настоящий RM, без тяжелой инфраструктуры; а) — отдельным опциональным стендом |
| Р2 | ИИ-ассистент | а) контейнер Ollama в `demo/sql` с загрузкой `qwen2.5-coder:7b` (~4.7 ГБ) при первом старте; б) оставить Ollama на хосте, без него ассистент выключен (`sql.ai.enabled: false`) вместо правил `mock-builtin` | б) по умолчанию, а) — compose-профилем `ai` |
| Р3 | Schema Registry DC1 | а) настоящий Hortonworks Registry [проверить: доступный образ и поддержка arm64]; б) оставить Confluent и сменить тип кластера на `CONFLUENT` | решить после проверки из таблицы выше |
| Р4 | Упрощения (Livy без SPNEGO, H2 в Explorer-стендах) | закрыть в этом плане или оставить | оставить: это не эмуляция, отдельный план |

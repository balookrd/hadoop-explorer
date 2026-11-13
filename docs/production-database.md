# Перевод YARN, HDFS, SQL и Spark Explorer на PostgreSQL и запуск нескольких экземпляров

Документ описывает перевод сервисов Pristan (`artel`, `sklad`, `tolmach`, `spark-explorer`) на внешний PostgreSQL и настройки, без которых два и более экземпляра одного сервиса за балансировщиком работают некорректно. Документ предназначен для администратора, который разворачивает сервисы в production.

Упакованные `application.yml` сервисов используют H2:

| Сервис | `spring.datasource.url` по умолчанию | Что хранится в БД |
|---|---|---|
| Artel | `${ARTEL_DB_URL:jdbc:h2:file:./data/artel;DB_CLOSE_DELAY=-1}` (файл) | Заявки на изменение конфигурации (`change_requests`) |
| Sklad | `jdbc:h2:mem:hdfs;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` (память) | JPA-сущностей нет; только таблицы `SessionStore` при `session-store.type: jdbc` |
| Tolmach | `jdbc:h2:mem:tolmach;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` (память) | Воркспейсы, история и сохраненные запросы |
| Spark Explorer | `jdbc:h2:mem:sparkexplorer;DB_CLOSE_DELAY=-1;MODE=PostgreSQL` (память) | Сессии Spark, история выполнения, воркспейсы |

В Sklad в сборку входит драйвер SQLite (`org.xerial:sqlite-jdbc` в `backend/sklad/pom.xml`), но упакованная конфигурация его не использует.

Независимо от БД в памяти процесса по умолчанию хранятся:

- Сессии и отозванные токены: `zastava.session-store.type` по умолчанию `memory`. Как перенести их в PostgreSQL — в разделе [3.1](#31-хранилище-сессий-в-postgresql).
- Корзины rate limiting (`RateLimiterService`). Он не обращается к БД ни при каком `session-store.type`, подробнее — в разделе [3.4](#34-rate-limiting-при-нескольких-экземплярах).

> **Внимание**
> На H2 сервис запускается только в одном экземпляре: in-memory H2 у каждого процесса своя, а файл H2 открывает один процесс.
> Для двух и более экземпляров за балансировщиком подключите внешний PostgreSQL и выполните настройки раздела 3.

---

## 1. Подготовка базы данных PostgreSQL

Создайте пользователя и по базе данных на каждый сервис (или одну базу с отдельными схемами и пользователями):

```sql
CREATE USER hadoop_explorer WITH PASSWORD 'your_strong_password';

CREATE DATABASE sklad OWNER hadoop_explorer;
CREATE DATABASE tolmach OWNER hadoop_explorer;
CREATE DATABASE spark_explorer OWNER hadoop_explorer;
CREATE DATABASE artel OWNER hadoop_explorer;
```

Длинные тексты SQL, Spark и Artel (текст запросов, код, логи, JSON и XML заявок) хранятся в колонках `text`. Large Object (`oid`) эти сервисы не используют, поэтому не нужны `vacuumlo` и права на `pg_largeobject`.

Поддерживается развертывание на пустой базе: таблицы создают Hibernate (`spring.jpa.hibernate.ddl-auto: update`) и `DefaultSessionStore`. `ddl-auto: update` не меняет типы существующих колонок: таблица, созданная прежней версией с колонкой `oid`, останется с `oid`, а не перейдет на `text`.

---

## 2. Настройка подключения к PostgreSQL

Задайте подключение переменными окружения:

| Переменная | Пример значения | Что делает |
|---|---|---|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://<pg_host>:5432/<database>` | Строка подключения JDBC (HikariCP). Драйвер Spring Boot выбирает по URL |
| `SPRING_DATASOURCE_USERNAME` | `hadoop_explorer` | Пользователь БД. Обязательна: упакованное значение `sa` предназначено для H2 |
| `SPRING_DATASOURCE_PASSWORD` | `your_strong_password` | Пароль пользователя БД. Обязательна: упакованное значение пустое |

`<pg_host>` — адрес сервера PostgreSQL, `<database>` — база сервиса из раздела 1.

Переменные `SPRING_DATASOURCE_*` действуют во всех четырех сервисах, включая Artel: переменные окружения переопределяют значения `spring.datasource.*` из упакованного `application.yml`. В Artel те же параметры можно задать через переменные, на которые ссылается его `application.yml`:

| Переменная Artel | Свойство | Упакованное значение |
|---|---|---|
| `ARTEL_DB_URL` | `spring.datasource.url` | `jdbc:h2:file:./data/artel;DB_CLOSE_DELAY=-1` |
| `ARTEL_DB_USER` | `spring.datasource.username` | `sa` |
| `ARTEL_DB_PASSWORD` | `spring.datasource.password` | пусто |
| `ARTEL_DB_DRIVER` | `spring.datasource.driverClassName` | пусто (драйвер выбирается по URL) |

Если заданы и `SPRING_DATASOURCE_URL`, и `ARTEL_DB_URL`, действует `SPRING_DATASOURCE_URL`.

Пример запуска контейнера Tolmach:

```bash
# <jwt_secret> - вывод `openssl rand -hex 32`, одинаковый на всех экземплярах сервиса
docker run -d \
  -e SPRING_DATASOURCE_URL="jdbc:postgresql://postgres:5432/tolmach" \
  -e SPRING_DATASOURCE_USERNAME="hadoop_explorer" \
  -e SPRING_DATASOURCE_PASSWORD="secret" \
  -e ZASTAVA_SESSIONSTORE_TYPE="jdbc" \
  -e JWT_SECRET_KEY="<jwt_secret>" \
  -p 8003:8003 \
  pristan/tolmach:latest
```

---

## 3. Настройки для двух и более экземпляров

### 3.1 Хранилище сессий в PostgreSQL

Для двух и более экземпляров сервиса задайте `zastava.session-store.type: jdbc` (переменная окружения `ZASTAVA_SESSIONSTORE_TYPE=jdbc`):

```yaml
hadoop:
  security:
    session-store:
      type: jdbc   # по умолчанию memory
```

При `jdbc` и настроенном `spring.datasource` `DefaultSessionStore` создает при старте таблицы `active_sessions` и `revoked_tokens` (`CREATE TABLE IF NOT EXISTS`) и хранит в них сессии и отозванные токены. Остальные параметры хранилища описаны в [CONFIGURATION.md, раздел 2.5](CONFIGURATION.md#25-хранилище-сессий-токенов-и-rate-limiting-sessionstore--tri-storage).

Что сломается при `memory`: выход пользователя на экземпляре A отзывает токен только в памяти A. Экземпляр B не находит токен в своем списке отозванных, берет сессию из своей памяти или проверяет подпись JWT и принимает токен до истечения его срока: `zastava.jwt.expiration-minutes`, в упакованных `application.yml` 480 минут (8 часов).

### 3.2 Одинаковый `JWT_SECRET_KEY` на всех экземплярах

Задайте один и тот же `JWT_SECRET_KEY` (`zastava.jwt.secret-key`) на всех экземплярах сервиса. Токен подписывает экземпляр, на котором пользователь вошел. Экземпляр, получивший запрос, сначала ищет сессию по хэшу токена в хранилище сессий и только если не нашел, проверяет подпись JWT своим ключом (`CommonAuthFilter`). При разных ключах проверка подписи не проходит, и пользователь получает 401:

- при `session-store.type: memory` — на каждом запросе, который балансировщик отправил не на экземпляр входа;
- при `jdbc` — когда сессии нет в `active_sessions`, например если запись в PostgreSQL при входе не удалась (в логе экземпляра входа `Failed to save session to JDBC, falling back to in-memory`).

### 3.3 Привязка клиента к экземпляру

Часть состояния SQL и Spark Explorer хранится в памяти экземпляра, который выполняет запрос, и в PostgreSQL не попадает:

| Сервис | Состояние в памяти | Что видит пользователь, если запрос попал на другой экземпляр |
|---|---|---|
| Tolmach | Строки результата (`resultCache` в `QueryManagerService`) | `GET /api/v1/queries/{queryId}/result` возвращает колонки и `total` из БД, но пустой список строк: результат выглядит пустым при `total > 0` |
| Tolmach | Признаки отмены выполняющихся запросов (`activeQueries`) | `POST /api/v1/queries/{queryId}/cancel` ставит в БД статус `CANCELLED`, но запрос продолжает выполняться на исходном экземпляре; по завершении тот перезаписывает статус на `FINISHED` |
| Tolmach | События запросов (`QueryEventHub`) и подписки на уведомления (`userEmitters`) | `GET /api/v1/queries/{queryId}/stream` на другом экземпляре не получает событий о ходе запроса; если запрос уже завершен, приходит только терминальное событие из `query_history`, без строк. `GET /api/v1/queries/notifications/stream` на другом экземпляре не получает уведомлений о завершении |
| Spark Explorer | Строки результата (`resultCache` в `SparkSessionManager`) | `GET /api/v1/statements/{executionId}/result` возвращает статус, колонки и `total` из БД, но пустой список строк |
| Spark Explorer | Признаки отмены (`activeExecutions`) | `POST /api/v1/statements/{executionId}/cancel` ставит в БД `CANCELLED`, но код продолжает выполняться; по завершении статус перезаписывается результатом выполнения |
| Spark Explorer | SSE-подписки (`executionEmitters`) | `GET /api/v1/statements/{executionId}/stream` на другом экземпляре не получает событий |
| Spark Explorer | Пайплайны и их запуски (`PipelineService`) | Пайплайн, созданный на одном экземпляре, не виден в `/api/v1/pipelines` на другом; после перезапуска экземпляра пайплайны пропадают |
| Sklad | Части загружаемого по частям файла: каталог `${java.io.tmpdir}/hdfs-uploads/<upload_id>` и счетчик `chunkTracker` в `HdfsFileOperationService` | Части одной загрузки (`POST /api/v1/clusters/{clusterId}/files/upload-chunk`), попавшие на разные экземпляры, ни один экземпляр не собирает целиком: файл в HDFS не появляется, `upload-chunk/status` показывает не все части |

Записи о сессиях Spark (`spark_sessions`) хранятся в БД и видны всем экземплярам. Статус сессии из Livy обновляет фоновая синхронизация каждого экземпляра (один `GET /sessions` на кластер раз в `spark.session-sync-interval-ms`; при N экземплярах — N запросов за интервал, записи меняются условными `UPDATE`). Остановку, закрытие по `spark.session-ttl-minutes` (задача `cleanupIdleSessions` запускается на каждом экземпляре, но `DELETE` в Livy отправляет только тот, чей условный `UPDATE` `markKilledIfIdleBefore` перевел запись из `idle` в `killed`; остальные экземпляры получают 0 измененных строк и `DELETE` не отправляют; сам `DELETE` выполняется в пуле потоков `SparkSessionManager`, а не в потоке планировщика) и закрытие при выходе (обрабатывает экземпляр, принявший `POST /api/v1/auth/logout`) может выполнить любой экземпляр: Livy-идентификатор сессии берется из БД. Остановка и закрытие при выходе так же переводят запись условным `UPDATE` (`markKilledIfActive`), поэтому одну сессию в Livy удаляет один экземпляр. Если Livy уже забыл сессию (перезапуск без восстановления), `DELETE` получает 404 и считается успешным. Если в закрытой сессии выполнялся код на другом экземпляре, это выполнение завершается `FAILED`, запись сессии остается `killed`.

В Artel пользовательского состояния в памяти нет: заявки хранятся в БД, в памяти только кэш клиентов YARN (`YarnClientFactory.clientCache`). Sklad, кроме загрузки по частям, тоже не хранит пользовательского состояния в памяти.

Для SQL, Spark и Sklad выберите один из вариантов:

- Настройте на балансировщике привязку клиента к экземпляру (sticky sessions).
- Запускайте SQL и Spark Explorer в одном экземпляре. Для Sklad без привязки не работает только загрузка по частям.

Пример для nginx с привязкой по cookie `access_token`. Стартер выставляет при входе cookie под всеми именами из `zastava.cookie.names`; `access_token` есть в списке по умолчанию и в упакованных `application.yml` SQL и Spark Explorer:

```nginx
upstream tolmach {
    hash $cookie_access_token consistent;
    server tolmach-1:8003;
    server tolmach-2:8003;
}
```

Если клиенты передают токен только в заголовке `Authorization` без cookie, используйте привязку по адресу клиента: `ip_hash;` вместо строки `hash ...`. При `ip_hash` все пользователи за одним NAT или прокси попадают на один экземпляр.

### 3.4 Rate limiting при нескольких экземплярах

`RateLimiterService` хранит корзины в памяти процесса (Caffeine) и к БД не обращается, у каждого экземпляра свои корзины. Если балансировщик распределяет запросы одного клиента по N экземплярам, фактический лимит до N раз выше настроенного в `zastava.rate-limiter.*`. При привязке клиента к экземпляру (раздел 3.3) запросы одного клиента попадают в корзины одного экземпляра.

---

## 4. Проверка перед запуском в production

- [ ] `SPRING_DATASOURCE_URL` (для Artel — `SPRING_DATASOURCE_URL` или `ARTEL_DB_URL`) указывает на `jdbc:postgresql://...`.
- [ ] Заданы `SPRING_DATASOURCE_USERNAME` и `SPRING_DATASOURCE_PASSWORD` (для Artel допустимы `ARTEL_DB_USER` и `ARTEL_DB_PASSWORD`).
- [ ] При двух и более экземплярах задан `ZASTAVA_SESSIONSTORE_TYPE=jdbc` (раздел 3.1).
- [ ] `JWT_SECRET_KEY` одинаковый на всех экземплярах сервиса (раздел 3.2).
- [ ] Для SQL, Spark и Sklad с несколькими экземплярами на балансировщике настроена привязка клиента к экземпляру, либо SQL и Spark Explorer запущены в одном экземпляре (раздел 3.3).
- [ ] Пароль базы данных не хранится в открытом виде в `application.yml` или `docker-compose.yml`, а передается через переменную окружения из хранилища секретов (например, HashiCorp Vault).
- [ ] Проверка доступности на балансировщике смотрит на `/actuator/health/readiness`. По умолчанию группа readiness проверяет только состояние приложения, не PostgreSQL. Чтобы экземпляр выводился из балансировки при недоступной базе, добавьте в конфигурацию сервиса `management.endpoint.health.group.readiness.include: readinessState,db`; при сбое PostgreSQL из балансировки тогда выйдут все экземпляры сразу.

---

## 5. Известные проблемы

### Предупреждения при одновременном первом старте на пустой базе

Если несколько экземпляров одновременно стартуют на пустой базе, они параллельно создают таблицы: `DefaultSessionStore` (`CREATE TABLE IF NOT EXISTS`) и Hibernate (`ddl-auto: update`). В логе части экземпляров появляются WARN с текстом `duplicate key value violates unique constraint "pg_type_typname_nsp_index"` и/или `relation "..." already exists`. Старт не прерывается, схема создается полностью (проверено на Tolmach и PostgreSQL 16).

Чтобы предупреждений не было, запустите первый экземпляр один и дождитесь, пока `/actuator/health/readiness` вернет 200, затем запустите остальные.

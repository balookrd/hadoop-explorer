# Руководство по конфигурации Pristan

Документ для администраторов платформы Pristan: параметры конфигурации, форматы файлов и переменные окружения компонентов.
- Общий стартер безопасности (`backend/zastava` — Java 21 / Spring Boot 3, SPNEGO, LDAP/Active Directory, JWT, L1/L2 сессии, Circuit Breaker, Rate Limiter, CSRF Guard).
- YARN Explorer (`backend/yarn` — Java 21 LTS / Spring Boot 3, YARN RM HA, Capacity Scheduler, партиции, Change Requests, Four-Eyes Principle, генерация XML, Ansible AWX).
- HDFS Explorer (`backend/hdfs` — Java 21 LTS / Spring Boot 3, подключение к HDFS Client, HA NameNode, Kerberos, doAs имперсонация, квоты, превью Parquet/ORC, Circuit Breaker).
- SQL Explorer (`backend/sql` — Java 21 LTS / Spring Boot 3, Trino DB API, Apache Hive / HiveServer2, AI-ассистент, история и кэширование).
- Spark Explorer (`backend/spark` — Java 21 LTS / Spring Boot 3, Apache Livy, PySpark, Scala, DAG Pipelines, Metastore, Circuit Breaker).
- Hadoop gRPC Replicator (`backend/replicator` — Java 21 / Spring Boot 3 Orchestrator, нативный Java gRPC Worker Daemon, топология ЦОД, Hierarchical Token Bucket, Kerberos, Snapshot Diff, Cron Scheduler).

---

## Содержание
1. [Общие принципы и приоритеты конфигурации](#1-общие-принципы-и-приоритеты-конфигурации)
2. [Общий стартер безопасности (zastava)](#2-общий-стартер-безопасности-zastava)
   - [Сервер и безопасность (CORS, CSRF, Cookies, JWT)](#21-сервер-и-безопасность-cors-csrf-cookies-jwt)
   - [Аутентификация: LDAPS / Active Directory](#22-аутентификация-ldaps--active-directory)
   - [Аутентификация: Kerberos SPNEGO SSO](#23-аутентификация-kerberos-spnego-sso)
   - [Конфигурация ролевой модели и прав доступа (RBAC)](#24-конфигурация-ролевой-модели-и-прав-доступа-rbac)
   - [Хранилище сессий, токенов и Rate Limiting (SessionStore & Tri-Storage)](#25-хранилище-сессий-токенов-и-rate-limiting-sessionstore--tri-storage)
   - [Отказоустойчивость: Circuit Breaker, Distributed Lock и Graceful Shutdown](#26-отказоустойчивость-circuit-breaker-retry-global-exception-handlers-distributed-lock-и-graceful-shutdown)
   - [Сквозное шифрование TLS/HTTPS и mTLS для REST и gRPC каналов](#27-сквозное-шифрование-tlshttps-и-mtls-для-rest-и-grpc-каналов)
3. [Настройка YARN Explorer](#3-настройка-yarn-explorer)
   - [YARN-кластеры и партиции](#31-yarn-кластеры-и-партиции)
   - [Ролевая модель и Change Requests](#32-ролевая-модель-и-change-requests)
   - [Интеграция с Ansible AWX (доставка и применение конфигурации)](#33-интеграция-с-ansible-awx-доставка-и-применение-конфигурации)
4. [Настройка HDFS Explorer](#4-настройка-hdfs-explorer)
5. [Настройка SQL Explorer (Trino & Hive)](#5-настройка-sql-explorer-trino--hive)
   - [Аналитические кластеры](#51-аналитические-кластеры)
   - [Параметры выполнения запросов и сохранение воркспейса](#52-параметры-выполнения-запросов-и-сохранение-воркспейса)
   - [ИИ-ассистент (On-Premise LLM / Ollama / vLLM)](#53-ии-ассистент-on-premise-llm--ollama--vllm)
6. [Настройка Spark Explorer (Livy, PySpark, Scala, Metastore)](#6-настройка-spark-explorer-livy-pyspark-scala-metastore)
   - [Кластеры и интеграция с Apache Livy](#61-кластеры-и-интеграция-с-apache-livy)
   - [Версии Spark и среды Python (HDFS Archives)](#62-версии-spark-и-среды-python-hdfs-archives)
   - [Интеграция с YARN и разграничение очередей](#63-интеграция-с-yarn-и-разграничение-очередей)
   - [Hive Metastore, Iceberg и профили ресурсов](#64-hive-metastore-iceberg-и-профили-ресурсов)
   - [Персистентность рабочих пространств пользователя](#65-персистентность-рабочих-пространств-пользователя)
7. [Настройка Hadoop gRPC Replicator (DC-DC WAN Sync & Throttling)](#7-настройка-hadoop-grpc-replicator-dc-dc-wan-sync--throttling)
   - [Архитектура и компоненты (Orchestrator, Agent)](#71-архитектура-и-компоненты-orchestrator-agent)
   - [Безопасные значения по умолчанию и продуктивный профиль](#711-безопасные-значения-по-умолчанию-и-продуктивный-профиль)
   - [Трассировка и health-пробы gRPC](#712-трассировка-и-health-пробы-grpc)
   - [Топология дата-центров и кластеров](#72-топология-дата-центров-и-кластеров)
   - [Многоуровневые лимиты полосы (DC-DC и HDFS-HDFS)](#73-многоуровневые-лимиты-полосы-dc-dc-и-hdfs-hdfs)
   - [Безопасность, RBAC и системная техучетка Kerberos](#74-безопасность-rbac-и-системная-техучетка-kerberos)
   - [Настройка Replicator Agent (Full-Duplex) и буфера Staging](#75-настройка-replicator-agent-full-duplex-и-буфера-staging)
   - [Разрешение целевых адресов (Service Discovery, AGENT_TARGET_* и Fallback)](#76-разрешение-целевых-адресов-service-discovery-agent_target_-и-fallback)
   - [Планировщик периодических задач (Cron Scheduler)](#77-планировщик-периодических-задач-cron-scheduler)
   - [Локальное ограничение пропускной способности агента (`AGENT_MAX_BANDWIDTH_MB_S`)](#78-локальное-ограничение-пропускной-способности-агента-agent_max_bandwidth_mb_s-и-модель-согласования-wan)
   - [Эталонный фрагмент конфигурации топологии (application.yml)](#79-эталонный-фрагмент-конфигурации-топологии-applicationyml)
8. [Переменные окружения](#8-переменные-окружения)
9. [Мониторинг Prometheus и дашборды Grafana](#9-мониторинг-prometheus-и-дашборды-grafana)
10. [Руководства администратора по развертыванию (DevOps Hub)](#10-руководства-администратора-по-развертыванию-devops-hub)

---

## 1. Общие принципы и приоритеты конфигурации

Сервисы — приложения Spring Boot 3 и загружают конфигурацию в стандартном порядке Spring Boot (по убыванию приоритета):
1. Переменные окружения. Ключ получается по правилам relaxed binding: `zastava.jwt.secret-key` → `ZASTAVA_JWT_SECRET_KEY`. Часть ключей в упакованном `application.yml` дополнительно ссылается на короткие переменные (`JWT_SECRET_KEY`, `HADOOP_AUTH_MODE`, `LDAP_URL` и т. п.).
2. Внешний файл `application.yml`, указанный в `SPRING_CONFIG_ADDITIONAL_LOCATION` (или `--spring.config.additional-location`).
3. `application.yml` (и `application-<profile>.yml` для профилей из `SPRING_PROFILES_ACTIVE`), упакованные в JAR из `backend/<service>/src/main/resources/`.

Переменные `CONFIG_PATH`, `HDFS_CONFIG_PATH`, `YARN_CONFIG_PATH`, `SQL_CONFIG_PATH`, `SPARK_CONFIG_PATH` и файлы `config/config.yaml` Java-сервисы не читают. Исключение — HDFS Explorer: список кластеров можно вынести в отдельный YAML и указать путь в `hadoop.hdfs.clusters-config-path`.

> [!IMPORTANT]
> Значения по умолчанию и проверки на старте (все Java-сервисы).
> Упакованный в JAR `application.yml` каждого сервиса рассчитан на продакшен: `zastava.debug: false`,
> `auth.mode: ${HADOOP_AUTH_MODE:ldap}`, JWT-секрет только из `JWT_SECRET_KEY`. Стартер безопасности
> не даст сервису запуститься, если:
> - `auth.mode` равен `mock` без `zastava.debug: true`. В mock-режиме демо-учетки входят без проверки
>   в каталоге, а для `admin`/`writer` подходит любой пароль;
> - `zastava.jwt.secret-key` пуст, короче 32 символов или совпадает со встроенным значением из исходников;
> - `auth.mode: ldap` задан при `ldap.enabled: false`;
> - `auth.mode` не входит в `mock | ldap | kerberos | hybrid`.
>
> Вне debug (`zastava.debug: false`) сервис также отказывается стартовать с любым JWT-секретом, опубликованным в репозитории (`SecurityConfigurationValidator.PUBLISHED_SECRET_KEYS`: значения из профилей `mock`, демо-стендов, прежних значений чартов и примеров документации), с сообщением `zastava.jwt.secret-key is a value published in the Pristan repository`.
>
> Локальная разработка без каталога включает профиль `mock` явно (`SPRING_PROFILES_ACTIVE=mock`,
> файл `application-mock.yml` в ресурсах сервиса). Демо-стенды `demo/*` входят через OpenLDAP стенда:
> Explorer-сервисы задают `auth.mode: ldap`/`hybrid` и `debug: true` в `application-demo.yml`, оркестратор
> Replicator — профилем `demo` (`SPRING_PROFILES_ACTIVE=demo`, `application-demo.yml`).
> Ключи из старых руководств (`auth-mode`, `ldap.server-uri`, `bind-dn`, `bind-password`, `user-base-dn`,
> `user-filter`, `kerberos.keytab-path`, `kerberos.principal`) принимаются как синонимы.

---

## 2. Общий стартер безопасности (zastava)

Параметры раздела читает `ZastavaProperties` (`@ConfigurationProperties(prefix = "zastava")`) из `backend/zastava`. Стартер подключают сервисы `yarn`, `hdfs`, `sql`, `spark` и `replicator/orchestrator`, поэтому ключи одинаковы для всех.

### 2.1 Сервер и безопасность (CORS, CSRF, Cookies, JWT)

```yaml
server:
  port: 8000

hadoop:
  security:
    debug: false
    jwt:
      secret-key: ${JWT_SECRET_KEY:}   # Обязателен: не короче 32 символов
      algorithm: HS256
      expiration-minutes: 480
    cookie:
      names: ["access_token", "pristan_session", "session_token", "hdfs_explorer_session"]
      domain:                          # Не задан: cookie привязана к хосту ответа
      path: /
      secure: true                     # Передавать cookie только по HTTPS (по умолчанию false)
      same-site: Lax
      http-only: true
    cors:
      allowed-origins:
        - "https://hadoop.company.local"
        - "https://hdfs.company.local"
```

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `jwt.secret-key` | встроенное значение из исходников | Ключ подписи JWT. Встроенное значение, пустое или короче 32 символов останавливает старт вне mock-режима (раздел 1) |
| `jwt.expiration-minutes` | `480` | Срок жизни токена и `Max-Age` cookie |
| `cookie.names` | 4 имени из примера | При входе токен записывается в cookie с каждым из этих имен |
| `cookie.secure` | `false` | Атрибут `Secure` cookie; за HTTPS-прокси выставьте `true` |
| `cors.allowed-origins` | `http://localhost:3000`, `:5173`, `:8000`, `:8001`, … | Разрешенные Origins для CORS; тот же список использует проверка CSRF |

Проверка CSRF (`CsrfProtectionValidator`) применяется только к запросам с аутентификацией по cookie и методами, меняющими состояние. Запрос отклоняется при `Sec-Fetch-Site: cross-site`; принимается при `X-Requested-With: XMLHttpRequest` либо при `Origin`/`Referer` из `cors.allowed-origins` или совпадающем с адресом самого сервиса.

#### HTTP-заголовки безопасности и CSP

Отдельной настройки заголовков в коде нет: `SecurityFilterChain` из `ZastavaAutoConfiguration` (и `replicatorSecurityFilterChain` в оркестраторе репликатора) не вызывает `http.headers(...)`, поэтому в ответы попадают заголовки Spring Security по умолчанию:
- `X-Content-Type-Options: nosniff`: браузер не подменяет MIME-тип ответа.
- `X-Frame-Options: DENY`: запрет встраивания в iframe (clickjacking).
- `Cache-Control: no-cache, no-store, max-age=0, must-revalidate`, `Pragma: no-cache`, `Expires: 0`.
- `X-XSS-Protection: 0`.
- `Strict-Transport-Security: max-age=31536000 ; includeSubDomains`: только в ответах на запросы по HTTPS.

`Content-Security-Policy` и `Referrer-Policy` сервисы не выставляют. Если они нужны, добавьте их на обратном прокси (ingress).

### 2.2 Аутентификация: LDAPS / Active Directory

`LdapAuthService` из стартера безопасности проверяет пароль по каталогу LDAP (OpenLDAP, FreeIPA, Microsoft Active Directory) через JNDI:
1. Подключается к `zastava.ldap.url` от имени `manager-dn` (если `manager-dn` пуст — анонимно).
2. Ищет пользователя в `user-search-base` (если не задан — в `base-dn`) по фильтру `user-search-filter`, подставляя имя вместо `{0}`. Перед подстановкой символы `\`, `*`, `(`, `)` и NUL экранируются (защита от LDAP injection).
3. Выполняет bind от DN найденного пользователя с введенным паролем.
4. Берет группы из атрибута `memberOf` (значение `CN` из DN группы) и по ним вычисляет роль (раздел 2.4).

Ключи `group-search-base` и `group-search-filter` есть в `ZastavaProperties`, но `LdapAuthService` их не использует: группы берутся только из `memberOf`. Для `ldaps://` сертификат сервера проверяется по truststore JVM.

```yaml
hadoop:
  security:
    auth:
      mode: ldap                # mock | ldap | kerberos | hybrid
    ldap:
      enabled: true
      url: "ldaps://ad.company.local:636"
      base-dn: "dc=company,dc=local"
      manager-dn: "cn=svc_pristan,ou=Service Accounts,dc=company,dc=local"
      manager-password: ${LDAP_MANAGER_PASSWORD:}
      user-search-base: "ou=Users,dc=company,dc=local"
      user-search-filter: "(&(objectClass=user)(sAMAccountName={0}))" # для OpenLDAP: (uid={0})
```

Значения по умолчанию: `url: ldap://localhost:389`, `base-dn: dc=example,dc=com`, `user-search-base: ou=users`, `user-search-filter: (&(objectClass=person)(uid={0}))`. Упакованный `application.yml` сервисов задает `url`, `manager-dn`, `manager-password` и `user-search-base` через переменные `LDAP_URL`, `LDAP_MANAGER_DN`, `LDAP_MANAGER_PASSWORD`, `LDAP_USER_SEARCH_BASE`.

### 2.3 Аутентификация: Kerberos SPNEGO SSO

Пользователь входит в веб-интерфейс по Kerberos-тикету из своей системной сессии, без ввода пароля: браузер передает заголовок `Authorization: Negotiate <ticket>`.

```yaml
hadoop:
  security:
    auth:
      mode: kerberos      # или hybrid: LDAP-форма и SPNEGO одновременно
    kerberos:
      enabled: true
```

`KerberosSpnegoService` проверяет тикет через Java GSS-API и создает контекст с учетными данными acceptor по умолчанию (`GSSManager.createContext((GSSCredential) null)`). Ключи `zastava.kerberos.keytab`, `service-principal` и `krb5-config` объявлены в `ZastavaProperties`, но сервис их не читает: изменение этих ключей на проверку тикета не влияет. Principal `HTTP/<host>@<REALM>` и его keytab JVM берет из JAAS-конфигурации: задайте их в секции `com.sun.security.jgss.accept` файла, переданного через `-Djava.security.auth.login.config=<путь>`, вместе с `-Djavax.security.auth.useSubjectCredsOnly=false` и `-Djava.security.krb5.conf=<путь к krb5.conf>`. Без этого JVM не находит ключ acceptor, и проверка тикета завершается ошибкой GSS.

### 2.4 Конфигурация ролевой модели и прав доступа (RBAC)

Все сервисы платформы вычисляют роль пользователя одним классом `RoleResolver` (метод `resolve`) из стартера безопасности:

```yaml
hadoop:
  security:
    auth:
      admin-users: ["admin", "superadmin"]
      admin-groups: ["hadoop-admins", "platform-admins", "domain admins"]
      writer-users: []
      writer-groups: ["data-engineers", "yarn-operators", "etl-developers"]
```

Имена пользователей и групп сравниваются без учета регистра. Группы из конфигурации добавляются к встроенным спискам, а не заменяют их:
- встроенные группы администраторов: `hadoop-admins`, `admins`, `data-platform-admins`, `platform-admins`, `superusers`;
- встроенные группы writer: `data-engineers`, `engineers`, `spark-users`, `etl-developers`, `yarn-operators`, `operators`, `replicator-operators`, `dev-leads`.

Роли:
- Пользователи из `admin-users` или из любой группы администраторов получают роль `ADMIN` (бейдж в UI — `ADM`).
- Пользователи из `writer-users` или из любой группы writer получают роль `WRITER` (бейдж `RW`).
- Остальные аутентифицированные пользователи получают роль `READER` (бейдж `RO`).
- Роль вычисляется на сервере, включается в полезную нагрузку токена и сессии, и возвращается через `/api/v1/auth/me`.

### 2.5 Хранилище сессий, токенов и Rate Limiting (SessionStore & Tri-Storage)

Интерфейс `SessionStore` реализует `DefaultSessionStore`. Он хранит:
- Сессии пользователей (таблица `active_sessions`): хэш токена, имя пользователя, сессия в JSON и `expires_at`. Если срок не передан, `expires_at` = текущее время + 480 минут. Просроченная сессия при чтении не возвращается.
- Черный список отозванных токенов (таблица `revoked_tokens`). Два уровня: L1 — кэш Caffeine в памяти (`l1-capacity` записей, запись живет `l1-ttl-seconds`), L2 — JDBC. Проверка отозванного токена сначала идет по L1; при попадании в L2 токен добавляется в L1.
- `cleanupExpired()` удаляет из обеих таблиц записи с истекшим `expires_at`.

L2 включается, только если `session-store.type` не равен `memory` и в контексте есть `DataSource` (`spring.datasource` сервиса). Таблицы создаются при старте (`CREATE TABLE IF NOT EXISTS`). При `type: memory` (значение по умолчанию) сессии и отзывы хранятся в памяти процесса: после перезапуска сервиса пользователю нужно войти заново, а реплики сервиса не видят сессий друг друга. Реализации на Redis в коде нет, хотя комментарий к `type` упоминает `redis`.

```yaml
hadoop:
  security:
    session-store:
      type: jdbc            # memory (по умолчанию) | jdbc
      fail-closed: false    # true: ошибка L2 при проверке отзыва считается отзывом, ошибка записи сессии — исключением
      l1-capacity: 10000
      l1-ttl-seconds: 3600
    rate-limiter:
      enabled: true
      requests-per-minute: 600
      burst-capacity: 100
      login:
        requests-per-minute: 30
        burst-capacity: 10
      trusted-proxies: []     # IP или CIDR обратных прокси, например ["10.0.0.0/8"]
      max-tracked-keys: 100000
```

При `fail-closed: false` ошибка JDBC пишется в лог (`Failed to save session to JDBC, falling back to in-memory`, `JDBC isTokenRevoked query failed`), и сервис продолжает работать с памятью процесса.

Фильтр `RateLimitFilter` ограничивает частоту запросов к `/api/*` (Token Bucket на Bucket4j, корзины в памяти процесса, у каждой реплики свои). Запрос сверх лимита получает HTTP 429, заголовок `Retry-After` (секунды до следующего токена) и тело `{"detail": "...", "status": 429}`. На уровне DEBUG логгера `io.github.balookrd.pristan.zastava.resilience.RateLimiterService` в журнал пишется `Rate limit exceeded: tier=..., key=..., retry_after=...s` — по строке на каждый отклонённый запрос, поэтому по умолчанию (INFO) она не выводится. Веб-интерфейс (`BaseApiClient.request` в `frontend/common/api/client.ts`) при ответе 429 повторяет запрос один раз после паузы из `Retry-After`, если она не больше 5 с; без заголовка или при большей паузе показывает ошибку. Повтор делается для любого метода: 429 выдаётся до обработчика, запрос не выполнялся.

| Параметр `zastava.rate-limiter.*` | По умолчанию | Что делает | Когда менять |
|---|---|---|---|
| `enabled` | `true` | `false` пропускает все запросы | Тестовые стенды и нагрузочные прогоны |
| `requests-per-minute` | `600` | Пополнение корзины API на ключ (пользователь; анонимный запрос — IP) | UI или скрипты получают 429 при штатной работе |
| `burst-capacity` | `100` | Емкость корзины API: столько запросов проходит подряд | То же |
| `login.requests-per-minute` | `30` | Пополнение корзины `POST /api/v1/auth/login` и `GET /api/v1/auth/sso` на IP клиента | Много пользователей за одним NAT без `trusted-proxies`; вход через SPNEGO тратит 2 токена (ответ 401 Negotiate и повторный запрос с билетом) |
| `login.burst-capacity` | `10` | Емкость корзины входа | То же |
| `trusted-proxies` | `[]` | Адреса (IP или CIDR) прокси, от которых учитывается `X-Forwarded-For`: несколько строк заголовка склеиваются по порядку в одну цепочку (HAProxy с `option forwardfor` добавляет свою строку, а не дописывает в клиентскую), клиентом считается крайний справа адрес не из списка. Имя хоста вместо IP останавливает старт сервиса (`IllegalArgumentException: ... Is it a host name?`) | Сервис стоит за ingress или балансировщиком: иначе все клиенты получают IP прокси и одну корзину входа |
| `max-tracked-keys` | `100000` | Предел числа корзин в памяти на уровень; корзина без запросов 10 минут удаляется | Очень много пользователей или IP |

Под Kubernetes (`CloudPlatform.KUBERNETES`) Spring Boot по умолчанию ставит `server.forward-headers-strategy=NATIVE`: `RemoteIpValve` встроенного Tomcat подставляет в `getRemoteAddr()` адрес из `X-Forwarded-For`, если прокси входит в его `internal-proxies` (по умолчанию частные диапазоны 10/8, 172.16/12, 192.168/16, 127/8 и т.п.). Фильтр применяет `trusted-proxies` уже к этому адресу: прокси из `internal-proxies` Tomcat снимает сам, а `trusted-proxies` нужен для прокси вне этих диапазонов. Чтобы Tomcat не трогал `getRemoteAddr()` и разбор шёл только по `trusted-proxies`, задайте `server.forward-headers-strategy=none`.

Фильтр стоит после `springSecurityFilterChain` (порядок -90): запросы, отклонённые цепочкой безопасности (401/403), токены не тратят.

### 2.6 Отказоустойчивость: Circuit Breaker, Retry, Global Exception Handlers, Distributed Lock и Graceful Shutdown

| Механизм | Поведение |
|---|---|
| Circuit Breaker | Считает сетевые ошибки вызовов к кластерам Hadoop/Spark/YARN. После 5 ошибок или таймаутов подряд узел переходит в `OPEN` на 30 секунд: вызовы к нему сразу завершаются ошибкой и не занимают потоки пула. Затем узел переходит в `HALF_OPEN`, и следующий запрос идет как пробный. Клиентские ошибки 4xx не учитываются. Метрики — через Actuator `/actuator/prometheus` (или `/metrics`). |
| Retry с экспоненциальным backoff | Повторяет вызов при ошибках соединения и сокета (`java.net.http.HttpConnectTimeoutException`, `java.io.IOException`), к задержке добавляется случайный джиттер. |
| Глобальный обработчик исключений (`@ControllerAdvice`) | Перехватывает необработанные исключения, возвращает 500 со стандартным телом и `incidentId`, stack trace наружу не отдает (CWE-209). |
| Distributed Lock | Только в оркестраторе репликатора: `DistributedLockService` берет блокировку строкой в таблице БД (`ClusterLockRepository.tryAcquireOrRenew`) с истечением по времени. В стартере безопасности распределенных блокировок нет. |
| Graceful shutdown | По SIGTERM/SIGINT останавливает `ThreadPoolTaskExecutor`, отменяет фоновые задачи и закрывает соединения с базами данных и сетевыми клиентами. |

### 2.7 Сквозное шифрование TLS/HTTPS и mTLS для REST и gRPC каналов

TLS для встроенного веб-сервера и исходящих REST-клиентов сервисов `yarn`, `hdfs`, `sql`, `spark` и `replicator/orchestrator` настраивается в одном месте — в секции `zastava.tls` стартера `zastava`:

```yaml
hadoop:
  security:
    tls:
      enabled: true                     # Активация TLS для встроенного Tomcat веб-сервера
      key-store-path: "/etc/security/tls/keystore.p12" # Путь к PKCS12 / JKS Keystore
      key-store-password: "${TLS_KEYSTORE_PASSWORD}"
      key-store-type: "PKCS12"
      trust-store-path: "/etc/security/tls/truststore.p12" # Хранилище доверенных CA
      trust-store-password: "${TLS_TRUSTSTORE_PASSWORD}"
      trust-store-type: "PKCS12"
      client-auth: "NONE"               # Режим mTLS: NONE | OPTIONAL | REQUIRE
      auto-generate-self-signed: true   # Автогенерация временного Keystore при отсутствии файла
      insecure-skip-verify: false       # Отключение проверки сертификатов для dev-стендов
      enabled-protocols:
        - "TLSv1.3"
        - "TLSv1.2"
      ciphers: []                       # Список допустимых шифронаборов (по умолчанию все безопасные)
```

Те же параметры через переменные окружения:
- `ZASTAVA_TLS_ENABLED=true`
- `ZASTAVA_TLS_KEY_STORE_PATH=/etc/security/tls/keystore.p12`
- `ZASTAVA_TLS_KEY_STORE_PASSWORD=secret`
- `ZASTAVA_TLS_TRUST_STORE_PATH=/etc/security/tls/truststore.p12`
- `ZASTAVA_TLS_CLIENT_AUTH=REQUIRE` (для включения двустороннего mTLS)
- `ZASTAVA_TLS_INSECURE_SKIP_VERIFY=true` (только для тестирования)

---


## 3. Настройка YARN Explorer

Файл конфигурации: `backend/yarn/src/main/resources/application.yml` (или внешний файл `application.yml` / переменные окружения).

### 3.1 YARN-кластеры и партиции

```yaml
yarn:
  clusters:
    - id: "prod-yarn"
      name: "Production Hadoop Cluster"
      description: "Основной YARN-кластер (120 узлов)"
      resource-manager-urls:
        - "http://rm1.prod.company.local:8088"
        - "http://rm2.prod.company.local:8088" # High Availability failover
      kerberos-enabled: true
      kerberos-principal: "yarn/rm1.prod.company.local@COMPANY.LOCAL"
      impersonation-enabled: true
      default-partition: "DEFAULT"
      partitions:
        - "DEFAULT"
        - "GPU"
        - "HIGH_MEM"
      resource-mode: "percentage" # percentage (Capacity Scheduler %) | absolute (MB / Cores)
      total-resources:
        memory-mb: 2097152        # 2 TB
        vcores: 1024
```

### 3.2 Ролевая модель и Change Requests

В YARN Explorer три роли: `ADMIN`, `WRITER`, `READER`. При `enforce-four-eyes: true` действует принцип четырех глаз: автор заявки не может сам ее согласовать.

```yaml
yarn:
  acl:
    enforce-four-eyes: true
  clusters:
    - id: "prod-yarn"
      acl:
        allowed-users: ["*"]
        allowed-groups: ["*"]
        roles:
          admin:
            groups: ["hadoop-admins", "platform-admins"]
            users: ["admin_user"]
          writer:
            groups: ["yarn-operators", "data-engineers"]
            users: ["writer_user"]
          reader:
            groups: ["*"]
            users: ["*"]
```

### 3.3 Интеграция с Ansible AWX (доставка и применение конфигурации)

YARN Explorer доставляет сгенерированную XML-конфигурацию на кластер и применяет ее без перезапуска, запуская Job Template в Ansible AWX / Red Hat Ansible Automation Platform:

```yaml
yarn:
  awx:
    # Глобальные параметры подключения к AWX
    enabled: true                          # Включение интеграции с AWX
    base-url: "https://awx.company.local"  # Базовый URL сервера AWX
    token: "SampleAwxApplicationTokenHere" # Токен приложения AWX (PAT / OAuth2)
    verify-ssl: true                       # Проверка TLS/SSL сертификата сервера
    default-job-template-id: 101           # ID Job Template по умолчанию
    poll-interval-seconds: 2               # Интервал опроса статуса задачи (сек)
    timeout-seconds: 180                   # Таймаут ожидания завершения задачи (сек)

  clusters:
    - id: "prod-yarn"
      name: "Production Hadoop Cluster"
      # ...
      awx:
        enabled: true                    # Включение деплоя для конкретного кластера
        job-template-id: 101             # Индивидуальный ID Job Template кластера
        inventory-id: 5                  # ID Inventory в AWX (необязательно)
```

#### Соответствующие переменные окружения:
| Переменная (Spring Boot Relaxed Binding) | Пример значения | Описание |
|---|---|---|
| `YARN_AWX_ENABLED` | `true` | Активация модуля автоматизации AWX |
| `YARN_AWX_BASE_URL` | `https://awx.company.local` | Базовый URL сервера AWX |
| `YARN_AWX_TOKEN` | `BearerSecretToken...` | Токен доступа к REST API AWX |
| `YARN_AWX_VERIFY_SSL` | `true` | Проверка TLS сертификата сервера AWX |
| `YARN_AWX_DEFAULT_JOB_TEMPLATE_ID` | `101` | Числовой идентификатор Job Template |
| `YARN_AWX_TIMEOUT_SECONDS` | `180` | Таймаут ожидания завершения задачи (сек) |

Развертывание роли Ansible, настройка шаблона AWX и откат: [docs/awx-yarn-deployment.md](awx-yarn-deployment.md).
Применение изменений из интерфейса Change Requests: [docs/yarn-user-guide.md](yarn-user-guide.md).

---

## 4. Настройка HDFS Explorer

Файл конфигурации: `backend/hdfs/src/main/resources/application.yml` (или внешний файл `application.yml` / переменные окружения).

### Пример секции `hadoop.hdfs.clusters`:

```yaml
hadoop:
  hdfs:
    clusters:
      - id: "datalake-prod"
        name: "Production DataLake"
        description: "Основной аналитический кластер HDFS"
        webhdfs-urls:
          - "http://nn1.prod.company.local:9870/webhdfs/v1"
          - "http://nn2.prod.company.local:9870/webhdfs/v1"
        hdfs-rpc-urls:
          - "hdfs://nn1.prod.company.local:8020"
          - "hdfs://nn2.prod.company.local:8020"
        auth-type: "kerberos"       # kerberos | simple
        service-principal: "hdfs/nn1.prod.company.local@COMPANY.LOCAL"
        keytab-path: "/etc/security/keytabs/hdfs-explorer.keytab"
        timeout-seconds: 30
        preview-max-bytes: 10485760 # Лимит чтения файлов для превью (10 MB)
        default-path: "/user/{username}"
        mock-storage: false
        acl:
          allowed-groups:
            - "hadoop-users"
            - "data-engineers"
            - "analytics"
          admin-groups:
            - "hadoop-admins"
            - "domain admins"

      - id: "datalake-archive"
        name: "Cold Storage Archive"
        webhdfs-urls:
          - "http://archive-httpfs.company.local:14000/webhdfs/v1"
        auth-type: "simple"
        default-path: "/archive"
        mock-storage: false
        acl:
          allowed-groups:
            - "domain users"
```

---

## 5. Настройка SQL Explorer (Trino & Hive)

Параметры читает `SqlProperties` (`@ConfigurationProperties(prefix = "sql")`). Базовые значения — в `backend/sql/src/main/resources/application.yml`; переопределяйте их внешним `application.yml` (`SPRING_CONFIG_ADDITIONAL_LOCATION`) или переменными окружения.

**Внимание.** При `mock-storage: false` кластер обслуживает реальный движок: `type: trino` — `TrinoSqlEngine` (`io.trino:trino-jdbc` 484), `type: hive` — `HiveSqlEngine` (`org.apache.kyuubi:kyuubi-hive-jdbc-shaded` 1.12.0, протокол HiveServer2). При `mock-storage: true` или другом `type` кластер обслуживает `MockSqlEngine` с демо-каталогами `tpch` и `analytics`.

### 5.1 Аналитические кластеры

```yaml
sql:
  clusters:
    - id: "trino-prod"
      name: "Trino Production Cluster"
      type: "trino"                 # trino | hive | mock
      host: "trino-coordinator.company.local"
      port: 8443                    # По умолчанию 8080
      catalog: "hive"
      schema: "default"
      mock-storage: false
      impersonation:
        enabled: true
        method: "header"            # header | doAs | session
      allowed-groups: ["bi-analysts", "data-engineers", "data-platform-admins"]
      allowed-users: []

    - id: "hive-prod"
      name: "HiveServer2 Core"
      type: "hive"
      host: "hs2.company.local"
      port: 10000
      catalog: "default"
      schema: "default"
      impersonation:
        enabled: true
        method: "doAs"
      allowed-groups: ["data-engineers", "data-platform-admins"]
```

Доступ к кластеру (`ClusterService`): если `allowed-users` и `allowed-groups` пусты, кластер виден всем аутентифицированным пользователям; иначе — пользователям из `allowed-users` и членам групп из `allowed-groups`. Режима только для чтения (`allow_dml_ddl`) нет: изменяющие команды ограничиваются правами пользователя на кластере.

Подключение к Trino (`mock-storage: false`). URL — `jdbc:trino://<host>:<port>/<catalog>/<schema>`. Сервис входит в Trino как служебный пользователь (свойство `user`), вошедший пользователь передается в свойстве `sessionUser` (заголовок `X-Trino-User`). Trino проверяет правило имперсонации «служебный пользователь → пользователь платформы» в своем контроле доступа (`access-control.properties`, правило `impersonation` в файле правил); без него запрос завершается ошибкой `Access Denied: User <служебный пользователь> cannot impersonate user <пользователь>`.

| Параметр | По умолчанию | Что делает | Когда менять |
|---|---|---|---|
| `mock-storage` | `false` | `true` — кластер обслуживает `MockSqlEngine`, подключения к кластеру нет | Стенды без Trino |
| `impersonation.enabled` | `true` | Передает вошедшего пользователя в `sessionUser` | `false` — все запросы выполняются от служебного пользователя |
| `impersonation.method` | `header` | Только отображается в UI и `/api/v1/clusters`; механизм имперсонации определяется `type` | — |
| `jdbc.ssl` | `false` | TLS к координатору (`SSL=true`) | Координатор слушает HTTPS; обязателен при `kerberos.enabled: true` |
| `jdbc.ssl-verification` | `FULL` | `FULL` — проверка цепочки и имени хоста, `CA` — только цепочки, `NONE` — без проверки | `CA`, если сертификат выпущен не на имя из `host` |
| `jdbc.ssl-truststore-path`, `jdbc.ssl-truststore-password` | — | Хранилище доверенных сертификатов (JKS или PKCS12) | Самоподписанный или корпоративный CA |
| `jdbc.service-user` | короткое имя `kerberos.principal`, иначе `hadoop-explorer` | Служебный пользователь (`user`) | Имя служебной учетки в Trino отличается от principal |
| `jdbc.connect-timeout-seconds` | `30` | Свойство драйвера `timeout`: таймаут установления соединения, чтения и записи каждого HTTP-запроса к координатору | Медленная сеть до координатора |
| `jdbc.query-timeout-seconds` | `1800` | По истечении сервис вызывает `Statement.cancel()`, запрос получает статус `FAILED` с текстом «Превышен лимит времени выполнения запроса» | Длинные отчеты |
| `jdbc.fetch-size` | `1000` | Размер пакета строк из драйвера и SSE-события `rows` | — |
| `kerberos.enabled` | `false` | Вход служебного пользователя по keytab. Требует `jdbc.ssl: true`, `kerberos.principal`, `kerberos.keytab` и непустой `kerberos.service-name`, иначе сервис не стартует | Trino с `http-server.authentication.type=KERBEROS` |
| `kerberos.principal`, `kerberos.keytab` | — | Principal и keytab сервиса; оба обязательны, вход по кэшу билетов процесса не поддерживается | — |
| `kerberos.krb5-conf` | не задан | `KerberosConfigPath` драйвера. Параметр глобальный для процесса: драйвер записывает его в системное свойство `java.security.krb5.conf`. Не задан — действует настройка JVM (`-Djava.security.krb5.conf`, иначе путь JDK по умолчанию) | Задавайте только абсолютным путем, равным `-Djava.security.krb5.conf`, или (только Trino, без Kerberos-кластеров Hive) при незаданном свойстве JVM; у всех кластеров Trino и Hive с Kerberos путь должен быть один. Относительный путь или расхождение — ошибка при старте сервиса |
| `kerberos.service-name` | `trino` | `KerberosRemoteServiceName`: SPN координатора `<service-name>/<host>@<REALM>`. Пустое значение запрещено: без него драйвер не включает SPNEGO | Координатор зарегистрирован под другим SPN |

При `kerberos.enabled: true` задавайте `-Djava.security.krb5.conf=<абсолютный путь>` в параметрах JVM явно: JDK читает `krb5.conf` один раз при первой инициализации Kerberos в процессе и затем использует кэшированную конфигурацию, поэтому путь, который драйвер установит позже, может не подействовать.

Драйвер не обращается к координатору при открытии соединения: первый HTTP-запрос уходит при выполнении запроса, когда отмена и `jdbc.query-timeout-seconds` уже действуют. Запросы каталога (`/api/v1/catalog/...`) выполняются синхронно, без `jdbc.query-timeout-seconds`; их ограничивает только `jdbc.connect-timeout-seconds`.

Подключение к HiveServer2 (`type: hive`, `mock-storage: false`). URL — `jdbc:hive2://<host>:<port>/<schema>`; транспорт `binary`, TLS и режим `http` к HiveServer2 не поддерживаются. Вошедший пользователь передается в `hive.server2.proxy.user`; HiveServer2 разрешает это, если в `core-site.xml` кластера заданы `hadoop.proxyuser.<служебный пользователь>.hosts` и `.groups`. Каталогов в HiveServer2 нет: дерево показывает один корень `default`.

| Параметр | По умолчанию | Что делает | Когда менять |
|---|---|---|---|
| `kerberos.enabled`, `kerberos.principal`, `kerberos.keytab` | `false`, —, — | Вход служебного пользователя по keytab (`kyuubiClientPrincipal`, `kyuubiClientKeytab`). При `kerberos.enabled: true` principal и keytab обязательны: без keytab драйвер вошел бы по кэшу билетов процесса | HiveServer2 с `hive.server2.authentication=KERBEROS` |
| `kerberos.server-principal` | — | Principal HiveServer2 (`principal=`), `_HOST` заменяется на `host` | Обязателен при `kerberos.enabled: true` |
| `kerberos.krb5-conf` | не задан | Драйвер Kyuubi этот путь не принимает и читает конфигурацию Kerberos JVM | Если задан, должен быть абсолютным и равным `-Djava.security.krb5.conf`. Если есть Kerberos-кластер Hive и `kerberos.krb5-conf` задан у любого Kerberos-кластера (в том числе Trino), `-Djava.security.krb5.conf` обязан быть равен этому пути. Иначе сервис не стартует |
| `jdbc.ssl` | `false` | Для `type: hive` не поддерживается | `true` — ошибка при старте сервиса |
| `jdbc.connect-timeout-seconds` | `30` | `connectTimeout` драйвера: таймаут TCP-подключения к HiveServer2 | Медленная сеть до HiveServer2 |
| `jdbc.socket-timeout-seconds` | `300` | `socketTimeout` драйвера: предел ожидания каждого ответа HiveServer2 по Thrift (SASL-рукопожатие, `OpenSession`, опрос статуса, `FetchResults`, запросы каталога). `0` — без ограничения | Медленные `FetchResults` или метаданные на большом Metastore дольше предела |
| `jdbc.service-user` | короткое имя `kerberos.principal`, иначе `hadoop-explorer` | Пользователь подключения при аутентификации `NONE` | — |
| `jdbc.query-timeout-seconds`, `jdbc.fetch-size` | `1800`, `1000` | Как для Trino | — |
| `impersonation.enabled` | `true` | Передает `hive.server2.proxy.user` | `false` — все запросы от служебного пользователя |

Путь к `krb5.conf` драйвер Kyuubi берет из JVM-параметра `-Djava.security.krb5.conf`, а не из `kerberos.krb5-conf`. Вход по keytab драйвер кэширует на процесс (один TGT на пару principal и keytab) и повторяет, когда TGT близок к истечению; при каждом входе JDK перечитывает `krb5.conf` (`refreshKrb5Config=true`). Имя пользователя подставляется в URL, поэтому имена с символами вне `[A-Za-z0-9._@-]` отклоняются ошибкой до подключения.

Подключение к HiveServer2 (SASL-рукопожатие, Kerberos, `OpenSession`) выполняется до запуска запроса, поэтому отмена и `jdbc.query-timeout-seconds` на этом этапе не действуют. `jdbc.connect-timeout-seconds` ограничивает TCP-подключение, `jdbc.socket-timeout-seconds` — каждое ожидание ответа: HiveServer2, который принял соединение и не отвечает, дает ошибку `java.net.SocketTimeoutException: Read timed out` через `jdbc.socket-timeout-seconds`. Длинные запросы этот предел не обрывают: драйвер запускает запрос асинхронно и опрашивает статус, а HiveServer2 отвечает на опрос не позже `hive.server2.long.polling.timeout` (по умолчанию 5 с). Предел срабатывает на одном долгом ответе, например на `FetchResults`, если HiveServer2 вычисляет строки при выборке.

Отмена (`POST /api/v1/queries/{queryId}/cancel`) вызывает `Statement.cancel()`; драйвер Trino отменяет запрос на координаторе. Предел строк `sql.max-rows-per-query` передается в `Statement.setMaxRows`; при превышении результат усекается, сообщение запроса содержит «Результат усечен до N строк».

### 5.2 Параметры выполнения запросов и сохранение воркспейса

```yaml
sql:
  max-rows-per-query: 10000     # Предел строк результата, передается движку
  default-page-size: 500        # Объявлен в SqlProperties, в коде не используется
  results-ttl-hours: 24         # Срок хранения результатов в памяти
```

#### Кэш результатов запросов и ротация по TTL
`QueryManagerService` хранит результаты запросов в памяти процесса (`resultCache`). Раз в час (`@Scheduled(fixedRate = 3600000)`) удаляются результаты старше `results-ttl-hours`. После перезапуска сервиса результаты теряются.

#### Персистентность рабочих пространств (User Workspace)
SQL Explorer сохраняет в БД (`spring.datasource`) открытые вкладки редактора, введенный SQL-код, выбранные кластеры и каталоги и последние результаты (модель `SqlUserWorkspace`, эндпоинт `/api/v1/workspace`). Рабочее пространство у каждого аутентифицированного пользователя свое: после смены пользователя в браузере загружается его контекст, чужая история не показывается. Упакованный `application.yml` использует H2 в памяти (`jdbc:h2:mem:sqlexplorer`): без внешней БД рабочие пространства теряются при перезапуске.

### 5.3 ИИ-ассистент (On-Premise LLM / Ollama / vLLM)

```yaml
sql:
  ai:
    enabled: true
    provider: "openai_compatible"   # mock-builtin | openai_compatible
    url: "http://llm-server.company.local:11434/v1"
    model: "qwen2.5-coder-32b"
    api-key: "${SQL_AI_API_KEY:}"   # ключ берется из переменной окружения
    timeout-seconds: 30
    status-timeout-seconds: 5
    max-concurrent-requests: 8
```

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `provider` | `mock-builtin` | `mock-builtin` — ответы строятся встроенными правилами, LLM не вызывается; `openai_compatible` — запросы `check`, `explain`, `optimize`, `fix`, `generate` идут в `POST {url}/chat/completions` (vLLM, Ollama `/v1`, LM Studio, OpenAI). Любое другое значение работает как `mock-builtin` |
| `url` | `http://localhost:8000/v1` | Базовый URL API, к нему дописываются `/chat/completions` и `/models`. Только `http`/`https`, без учетных данных, query (`?...`) и fragment (`#...`) |
| `model` | `qwen2.5-coder-32b` | Поле `model` запроса |
| `api-key` | пусто | Если задан — заголовок `Authorization: Bearer <api-key>`. Пробелы и переводы строк по краям отбрасываются; управляющие и не-ASCII символы внутри ключа останавливают запуск |
| `timeout-seconds` | `30` | Предельное время всего ответа модели (заголовки и тело) для `check`, `explain`, `optimize`, `fix`, `generate`; подключение — не дольше 10 с. Больше 0 |
| `status-timeout-seconds` | `5` | Предельное время `GET {url}/models` для `/api/v1/ai/status` (не больше `timeout-seconds`). Больше 0 |
| `max-concurrent-requests` | `8` | Сколько вызовов модели на экземпляр сервиса могут ждать ответа одновременно; вызов сверх лимита сразу отвечает правилами (`LLM ... уже выполняется N вызовов` в логе). Больше 0. Увеличивайте, если в логе часто видно это сообщение, а сервер LLM справляется |
| `enabled` | `true` | `false` — LLM не вызывается при любом `provider` |

Упакованный `application.yml` задает `provider: "mock-builtin"`: без явной настройки сервис к LLM не обращается.

При `provider: openai_compatible` сервис проверяет параметры при старте и останавливает запуск с `IllegalArgumentException` (`sql.ai.url ...`, `sql.ai.api-key ...`, `sql.ai.<параметр> должен быть больше 0`), если: URL без хоста, со схемой не `http`/`https`, с учетными данными (`http://user:password@host/...`), query или fragment; в `api-key` есть управляющий или не-ASCII символ; `timeout-seconds`, `status-timeout-seconds` или `max-concurrent-requests` ≤ 0. Пароль из URL, query и значение ключа в сообщение не попадают.

Если модель не ответила за `timeout-seconds`, вернула HTTP-ошибку, ответ больше 1 МиБ или ответ без JSON-объекта с обязательным полем, все `max-concurrent-requests` вызовов заняты, сервис отвечает встроенными правилами: в ответе `provider: mock-builtin`, `model: builtin-rules`, у `check` и `generate` — `fallback_used: true`; в лог пишется `LLM недоступна для <операция>, ответ построен встроенными правилами: <причина>`. По истечении `timeout-seconds` HTTP-обмен с моделью отменяется, поток обработки запроса освобождается. `format` всегда выполняется правилами.

`GET /api/v1/ai/status` проверяет `GET {url}/models` (веб-интерфейс вызывает его при каждом открытии окна ассистента): `available: true` и имя модели — только если LLM отвечает. При `mock-builtin` и при недоступной LLM — `available: false`, `provider: mock-builtin`, `model: builtin-rules`. В `message` — только краткая причина (`HTTP 401`, `таймаут 5 с`, тип исключения вроде `ConnectException`); подробности с текстом ответа сервера — в логе (`Проверка LLM для /api/v1/ai/status не прошла: ...`).

Ключ `api-key`:
- задавайте через переменную окружения (`SQL_AI_API_KEY` в примере выше) или в `application.yml` с правами `600`, не в образе и не в репозитории;
- в журнал и ответы API ключ не попадает: в строках лога `LLM недоступна ...` и `Проверка LLM ...` вхождения ключа и токены `Bearer`/`Basic` заменяются на `***`, текст ответа сервера обрезается до 200 символов; `message` статуса текста сервера не содержит.

Что уходит в LLM. При `openai_compatible` содержимое запросов пользователя передается на сервер из `url`; если это внешний сервис (например, OpenAI), это передача данных за пределы кластера. Каждый запрос содержит системный промпт с JSON-схемой ответа, диалект (`trino`/`hive`) и:

| Операция | Данные пользователя в запросе к модели |
|---|---|
| `check`, `optimize` | Текст SQL; `catalog_context` из тела запроса, если клиент его передал |
| `explain` | Текст SQL |
| `fix` | Текст SQL и текст ошибки движка (может содержать имена таблиц, колонок, пользователей, пути HDFS) |
| `generate` | Описание на естественном языке; `catalog_context`, если клиент его передал |

Веб-интерфейс `catalog_context` не передает; клиент API может передать в нем произвольный JSON, и он уйдет в модель целиком. Имя пользователя, строки таблиц и результаты запросов в модель не отправляются; литералы внутри SQL (значения в `WHERE`, `VALUES`) уходят как часть текста.

Ответ модели показывается в окне ассистента как текст (без интерпретации HTML) и подставляется в редактор только по кнопке пользователя; сервис его не выполняет. Позиции замечаний `check` приводятся к диапазону 1..1 000 000, `severity` — к `error`/`warning`/`info` (иначе `warning`), `category` — к `syntax`/`performance`/`security`/`style` (иначе `style`), `complexity_score` — к 1..10.

Вызовы ассистента идут через `/api/v1/ai/*` и подпадают под общий ограничитель частоты `zastava.rate-limiter.requests-per-minute` (раздел 2.5); отдельного лимита частоты на обращения к LLM нет, одновременность ограничивает `max-concurrent-requests`. Каждый вызов `check`, `explain`, `optimize`, `fix`, `generate` держит поток обработки до ответа модели, но не дольше `timeout-seconds`.

---

## 6. Настройка Spark Explorer (Livy, PySpark, Scala, Metastore)

Параметры читает `SparkProperties` (`@ConfigurationProperties(prefix = "spark")`). Базовые значения — в `backend/spark/src/main/resources/application.yml`.

**Внимание.** При `mock-storage: true` (или `type`, отличном от `livy`) сессии и код обслуживает встроенный `MockSparkEngine`. При `mock-storage: false` Spark Explorer создает интерактивные сессии Apache Livy с `proxyUser` — Livy должен работать с `livy.impersonation.enabled = true`, а его principal должен быть разрешен как `hadoop.proxyuser.<livy>.*` в `core-site.xml` кластера. Значение `mock-storage` по умолчанию — `false`: кластер без этого ключа идет в Livy.

**Внимание.** При `livy.impersonation.enabled = false` Livy не отклоняет запрос с `proxyUser`, а молча его игнорирует: сессия, ее приложение YARN и доступ к HDFS и Hive работают от учетки Livy, права вошедшего пользователя не проверяются. Как увидеть: в ответе Livy `GET /sessions/<livy_session_id>` поле `proxyUser` равно `null`, в ResourceManager владелец приложения (`User`) — учетка Livy, а не пользователь платформы. На демо-стенде `demo/spark` имперсонация включена; ограничения стенда описаны в `demo/spark/README.md`. Откат: остановить сессии, включить `livy.impersonation.enabled = true` в `livy.conf` и перезапустить Livy.

**Внимание.** `proxyUser` изолирует пользователей друг от друга только при `livy.spark.master = yarn` и `livy.spark.deploy-mode = cluster` в `livy.conf`: тогда драйвер каждой сессии запускается в контейнере YARN от имени пользователя. ОС-учетка процессов контейнера совпадает с пользователем только при `yarn.nodemanager.container-executor.class = org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor` на NodeManager; с `DefaultContainerExecutor` контейнеры всех пользователей работают под ОС-учеткой NodeManager (`yarn`) и читают ее keytab. Проверка: `import subprocess; print(subprocess.check_output(["id"]).decode())` в сессии выводит `uid=...(<пользователь>)`; `getpass.getuser()` для проверки не подходит, он берет `$USER`, который YARN выставляет по владельцу приложения. При `livy.spark.master = local[*]` или `livy.spark.deploy-mode = client` драйвер и код пользователя выполняются на хосте Livy под ОС-учеткой `livy`. Код любого пользователя может прочитать keytab Livy, а эта учетка входит в `livy.superusers` и `hadoop.proxyuser.livy.*`, то есть с ее keytab можно имперсонировать любого пользователя в HDFS, Hive и YARN. Как увидеть такой режим: в ответе `curl -s http://<livy_host>:8998/sessions` у сессий `"appId": null` после перехода в `idle`. Для многопользовательского стенда переведите Livy на `yarn` + `cluster`; `spark.master` и `spark.submit.deployMode` в `spark_conf` пользователь переопределить не может (см. таблицу всегда запрещенных префиксов ниже).

### 6.1 Кластеры и интеграция с Apache Livy

```yaml
spark:
  session-ttl-minutes: 120      # Сессии idle без активности дольше этого срока удаляются в Livy и получают статус killed
  results-ttl-hours: 24         # Объявлен в SparkProperties, в коде не используется
  close-sessions-on-logout: true # Выход пользователя закрывает его сессии в Livy
  session-sync-interval-ms: 5000 # Пауза между проходами фоновой синхронизации статусов сессий из Livy (GET /sessions на кластер)
  session-sync-pause-seconds: 30 # Сколько не опрашивать кластер после ошибки Livy при синхронизации
  clusters:
    - id: "prod-hadoop"
      name: "Production Hadoop (Spark & HMS)"
      description: "Кластер с Spark 3.5 и 2.4"
      type: "livy"
      livy-url: "http://livy-prod.company.local:8998"
      yarn-cluster-id: "prod-yarn"
      mock-storage: false
      allowed-groups: ["*"]
      allowed-users: []
      default-repositories: []
```

| Параметр | По умолчанию | Что делает | Когда менять |
|---|---|---|---|
| `livy.auth` | `none` | `none` — без заголовка `Authorization`; `spnego` — `Authorization: Negotiate <токен>` на каждый запрос, SPN `HTTP/<хост из livy-url>`. С другим значением сервис не стартует | Livy с `livy.server.auth.type = kerberos` |
| `livy.principal`, `livy.keytab` | — | Principal и keytab сервиса для `spnego`; `livy.keytab` — абсолютный путь. Путь к `krb5.conf` берется из JVM-параметра `-Djava.security.krb5.conf` | Обязательны при `livy.auth: spnego` |
| `livy.relogin-interval-minutes` | `60` | Через сколько минут сервис заново входит по keytab. Если TGT живет меньше, вход повторяется раньше — на 80 % срока жизни TGT | Политика realm требует более частого входа |
| `livy.proxy-user` | `true` | Передает вошедшего пользователя в `proxyUser` сессии | `false` — сессии от имени учетки Livy |
| `livy.connect-timeout-seconds`, `livy.read-timeout-seconds` | `10`, `60` | Таймауты HTTP к Livy: установка соединения и получение всего ответа на один запрос (заголовки и тело целиком). По истечении обмен отменяется, запрос получает ошибку (HTTP 504 в API, `FAILED` у выполнения). Ответ больше 4 МиБ отбрасывается (HTTP 502) | Медленная сеть |
| `livy.session-start-timeout-seconds` | `300` | Сколько statement ждет перехода сессии из `starting` в `idle` | YARN долго выделяет контейнер драйвера |
| `livy.statement-timeout-seconds` | `3600` | По истечении сервис отменяет statement (`POST .../cancel`) и отмечает выполнение `FAILED` | Длинные задания |
| `livy.poll-interval-ms` | `500` | Период опроса состояния сессии и statement | — |
| `spark.denied-spark-conf-prefixes` | `spark.master`, `spark.submit.deployMode`, `spark.yarn.queue`, `spark.yarn.keytab`, `spark.yarn.principal`, `spark.kerberos.`, `spark.jars.packages` | Префиксы ключей `spark_conf`, которые пользователь не может задать (сравнение без учета регистра); запрос сессии с ними получает HTTP 400 | Расширить список под политику кластера; убрать `spark.jars.packages`, если пользователям разрешено подключать пакеты через `spark_conf` (последствия — как у поля `packages`, см. ниже) |
| `spark.allowed-remote-schemes` | `hdfs`, `viewfs`, `s3a`, `abfs`, `abfss` | Схемы, допустимые в путях `spark.files`, `spark.jars`, `spark.submit.pyFiles`, `spark.archives` и в полях запроса `jars`, `py_files`, `custom_python_archive` (сравнение без учета регистра) | Добавить `http`/`https` только с учетом предупреждения ниже |

При `livy.auth: spnego` сервис входит в KDC по keytab при первом запросе к Livy и использует полученный TGT до следующего входа (см. `livy.relogin-interval-minutes`). Пока один запрос выполняет повторный вход, остальные продолжают работать с прежним TGT, если его срок не истек. Если повторный вход не удался (KDC недоступен, keytab не читается), а прежний TGT еще действует, сервис пишет в лог `WARN Повторный Kerberos-вход <principal> не выполнен, используется прежний TGT ...` и повторяет попытку через 1 минуту (или раньше, если TGT истекает раньше). Ошибку (HTTP 502) запрос получает, только если действующего TGT нет: первый вход или TGT уже истек. Для каждого запроса к Livy строится новый SPNEGO-токен из билета `HTTP/<хост из livy-url>`; сам билет JDK получает у KDC один раз и хранит до следующего входа. Хост берется из `livy-url` в нижнем регистре; JDK 21 заменяет его по DNS только на более длинную форму того же имени (`livy01` → `livy01.company.local`), алиас (CNAME) в имя целевого хоста не превращается. Если в `livy-url` алиас, а principal Livy выписан на имя хоста, KDC не найдет principal. IP-адрес (IPv4 или IPv6) в `livy-url` при `livy.auth: spnego` не принимается при старте. Служебный principal должен входить в `livy.superusers` Livy: иначе Livy отклонит `proxyUser`, отличный от аутентифицированного пользователя [проверить на стенде: текст ответа Livy].

Независимо от `denied-spark-conf-prefixes` в `spark_conf` пользователя всегда запрещены (без учета регистра):

| Префикс | Почему |
|---|---|
| `livy.` | Livy копирует `conf` сессии в настройки своего launcher: `livy.rsc.proxy-user` превратился бы в `--proxy-user` для `spark-submit` |
| `spark.yarn.keytab`, `spark.yarn.principal`, `spark.kerberos.` | Учетные данные учетки Livy |
| `spark.master`, `spark.submit.deployMode` | Режим `local` или `client` запускает код пользователя на хосте Livy от учетки Livy |
| `spark.yarn.queue` | Очередь задает сервис из поля `queue` после проверки по `yarn-queues`; ключ в `spark_conf` обошел бы эту проверку, даже если администратор убрал его из `spark.denied-spark-conf-prefixes` |
| `spark.yarn.dist.`, `spark.yarn.jars`, `spark.yarn.archive` | `spark-submit` работает на хосте Livy от учетки Livy и загружает эти локальные файлы в контейнер пользователя: `"spark.yarn.dist.files": "file:///etc/security/keytabs/livy.service.keytab"` отдал бы keytab Livy |
| `spark.jars.ivy`, `spark.jars.ivySettings`, `spark.jars.repositories` | Ivy работает на хосте Livy от учетки Livy: ключи задали бы локальный файл настроек, каталог кэша или произвольный репозиторий |

Ключи `spark.files`, `spark.jars`, `spark.submit.pyFiles`, `spark.archives` и поля запроса `jars`, `py_files`, `custom_python_archive` принимаются только с путями в схемах из `spark.allowed-remote-schemes`; путь `file:`, `local:`, без схемы или со схемой не из списка получает HTTP 400 (локальный путь указывает на хост Livy). Список разделяется только запятыми: значение через пробел — один некорректный путь, тоже HTTP 400.

**Внимание.** `http` и `https` в `spark.allowed-remote-schemes` по умолчанию не входят. Если их добавить, файл по URL читает `spark-submit` на хосте Livy, а не контейнер YARN: при схеме из `spark.yarn.dist.forceDownloadSchemes` или без класса `FileSystem` для этой схемы в Hadoop файл скачивается при отправке (`SparkSubmit`), а иначе YARN-клиент Spark копирует файл с файловой системы, отличной от staging-каталога, в staging через `HttpFileSystem` Hadoop. В обоих случаях запрос уходит с хоста Livy. Тогда пользователь может отправлять с хоста Livy запросы во внутреннюю сеть (SSRF), доступную этому хосту. Как заметить: обращения от адреса хоста Livy в журналах внутренних HTTP-сервисов. Откат: убрать `http`/`https` из списка и перезапустить сервис.

Поле запроса `packages` (Maven-координаты) передается в `spark.jars.packages`. Пакеты разрешает Ivy внутри `spark-submit` на хосте Livy от учетки Livy, из репозиториев, настроенных на этом хосте: загрузка идет через сеть хоста Livy, кэш (`~/.ivy2` учетки Livy) занимает его диск, а при подмене зеркала репозитория в сессию попадет подмененный jar. Ключ с пробелами или управляющими символами и пустой ключ тоже получают HTTP 400. `spark-conf` версий, окружений и метастора задает администратор, эти значения не проверяются; перекрыть их пользователь может только ключами, не попадающими под запреты. `proxyUser` берется только из имени вошедшего пользователя.

Настройки Livy каждого кластера с `mock-storage: false` и `type: livy` проверяются при старте. Сервис не стартует (`IllegalStateException` в логе), если `livy-url` не задан, не является URL `http(s)://<livy_host>:<port>` или содержит учетные данные (`user:password@`), если `livy.auth` имеет неизвестное значение или таймаут не больше нуля, а при `livy.auth: spnego` — если не задан `livy.principal` или `livy.keytab`, `livy.keytab` не является абсолютным путем, `livy.relogin-interval-minutes` не больше нуля или хост в `livy-url` задан IP-адресом. Сам keytab при старте не читается: ошибка чтения проявится на первом запросе к Livy (HTTP 502). Пример сообщения:

```text
spark.clusters[prod-hadoop].livy-url не задан: кластер с mock-storage: false работает через Apache Livy. Укажите URL Livy (http://<livy_host>:8998) или mock-storage: true
```

`spark-versions[].spark-conf`, `spark-versions[].python-versions[].spark-conf` и `metastores[].spark-conf` задают параметры Spark, которые добавляются в `conf` сессии при выборе соответствующего элемента. Ключи с точками в YAML пишутся в квадратных скобках, иначе Spring Boot разберет их как вложенные:

```yaml
      spark-versions:
        - id: "spark-3.5.1"
          spark-conf:
            "[spark.yarn.archive]": "hdfs:///apps/spark/spark-3.5.1-jars.zip"
          python-versions:
            - id: "py310"
              spark-conf:
                "[spark.yarn.dist.archives]": "hdfs:///apps/python/py310.tar.gz#py310"
                "[spark.pyspark.python]": "./py310/bin/python"
```

Ключей `use_ssl` и `impersonation` в `SparkProperties` нет. `allowed-groups` и `allowed-users` объявлены, но код сервиса их не читает: доступ к кластеру по ним не ограничивается.

### 6.2 Версии Spark и среды Python (HDFS Archives)

Пользователь выбирает версию Spark и окружение Python из списка в конфигурации. Пути к архивам Spark и интерпретаторам Python задаются через `spark-conf` версии и окружения (раздел 6.1).

```yaml
spark:
  clusters:
    - id: "prod-hadoop"
      spark-versions:
        - id: "spark-3.5.1"
          name: "Apache Spark 3.5.1"
          default-version: true
          python-versions:
            - id: "py310"
              name: "Python 3.10 (DataScience Stack)"
              default-env: true
            - id: "py311"
              name: "Python 3.11 (Minimal)"
              default-env: false
```

#### Пользовательские (Ad-hoc) виртуальные окружения Python
В окне параметров сессии пользователь может указать собственный архив окружения:

| Параметр запроса | Что делает |
|---|---|
| `custom_python_archive` | HDFS-путь к архиву окружения (`hdfs:///.../*.tar.gz#environment`) |
| `custom_python_path` | Путь к интерпретатору внутри распакованного архива |

Оба значения сохраняются в записи сессии (`SparkSessionRecord`) и возвращаются в API; при `mock-storage: false` архив передается в `archives` сессии Livy, путь — в `spark.pyspark.python`.

### 6.3 Интеграция с YARN и разграничение очередей

```yaml
spark:
  clusters:
    - id: "prod-hadoop"
      yarn-cluster-id: "prod-yarn"
      yarn-queues: ["root.analytics", "root.adhoc", "root.etl"]
      default-queue: "root.analytics"
```

Список `yarn-queues` задает очереди, доступные для выбора в UI. Для кластера Livy (`mock-storage: false`) очередь из запроса (`yarn_queue`) должна входить в `yarn-queues`, иначе создание сессии получает HTTP 400 `Очередь YARN <queue> не входит в список очередей кластера <id>: ...`; пустой `yarn-queues` разрешает любую очередь. Если очередь в запросе пустая, в Livy передается `default-queue`. Права на очередь дополнительно проверяет YARN для пользователя из `proxyUser`. ACL на уровне отдельных очередей (`queue_acl`) и адресов ResourceManager в `SparkProperties` нет.

### 6.4 Hive Metastore, Iceberg и профили ресурсов

```yaml
spark:
  clusters:
    - id: "prod-hadoop"
      metastores:
        - id: "lakehouse-core"
          name: "Lakehouse Core Metastore (HMS)"
          default-metastore: true
      resource-profiles:
        - id: "small"
          name: "Small (Driver 2G/1c, 2 Exec 4G/2c)"
          driver-memory: "2g"
          driver-cores: 1
          executor-memory: "4g"
          executor-cores: 2
          num-executors: 2
        - id: "medium"
          name: "Medium (Driver 4G/2c, 4 Exec 8G/2c)"
          driver-memory: "4g"
          driver-cores: 2
          executor-memory: "8g"
          executor-cores: 2
          num-executors: 4
```

У метастора есть `id`, `name`, `default-metastore` и `spark-conf` (например, `"[spark.hadoop.hive.metastore.uris]": "thrift://<hms_host>:9083"`), отдельного ключа для URI Thrift нет. `resource-profiles` — список, а не словарь.

### 6.5 Персистентность рабочих пространств пользователя

Spark Explorer сохраняет в базу данных состояние сессий, активные вкладки, буферы кода (`pyspark`, `scalaspark`, `sql`) и буферы результатов (`SparkUserWorkspace`, `/api/v1/workspace`):
- У каждого языка (PySpark, Scala, SQL) свой буфер результатов: переключение языка не затирает результаты другого.
- При входе другого пользователя загружается его рабочее пространство.

---

## 7. Настройка Hadoop gRPC Replicator (DC-DC WAN Sync & Throttling)

Сервис межкластерной репликации `backend/replicator` (Java 21 LTS, Spring Boot 3, gRPC) копирует данные HDFS между ЦОД по WAN с ограничением полосы. Конфигурация задается в `application.yml` или переменными окружения.

### 7.1 Архитектура и компоненты (Orchestrator, Agent)

Сервис состоит из двух компонентов:
1. Replicator Orchestrator (Java 21 / Spring Boot 3, порт `8005`):
   - хранит задачи репликации (`Job` / `JobRun`) и их состояние в реляционной БД (H2 / PostgreSQL);
   - отдает REST API и Web UI (Svelte 5 SPA);
   - выдает агентам сетевые квоты через многоуровневый Token Bucket;
   - запускает периодические задачи по расписанию (Cron Scheduler);
   - экспортирует метрики Prometheus (`/actuator/prometheus`).
2. Replicator Agent (Java 21, gRPC, порт `50051`):
   - разворачивается в каждом ЦОД (например, DC1 и DC2) и работает в обе стороны;
   - Receiver принимает данные по gRPC streaming (HTTP/2), сверяет SHA-256 и атомарно фиксирует файл в HDFS; Sender опрашивает задачи в статусе `QUEUED`, запрашивает токены у оркестратора и передает данные потоком;
   - работает с HDFS через `HadoopFsManager`: Kerberos UGI и doAs-имперсонация, чтобы политики Apache Ranger применялись к пользователю задачи.

---

### 7.1.1 Безопасные значения по умолчанию и продуктивный профиль

Конфигурация оркестратора по умолчанию (`application.yml`) — H2 в памяти и боевая аутентификация
(`auth.mode: ldap`, JWT-секрет из `JWT_SECRET_KEY`): без них оркестратор не стартует.
Тестовые пользователи и `auth.mode: mock` вынесены в профиль `mock` (`SPRING_PROFILES_ACTIVE=mock`)
для локальной разработки. Демо-стенды `demo/replicator` включают профиль `demo`: вход через OpenLDAP стенда
(`ldap://ldap:389`), группы — поиском `groupOfNames` по `member`. Схема базы во всех профилях
создается миграциями Flyway, а `ddl-auto` стоит в `validate`. Для продуктива
предусмотрен отдельный профиль `application-prod.yml`, активируемый через
`SPRING_PROFILES_ACTIVE=prod`: PostgreSQL, `auth.mode: kerberos`,
`cookie.secure: true`, `health.show-details: when-authorized` и обязательный TLS для gRPC
Control Plane. Все секреты в этом профиле читаются только из окружения
(`REPLICATOR_AGENT_SECRET`, `JWT_SECRET_KEY`, `POSTGRES_USER`, `POSTGRES_PASSWORD`).

Защитные параметры и их значения по умолчанию:

| Параметр | Значение по умолчанию | Назначение |
| :--- | :--- | :--- |
| `hadoop.replicator.agent-secret` | не задан (только из `REPLICATOR_AGENT_SECRET`) | Зашитый в код секрет был единым публично известным ключом для всех установок |
| `hadoop.replicator.enforce-agent-secret` | `true` | Запуск прерывается, если секрет не задан, короче 16 символов или равен прежнему значению из репозитория |
| `hadoop.replicator.enforce-cluster-whitelist` | `true` | Агент регистрируется только в кластере из топологии |
| `REPLICATOR_ENFORCE_TASK_ATTESTATION` (агент) | `true` | Приемник требует HMAC-токен аттестации задачи, выдаваемый оркестратором при заборе задач |

> [!IMPORTANT]
> gRPC-серверы (Control Plane оркестратора, HDFS Receiver и приемник Kafka-батчей агента)
> работают в режиме fail-closed: интерцептор аутентификации устанавливается всегда, и при
> незаданном `x-agent-secret` все входящие вызовы отклоняются со статусом `UNAUTHENTICATED`.
> Поэтому пустой секрет не «открывает» стенд, а полностью останавливает репликацию.
> Демо-стенд (`demo/replicator`) задает общий секрет через переменную `DEMO_AGENT_SECRET`.

> [!WARNING]
> При включенном TLS (`grpc.tls-enabled: true`) недоступный или некорректный файл доверенных
> сертификатов прерывает запуск клиента control plane. Молчаливого отката в plaintext нет:
> канал несет заголовок `x-agent-secret`.

Агенты работают только через gRPC Control Plane: отчеты о прогрессе (`ReportHmsProgress`),
выдача HMS-задач (`GetPendingHmsJobs`) и продление аренды стримера (`RenewStreamingLease`)
проходят через интерцептор `x-agent-secret`. Прежние REST-дублеры этих вызовов
(`POST /api/v1/hms/jobs/{id}/progress`, `GET /api/v1/hms/jobs/{id}/pending`,
`POST /api/v1/streaming/lease/renew`) удалены вслед за `POST /api/v1/agents/register`
и отвечают `404`.

REST API остается интерфейсом Web UI и операторов. Детали HMS-задач (`/{id}`, `/{id}/events`,
`/{id}/subtasks`, `/{id}/dlq`) и список задач видны администратору и владельцу задачи:
непривилегированный пользователь не может перечислить структуру чужой репликации по ее
идентификатору.

Контейнеры оркестратора и агента работают от непривилегированного пользователя `appuser`
(UID 10001).

---

### 7.1.2 Трассировка и health-пробы gRPC

Операция репликации проходит через несколько процессов: агент забирает задачу у оркестратора,
агент-отправитель передает данные агенту-приемнику, схемы Hive и батчи Kafka идут своими
вызовами. Чтобы участки собирались в одну трассу, все вызовы gRPC проходят через перехватчики
трассировки: клиент кладет в метаданные W3C `traceparent`, сервер поднимает его и продолжает тот
же trace, а `trace_id` и `span_id` выставляются в MDC — по логам разных процессов собирается одна
операция.

Трассировка включена по умолчанию, но экспорт спанов в коллектор включается отдельно: пока
адрес не задан, экспортер не создается и спаны никуда не уходят (корреляция в логах и
`traceparent` в вызовах при этом работают).

| Переменная | Компонент | Назначение |
| :--- | :--- | :--- |
| `MANAGEMENT_OTLP_TRACING_ENDPOINT` | Оркестратор | Адрес OTLP-коллектора, например `http://otel-collector:4318/v1/traces`. Не задан — экспорта нет |
| `TRACING_SAMPLING_PROBABILITY` | Оркестратор | Доля трассируемых запросов. По умолчанию `1.0`, в профиле `prod` — `0.1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Агент, Kafka-демон | Адрес коллектора (суффикс `/v1/traces` добавляется сам). Не задан — трассировка выключена |
| `OTEL_TRACES_SAMPLER_ARG` | Агент, Kafka-демон | Доля трассируемых вызовов, по умолчанию `0.05`: на пути передачи данных вызовов на порядки больше, чем в control plane |

Серверы gRPC — Control Plane оркестратора, приемник агента и приемник Kafka-батчей — отвечают по
стандартному протоколу `grpc.health.v1.Health`. Статус выставляется и в целом по серверу, и
отдельно по подсистемам (`replicator.DataTransferService`,
`replicator.kafka.KafkaDataTransferService`), а при остановке переводится в терминальное
состояние до закрытия порта, чтобы балансировщик и отправители успели увести трафик.

Health-проба освобождена от аутентификации по `x-agent-secret`: пробы балансировщика, kubelet и
docker секрета агента не знают, а наблюдают они только `SERVING`/`NOT_SERVING`. Остальные вызовы
остаются закрытыми — исключение действует строго на методы `grpc.health.v1.Health/*`.

Удержание истории пофайловых задач настраивается отдельно: таблица подзадач пополняется записью
на каждый файл, и без очистки она растет без границ.

| Параметр | По умолчанию | Назначение |
| :--- | :--- | :--- |
| `hadoop.replicator.task-retention-days` | `7` | Возраст, после которого завершенная задача удаляется. `0` выключает очистку |
| `hadoop.replicator.task-retention-batch-size` | `500` | Размер пакета удаления: одна транзакция на пакет, а не на все окно |
| `hadoop.replicator.task-retention-max-batches-per-run` | `20` | Сколько пакетов удаляется за проход (по умолчанию до 10 000 записей) |
| `hadoop.replicator.task-cleanup-interval-ms` | `3600000` | Интервал запуска очистки |

Удаляются только терминальные статусы (`COMPLETED`, `FAILED`, `SKIPPED`): задача в `RUNNING`
может прямо сейчас передаваться агентом, а зависшую подберет failover. В active-active очистку
выполняет один узел под распределенной блокировкой `task_retention_lock`.

Проба контейнера агента выполняется тем же jar-ом, что и сам агент, поэтому `grpc_health_probe` в
образ ставить не нужно:

```bash
java -cp /app/replicator-agent.jar \
    io.github.balookrd.pristan.replicator.telemetry.GrpcHealthProbeMain 127.0.0.1:50051
```

Код возврата `0` — `SERVING`, `1` — любое другое состояние или недоступность. Третьим аргументом
задается таймаут в секундах, вторым — имя подсистемы.

---

### 7.2 Топология дата-центров и кластеров

Топология описывает ЦОД и привязку кластеров к ним (`dc-id`). Задается в `hadoop.replicator.datacenters` и `hadoop.replicator.clusters` (`ReplicatorProperties`).

На демо-стенде два ЦОД (`dc1`, `dc2`) и по одному кластеру в каждом.

```yaml
hadoop:
  replicator:
    datacenters:
      - id: "dc1"
        name: "ЦОД 1 (Primary DC)"
        network-zone: "zone-a"
      - id: "dc2"
        name: "ЦОД 2 (Disaster Recovery)"
        network-zone: "zone-b"
    clusters:
      - id: "prod-cluster"
        name: "HDFS Primary"
        dc-id: "dc1"
        hdfs-rpc-address: "hdfs://namenode-dc1:8020"
        grpc-host: "agent-dc1"        # Адрес агента-приемника для этого кластера
        grpc-port: 50051
        kerberos-enabled: true
        principal: "replicator@COMPANY.LOCAL"
        keytab-path: "/etc/security/keytabs/replicator.keytab"
      - id: "backup-cluster"
        name: "HDFS DR"
        dc-id: "dc2"
        hdfs-rpc-address: "hdfs://namenode-dc2:8020"
        grpc-host: "agent-dc2"
        grpc-port: 50051
```

Для Kafka-репликации у кластера задаются `kafka-bootstrap-servers`, `schema-registry-url`, `schema-registry-type` (`CONFLUENT` | `HORTONWORKS`) и при TLS — `kafka-security-protocol`, `kafka-service-name`, `kafka-truststore-location`, `kafka-truststore-password`, `kafka-truststore-type` (пример — в `backend/replicator/orchestrator/src/main/resources/application.yml`).

---

### 7.3 Многоуровневые лимиты полосы (DC-DC и HDFS-HDFS)

Чтобы репликация не забивала каналы WAN, оркестратор ограничивает полосу иерархическим Token Bucket с тремя уровнями:
1. Глобальный лимит — верхняя граница суммарной скорости передачи по всей платформе. Начальное значение — `hadoop.replicator.global-limit-bytes-per-sec` (по умолчанию `104857600`, 100 МБ/с).
2. Лимиты между ЦОД — емкость магистрального канала между площадками. При старте для каждой пары ЦОД из `datacenters` ставится 100 МБ/с.
3. Лимиты между парами кластеров — квота полосы под конкретное направление репликации. При старте для каждой пары кластеров из `clusters` ставится 60 МБ/с.

Лимиты между ЦОД и между кластерами в `application.yml` не задаются (`TopologyRegistry.resetLimitsToDefaults`). Значение `0.0` снимает ограничение (в UI — «Без лимита»).

Пользователь с ролью `ADMIN` меняет любой лимит без перезапуска через Web UI или API. Тело запроса принимает `limit_bytes_per_sec` или `limit_mb_per_sec`:

```bash
curl -X POST http://<orchestrator_host>:8005/api/v1/limits/dc-dc \
  -H 'Content-Type: application/json' -H 'X-Requested-With: XMLHttpRequest' -b cookies.txt \
  -d '{"source_dc": "dc1", "target_dc": "dc2", "limit_mb_per_sec": 100}'
```

`<orchestrator_host>` — адрес оркестратора, `cookies.txt` — cookie сессии после входа. Аналогично: `POST /api/v1/limits/global` (`limit_mb_per_sec`) и `POST /api/v1/limits/hdfs-hdfs` (`source_cluster`, `target_cluster`, `limit_mb_per_sec`). Новое значение действует для всех воркеров в течение 1 секунды.

---

### 7.4 Безопасность, RBAC и системная техучетка Kerberos

Роли:
- `ADMIN` (`admin_user`): видит и изменяет задачи всех пользователей, меняет лимиты полосы, отменяет любые задачи.
- `WRITER` / `READER` (`de_user`, `analyst_user`): видят только свои задачи и системные задачи техучетки. Запрос на изменение лимитов возвращает `HTTP 403 Forbidden`.

Системная техучетка. Задача с флагом `run_as_service_account: true` выполняется от системного принципала (`hdfs-replicator@REALM.LOCAL`). Флаг нужен для периодических репликаций по расписанию, которые запускаются без участия пользователя.

Изоляция Kerberos-кэша. Для каждой операции воркер создает отдельный credential cache `KRB5CCNAME=/tmp/krb5cc_repl_{uuid}` и удаляет его после завершения передачи, так что билеты одной задачи не попадают в другую.

---

### 7.5 Настройка Replicator Agent (Full-Duplex) и буфера Staging

Агент репликации `backend.replicator.agent` объединяет в одном процессе gRPC-сервер приема (`Receiver`) и фоновый воркер передачи (`Sender`), поэтому один агент реплицирует в обе стороны (`DC1 ⇄ DC2`).

При старте агент регистрируется на оркестраторе: объявляет свой gRPC-адрес (`advertised_grpc_address`) и кластер, который обслуживает, а затем периодически шлет heartbeat. Адреса агентов в `application.yml` прописывать не нужно.

Агент настраивается следующими переменными окружения:

```bash
# Идентификатор агента и обслуживаемый HDFS-кластер
AGENT_ID=agent-dc1
AGENT_CLUSTER_ID=demo-cluster

# Режим работы: 'all' (полный дуплекс), 'sender' (только передача), 'receiver' (только прием)
AGENT_MODE=all

# URL Оркестратора для опроса очередей, получения сетевых токенов и регистрации
ORCHESTRATOR_URL=http://orchestrator:8005

# Адрес и порт входящего gRPC сервера (прием файлов от удаленных ЦОД)
RECEIVER_HOST=0.0.0.0
RECEIVER_PORT=50051

# Объявляемый сетевой адрес для вызовов от удаленных агентов (по умолчанию $HOSTNAME:$RECEIVER_PORT)
# AGENT_ADVERTISED_ADDRESS=agent-dc1:50051

# Включение динамической регистрации и интервал keepalive (секунды)
AGENT_ENABLE_REGISTRATION=true
AGENT_HEARTBEAT_INTERVAL_SEC=5.0

# Локальная буферная директория для временного приема чанков перед коммитом
REPLICATOR_STAGING_DIR=/tmp/staging

# Интервал опроса очереди задач в Оркестраторе (в секундах)
POLL_INTERVAL_SEC=2.0

# Настройки защищенного TLS / mTLS канала для gRPC
REPLICATOR_GRPC_TLS_ENABLED=true
REPLICATOR_GRPC_CERT_CHAIN_PATH=/etc/security/tls/agent-cert.pem
REPLICATOR_GRPC_PRIVATE_KEY_PATH=/etc/security/tls/agent-key.pem
REPLICATOR_GRPC_TRUST_CERT_COLLECTION_PATH=/etc/security/tls/ca-chain.pem
REPLICATOR_GRPC_CLIENT_AUTH=REQUIRE # NONE | OPTIONAL | REQUIRE (mTLS)
REPLICATOR_GRPC_INSECURE_SKIP_VERIFY=false # true для тестовых сред
ORCHESTRATOR_TLS_INSECURE_SKIP_VERIFY=false
```

#### Регистрация и heartbeat агента

Вызовы идут через gRPC Control Plane оркестратора (`orchestrator_control_plane.proto`); REST-эндпоинт `POST /api/v1/agents/register` удален (см. раздел 7.1.1).
1. Регистрация при старте (`RegisterAgent`). Агент передает `agent_id`, `cluster_id`, `advertised_grpc_address` и версию. Оркестратор переводит агента в статус `ONLINE` и связывает его с кластером.
2. Heartbeat (`Heartbeat`). Каждые 5 секунд (`AGENT_HEARTBEAT_INTERVAL_SEC`) агент сообщает число текущих передач (`active_transfers`).
3. Обнаружение сбоя. Если heartbeat нет 15 секунд (`hadoop.replicator.agent-heartbeat-timeout-seconds`), агент переходит в `STALE` и новые передачи на него не направляются; через 45 секунд (`agent-offline-timeout-seconds`) — в `OFFLINE`.
4. Плановая остановка (`UnregisterAgent`). Агент сообщает оркестратору о завершении, статус меняется на `OFFLINE`.
5. Балансировка. Если в кластере зарегистрировано несколько агентов, оркестратор направляет новую передачу агенту с наименьшим `active_transfers`.

#### Защита регистрации

- Общий секрет агентов. Задается в `REPLICATOR_AGENT_SECRET` (не короче 16 символов, проверка в `AgentRegistry`). Агент передает его в заголовке `x-agent-secret`; вызов без секрета или с неверным секретом отклоняется со статусом `UNAUTHENTICATED`. В профиле `prod` секрет обязателен.
- Белый список кластеров. При `HADOOP_REPLICATOR_ENFORCE_CLUSTER_WHITELIST=true` (значение по умолчанию) агент регистрируется только в кластере из топологии. Регистрация в чужом кластере завершается статусом gRPC `INTERNAL` с сообщением о политике `enforce-cluster-whitelist`.
- Защита от SSRF. Оркестратор отклоняет `advertised_grpc_address` из диапазонов link-local (`169.254.0.0/16`, `fe80::/10`) и адреса облачных метаданных (`169.254.169.254`, `metadata.google.internal`).

#### Атомарная фиксация файла

1. Передающий агент режет файл на чанки размером `CHUNK_SIZE_BYTES` (по умолчанию 64 КБ) или объединяет мелкие файлы в виртуальный TAR-поток.
2. Принимающий агент пишет чанки во временный файл прямо в целевом каталоге HDFS (`targetPath + "._staging_" + jobId` или `taskId`), без промежуточного локального диска.
3. После окончания потока агент сверяет контрольную сумму SHA-256 всего файла.
4. При совпадении временный файл переименовывается в целевой путь в том же каталоге (`HadoopFsManager.renameFile` / `FileSystem.rename`); переименование в HDFS атомарно.
5. При сетевой ошибке или расхождении хеша временный файл удаляется с целевой HDFS: сразу в `cleanup()` или позже фоновой очисткой по TTL (Reaper).

---

### 7.6 Разрешение целевых адресов (Service Discovery, AGENT_TARGET_* и Fallback)

Агент-отправитель выбирает gRPC-адрес агента-приемника в удаленном ЦОД в таком порядке:

1. Реестр активных агентов оркестратора (`AgentRegistry`) — штатный режим.
   - Оркестратор хранит список зарегистрированных агентов и их статусы.
   - Метод `AgentRegistry.getLeastLoadedAgentForCluster(clusterId)` выбирает среди живых агентов кластера в режиме `all` или `receiver` агента с наименьшим числом текущих передач (`activeTransfers`). Живым считается агент, чей последний heartbeat моложе `hadoop.replicator.agent-offline-timeout-seconds` (по умолчанию 45 с).
   - Адрес кластера из топологии агент перезаписать не может, если в `hadoop.replicator.clusters` для него задан `grpc-host`, отличный от `localhost` (`TopologyRegistry.updateClusterGrpcAddress`).
2. Статическая топология (`GET /api/v1/clusters`).
   - В `hadoop.replicator.clusters` у кластера задаются `grpc-host` и `grpc-port` (например, `grpc-host: agent-dc2`, `grpc-port: 50051`).
   - Используется, если регистрация отключена (`AGENT_ENABLE_REGISTRATION=false`) или агент-приемник не на связи.
3. Переменная `AGENT_TARGET_<CLUSTER_ID>` на агенте-отправителе.
   - Нужна при NAT, маршрутизации через ingress/mesh или других портах в DMZ.
   - Пример: `AGENT_TARGET_BACKUP_CLUSTER=agent-dc2:50051` меняет адрес только для кластера `backup-cluster`.
4. Резервный адрес `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS` (по умолчанию `localhost:50051`).
   - Используется, если целевой кластер не указан в задаче или не найден ни в одном реестре оркестратора.

---

### 7.7 Планировщик периодических задач (Cron Scheduler)

Планировщик `ReplicationScheduler` (пакет `io.github.balookrd.pristan.replicator.orchestrator.scheduler`) встроен в оркестратор:
- раз в 5 секунд (`@Scheduled(fixedDelay = 5000)`) выбирает задачи с `is_scheduled = true` и `next_run_at <= now`; при наличии `DistributedLockService` тик выполняется под блокировкой `cron_scheduler_lock`, поэтому в нескольких репликах оркестратора задачу запускает одна;
- пропускает задачи в статусе `RUNNING`;
- для остальных:
  1. ставит задачу в очередь (`status = QUEUED`) и создает запись запуска `JobRun` с `trigger_type = SCHEDULED` и `triggered_by = scheduler`;
  2. записывает `last_run_at = now`;
  3. вычисляет `next_run_at` через `CronSchedule` в поясе `hadoop.replicator.scheduler.time-zone`;
  4. удаляет старые записи `JobRun` сверх `history_retention_runs` задачи.

`CronSchedule` разбирает выражение двумя способами: интервальные макросы отсчитываются от момента запуска планировщиком, календарные считает Spring `CronExpression` в часовом поясе `hadoop.replicator.scheduler.time-zone` (по умолчанию `UTC`, переменная `HADOOP_REPLICATOR_SCHEDULER_TIME_ZONE`). Неизвестный пояс останавливает старт оркестратора: пояс разбирается в конструкторах `ReplicationScheduler` и `ScheduleReconciler`, исключение `java.time.DateTimeException` (для неизвестного IANA-id — его подкласс `java.time.zone.ZoneRulesException`) приходит в журнал обернутым в `BeanCreationException` (`Error creating bean with name ...`), причина — в цепочке `Caused by`. Фактический запуск — до 5 секунд позже расчетного (период тика).

| Выражение | Когда запускается |
|---|---|
| `@every_<N>s`, `@every_<N>m`, `@every_<N>h` (UI: `@every_5m`, `@every_15m`) | Через N секунд/минут/часов после предыдущего запуска; меньше 5 секунд поднимается до 5 |
| `every_<N>s` | Устаревшая форма `@every_<N>s` |
| `@minutely` | В начале каждой минуты |
| `@hourly` | В `hh:00` |
| `@daily`, `@midnight` | В 00:00 |
| `@weekly` | В воскресенье в 00:00 |
| `@monthly` | 1-го числа в 00:00 |
| `@yearly`, `@annually` | 1 января в 00:00 |
| 5 полей: `минута час день-месяца месяц день-недели` | По календарю: `0 0 * * *` — в 00:00, `*/10 * * * *` — в 00, 10, 20… минут, `30 2 * * 1-5` — в 02:30 по будням |

Выражение из 6 полей, неизвестный макрос, значение вне диапазона (`61 * * * *`) и выражение, которое никогда не срабатывает (`0 0 30 2 *`), невалидны. Если заданы и день месяца, и день недели, должны совпасть оба: `0 0 13 * 5` — только пятница 13-е (в Vixie cron — любое 13-е число и любая пятница).

Если у задачи с `is_scheduled = true` выражение не разбирается или больше не срабатывает, планировщик не запускает её: ставит `is_scheduled = false`, переводит статус `SCHEDULED` в `STOPPED`, записывает в `message` текст `Расписание отключено: …` и пишет в лог WARN `Schedule disabled for job`. Остальные задачи тика обрабатываются как обычно. Чтобы вернуть расписание, исправьте выражение и включите его заново (`PUT` задачи с `is_scheduled: true`): если статус `STOPPED` выставил планировщик (сообщение начинается с `Расписание отключено`), задача возвращается в статус `SCHEDULED`, `message` очищается. Задача, остановленная оператором, остается в `STOPPED`.

При старте оркестратора `ScheduleReconciler` (под блокировкой `cron_scheduler_lock`) сверяет все задачи с `is_scheduled = true`:
- невалидное выражение или выражение, которое больше не срабатывает, — расписание снимается: `is_scheduled = false`, статус `SCHEDULED` меняется на `STOPPED`, в `message` — «Расписание отключено: » с текстом ошибки, в журнале `Schedule disabled for job '<id>': ...`. Исправьте выражение в задаче и включите расписание;
- календарное выражение с `next_run_at` в будущем — `next_run_at` пересчитывается (записи до перехода на `CronExpression` хранят время «предыдущий запуск + интервал»);
- задача без `next_run_at` получает ближайшее срабатывание;
- просроченный `next_run_at` не меняется: задачу запустит ближайший тик.

Итог пишется в журнал: `Schedule reconciliation updated N job(s)`.

Если блокировку `cron_scheduler_lock` в момент старта держит другой узел оркестратора, сверка на этом узле не выполняется и не повторяется, в журнал пишется INFO `Schedule reconciliation skipped: lock 'cron_scheduler_lock' is held by another node`. Сверку на этом старте заменяет обычная работа тика: задачи с наступившим `next_run_at` он запускает или снимает с расписания при невалидном выражении; календарный `next_run_at` в будущем остается прежним до ближайшего запуска, после которого тик считает следующий через `CronSchedule`; задачу без `next_run_at` тик не выбирает, ее спланирует сверка при следующем старте, на котором узел получит блокировку.

---

### 7.8 Локальное ограничение пропускной способности агента (`AGENT_MAX_BANDWIDTH_MB_S`) и модель согласования WAN

Если Replicator Agent стоит на узлах Hadoop (рядом с DataNode) или на общих edge-узлах, ограничьте его полосу локально, чтобы репликация не отнимала сеть и диски у DataNode:

| Параметр | По умолчанию | Что делает |
|---|---|---|
| `AGENT_MAX_BANDWIDTH_MB_S` / `--max-bandwidth-mb-s` (`-b`) | `0` (без ограничения) | Предел полосы одного процесса агента, МБ/с |

Как лимит агента сочетается с лимитами WAN:
- Агент передает свой лимит при регистрации и в каждом heartbeat (`configuredBandwidthMbS`). Физическую сеть хостов (4x25G, бонды, L3-маршруты) оркестратор не моделирует; доступная полоса репликации считается как $\min(\text{wanQuotaMbS}, \sum \text{agentConfiguredBandwidthMbS})$.
- Межброкерная репликация Kafka (ISR) идет внутри ЦОД по выделенным интерфейсам кластера (например, 2x25G) и в расчет межЦОДного шейпинга не входит.
- Для потоковой репликации Kafka оркестратор рассчитывает число потоков вычитки как $\text{targetTotalThreads} = \max(64, \lceil \text{bandwidthMbS} / 30.0 \rceil)$, но не больше 32 потоков на одну JVM агента: больше потоков упираются в число сокетов и конкуренцию за CPU. Размер микробатча — от 10 000 до 50 000 сообщений (`batch.max.bytes` до 16 МБ).
- Алгоритм — Token Bucket (`LocalBandwidthLimiter`, пакет `io.github.balookrd.pristan.replicator.shaper`) с burst-буфером 0,5 с. Метод `throttle(bytes)` блокирует вызывающий поток (`Thread.sleep`) на время, нужное для накопления токенов; `reserve(bytes)` только списывает токены и возвращает время ожидания (используется потоком опроса Kafka).
- Ограничение работает с двух сторон:
  1. Sender (`ReplicationSender`, `TarStreamPipeline`) вызывает `bandwidthLimiter.throttle(wireBytes)` перед отправкой каждого чанка в gRPC;
  2. Receiver (`DataTransferServiceImpl`) вызывает `throttle` при приеме чанков в `transferFile` и `transferTarStream`; окно TCP и flow control HTTP/2 при этом замедляют удаленного отправителя.
- В UI показываются «Суммарный лимит подключенных агентов» и «Лимит процесса агента»; прежнего потолка отображения в 5000 МБ/с нет.

---

### 7.9 Эталонный фрагмент конфигурации топологии (application.yml)

Эталоном является секция `hadoop.replicator` в `backend/replicator/orchestrator/src/main/resources/application.yml`; ключи топологии описаны в разделе 7.2, лимиты — в разделе 7.3. Продуктивные переопределения (PostgreSQL, TLS gRPC) — в `application-prod.yml` того же каталога.

---

## 8. Переменные окружения

Переменные окружения сервисов (задаются в окружении процесса или контейнера):

| Переменная | Описание | Значение по умолчанию |
|---|---|---|
| `SPRING_CONFIG_ADDITIONAL_LOCATION` | Дополнительный путь к конфигурационному файлу Spring Boot (`application.yml`) | — |
| `HADOOP_REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC` | Глобальный лимит скорости репликации (байт/сек) | `104857600` (100 МБ/с) |
| `SPRING_DATASOURCE_URL` | Строка подключения JDBC к БД Оркестратора (H2/PostgreSQL) | `jdbc:h2:mem:replicator` |
| `HADOOP_REPLICATOR_AGENT_SECRET` / `REPLICATOR_AGENT_SECRET` | Общий секрет аутентификации агентов (`x-agent-secret` в метаданных gRPC), не короче 16 символов | Не задан; без него оркестратор не стартует при `enforce-agent-secret: true` |
| `HADOOP_REPLICATOR_ENFORCE_AGENT_SECRET` | Запрет запуска оркестратора без секрета агентов (fail-closed) | `true` (`ReplicatorProperties`) |
| `REPLICATOR_METRICS_SCRAPE_TOKEN` (`hadoop.replicator.metrics-scrape-token`) | Пароль HTTP Basic для сборщика метрик: `prometheus:<token>` на `GET /metrics`; на остальные пути не действует | Пусто: Basic-доступ выключен, `/metrics` доступен только аутентифицированному пользователю |
| `HADOOP_REPLICATOR_ENFORCE_TASK_ATTESTATION` | Включение криптографической аттестации задач HMAC-SHA256 | `true` |
| `HADOOP_REPLICATOR_ALLOW_LOOPBACK_AGENTS` | Разрешение регистрации агентов на loopback-адресах (`127.0.0.1`, `localhost`) | `false` |
| `HADOOP_REPLICATOR_PROGRESS_DEBOUNCE_MS` | Интервал дебаунсинга буфера фиксации прогресса задач в БД (мс) | `500` |
| `HADOOP_REPLICATOR_PROGRESS_BUFFER_SIZE` | Максимальный размер буфера завершенных задач до принудительного flush | `1000` |
| `REPLICATOR_LOCAL_SANDBOX` / `LOCAL_SANDBOX_DIR` | Корневой каталог Chroot-песочницы локальных файловых операций (SEC-10) | `/var/lib/hadoop-replicator/sandbox` |
| `REPLICATOR_SR_MUTUAL_AUTH` | Включение взаимной аутентификации SPNEGO для Schema Registry | `false` |
| `HADOOP_REPLICATOR_ENFORCE_CLUSTER_WHITELIST` | Запрет регистрации агентов в кластерах, которых нет в топологии | `true` (`application.yml`) |
| `AGENT_ID` | Уникальный идентификатор агента репликации | `agent-<uuid>` |
| `AGENT_CLUSTER_ID` | Идентификатор HDFS-кластера, обслуживаемого агентом | `null` |
| `AGENT_MODE` | Режим работы Replicator Agent (`all`, `sender`, `receiver`, `streamer`) | `all` |
| `AGENT_ENABLE_REGISTRATION` | Включение динамической авторегистрации агента на Оркестраторе | `true` |
| `AGENT_ADVERTISED_ADDRESS` | Сетевой gRPC адрес, анонсируемый агентом удаленным узлам | `$HOSTNAME:$RECEIVER_PORT` |
| `POLL_INTERVAL_SEC` | Интервал опроса очереди задач в Оркестраторе и отправки heartbeat (секунды) | `3.0` |
| `AGENT_MAX_BANDWIDTH_MB_S` | Локальный лимит скорости агента (Token Bucket), МБ/с | `0.0` (без ограничений) |
| `AGENT_TARGET_<CLUSTER_ID>` | Ручной оверрайд сетевого gRPC-адреса для конкретного целевого кластера (для NAT/DMZ) | Берется из топологии Оркестратора |
| `FALLBACK_TARGET_ADDRESS` / `RECEIVER_ADDRESS` | Резервный gRPC-адрес назначения (fallback при отсутствии кластера в топологии) | `localhost:50051` |
| `RECEIVER_HOST` / `RECEIVER_PORT` | Адрес и порт входящего gRPC-сервера Replicator Agent | `0.0.0.0:50051` |
| `REPLICATOR_STAGING_DIR` | Буферная директория Staging для локального коммита (для HDFS используется прямой Zero-Staging) | `/tmp/staging` |
| `REPLICATOR_SMALL_FILE_THRESHOLD_BYTES` | Порог размера файла (в байтах) для бандлинга мелких файлов в виртуальные TAR-архивы | `1048576` (1 МБ) |
| `REPLICATOR_BUNDLE_TARGET_SIZE_BYTES` | Целевой объем файлов в одном виртуальном TAR-бандле `BUNDLE_TAR` | `16777216` (16 МБ) |
| `REPLICATOR_MAX_BUNDLE_FILES` | Максимальное количество файлов в одном виртуальном бандле | `500` |
| `REPLICATOR_BUNDLE_COMMIT_CONCURRENCY` | Количество параллельных потоков прямой записи файлов бандла в целевой HDFS | `8` |
| `REPLICATOR_STAGING_CLEANUP_ENABLED` | Включение фонового сборщика мусора (Reaper) для очистки осиротевших staging-файлов на HDFS | `true` |
| `REPLICATOR_STAGING_CLEANUP_INTERVAL_MINUTES` | Интервал запуска периодической очистки staging-файлов (минуты) | `15` |
| `REPLICATOR_STAGING_TTL_MINUTES` | Время жизни (TTL) staging-файлов, после которого они удаляются (минуты) | `30` |
| `REPLICATOR_WIRE_COMPRESSION` | Кодек потокового сжатия в канале: `zstd` (Zstandard), `lz4` (LZ4), `none` | `zstd` |
| `REPLICATOR_WIRE_COMPRESSION_LEVEL` | Уровень сжатия для Zstandard (1–22) | `3` |
| `ORCHESTRATOR_URL` | Адрес Orchestrator для агентов/воркеров | `http://localhost:8005` |
| `SPRING_PROFILES_ACTIVE` | Профили Spring Boot (`mock` для локальной разработки, `demo` для демо-стендов и `prod` для оркестратора репликатора) | — |
| `HADOOP_AUTH_MODE` | `zastava.auth.mode` в сервисах `yarn`, `hdfs`, `sql`, `spark` | `ldap` |
| `JWT_SECRET_KEY` | `zastava.jwt.secret-key`: ключ подписи JWT, не короче 32 символов | — (без него сервис не стартует вне mock-режима) |
| `LDAP_URL` | `zastava.ldap.url` | `ldaps://ldap.company.local:636` |
| `LDAP_MANAGER_DN` / `LDAP_MANAGER_PASSWORD` | Учетная запись для поиска пользователей в LDAP | — |
| `LDAP_USER_SEARCH_BASE` | `zastava.ldap.user-search-base` | `ou=users,dc=company,dc=local` |
| `YARN_DB_URL` / `YARN_DB_DRIVER` / `YARN_DB_USER` / `YARN_DB_PASSWORD` | БД YARN Explorer (Change Requests, аудит) | `jdbc:h2:file:./data/yarn_explorer` / пусто (драйвер по URL) / `sa` / пусто |
| `YARN_KERBEROS_PRINCIPAL` / `YARN_KERBEROS_KEYTAB` | Учетная запись YARN Explorer для SPNEGO-запросов к ResourceManager | — |
| `YARN_AWX_BASE_URL` / `YARN_AWX_TOKEN` / `YARN_AWX_JOB_TEMPLATE_ID` | Подключение к AWX (раздел 3.3) | — / — / `101` |

Любой другой ключ задается переменной по правилам relaxed binding (раздел 1), например `SQL_RESULTS_TTL_HOURS=48` или `ZASTAVA_COOKIE_SECURE=true`.

---

## 9. Мониторинг Prometheus и дашборды Grafana

### 9.1 Эндпоинты метрик и доступ

| Сервис | Эндпоинт | Порт в контейнере | Доступ | Содержимое |
|---|---|---|---|---|
| YARN Explorer | `GET /actuator/prometheus` | `8000` | без аутентификации | Micrometer, `yarn_change_requests_total` |
| HDFS Explorer | `GET /actuator/prometheus` | `8000` | без аутентификации | Micrometer, `hdfs_*` |
| SQL Explorer | `GET /actuator/prometheus` | `8003` | без аутентификации | Micrometer, `sql_query_duration_seconds` |
| Spark Explorer | `GET /actuator/prometheus` | `8004` | без аутентификации | Micrometer, `spark_*` |
| Оркестратор Replicator | `GET /actuator/prometheus` | `8005` | без аутентификации | только Micrometer |
| Оркестратор Replicator | `GET /metrics` | `8005` | пользователь платформы или HTTP Basic `prometheus:<scrape_token>` | Micrometer и `replicator_*`, `kafka_*` |

`<scrape_token>` — значение `REPLICATOR_METRICS_SCRAPE_TOKEN` (`hadoop.replicator.metrics-scrape-token`) оркестратора, например вывод `openssl rand -hex 32`; учетка действует только на `GET /metrics`, пустое значение выключает Basic-доступ.

Порты задает `SERVER_PORT` в `docker/Dockerfile.<service>-java`. `GET /metrics` у YARN, HDFS, SQL и Spark Explorer возвращает 404.

Доступ к Actuator в YARN, HDFS, SQL и Spark Explorer задает `ZastavaAutoConfiguration.securityFilterChain` (`zastava`): `/actuator/health`, `/actuator/health/**`, `/actuator/info` и `/actuator/prometheus` открыты, остальные пути `/actuator/**` требуют роли ADMIN (без токена — 401, с ролью READER или WRITER — 403). Так же устроен оркестратор (`ReplicatorOrchestratorApplication`).

Внимание: `/actuator/prometheus` отдается без аутентификации. Метрики не содержат имен пользователей, путей HDFS и текстов запросов, но раскрывают идентификаторы кластеров, шаблоны URI API и объем трафика. Если сервис опубликован наружу через обратный прокси или балансировщик, закройте на нем этот путь или разрешите его только адресу Prometheus. Пример для nginx:

```nginx
location = /actuator/prometheus {
    allow <prometheus_ip>;
    deny all;
    proxy_pass http://<upstream>;
}
```

`<prometheus_ip>` — адрес, с которого Prometheus опрашивает сервис; без строки `allow` путь закрыт для всех. `<upstream>` — имя `upstream`-блока или адрес сервиса, на который проксируются остальные `location`; без `proxy_pass` разрешенный адрес получит ответ nginx, а не метрики. Проверка: `curl -s -o /dev/null -w '%{http_code}' https://<public_host>/actuator/prometheus` с внешнего адреса возвращает `403`, где `<public_host>` — внешнее имя сервиса.

Настройки в `src/main/resources/application.yml` каждого сервиса:

| Параметр | Значение | Что делает | Когда менять |
|---|---|---|---|
| `management.endpoints.web.exposure.include` | `health,prometheus` | Публикует по HTTP только health и Prometheus | Не расширять без необходимости: остальные эндпоинты открываются только ADMIN, но `env` и `configprops` показывают конфигурацию |
| `management.prometheus.metrics.export.enabled` | `true` | Создает `PrometheusMeterRegistry`. Задан явно: `@SpringBootTest` по умолчанию отключает экспорт метрик | Не менять |
| `management.metrics.tags.app` | `yarn`, `hdfs`, `sql`, `spark` | Лейбл `app` у всех метрик сервиса; по нему фильтрует переменная `$app` дашборда `pristan-overview` и группируют алерты | Не менять; для нескольких инсталляций в одном Prometheus добавьте лейбл через `relabel_configs` |
| `management.metrics.distribution.slo.http.server.requests` | `50ms,100ms,250ms,500ms,1s,2s,5s,10s,30s` | Границы бакетов `http_server_requests_seconds_bucket` | Если p95 API выходит за 30 с или нужна точность лучше 50 мс. Каждая граница добавляет серию на каждую комбинацию `uri`/`method`/`status` |

### 9.2 Экспортируемые метрики

YARN, HDFS, SQL и Spark Explorer:

| Метрика | Тип | Лейблы | Что считает |
|---|---|---|---|
| `http_server_requests_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app`, `method`, `uri`, `status`, `outcome`, `exception`, `le` | Входящие HTTP-запросы; `uri` — шаблон маршрута Spring MVC, для неизвестных путей `UNKNOWN` |
| `http_server_requests_active_seconds` (`_count`, `_sum`, `_max`) | LongTaskTimer (в Prometheus — `summary`) | `app`, `method`; `uri`, `status`, `outcome`, `exception` фиксируются в начале запроса (`uri="UNKNOWN"`) и смысла не несут | Запросы в обработке на момент опроса: `_count` — их число, включая сам запрос к `/actuator/prometheus`; `_sum` и `_max` — сколько секунд они уже выполняются |
| `hadoop_rate_limit_blocks_total` | Counter | `app`, `tier` | Запросы `/api/*`, отклоненные `RateLimitFilter` ответом 429. `tier`: `login` (пути `/api/v1/auth/login` и `/api/v1/auth/sso`) или `api` (остальные пути `/api/*`). Серия появляется после первого отказа на уровне. Алерты: `HadoopLoginRateLimitSpike` (`login`, больше 0,2 отказа/с за 5 мин) и `HadoopApiRateLimitSpike` (`api`, больше 1 отказа/с) |
| `hdfs_operations_total` | Counter | `app`, `cluster`, `operation`, `status` | Вызовы клиента HDFS (`list_status`, `get_file_status`, `open`, `create`, `mkdirs`, `rename`, `delete`), `status`: `success`, `failed` |
| `hdfs_bytes_transferred_total` | Counter | `app`, `cluster`, `direction` | Байты, прочитанные из HDFS (`download`) и записанные в HDFS (`upload`) |
| `yarn_change_requests_total` | Counter | `app`, `cluster`, `status` | Переходы заявок Change Request: `SUBMITTED`, `APPROVED`, `REJECTED`, `CANCELLED` |
| `sql_query_duration_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app`, `cluster`, `engine`, `status`, `le` | Время SQL-запроса до конечного статуса `FINISHED`, `CANCELLED`, `FAILED` |
| `spark_statement_duration_seconds` (`_count`, `_sum`, `_max`, `_bucket`) | Timer | `app`, `cluster`, `language`, `status`, `le` | Время Spark-инструкции до конечного статуса |
| `spark_sessions_active` | Gauge | `app`, `cluster`, `kind` | Сессии не в статусах `dead`/`killed`, обновление раз в 30 с. Значение берется из общей БД: при нескольких экземплярах каждый отдает полное число, поэтому агрегируйте через `max by (cluster, kind)`, а не `sum` |

Плюс стандартные метрики Spring Boot Actuator (`jvm_*`, `process_*`, `system_*` и др.). Подробности по каждому сервису — в разделе 4.1 руководств администратора: [YARN](yarn-admin-guide.md), [HDFS](hdfs-admin-guide.md), [SQL](sql-admin-guide.md), [Spark](spark-admin-guide.md).

Метрик `hadoop_circuit_breaker_*`, `hadoop_retry_attempts_total`, `hadoop_auth_attempts_total` и `hadoop_exceptions_total` нет: `SimpleCircuitBreaker` из `zastava` сервисы не вызывают, повторы, попытки входа и исключения отдельными счетчиками не считаются. Входы и ответы 5xx считаются по `http_server_requests_seconds_count` (см. 9.6).

Оркестратор Replicator (`GET /metrics`):

| Метрика | Тип | Лейблы | Описание |
|---|---|---|---|
| `replicator_bytes_total` | Counter | `status` | Объем переданных байтов репликации (`IN_PROGRESS`, `COMPLETED`, `FAILED`) |
| `replicator_jobs_total` | Gauge | `status`, `mode` | Количество задач репликации HDFS по статусам и типу запуска. Ряды `SCHEDULED` и `MANUAL` несут лейбл `mode` и пересекаются со статусами: по статусам суммируйте `{mode=""}`, по режимам — `{mode!=""}` |
| `replicator_tasks_total` | Gauge | `status` | Количество пофайловых задач в распределенном пуле |
| `replicator_active_workers` | Gauge | — | Число активных онлайн-воркеров репликации |
| `replicator_active_agents` | Gauge | `status` | Число доступных агентов репликации (`online`, `total`) |
| `replicator_throttling_delay_seconds_total` | Counter | — | Суммарная задержка шейпинга сетевого канала (Token Bucket) |
| `replicator_file_size_bytes` | Histogram | `le` | Распределение размеров файлов репликации |
| `replicator_hms_jobs_total` | Gauge | `status` | Количество задач репликации Hive Metastore |
| `replicator_hms_tables_total` | Gauge | `type` | Количество таблиц HMS (`total`, `replicated`) |
| `replicator_hms_partitions_total` | Gauge | `type` | Количество партиций HMS (`total`, `replicated`) |
| `replicator_hms_speed_bytes_per_second` | Gauge | — | Скорость первичной синхронизации HMS |
| `replicator_dr_cluster_fenced` | Gauge | `cluster_id` | Статус изоляции кластера (1 = Fenced / Kill-Switch активирован, 0 = Active) |
| `replicator_dr_reverse_jobs_total` | Gauge | `status` | Количество задач обратной репликации Failover |
| `replicator_agent_status` | Gauge | `agent_id`, `cluster_id`, `mode`, `status` | Доступность агента: `1` для `status="online"`, `0` для `offline` и `stale` |
| `replicator_agent_active_transfers` | Gauge | `agent_id`, `cluster_id`, `mode` | Текущее количество параллельных передач на агенте |
| `replicator_agent_transferred_bytes_total` | Counter | `agent_id`, `cluster_id` | Суммарный объем байтов, переданных агентом |
| `replicator_agent_errors_total` | Counter | `agent_id`, `cluster_id` | Количество ошибок передачи данных агента |
| `replicator_agent_heartbeat_age_seconds` | Gauge | `agent_id`, `cluster_id` | Задержка keepalive heartbeat агента |
| `replicator_agent_max_bandwidth_mb_s` | Gauge | `agent_id`, `cluster_id` | Выделенный локальный лимит полосы HTB агента в МБ/с |

### 9.3 Настройка сбора метрик в Prometheus

```yaml
scrape_configs:
  - job_name: 'pristan'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets:
          - 'yarn-explorer:8000'
          - 'hdfs-explorer:8000'
          - 'sql-explorer:8003'
          - 'spark-explorer:8004'
```

Готовые конфигурации: [`monitoring/prometheus/prometheus.yml`](../monitoring/prometheus/prometheus.yml) и [`demo/monitoring/prometheus/prometheus.yml`](../demo/monitoring/prometheus/prometheus.yml), правила алертов — в `alert.rules.yml` рядом.

Проверка: в Prometheus UI (**Status** → **Targets**) у заданий `hadoop-explorer` статус `UP`; запрос `up{job="pristan"}` возвращает `1` для каждой цели.

Задание `hadoop-replicator` опрашивает `GET /metrics` оркестратора с HTTP Basic: пользователь `prometheus`, пароль — `REPLICATOR_METRICS_SCRAPE_TOKEN` оркестратора. В демо-стенде одно и то же значение задано в `demo/replicator/docker-compose.yml` и `demo/replicator/docker-compose.kafka.yml` (переменная `DEMO_METRICS_SCRAPE_TOKEN` его переопределяет) и в `demo/monitoring/prometheus/prometheus.yml`; при замене меняйте значение во всех трех файлах, а если задаете `DEMO_METRICS_SCRAPE_TOKEN`, впишите то же значение в `demo/monitoring/prometheus/prometheus.yml`. `monitoring/prometheus/prometheus.yml` читает пароль из файла `/etc/prometheus/secrets/replicator-metrics-scrape-token`: смонтируйте его в контейнер Prometheus. При неверном или незаданном токене цель в Prometheus `DOWN` с `server returned HTTP status 401`.

### 9.4 Сбор метрик в Kubernetes

Пример `ServiceMonitor` (Prometheus Operator) для одного сервиса:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: hdfs-explorer
  namespace: hadoop-explorer
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: hdfs-explorer
  endpoints:
    - port: http
      path: /actuator/prometheus
      interval: 30s
```

`selector.matchLabels` должен совпадать с лейблами вашего Service, а `port` — с именем его порта, который ведет на порт приложения (`8000` для HDFS Explorer). Если в неймспейсе действует NetworkPolicy, разрешите входящие соединения на этот порт из неймспейса Prometheus, иначе цель будет в статусе `DOWN` с ошибкой `context deadline exceeded`.

### 9.5 Готовые дашборды Grafana

В каталоге [`monitoring/grafana/dashboards/`](../monitoring/grafana/dashboards/):

1. [`pristan_overview.json`](../monitoring/grafana/dashboards/pristan_overview.json) — RPS, доля 5xx, p50/p90/p95/p99, запросы в обработке по сервисам, топ-5 медленных URI, входы по кодам ответа, ответы 429 ограничителя частоты по уровням, ответы 5xx по исключению (фильтр `$app`), сводка Replicator.
2. [`hdfs_explorer_operations.json`](../monitoring/grafana/dashboards/hdfs_explorer_operations.json) — темп и доля ошибок операций HDFS, трафик upload/download по кластерам.
3. [`yarn_explorer_queues.json`](../monitoring/grafana/dashboards/yarn_explorer_queues.json) — переходы заявок Change Request по статусам, p95 API YARN Explorer по URI.
4. [`spark_sql_explorer_analytics.json`](../monitoring/grafana/dashboards/spark_sql_explorer_analytics.json) — активные сессии Spark, p95 Spark-инструкций, темп и p95 SQL-запросов по движкам.
5. Дашборды Replicator: [`hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json), [`hadoop_replicator_agents.json`](../monitoring/grafana/dashboards/hadoop_replicator_agents.json), [`hadoop_replicator_hms_dr.json`](../monitoring/grafana/dashboards/hadoop_replicator_hms_dr.json), [`hadoop_replicator_kafka.json`](../monitoring/grafana/dashboards/hadoop_replicator_kafka.json).

#### Импорт дашбордов в Grafana
1. В веб-интерфейсе Grafana перейдите в **Dashboards** → **New** → **Import**.
2. Загрузите нужный JSON-файл из каталога `monitoring/grafana/dashboards/`.
3. Выберите источник данных Prometheus и нажмите **Import**.

Чтобы Grafana загружала дашборды при старте (provisioning), скопируйте файл [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml) в `/etc/grafana/provisioning/dashboards/`.

### 9.6 Примеры PromQL-запросов

Опрос Prometheus сам попадает в `http_server_requests_seconds` с `uri="/actuator/prometheus"` (раз в `scrape_interval` на каждую цель) и на малом трафике занижает долю 5xx и p95 и завышает RPS, поэтому запросы ниже, панели дашбордов и алерты `HadoopHigh5xxErrorRate` и `HadoopHighApiLatency` исключают его фильтром `uri!~"/actuator/.*"`.

- RPS по сервисам:
  ```promql
  sum(rate(http_server_requests_seconds_count{uri!~"/actuator/.*"}[1m])) by (app)
  ```
- Доля ответов 5xx, %:
  ```promql
  sum(rate(http_server_requests_seconds_count{status=~"5..", uri!~"/actuator/.*"}[5m])) by (app) / sum(rate(http_server_requests_seconds_count{uri!~"/actuator/.*"}[5m])) by (app) * 100
  ```
- p95 задержки API:
  ```promql
  histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket{uri!~"/actuator/.*"}[5m])) by (le, app))
  ```
- Неуспешные входы:
  ```promql
  sum(rate(http_server_requests_seconds_count{uri="/api/v1/auth/login", status="401"}[5m])) by (app)
  ```
- Ответы 429 ограничителя частоты по уровням:
  ```promql
  sum by (app, tier) (rate(hadoop_rate_limit_blocks_total[5m]))
  ```
- Активные сессии Spark по кластерам:
  ```promql
  max by (cluster) (sum by (instance, cluster) (spark_sessions_active))
  ```
- Доля ошибок операций HDFS по кластерам:
  ```promql
  sum(rate(hdfs_operations_total{status="failed"}[5m])) by (cluster) / sum(rate(hdfs_operations_total[5m])) by (cluster)
  ```

---

## 10. Руководства администратора по развертыванию (DevOps Hub)

Руководства по развертыванию каждого компонента в режимах Standalone (systemd) и Docker / Docker Compose:

- [Руководство администратора платформы (admin-guide.md)](admin-guide.md)
- [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)


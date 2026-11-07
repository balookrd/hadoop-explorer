# Руководство по конфигурации Hadoop Explorer Platform

Документ для администраторов платформы Hadoop Explorer: параметры конфигурации, форматы файлов и переменные окружения компонентов.
- Общий стартер безопасности (`backend/common-security-starter` — Java 21 / Spring Boot 3, SPNEGO, LDAP/Active Directory, JWT, L1/L2 сессии, Circuit Breaker, Rate Limiter, CSRF Guard).
- YARN Explorer (`backend/yarn` — Java 21 LTS / Spring Boot 3, YARN RM HA, Capacity Scheduler, партиции, Change Requests, Four-Eyes Principle, генерация XML, Ansible AWX).
- HDFS Explorer (`backend/hdfs` — Java 21 LTS / Spring Boot 3, подключение к HDFS Client, HA NameNode, Kerberos, doAs имперсонация, квоты, превью Parquet/ORC, Circuit Breaker).
- SQL Explorer (`backend/sql` — Java 21 LTS / Spring Boot 3, Trino DB API, Apache Hive / HiveServer2, AI-ассистент, история и кэширование).
- Spark Explorer (`backend/spark` — Java 21 LTS / Spring Boot 3, Apache Livy, PySpark, Scala, DAG Pipelines, Metastore, Circuit Breaker).
- Hadoop gRPC Replicator (`backend/replicator` — Java 21 / Spring Boot 3 Orchestrator, нативный Java gRPC Worker Daemon, топология ЦОД, Hierarchical Token Bucket, Kerberos, Snapshot Diff, Cron Scheduler).
- Развертывание в Kubernetes (Helm).

---

## Содержание
1. [Общие принципы и приоритеты конфигурации](#1-общие-принципы-и-приоритеты-конфигурации)
2. [Общий стартер безопасности (common-security-starter)](#2-общий-стартер-безопасности-common-security-starter)
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
9. [Конфигурация в Kubernetes (Helm)](#9-конфигурация-в-kubernetes-helm)
10. [Мониторинг Prometheus и дашборды Grafana](#10-мониторинг-prometheus-и-дашборды-grafana)
11. [Руководства администратора по развертыванию (DevOps Hub)](#11-руководства-администратора-по-развертыванию-devops-hub)

---

## 1. Общие принципы и приоритеты конфигурации

Сервисы — приложения Spring Boot 3 и загружают конфигурацию в стандартном порядке Spring Boot (по убыванию приоритета):
1. Переменные окружения. Ключ получается по правилам relaxed binding: `hadoop.security.jwt.secret-key` → `HADOOP_SECURITY_JWT_SECRET_KEY`. Часть ключей в упакованном `application.yml` дополнительно ссылается на короткие переменные (`JWT_SECRET_KEY`, `HADOOP_AUTH_MODE`, `LDAP_URL` и т. п.).
2. Внешний файл `application.yml`, указанный в `SPRING_CONFIG_ADDITIONAL_LOCATION` (или `--spring.config.additional-location`).
3. `application.yml` (и `application-<profile>.yml` для профилей из `SPRING_PROFILES_ACTIVE`), упакованные в JAR из `backend/<service>/src/main/resources/`.

Переменные `CONFIG_PATH`, `HDFS_CONFIG_PATH`, `YARN_CONFIG_PATH`, `SQL_CONFIG_PATH`, `SPARK_CONFIG_PATH` и файлы `config/config.yaml` Java-сервисы не читают. Исключение — HDFS Explorer: список кластеров можно вынести в отдельный YAML и указать путь в `hadoop.hdfs.clusters-config-path`.

> [!IMPORTANT]
> Значения по умолчанию и проверки на старте (все Java-сервисы).
> Упакованный в JAR `application.yml` каждого сервиса рассчитан на продакшен: `hadoop.security.debug: false`,
> `auth.mode: ${HADOOP_AUTH_MODE:ldap}`, JWT-секрет только из `JWT_SECRET_KEY`. Стартер безопасности
> не даст сервису запуститься, если:
> - `auth.mode` равен `mock` без `hadoop.security.debug: true`. В mock-режиме демо-учетки входят без проверки
>   в каталоге, а для `admin`/`writer` подходит любой пароль;
> - `hadoop.security.jwt.secret-key` пуст, короче 32 символов или совпадает со встроенным значением из исходников;
> - `auth.mode: ldap` задан при `ldap.enabled: false`;
> - `auth.mode` не входит в `mock | ldap | kerberos | hybrid`.
>
> Локальная разработка и демо-стенды включают профиль `mock` явно (`SPRING_PROFILES_ACTIVE=mock`,
> файл `application-mock.yml` в ресурсах сервиса) либо задают `auth.mode`/`debug` в своем конфиге.
> Ключи из старых руководств (`auth-mode`, `ldap.server-uri`, `bind-dn`, `bind-password`, `user-base-dn`,
> `user-filter`, `kerberos.keytab-path`, `kerberos.principal`) принимаются как синонимы.

---

## 2. Общий стартер безопасности (common-security-starter)

Параметры раздела читает `CommonSecurityProperties` (`@ConfigurationProperties(prefix = "hadoop.security")`) из `backend/common-security-starter`. Стартер подключают сервисы `yarn`, `hdfs`, `sql`, `spark` и `replicator/orchestrator`, поэтому ключи одинаковы для всех.

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
      names: ["access_token", "hadoop_explorer_session", "session_token", "hdfs_explorer_session"]
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

Отдельной настройки заголовков в коде нет: `SecurityFilterChain` из `CommonSecurityAutoConfiguration` (и `replicatorSecurityFilterChain` в оркестраторе репликатора) не вызывает `http.headers(...)`, поэтому в ответы попадают заголовки Spring Security по умолчанию:
- `X-Content-Type-Options: nosniff`: браузер не подменяет MIME-тип ответа.
- `X-Frame-Options: DENY`: запрет встраивания в iframe (clickjacking).
- `Cache-Control: no-cache, no-store, max-age=0, must-revalidate`, `Pragma: no-cache`, `Expires: 0`.
- `X-XSS-Protection: 0`.
- `Strict-Transport-Security: max-age=31536000 ; includeSubDomains`: только в ответах на запросы по HTTPS.

`Content-Security-Policy` и `Referrer-Policy` сервисы не выставляют. Если они нужны, добавьте их на обратном прокси (ingress).

### 2.2 Аутентификация: LDAPS / Active Directory

`LdapAuthService` из стартера безопасности проверяет пароль по каталогу LDAP (OpenLDAP, FreeIPA, Microsoft Active Directory) через JNDI:
1. Подключается к `hadoop.security.ldap.url` от имени `manager-dn` (если `manager-dn` пуст — анонимно).
2. Ищет пользователя в `user-search-base` (если не задан — в `base-dn`) по фильтру `user-search-filter`, подставляя имя вместо `{0}`. Перед подстановкой символы `\`, `*`, `(`, `)` и NUL экранируются (защита от LDAP injection).
3. Выполняет bind от DN найденного пользователя с введенным паролем.
4. Берет группы из атрибута `memberOf` (значение `CN` из DN группы) и по ним вычисляет роль (раздел 2.4).

Ключи `group-search-base` и `group-search-filter` есть в `CommonSecurityProperties`, но `LdapAuthService` их не использует: группы берутся только из `memberOf`. Для `ldaps://` сертификат сервера проверяется по truststore JVM.

```yaml
hadoop:
  security:
    auth:
      mode: ldap                # mock | ldap | kerberos | hybrid
    ldap:
      enabled: true
      url: "ldaps://ad.company.local:636"
      base-dn: "dc=company,dc=local"
      manager-dn: "cn=svc_hadoop_explorer,ou=Service Accounts,dc=company,dc=local"
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

`KerberosSpnegoService` проверяет тикет через Java GSS-API и создает контекст с учетными данными acceptor по умолчанию (`GSSManager.createContext((GSSCredential) null)`). Ключи `hadoop.security.kerberos.keytab`, `service-principal` и `krb5-config` объявлены в `CommonSecurityProperties`, но сервис их не читает: изменение этих ключей на проверку тикета не влияет. Principal `HTTP/<host>@<REALM>` и его keytab JVM берет из JAAS-конфигурации: задайте их в секции `com.sun.security.jgss.accept` файла, переданного через `-Djava.security.auth.login.config=<путь>`, вместе с `-Djavax.security.auth.useSubjectCredsOnly=false` и `-Djava.security.krb5.conf=<путь к krb5.conf>`. Без этого JVM не находит ключ acceptor, и проверка тикета завершается ошибкой GSS.

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

Фильтр `RateLimitFilter` ограничивает частоту запросов к `/api/*` (Token Bucket на Bucket4j, корзины в памяти процесса, у каждой реплики свои). Запрос сверх лимита получает HTTP 429, заголовок `Retry-After` (секунды до следующего токена) и тело `{"detail": "...", "status": 429}`. На уровне DEBUG логгера `org.apache.hadoop.explorer.common.resilience.RateLimiterService` в журнал пишется `Rate limit exceeded: tier=..., key=..., retry_after=...s` — по строке на каждый отклонённый запрос, поэтому по умолчанию (INFO) она не выводится. Веб-интерфейс (`BaseApiClient.request` в `frontend/common/api/client.ts`) при ответе 429 повторяет запрос один раз после паузы из `Retry-After`, если она не больше 5 с; без заголовка или при большей паузе показывает ошибку. Повтор делается для любого метода: 429 выдаётся до обработчика, запрос не выполнялся.

| Параметр `hadoop.security.rate-limiter.*` | По умолчанию | Что делает | Когда менять |
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

TLS для встроенного веб-сервера и исходящих REST-клиентов сервисов `yarn`, `hdfs`, `sql`, `spark` и `replicator/orchestrator` настраивается в одном месте — в секции `hadoop.security.tls` стартера `common-security-starter`:

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
- `HADOOP_SECURITY_TLS_ENABLED=true`
- `HADOOP_SECURITY_TLS_KEY_STORE_PATH=/etc/security/tls/keystore.p12`
- `HADOOP_SECURITY_TLS_KEY_STORE_PASSWORD=secret`
- `HADOOP_SECURITY_TLS_TRUST_STORE_PATH=/etc/security/tls/truststore.p12`
- `HADOOP_SECURITY_TLS_CLIENT_AUTH=REQUIRE` (для включения двустороннего mTLS)
- `HADOOP_SECURITY_TLS_INSECURE_SKIP_VERIFY=true` (только для тестирования)

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

**Внимание.** `TrinoSqlEngine` и `HiveSqlEngine` в текущем коде не подключаются к кластерам: каталоги, схемы, таблицы и выполнение запросов делегируются `MockSqlEngine` при любом значении `mock-storage`. Параметры `host`, `port` и `impersonation` отображаются в UI и API, но на выполнение запросов не влияют.

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

Доступ к кластеру (`ClusterService`): если `allowed-users` и `allowed-groups` пусты, кластер виден всем аутентифицированным пользователям; иначе — пользователям из `allowed-users` и членам групп из `allowed-groups`. Ключей аутентификации к Trino/HiveServer2 (`auth`, `use_ssl`) и режима только для чтения (`allow_dml_ddl`) в `SqlProperties` нет.

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
    provider: "mock-builtin"
    url: "http://llm-server.company.local:11434/v1"
    model: "qwen2.5-coder-32b"
    api-key: ""
```

`AiService` не обращается к LLM: ответы ассистента формируются встроенными правилами (например, предупреждение о `SELECT *` без `LIMIT`, ошибка `JOIN` без `ON`/`USING`). Значения `provider`, `model` и `url` только возвращаются в ответе статуса ассистента; `api-key` не используется.

---

## 6. Настройка Spark Explorer (Livy, PySpark, Scala, Metastore)

Параметры читает `SparkProperties` (`@ConfigurationProperties(prefix = "spark")`). Базовые значения — в `backend/spark/src/main/resources/application.yml`.

**Внимание.** `LivyClient` в текущем коде не обращается к Apache Livy: создание сессии и выполнение кода идут через встроенный `MockSparkEngine`. Значения `livy-url`, версий Spark, окружений Python, метастора и профилей ресурсов отображаются в UI и API, но в Livy не передаются.

### 6.1 Кластеры и интеграция с Apache Livy

```yaml
spark:
  session-ttl-minutes: 120      # Сессии без активности дольше этого срока закрываются
  results-ttl-hours: 24         # Объявлен в SparkProperties, в коде не используется
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

Ключей `use_ssl`, `auth` и `impersonation` в `SparkProperties` нет. `allowed-groups` и `allowed-users` объявлены, но код сервиса их не читает: доступ к кластеру по ним не ограничивается.

### 6.2 Версии Spark и среды Python (HDFS Archives)

Пользователь выбирает версию Spark и окружение Python из списка в конфигурации. У версии и окружения есть только идентификатор, имя и признак значения по умолчанию; пути к архивам Spark и интерпретаторам Python в `SparkProperties` не задаются.

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

Оба значения сохраняются в записи сессии (`SparkSessionRecord`) и возвращаются в API; в Livy они не передаются (см. предупреждение в начале раздела).

### 6.3 Интеграция с YARN и разграничение очередей

```yaml
spark:
  clusters:
    - id: "prod-hadoop"
      yarn-cluster-id: "prod-yarn"
      yarn-queues: ["root.analytics", "root.adhoc", "root.etl"]
      default-queue: "root.analytics"
```

Список `yarn-queues` задает очереди, доступные для выбора в UI. ACL на уровне отдельных очередей (`queue_acl`) и адресов ResourceManager в `SparkProperties` нет.

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

У метастора есть только `id`, `name` и `default-metastore`; URI Thrift и `spark_conf` в `SparkProperties` не задаются. `resource-profiles` — список, а не словарь.

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
Тестовые пользователи и `auth.mode: mock` вынесены в профиль `mock` (`SPRING_PROFILES_ACTIVE=mock`),
который включают демо-стенды `demo/replicator`. Схема базы во всех профилях
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
    org.apache.hadoop.explorer.replicator.telemetry.GrpcHealthProbeMain 127.0.0.1:50051
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

Планировщик `ReplicationScheduler` (пакет `org.apache.hadoop.explorer.replicator.orchestrator.scheduler`) встроен в оркестратор:
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
- Алгоритм — Token Bucket (`LocalBandwidthLimiter`, пакет `org.apache.hadoop.explorer.replicator.shaper`) с burst-буфером 0,5 с. Метод `throttle(bytes)` блокирует вызывающий поток (`Thread.sleep`) на время, нужное для накопления токенов; `reserve(bytes)` только списывает токены и возвращает время ожидания (используется потоком опроса Kafka).
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
| `SPRING_PROFILES_ACTIVE` | Профили Spring Boot (`mock` для демо, `prod` для оркестратора репликатора) | — |
| `HADOOP_AUTH_MODE` | `hadoop.security.auth.mode` в сервисах `yarn`, `hdfs`, `sql`, `spark` | `ldap` |
| `JWT_SECRET_KEY` | `hadoop.security.jwt.secret-key`: ключ подписи JWT, не короче 32 символов | — (без него сервис не стартует вне mock-режима) |
| `LDAP_URL` | `hadoop.security.ldap.url` | `ldaps://ldap.company.local:636` |
| `LDAP_MANAGER_DN` / `LDAP_MANAGER_PASSWORD` | Учетная запись для поиска пользователей в LDAP | — |
| `LDAP_USER_SEARCH_BASE` | `hadoop.security.ldap.user-search-base` | `ou=users,dc=company,dc=local` |
| `YARN_DB_URL` / `YARN_DB_DRIVER` / `YARN_DB_USER` / `YARN_DB_PASSWORD` | БД YARN Explorer (Change Requests, аудит) | `jdbc:h2:file:./data/yarn_explorer` / `org.h2.Driver` / `sa` / пусто |
| `YARN_KERBEROS_PRINCIPAL` / `YARN_KERBEROS_KEYTAB` | Учетная запись YARN Explorer для SPNEGO-запросов к ResourceManager | — |
| `YARN_AWX_BASE_URL` / `YARN_AWX_TOKEN` / `YARN_AWX_JOB_TEMPLATE_ID` | Подключение к AWX (раздел 3.3) | — / — / `101` |

Любой другой ключ задается переменной по правилам relaxed binding (раздел 1), например `SQL_RESULTS_TTL_HOURS=48` или `HADOOP_SECURITY_COOKIE_SECURE=true`.

---

## 9. Конфигурация в Kubernetes (Helm)

Umbrella Chart `helm/hadoop-explorer` подключает чарты `helm/charts/hdfs-explorer`, `sql-explorer`, `yarn-explorer` и `spark-explorer`. Чарта для репликатора нет. Параметры дочернего чарта передаются в секции с его именем:

```yaml
global:
  environment: "production"
  ingressDomain: "hadoop.company.local"

yarn-explorer:
  enabled: true
  replicaCount: 2
  podDisruptionBudget:
    enabled: true
    maxUnavailable: 1
  extraEnv:
    - name: HADOOP_SECURITY_COOKIE_SECURE
      value: "true"
  secrets:
    jwtSecret: "<jwt_secret>"
    ldapBindPassword: "<ldap_password>"

hdfs-explorer:
  enabled: true
  replicaCount: 2
  secrets:
    jwtSecret: "<jwt_secret>"
    ldapBindPassword: "<ldap_password>"

sql-explorer:
  enabled: true

spark-explorer:
  enabled: true
```

`<jwt_secret>` — случайная строка не короче 32 символов, `<ldap_password>` — пароль сервисной учетной записи LDAP.

**Внимание.** Секция `config` в `values.yaml` дочерних чартов осталась в формате прежних Python-сервисов (`server_uri`, `bind_dn`, `access_token_expire_minutes`, `sqlite:///...`). Чарты рендерят ее в ConfigMap `config.yaml` и передают путь в `CONFIG_PATH` / `HDFS_CONFIG_PATH`, но Java-сервисы этот файл не читают (раздел 1): значения из `config` на работу сервиса не влияют. Задавайте параметры переменными окружения по правилам relaxed binding (раздел 8): в `yarn-explorer` — через `extraEnv`/`envFrom`; в остальных чартах таких полей нет. `sql-explorer` и `spark-explorer` передают из `secrets.jwtSecret` переменную `JWT_SECRET_KEY`; `hdfs-explorer` передает `HDFS_SECRET_KEY`, `HDFS_LDAP_PASSWORD` и `HDFS_DATABASE_URL`, которые Java-сервис не читает, поэтому HDFS Explorer из этого чарта без `JWT_SECRET_KEY` не стартует.

### 9.1 Рекомендации по High-Availability в продакшне
1. База данных. При `replicaCount > 1` подключите внешний PostgreSQL (`SPRING_DATASOURCE_URL: jdbc:postgresql://...`). Встроенная H2 — только для локальной разработки и демо-стендов. Порядок перехода — в [docs/production-database.md](production-database.md).
2. PodDisruptionBudget. При `replicaCount > 1` шаблоны чартов создают `PodDisruptionBudget`, и drain узлов Kubernetes не выводит из работы все реплики сразу.
3. NetworkPolicy. Включена по умолчанию (`networkPolicy.enabled: true`): входящий трафик к подам разрешен только от Ingress-контроллера.
4. Мониторинг. Настройте сбор метрик с `/metrics` (порт 8000 / 8005): HTTP golden signals, состояние `CircuitBreaker`, повторы и ошибки.

---

## 10. Мониторинг Prometheus и дашборды Grafana

Все сервисы платформы отдают метрики в формате OpenMetrics / Prometheus.

### 10.1 Экспортируемые метрики

| Метрика | Тип | Лейблы | Описание |
|---|---|---|---|
| `http_requests_total` | Counter | `app`, `method`, `path`, `status` | Количество входящих HTTP-запросов (с нормализацией путей) |
| `http_request_duration_seconds` | Histogram | `app`, `method`, `path`, `le` | Задержка обработки запросов (бакеты от 5ms до 10s) |
| `http_requests_in_progress` | Gauge | `app` | Число одновременных запросов в обработке (concurrency) |
| `hadoop_circuit_breaker_state` | Gauge | `name` | Текущий статус Circuit Breaker: `0=CLOSED`, `1=HALF_OPEN`, `2=OPEN` |
| `hadoop_circuit_breaker_calls_total` | Counter | `name`, `status` | Вызовы Circuit Breaker со статусами `success`, `failed`, `rejected` |
| `hadoop_retry_attempts_total` | Counter | `app`, `operation`, `status` | Срабатывания механизма повторов (`retry`, `exhausted`, `success`) |
| `hadoop_auth_attempts_total` | Counter | `app`, `provider`, `status` | Попытки аутентификации (`ldap`, `kerberos`, `mock`) и результат |
| `hadoop_rate_limit_blocks_total` | Counter | `app` | Блокировки по Rate Limiter (HTTP 429) |
| `hadoop_exceptions_total` | Counter | `app`, `exception_type` | Непредвиденные исключения 500 с `incident_id` (CWE-209) |
| `replicator_bytes_total` | Counter | `status` | Объем переданных байтов репликации (`IN_PROGRESS`, `COMPLETED`, `FAILED`) |
| `replicator_jobs_total` | Gauge | `status`, `mode` | Количество задач репликации HDFS по статусам и типу запуска |
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
| `replicator_agent_status` | Gauge | `agent_id`, `cluster_id`, `mode`, `status` | Доступность узла агента (`1` при совпадении статуса, `0` иначе) |
| `replicator_agent_active_transfers` | Gauge | `agent_id`, `cluster_id`, `mode` | Текущее количество параллельных передач на агенте |
| `replicator_agent_transferred_bytes_total` | Counter | `agent_id`, `cluster_id` | Суммарный объем байтов, переданных агентом |
| `replicator_agent_errors_total` | Counter | `agent_id`, `cluster_id` | Количество ошибок передачи данных агента |
| `replicator_agent_heartbeat_age_seconds` | Gauge | `agent_id`, `cluster_id` | Задержка keepalive heartbeat агента |
| `replicator_agent_max_bandwidth_mb_s` | Gauge | `agent_id`, `cluster_id` | Выделенный локальный лимит полосы HTB агента в МБ/с |

### 10.2 Настройка сбора метрик в Prometheus (Scrape Config)

```yaml
scrape_configs:
  - job_name: 'hadoop-explorer'
    scrape_interval: 15s
    metrics_path: '/metrics'
    static_configs:
      - targets:
          - 'yarn-explorer:8000'
          - 'hdfs-explorer:8000'
          - 'sql-explorer:8003'
          - 'spark-explorer:8004'
          - 'replicator-orchestrator:8005'
```

Пример Kubernetes `ServiceMonitor` (Prometheus Operator):
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: hadoop-explorer-monitor
  namespace: hadoop-explorer
spec:
  selector:
    matchLabels:
      app.kubernetes.io/part-of: hadoop-explorer
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

### 10.3 Готовые дашборды Grafana

В каталоге [`monitoring/grafana/dashboards/`](../monitoring/grafana/dashboards/) лежат 5 дашбордов:

1. Сводный обзор платформы: [`hadoop_explorer_overview.json`](../monitoring/grafana/dashboards/hadoop_explorer_overview.json) — Golden Signals (RPS, Latency p95/p99, 5xx rate), Circuit Breaker, Retries, безопасность (Auth, Rate Limiting) и общая активность репликации.
2. Межкластерная репликация: [`hadoop_replicator_overview.json`](../monitoring/grafana/dashboards/hadoop_replicator_overview.json) — скорость репликации WAN, задержки иерархического шейпера Token Bucket Throttler, активные gRPC-воркеры, статусы задач (RUNNING, SCHEDULED, QUEUED, COMPLETED) и гистограмма размеров файлов.
3. Файловые операции HDFS: [`hdfs_explorer_operations.json`](../monitoring/grafana/dashboards/hdfs_explorer_operations.json) — файловые операции WebHDFS, скорость загрузки/скачивания (Upload vs Download), отказы операций.
4. Очереди YARN: [`yarn_explorer_queues.json`](../monitoring/grafana/dashboards/yarn_explorer_queues.json) — утилизация очередей Capacity Scheduler, запросы на изменение квот, состояние HA ResourceManager.
5. Аналитика Spark и SQL: [`spark_sql_explorer_analytics.json`](../monitoring/grafana/dashboards/spark_sql_explorer_analytics.json) — активные сессии Apache Livy, интерактивные инструкции Spark, запросы Trino и Hive Metastore.

#### Импорт дашбордов в Grafana
1. В веб-интерфейсе Grafana перейдите в **Dashboards** → **New** → **Import**.
2. Загрузите нужный JSON-файл из каталога `monitoring/grafana/dashboards/`.
3. Выберите источник данных Prometheus и нажмите **Import**.

Чтобы Grafana загружала дашборды при старте (provisioning), скопируйте файл [`monitoring/grafana/provisioning/dashboards/dashboards.yaml`](../monitoring/grafana/provisioning/dashboards/dashboards.yaml) в `/etc/grafana/provisioning/dashboards/`.

### 10.4 Примеры полезных PromQL запросов

- Текущий RPS всей платформы:
  ```promql
  sum(rate(http_requests_total[1m]))
  ```
- Процент ошибок 5xx:
  ```promql
  (sum(rate(http_requests_total{status=~"5.."}[1m])) / sum(rate(http_requests_total[1m]))) * 100
  ```
- Задержка p95 по сервисам:
  ```promql
  histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[1m])) by (le, app))
  ```
- Алерт на срабатывание Circuit Breaker (кластер недоступен):
  ```promql
  hadoop_circuit_breaker_state == 2
  ```

---

## 11. Руководства администратора по развертыванию (DevOps Hub)

Руководства по развертыванию каждого компонента в режимах Standalone (systemd), Docker / Docker Compose и Kubernetes (Helm):

- [Руководство администратора платформы (admin-guide.md)](admin-guide.md)
- [Руководство администратора YARN Explorer (yarn-admin-guide.md)](yarn-admin-guide.md)
- [Руководство администратора HDFS Explorer (hdfs-admin-guide.md)](hdfs-admin-guide.md)
- [Руководство администратора SQL Explorer (sql-admin-guide.md)](sql-admin-guide.md)
- [Руководство администратора Spark Explorer (spark-admin-guide.md)](spark-admin-guide.md)
- [Руководство администратора Hadoop gRPC Replicator (replicator-admin-guide.md)](replicator-admin-guide.md)


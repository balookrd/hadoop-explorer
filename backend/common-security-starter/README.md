# Hadoop Explorer :: Common Security Starter (Java 21 / Spring Boot 3)

Spring Boot стартер аутентификации, сессий, CSRF-защиты, RBAC, rate limiting, circuit breaker и аудита для сервисов Hadoop Explorer Platform. Java 21 LTS, Spring Boot 3.3+.

Стартер закрывает уязвимости из OWASP Top-10 и CWE, перечисленные ниже.

---

## Возможности стартера

### Аутентификация

- Kerberos SPNEGO SSO (RFC 4559) через Java GSS-API (`org.ietf.jgss`) без внешних бинарных зависимостей.
- LDAPS / Active Directory с экранированием фильтров от LDAP Injection (RFC 4515 / CWE-90).
- Mock-режим для локальной разработки и демо-стендов; при `debug: false` запрещен.

### Токены и сессии

- Токены выдаются только в cookies с флагами `HttpOnly`, `SameSite=Lax`, `Secure`; `localStorage` не используется.
- `Authorization: Bearer <token>` для межсервисных вызовов.
- Двухуровневое хранилище сессий (`SessionStore`):
  - L1 — in-memory LRU-кэш на Caffeine, проверка отзыва токена за O(1);
  - L2 — БД через JDBC (PostgreSQL, SQLite в режиме WAL, H2); таблицы `active_sessions` и `revoked_tokens` создаются при старте.
- Защита от fail-open при сбое хранилища L2 (CWE-613) включается параметром `session-store.fail-closed: true`; значение по умолчанию в `CommonSecurityProperties` — `false`.

### Защита от CSRF (CWE-352)

- Запрос с заголовком `Sec-Fetch-Site: cross-site` отклоняется.
- Заголовки `Origin` и `Referer` проверяются по белому списку CORS-источников.
- Заголовок `X-Requested-With: XMLHttpRequest` для AJAX-запросов.

### Ролевая модель (RBAC)

- Системные роли: `ADMIN`, `WRITER`, `READER`.
- `RoleResolver` сопоставляет группам роли: группы по умолчанию (`hadoop-admins`, `data-engineers` и др.) и списки из конфигурации.

### Отказоустойчивость и аудит

- Rate limiter на алгоритме Token Bucket (Bucket4j).
- Circuit breaker (`SimpleCircuitBreaker`) с состояниями `CLOSED`, `OPEN`, `HALF_OPEN`: после `failureThreshold` сбоев подряд вызовы к NameNode или RM отклоняются, пока не истечет `recoveryTimeoutMs`; затем пропускается пробный вызов (`HALF_OPEN`).
- JSON-аудит (`AuditLogger` и AOP-аннотация `@Audited`).

### REST-эндпоинты

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/api/v1/auth/login` | Вход по логину и паролю |
| `GET` | `/api/v1/auth/sso` | Вход через Kerberos SPNEGO |
| `POST` | `/api/v1/auth/logout` | Отзыв токена и удаление cookie |
| `GET` | `/api/v1/auth/me` | Профиль текущей сессии |

### TLS/HTTPS и mTLS

- HTTPS для REST API настраивает `TlsWebServerCustomizer`.
- Хранилища ключей PKCS12 и JKS (keystore и truststore).
- Взаимная аутентификация сертификатами (mTLS, `client-auth: REQUIRE / OPTIONAL`).
- `TlsContextFactory` настраивает TLS для исходящих REST-клиентов (`HttpClient`).
- Генерация временных самоподписанных PKCS12-сертификатов (`auto-generate-self-signed`) и отключение проверки сертификатов для тестовых стендов (`insecure-skip-verify`).

---

## Подключение к сервису Spring Boot

Добавьте зависимость в `pom.xml` сервиса:

```xml
<dependency>
    <groupId>org.apache.hadoop.explorer</groupId>
    <artifactId>common-security-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

Стартер регистрирует через автоконфигурацию `SecurityFilterChain`, фильтры аутентификации, контроллеры и компоненты управления сессиями.

---

## Конфигурация (`application.yml`)

```yaml
hadoop:
  security:
    debug: false # в production mock-аутентификация категорически заблокирована
    auth:
      mode: ldap # варианты: ldap | kerberos | mock
      admin-groups:
        - "hadoop-admins"
        - "platform-admins"
      writer-groups:
        - "data-engineers"
        - "spark-users"

    jwt:
      secret-key: "your-production-secret-key-at-least-32-chars-long"
      algorithm: "HS256"
      expiration-minutes: 480

    cookie:
      secure: true
      same-site: "Lax"
      names:
        - "access_token"
        - "hadoop_explorer_session"

    cors:
      allowed-origins:
        - "https://hadoop.corp.internal"
        - "https://yarn.corp.internal"

    ldap:
      enabled: true
      url: "ldaps://ldap.corp.internal:636"
      base-dn: "dc=corp,dc=internal"
      user-search-base: "ou=users"
      user-search-filter: "(&(objectClass=person)(uid={0}))"
      group-search-base: "ou=groups"
      group-search-filter: "(&(objectClass=groupOfNames)(member={0}))"
      manager-dn: "cn=service-account,ou=services,dc=corp,dc=internal"
      manager-password: "${LDAP_SERVICE_PASSWORD}"

    kerberos:
      enabled: false
      krb5-config: "/etc/krb5.conf"
      keytab: "/etc/security/keytabs/spnego.service.keytab"
      service-principal: "HTTP/hadoop.corp.internal@CORP.INTERNAL"

    session-store:
      type: jdbc # memory | jdbc | redis
      fail-closed: true
      l1-capacity: 10000
      l1-ttl-seconds: 3600

    rate-limiter:
      enabled: true
      requests-per-minute: 600
      burst-capacity: 100

    tls:
      enabled: true
      key-store-path: "/etc/security/tls/keystore.p12"
      key-store-password: "${TLS_KEYSTORE_PASSWORD}"
      key-store-type: "PKCS12"
      trust-store-path: "/etc/security/tls/truststore.p12"
      trust-store-password: "${TLS_TRUSTSTORE_PASSWORD}"
      trust-store-type: "PKCS12"
      client-auth: "NONE" # NONE | OPTIONAL | REQUIRE (mTLS)
      insecure-skip-verify: false # true для локальных демо-стендов
      auto-generate-self-signed: true # автогенерация при отсутствии файла keystore
      enabled-protocols:
        - "TLSv1.3"
        - "TLSv1.2"
```

---

## Запуск тестов

Все тесты JUnit 5 модуля:

```bash
cd backend/common-security-starter
mvn clean test
```

Команду запускайте на JDK 21 (`JAVA_HOME=<jdk21>`). Цель `make test-security-starter` из корня репозитория находит JDK 21 сама.

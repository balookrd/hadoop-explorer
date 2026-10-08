# 🛡️ Hadoop Explorer :: Common Security Starter (Java 21 / Spring Boot 3)

Единый повторно используемый стартер безопасности и инфраструктурной отказоустойчивости для платформы **Hadoop Explorer Platform** на базе **Java 21 LTS** и **Spring Boot 3.3+**.

Модуль является Java-эквивалентом `backend/common` и реализует требования корпоративной безопасности, предотвращая уязвимости OWASP Top-10 / CWE.

---

## 🚀 Возможности

1. **Единый контур аутентификации**:
   - **Kerberos SPNEGO SSO** (RFC 4559) через нативный Java GSS-API (`org.ietf.jgss`) без сторонних бинарных зависимостей.
   - **LDAPS / Active Directory** с защитой от LDAP Injection (RFC 4515 / CWE-90).
   - **Mock-режим** для локальной разработки и демо-стендов (строго блокируется при `debug: false`).
2. **Безопасная работа с токенами и сессиями**:
   - **Cookie-First & Zero LocalStorage**: выпуск токенов исключительно через `HttpOnly`, `SameSite=Lax`, `Secure` Cookie.
   - Поддержка `Authorization: Bearer <token>` для межсервисного взаимодействия.
   - **Двухуровневое хранилище сессий (`SessionStore`)**:
     - **L1**: In-Memory LRU кэш на базе **Caffeine** для моментальной проверки отзыва токенов (O(1)).
     - **L2**: База данных через JDBC (PostgreSQL / SQLite WAL / H2) с автоматической инициализацией таблиц `active_sessions` и `revoked_tokens`.
     - Защита от **Fail-Open** при сбоях L2 хранилища (CWE-613).
3. **Защита от CSRF (CWE-352)**:
   - Анализ заголовка `Sec-Fetch-Site: cross-site` с мгновенным отклонением межсайтовых вызовов.
   - Валидация заголовков `Origin` и `Referer` по белому списку разрешенных CORS-источников.
   - Поддержка заголовка `X-Requested-With: XMLHttpRequest` для асинхронных AJAX-запросов.
4. **Ролевая модель доступа (RBAC)**:
   - Системные роли: `ADMIN`, `WRITER`, `READER`.
   - Резолвер ролей `RoleResolver` с поддержкой корпоративных групп по умолчанию (`hadoop-admins`, `data-engineers` и др.) и настраиваемых списков.
5. **Отказоустойчивость и мониторинг**:
   - **Rate Limiter** на базе алгоритма Token Bucket (**Bucket4j**).
   - **Circuit Breaker** (`SimpleCircuitBreaker`) с состояниями `CLOSED`, `OPEN`, `HALF_OPEN` для защиты от лавинных сбоев NameNode/RM.
   - **Структурированный JSON-аудит** (`AuditLogger` и AOP-аннотация `@Audited`).
6. **Готовые REST-эндпоинты**:
   - `POST /api/v1/auth/login` — вход по логину/паролю.
   - `GET /api/v1/auth/sso` — автоматический Kerberos SPNEGO вход.
   - `POST /api/v1/auth/logout` — отзыв токена и удаление Cookie.
   - `GET /api/v1/auth/me` — получение профиля текущей сессии.

---

## 📦 Подключение к Spring Boot сервису

Добавьте зависимость в `pom.xml` вашего микросервиса:

```xml
<dependency>
    <groupId>org.apache.hadoop.explorer</groupId>
    <artifactId>common-security-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

Стартер автоматически зарегистрирует `SecurityFilterChain`, фильтры аутентификации, контроллеры и компоненты управления сессиями.

---

## ⚙️ Конфигурация (`application.yml`)

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
```

---

## 🧪 Запуск тестов

Сборка и запуск полного тестового набора JUnit 5:

```bash
cd backend/common-security-starter
mvn clean test
```

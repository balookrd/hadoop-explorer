# YARN Queue Explorer API (Java 21 / Spring Boot 3)

Бэкенд YARN Queue Explorer на Java 21 LTS и Spring Boot 3.3.4. Сервис читает очереди Capacity Scheduler и метрики с YARN ResourceManager (с учетом RM HA), проводит изменения очередей через заявки с согласованием и развертывает их на RM через Ansible AWX.

## Структура модуля

```
backend/yarn/
├── pom.xml                   # Maven-проект (io.github.balookrd.pristan:yarn-explorer-java:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, Клиенты, Сущности, Модели
│   │   └── resources/        # application.yml, capacity-scheduler-template.xml, статика SPA
│   └── test/                 # Юнит- и интеграционные тесты (Spring Boot Test, MockMvc)
└── README.md                 # Документация модуля
```

## Функции сервиса

1. Capacity Scheduler Engine:
   - Валидация правил Capacity Scheduler: на каждом уровне иерархии сумма capacity дочерних очередей должна быть равна 100%, отдельно для каждого раздела (Node Labels).
   - Расчет детального diff между live-конфигурацией и черновиком изменений (`created`, `modified`, `deleted`, `unchanged`, дельты capacity, max-capacity, RAM и vCores).
   - Формирование перечня правок `capacity-scheduler.xml` в виде XML (`generate-xml`, `preview-xml`). ResourceManager не отдает развернутый файл по REST, поэтому сервис описывает только свойства, которые меняет черновик, а не файл целиком.

2. Клиенты ResourceManager и HA failover:
   - Поддержка активного/резервного RM (`GET /ws/v1/cluster/info` с определением `haState == "ACTIVE"`).
   - SPNEGO/Kerberos-аутентификация и имперсонация пользователя (`doAs`).
   - Mock-режим (`MockYarnClient`) для тестов и локальной разработки без кластера Hadoop.

3. Жизненный цикл Change Requests и принцип четырех глаз:
   - Полный цикл управления заявками на изменение конфигурации очередей (`SUBMITTED`, `APPROVED`, `REJECTED`, `CANCELLED`).
   - Принцип разделения обязанностей (Four-Eyes Principle): автор заявки не может согласовать ее сам, заявку согласует другой администратор кластера.
   - Предпросмотр перечня правок перед развертыванием.

4. Интеграция с Ansible AWX:
   - Запуск Job Template в AWX, который раскладывает `capacity-scheduler.xml` на узлы ResourceManager и вызывает `yarn rmadmin -refreshQueues`. Для согласованной заявки сервис передает роли список правок (`capacity_scheduler_patch_b64`), для `deploy-xml` — файл целиком (`capacity_scheduler_xml_b64`).
   - Ожидание завершения задачи (polling) и запись результата в аудит.

5. Безопасность:
   - Интеграция со стартером `zastava`: JWT-токены, cookie-аутентификация, RBAC (READER, WRITER, ADMIN), ролевой маппинг пользователей и групп по кластерам, аудит-логирование операций, помеченных `@Audited`.
   - Защита от CSRF, Bucket4j Rate Limiting, защита вызовов RM через `SimpleCircuitBreaker`.

## Сборка и запуск

### Требования
- JDK 21 LTS (`JAVA_HOME=/opt/homebrew/opt/openjdk` или системный JDK 21)
- Apache Maven 3.9+
- Установленный модуль `zastava` в локальном Maven-репозитории

### Команды
```bash
# Запуск модульных и интеграционных тестов
make test-yarn
# или
mvn test -f backend/yarn/pom.xml

# Сборка исполняемого Spring Boot JAR
make build-yarn
# или
mvn clean package -DskipTests -f backend/yarn/pom.xml

# Локальный запуск без кластера и каталога: профиль mock (демо-учетки, debug)
java -jar backend/yarn/target/yarn-explorer-java-1.0.0.jar --spring.profiles.active=mock

# Боевой запуск: без JWT_SECRET_KEY и LDAP/Kerberos сервис не стартует
JWT_SECRET_KEY=... java -jar backend/yarn/target/yarn-explorer-java-1.0.0.jar \
  --spring.config.additional-location=file:/etc/pristan/yarn/application.yml
```

Сервис слушает порт `8000` (`server.port` в `application.yml`).

## REST API

| Метод | URI | Описание | Доступ |
|---|---|---|---|
| `GET` | `/health` / `/healthz` | Liveness / Readiness health-check | Публичный |
| `GET` | `/actuator/prometheus` | Метрики Prometheus (HTTP, переходы Change Request) | Публичный |
| `POST`| `/api/v1/auth/login` | Вход пользователя (генерация JWT Cookie) | Публичный |
| `POST`| `/api/v1/auth/sso` | Kerberos SPNEGO SSO-аутентификация | Публичный |
| `GET` | `/api/v1/auth/me` | Данные текущего пользователя и роли по кластерам | Аутентифицирован |
| `POST`| `/api/v1/auth/logout` | Отзыв токена и завершение сессии | Аутентифицирован |
| `GET` | `/api/v1/clusters` | Список кластеров YARN и роли пользователя | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/queues` | Дерево очередей Capacity Scheduler, метрики и баланс | Аутентифицирован |
| `POST`| `/api/v1/clusters/{id}/validate` | Валидация черновика дерева очередей (правило 100%) | WRITER / ADMIN |
| `POST`| `/api/v1/clusters/{id}/diff` | Расчет diff изменений между live и draft | WRITER / ADMIN |
| `POST`| `/api/v1/clusters/{id}/generate-xml` | Перечень правок `capacity-scheduler.xml` для ручного применения | ADMIN кластера |
| `POST`| `/api/v1/clusters/{id}/deploy-xml` | Полная замена `capacity-scheduler.xml` через AWX (аварийный путь, без согласования) | ADMIN кластера |
| `GET` | `/api/v1/change-requests` | Список заявок на изменение (фильтр по cluster_id, status) | Аутентифицирован |
| `GET` | `/api/v1/change-requests/pending-count` | Количество заявок на согласовании (pending) | Аутентифицирован |
| `POST`| `/api/v1/change-requests` | Создание новой заявки на согласование очередей | WRITER / ADMIN |
| `GET` | `/api/v1/change-requests/{id}` | Детали заявки с diff изменений | Аутентифицирован |
| `GET` | `/api/v1/change-requests/{id}/preview-xml` (`/xml`) | Перечень правок заявки в виде XML | Аутентифицирован |
| `POST`| `/api/v1/change-requests/{id}/approve` | Согласование заявки (Four-Eyes Principle) | ADMIN кластера |
| `POST`| `/api/v1/change-requests/{id}/reject` | Отклонение заявки с указанием причины | ADMIN кластера |
| `POST`| `/api/v1/change-requests/{id}/cancel` | Отзыв заявки автором или администратором | Автор / ADMIN |
| `POST`| `/api/v1/change-requests/{id}/deploy` | Деплой согласованной заявки через Ansible AWX | ADMIN кластера |
| `GET` | `/api/v1/change-requests/{id}/deploy-status` | Статус деплоя заявки и вывод Ansible AWX | Аутентифицирован |

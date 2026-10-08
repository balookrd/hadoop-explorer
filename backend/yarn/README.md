# YARN Queue Explorer API (Java 21 / Spring Boot 3)

Высокопроизводительный сервис **YARN Queue Explorer API**, реализованный на **Java 21 LTS** и **Spring Boot 3.3.4**, предназначенный для управления очередями Capacity Scheduler, мониторинга метрик YARN ResourceManager, поддержки высокой доступности (RM HA failover) и безопасного согласования и развертывания конфигураций очередей через Ansible AWX.

---

## 🏛 Архитектура компонентов

```
backend/yarn/
├── pom.xml                   # Maven проект (org.apache.hadoop.explorer:yarn-explorer-java:1.0.0)
├── src/
│   ├── main/
│   │   ├── java/             # Контроллеры, Сервисы, Клиенты, Сущности, Модели
│   │   └── resources/        # application.yml, capacity-scheduler-template.xml, статика SPA
│   └── test/                 # Юнит- и интеграционные тесты (Spring Boot Test, MockMvc)
└── README.md                 # Документация модуля
```

---

## 🌟 Ключевые возможности

1. **Capacity Scheduler Engine**:
   - Валидация правил Capacity Scheduler: гарантированное соблюдение 100% суммы мощностей дочерних очередей на каждом уровне иерархии (с поддержкой разделов/партиций и Node Labels).
   - Расчет детального diff между live конфигурацией и черновиком изменений (`created`, `modified`, `deleted`, `unchanged`, дельты capacity, max-capacity, RAM и vCores).
   - Генерация и форматирование XML (`capacity-scheduler.xml`) с сохранением всех неуправляемых настроек, комментариев и аудит-метаданных.

2. **ResourceManager HA Failover & Клиенты**:
   - Поддержка активного/резервного RM (`GET /ws/v1/cluster/info` с определением `haState == "ACTIVE"`).
   - SPNEGO Kerberos аутентификация и безопасная имперсонация (`doAs`).
   - Mock-режим (`MockYarnClient`) для автономного тестирования и локальной разработки без подключения к живому кластеру Hadoop.

3. **Жизненный цикл Change Requests & Four-Eyes Principle**:
   - Полный цикл управления заявками на изменение конфигурации очередей (`SUBMITTED`, `APPROVED`, `REJECTED`, `CANCELLED`).
   - Принцип разделения обязанностей (**Four-Eyes Principle**): создатель заявки не может самостоятельно согласовать свой собственный запрос, требуя ревью независимым администратором кластера.
   - Предпросмотр сгенерированного XML перед развертыванием.

4. **Интеграция с Ansible AWX**:
   - Запуск шаблонов Job Template в AWX для раскатки `capacity-scheduler.xml` на узлы ResourceManager и вызова `yarn rmadmin -refreshQueues`.
   - Поддержка ожидания завершения задачи (polling) и логирование результатов выполнения в аудит.

5. **Безопасность платформы**:
   - Интеграция со стартером `common-security-starter`: JWT токены, Cookie-аутентификация, RBAC (READER, WRITER, ADMIN), ролевой маппинг пользователей и групп по кластерам, аудит-логирование ключевых операций (`@Audited`).
   - Защита от CSRF, Bucket4j Rate Limiting, защита вызовов RM через `SimpleCircuitBreaker`.

---

## 🚀 Сборка и запуск

### Требования
- JDK 21 LTS (`JAVA_HOME=/opt/homebrew/opt/openjdk` или системный JDK 21)
- Apache Maven 3.9+
- Установленный модуль `common-security-starter` в локальном Maven-репозитории

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

# Запуск сервиса
java -jar backend/yarn/target/yarn-explorer-java-1.0.0.jar
```

Сервис запустится на порту `8000` (стандартный порт YARN Explorer API).

---

## 📡 REST API Контракты

| Метод | URI | Описание | Доступ |
|---|---|---|---|
| `GET` | `/health` / `/actuator/health` | Liveness / Readiness health-check | Публичный |
| `GET` | `/metrics` / `/actuator/prometheus` | Экспорт Prometheus метрик и Circuit Breaker | Публичный |
| `POST`| `/api/v1/auth/login` | Вход пользователя (генерация JWT Cookie) | Публичный |
| `POST`| `/api/v1/auth/sso` | Kerberos SPNEGO SSO аутентификация | Публичный |
| `GET` | `/api/v1/auth/me` | Данные текущего пользователя и роли по кластерам | Аутентифицирован |
| `POST`| `/api/v1/auth/logout` | Отзыв токена и завершение сессии | Аутентифицирован |
| `GET` | `/api/v1/clusters` | Список кластеров YARN и роли пользователя | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/queues` | Актуальное дерево очередей Capacity Scheduler | Аутентифицирован |
| `GET` | `/api/v1/clusters/{id}/metrics` | Метрики кластера (vCores, RAM, Applications) | Аутентифицирован |
| `POST`| `/api/v1/clusters/{id}/queues/validate` | Валидация черновика дерева очередей (правило 100%) | WRITER / ADMIN |
| `POST`| `/api/v1/clusters/{id}/queues/diff` | Расчет diff изменений между live и draft | WRITER / ADMIN |
| `GET` | `/api/v1/change-requests` | Список заявок на изменение (фильтр по кластеру) | Аутентифицирован |
| `POST`| `/api/v1/change-requests` | Создание новой заявки на согласование очередей | WRITER / ADMIN |
| `GET` | `/api/v1/change-requests/{id}` | Детали заявки с предпросмотром XML и diff | Аутентифицирован |
| `POST`| `/api/v1/change-requests/{id}/approve` | Согласование заявки (Four-Eyes Principle) | ADMIN кластера |
| `POST`| `/api/v1/change-requests/{id}/reject` | Отклонение заявки с указанием причины | ADMIN кластера |
| `POST`| `/api/v1/change-requests/{id}/cancel` | Отзыв заявки автором | Автор заявки |
| `POST`| `/api/v1/change-requests/{id}/deploy` | Деплой согласованной заявки через Ansible AWX | ADMIN кластера |

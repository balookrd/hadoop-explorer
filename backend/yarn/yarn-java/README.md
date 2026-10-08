# YARN Explorer API (Java 21 / Spring Boot 3)

Высокопроизводительный сервис **YARN Explorer API**, реализованный на **Java 21 LTS** и **Spring Boot 3.3.4**, предназначенный для управления очередями Capacity Scheduler, мониторинга метрик YARN ResourceManager, поддержки высокой доступности (RM HA failover) и безопасного согласования и развертывания конфигураций очередей через Ansible AWX.

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

---

## 🚀 Сборка и запуск

### Требования
- JDK 21 LTS (`JAVA_HOME=/opt/homebrew/opt/openjdk` или системный JDK 21)
- Apache Maven 3.9+
- Установленный модуль `common-security-starter` в локальном Maven-репозитории

### Команды
```bash
# Запуск модульных и интеграционных тестов
make test-yarn-java
# или
mvn test -f backend/yarn/yarn-java/pom.xml

# Сборка исполняемого Spring Boot JAR
make build-yarn-java
# или
mvn clean package -DskipTests -f backend/yarn/yarn-java/pom.xml

# Запуск сервиса
java -jar backend/yarn/yarn-java/target/yarn-explorer-java-1.0.0.jar
```

Сервис запустится на порту `8000` (стандартный порт YARN Explorer API).

---

## 📡 REST API Контракты

| Метод | URL | Роль | Описание |
|---|---|---|---|
| `GET` | `/health` | Публичный | Health check (`{"status": "ok"}`) |
| `GET` | `/api/v1/clusters` | READER | Список кластеров YARN с правами и ролями пользователя |
| `GET` | `/api/v1/clusters/{id}/queues` | READER | Дерево очередей, метрики кластера и балансы |
| `POST` | `/api/v1/clusters/{id}/validate` | WRITER | Валидация баланса очередей (правило 100%) |
| `POST` | `/api/v1/clusters/{id}/diff` | WRITER | Вычисление изменений (live vs draft) |
| `POST` | `/api/v1/clusters/{id}/generate-xml` | ADMIN | Генерация `capacity-scheduler.xml` |
| `POST` | `/api/v1/clusters/{id}/deploy-xml` | ADMIN | Прямой деплой XML через AWX |
| `GET` | `/api/v1/change-requests` | READER | Список заявок на изменение очередей |
| `GET` | `/api/v1/change-requests/pending-count` | READER | Количество ожидающих заявок |
| `POST` | `/api/v1/change-requests` | WRITER | Создание заявки на изменение очередей |
| `GET` | `/api/v1/change-requests/{id}` | READER | Просмотр деталей заявки и diff |
| `POST` | `/api/v1/change-requests/{id}/approve` | ADMIN | Одобрение заявки (с проверкой Four-Eyes) |
| `POST` | `/api/v1/change-requests/{id}/reject` | ADMIN | Отклонение заявки |
| `POST` | `/api/v1/change-requests/{id}/cancel` | WRITER/ADMIN | Отзыв заявки автором |
| `GET` | `/api/v1/change-requests/{id}/xml` | READER | Предпросмотр XML для заявки |
| `POST` | `/api/v1/change-requests/{id}/deploy` | ADMIN | Развертывание заявки через AWX |

# Демонстрационный стенд Hadoop gRPC Replicator

В данной папке находятся конфигурации и скрипты полностью автономного демонстрационного стенда сервиса межкластерной репликации **Hadoop gRPC Replicator**.

---

## 🚀 Быстрый запуск и остановка

### Через корневой Makefile:
```bash
# Запуск стенда (сборка контейнеров и старт в фоне)
make demo-replicator

# Остановка и очистка контейнеров
make demo-replicator-stop
```

### Через Docker Compose:
```bash
# Запуск
docker compose -f demo/replicator/docker-compose.yml up -d --build

# Остановка
docker compose -f demo/replicator/docker-compose.yml down -v
```

---

## 🌐 Сетевые интерфейсы и порты

| Сервис | Порт | Технология | Описание | URL |
|---|---|---|---|---|
| **Replicator Orchestrator** | `8005` | Java 21 LTS (Spring Boot 3 + Svelte 5) | Веб-интерфейс, REST API, Token Bucket и шедулер | [http://localhost:8005](http://localhost:8005) |
| **Prometheus Metrics** | `8005` | Spring Boot Actuator | Экспорт системных и сетевых метрик | [http://localhost:8005/actuator/prometheus](http://localhost:8005/actuator/prometheus) |
| **Agent DC1 (Primary ЦОД)** | `50051` | Java 21 LTS (gRPC) | Дуплексный агент кластера `dc1` | `localhost:50051` |
| **Agent DC2 (DR ЦОД)** | `50052` | Java 21 LTS (gRPC) | Дуплексный агент кластера `dc2` | `localhost:50052` |

---

## 🧪 Запуск демонстрационных задач распределенной репликации

Стенд демонстрирует полный цикл работы изолированных ЦОД с распределенным пулом пофайловых задач:

```bash
# 1. Запуск стенда
./start-demo.sh

# 2. Создание демо-задач (DC1 ➔ DC2 и DC2 ➔ DC1)
./create-test-jobs.sh
```

Скрипт автоматически:
1. Генерирует файлы в общем томе данных (`prod_sales_dc1.csv` 5 МБ, `events_stream_dc1.json` 2 МБ и `analytics_report_dc2.parquet` 3 МБ).
2. Ставит задачу репликации **Agent DC1 ➔ Agent DC2** (`prod_sales_dc1.csv`) от принципала `writer_user@REALM.LOCAL`.
3. Ставит задачу репликации **Agent DC2 ➔ Agent DC1** (`analytics_report_dc2.parquet`) от принципала `de_user@REALM.LOCAL`.
4. Ставит задачу потоковой передачи **Agent DC1 ➔ Agent DC2** (`events_stream_dc1.json`) от принципала `reader_user@REALM.LOCAL`.
5. Тестирует обработку ошибок при репликации несуществующего пути (задача переходит в `FAILED`).
6. Агенты выполняют анализ каталогов, формируют пул пофайловых подзадач, параллельные воркеры забирают задачи через `claimTasks` и передают потоком gRPC с валидацией контрольных сумм SHA-256.

---

## 🔬 Автоматические Smoke-тесты репликации Hive Metastore и HDFS (2 ЦОД)

В стенд включен автоматический end-to-end smoke-тест полного жизненного цикла репликации схемы и данных:
1. Создание таблицы Hive и запись данных в **DC1** (Primary ЦОД).
2. Запуск репликации схемы базы данных, ожидание завершения первичного **Bootstrap sync** (статус `ACTIVE`).
3. Создание **НОВОЙ таблицы** в синхронизированной схеме в **DC1** + запись данных (генерация CDC-события `CREATE_TABLE` в `NOTIFICATION_LOG`).
4. Запуск **CDC sync** и перенос метаданных и файлов партиций на **DC2**.
5. Финальная верификация: подтверждение появления схемы в HMS DC2, физического коммита файлов данных на HDFS DC2 и изоляции подзадач `HMS_SUBJOB`.

### Запуск smoke-тестов:
```bash
# Вариант 1: Запуск скрипта напрямую
./demo/replicator/run-smoke-tests.sh

# Вариант 2: Запуск изолированного тест-раннера внутри Docker сети
docker compose -f demo/replicator/docker-compose.yml --profile test run --rm smoke-test
```

---

## 🗺️ Топология стенда
- **ЦОД 1 (Москва / Primary)**: содержит 2 кластера HDFS (`demo-cluster` — Prod DataLake и `analytics-cluster` — Secondary DataLake).
- **ЦОД 2 (Санкт-Петербург / Disaster Recovery)**: содержит 1 кластер HDFS (`backup-cluster` — DR Mirror DataLake).
- **Лимиты каналов**:
  - Магистраль **DC1 ➔ DC2**: 100 МБ/с.
  - Канал **Prod ➔ Backup**: 60 МБ/с.
  - Канал **Analytics ➔ Backup**: 40 МБ/с.
  - Локальный обмен **Prod ➔ Analytics (DC1)**: 80 МБ/с.
  - Глобальный пул полосы: 120 МБ/с.

---

## 👥 Тестовые учетные записи (RBAC)

Для проверки разграничения прав доступа в окне авторизации доступны три преднастроенных пользователя:

1. **👑 `admin_user` / `password123` (Александр Админов)**:
   - Роль: `ADMIN`.
   - Права: видит **все задачи всех пользователей**, имеет право менять сетевые лимиты DC-DC и HDFS-HDFS в реальном времени.
2. **⚙️ `de_user` / `password123` (Иван Датаинженеров)**:
   - Роль: `WRITER`.
   - Права: видит **только свои персональные задачи** репликации.
3. **📊 `analyst_user` / `password123` (Анна Аналитикова)**:
   - Роль: `READER`.
   - Права: видит **только свои задачи** в режиме чтения.

---

## 📖 Документация
- Подробное руководство пользователя: [docs/replicator-user-guide.md](../../docs/replicator-user-guide.md)
- Руководство по конфигурации: [docs/CONFIGURATION.md](../../docs/CONFIGURATION.md)
- Архитектурный документ: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)

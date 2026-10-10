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

## 🌐 Сетевые интерфейсы и компоненты стенда

| Сервис | Порт | Технология | Описание | URL / Доступ |
|---|---|---|---|---|
| **Replicator Orchestrator** | `8005` | Java 21 LTS (Spring Boot 3 + Svelte 5) | Веб-интерфейс, REST API, Token Bucket и шедулер | [http://localhost:8005](http://localhost:8005) |
| **Prometheus Metrics** | `8005` | Spring Boot Actuator | Экспорт системных и сетевых метрик | [http://localhost:8005/actuator/prometheus](http://localhost:8005/actuator/prometheus) |
| **Agent DC1 (Primary ЦОД)** | `50051` | Java 21 LTS (gRPC) | Дуплексный агент кластера `dc1` (HDFS 1 Kerberos) | `localhost:50051` |
| **Agent DC2 (DR ЦОД)** | `50052` | Java 21 LTS (gRPC) | Дуплексный агент кластера `dc2` (HDFS 2 Kerberos) | `localhost:50052` |
| **Primary HDFS (Cluster 1)** | `9870` | Apache Hadoop 3.3 (WebHDFS) | Исходный DataLake кластер (DC1) | [http://localhost:9870](http://localhost:9870) |
| **DR Backup HDFS (Cluster 2)** | `9872` | Apache Hadoop 3.3 (WebHDFS) | Резервный DataLake кластер (DC2) | [http://localhost:9872](http://localhost:9872) |
| **Hive Metastore 1 (DC1)** | `9083` | Apache Hive 4.0.0 (Thrift) | Метастор схем первичного кластера (DC1) | `thrift://localhost:9083` |
| **Hive Metastore 2 (DC2)** | `9084` | Apache Hive 4.0.0 (Thrift) | Метастор схем резервного кластера (DC2) | `thrift://localhost:9084` |
| **MIT Kerberos KDC** | `88` | Kerberos 5 KDC Daemon | Единый KDC аутентификации (Realm `COMPANY.LOCAL`) | `localhost:88` |

---

## 🧪 Запуск демонстрационных задач распределенной репликации

Стенд демонстрирует полный цикл работы изолированных ЦОД с распределенным пулом пофайловых задач:

```bash
# 1. Запуск стенда (поднимает KDC, 2x HDFS, 2x HMS, Orchestrator и 2x Агента)
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

## 🔬 Полноценные End-to-End Smoke-тесты репликации (Таблицы, Партиции, Дописывание, Удаление, Inotify/CDC)

В стенд включен расширенный автоматический smoke-тест полного жизненного цикла репликации схемы и данных, поддерживающий режимы работы:
* **`all`** (по умолчанию) — сквозное тестирование всех фаз (стандартная репликация, HMS CDC и HDFS Inotify streaming);
* **`standard`** — проверка переноса файлов через воркеры без использования Inotify и без CDC;
* **`cdc`** — проверка репликации метаданных Hive Metastore (схемы, партиции, дописывание, удаление);
* **`inotify`** — проверка HDFS Inotify стриминга (Active/Standby лизинг, фильтрация staging, коммит по RenameEvent).

### Проверяемые сценарии:
1. **Проверка доступности инфраструктуры**: KDC, 2x HDFS NameNode (WebHDFS), 2x Hive Metastore (Thrift), воркеры DC1/DC2 (gRPC), стримеры DC1/DC2, Оркестратор.
2. **Перенос таблицы (Bootstrap Sync)**: создание БД и таблицы со схемой в DC1 (External Table), перенос на DC2 и верификация схемы и файлов.
3. **Дописывание файла в существующую таблицу**: добавление нового файла данных в каталог существующей таблицы в DC1, синхронизация дельты и верификация на DC2.
4. **Перенос партиции**: создание партиционированной таблицы (`dt`), добавление партиции `dt=2026-10-10`, перенос схемы и данных партиции в DC2.
5. **Дописывание файла в существующую партицию**: добавление нового файла в каталог уже созданной партиции `dt=2026-10-10`, репликация дельты и валидация присутствия обоих файлов на DC2.
6. **Удаление партиции**: удаление партиции `dt=2026-10-10` в DC1, применение CDC события `DROP_PARTITION` и верификация удаления партиции в HMS DC2.
7. **Удаление таблицы**: удаление таблицы в DC1, применение CDC события `DROP_TABLE` и верификация удаления таблицы в HMS DC2.
8. **HDFS Inotify Streaming HA**: опрос распределенного лизинга (`/api/v1/streaming/lease/all`), проверка ролей `ACTIVE` и `STANDBY`, проверка изоляции привилегий (HTTP 403 Forbidden при попытке забора задач воркера стримером).
9. **Распознавание коммита (RenameEvent)**: фильтрация временных путей (`.staging`, `_temporary`), фиксация коммита при `rename` и автоматическое порождение задач воркерам без ручного триггера.

### Запуск smoke-тестов:
```bash
# Вариант 1: Полный smoke-тест (все сценарии)
./demo/replicator/run-smoke-tests.sh all

# Вариант 2: Запуск конкретного режима (standard | cdc | inotify)
./demo/replicator/run-smoke-tests.sh cdc
./demo/replicator/run-smoke-tests.sh standard
./demo/replicator/run-smoke-tests.sh inotify

# Вариант 3: Запуск внутри Docker-контейнера через Docker Compose
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

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

## 🔬 Полноценные End-to-End Smoke-тесты репликации HMS и HDFS (2 ЦОД)

В стенд включен автоматический end-to-end smoke-тест полного жизненного цикла репликации схемы и данных между реальными сервисами:
1. **Проверка доступности всей инфраструктуры**: Kerberos KDC, 2x HDFS NameNode (WebHDFS), 2x Hive Metastore (Thrift), 2x Replicator Agents (gRPC), Orchestrator.
2. **Создание таблицы Hive и запись данных в Primary HDFS DC1**: генерация схемы базы данных и таблицы `sales_initial` (EXTERNAL_TABLE) с реальным файлом данных 512 КБ в `hdfs://hdfs-cluster-1:9000`.
3. **Проверка чистоты DR кластера**: гарантированное подтверждение отсутствия схемы в HMS DC2 (HTTP 404) и отсутствия данных на HDFS DC2 до старта репликации.
4. **Запуск репликации схемы и Bootstrap Sync**: трансляция путей с `hdfs://hdfs-cluster-1:9000` на `hdfs://hdfs-cluster-2:9000`, создание скрытого саб-джоба `HMS_SUBJOB`, передача файлов данных агентами по gRPC с валидацией контрольных сумм SHA-256.
5. **Создание НОВОЙ таблицы в DC1 и потоковая CDC-синхронизация**: добавление таблицы `customers_cdc` (MANAGED non-transactional) с файлом данных 256 КБ, фиксация события в `NOTIFICATION_LOG`, запуск CDC sync и подтверждение появления схемы в HMS DC2 и файлов в HDFS DC2.
6. **Проверка изоляции подзадач**: подтверждение сокрытия служебных подзадач передачи данных из общего регламентного списка.

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

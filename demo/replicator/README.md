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
| **Replicator Orchestrator** | `8005` | Python (FastAPI + Svelte 5) | Веб-интерфейс, REST API, Token Bucket и шедулер | [http://localhost:8005](http://localhost:8005) |
| **Prometheus Metrics** | `8005` | Prometheus Client | Экспорт системных и сетевых метрик | [http://localhost:8005/metrics](http://localhost:8005/metrics) |
| **Agent DC1 (Primary ЦОД)** | `50051` | Python 3.12 (gRPC) | Дуплексный агент кластера `demo-cluster` | `localhost:50051` |
| **Agent DC2 (DR ЦОД)** | `50052` | Java 17 LTS (gRPC) | Дуплексный нативный Hadoop агент `backup-cluster` | `localhost:50052` |

---

## 🧪 Запуск демонстрационных задач интероперабельности (Python ⇄ Java)

Стенд демонстрирует полную сквозную совместимость между Python и Java 17 агентами в обоих направлениях:

```bash
# 1. Запуск стенда
./start-demo.sh

# 2. Создание демо-задач (Python DC1 ➔ Java DC2 и Java DC2 ➔ Python DC1)
./create-test-jobs.sh
```

Скрипт автоматически:
1. Генерирует файлы в общем томе данных (`prod_sales_dc1.csv` 5 МБ и `analytics_report_dc2.parquet` 3 МБ).
2. Ставит задачу репликации **Python Agent ➔ Java 17 Agent** от принципала `data_engineer@REALM.LOCAL`.
3. Ставит задачу репликации **Java 17 Agent ➔ Python Agent** от принципала `lead_analyst@REALM.LOCAL`.
4. В обоих направлениях проверяется совпадение контрольных сумм SHA-256 после передачи.

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

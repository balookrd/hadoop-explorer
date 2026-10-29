# Демонстрационный стенд YARN Explorer

В данной папке находятся конфигурации и скрипты полностью автономного демонстрационного стенда с поддержкой **Kerberos (KDC)**, **OpenLDAP** и двух кластеров **Apache Hadoop YARN 3.4.0**.

## 🚀 Запуск и остановка

- Запуск: `./start-demo.sh` (или `docker compose up -d`)
- Остановка: `./stop-demo.sh` (или `docker compose down -v`)

После запуска веб-интерфейс доступен по адресу:
👉 **http://localhost:8001**

## 🔐 Как устроен стенд

- Оба ResourceManager работают в secure-режиме: REST (`/ws/v1/cluster/*`) и веб-интерфейс защищены SPNEGO (`hadoop.http.filter.initializers` в `cluster-*/core-site.xml`), анонимный запрос получает `401`.
- YARN Explorer читает очереди с RM по SPNEGO под учетной записью `yarn-explorer@COMPANY.LOCAL` (keytab из KDC стенда) и передает пользователя через `doAs`. Учетная запись прописана как proxy user в `core-site.xml` кластеров.
- Очереди — из `cluster-*/capacity-scheduler.xml`: `prod-yarn` — prod (spark / flink / hive), dev, default с метками GPU и HIGH_MEM; `analytics-yarn` — batch (GPU), interactive, streaming. NodeManager в стенде нет, поэтому метрики ресурсов нулевые.
- Развертывание заявок — симуляция AWX (`yarn.awx.simulate: true`): заявка проходит согласование и «деплой», но RM не меняется.

Проверка SPNEGO вручную изнутри сети стенда:

```bash
docker run --rm --network demo-platform-net curlimages/curl:8.10.1 -s -o /dev/null -w '%{http_code}\n' \
  http://yarn-rm-1.yarn-demo-net:8088/ws/v1/cluster/scheduler   # 401 без Kerberos
```

---

📖 **Полная документация со сценариями демонстрации и архитектурой**:
👉 **[README.md](../README.md)**

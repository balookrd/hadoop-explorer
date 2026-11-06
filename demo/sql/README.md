# Демонстрационный стенд SQL Web Explorer

Конфигурации и скрипты демонстрационного стенда в Docker Compose: MIT Kerberos KDC, OpenLDAP, Apache Hive 4.0.0 (HiveServer2 + Metastore), Trino Coordinator и веб-интерфейс SQL Web Explorer. Внешние сервисы стенду не нужны.

## Запуск и остановка

- Запуск стенда:
  ```bash
  ./start-demo.sh
  ```
  (или `docker compose up -d`)

- Остановка стенда:
  ```bash
  ./stop-demo.sh
  ```
  (или `docker compose down -v`)

После запуска веб-интерфейс доступен по адресу http://localhost:8003.

---

Сценарии демонстрации, учетные записи и архитектура стенда описаны в [README.md](../README.md).

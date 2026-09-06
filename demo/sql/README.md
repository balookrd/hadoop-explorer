# Демонстрационный стенд SQL Web Explorer

В данной папке находятся конфигурации и скрипты полностью автономного демонстрационного стенда в Docker Compose с поддержкой **MIT Kerberos KDC**, **OpenLDAP**, **Apache Hive 4.0.0 (HiveServer2 + Metastore)**, **Trino Coordinator** и веб-интерфейса **SQL Web Explorer**.

## 🚀 Быстрый запуск и остановка

- Запуск стенда:
  ```bash
  ./start-demo.sh
  ```
  *(или `docker compose up -d`)*

- Остановка стенда:
  ```bash
  ./stop-demo.sh
  ```
  *(или `docker compose down -v`)*

После запуска веб-интерфейс доступен по адресу:
👉 **http://localhost:8002**

---

📖 **Полная документация со сценариями демонстрации, учетными записями и архитектурой**:
👉 **[DEMO.md](../DEMO.md)**

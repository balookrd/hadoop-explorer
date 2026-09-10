# 🤖 Руководство и правила для ИИ-ассистентов (AGENTS.md)

Данный файл содержит карту репозитория **Hadoop Explorer Platform** и регламент работы для предотвращения перерасхода контекста и токенов.

---

## 1. Карта репозитория (Architecture Map)

Hadoop Explorer Platform — монорепозиторий с 4 независимыми сервисами и общим ядром безопасности.

```
hadoop-explorer/
├── backend/                  # Python (FastAPI, uv workspaces)
│   ├── common/               # Ядро: Auth, SessionStore, Kerberos, CircuitBreaker, Audit, RateLimit
│   ├── yarn/                 # YARN Explorer API (Capacity Scheduler, RM HA)
│   ├── hdfs/                 # HDFS Explorer API (WebHDFS, Kerberos)
│   ├── sql/                  # SQL Explorer API (Trino, Hive Metastore)
│   └── spark/                # Spark Explorer API (Livy, PySpark, Scala)
├── frontend/                 # TypeScript (Svelte 5, Tailwind CSS, Vite)
│   ├── common/               # Общие типы (types/generated), API клиенты, UI компоненты
│   └── apps/                 # SPA приложения: yarn, hdfs, sql, spark
├── data/                     # ⚠️ ВНИМАНИЕ: Локальные SQLite БД (spark_explorer.db, sql_explorer.db, yarn_explorer.db)
├── scripts/                  # Утилиты автоматизации (build, tests, types, AST skeletonizer)
├── demo/                     # Docker Compose демо-стенды (all, yarn, hdfs, sql, spark, monitoring)
└── docs/                     # Архитектурная документация на русском языке (ARCHITECTURE.md)
```

---

## 2. Правила экономии токенов и контекста (Token Efficiency)

1. **Строгая изоляция сервисов**:
   - При работе над задачей для конкретного сервиса (например, YARN) **никогда не исследовать** директории других сервисов (`hdfs/`, `spark/`, `sql/`).
   - Использовать только связку `backend/<service>` + `frontend/apps/<service>` + `common`.

2. **Запретные для сканирования и чтения директории**:
   - `data/` — бинарные файлы SQLite (`*.db`, `*.db-wal`, `*.db-shm`). Чтение категорически запрещено (тратит десятки тысяч токенов впустую).
   - `node_modules/`, `.venv/`, `.pytest_cache/`, `.ruff_cache/`, `dist/`, `build/`.
   - Lock-файлы (`package-lock.json`, `pnpm-lock.yaml`, `uv.lock`) — не читать целиком; зависимости проверять в `pyproject.toml` и `package.json`.

3. **Использование AST-скелета вместо чтения файлов целиком**:
   - Для ознакомления с архитектурой сервиса или общих библиотек запускать AST-скелетизатор:
     ```bash
     python3 scripts/generate_skeleton.py backend/<service>
     # или без docstrings для максимального сжатия:
     python3 scripts/generate_skeleton.py backend/<service> --no-docstrings
     ```
   - Это сокращает объём передаваемого кода на **75–90%**, сохраняя все сигнатуры функций, типы аргументов, Pydantic-схемы и интерфейсы.

4. **Чтение файлов только фрагментами (Slice Viewing)**:
   - Не читать исходные файлы длиннее 100 строк целиком. Всегда использовать `StartLine` и `EndLine` для чтения только целевых функций или классов.

5. **Делегирование тяжелых поисков субагентам**:
   - При исследовании сложных багов, grep по нескольким модулям или разборе логов запускать субагента `research`. Субагент собирает данные в изолированном контексте и возвращает только краткую выжимку, сохраняя чистым контекст основной сессии.

---

## 3. Команды для разработки и проверки

* **Синхронизация окружения**: `uv sync --all-packages` (или `make sync`)
* **Линтинг и форматирование**:
  * `uv run ruff check backend`
  * `uv run ruff format backend`
* **Модульные тесты**:
  * Сервис целиком: `uv run pytest backend/<service>/tests`
  * Все тесты: `make test`
* **Кодогенерация типов TypeScript из бэкенда**:
  * `make generate-types` (обновляет `frontend/common/types/generated/`)
* **Генерация AST-скелетов**:
  * `make skeleton-all` (сохраняет каркас модулей в `.context/`)

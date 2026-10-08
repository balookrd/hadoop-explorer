# 🤖 Руководство и правила для ИИ-ассистентов (AGENTS.md)

Данный файл содержит карту репозитория **Hadoop Explorer Platform** и регламент работы для предотвращения перерасхода контекста и токенов.

---

## 1. Карта репозитория (Architecture Map)

Hadoop Explorer Platform — монорепозиторий с 5 независимыми сервисами и общим ядром безопасности.

```
hadoop-explorer/
├── backend/                  # Python (FastAPI, uv workspaces) & Java (Spring Boot 3, Java 21)
│   ├── common/               # Ядро Python: Auth, SessionStore, Kerberos, CircuitBreaker, Audit, RateLimit
│   ├── common-security-starter/ # Ядро Java: Spring Boot 3 стартер безопасности (Java 21, SPNEGO, LDAP, JWT, L1/L2)
│   ├── yarn/                 # YARN Explorer API (Capacity Scheduler, RM HA)
│   ├── hdfs/                 # HDFS Explorer API (WebHDFS, Kerberos)
│   ├── sql/                  # SQL Explorer API (Trino, Hive Metastore)
│   ├── spark/                # Spark Explorer API (Livy, PySpark, Scala)
│   └── replicator/           # Hadoop gRPC Replicator API & Daemons (Token Bucket, gRPC, Kerberos, agent-java)
├── frontend/                 # TypeScript (Svelte 5, Tailwind CSS, Vite)
│   ├── common/               # Общие типы (types/generated), API клиенты, UI компоненты
│   └── apps/                 # SPA приложения: yarn, hdfs, sql, spark, replicator
├── data/                     # ⚠️ ВНИМАНИЕ: Локальные SQLite БД (spark_explorer.db, sql_explorer.db, yarn_explorer.db, replicator.db)
├── scripts/                  # Утилиты автоматизации (build, tests, types, AST skeletonizer)
├── demo/                     # Docker Compose демо-стенды (all, yarn, hdfs, sql, spark, replicator, monitoring)
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

6. **Структурный AST-поиск в Java (`scripts/java-index-query.sh`)**:
   - При исследовании и рефакторинге Java-кода (`backend/common-security-starter`, `backend/replicator/agent-java`, `orchestrator-java`) **не читать файлы классов целиком**.
   - Использовать быстрый потоковый CLI-запрос по AST-индексу:
     ```bash
     ./scripts/java-index-query.sh class <ClassName>     # структура класса, поля, аннотации
     ./scripts/java-index-query.sh method <methodName>   # сигнатура метода, параметры, вызовы
     ./scripts/java-index-query.sh tests <ClassName>     # автоматический поиск затронутых тестов
     ./scripts/java-index-query.sh call <methodName>     # места вызова метода
     ./scripts/java-index-query.sh new <TypeName>        # места создания экземпляров
     ```
   - Индекс компилируется автономно (Java Compiler API без внешних либ) и автоматически обновляется при изменении исходников Java.
   - Для структурного сопоставления синтаксических шаблонов использовать `ast-grep` (`sgconfig.yml`, `.ast-grep/rules/`).

---

## Навигация по коду и экономия токенов (Token Efficiency, Java)

1. **Запретные для сканирования и чтения директории**:
    - `target/`, `build/`, `.gradle/`, `.git/`, `logs/` — временные артефакты сборки.
2. **Использование AST-поиска и AST-индекса**:
    - Применяй `scripts/java-index-query.sh` (`class`, `method`, `call`, `new`, `tests`) для быстрого точечного поиска символов вместо слепого grep.
    - Для структурного поиска кода используй `ast-grep` (`find_code` / `ast-grep run -l java -p '...'`).
    - Исходные файлы открывай точечно по найденным путям и номерам строк.
3. **Чтение файлов только фрагментами (Slice Viewing)**:
    - Избегай чтения файлов целиком — используй параметры ограничения строк (StartLine/EndLine) для чтения только целевых методов или блоков.
4. **Актуализация индекса**:
    - После изменения структуры кода или сборки проекта обновляй AST-индекс командой `./scripts/build-java-ast-index.sh`.

## 3. Команды для разработки и проверки

* **Синхронизация окружения**: `uv sync --all-packages` (или `make sync`)
* **Линтинг и форматирование**:
  * `uv run ruff check backend`
  * `uv run ruff format backend`
* **Модульные тесты**:
  * Сервис целиком: `uv run pytest backend/<service>/tests`
  * Java стартер безопасности: `make test-security-starter`
  * Все Java тесты: `make test-java`
  * Все тесты: `make test`
* **Кодогенерация типов TypeScript из бэкенда**:
  * `make generate-types` (обновляет `frontend/common/types/generated/`)
* **Генерация AST-скелетов и индексов**:
  * `make skeleton-all` (сохраняет каркас модулей в `.context/`)
  * `make java-index` (генерация Java AST-индекса в `target/java-ast-index/`)
  * `make java-query Q="class <Name>"` (структурный поиск по Java коду)
  * `ast-grep scan` (проверка структурных AST-правил кодовой базы)

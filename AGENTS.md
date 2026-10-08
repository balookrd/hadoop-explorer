# 🤖 Руководство и правила для ИИ-ассистентов (AGENTS.md)

Данный файл содержит карту репозитория **Hadoop Explorer Platform** и регламент работы для предотвращения перерасхода контекста и токенов.

---

## 1. Карта репозитория (Architecture Map)

Hadoop Explorer Platform — монорепозиторий на **Java 21 LTS (Spring Boot 3)** и **TypeScript (Svelte 5)** с 5 независимыми сервисами и общим ядром безопасности.

```
hadoop-explorer/
├── backend/                  # Java 21 LTS (Spring Boot 3.3.4, Maven)
│   ├── common-security-starter/ # Ядро платформы: Spring Boot 3 стартер безопасности (Java 21, SPNEGO, LDAP, JWT, L1/L2)
│   ├── yarn/                 # YARN Explorer API (Java 21 / Spring Boot 3, Capacity Scheduler, RM HA)
│   ├── hdfs/                 # HDFS Explorer API (Java 21 / Spring Boot 3, WebHDFS, Kerberos, Parquet/ORC Preview)
│   ├── sql/                  # SQL Explorer API (Java 21 / Spring Boot 3, Trino, Hive Metastore, AI Assistant)
│   ├── spark/                # Spark Explorer API (Java 21 / Spring Boot 3, Livy, PySpark, Scala, DAG Pipelines)
│   └── replicator/           # Hadoop gRPC Replicator API & Daemons
│       ├── orchestrator/     # Replicator Orchestrator API (Java 21 / Spring Boot 3, Hierarchical Token Bucket)
│       └── agent/            # Нативный Replicator gRPC Worker Daemon (Java 21, Kerberos doAs)
├── frontend/                 # TypeScript (Svelte 5, Tailwind CSS, Vite)
│   ├── common/               # Общие типы (types/generated), API клиенты, UI компоненты
│   └── apps/                 # SPA приложения: yarn, hdfs, sql, spark, replicator
├── data/                     # ⚠️ ВНИМАНИЕ: Локальные SQLite / H2 БД (*.db, *.mv.db). Чтение запрещено!
├── scripts/                  # Утилиты автоматизации (build-containers.sh, run-tests.sh, java AST index)
├── demo/                     # Docker Compose демо-стенды (all, yarn, hdfs, sql, spark, replicator, monitoring)
└── docs/                     # Архитектурная и административная документация (ARCHITECTURE.md, guides)
```

---

## 2. Правила экономии токенов и контекста (Token Efficiency)

1. **Строгая изоляция сервисов**:
   - При работе над задачей для конкретного сервиса (например, YARN) **никогда не исследовать** директории других сервисов (`hdfs/`, `spark/`, `sql/`).
   - Использовать только связку `backend/<service>` + `frontend/apps/<service>` + `backend/common-security-starter`.

2. **Запретные для сканирования и чтения директории**:
   - `data/` — бинарные файлы БД (`*.db`, `*.mv.db`, `*.trace.db`). Чтение категорически запрещено (тратит десятки тысяч токенов впустую).
   - `node_modules/`, `target/`, `build/`, `.gradle/`, `.git/`, `dist/`.
   - Lock-файлы (`package-lock.json`, `pnpm-lock.yaml`) — не читать целиком; зависимости проверять в `pom.xml` и `package.json`.

3. **Чтение файлов только фрагментами (Slice Viewing)**:
   - Не читать исходные файлы длиннее 100 строк целиком. Всегда использовать `StartLine` и `EndLine` для чтения только целевых функций или классов.

4. **Делегирование тяжелых поисков субагентам**:
   - При исследовании сложных багов, grep по нескольким модулям или разборе логов запускать субагента `research`. Субагент собирает данные в изолированном контексте и возвращает только краткую выжимку, сохраняя чистым контекст основной сессии.

5. **Структурный AST-поиск в Java (`scripts/java-index-query.sh`)**:
   - При исследовании и рефакторинге Java-кода (`backend/common-security-starter`, `backend/*/*-java`) **не читать файлы классов целиком**.
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

## 3. Команды для разработки и проверки

* **Сборка и тесты бэкенда (Java 21 / Spring Boot 3)**:
  * Единый запуск всех тестов: `./scripts/run-tests.sh all` (или `make test`)
  * Все тесты Java: `make test-java`
  * Стартер безопасности: `make test-security-starter`
  * Сервис YARN: `mvn test -f backend/yarn/pom.xml` (или `make test-yarn`)
  * Сервис HDFS: `mvn test -f backend/hdfs/pom.xml` (или `make test-hdfs`)
  * Сервис SQL: `mvn test -f backend/sql/pom.xml` (или `make test-sql`)
  * Сервис Spark: `mvn test -f backend/spark/pom.xml` (или `make test-spark`)
  * Сервис Replicator: `mvn test -f backend/replicator/pom.xml`
* **Фронтенд тесты (Svelte 5 / Vitest)**:
  * `make test-ui`
* **Сборка Docker-контейнеров**:
  * `./scripts/build-containers.sh all` (или `make build`)
* **Генерация AST-скелетов и индексов**:
  * `make java-index` (генерация Java AST-индекса в `target/java-ast-index/`)
  * `make java-query Q="class <Name>"` (структурный поиск по Java коду)
  * `ast-grep scan` (проверка структурных AST-правил кодовой базы)

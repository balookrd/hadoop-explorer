# Frontend: Spark Web Explorer

Современный веб-интерфейс аналитической веб-студии **Spark Explorer** для интерактивной работы с Apache Spark (PySpark, Scala Spark, Spark SQL) через Apache Livy, построенный на **Svelte 5** (Runes), **TypeScript**, **Monaco Editor** и **Tailwind CSS 4**.

---

## 🛠️ Технологический стек

- **Фреймворк**: [Svelte 5](https://svelte.dev/) с реактивными примитивами `$state`, `$derived`, `$props`, `$effect`.
- **Сборщик**: [Vite](https://vitejs.dev/) / Rolldown с поддержкой Hot Module Replacement (HMR).
- **Редактор кода**: [Monaco Editor](https://microsoft.github.io/monaco-editor/) с динамической подсветкой синтаксиса Python, Scala и SQL, горячими клавишами (`Ctrl+Enter` / `Cmd+Enter`).
- **Стилизация**: [Tailwind CSS 4](https://tailwindcss.com/) — единый корпоративный UI в стилистике Hadoop Explorer, кастомные скроллбары, янтарно-оранжевая палитра Spark.
- **Иконки**: [Lucide Svelte](https://lucide.dev/).
- **Связь с сервером**: Cookie-first HTTP REST API + сохранение сессий в защищённую базу данных (`/api/v1/workspace`).

---

## 🏛️ Архитектура компонентов (`src/`)

```
frontend/apps/spark/src/
├── api/
│   └── client.ts             # Типизированный клиент API: сессии Livy, statements, метаданные, workspace, auth
├── components/
│   ├── Header.svelte         # Шапка: выбор кластера, статус сессии Livy, YARN App ID, профиль пользователя
│   ├── SessionBar.svelte     # Панель управления сессией: Start/Stop, выбор языка (PySpark / Scala / SQL), кнопка Configure
│   ├── ConfigModal.svelte    # Модальное окно конфигурации сессии (версии Spark/Python, очереди YARN, JARs, PyFiles)
│   ├── Sidebar.svelte        # Дерево каталога метаданных (Hive Metastore / Iceberg: DB -> Tables -> Columns)
│   ├── Editor.svelte         # Monaco Editor с поддержкой нескольких вкладок, горячих клавиш и сниппетов
│   ├── ResultsView.svelte    # Панель вывода результатов: таблица строк с пагинацией, лог сессии, блок ошибок
│   ├── QueueView.svelte      # Мониторинг очереди задач и истории выполнения с возможностью отмены
│   └── LoginModal.svelte     # Модальное окно входа с поддержкой Kerberos SPNEGO SSO и LDAPS
├── types.ts                  # TypeScript интерфейсы: SessionConfig, Statement, Tab, TabResultData, Catalog
├── app.css                   # Глобальные стили Tailwind 4 и темы Monaco
├── App.svelte                # Главный контейнер приложения, управление вкладками, буферами языков и синхронизация с БД
└── main.ts                   # Точка входа в SPA
```

---

## 💡 Ключевые возможности интерфейса

### 1. Мульти-движок и изоляция результатов в рамках одной вкладки
- Каждая вкладка (`Tab`) поддерживает мгновенное переключение между тремя языками:
  - 🐍 **PySpark** (Python API)
  - ☕ **Scala Spark** (JVM Shell)
  - 🗄️ **Spark SQL** (ANSI SQL запросы к каталогу)
- **Изолированные буферы**:
  - `codeBuffers`: сохраняет отдельный текст скрипта для каждого языка.
  - `resultBuffers`: сохраняет отдельный снимок результатов выполнения (таблица строк, столбцы, лог, ошибки, затраченное время) для каждого языка. При переключении языка результаты не смешиваются и восстанавливаются автоматически.

### 2. Персистентность рабочих пространств (`User Workspace`)
- Вкладки, открытый код, настройки сессий и последние результаты сохраняются на сервере в базе данных (`/api/v1/workspace`).
- При входе под разными пользователями загружается личное рабочее пространство текущего пользователя, гарантируя полную изоляцию данных и истории.

### 3. Интерактивная конфигурация сессий Livy
- Выбор целевой очереди YARN Capacity Scheduler с фильтрацией по правам доступа пользователя.
- Выбор версий Apache Spark (2.4, 3.2, 3.5) и Python-окружений (Conda / Venv в HDFS).
- Добавление сторонних зависимостей:
  - Пути к JAR-файлам в HDFS.
  - Координаты пакетов Maven (например, `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`).
  - Python-архивы (`.whl`, `.egg`, `.zip`).
- Подключение каталогов Hive Metastore и Apache Iceberg.

### 4. Навигатор метаданных (Catalog Sidebar)
- Просмотр доступных баз данных и таблиц Hive Metastore.
- Вставка сниппетов кода в редактор по клику на таблицу с автоматической адаптацией под выбранный язык:
  - PySpark: `df = spark.table("db.table")`
  - Scala: `val df = spark.table("db.table")`
  - SQL: `SELECT * FROM db.table LIMIT 50;`

---

## 🚀 Команды разработки

```bash
# Установка зависимостей (из корня frontend)
npm install

# Запуск dev-сервера с HMR
cd apps/spark && npm run dev
# Откроется http://localhost:5174 (прокси на бэкенд :8004)

# Продакшн-сборка
npm run build

# Проверка типов
npm run check
```

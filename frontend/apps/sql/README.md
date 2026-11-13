# Фронтенд SQL Web Explorer

Веб-интерфейс для запросов к Trino и Apache Hive на Svelte 5 (runes), TypeScript, Monaco Editor и Tailwind CSS.

---

## Технологический стек

| Компонент | Технология |
|---|---|
| Фреймворк | [Svelte 5](https://svelte.dev/), runes: `$state`, `$derived`, `$props`, `$effect` |
| Сборщик | [Vite](https://vitejs.dev/) с HMR |
| Редактор кода | [Monaco Editor](https://microsoft.github.io/monaco-editor/) (редактор из VS Code): подсветка SQL, автодополнение, горячие клавиши |
| Стили | [Tailwind CSS](https://tailwindcss.com/): светлая тема, скроллбары 6px, шрифты `Inter` и `JetBrains Mono` |
| Иконки | [Lucide Svelte](https://lucide.dev/) |
| Связь с сервером | REST API и Server-Sent Events (SSE) для статусов и прогресса запросов |

---

## Архитектура компонентов (`src/`)

```
frontend/apps/sql/src/
├── api/
│   └── client.ts              # Типизированный клиент API, методы ИИ-ассистента и SSE стриминг
├── components/
│   ├── AIAssistantModal.svelte # Интерактивная модальная панель ИИ (Линтер, Explain, Optimize, Fix)
│   ├── Sidebar.svelte         # Дерево схемы (Каталог -> Схема -> Таблица -> Колонки)
│   ├── SqlEditor.svelte       # Monaco Editor с поддержкой маркеров ИИ, мульти-запросов и шорткатов
│   ├── QueryToolbar.svelte    # Тулбар запуска («Run», «Stop», «ИИ Генератор», «Анализ», метрики времени и строк)
│   ├── ResultsGrid.svelte     # Таблица результатов, экспорт и кнопка «Исправить с ИИ»
│   └── QueueView.svelte       # Фоновая очередь задач, отмена и загрузка архива результатов
│   (Header с селектором кластеров и бейджами ролей ADM/RW/RO, LoginModal подключаются из @pristan/common)
├── utils/
│   └── sqlSplitter.ts         # Утилита парсинга и разбивки мульти-запросов по точкам с запятой
├── types.ts                   # TypeScript интерфейсы и типы данных (включая AI схемы)
├── app.css                    # Глобальные CSS-переменные темы и стили скроллбаров
├── App.svelte                 # Главный контейнер приложения, вкладки запросов и раскладка
└── main.ts                    # Точка входа приложения
frontend/apps/sql/tests/       # Модульные и компонентные тесты (Vitest + Testing Library)
├── query_toolbar.test.ts      # Тесты тулбара запуска, отмены, таймингов и ИИ-кнопок
├── results_grid.test.ts       # Тесты таблицы результатов, фильтрации, пагинации и ошибок
├── queue_view.test.ts         # Тесты очереди фоновых запросов Trino/Hive
└── ai_assistant_modal.test.ts # Тесты модалки ИИ-генератора с шаблонами Text-to-SQL
```

---

## Функции интерфейса

- Вкладки:
  - несколько запросов на разных вкладках;
  - цепочки запросов, разделенные `;`;
  - запуск выделенного фрагмента или запроса под курсором (`Cmd+Enter` / `Ctrl+Enter`).
- ИИ-ассистент и линтер:
  - ошибки и антипаттерны подчеркиваются волнистой линией в редакторе Monaco;
  - переход к строке с ошибкой по клику;
  - разбор логики запроса (Explain) и оптимизация (Optimize) с показом изменений;
  - исправление ошибки из окна результатов (кнопка **«Исправить с ИИ»**).
- Дерево схемы (Schema Explorer):
  - каталоги, схемы и таблицы Trino и Hive загружаются при раскрытии узла;
  - клик по таблице вставляет `SELECT ... FROM ... LIMIT 50`.
- Очередь запросов (Background Execution):
  - запросы выполняются на сервере асинхронно; состояние сохраняется после перезагрузки страницы или закрытия браузера;
  - кнопка отмены прерывает выполнение на кластере;
  - сохраненные результаты скачиваются в сжатом виде.
- Уведомления (Desktop Notifications): уведомление ОС о завершении запроса.


---

## Команды разработки

### Установка зависимостей
```bash
npm install
```

### Запуск сервера разработки
```bash
npm run dev
```
Сервер разработки слушает `http://localhost:5173`, запросы к `/api` проксируются на бэкенд `http://localhost:8003`.

### Сборка
```bash
npm run build
```
Бандл собирается в `backend/sql/src/main/resources/static` (`build.outDir` в `vite.config.ts`) и попадает в JAR бэкенда, который раздает его через Spring Boot.

### Локальный предпросмотр сборки
```bash
npm run preview
```

### Запуск тестов компонентов
```bash
# Запуск тестов компонентов SQL Explorer
npx vitest run apps/sql/tests

# Запуск с отслеживанием изменений (watch-режим)
npx vitest apps/sql/tests
```

### Проверка типов (TypeScript / Svelte)
```bash
npx svelte-check --tsconfig ./tsconfig.json
```


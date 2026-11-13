# Frontend: Sklad (HDFS)

Фронтенд Sklad (HDFS) на Svelte 5 (Runes), Vite 8 и Tailwind CSS v4: навигация по каталогам HDFS, загрузка и скачивание файлов, предпросмотр Parquet, ORC, CSV и TSV.

---

## Структура каталогов

```
frontend/apps/sklad/
├── src/
│   ├── lib/
│   │   ├── api/                 # Клиент API (Fetch-обертка, CSRF-заголовки)
│   │   ├── components/          # UI-компоненты (FileList, Breadcrumbs, Modals)
│   │   │   └── Modals/          # Модальные окна (Upload, Mkdir, Delete, Preview, CrossClusterCopy)
│   │   ├── stores/              # Реактивные состояния Svelte 5 (authStore, explorerStore)
│   │   ├── types/               # TypeScript-интерфейсы HDFS и пользователя
│   │   └── utils/               # Утилиты форматирования размеров файлов и дат
│   │   (Компоненты Header с метками ролей ADM/RW/RO и LoginModal подключаются из @pristan/common)
│   ├── App.svelte               # Корневой компонент интерфейса
│   ├── app.css                  # Подключение Tailwind CSS v4 и theme.css
│   └── main.ts                  # Точка входа приложения
├── tests/                       # Модульные и компонентные тесты (Vitest + Testing Library)
│   ├── breadcrumbs.test.ts      # Тесты хлебных крошек и навигации
│   ├── action_toolbar.test.ts   # Тесты тулбара (загрузка, папки, поиск, статистика)
│   ├── batch_action_bar.test.ts # Тесты панели пакетных действий
│   ├── file_list.test.ts        # Тесты таблицы файлов, сортировки и состояний
│   └── modals.test.ts           # Тесты модальных окон Mkdir, Delete, Rename
├── index.html                   # Базовый HTML-шаблон
├── package.json                 # Зависимости (@tailwindcss/vite, lucide-svelte, svelte)
├── tsconfig.json                # Конфигурация TypeScript
└── vite.config.ts               # Конфигурация сборщика Vite и dev-прокси на backend:8000
```

---

## Защита на стороне клиента

- XSS: сессионные токены не пишутся в `localStorage`, браузер передает их в cookie с флагом `HttpOnly`, недоступной из JavaScript.
- CSRF: API-клиент добавляет к каждому запросу заголовок `X-Requested-With: XMLHttpRequest`.
- Предпросмотр: содержимое файла выводится как текст или таблица, HTML не интерпретируется (с санитизацией).

---

## Запуск и сборка

### 1. Установка зависимостей
```bash
# Из корня frontend
npm install
```

### 2. Запуск локального сервера разработки с HMR
```bash
cd apps/sklad && npm run dev
```

### 3. Проверка типов
```bash
npm run check
```

### 4. Запуск UI и компонентных тестов
```bash
# Из корня frontend
npx vitest run apps/sklad/tests
```

### 5. Сборка production-бандла
```bash
npm run build
```

# Frontend: HDFS Web Explorer

Фронтенд веб-портала **HDFS Web Explorer**, разработанный на базе **Svelte 5** (Runes), **Vite 8** и **Tailwind CSS v4**. Обеспечивает отзывчивый интерфейс для навигации по файловой структуре Apache Hadoop HDFS, загрузки, скачивания и быстрого предпросмотра файлов аналитических форматов (Parquet, ORC, Avro).

---

## 🏛 Архитектура компонентов фронтенда

```
frontend/apps/hdfs/
├── src/
│   ├── lib/
│   │   ├── api/                 # Клиент API (Fetch обертка, CSRF заголовки)
│   │   ├── components/          # UI компоненты (FileList, Breadcrumbs, Modals)
│   │   │   └── Modals/          # Модальные окна (Upload, Mkdir, Delete, Preview)
│   │   ├── stores/              # Реактивные состояния Svelte 5 (authStore, explorerStore)
│   │   ├── types/               # TypeScript интерфейсы HDFS и пользователя
│   │   └── utils/               # Утилиты форматирования размеров файлов и дат
│   │   (Header с бейджами ролей ADM/RW/RO и LoginModal подключаются из @hadoop-explorer/common)
│   ├── App.svelte               # Корневой компонент интерфейса
│   ├── app.css                  # Подключение Tailwind CSS v4 и theme.css
│   └── main.ts                  # Точка входа приложения
├── index.html                   # Базовый HTML-шаблон
├── package.json                 # Зависимости (@tailwindcss/vite, lucide-svelte, svelte)
├── tsconfig.json                # Конфигурация TypeScript
└── vite.config.ts               # Конфигурация сборщика Vite и dev-прокси на backend:8002
```

---

## 🛡️ Безопасность фронтенда

1. **Защита от XSS (Cross-Site Scripting)**:
   - Сессионные токены не сохраняются в небезопасном `localStorage` и передаются через защищенные `HttpOnly` cookie.
2. **Защита от CSRF**:
   - Каждый исходящий запрос автоматически снабжается заголовком `X-Requested-With: XMLHttpRequest` в API-клиенте.
3. **Безопасный предпросмотр**:
   - Предпросмотр содержимого файлов отображается как безопасный форматированный текст/таблица без интерпретации HTML (`sanitize`).

---

## 🚀 Запуск и сборка

```bash
# Установка зависимостей (из корня frontend)
npm install

# Запуск локального сервера разработки с HMR
cd apps/hdfs && npm run dev
```

# Проверка типов
npm run check

# Сборка production бандла
npm run build
```

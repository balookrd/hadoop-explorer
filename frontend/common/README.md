# Hadoop Explorer Common Frontend (`@hadoop-explorer/common`)

Пакет общих компонентов интерфейса, API-клиентов, утилит и типов данных для фронтенд-приложений платформы **Hadoop Explorer** (Svelte 5 + Tailwind CSS v4).

## 📦 Содержимое пакета

### 1. Компоненты (`components/`)
- **`Header.svelte`** — единая корпоративная шапка приложения:
  - Логотип и название сервиса (со слотом под иконку Lucide).
  - Селектор кластеров и индикатор пользователя (`doAs: <username>`).
  - Профиль пользователя: аватар, `display_name`, `@username`, `auth_method` (SSO/LDAP).
  - Выпадающая карточка с LDAP-группами, системной ролью (`ADMIN`, `WRITER`, `READER`) и кнопкой «Выйти из системы» (`onLogout`).
  - Слот `extraActions` для размещения кнопок конкретного приложения.
- **`LoginModal.svelte`** — модальное окно / полноэкранная форма аутентификации:
  - Вход через Kerberos SPNEGO SSO (`onKerberosSso`).
  - Форма ввода учетной записи LDAP и пароля (`onLogin`).
  - Быстрый выбор тестовых пользователей (`mockUsers`) для демонстрационных стендов.
  - Обработка и отображение ошибок аутентификации.
- **`Modal.svelte`** — базовый переиспользуемый компонент диалогового окна:
  - Backdrop с эффектом `backdrop-blur` и закрытием по клику вне окна.
  - Автоматическое закрытие по нажатию клавиши `Escape`.
  - Стандартизированная шапка, тело скролла и подвал (`footer`).
- **`StatusBadge.svelte`** — индикатор статуса задач и сессий (Success, Warning, Error, Running, Queued).
- **`NotificationToast.svelte`** — всплывающие уведомления.

### 2. Утилиты (`utils/`)
- **`sqlSplitter.ts`** — высокопроизводительный анализатор и парсер SQL-скриптов:
  - `splitSqlStatements(sql: string)` — разбиение скрипта на отдельные запросы с учетом строковых литералов (`'...'`, `"..."`), бэктиков (`` `...` ``), однострочных (`--`) и многострочных (`/* ... */`) комментариев.
  - `getStatementAtCursor(sql: string, cursorOffset: number)` — точное определение запроса под курсором каретки для выполнения по `Ctrl+Enter`.
  - `sanitizeSql(sql: string)` — очистка от завершающих точек с запятой и лишних пробелов.
- **`useResizable.svelte.ts`** — Svelte 5 хелперы для Drag & Drop изменения размеров:
  - `createHorizontalResizable(options)` — горизонтальный ресайз сайдбара с фиксацией минимальной/максимальной ширины.
  - `createVerticalResizable(options)` — вертикальный ресайз высоты редактора кода и панели результатов.

### 3. API-клиент (`api/client.ts`)
- **`BaseApiClient`** — базовый класс для всех сетевых клиентов:
  - Zero LocalStorage архитектура: токены хранятся только в памяти сессии.
  - Cookie-First аутентификация с поддержкой `credentials: 'include'`.
  - Автоматическая CSRF-защита (заголовок `X-Requested-With: XMLHttpRequest`).
  - Прозрачная обработка HTTP 401 с фоновым реконнектом через Kerberos SSO (`/auth/sso`).
  - Подписка на истечение сессий (`onUnauthorized`).

### 4. Типы (`types/`)
- **`types/auth.ts`** — интерфейсы `UserSession`, `UserInfo`, `TokenResponse`, `ClusterPublicInfo`.
- **`types/generated/`** — автоматически сгенерированные TypeScript-типы из OpenAPI схем FastAPI (`hdfs.ts`, `spark.ts`, `sql.ts`, `yarn.ts`).

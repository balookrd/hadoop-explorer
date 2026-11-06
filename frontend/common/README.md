# Hadoop Explorer Common Frontend (`@hadoop-explorer/common`)

Общие компоненты интерфейса, API-клиент, утилиты и типы для фронтенд-приложений Hadoop Explorer (Svelte 5, Tailwind CSS v4).

## Содержимое пакета

### Компоненты (`components/`)
- `Header.svelte` — шапка приложения:
  - Логотип и название сервиса (со слотом под иконку Lucide).
  - Селектор кластеров и индикатор пользователя (`doAs: <username>`).
  - Профиль пользователя: аватар и бейдж действующей роли (`ADM`, `RW`, `RO`) на кнопке шапки.
  - Выпадающая карточка с полным именем (`display_name`), `@username`, типом авторизации (SSO/LDAP), списком LDAP-групп, полной системной ролью (`ADMIN`, `WRITER`, `READER`) и кнопкой **Выйти из системы** (`onLogout`).
  - Слот `extraActions` для кнопок и виджетов конкретного приложения (например, `SparkSessionWidget` в Spark Explorer).
- `LoginModal.svelte` — модальное окно или полноэкранная форма входа:
  - Используется всеми SPA платформы.
  - Вход через Kerberos SPNEGO SSO (`onKerberosSso`).
  - Форма ввода учетной записи LDAP и пароля (`onLogin`).
  - Выбор тестовых пользователей (`mockUsers`) для демонстрационных стендов.
  - Показ ошибок аутентификации.
- `Modal.svelte` — базовое диалоговое окно:
  - Backdrop с эффектом `backdrop-blur` и закрытием по клику вне окна.
  - Закрытие по клавише `Escape`.
  - Шапка, прокручиваемое тело и подвал (`footer`).
- `StatusBadge.svelte` — индикатор статуса задач и сессий (Success, Warning, Error, Running, Queued).
- `NotificationToast.svelte` — всплывающие уведомления.

### Стили и темы (`theme.css`)
- Общая для всех SPA палитра темной и светлой тем, шрифт `Inter`, отступы, радиусы скругления и стили кнопок.
- CSS-переменные и классы (`btn-primary`, `btn-secondary`, `badge-*`), одинаковые во всех приложениях.

### Утилиты (`utils/`)
- `sqlSplitter.ts` — разбор SQL-скриптов:
  - `splitSqlStatements(sql: string)` — разбиение скрипта на отдельные запросы с учетом строковых литералов (`'...'`, `"..."`), бэктиков (`` `...` ``), однострочных (`--`) и многострочных (`/* ... */`) комментариев.
  - `getStatementAtCursor(sql: string, cursorOffset: number)` — запрос под курсором для выполнения по `Ctrl+Enter`.
  - `sanitizeSql(sql: string)` — очистка от завершающих точек с запятой и лишних пробелов.
- `useResizable.svelte.ts` — хелперы Svelte 5 для изменения размеров панелей перетаскиванием:
  - `createHorizontalResizable(options)` — изменение ширины боковой панели в пределах минимальной и максимальной ширины.
  - `createVerticalResizable(options)` — изменение высоты редактора кода и панели результатов.

### API-клиент (`api/client.ts`)
- `BaseApiClient` — базовый класс сетевых клиентов:
  - Токен не хранится в `localStorage`; при создании клиент удаляет оставшийся там ключ `access_token`.
  - Сессия передается в cookie, запросы отправляются с `credentials: 'include'`.
  - Заголовок `X-Requested-With: XMLHttpRequest` для CSRF-защиты добавляется к каждому запросу.
  - При HTTP 401 клиент в фоне повторяет вход через Kerberos SSO (`/auth/sso`).
  - Подписка на истечение сессий (`onUnauthorized`).

### Типы (`types/`)
- `types/auth.ts` — интерфейсы `UserSession`, `UserInfo`, `TokenResponse`, `ClusterPublicInfo`.
- `types/generated/` — TypeScript-типы, сгенерированные `openapi-typescript` (`yarn.ts`, `hdfs.ts`, `sql.ts`, `spark.ts`). Приложения их не импортируют. Типы не перегенерируются: цели `make generate-types`, на которую ссылается комментарий в `types/generated/index.ts`, в `Makefile` нет, а Java-бэкенды не отдают OpenAPI-схему (springdoc в `pom.xml` не подключен). Содержимое файлов может не совпадать с текущим API.

### Тесты (`tests/`)
- `tests/components.test.ts` — юнит- и интеграционные тесты общих UI-компонентов:
  - `Header`: рендеринг логотипа, индикатора пользователя, бейджа роли (`ADM`, `RW`, `RO`), выпадающей карточки профиля, слота `extraActions`, обработка клика выхода (`onLogout`).
  - `LoginModal`: ввод учетных данных LDAP, валидация полей, клики выбора демонстрационных пользователей (`mockUsers`), переключение на Kerberos SSO, отображение ошибок авторизации.
  - `Modal`: рендеринг заголовка, тела и слота подвала, проверка вызова `onClose` при клике на крестик, фон (backdrop) и при нажатии клавиши `Escape`.
  - `StatusBadge`: цвет и текст для каждого статуса выполнения.
  - `NotificationToast`: отображение текста тостов и авто-скрытие.
- `tests/apps_smoke.test.ts` — smoke-тест монтирования корневых контейнеров `App.svelte` всех четырёх SPA-приложений платформы (YARN, HDFS, SQL, Spark) в среде jsdom.
- `tests/auth_flow.test.ts` — жизненный цикл авторизации, обновления токенов и смены учетных записей.
- `tests/client.test.ts` — тесты `BaseApiClient`:
  - Отправка запросов с `credentials: 'include'` и заголовком `X-Requested-With`.
  - Перехват HTTP 401 и вызов коллбэка `onUnauthorized`.

---

## Запуск тестов

Команды выполняются из каталога `frontend/`.

```bash
# Запуск всех тестов пакета common
npx vitest run common/tests

# Запуск тестов компонентов
npx vitest run common/tests/components.test.ts

# Запуск тестов сетевого клиента
npx vitest run common/tests/client.test.ts

# Запуск всех UI-тестов фронтенд-монорепозитория
npm run test:unit
```


# Frontend: YARN Queue Explorer

Клиентская часть приложения **YARN Queue Explorer**, разработанная на современном стеке **Svelte 5 (с рунами `$state`, `$derived`, `$props`)**, **Vite** и **Tailwind CSS**.

---

## 🎨 Архитектура компонентов фронтенда

```
frontend/src/
├── api/                         # Клиент взаимодействия с REST API бэкенда
│   └── client.ts                # HTTP клиент (Fetch + Bearer JWT, перехват 401)
├── components/                  # UI компоненты
│   ├── QueueTreeTable.svelte    # Главная древовидная таблица иерархии очередей
│   ├── QueueRow.svelte          # Строка очереди: емкость, лимиты, Policy, RAM/vCPU
│   ├── QueueEditDrawer.svelte   # Боковая панель детального редактирования очереди
│   ├── AddQueueModal.svelte     # Модальное окно добавления дочерней очереди
│   ├── DiffPanel.svelte         # Панель Unified Diff (Live vs Draft)
│   ├── XmlExportModal.svelte    # Модальное окно экспорта capacity-scheduler.xml
│   ├── QueueMappingsModal.svelte# Конструктор правил сопоставления очередей
│   ├── ChangeRequestsModal.svelte # Центр согласования заявок (Approval Workflow)
│   ├── CapacityBar.svelte       # Компонент визуализации прогресс-бара ресурсов
│   └── Header.svelte            # Шапка: выбор кластера, профиль, индикатор заявок
├── utils/                       # Утилиты форматирования и вычислений
│   ├── formatters.ts            # Форматирование MB/GB/TB, ядер vCPU и процентов
│   └── tree.ts                  # Построение дерева очередей и пересчет емкости
├── types.ts                     # TypeScript интерфейсы (QueueNode, Cluster, Diff и др.)
├── App.svelte                   # Корневой компонент приложения с роутингом состояний
└── main.ts                      # Входная точка Svelte приложения
```

---

## ⚡️ Ключевые возможности интерфейса

1. **Реактивность Svelte 5 (Runes)**:
   - Использование `$state` для состояния очередей, черновиков (drafts) и активного кластера.
   - Использование `$derived` для мгновенного пересчета суммарной емкости веток, RAM/vCPU и баланса инварианта 100%.
2. **Двусторонний автопересчет ресурсов**:
   - Переключение между процентами (%) и абсолютными физическими значениями (GB / vCores).
   - Связанный режим (**Linked Mode**): синхронное изменение памяти и вычислительных ядер.
3. **Визуализация политик и лимитов очередей**:
   - Столбец **Policy** с интерактивным бейджем политики планирования (`FAIR · 0.2x` или `FIFO · 1.0x`).
   - Настройка Application Limits (`maximum-applications`, `maximum-am-resource-percent`, `max-parallel-apps`, `maximum-application-lifetime`).
4. **Конструктор Queue Mappings**:
   - Визуальное добавление и сортировка правил (`u:%user`, `g:%group`) с выбором очередей из дерева.
   - Поддержка режима быстрого редактирования Raw-строки.
5. **Центр согласования заявок (Change Requests)**:
   - Разделение прав доступа: операторы (**WRITER**) создают заявки с обоснованием, администраторы (**ADMIN**) одобряют или отклоняют их.

---

## 💻 Разработка и сборка

### 1. Установка зависимостей
```bash
cd frontend
npm install
```

### 2. Запуск сервера разработки (с HMR)
```bash
npm run dev
```
Фронтенд запустится на `http://localhost:5173` с автоматическим проксированием запросов `/api` на бэкенд (`http://localhost:8080`).

### 3. Проверка типов и линтинг
```bash
npm run check
```

### 4. Production-сборка
```bash
npm run build
```
Скомпилированные статические файлы сохраняются в директорию `frontend/dist`, которая раздается FastAPI бэкендом в production-режиме.

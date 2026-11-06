# Frontend: YARN Queue Explorer

Клиентская часть YARN Queue Explorer: Svelte 5 (руны `$state`, `$derived`, `$props`), Vite, Tailwind CSS.

## Структура каталога

```
frontend/apps/yarn/src/
├── api/                         # Клиент взаимодействия с REST API бэкенда
│   └── client.ts                # HTTP-клиент (Fetch + Bearer JWT, перехват 401)
├── components/                  # UI-компоненты
│   ├── QueueTreeTable.svelte    # Главная древовидная таблица иерархии очередей и ресурсов
│   ├── QueueEditDrawer.svelte   # Боковая панель детального редактирования очереди
│   ├── AddQueueModal.svelte     # Модальное окно добавления дочерней очереди
│   ├── DiffPanel.svelte         # Панель Unified Diff (Live vs Draft)
│   ├── XmlExportModal.svelte    # Модальное окно экспорта правок capacity-scheduler.xml
│   ├── QueueMappingsModal.svelte# Конструктор правил сопоставления очередей
│   ├── ChangeRequestsDrawer.svelte # Центр согласования заявок (Approval & AWX Deployment)
│   ├── ClusterMetricsBar.svelte # Верхняя панель агрегированных метрик кластера
│   ├── ResourceBalanceCard.svelte # Карточка валидации баланса 100% емкости
│   ├── PartitionSelector.svelte # Селектор разделов кластера (Node Labels)
│   └── SubmitChangeRequestModal.svelte # Модальное окно создания заявки Change Request
│   (Header с бейджами ролей ADM/RW/RO и LoginModal подключаются из @hadoop-explorer/common)
├── utils/                       # Утилиты форматирования и вычислений
│   └── resourceUtils.ts         # Форматирование MB/GB/TB, ядер vCPU, пересчет квот и расчет баланса
├── types.ts                     # TypeScript-интерфейсы (QueueNode, Cluster, Diff, Deploy и др.)
├── App.svelte                   # Корневой компонент приложения с роутингом состояний
└── main.ts                      # Точка входа Svelte-приложения
frontend/apps/yarn/tests/        # Модульные и компонентные тесты (Vitest + Testing Library)
├── cluster_metrics_bar.test.ts  # Тесты метрик кластера (память, ядра, узлы, контейнеры)
├── partition_selector.test.ts   # Тесты селектора разделов (партиций) узлов
├── resource_balance_card.test.ts# Тесты балансировки долей и перераспределения
├── diff_panel.test.ts           # Тесты Unified Diff (Live vs Draft)
└── modals.test.ts               # Тесты модальных окон AddQueueModal и XmlExportModal
```

## Функции интерфейса

1. Состояние на рунах Svelte 5:
   - `$state` хранит очереди, черновики (drafts) и активный кластер.
   - `$derived` пересчитывает суммарную емкость веток, RAM/vCPU и проверку баланса 100% при каждом изменении черновика.
2. Двусторонний пересчет ресурсов:
   - Переключение между процентами (%) и абсолютными физическими значениями (GB / vCores).
   - Связанный режим (Linked Mode): изменение доли памяти синхронно меняет долю vCPU.
3. Политики и лимиты очередей:
   - Столбец **Policy** с кликабельным бейджем политики планирования (`FAIR · 0.2x` или `FIFO · 1.0x`).
   - Настройка Application Limits (`maximum-applications`, `maximum-am-resource-percent`, `max-parallel-apps`, `maximum-application-lifetime`).
4. Конструктор Queue Mappings:
   - Добавление и сортировка правил (`u:%user`, `g:%group`) с выбором очереди из дерева.
   - Режим редактирования правил одной Raw-строкой.
5. Центр согласования заявок (Change Requests):
   - Разделение прав: операторы (`WRITER`, бейдж `RW`) создают заявки с обоснованием, администраторы (`ADMIN`, бейдж `ADM`) одобряют или отклоняют их, аналитики (`READER`, бейдж `RO`) только просматривают.
6. Доставка и применение через Ansible AWX:
   - Запуск деплоя из карточки одобренной заявки. `XmlExportModal` показывает только перечень правок `capacity-scheduler.xml` (копирование, скачивание) и деплой не запускает; метод `deployXml` в `api/client.ts` из интерфейса не вызывается.
   - Бейдж статуса доставки: `AWX...` (выполняется), `AWX ✓` (успех), `AWX ✕` (ошибка); время деплоя и ссылка на `AWX Job #ID`.
   - Кнопка **«Лог AWX»** запрашивает `deploy-status` и показывает вывод плейбука (`stdout`) на момент запроса.
7. Разделы кластера (Partitions / Node Labels):
   - Активный раздел показан в тулбаре и в заголовках колонок ресурсов таблицы.
   - В `QueueTreeTable` бейдж `DEFAULT` не выводится; очереди, не настроенные для выбранного раздела, показаны приглушенной строкой с зачеркнутым названием (`line-through`).
   - Селектор разделов в `QueueEditDrawer`: статус раздела на кнопке (`●` настроен, `○` нет квот), подсказки, кнопки добавления (`+ Настроить раздел`) и удаления квот раздела.
   - Окно «Changes Review» (`DiffPanel`) показывает изменения ресурсов по всем разделам, разрешенных меток (`accessible-node-labels`) и метки по умолчанию (`default-node-label-expression`).

Руководство пользователя: [docs/yarn-user-guide.md](../../../docs/yarn-user-guide.md). Доставка конфигурации через AWX: [docs/awx-yarn-deployment.md](../../../docs/awx-yarn-deployment.md).

## Разработка и сборка

### 1. Установка зависимостей
```bash
# Из корня frontend
npm install
```

### 2. Запуск сервера разработки (с HMR)
```bash
cd apps/yarn && npm run dev
```
Dev-сервер слушает `http://localhost:5173` и проксирует запросы `/api` на бэкенд `http://localhost:8000`.

### 3. Проверка типов
```bash
npm run check
```

### 4. Запуск UI и компонентных тестов
```bash
# Из корня frontend
npx vitest run apps/yarn/tests
```

### 5. Production-сборка
```bash
npm run build
```
Сборка складывается в `dist`, затем упаковывается в Spring Boot Fat JAR (`backend/yarn/src/main/resources/static`), из которого бэкенд раздает статику.

#!/usr/bin/env python3
"""
Скрипт генерации строго технической интерактивной презентации docs/replicator-presentation.html
на 13 слайдов для DevOps-инженеров, Архитекторов и Data Platform команд.
"""

import os

HTML_CONTENT = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Hadoop gRPC Replicator: Системная архитектура и эксплуатация (Техническая спецификация)</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(22, 30, 49, 0.85);
      --card-border: rgba(255, 255, 255, 0.08);
      --primary: #6366f1;
      --primary-light: #818cf8;
      --accent: #06b6d4;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --code-bg: #0b1120;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      background: radial-gradient(circle at 50% 10%, #1e1b4b 0%, var(--bg) 60%);
      color: var(--text);
      min-height: 100vh;
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      user-select: none;
    }

    header {
      height: 54px;
      min-height: 54px;
      padding: 0 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--card-border);
      background: rgba(9, 13, 22, 0.9);
      backdrop-filter: blur(12px);
      z-index: 100;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      font-weight: 700;
      font-size: 1.05rem;
      letter-spacing: -0.01em;
    }

    .brand img {
      height: 28px;
      filter: drop-shadow(0 0 8px rgba(99, 102, 241, 0.4));
    }

    .brand-tag {
      background: linear-gradient(135deg, #4f46e5, #06b6d4);
      color: white;
      font-size: 0.68rem;
      padding: 3px 8px;
      border-radius: 999px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .header-controls {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .slide-counter {
      font-size: 0.88rem;
      color: var(--text-muted);
      font-weight: 600;
      min-width: 80px;
      text-align: center;
    }

    .btn {
      background: rgba(255, 255, 255, 0.06);
      border: 1px solid var(--card-border);
      color: var(--text);
      padding: 6px 12px;
      border-radius: 8px;
      cursor: pointer;
      font-size: 0.82rem;
      font-weight: 500;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.15s ease;
    }

    .btn:hover {
      background: rgba(255, 255, 255, 0.12);
      border-color: var(--primary-light);
      transform: translateY(-1px);
    }

    .btn-primary {
      background: linear-gradient(135deg, #4f46e5, #4338ca);
      border: 1px solid #6366f1;
      color: white;
      box-shadow: 0 4px 14px rgba(79, 70, 229, 0.35);
    }

    .progress-bar-container {
      width: 100%;
      height: 3px;
      background: rgba(255, 255, 255, 0.05);
      position: relative;
    }

    .progress-bar {
      height: 100%;
      background: linear-gradient(90deg, #6366f1, #06b6d4, #10b981);
      width: 0%;
      transition: width 0.3s ease;
      box-shadow: 0 0 10px rgba(6, 182, 212, 0.5);
    }

    main {
      flex: 1;
      position: relative;
      overflow: hidden;
      display: flex;
      align-items: stretch;
      justify-content: stretch;
      padding: 16px 28px;
    }

    .slide {
      position: absolute;
      top: 16px;
      left: 28px;
      right: 28px;
      bottom: 16px;
      opacity: 0;
      visibility: hidden;
      transform: translateX(30px) scale(0.99);
      transition: all 0.28s cubic-bezier(0.16, 1, 0.3, 1);
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .slide.active {
      opacity: 1;
      visibility: visible;
      transform: translateX(0) scale(1);
    }

    .slide.prev {
      transform: translateX(-30px) scale(0.99);
    }

    .slide-header {
      margin-bottom: 12px;
      flex-shrink: 0;
    }

    .slide-subtitle {
      color: var(--accent);
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 3px;
    }

    .slide-title {
      font-size: 1.75rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      line-height: 1.2;
      background: linear-gradient(135deg, #ffffff 60%, #cbd5e1);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .slide-desc {
      color: var(--text-muted);
      font-size: 0.88rem;
      margin-top: 3px;
      line-height: 1.35;
    }

    .slide-body {
      flex: 1;
      min-height: 0;
      display: flex;
      gap: 16px;
      overflow-y: auto;
    }

    .grid-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      width: 100%;
      height: 100%;
    }

    .grid-3 {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
      width: 100%;
      height: 100%;
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 16px;
      backdrop-filter: blur(12px);
      display: flex;
      flex-direction: column;
      position: relative;
    }

    .card-accent-indigo { border-top: 3px solid var(--primary); }
    .card-accent-cyan { border-top: 3px solid var(--accent); }
    .card-accent-green { border-top: 3px solid var(--success); }
    .card-accent-amber { border-top: 3px solid var(--warning); }
    .card-accent-red { border-top: 3px solid var(--danger); }

    .card-title {
      font-size: 1rem;
      font-weight: 700;
      color: #fff;
      margin-bottom: 10px;
      display: flex;
      align-items: center;
      gap: 8px;
      flex-shrink: 0;
    }

    .card-list {
      list-style: none;
      display: flex;
      flex-direction: column;
      gap: 6px;
      overflow-y: auto;
    }

    .card-list li {
      font-size: 0.85rem;
      color: var(--text-muted);
      line-height: 1.45;
      position: relative;
      padding-left: 14px;
    }

    .card-list li::before {
      content: "•";
      color: var(--accent);
      position: absolute;
      left: 0;
      font-weight: bold;
    }

    .code-box {
      background: var(--code-bg);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 8px;
      padding: 10px 14px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.78rem;
      color: #38bdf8;
      overflow-x: auto;
      line-height: 1.45;
      margin-top: 8px;
      white-space: pre-wrap;
    }

    .arch-container {
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 10px;
      cursor: zoom-in;
      position: relative;
      overflow: hidden;
    }

    .arch-container img {
      max-width: 100%;
      max-height: calc(100vh - 190px);
      width: auto;
      height: auto;
      object-fit: contain;
      border-radius: 6px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    }

    .arch-zoom-hint {
      position: absolute;
      bottom: 14px;
      right: 18px;
      background: rgba(15, 23, 42, 0.88);
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 0.75rem;
      color: var(--accent);
      font-weight: 600;
      backdrop-filter: blur(8px);
      pointer-events: none;
    }

    .table-container {
      width: 100%;
      height: 100%;
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow-y: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }

    th {
      background: #1e293b;
      color: var(--accent);
      font-size: 0.8rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 10px 14px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      position: sticky;
      top: 0;
      z-index: 10;
    }

    td {
      padding: 9px 14px;
      font-size: 0.83rem;
      color: var(--text-muted);
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
    }

    tr:nth-child(even) td { background: rgba(255, 255, 255, 0.015); }
    tr:hover td { background: rgba(99, 102, 241, 0.06); color: var(--text); }

    .port-badge {
      display: inline-block;
      padding: 2px 6px;
      border-radius: 4px;
      font-family: monospace;
      font-weight: 700;
      font-size: 0.8rem;
    }

    .port-wan { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .port-control { background: rgba(99, 102, 241, 0.2); color: #818cf8; border: 1px solid rgba(99, 102, 241, 0.4); }
    .port-lan { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }

    .ui-split {
      display: grid;
      grid-template-columns: minmax(360px, 420px) 1fr;
      gap: 16px;
      width: 100%;
      height: 100%;
      align-items: stretch;
    }

    .ui-left {
      display: flex;
      flex-direction: column;
      gap: 12px;
      height: 100%;
      overflow-y: auto;
    }

    .ui-screenshot-card {
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 8px;
      display: flex;
      align-items: center;
      justify-content: center;
      cursor: zoom-in;
      position: relative;
      overflow: hidden;
      height: 100%;
    }

    .ui-screenshot-card img {
      max-width: 100%;
      max-height: calc(100vh - 190px);
      width: auto;
      height: auto;
      object-fit: contain;
      border-radius: 6px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    }

    footer {
      height: 38px;
      min-height: 38px;
      padding: 0 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-top: 1px solid var(--card-border);
      background: rgba(9, 13, 22, 0.9);
      font-size: 0.78rem;
      color: var(--text-muted);
      z-index: 100;
    }

    .lightbox {
      position: fixed;
      top: 0; left: 0; width: 100%; height: 100%;
      background: rgba(0, 0, 0, 0.94);
      backdrop-filter: blur(16px);
      z-index: 1000;
      display: flex; align-items: center; justify-content: center;
      opacity: 0; visibility: hidden;
      transition: all 0.25s ease;
      cursor: zoom-out;
    }

    .lightbox.active { opacity: 1; visibility: visible; }
    .lightbox img {
      max-width: 95vw; max-height: 95vh;
      border-radius: 8px; box-shadow: 0 0 50px rgba(0, 0, 0, 0.8);
      border: 1px solid rgba(255, 255, 255, 0.1);
    }

    .overview-modal {
      position: fixed; top: 0; left: 0; width: 100%; height: 100%;
      background: rgba(9, 13, 22, 0.96);
      backdrop-filter: blur(16px);
      z-index: 900;
      display: flex; flex-direction: column;
      padding: 32px;
      opacity: 0; visibility: hidden;
      transition: all 0.25s ease;
    }

    .overview-modal.active { opacity: 1; visibility: visible; }
    .overview-header {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 20px; flex-shrink: 0;
    }
    .overview-grid {
      display: grid; grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
      gap: 14px; overflow-y: auto; padding-bottom: 20px;
    }
    .overview-item {
      background: var(--card-bg); border: 1px solid var(--card-border);
      border-radius: 10px; padding: 14px; cursor: pointer; transition: all 0.2s ease;
    }
    .overview-item:hover { border-color: var(--accent); transform: translateY(-2px); }
    .overview-item.current { border-color: var(--primary-light); box-shadow: 0 0 14px rgba(99, 102, 241, 0.4); }
  </style>
</head>
<body>

  <!-- ВЕРХНИЙ БАР -->
  <header>
    <div class="brand">
      <img src="images/logo_white.png" onerror="this.src='../images/logo_white.png'" alt="Hadoop Explorer Platform">
      <span>gRPC Replicator</span>
      <span class="brand-tag">Tech Specs</span>
    </div>

    <div class="header-controls">
      <button class="btn" onclick="toggleOverview()" title="Сетка слайдов (Клавиша O)">
        <span>📑 Обзор</span>
      </button>
      <div class="slide-counter" id="slideCounter">1 / 13</div>
      <button class="btn" onclick="prevSlide()" title="Предыдущий слайд (Стрелка влево)">
        <span>◀ Назад</span>
      </button>
      <button class="btn btn-primary" onclick="nextSlide()" title="Следующий слайд (Стрелка вправо / Пробел)">
        <span>Вперед ▶</span>
      </button>
      <button class="btn" onclick="toggleFullscreen()" title="На весь экран (Клавиша F)">
        <span>⛶ Экран</span>
      </button>
    </div>
  </header>

  <div class="progress-bar-container">
    <div class="progress-bar" id="progressBar"></div>
  </div>

  <!-- ОСНОВНОЙ КОНТЕЙНЕР СЛАЙДОВ -->
  <main id="slideDeck">

    <!-- ========================================== -->
    <!-- СЛАЙД 1: СИСТЕМНАЯ СПЕЦИФИКАЦИЯ -->
    <!-- ========================================== -->
    <div class="slide active" id="slide-1">
      <div class="slide-header">
        <div class="slide-subtitle">Архитектурная спецификация • DevOps / Архитектура / SRE</div>
        <h1 class="slide-title">Hadoop gRPC Replicator: Системная архитектура</h1>
        <p class="slide-desc">Физическая модель компонентов, интерфейсы взаимодействия, сетевой шейпинг и регламент Disaster Recovery</p>
      </div>
      <div class="slide-body">
        <div class="grid-3">
          <div class="card card-accent-indigo">
            <div class="card-title">⚙️ Control Plane (Orchestrator)</div>
            <ul class="card-list">
              <li><strong>Стек:</strong> Java 21 LTS, Spring Boot 3.3.4, Spring Data JPA.</li>
              <li><strong>Порт:</strong> <code>TCP :8005</code> (REST API, SSE Event Bus, Svelte 5 SPA).</li>
              <li><strong>БД метаданных:</strong> PostgreSQL 14+ / H2 (таблицы jobs, tasks, hms_jobs, lease).</li>
              <li><strong>Координация:</strong> Распределение квот Token Bucket, арбитраж Inotify Lease HA.</li>
              <li><strong>⚠️ Инвариант:</strong> Оркестратор <strong>НЕ прокачивает байты файлов</strong> через себя!</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">🚀 Data Plane (Replicator Agents)</div>
            <ul class="card-list">
              <li><strong>Стек:</strong> Java 21 LTS, Netty 4.1 gRPC Daemon (Full-Duplex).</li>
              <li><strong>Порт приемника:</strong> <code>TCP :50051</code> (gRPC HTTP/2, mTLS v1.3).</li>
              <li><strong>Протоколы LAN:</strong> Hadoop RPC (<code>:8020/:9000</code>), SASL DTP (<code>:9866</code>), Thrift (<code>:9083</code>).</li>
              <li><strong>Авторизация:</strong> Системный keytab + Kerberos <code>doAs</code> Impersonation.</li>
              <li><strong>Сжатие в канале:</strong> Потоковый Zstandard (level 3) или LZ4.</li>
            </ul>
          </div>
          <div class="card card-accent-cyan">
            <div class="card-title">🛡️ Сетевая топология и DR</div>
            <ul class="card-list">
              <li><strong>МежЦОДный фаервол:</strong> Ровно 1 открытый порт — <code>TCP :50051</code>.</li>
              <li><strong>Шейпер полосы:</strong> Hierarchical Token Bucket на Netty буферах (без iptables).</li>
              <li><strong>Авария DC1 (Fencing):</strong> Сброс WAN в 0 МБ/с, остановка задач, защита от Split-Brain.</li>
              <li><strong>Восстановление:</strong> Unfence без автозапуска + 1-Click Reverse Replication (DC2 ➔ DC1).</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 2: ГЕНЕРАЛЬНАЯ АРХИТЕКТУРНАЯ СХЕМА -->
    <!-- ========================================== -->
    <div class="slide" id="slide-2">
      <div class="slide-header">
        <div class="slide-subtitle">Генеральная схема развертывания</div>
        <h1 class="slide-title">Физическое размещение и сетевые потоки (Data & Control Flows)</h1>
        <p class="slide-desc">Сетевые сегменты Management, DC1 Primary, WAN магистраль и DC2 Standby/DR с портами и протоколами</p>
      </div>
      <div class="slide-body">
        <div class="arch-container" onclick="openLightbox('images/replicator/architecture_deployment_traffic.png')">
          <img src="images/replicator/architecture_deployment_traffic.png" alt="Генеральная архитектурная схема">
          <div class="arch-zoom-hint">🔍 Нажмите на схему для полноэкранного зума</div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 3: ПАРАМЕТРЫ ЗАПУСКА И РАЗМЕЩЕНИЕ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-3">
      <div class="slide-header">
        <div class="slide-subtitle">Инсталляция и запуск процессов</div>
        <h1 class="slide-title">Параметры запуска CLI, systemd и модели размещения</h1>
        <p class="slide-desc">Спецификация аргументов командной строки агента и сравнение топологий размещения в кластере</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">Параметры CLI агента (ReplicatorAgentMain)</div>
            <ul class="card-list">
              <li><code>--agent-id &lt;id&gt;</code>: Уникальный ID агента (hostname).</li>
              <li><code>--cluster-id &lt;id&gt;</code>: ID обслуживаемого кластера (<code>dc1</code> / <code>dc2</code>).</li>
              <li><code>--mode &lt;all|sender|receiver&gt;</code>: Режим работы (default: <code>all</code>).</li>
              <li><code>--orchestrator &lt;url&gt;</code>: URL оркестратора (<code>http://orch:8005</code>).</li>
              <li><code>--port &lt;50051&gt;</code>: Порт входящего gRPC Netty сервера.</li>
              <li><code>--bandwidth &lt;MB/s&gt;</code>: Локальный лимит полосы пропускания.</li>
              <li><code>--compression &lt;zstd|lz4|none&gt;</code>: Кодек сжатия (default: <code>zstd</code>).</li>
              <li><code>--compression-level &lt;1-22&gt;</code>: Уровень сжатия Zstd (default: <code>3</code>).</li>
            </ul>
            <div class="code-box"># Запуск демона под Kerberos учеткой:
java -Xms4g -Xmx8g -XX:+UseG1GC \
  -jar replicator-agent.jar \
  --agent-id=agent-dc1-01 --cluster-id=dc1 \
  --mode=all --orchestrator=http://orch:8005 \
  --port=50051 --bandwidth=100 --compression=zstd</div>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">Сравнение моделей размещения агентов</div>
            <ul class="card-list">
              <li><strong>Модель 1: Colocated (на узлах Hadoop DataNode):</strong>
                <br/>• Максимальная скорость за счет <em>Short-Circuit Local Read</em>.
                <br/>• <strong>0 байт</strong> паразитного трафика в локальной сети ЦОД.
                <br/>• Требует: 2–4 vCPU, 4–8 GB JVM Heap на узел DataNode.
              </li>
              <li><strong>Модель 2: Dedicated Edge Gateway:</strong>
                <br/>• Агенты вынесены на 2–4 отдельных шлюзовых сервера с 25G/40G NIC.
                <br/>• Чтение блоков идет по сети LAN ЦОД через Data Transfer Protocol (:9866).
                <br/>• Полная изоляция от production-узлов Hadoop.
              </li>
              <li><strong>Оркестратор:</strong> Отдельная VM / K8s Deployment (4 vCPU, 8 GB RAM, 50 GB SSD).</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 4: СЕТЕВАЯ МАТРИЦА ПОРТОВ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-4">
      <div class="slide-header">
        <div class="slide-subtitle">Сетевая безопасность и NetOps</div>
        <h1 class="slide-title">Сетевая матрица портов, фаервол и протоколы передачи</h1>
        <p class="slide-desc">Полный перечень L4/L7 соединений для согласования со службами ИБ и сетевыми инженерами</p>
      </div>
      <div class="slide-body">
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Направление</th>
                <th>Протокол L4 / L7</th>
                <th>Порт</th>
                <th>Назначение сетевого потока</th>
                <th>Сетевой сегмент</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>Agent DC1 ➔ Agent DC2</strong></td>
                <td>TCP / gRPC (mTLS v1.3)</td>
                <td><span class="port-badge port-wan">TCP 50051</span></td>
                <td>Прямая передача блоков HDFS и DDL пакетов Hive CDC</td>
                <td><strong>WAN (МежЦОД)</strong></td>
              </tr>
              <tr>
                <td><strong>Agent DC2 ➔ Agent DC1</strong></td>
                <td>TCP / gRPC (mTLS v1.3)</td>
                <td><span class="port-badge port-wan">TCP 50051</span></td>
                <td>Обратный поток при Reverse Replication в сценарии DR</td>
                <td><strong>WAN (МежЦОД)</strong></td>
              </tr>
              <tr>
                <td><strong>Браузер ➔ Orchestrator</strong></td>
                <td>TCP / HTTPS (TLS 1.3)</td>
                <td><span class="port-badge port-control">TCP 8005</span></td>
                <td>Web UI Svelte 5, REST API, SSE шина событий</td>
                <td>Corporate LAN</td>
              </tr>
              <tr>
                <td><strong>Agents ➔ Orchestrator</strong></td>
                <td>TCP / HTTP REST</td>
                <td><span class="port-badge port-control">TCP 8005</span></td>
                <td>Heartbeat (каждые 5с), Claim подзадач, продление Lease HA</td>
                <td>Management LAN</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ NameNode (LAN)</strong></td>
                <td>TCP / Hadoop RPC</td>
                <td><span class="port-badge port-lan">TCP 8020/9000</span></td>
                <td>Листинг каталогов, получение манифестов, атомарный rename</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ DataNodes (LAN)</strong></td>
                <td>TCP / SASL DTP</td>
                <td><span class="port-badge port-lan">TCP 9866</span></td>
                <td>Прямое блочное чтение/запись данных под UGI (doAs)</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ Hive Metastore</strong></td>
                <td>TCP / Thrift RPC</td>
                <td><span class="port-badge port-lan">TCP 9083</span></td>
                <td>Опрос NOTIFICATION_LOG (DC1) и накат DDL схемы (DC2)</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ Kerberos KDC</strong></td>
                <td>TCP/UDP / Kerberos</td>
                <td><span class="port-badge port-lan">TCP/UDP 88</span></td>
                <td>Получение TGT тикетов по системному keytab</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 5: DATA PLANE МЕХАНИКА -->
    <!-- ========================================== -->
    <div class="slide" id="slide-5">
      <div class="slide-header">
        <div class="slide-subtitle">Data Plane Deep Dive</div>
        <h1 class="slide-title">Протокол HDFS Data Plane: Wire-формат, Tar-Stream и Zero-Staging</h1>
        <p class="slide-desc">Прямой стриминг чанков, упаковка мелких файлов, сквозной хеш SHA-256 и атомарный коммит</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">1. Fast In-Memory Diff и Чанкирование</div>
            <ul class="card-list">
              <li><strong>Метод <code>GetDirectoryManifest()</code>:</strong> Возвращает список <code>{path, size, mtime}</code> целевой папки одним gRPC вызовом.</li>
              <li><strong>In-Memory Diff:</strong> Идентичные файлы отсекаются за миллисекунды (0 байт в WAN канале).</li>
              <li><strong>Чанки по 4 МБ:</strong> Большие файлы передаются через <code>TransferFile(stream)</code> чанками по 4 МБ со сжатием Zstd.</li>
              <li><strong>Сквозной SHA-256:</strong> Хеш считается на лету при чтении и сверяется на стороне приемника перед коммитом.</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">2. Tar-Streaming и Zero-Staging коммит</div>
            <ul class="card-list">
              <li><strong>Виртуальный Tar-Streaming (&lt; 1 МБ):</strong> Метод <code>TransferTarStream()</code> упаковывает сотни мелких файлов в непрерывный поток в RAM. Приемник потоково пишет файлы прямо в HDFS (8 параллельных потоков). <strong>0 шторма RPC к NameNode!</strong></li>
              <li><strong>Zero-Staging коммит:</strong> Приемник пишет поток напрямую в целевой HDFS: <code>&lt;path&gt;._staging_&lt;jobId&gt;</code> без локального диска.</li>
              <li><strong>Атомарный <code>fs.rename()</code>:</strong> Чистовой файл фиксируется атомарным RPC переименованием NameNode.</li>
              <li><strong>Staging Garbage Collection:</strong> Демон удаляет осиротевшие файлы <code>._staging_*</code> старше 24ч раз в 15 минут.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 6: АЛГОРИТМ ШЕЙПИНГА -->
    <!-- ========================================== -->
    <div class="slide" id="slide-6">
      <div class="slide-header">
        <div class="slide-subtitle">Сетевой шейпинг полосы</div>
        <h1 class="slide-title">Алгоритм сетевого шейпинга: Hierarchical Token Bucket</h1>
        <p class="slide-desc">Аппаратный контроль полосы пропускания WAN на уровне Netty буферов без использования iptables/tc</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-cyan">
            <div class="card-title">Иерархия лимитов полосы</div>
            <ul class="card-list">
              <li><strong>Уровень 1 (Global Limit):</strong> Суммарная планка всей платформы (например, 200 МБ/с).</li>
              <li><strong>Уровень 2 (DC Channel Limit):</strong> Лимит физического WAN канала между площадками (150 МБ/с).</li>
              <li><strong>Уровень 3 (Task Queue):</strong>
                <br/>• <code>CRITICAL</code> (HMS CDC, витрины): Гарантированная полоса (50 МБ/с), Burstable.
                <br/>• <code>BATCH</code> (Ночные синки сырых слоев): Лимит 80 МБ/с.
                <br/>• <code>BACKGROUND</code> (Архивы, бэкапы): Best Effort (20 МБ/с).
              </li>
              <li><strong>Динамическое изменение:</strong> Новые лимиты применяются через SSE за 100 мс без рестарта демонов.</li>
            </ul>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">Механизм троттлинга (LocalBandwidthLimiter)</div>
            <ul class="card-list">
              <li><strong>Пополнение корзины:</strong> Каждую миллисекунду корзина токенов пополняется на <code>allowedBytesPerSec / 1000</code>.</li>
              <li><strong>Троттлинг потока:</strong> Перед отправкой чанка вызывается <code>limiter.throttle(chunkSize)</code>.</li>
              <li><strong>Высокоточная пауза:</strong> При нехватке токенов поток усыпляется через <code>LockSupport.parkNanos()</code>.</li>
              <li><strong>HTTP/2 Flow Control:</strong> Обратное давление (Backpressure) удерживает Netty буферы от переполнения.</li>
              <li><strong>Гарантия без голодания:</strong> Низкоприоритетные очереди получают гарантированный квант полосы.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 7: HIVE METASTORE CDC -->
    <!-- ========================================== -->
    <div class="slide" id="slide-7">
      <div class="slide-header">
        <div class="slide-subtitle">Репликация метаданных</div>
        <h1 class="slide-title">Протокол Hive Metastore CDC, Inotify Lease HA и Non-ACID Gate</h1>
        <p class="slide-desc">Вычитка NOTIFICATION_LOG, распределенный лизинг воркеров, трансляция путей и защита данных</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">1. Вычитка событий и Inotify Lease HA</div>
            <ul class="card-list">
              <li><strong>Опрос NOTIFICATION_LOG:</strong> Агент опрашивает Thrift <code>:9083</code> методом <code>get_next_notification()</code>.</li>
              <li><strong>Распределенный лизинг (Lease HA):</strong> Оркестратор выдает эксклюзивный токен аренды потока схемы на 60 сек (heartbeat раз в 20 сек). Ровно 1 воркер читает поток схемы.</li>
              <li><strong>Автоматический Failover воркера:</strong> При падении держателя аренда протухает через 60 сек, и поток подхватывает соседний агент с позиции <code>last_processed_event_id</code>.</li>
              <li><strong>Non-ACID Gate:</strong> Таблицы с <code>transactional=true</code> отфильтровываются, исключая повреждение дельт.</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">2. Federation Rewrite и накат схемы</div>
            <ul class="card-list">
              <li><strong>Трансляция NameService:</strong> <code>HmsPathRewriter</code> на лету переписывает URI в DDL: <code>hdfs://ns-dc1/</code> ➔ <code>hdfs://ns-dc2/</code> для таблиц и партиций.</li>
              <li><strong>gRPC протокол <code>HmsTransferService</code>:</strong> Методы <code>ApplyDatabase</code>, <code>ApplyTable</code>, <code>ApplyPartitionBatch</code>, <code>ApplyCdcBatch</code>.</li>
              <li><strong>Защита от удаления файлов:</strong> При операциях <code>DROP_TABLE</code> и <code>DROP_PARTITION</code> в Thrift вызове принудительно ставится <code>deleteData=false</code>.</li>
              <li><strong>Фоновые подзадачи:</strong> Перемещение файлов партиций HDFS изолировано и скрыто из общего UI.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 8: БЕЗОПАСНОСТЬ И KERBEROS -->
    <!-- ========================================== -->
    <div class="slide" id="slide-8">
      <div class="slide-header">
        <div class="slide-subtitle">Информационная безопасность</div>
        <h1 class="slide-title">Безопасность, Kerberos Impersonation (doAs) и Apache Ranger Audit</h1>
        <p class="slide-desc">Сквозной аудит доступа под реальным логином автора задачи и mTLS шифрование канала</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-cyan">
            <div class="card-title">Kerberos Proxy User doAs Impersonation</div>
            <ul class="card-list">
              <li><strong>Системный Keytab:</strong> Агент стартует под техучеткой <code>hdfs-replicator/node@REALM</code> (прописана в <code>hadoop.proxyuser.hosts</code>).</li>
              <li><strong>Выполнение под UGI автора:</strong> Чтение и запись в HDFS производятся строго от имени автора задачи:
                <br/><code>UGI.createProxyUser(jobAuthor, systemUGI).doAs(...)</code>.
              </li>
              <li><strong>Apache Ranger Audit:</strong> В аудит-логах HDFS фиксируется реальный автор (например, <code>ivan_de</code>), а не техническая учетная запись.</li>
              <li><strong>Сохранение POSIX прав:</strong> В <code>FileMetadata</code> передаются <code>file_mode</code>, <code>owner</code>, <code>group</code>. На приемнике права выставляются через <code>fs.setPermission()</code>.</li>
            </ul>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">Аутентификация и сетевое шифрование</div>
            <ul class="card-list">
              <li><strong>mTLS v1.3:</strong> МежЦОДное gRPC соединение защищено взаимными x509 сертификатами с валидацией корпоративным CA.</li>
              <li><strong>Web UI SSO:</strong> Вход в веб-интерфейс через Kerberos SPNEGO SSO в 1 клик (Negotiate header) или LDAP Bind.</li>
              <li><strong>JWT Сессии:</strong> Безопасные HttpOnly / SameSite Lax Cookie с валидацией ролей (ADMIN, WRITER, READER).</li>
              <li><strong>Защита секретов:</strong> Ключи агентов и токены передаются в защищенных заголовках <code>X-Replicator-Agent-Secret</code>.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 9: SRE RUNBOOK - АВАРИЯ И KILL-SWITCH -->
    <!-- ========================================== -->
    <div class="slide" id="slide-9">
      <div class="slide-header">
        <div class="slide-subtitle">Disaster Recovery Runbook • Часть 1</div>
        <h1 class="slide-title">SRE Runbook: Авария основного ЦОД и сетевое ограждение (Kill-Switch)</h1>
        <p class="slide-desc">Пошаговый регламент действий дежурного SRE при отказе DC1 и защита от Split-Brain</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-red">
            <div class="card-title">Пошаговый регламент действий при аварии DC1</div>
            <ul class="card-list">
              <li><strong>Шаг 1 (Детектирование):</strong> Агенты DC1 не присылают heartbeat (> 15 сек). Кластер недоступен.</li>
              <li><strong>Шаг 2 (Сетевое ограждение Fencing):</strong> Дежурный SRE нажимает 🛑 <strong>Kill-Switch</strong> в UI DR Hub (или вызывает <code>POST /api/dr/fencing/activate</code>).</li>
              <li><strong>Шаг 3 (Системные действия):</strong>
                <br/>• Лимит WAN полосы в Token Bucket сбрасывается в <strong>0 МБ/с</strong>.
                <br/>• Все активные gRPC потоки отменяются (<code>Status.CANCELLED</code>).
                <br/>• Все прямые задачи репликации переводятся в статус <code>STOPPED</code>.
                <br/>• Планировщик Cron Schedules принудительно отключается.
              </li>
              <li><strong>Шаг 4 (Промоушен DC2):</strong> Клиентский трафик (Spark, Trino, BI) переключается на ЦОД-2. ЦОД-2 становится Active кластером на запись.</li>
            </ul>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">Защита от Split-Brain: Физический барьер</div>
            <ul class="card-list">
              <li><strong>В чем риск:</strong> Если упавший ЦОД-1 внезапно оживет без сетевого ограждения, старые фоновые задачи репликации перетерли бы новые данные, записанные клиентами на ЦОД-2!</li>
              <li><strong>Как решено:</strong> Kill-Switch ставит двойной барьер — программный (заморозка задач) и сетевой (квота шейпера 0 МБ/с). Ни один байт не покидает периметр.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 10: SRE RUNBOOK - ВОССТАНОВЛЕНИЕ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-10">
      <div class="slide-header">
        <div class="slide-subtitle">Disaster Recovery Runbook • Часть 2</div>
        <h1 class="slide-title">SRE Runbook: Восстановление, безопасный Unfence и Reverse Replication</h1>
        <p class="slide-desc">Регламент ввода ожившего ЦОД-1 в строй: разворот потока трафика и догон дельты</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-cyan">
            <div class="card-title">Безопасное снятие изоляции (Unfence)</div>
            <ul class="card-list">
              <li><strong>Оживание площадки:</strong> Инфраструктура ЦОД-1 восстановлена и включена.</li>
              <li><strong>Снятие изоляции:</strong> В UI нажимается кнопка <em>Снять изоляцию</em> (Unfence).</li>
              <li><strong>⚠️ КРИТИЧЕСКИЙ ФАКТ:</strong> Снятие изоляции <strong>НЕ ЗАПУСКАЕТ прямые задачи!</strong> Они остаются в статусе <code>STOPPED</code>, исключая стирание данных на DC2.</li>
              <li><strong>Снятие барьера:</strong> Лимит WAN канала возвращается в штатное состояние (100 МБ/с).</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">Запуск Reverse Replication (Догон дельты)</div>
            <ul class="card-list">
              <li><strong>1-Click Reverse Wizard:</strong> Мастер автоматически генерирует зеркальные задачи с инвертированными путями: <strong>DC2 (Standby) ➔ DC1 (Primary)</strong>.</li>
              <li><strong>Разворот потока данных:</strong> Агент DC2 читает свежие файлы и стримит их в Агент DC1 по WAN :50051.</li>
              <li><strong>Контроль выравнивания:</strong> Мониторинг проверяет: <code>Remaining Bytes = 0</code> и <code>HMS Event Lag = 0</code>.</li>
              <li><strong>Failback:</strong> Клиентские приложения переключаются обратно на ЦОД-1. Возобновляется штатный прямой поток.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 11: АРХИТЕКТУРА В ИНТЕРФЕЙСЕ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-11">
      <div class="slide-header">
        <div class="slide-subtitle">Инструменты управления платформой</div>
        <h1 class="slide-title">Управление платформой в Web UI: Топология, Задачи, HMS и DR Hub</h1>
        <p class="slide-desc">Рабочие экраны администратора и инженера данных для мониторинга и оперативного контроля</p>
      </div>
      <div class="slide-body">
        <div class="ui-split">
          <div class="ui-left">
            <div class="card card-accent-indigo">
              <div class="card-title">Экраны консоли управления</div>
              <ul class="card-list">
                <li><strong>Топология ЦОД:</strong> Настройка пирамиды лимитов WAN и очередей в рантайме.</li>
                <li><strong>Дашборд репликации:</strong> Мониторинг скорости в МБ/с, прогресс-бары и ETA.</li>
                <li><strong>HMS Replication:</strong> Мониторинг отставания событий и статуса Lease HA.</li>
                <li><strong>DR Hub:</strong> Индикация барьера Kill-Switch и мастер Reverse Replication.</li>
              </ul>
            </div>
            <div class="card card-accent-green">
              <div class="card-title">Интеграция с экосистемой</div>
              <ul class="card-list">
                <li>Единая навигационная панель Hadoop Explorer Platform (YARN, HDFS, SQL, Spark).</li>
                <li>Kerberos SPNEGO бесшовный вход без ввода пароля.</li>
              </ul>
            </div>
          </div>
          <div class="ui-screenshot-card" onclick="openLightbox('images/replicator/04_topology_bandwidth.png')">
            <img src="images/replicator/04_topology_bandwidth.png" onerror="this.src='images/replicator/replicator_topology_screen.png'" alt="Управление топологией дата-центров">
            <div class="arch-zoom-hint">🔍 Нажмите для зума</div>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 12: НАБЛЮДАЕМОСТЬ И МЕТРИКИ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-12">
      <div class="slide-header">
        <div class="slide-subtitle">Observability & Monitoring</div>
        <h1 class="slide-title">Наблюдаемость: Метрики Prometheus, Healthcheck и Алерты</h1>
        <p class="slide-desc">Реестр системных метрик, эндпоинты мониторинга и рекомендуемые правила Alertmanager</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-cyan">
            <div class="card-title">Реестр ключевых метрик Prometheus</div>
            <ul class="card-list">
              <li><code>replicator_throughput_bytes_total</code> (Counter): Суммарный объем переданных байт по WAN.</li>
              <li><code>replicator_throughput_bytes_per_second</code> (Gauge): Текущая мгновенная скорость передачи.</li>
              <li><code>replicator_task_duration_seconds</code> (Histogram): Длительность выполнения задач.</li>
              <li><code>replicator_hms_lag_events</code> (Gauge): Отставание CDC в событиях NOTIFICATION_LOG.</li>
              <li><code>replicator_active_streams_count</code> (Gauge): Количество открытых параллельных gRPC стримов.</li>
              <li><code>replicator_staging_orphans_count</code> (Gauge): Количество обнаруженных осиротевших staging файлов.</li>
            </ul>
            <div class="code-box"># Эндпоинты сбора метрик:
GET http://orchestrator:8005/actuator/prometheus
GET http://orchestrator:8005/actuator/health</div>
          </div>
          <div class="card card-accent-red">
            <div class="card-title">Правила алертинга (Prometheus Alertmanager)</div>
            <ul class="card-list">
              <li><strong>ReplicatorAgentOffline (Critical):</strong>
                <br/><code>time() - replicator_agent_heartbeat_timestamp > 30s</code>
                <br/>Агент не присылает heartbeat более 30 секунд.
              </li>
              <li><strong>ReplicatorHmsLagHigh (Warning):</strong>
                <br/><code>replicator_hms_lag_events > 10000</code> в течение 5 минут.
                <br/>Отставание репликации метаданных превысило допустимый порог.
              </li>
              <li><strong>ReplicatorChecksumMismatch (Critical):</strong>
                <br/>Ошибка сквозной проверки хеша SHA-256 при коммите файла.
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 13: САЙЗИНГ И ТЮНИНГ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-13">
      <div class="slide-header">
        <div class="slide-subtitle">Инфраструктура и тюнинг ОС</div>
        <h1 class="slide-title">Сайзинг оборудования, тюнинг ОС Linux, JVM и траблшутинг</h1>
        <p class="slide-desc">Рекомендации по sysctl сетевого стека для 10G/40G WAN, опции JVM и разбор типовых инцидентов</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">Тюнинг ядра Linux для 10G/40G WAN (/etc/sysctl.conf)</div>
            <ul class="card-list">
              <li><code>net.core.rmem_max = 67108864</code> (64MB сетевой буфер приема).</li>
              <li><code>net.core.wmem_max = 67108864</code> (64MB сетевой буфер отправки).</li>
              <li><code>net.ipv4.tcp_rmem = 4096 87380 33554432</code> (TCP авто-окно).</li>
              <li><code>net.ipv4.tcp_wmem = 4096 65536 33554432</code> (TCP авто-окно).</li>
              <li><code>net.core.somaxconn = 4096</code>, <code>fs.file-max = 2097152</code>.</li>
            </ul>
            <div class="code-box"># Рекомендуемые опции JVM (Java 21):
-XX:+UseG1GC -XX:MaxGCPauseMillis=50
-XX:InitiatingHeapOccupancyPercent=45
-Dio.netty.allocator.type=pooled</div>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">Диагностика типовых инцидентов (SRE FAQ)</div>
            <ul class="card-list">
              <li><strong>GSSException / TGT Expired:</strong> Проверить валидность keytab: <code>klist -kt &lt;keytab&gt;</code>. Агент автоматически обновляет билет за 10 мин до экспирации.</li>
              <li><strong>NameNode in SafeMode:</strong> Дождаться выхода целевого кластера из безопасного режима (<code>hdfs dfsadmin -safemode wait</code>).</li>
              <li><strong>Сброс соединений Stateful Firewall:</strong> В Netty активен TCP Keepalive (<code>keepAliveTime=30s</code>), предотвращая разрыв сессий межсетевыми экранами.</li>
              <li><strong>Осиротевшие staging-файлы:</strong> Сборщик мусора удаляет файлы <code>._staging_*</code> старше 24ч автоматически.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

  </main>

  <!-- НИЖНИЙ БАР -->
  <footer>
    <div>Hadoop Explorer Platform • Архитектура развертывания и сетевые потоки</div>
    <div>Навигация: ◄ / ► (Стрелки) &nbsp;|&nbsp; Пробел &nbsp;|&nbsp; F (На весь экран) &nbsp;|&nbsp; O (Обзор)</div>
  </footer>

  <div class="lightbox" id="lightbox" onclick="closeLightbox()">
    <img id="lightboxImg" src="" alt="Увеличенное изображение">
  </div>

  <div class="overview-modal" id="overviewModal">
    <div class="overview-header">
      <h2 style="font-size: 1.4rem; font-weight: 800; color: #fff;">Карта слайдов презентации</h2>
      <button class="btn btn-primary" onclick="closeOverview()">✕ Закрыть</button>
    </div>
    <div class="overview-grid" id="overviewGrid"></div>
  </div>

  <script>
    let currentSlide = 1;
    const slides = document.querySelectorAll('.slide');
    const totalSlides = slides.length;

    function parseHash() {
      const hash = window.location.hash.replace('#', '').replace('slide-', '');
      const num = parseInt(hash, 10);
      if (!isNaN(num) && num >= 1 && num <= totalSlides) {
        return num;
      }
      return 1;
    }

    function updateSlide(updateHash = true) {
      slides.forEach((slide, idx) => {
        slide.classList.remove('active', 'prev');
        if (idx + 1 === currentSlide) {
          slide.classList.add('active');
        } else if (idx + 1 < currentSlide) {
          slide.classList.add('prev');
        }
      });

      document.getElementById('slideCounter').textContent = `${currentSlide} / ${totalSlides}`;
      const progress = ((currentSlide - 1) / (totalSlides - 1)) * 100;
      document.getElementById('progressBar').style.width = `${progress}%`;

      const overviewItems = document.querySelectorAll('.overview-item');
      overviewItems.forEach((item, idx) => {
        item.classList.toggle('current', idx + 1 === currentSlide);
      });

      if (updateHash) {
        history.replaceState(null, '', '#slide-' + currentSlide);
      }
    }

    function nextSlide() {
      if (currentSlide < totalSlides) {
        currentSlide++;
        updateSlide();
      }
    }

    function prevSlide() {
      if (currentSlide > 1) {
        currentSlide--;
        updateSlide();
      }
    }

    function goToSlide(n) {
      if (n >= 1 && n <= totalSlides) {
        currentSlide = n;
        updateSlide();
        closeOverview();
      }
    }

    window.addEventListener('hashchange', () => {
      currentSlide = parseHash();
      updateSlide(false);
    });

    window.addEventListener('keydown', (e) => {
      if (document.getElementById('lightbox').classList.contains('active')) {
        if (e.key === 'Escape') closeLightbox();
        return;
      }

      if (document.getElementById('overviewModal').classList.contains('active')) {
        if (e.key === 'Escape' || e.key === 'o' || e.key === 'O') closeOverview();
        return;
      }

      if (e.key === 'ArrowRight' || e.key === 'ArrowDown' || e.key === ' ' || e.key === 'PageDown') {
        e.preventDefault();
        nextSlide();
      } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp' || e.key === 'PageUp') {
        e.preventDefault();
        prevSlide();
      } else if (e.key === 'f' || e.key === 'F') {
        toggleFullscreen();
      } else if (e.key === 'o' || e.key === 'O') {
        toggleOverview();
      } else if (e.key === 'Home') {
        currentSlide = 1;
        updateSlide();
      } else if (e.key === 'End') {
        currentSlide = totalSlides;
        updateSlide();
      }
    });

    function toggleFullscreen() {
      if (!document.fullscreenElement) {
        document.documentElement.requestFullscreen().catch(() => {});
      } else {
        if (document.exitFullscreen) {
          document.exitFullscreen();
        }
      }
    }

    function openLightbox(src) {
      const lb = document.getElementById('lightbox');
      const img = document.getElementById('lightboxImg');
      img.src = src;
      lb.classList.add('active');
    }

    function closeLightbox() {
      document.getElementById('lightbox').classList.remove('active');
    }

    function toggleOverview() {
      const modal = document.getElementById('overviewModal');
      if (modal.classList.contains('active')) {
        closeOverview();
      } else {
        openOverview();
      }
    }

    function openOverview() {
      document.getElementById('overviewModal').classList.add('active');
    }

    function closeOverview() {
      document.getElementById('overviewModal').classList.remove('active');
    }

    function initOverview() {
      const grid = document.getElementById('overviewGrid');
      grid.innerHTML = '';
      slides.forEach((slide, idx) => {
        const title = slide.querySelector('.slide-title')?.textContent || `Слайд ${idx + 1}`;
        const subtitle = slide.querySelector('.slide-subtitle')?.textContent || '';
        const item = document.createElement('div');
        item.className = `overview-item ${idx + 1 === currentSlide ? 'current' : ''}`;
        item.onclick = () => goToSlide(idx + 1);
        item.innerHTML = `
          <div style="font-size: 0.72rem; color: var(--accent); font-weight: 700; text-transform: uppercase;">Слайд ${idx + 1}</div>
          <div style="font-size: 0.88rem; font-weight: 700; margin: 3px 0; color: #fff; line-height: 1.3;">${title}</div>
          <div style="font-size: 0.74rem; color: var(--text-muted);">${subtitle}</div>
        `;
        grid.appendChild(item);
      });
    }

    currentSlide = parseHash();
    initOverview();
    updateSlide(false);
  </script>
</body>
</html>
"""

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target_path = os.path.join(base_dir, "docs", "replicator-presentation.html")
    with open(target_path, "w", encoding="utf-8") as f:
        f.write(HTML_CONTENT)
    print(f"✅ Успешно сгенерирован технический HTML слайд-дек: {target_path}")

if __name__ == "__main__":
    main()

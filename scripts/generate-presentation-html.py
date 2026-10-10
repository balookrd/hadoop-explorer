#!/usr/bin/env python3
"""
Скрипт генерации интерактивной адаптивной HTML-презентации docs/replicator-presentation.html
на 13 слайдов с архитектурной схемой развертывания и сетевого трафика.
"""

import os

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Презентация: Hadoop gRPC Replicator — Архитектура развертывания и потоки трафика</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(22, 30, 49, 0.75);
      --card-border: rgba(255, 255, 255, 0.08);
      --primary: #6366f1;
      --primary-light: #818cf8;
      --accent: #06b6d4;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --text: #f8fafc;
      --text-muted: #94a3b8;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

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

    /* Верхний бар навигации */
    header {
      height: 56px;
      min-height: 56px;
      padding: 0 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--card-border);
      background: rgba(9, 13, 22, 0.88);
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
      font-size: 0.7rem;
      padding: 3px 8px;
      border-radius: 999px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .header-controls {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .slide-counter {
      font-size: 0.9rem;
      color: var(--text-muted);
      font-weight: 600;
      min-width: 85px;
      text-align: center;
    }

    .btn {
      background: rgba(255, 255, 255, 0.06);
      border: 1px solid var(--card-border);
      color: var(--text);
      padding: 6px 14px;
      border-radius: 8px;
      cursor: pointer;
      font-size: 0.85rem;
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

    .btn:active {
      transform: translateY(0);
    }

    .btn-primary {
      background: linear-gradient(135deg, #4f46e5, #4338ca);
      border: 1px solid #6366f1;
      color: white;
      box-shadow: 0 4px 14px rgba(79, 70, 229, 0.35);
    }

    .btn-primary:hover {
      background: linear-gradient(135deg, #6366f1, #4f46e5);
    }

    /* Полоса прогресса */
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

    /* Контейнер слайдов */
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
      transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
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

    /* Стили шапки слайда */
    .slide-header {
      margin-bottom: 14px;
      flex-shrink: 0;
    }

    .slide-subtitle {
      color: var(--accent);
      font-size: 0.8rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 4px;
    }

    .slide-title {
      font-size: 1.85rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      line-height: 1.2;
      background: linear-gradient(135deg, #ffffff 60%, #cbd5e1);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .slide-desc {
      color: var(--text-muted);
      font-size: 0.92rem;
      margin-top: 4px;
      line-height: 1.4;
    }

    .slide-body {
      flex: 1;
      min-height: 0;
      display: flex;
      gap: 20px;
      overflow-y: auto;
    }

    /* Сетка карточек */
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

    .grid-4 {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
      width: 100%;
      height: 100%;
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 18px;
      backdrop-filter: blur(12px);
      transition: border-color 0.2s ease, transform 0.2s ease;
      position: relative;
      display: flex;
      flex-direction: column;
    }

    .card:hover {
      border-color: rgba(99, 102, 241, 0.4);
      transform: translateY(-2px);
    }

    .card-accent-indigo { border-top: 3px solid var(--primary); }
    .card-accent-cyan { border-top: 3px solid var(--accent); }
    .card-accent-green { border-top: 3px solid var(--success); }
    .card-accent-amber { border-top: 3px solid var(--warning); }
    .card-accent-red { border-top: 3px solid var(--danger); }

    .card-title {
      font-size: 1.05rem;
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
      gap: 7px;
      overflow-y: auto;
    }

    .card-list li {
      font-size: 0.88rem;
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

    /* Полноразмерный контейнер генеральной схемы */
    .arch-container {
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 12px;
      cursor: zoom-in;
      position: relative;
      overflow: hidden;
    }

    .arch-container img {
      max-width: 100%;
      max-height: calc(100vh - 200px);
      width: auto;
      height: auto;
      object-fit: contain;
      border-radius: 6px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5);
    }

    .arch-zoom-hint {
      position: absolute;
      bottom: 16px;
      right: 20px;
      background: rgba(15, 23, 42, 0.88);
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 5px 12px;
      border-radius: 20px;
      font-size: 0.78rem;
      color: var(--accent);
      font-weight: 600;
      backdrop-filter: blur(8px);
      pointer-events: none;
    }

    /* Таблицы */
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
      font-size: 0.82rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      position: sticky;
      top: 0;
      z-index: 10;
    }

    td {
      padding: 10px 16px;
      font-size: 0.86rem;
      color: var(--text-muted);
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
    }

    tr:nth-child(even) td {
      background: rgba(255, 255, 255, 0.015);
    }

    tr:hover td {
      background: rgba(99, 102, 241, 0.06);
      color: var(--text);
    }

    .port-badge {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 4px;
      font-family: monospace;
      font-weight: 700;
      font-size: 0.82rem;
    }

    .port-wan { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .port-control { background: rgba(99, 102, 241, 0.2); color: #818cf8; border: 1px solid rgba(99, 102, 241, 0.4); }
    .port-lan { background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }

    /* Слайды со скриншотами UI */
    .ui-split {
      display: grid;
      grid-template-columns: minmax(360px, 420px) 1fr;
      gap: 20px;
      width: 100%;
      height: 100%;
      align-items: stretch;
    }

    .ui-left {
      display: flex;
      flex-direction: column;
      gap: 14px;
      height: 100%;
      overflow-y: auto;
    }

    .ui-screenshot-card {
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 10px;
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
      max-height: calc(100vh - 200px);
      width: auto;
      height: auto;
      object-fit: contain;
      border-radius: 6px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    }

    /* Нижний колонтитул */
    footer {
      height: 40px;
      min-height: 40px;
      padding: 0 28px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-top: 1px solid var(--card-border);
      background: rgba(9, 13, 22, 0.88);
      font-size: 0.8rem;
      color: var(--text-muted);
      z-index: 100;
    }

    /* Модальное окно Lightbox */
    .lightbox {
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.94);
      backdrop-filter: blur(16px);
      z-index: 1000;
      display: flex;
      align-items: center;
      justify-content: center;
      opacity: 0;
      visibility: hidden;
      transition: all 0.25s ease;
      cursor: zoom-out;
    }

    .lightbox.active {
      opacity: 1;
      visibility: visible;
    }

    .lightbox img {
      max-width: 95vw;
      max-height: 95vh;
      border-radius: 8px;
      box-shadow: 0 0 50px rgba(0, 0, 0, 0.8);
      border: 1px solid rgba(255, 255, 255, 0.1);
    }

    /* Меню обзора слайдов (Overview) */
    .overview-modal {
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(9, 13, 22, 0.96);
      backdrop-filter: blur(16px);
      z-index: 900;
      display: flex;
      flex-direction: column;
      padding: 32px;
      opacity: 0;
      visibility: hidden;
      transition: all 0.25s ease;
    }

    .overview-modal.active {
      opacity: 1;
      visibility: visible;
    }

    .overview-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
      flex-shrink: 0;
    }

    .overview-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
      gap: 14px;
      overflow-y: auto;
      padding-bottom: 20px;
    }

    .overview-item {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 14px;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    .overview-item:hover {
      border-color: var(--accent);
      transform: translateY(-2px);
      background: rgba(30, 41, 59, 0.8);
    }

    .overview-item.current {
      border-color: var(--primary-light);
      box-shadow: 0 0 14px rgba(99, 102, 241, 0.4);
    }
  </style>
</head>
<body>

  <!-- ВЕРХНИЙ БАР -->
  <header>
    <div class="brand">
      <img src="images/logo_white.png" onerror="this.src='../images/logo_white.png'" alt="Hadoop Explorer Platform">
      <span>gRPC Replicator</span>
      <span class="brand-tag">Architecture</span>
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
    <!-- СЛАЙД 1: ТИТУЛЬНЫЙ -->
    <!-- ========================================== -->
    <div class="slide active" id="slide-1">
      <div class="slide-header">
        <div class="slide-subtitle">Hadoop Explorer Platform • Системная Архитектура</div>
        <h1 class="slide-title">Hadoop gRPC Replicator</h1>
        <p class="slide-desc">Архитектура развертывания («Что куда ставится») и сетевые потоки («Как ходит трафик»)</p>
      </div>
      <div class="slide-body">
        <div class="grid-3">
          <div class="card card-accent-indigo">
            <div class="card-title">🏢 Что куда устанавливается?</div>
            <ul class="card-list">
              <li><strong>Control Plane (Orchestrator)</strong>: отдельный хост управления, VM или K8s Pod. Слушает порт <code>:8005</code> (REST API, Web UI Svelte 5).</li>
              <li><strong>Data Plane (Replicator Agents)</strong>: устанавливаются в каждом ЦОД на узлы DataNode (Colocated) или Edge Gateway с 10/25G LAN.</li>
              <li><strong>Hadoop узлы</strong>: NameNode, DataNodes, Hive Metastore работают штатно без сторонних плагинов или изменений в ядре.</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">🌐 Как ходит трафик данных?</div>
            <ul class="card-list">
              <li><strong>Прямой gRPC WAN стрим</strong>: трафик данных идет <strong>НАПРЯМУЮ</strong> от Agent DC1 в Agent DC2 (<code>:50051</code>) минуя Оркестратор!</li>
              <li><strong>Оркестратор НЕ качает файлы</strong> через себя (чистый Control Plane).</li>
              <li>Потоковая нарезка чанков по 4 МБ, Tar-Streaming мелких файлов, сквозной хэш SHA-256.</li>
            </ul>
          </div>
          <div class="card card-accent-cyan">
            <div class="card-title">🛡️ Сеть и Disaster Recovery</div>
            <ul class="card-list">
              <li>В межЦОДном фаерволе открывается <strong>ТОЛЬКО ОДИН ПОРТ</strong>: <code>TCP :50051</code> (mTLS v1.3).</li>
              <li>Иерархический шейпер Token Bucket: строгий аппаратный контроль полосы WAN.</li>
              <li>Авария и DR: Kill-Switch (0 МБ/с) и разворот Reverse Replication (DC2 ➔ DC1).</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 2: ГЛАВНАЯ АРХИТЕКТУРНАЯ СХЕМА -->
    <!-- ========================================== -->
    <div class="slide" id="slide-2">
      <div class="slide-header">
        <div class="slide-subtitle">Генеральная схема</div>
        <h1 class="slide-title">Архитектура развертывания и потоки трафика (Deployment & Data Flow)</h1>
        <p class="slide-desc">Физическое размещение компонентов в ЦОД, роли узлов, порты фаервола и направления сетевых потоков</p>
      </div>
      <div class="slide-body">
        <div class="arch-container" onclick="openLightbox('images/replicator/architecture_deployment_traffic.png')">
          <img src="images/replicator/architecture_deployment_traffic.png" alt="Архитектура развертывания и сетевые потоки">
          <div class="arch-zoom-hint">🔍 Нажмите на схему для полноэкранного зума</div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 3: ЧТО КУДА УСТАНАВЛИВАЕТСЯ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-3">
      <div class="slide-header">
        <div class="slide-subtitle">Топология размещения</div>
        <h1 class="slide-title">Что куда устанавливается (Component Placement Map)</h1>
        <p class="slide-desc">Детальная спецификация хостов, контейнеров и ролей в корпоративной инфраструктуре</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">1. Control Plane Host (Orchestrator)</div>
            <ul class="card-list">
              <li><strong>Где работает:</strong> Выделенная виртуальная машина или Kubernetes Pod.</li>
              <li><strong>Процесс:</strong> <code>replicator-orchestrator</code> (Java 21 LTS / Spring Boot 3).</li>
              <li><strong>Порт:</strong> <code>TCP :8005</code> (HTTP/REST API, SSE подписки, веб-консоль Svelte 5).</li>
              <li><strong>База данных:</strong> PostgreSQL / H2 (хранение задач, cron расписаний, истории).</li>
              <li><strong>⚠️ Роль:</strong> Только координация и лимиты. Файлы через него НЕ идут!</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">2. ЦОД-1 Узлы (Primary — Москва)</div>
            <ul class="card-list">
              <li><strong>Где работает:</strong> Узлы DataNode кластера или Edge Gateway узлы с 10/25G LAN.</li>
              <li><strong>Процесс:</strong> <code>replicator-agent-dc1</code> (Java 21 / Netty gRPC демон).</li>
              <li><strong>Порт:</strong> <code>TCP :50051</code> (gRPC Server / Client, Full-Duplex режим).</li>
              <li><strong>Kerberos:</strong> Системный keytab <code>hdfs-cluster-1.keytab</code>.</li>
              <li><strong>Роль:</strong> Diff манифестов, блочное чтение из DataNode под UGI <code>doAs</code>, опрос HMS NOTIFICATION_LOG, стриминг в WAN.</li>
            </ul>
          </div>
          <div class="card card-accent-cyan">
            <div class="card-title">3. ЦОД-2 Узлы (Standby / DR — Санкт-Петербург)</div>
            <ul class="card-list">
              <li><strong>Где работает:</strong> Узлы DataNode кластера или Edge Gateway узлы DC2.</li>
              <li><strong>Процесс:</strong> <code>replicator-agent-dc2</code> (Java 21 / Netty gRPC демон).</li>
              <li><strong>Порт:</strong> <code>TCP :50051</code> (gRPC Server, Full-Duplex приемник).</li>
              <li><strong>Kerberos:</strong> Системный keytab <code>hdfs-cluster-2.keytab</code>.</li>
              <li><strong>Роль:</strong> Прием 4 МБ чанков по WAN, Zero-Staging запись в DataNode (<code>:9866</code>), сверка SHA-256, rename, накат DDL в HMS DC2.</li>
            </ul>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">4. Рабочие станции инженеров и клиентов</div>
            <ul class="card-list">
              <li><strong>Где работает:</strong> Браузер пользователя (Chrome, Safari, Firefox).</li>
              <li><strong>Сетевой доступ:</strong> HTTPS <code>:8005</code> к Orchestrator.</li>
              <li><strong>Аутентификация:</strong> Kerberos SPNEGO SSO в 1 клик или LDAP логин/пароль.</li>
              <li><strong>Роли RBAC:</strong> ADMIN (полный доступ + DR Hub), WRITER (свои задачи), READER (аудит).</li>
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
        <div class="slide-subtitle">Сетевая безопасность</div>
        <h1 class="slide-title">Сетевая матрица портов и фаервола (Network Matrix)</h1>
        <p class="slide-desc">Какие порты открываются в межЦОДных межсетевых экранах (WAN) и внутри дата-центров (LAN)</p>
      </div>
      <div class="slide-body">
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Направление соединения</th>
                <th>Протокол</th>
                <th>Порт</th>
                <th>Назначение сетевого потока</th>
                <th>Сетевой сегмент</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>Agent DC1 ➔ Agent DC2</strong></td>
                <td>gRPC / HTTP/2 (mTLS)</td>
                <td><span class="port-badge port-wan">TCP 50051</span></td>
                <td>Прямая передача блоков HDFS и DDL пакетов Hive Metastore</td>
                <td><strong>WAN (МежЦОД)</strong></td>
              </tr>
              <tr>
                <td><strong>Agent DC2 ➔ Agent DC1</strong></td>
                <td>gRPC / HTTP/2 (mTLS)</td>
                <td><span class="port-badge port-wan">TCP 50051</span></td>
                <td>Обратная репликация Reverse Replication при аварии (DR)</td>
                <td><strong>WAN (МежЦОД)</strong></td>
              </tr>
              <tr>
                <td><strong>Браузер ➔ Orchestrator</strong></td>
                <td>HTTPS / HTTP</td>
                <td><span class="port-badge port-control">TCP 8005</span></td>
                <td>Доступ к UI Svelte 5, REST API, SSE подпискам</td>
                <td>Corporate LAN</td>
              </tr>
              <tr>
                <td><strong>Agents ➔ Orchestrator</strong></td>
                <td>HTTP REST</td>
                <td><span class="port-badge port-control">TCP 8005</span></td>
                <td>Heartbeat (5с), Claim подзадач, продление Lease HA</td>
                <td>Management LAN</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ NameNode (локально)</strong></td>
                <td>Hadoop RPC</td>
                <td><span class="port-badge port-lan">TCP 9000 / 8020</span></td>
                <td>Листинг каталогов, манифесты блоков, атомарный rename</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ DataNodes (локально)</strong></td>
                <td>Data Transfer Protocol</td>
                <td><span class="port-badge port-lan">TCP 9866 (SASL)</span></td>
                <td>Прямое блочное чтение и запись блоков HDFS</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ Hive Metastore</strong></td>
                <td>Thrift RPC</td>
                <td><span class="port-badge port-lan">TCP 9083</span></td>
                <td>Чтение NOTIFICATION_LOG (DC1) и накат DDL (DC2)</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
              <tr>
                <td><strong>Agent ➔ Kerberos KDC</strong></td>
                <td>Kerberos AS/TGS</td>
                <td><span class="port-badge port-lan">TCP/UDP 88</span></td>
                <td>Получение тикетов TGT по keytab техучетки</td>
                <td>DC LAN (Внутри ЦОД)</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 5: DATA FLOW HDFS -->
    <!-- ========================================== -->
    <div class="slide" id="slide-5">
      <div class="slide-header">
        <div class="slide-subtitle">HDFS Data Flow</div>
        <h1 class="slide-title">Как ходит трафик при репликации файлов HDFS (Пошаговый цикл)</h1>
        <p class="slide-desc">От построения дельты до атомарного переименования в целевом кластере</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">Шаг 1. Анализ дельты и планирование</div>
            <ul class="card-list">
              <li>Воркер в DC1 забирает подзадачу из Orchestrator (<code>:8005 /tasks/claim</code>).</li>
              <li>Запрашивает манифест у локальной NameNode DC1 (<code>:9000</code>).</li>
              <li>Одним gRPC вызовом <code>GetDirectoryManifest</code> запрашивает манифест у Agent DC2.</li>
              <li>В памяти строится O(N) Diff: неизмененные файлы пропускаются (0 байт WAN!).</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">Шаг 2. Чтение блоков и упаковка</div>
            <ul class="card-list">
              <li>Агент DC1 читает блоки из DataNodes DC1 (<code>:9866</code>) под UGI автора задачи (<code>doAs</code>).</li>
              <li>Запрашивает разрешение на передачу у Token Bucket шейпера полосы.</li>
              <li>Файлы &lt; 1 МБ упаковываются в виртуальный Tar-Stream на лету.</li>
              <li>Файлы &gt;= 1 МБ нарезаются на чанки по 4 МБ со сжатием Zstd/LZ4.</li>
            </ul>
          </div>
          <div class="card card-accent-cyan">
            <div class="card-title">Шаг 3. Прямой gRPC WAN стриминг</div>
            <ul class="card-list">
              <li>Агент DC1 стримит чанки <strong>НАПРЯМУЮ</strong> в Agent DC2 (<code>:50051 gRPC, mTLS</code>).</li>
              <li>Никакие байты файлов <strong>НЕ проходят через Оркестратор!</strong></li>
              <li>Потоковое вычисление контрольной суммы SHA-256 на обеих сторонах.</li>
              <li>Скорость строго удерживается шейпером Token Bucket.</li>
            </ul>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">Шаг 4. Zero-Staging и фиксация в HDFS</div>
            <ul class="card-list">
              <li>Агент DC2 пишет блоки в DataNodes DC2 (<code>:9866</code>) во временный файл <code>._staging_</code>.</li>
              <li>Сверка хэша SHA-256: при совпадении вызывается атомарный <code>fs.rename()</code>.</li>
              <li>Для мелких файлов Tar-Stream распаковывается прямо в HDFS без диска.</li>
              <li>Агент DC1 отправляет рапорт в Orchestrator (<code>:8005 /progress</code>) с обновлением ETA.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 6: DATA FLOW HIVE CDC -->
    <!-- ========================================== -->
    <div class="slide" id="slide-6">
      <div class="slide-header">
        <div class="slide-subtitle">Metadata Data Flow</div>
        <h1 class="slide-title">Как ходит трафик при репликации Hive Metastore (HMS CDC)</h1>
        <p class="slide-desc">Потоковая передача DDL-событий с распределенным лизингом Inotify Lease HA</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">1. Захват эксклюзивной аренды (Lease)</div>
            <ul class="card-list">
              <li>Агент DC1 запрашивает аренду схемы: <code>POST /hms/lease/claim</code> (Оркестратор).</li>
              <li>Оркестратор выдает эксклюзивный токен аренды на 60 секунд с автопродлением.</li>
              <li>Исключены гонки: ровно один воркер в кластере читает CDC-поток схемы.</li>
              <li>При сбое воркера аренда протухает, и другой агент подхватывает стрим.</li>
            </ul>
          </div>
          <div class="card card-accent-amber">
            <div class="card-title">2. Локальный опрос NOTIFICATION_LOG</div>
            <ul class="card-list">
              <li>Агент DC1 по LAN Thrift <code>:9083</code> вычитывает события из Hive Metastore DC1.</li>
              <li>События: <code>CREATE_TABLE</code>, <code>ADD_PARTITION</code>, <code>ALTER_TABLE</code>, <code>DROP_PARTITION</code>.</li>
              <li><strong>Non-ACID Gate:</strong> ACID transactional таблицы безопасно пропускаются.</li>
              <li>Пакеты событий формируются пачками для минимизации RPC.</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">3. Передача пакетов DDL по WAN</div>
            <ul class="card-list">
              <li>Агент DC1 передает пачку DDL в Agent DC2 по WAN <code>:50051</code> (gRPC contract).</li>
              <li>Агент DC2 транслирует Federation NameService: <code>hdfs://ns-dc1/</code> ➔ <code>hdfs://ns-dc2/</code>.</li>
              <li>Перелинковка <code>sdLocation</code> на целевой кластер и генерация саб-джобов HDFS.</li>
              <li>Изоляция сабтасок: перенос файлов партиций скрыт из основного списка.</li>
            </ul>
          </div>
          <div class="card card-accent-cyan">
            <div class="card-title">4. Применение DDL и подтверждение</div>
            <ul class="card-list">
              <li>Агент DC2 по LAN Thrift <code>:9083</code> накатывает DDL в Hive Metastore DC2.</li>
              <li>Безопасность: при <code>DROP_TABLE</code> флаг <code>deleteData=false</code> (файлы не стираются!).</li>
              <li>Агент DC2 подтверждает накат ➔ Агент DC1 рапортует прогресс в Orchestrator.</li>
              <li>Фиксация <code>last_processed_event_id</code>: позиция гарантированно сохранена.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 7: ТРАФИК В DISASTER RECOVERY -->
    <!-- ========================================== -->
    <div class="slide" id="slide-7">
      <div class="slide-header">
        <div class="slide-subtitle">Disaster Recovery Traffic</div>
        <h1 class="slide-title">Потоки трафика в Disaster Recovery: Штатно vs Kill-Switch vs Reverse</h1>
        <p class="slide-desc">Как ведет себя сеть при аварии основного ЦОД и как безопасно разворачивается поток данных</p>
      </div>
      <div class="slide-body">
        <div class="grid-3">
          <div class="card card-accent-green">
            <div class="card-title">1. Штатный режим (DC1 ➔ DC2)</div>
            <ul class="card-list">
              <li><strong>Клиентский трафик:</strong> Направлен на DC1.</li>
              <li><strong>Data Plane WAN:</strong> Поток идет от Agent DC1 в Agent DC2 (<code>:50051</code>).</li>
              <li><strong>Шейпер полосы:</strong> Лимит канала (100 МБ/с).</li>
              <li><strong>HMS CDC:</strong> Стриминг дельты в DC2.</li>
              <li>DC2 выступает пассивным Standby-приемником.</li>
            </ul>
          </div>
          <div class="card card-accent-red">
            <div class="card-title">2. Авария DC1 и Kill-Switch</div>
            <ul class="card-list">
              <li><strong>Событие:</strong> DC1 упал. Клиенты переключены на DC2.</li>
              <li><strong>Действие оператора:</strong> Нажатие 🛑 Kill-Switch.</li>
              <li><strong>WAN сетевой барьер:</strong> Лимит канала ➔ <strong>0 МБ/с (Fencing)</strong>.</li>
              <li><strong>Задачи:</strong> Все прямые задачи заморожены (STOPPED, Cron OFF).</li>
              <li><strong>Защита от Split-Brain:</strong> При оживании DC1 старые задачи НЕ запустятся и не затрут данные DC2!</li>
            </ul>
          </div>
          <div class="card card-accent-cyan">
            <div class="card-title">3. Восстановление DC1 (Reverse)</div>
            <ul class="card-list">
              <li><strong>Событие:</strong> DC1 починили и включили.</li>
              <li><strong>Снятие изоляции:</strong> Прямые задачи остаются STOPPED!</li>
              <li><strong>Reverse Replication:</strong> Запуск зеркальных задач <strong>DC2 ➔ DC1</strong>.</li>
              <li><strong>Поток данных РАЗВОРАЧИВАЕТСЯ:</strong> Agent DC2 стримит дельту в Agent DC1.</li>
              <li>После догона дельты DC1 готов к возврату в продуктив.</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 8: ИНТЕРФЕЙС - ТОПОЛОГИЯ И ШЕЙПЕР -->
    <!-- ========================================== -->
    <div class="slide" id="slide-8">
      <div class="slide-header">
        <div class="slide-subtitle">Архитектура в интерфейсе</div>
        <h1 class="slide-title">Управление топологией дата-центров и шейпером полосы</h1>
        <p class="slide-desc">Иерархический Token Bucket: глобальные лимиты каналов и пулы приоритетов задач</p>
      </div>
      <div class="slide-body">
        <div class="ui-split">
          <div class="ui-left">
            <div class="card card-accent-indigo">
              <div class="card-title">Пирамида лимитов полосы</div>
              <ul class="card-list">
                <li><strong>Глобальный лимит ЦОД:</strong> Верхняя планка суммарного WAN канала между Москвой и Санкт-Петербургом.</li>
                <li><strong>Гарантии без голодания:</strong> Приоритетные задачи (CRITICAL) получают полосу первыми.</li>
                <li><strong>Zero-Overhead:</strong> Контроль токенов в памяти воркеров, мгновенный троттлинг Netty буферов.</li>
              </ul>
            </div>
            <div class="card card-accent-green">
              <div class="card-title">Динамическое изменение</div>
              <ul class="card-list">
                <li>Смена лимита в интерфейсе мгновенно вступает в силу через SSE без рестарта демонов.</li>
                <li>Сетевой стек точно выдерживает профиль нагрузки.</li>
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
    <!-- СЛАЙД 9: ИНТЕРФЕЙС - ЗАДАЧИ И ДАШБОРД -->
    <!-- ========================================== -->
    <div class="slide" id="slide-9">
      <div class="slide-header">
        <div class="slide-subtitle">Архитектура в интерфейсе</div>
        <h1 class="slide-title">Контроль задач и прямого статуса репликации в реальном времени</h1>
        <p class="slide-desc">Метрики пропускной способности, прогресс-бары, статус воркеров и детальный аудит</p>
      </div>
      <div class="slide-body">
        <div class="ui-split">
          <div class="ui-left">
            <div class="card card-accent-cyan">
              <div class="card-title">Мгновенный мониторинг</div>
              <ul class="card-list">
                <li>Отображение текущей сетевой утилизации в МБ/с.</li>
                <li>Количество переданных файлов и байт.</li>
                <li>Динамический расчет оставшегося времени (ETA) на базе скользящей средней скорости.</li>
              </ul>
            </div>
            <div class="card card-accent-amber">
              <div class="card-title">Гибкое управление</div>
              <ul class="card-list">
                <li>Мастер создания задач: указание source/target путей HDFS.</li>
                <li>Привязка к профилю шейпинга и очередей полосы.</li>
                <li>Поддержка Cron расписаний и ручного триггера.</li>
              </ul>
            </div>
          </div>
          <div class="ui-screenshot-card" onclick="openLightbox('images/replicator/02_main_dashboard.png')">
            <img src="images/replicator/02_main_dashboard.png" onerror="this.src='images/replicator/replicator_dashboard_screen.png'" alt="Дашборд задач репликации">
            <div class="arch-zoom-hint">🔍 Нажмите для зума</div>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 10: ИНТЕРФЕЙС - HIVE METASTORE -->
    <!-- ========================================== -->
    <div class="slide" id="slide-10">
      <div class="slide-header">
        <div class="slide-subtitle">Архитектура в интерфейсе</div>
        <h1 class="slide-title">Потоковая репликация метаданных Hive Metastore (HMS Console)</h1>
        <p class="slide-desc">Непрерывная синхронизация каталогов DDL и статус распределенной аренды Inotify Lease</p>
      </div>
      <div class="slide-body">
        <div class="ui-split">
          <div class="ui-left">
            <div class="card card-accent-indigo">
              <div class="card-title">Контроль отставания (Lag)</div>
              <ul class="card-list">
                <li>Отображение разницы между <code>Max Event ID</code> источника и <code>Last Processed Event ID</code> приемника.</li>
                <li>Автоматическая сигнализация при росте задержки синхронизации метаданных.</li>
              </ul>
            </div>
            <div class="card card-accent-green">
              <div class="card-title">Статус Inotify Lease HA</div>
              <ul class="card-list">
                <li>Информация об активном воркере-держателе аренды.</li>
                <li>Отображение оставшегося TTL аренды и частоты продления.</li>
                <li>Скрытие служебных подзадач переноса партиций из общего списка.</li>
              </ul>
            </div>
          </div>
          <div class="ui-screenshot-card" onclick="openLightbox('images/replicator/06_hms_replication_dashboard.png')">
            <img src="images/replicator/06_hms_replication_dashboard.png" onerror="this.src='images/replicator/replicator_hms_screen.png'" alt="Консоль Hive Metastore CDC">
            <div class="arch-zoom-hint">🔍 Нажмите для зума</div>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 11: ИНТЕРФЕЙС - DR HUB И ИЗОЛЯЦИЯ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-11">
      <div class="slide-header">
        <div class="slide-subtitle">Архитектура в интерфейсе</div>
        <h1 class="slide-title">Центр катастрофоустойчивости DR Hub и режим изоляции Kill-Switch</h1>
        <p class="slide-desc">Защита от Split-Brain: мгновенная блокировка сетевого канала и остановка прямых задач</p>
      </div>
      <div class="slide-body">
        <div class="ui-split">
          <div class="ui-left">
            <div class="card card-accent-red">
              <div class="card-title">🛑 Аварийный барьер (Fencing)</div>
              <ul class="card-list">
                <li>Яркий баннер аварийного состояния на всех экранах платформы.</li>
                <li>Установка лимита WAN в 0 МБ/с: ни один пакет не покидает периметр.</li>
                <li>Принудительная остановка фоновых задач и отключение планировщика Cron.</li>
              </ul>
            </div>
            <div class="card card-accent-amber">
              <div class="card-title">Безопасность данных</div>
              <ul class="card-list">
                <li>Гарантирует, что оживший аварийный узел не перезапишет свежие клиентские изменения на резервном ЦОД.</li>
              </ul>
            </div>
          </div>
          <div class="ui-screenshot-card" onclick="openLightbox('images/replicator/10_disaster_recovery_fenced_state.png')">
            <img src="images/replicator/10_disaster_recovery_fenced_state.png" onerror="this.src='images/replicator/replicator_dr_isolated_screen.png'" alt="DR Hub в режиме изоляции">
            <div class="arch-zoom-hint">🔍 Нажмите для зума</div>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 12: ИНТЕРФЕЙС - UNFENCE И REVERSE -->
    <!-- ========================================== -->
    <div class="slide" id="slide-12">
      <div class="slide-header">
        <div class="slide-subtitle">Архитектура в интерфейсе</div>
        <h1 class="slide-title">Снятие изоляции и мастер 1-Click Reverse Replication</h1>
        <p class="slide-desc">Безопасный возврат узла в строй: разворот потока данных для догона дельты на оживший кластер</p>
      </div>
      <div class="slide-body">
        <div class="ui-split">
          <div class="ui-left">
            <div class="card card-accent-cyan">
              <div class="card-title">Безопасный Unfence</div>
              <ul class="card-list">
                <li>Снятие изоляции возвращает лимит канала, но <strong>НЕ запускает старые задачи</strong>!</li>
                <li>Исключен риск случайного стирания свежих данных.</li>
              </ul>
            </div>
            <div class="card card-accent-green">
              <div class="card-title">1-Click Reverse Wizard</div>
              <ul class="card-list">
                <li>Автоматическое создание зеркальных задач с инвертированными путями (<code>DC2 ➔ DC1</code>).</li>
                <li>Чекбоксы для выборочного догона критических таблиц.</li>
                <li>Поток данных идет в обратную сторону до полной синхронизации.</li>
              </ul>
            </div>
          </div>
          <div class="ui-screenshot-card" onclick="openLightbox('images/replicator/12_reverse_replication_modal.png')">
            <img src="images/replicator/12_reverse_replication_modal.png" onerror="this.src='images/replicator/replicator_unfence_reverse_screen.png'" alt="Снятие изоляции и Reverse Replication">
            <div class="arch-zoom-hint">🔍 Нажмите для зума</div>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================== -->
    <!-- СЛАЙД 13: САЙЗИНГ И БЕЗОПАСНОСТЬ -->
    <!-- ========================================== -->
    <div class="slide" id="slide-13">
      <div class="slide-header">
        <div class="slide-subtitle">Эксплуатация и комплаенс</div>
        <h1 class="slide-title">Аппаратный сайзинг и чеклист информационной безопасности (ИБ)</h1>
        <p class="slide-desc">Рекомендации по ресурсам хостов и сквозной аудит доступа Kerberos / Apache Ranger</p>
      </div>
      <div class="slide-body">
        <div class="grid-2">
          <div class="card card-accent-indigo">
            <div class="card-title">Рекомендации по аппаратному сайзингу</div>
            <ul class="card-list">
              <li><strong>Модель Colocated (на узлах DataNode):</strong> максимальная локальность данных (Short-Circuit Local Read), 0 дополнительной нагрузки на сеть ЦОД. Требует: 2-4 vCPU, 4-8 GB JVM heap, 10G/25G сетевая карта.</li>
              <li><strong>Модель Dedicated Gateway:</strong> полная изоляция от узлов Hadoop, независимое обновление. Чтение идет по LAN ЦОД. Требует: 8-16 vCPU, 16-32 GB RAM, 25G/40G сетевая карта.</li>
              <li><strong>Orchestrator Host:</strong> 4 vCPU, 8 GB RAM, 50 GB SSD для журнала и БД метаданных.</li>
            </ul>
          </div>
          <div class="card card-accent-green">
            <div class="card-title">Чеклист информационной безопасности</div>
            <ul class="card-list">
              <li>✅ <strong>Изоляция портов:</strong> в межЦОДном фаерволе открыт ровно 1 порт — <code>TCP :50051</code>.</li>
              <li>✅ <strong>Шифрование:</strong> mTLS v1.3 со взаимной аутентификацией агентов.</li>
              <li>✅ <strong>Kerberos impersonation:</strong> чтение и запись блоков под UGI автора задачи (<code>UserGroupInformation.doAs</code>).</li>
              <li>✅ <strong>Apache Ranger Audit:</strong> все операции логируются в аудит HDFS под именем инициатора задачи.</li>
              <li>✅ <strong>Hive Safety:</strong> при операциях DROP флаг <code>deleteData=false</code> исключает потерю физических файлов.</li>
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

  <!-- LIGHTBOX МОДАЛКА -->
  <div class="lightbox" id="lightbox" onclick="closeLightbox()">
    <img id="lightboxImg" src="" alt="Увеличенное изображение">
  </div>

  <!-- OVERVIEW МОДАЛКА -->
  <div class="overview-modal" id="overviewModal">
    <div class="overview-header">
      <h2 style="font-size: 1.4rem; font-weight: 800; color: #fff;">Карта слайдов презентации</h2>
      <button class="btn btn-primary" onclick="closeOverview()">✕ Закрыть</button>
    </div>
    <div class="overview-grid" id="overviewGrid"></div>
  </div>

  <!-- СКРИПТ НАВИГАЦИИ -->
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
          <div style="font-size: 0.9rem; font-weight: 700; margin: 3px 0; color: #fff; line-height: 1.3;">${title}</div>
          <div style="font-size: 0.75rem; color: var(--text-muted);">${subtitle}</div>
        `;
        grid.appendChild(item);
      });
    }

    // Инициализация при старте с учетом hash
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
        f.write(HTML_TEMPLATE)
    print(f"✅ Успешно сгенерирован HTML слайд-дек: {target_path}")

if __name__ == "__main__":
    main()

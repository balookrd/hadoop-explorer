#!/usr/bin/env python3
"""
Скрипт генерации архитектурной презентации PowerPoint (.pptx)
для сервиса Hadoop gRPC Replicator с фокусом на:
- «Что ставится куда» (Deployment Topology)
- «Как ходит трафик» (Network Flows & Protocols)
"""

import os
import sys
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE
from pptx.dml.color import RGBColor

# Цветовая палитра Hadoop Explorer Platform
BG_COLOR = RGBColor(11, 15, 25)          # #0B0F19 темный глубокий фон
CARD_BG = RGBColor(22, 30, 49)           # #161E31 фон карточек
CARD_BORDER = RGBColor(51, 65, 85)       # #334155 границы
TEXT_WHITE = RGBColor(248, 250, 252)     # #F8FAFC основной текст
TEXT_MUTED = RGBColor(148, 163, 184)     # #94A3B8 вторичный текст
ACCENT_CYAN = RGBColor(6, 182, 212)      # #06B6D4 акцент циан
ACCENT_INDIGO = RGBColor(99, 102, 241)   # #6366F1 индиго
ACCENT_GREEN = RGBColor(16, 185, 129)    # #10B981 зеленый
ACCENT_AMBER = RGBColor(245, 158, 11)    # #F59E0B янтарный
ACCENT_RED = RGBColor(239, 68, 68)       # #EF4444 красный
TABLE_HEADER_BG = RGBColor(30, 41, 59)   # #1E293B заголовок таблицы
TABLE_ROW_ALT = RGBColor(17, 24, 39)     # #111827 альт. строка

TOTAL_SLIDES = 13

def set_shape_bg(shape, color, border_color=None, border_width=Pt(1)):
    shape.fill.solid()
    shape.fill.fore_color.rgb = color
    if border_color:
        shape.line.color.rgb = border_color
        shape.line.width = border_width
    else:
        shape.line.fill.background()

def create_base_slide(prs, slide_num, category, title, description):
    slide_layout = prs.slide_layouts[6] # пустой макет
    slide = prs.slides.add_slide(slide_layout)

    # 1. Заливка фона
    bg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
    set_shape_bg(bg, BG_COLOR)

    # 2. Верхний колонтитул
    header_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.35), Inches(11.733), Inches(1.3))
    tf = header_box.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_top = tf.margin_right = tf.margin_bottom = 0

    p_cat = tf.paragraphs[0]
    p_cat.text = category.upper()
    p_cat.font.size = Pt(10)
    p_cat.font.bold = True
    p_cat.font.color.rgb = ACCENT_CYAN
    p_cat.space_after = Pt(2)

    p_title = tf.add_paragraph()
    p_title.text = title
    p_title.font.size = Pt(21)
    p_title.font.bold = True
    p_title.font.color.rgb = TEXT_WHITE
    p_title.space_after = Pt(3)

    if description:
        p_desc = tf.add_paragraph()
        p_desc.text = description
        p_desc.font.size = Pt(10.5)
        p_desc.font.color.rgb = TEXT_MUTED

    # 3. Нижний колонтитул
    footer_box = slide.shapes.add_textbox(Inches(0.8), Inches(7.05), Inches(11.733), Inches(0.35))
    ftf = footer_box.text_frame
    ftf.word_wrap = True
    ftf.margin_left = ftf.margin_top = ftf.margin_right = ftf.margin_bottom = 0
    p_foot = ftf.paragraphs[0]
    p_foot.text = "Hadoop Explorer Platform • Архитектура развертывания и сетевой трафик"
    p_foot.font.size = Pt(9)
    p_foot.font.color.rgb = RGBColor(100, 116, 139)

    p_num = ftf.add_paragraph()
    p_num.text = f"{slide_num} / {TOTAL_SLIDES}"
    p_num.alignment = PP_ALIGN.RIGHT
    p_num.font.size = Pt(9)
    p_num.font.bold = True
    p_num.font.color.rgb = ACCENT_CYAN

    # Линия разделителя внизу
    line = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), Inches(6.95), Inches(11.733), Pt(1))
    set_shape_bg(line, RGBColor(30, 41, 59))

    return slide

def add_card(slide, left, top, width, height, title, items, top_border_color=None):
    card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
    set_shape_bg(card, CARD_BG, CARD_BORDER, Pt(1))

    if top_border_color:
        accent_strip = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, left, top, width, Pt(3))
        set_shape_bg(accent_strip, top_border_color)

    tb = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.18), width - Inches(0.4), height - Inches(0.36))
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_top = tf.margin_right = tf.margin_bottom = 0

    if title:
        p_title = tf.paragraphs[0]
        p_title.text = title
        p_title.font.size = Pt(12)
        p_title.font.bold = True
        p_title.font.color.rgb = TEXT_WHITE
        p_title.space_after = Pt(6)
        first_item = True
    else:
        first_item = False

    for item in items:
        p = tf.add_paragraph() if (title or not first_item) else tf.paragraphs[0]
        first_item = False
        p.text = f"• {item}"
        p.font.size = Pt(9.8)
        p.font.color.rgb = TEXT_MUTED
        p.space_after = Pt(4)

def add_image_card(slide, left, top, width, height, image_path, caption=None):
    frame = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
    set_shape_bg(frame, RGBColor(15, 23, 42), CARD_BORDER, Pt(1))

    if os.path.exists(image_path):
        caption_height = Inches(0.3) if caption else 0
        img_top = top + Inches(0.08)
        img_left = left + Inches(0.08)
        img_width = width - Inches(0.16)

        try:
            slide.shapes.add_picture(image_path, img_left, img_top, width=img_width)
        except Exception as e:
            print(f"Ошибка загрузки картинки {image_path}: {e}")

        if caption:
            tb = slide.shapes.add_textbox(left, top + height - caption_height, width, caption_height)
            tf = tb.text_frame
            tf.word_wrap = True
            tf.margin_left = tf.margin_top = tf.margin_right = tf.margin_bottom = 0
            p = tf.paragraphs[0]
            p.text = caption
            p.alignment = PP_ALIGN.CENTER
            p.font.size = Pt(8.5)
            p.font.italic = True
            p.font.color.rgb = TEXT_MUTED

def add_table_custom(slide, left, top, width, height, headers, rows, col_widths=None):
    num_rows = len(rows) + 1
    num_cols = len(headers)
    table_shape = slide.shapes.add_table(num_rows, num_cols, left, top, width, height)
    table = table_shape.table

    if col_widths:
        for idx, w in enumerate(col_widths):
            table.columns[idx].width = w

    for col_idx, h_text in enumerate(headers):
        cell = table.cell(0, col_idx)
        cell.fill.solid()
        cell.fill.fore_color.rgb = TABLE_HEADER_BG
        cell.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = cell.text_frame.paragraphs[0]
        p.text = h_text
        p.font.bold = True
        p.font.size = Pt(10)
        p.font.color.rgb = ACCENT_CYAN

    for row_idx, row_data in enumerate(rows):
        bg = TABLE_ROW_ALT if row_idx % 2 == 1 else CARD_BG
        for col_idx, val in enumerate(row_data):
            cell = table.cell(row_idx + 1, col_idx)
            cell.fill.solid()
            cell.fill.fore_color.rgb = bg
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            p = cell.text_frame.paragraphs[0]
            p.text = val
            p.font.size = Pt(9.2)
            p.font.color.rgb = TEXT_WHITE if col_idx == 0 else TEXT_MUTED

def generate_presentation(output_pptx_path):
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    img_dir = os.path.join(base_dir, "docs", "images", "replicator")
    logo_path = os.path.join(base_dir, "images", "logo_white.png")
    arch_img_path = os.path.join(img_dir, "architecture_deployment_traffic.png")

    print(f"🚀 Генерация 13 слайдов с архитектурными схемами в {output_pptx_path}...")

    # =========================================================================
    # СЛАЙД 1: ТИТУЛЬНЫЙ
    # =========================================================================
    slide1 = prs.slides.add_slide(prs.slide_layouts[6])
    bg1 = slide1.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
    set_shape_bg(bg1, BG_COLOR)

    if os.path.exists(logo_path):
        slide1.shapes.add_picture(logo_path, Inches(0.8), Inches(0.8), width=Inches(3.2))

    tb1 = slide1.shapes.add_textbox(Inches(0.8), Inches(1.8), Inches(11.733), Inches(1.8))
    tf1 = tb1.text_frame
    tf1.word_wrap = True
    p = tf1.paragraphs[0]
    p.text = "HADOOP EXPLORER PLATFORM • СИСТЕМНАЯ АРХИТЕКТУРА"
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = ACCENT_CYAN

    p = tf1.add_paragraph()
    p.text = "Hadoop gRPC Replicator"
    p.font.size = Pt(32)
    p.font.bold = True
    p.font.color.rgb = TEXT_WHITE

    p = tf1.add_paragraph()
    p.text = "Архитектура развертывания («Что куда ставится») и сетевые потоки («Как ходит трафик»)"
    p.font.size = Pt(13)
    p.font.color.rgb = TEXT_MUTED

    # 3 ключевых тезиса
    add_card(slide1, Inches(0.8), Inches(3.8), Inches(3.7), Inches(2.8),
             "🏢 Что куда устанавливается?",
             [
                 "Control Plane (Orchestrator): отдельный сервер управления / K8s / VM. Порт 8005 (REST + Web UI).",
                 "Data Plane (Replicator Agents): устанавливаются в каждом ЦОД на узлы DataNode или Edge/Gateway.",
                 "Hadoop узлы: NameNode, DataNodes, HMS работают в штатном режиме без плагинов."
             ], ACCENT_INDIGO)

    add_card(slide1, Inches(4.8), Inches(3.8), Inches(3.7), Inches(2.8),
             "🌐 Как ходит трафик данных?",
             [
                 "Прямой gRPC WAN стрим: Agent DC1 ➔ Agent DC2 (:50051) минуя Оркестратор!",
                 "Оркестратор НЕ качает байты через себя (чистый Control Plane).",
                 "Чанки по 4 МБ, Tar-Streaming мелких файлов, сквозной хэш SHA-256."
             ], ACCENT_GREEN)

    add_card(slide1, Inches(8.8), Inches(3.8), Inches(3.7), Inches(2.8),
             "🛡️ Сеть и Disaster Recovery",
             [
                 "В межЦОДном фаерволе открывается ТОЛЬКО один порт: TCP 50051 (mTLS).",
                 "Иерархический шейпер Token Bucket: жесткий контроль полосы WAN.",
                 "Авария и DR: Kill-Switch (0 МБ/с) и разворот Reverse Replication (DC2 ➔ DC1)."
             ], ACCENT_CYAN)

    # =========================================================================
    # СЛАЙД 2: ГЛАВНАЯ АРХИТЕКТУРНАЯ СХЕМА (ВО ВЕСЬ ЭКРАН)
    # =========================================================================
    slide2 = create_base_slide(prs, 2, "Генеральная схема",
                               "Архитектура развертывания и потоки трафика (Deployment & Data Flow)",
                               "Физическое размещение компонентов в ЦОД, роли узлов, порты фаервола и направления сетевых потоков")

    if os.path.exists(arch_img_path):
        slide2.shapes.add_picture(arch_img_path, Inches(0.8), Inches(1.85), width=Inches(11.733))
    else:
        add_card(slide2, Inches(0.8), Inches(1.85), Inches(11.733), Inches(4.8),
                 "Схема развертывания", ["Схема architecture_deployment_traffic.png генерируется из HTML."])

    # =========================================================================
    # СЛАЙД 3: ЧТО КУДА УСТАНАВЛИВАЕТСЯ (КОМПОНЕНТНАЯ КАРТА)
    # =========================================================================
    slide3 = create_base_slide(prs, 3, "Топология размещения",
                               "Что куда устанавливается (Component Placement Map)",
                               "Детальная спецификация хостов, контейнеров и ролей в инфраструктуре")

    add_card(slide3, Inches(0.8), Inches(1.85), Inches(5.7), Inches(2.4),
             "1. Control Plane Host (Orchestrator)",
             [
                 "Где работает: Выделенная виртуальная машина или Kubernetes Pod.",
                 "Процесс: replicator-orchestrator (Java 21 LTS / Spring Boot 3).",
                 "Порт: 8005 (HTTP/REST API, SSE подписки, веб-консоль Svelte 5).",
                 "База данных: PostgreSQL / H2 (хранение задач, cron расписаний, истории).",
                 "⚠️ Роль: Только координация и лимиты. Файлы через него НЕ идут!"
             ], ACCENT_INDIGO)

    add_card(slide3, Inches(6.8), Inches(1.85), Inches(5.7), Inches(2.4),
             "2. ЦОД-1 Узлы (Primary — Москва)",
             [
                 "Где работает: Узлы DataNode кластера или выделенные Edge Gateway узлы.",
                 "Процесс: replicator-agent-dc1 (Java 21 / Netty gRPC демон).",
                 "Порт: 50051 (gRPC Server, Full-Duplex режим).",
                 "Kerberos: Системный keytab hdfs-cluster-1.keytab.",
                 "Роль: Анализ Diff, чтение из HDFS pod UGI doAs, опрос HMS NOTIFICATION_LOG, отправка в WAN."
             ], ACCENT_GREEN)

    add_card(slide3, Inches(0.8), Inches(4.45), Inches(5.7), Inches(2.35),
             "3. ЦОД-2 Узлы (Standby / DR — Санкт-Петербург)",
             [
                 "Где работает: Узлы DataNode кластера или Edge Gateway узлы DC2.",
                 "Процесс: replicator-agent-dc2 (Java 21 / Netty gRPC демон).",
                 "Порт: 50051 (gRPC Server, Full-Duplex режим).",
                 "Kerberos: Системный keytab hdfs-cluster-2.keytab.",
                 "Роль: Прием 4 МБ чанков, запись в ._staging_, атомарный rename, накат DDL в HMS DC2."
             ], ACCENT_CYAN)

    add_card(slide3, Inches(6.8), Inches(4.45), Inches(5.7), Inches(2.35),
             "4. Рабочие станции инженеров и клиентов",
             [
                 "Где работает: Браузер пользователя (Chrome, Safari, Firefox).",
                 "Сетевой доступ: HTTPS :8005 к Orchestrator.",
                 "Аутентификация: Kerberos SPNEGO SSO в 1 клик или LDAP логин/пароль.",
                 "Роли RBAC: ADMIN (полный доступ + DR), WRITER (свои задачи), READER (аудит)."
             ], ACCENT_AMBER)

    # =========================================================================
    # СЛАЙД 4: СЕТЕВАЯ МАТРИЦА И ПРАВИЛА ФАЕРВОЛА
    # =========================================================================
    slide4 = create_base_slide(prs, 4, "Сетевая безопасность",
                               "Сетевая матрица портов и фаервола (Network Matrix)",
                               "Какие порты открываются в межЦОДных межсетевых экранах (WAN) и внутри дата-центров (LAN)")

    headers4 = ["Направление трафика", "Протокол", "Порт", "Назначение", "Сетевой сегмент"]
    rows4 = [
        ["Agent DC1 ➔ Agent DC2", "gRPC / HTTP/2 (mTLS)", "TCP 50051", "Прямая передача блоков HDFS и DDL пакетов HMS", "WAN (МежЦОД)"],
        ["Agent DC2 ➔ Agent DC1", "gRPC / HTTP/2 (mTLS)", "TCP 50051", "Обратная репликация Reverse Replication в DR", "WAN (МежЦОД)"],
        ["Браузер ➔ Orchestrator", "HTTPS / HTTP", "TCP 8005", "Доступ к UI Svelte 5, REST API, SSE событиям", "Corporate LAN"],
        ["Agents ➔ Orchestrator", "HTTP REST", "TCP 8005", "Heartbeat (5с), Claim подзадач, Lease продление", "Management LAN"],
        ["Agent ➔ NameNode (локально)", "Hadoop RPC", "TCP 9000 / 8020", "Листинг каталогов, метаданные блоков, атомарный rename", "DC LAN (Внутри ЦОД)"],
        ["Agent ➔ DataNodes (локально)", "Data Transfer Protocol", "TCP 9866 (SASL)", "Прямое чтение и запись блоков HDFS", "DC LAN (Внутри ЦОД)"],
        ["Agent ➔ Hive Metastore", "Thrift RPC", "TCP 9083", "Чтение NOTIFICATION_LOG (DC1) и применение DDL (DC2)", "DC LAN (Внутри ЦОД)"],
        ["Agent ➔ Kerberos KDC", "Kerberos AS/TGS", "TCP/UDP 88", "Получение тикетов по keytab техучетки", "DC LAN (Внутри ЦОД)"]
    ]
    add_table_custom(slide4, Inches(0.8), Inches(1.85), Inches(11.733), Inches(4.8), headers4, rows4,
                     [Inches(2.7), Inches(1.8), Inches(1.3), Inches(4.333), Inches(1.6)])

    # =========================================================================
    # СЛАЙД 5: ЖИЗНЕННЫЙ ЦИКЛ ПЕРЕДАЧИ HDFS ФАЙЛА
    # =========================================================================
    slide5 = create_base_slide(prs, 5, "HDFS Data Flow",
                               "Как ходит трафик при репликации файлов HDFS (Пошаговый цикл)",
                               "От анализа дельты до атомарного переименования в целевом кластере")

    add_card(slide5, Inches(0.8), Inches(1.85), Inches(5.7), Inches(2.35),
             "Шаг 1. Анализ дельты и планирование",
             [
                 "1. Воркер в DC1 забирает подзадачу из Orchestrator (:8005 /tasks/claim).",
                 "2. Запрашивает манифест у локальной NameNode DC1 (:9000).",
                 "3. Одним gRPC вызовом GetDirectoryManifest запрашивает манифест у Agent DC2.",
                 "4. В памяти строится O(N) Diff: неизмененные файлы пропускаются (0 байт WAN!)."
             ], ACCENT_INDIGO)

    add_card(slide5, Inches(6.8), Inches(1.85), Inches(5.7), Inches(2.35),
             "Шаг 2. Чтение блоков и упаковка",
             [
                 "5. Агент DC1 читает блоки из DataNodes DC1 (:9866) под UGI автора задачи (doAs).",
                 "6. Запрашивает разрешение на передачу у Token Bucket шейпера полосы.",
                 "7. Файлы < 1 МБ упаковываются в виртуальный Tar-Stream на лету.",
                 "8. Файлы >= 1 МБ нарезаются на чанки по 4 МБ со сжатием Zstd/LZ4."
             ], ACCENT_GREEN)

    add_card(slide5, Inches(0.8), Inches(4.35), Inches(5.7), Inches(2.45),
             "Шаг 3. Прямой gRPC WAN стриминг",
             [
                 "9. Агент DC1 стримит чанки НАПРЯМУЮ в Agent DC2 (:50051 gRPC, mTLS).",
                 "10. Никакие байты файлов НЕ проходят через Оркестратор!",
                 "11. Потоковое вычисление контрольной суммы SHA-256 на обеих сторонах.",
                 "12. Скорость строго удерживается шейпером Token Bucket."
             ], ACCENT_CYAN)

    add_card(slide5, Inches(6.8), Inches(4.35), Inches(5.7), Inches(2.45),
             "Шаг 4. Zero-Staging и фиксация в HDFS",
             [
                 "13. Агент DC2 пишет блоки в DataNodes DC2 (:9866) во временный файл ._staging_.",
                 "14. Сверка хэша SHA-256: при совпадении вызывается атомарный fs.rename().",
                 "15. Для мелких файлов Tar-Stream распаковывается прямо в HDFS без диска.",
                 "16. Агент DC1 отправляет рапорт в Orchestrator (:8005 /progress) с обновлением ETA."
             ], ACCENT_AMBER)

    # =========================================================================
    # СЛАЙД 6: ЖИЗНЕННЫЙ ЦИКЛ HIVE METASTORE CDC
    # =========================================================================
    slide6 = create_base_slide(prs, 6, "Metadata Data Flow",
                               "Как ходит трафик при репликации Hive Metastore (HMS CDC)",
                               "Потоковая передача DDL-событий с распределенным лизингом Inotify Lease HA")

    add_card(slide6, Inches(0.8), Inches(1.85), Inches(5.7), Inches(2.35),
             "1. Захват эксклюзивной аренды (Lease)",
             [
                 "Агент DC1 запрашивает аренду схемы: POST /hms/lease/claim (Оркестратор).",
                 "Оркестратор выдает эксклюзивный токен аренды на 60 секунд.",
                 "Исключены гонки: ровно один воркер в кластере читает CDC-поток схемы.",
                 "При сбое воркера аренда протухает, и другой агент подхватывает стрим."
             ], ACCENT_INDIGO)

    add_card(slide6, Inches(6.8), Inches(1.85), Inches(5.7), Inches(2.35),
             "2. Локальный опрос NOTIFICATION_LOG",
             [
                 "Агент DC1 по LAN Thrift :9083 вычитывает события из Hive Metastore DC1.",
                 "События: CREATE_TABLE, ADD_PARTITION, ALTER_TABLE, DROP_PARTITION.",
                 "Non-ACID Gate: ACID transactional таблицы безопасно пропускаются.",
                 "Пакеты событий формируются пачками для минимизации RPC."
             ], ACCENT_AMBER)

    add_card(slide6, Inches(0.8), Inches(4.35), Inches(5.7), Inches(2.45),
             "3. Передача пакетов DDL по WAN",
             [
                 "Агент DC1 передает пачку DDL в Agent DC2 по WAN :50051 (gRPC contract).",
                 "Агент DC2 транслирует Federation NameService: hdfs://ns-dc1/ ➔ hdfs://ns-dc2/.",
                 "Перелинковка sdLocation на целевой кластер и генерация саб-джобов HDFS.",
                 "Изоляция сабтасок: перенос файлов партиций скрыт из основного списка."
             ], ACCENT_GREEN)

    add_card(slide6, Inches(6.8), Inches(4.35), Inches(5.7), Inches(2.45),
             "4. Применение DDL и подтверждение",
             [
                 "Агент DC2 по LAN Thrift :9083 накатывает DDL в Hive Metastore DC2.",
                 "Безопасность: при DROP_TABLE флаг deleteData=false (файлы не стираются!).",
                 "Агент DC2 подтверждает накат ➔ Агент DC1 рапортует прогресс в Orchestrator.",
                 "Фиксация last_processed_event_id: позиция гарантированно сохранена."
             ], ACCENT_CYAN)

    # =========================================================================
    # СЛАЙД 7: ТРАФИК В DISASTER RECOVERY
    # =========================================================================
    slide7 = create_base_slide(prs, 7, "Disaster Recovery Traffic",
                               "Потоки трафика в Disaster Recovery: Штатно vs Kill-Switch vs Reverse",
                               "Как ведет себя сеть при аварии основного ЦОД и как разворачивается поток данных")

    add_card(slide7, Inches(0.8), Inches(1.85), Inches(3.7), Inches(4.8),
             "1. Штатный режим (DC1 ➔ DC2)",
             [
                 "Трафик клиентов: Направлен на DC1.",
                 "Data Plane WAN: Поток идет от Agent DC1 в Agent DC2 (:50051).",
                 "Шейпер: Лимит 100 МБ/с.",
                 "HMS CDC: Стриминг дельты в DC2.",
                 "DC2 выступает пассивным Standby-приемником."
             ], ACCENT_GREEN)

    add_card(slide7, Inches(4.8), Inches(1.85), Inches(3.7), Inches(4.8),
             "2. Авария DC1 и Kill-Switch",
             [
                 "Событие: DC1 упал. Клиенты переключены на DC2.",
                 "Действие оператора: Нажатие 🛑 Kill-Switch.",
                 "WAN сетевой барьер: Лимит канала ➔ 0 МБ/с (Fencing).",
                 "Задачи: Все прямые задачи заморожены (STOPPED, Cron OFF).",
                 "Защита от Split-Brain: При оживании DC1 старые задачи НЕ запустятся и не затрут свежие данные DC2!"
             ], ACCENT_RED)

    add_card(slide7, Inches(8.8), Inches(1.85), Inches(3.7), Inches(4.8),
             "3. Оживание DC1 и Reverse",
             [
                 "Unfence: Снятие изоляции открывает сеть (100 МБ/с), задачи остаются STOPPED.",
                 "Reverse Replication: Нажатие 🔄 Reverse Replication.",
                 "РАЗВОРОТ ТРАФИКА: Agent DC2 становится Sender ➔ Agent DC1 (:50051 Receiver).",
                 "Догон дельты: DC2 выкачивает накопленные изменения обратно в DC1 до RPO=0.",
                 "Failback: Возврат клиентов на DC1, отзыв зеркал."
             ], ACCENT_CYAN)

    # =========================================================================
    # СЛАЙД 8: СКРИНШОТ ТОПОЛОГИИ И ШЕЙПЕРА
    # =========================================================================
    slide8 = create_base_slide(prs, 8, "Интерфейс оператора",
                               "Топология ЦОД и управление полосой WAN в интерфейсе",
                               "Рантайм-управление квотами пропускной способности без перезапуска воркеров")

    add_image_card(slide8, Inches(0.8), Inches(1.85), Inches(7.5), Inches(4.8),
                   os.path.join(img_dir, "04_topology_bandwidth.png"),
                   "Консоль «Топология ЦОД и Полоса»: шейпер Token Bucket (DC-DC, HDFS-HDFS, Global)")

    add_card(slide8, Inches(8.5), Inches(1.85), Inches(4.0), Inches(4.8),
             "⚡ Физика шейпинга в Data Plane",
             [
                 "Где работает: Внутри процесса каждого Replicator Agent перед отправкой 4 МБ чанка.",
                 "Как работает: Вычисление задержки по формуле max(global, dc_dc, hdfs_hdfs).",
                 "Реакция на мутацию: Оператор нажимает «Сохранить» в UI ➔ Оркестратор пушит лимит в кэш ➔ воркеры применяют за < 1 сек.",
                 "Защита WAN: Ни при каких обстоятельствах суммарный трафик репликации не превысит установленный потолок."
             ], ACCENT_CYAN)

    # =========================================================================
    # СЛАЙД 9: СКРИНШОТ ГЛАВНОГО ДАШБОРДА
    # =========================================================================
    slide9 = create_base_slide(prs, 9, "Интерфейс оператора",
                               "Главная панель управления HDFS и создание задач",
                               "Мониторинг скорости в реальном времени (⚡ МБ/с), расчет ETA и Kerberos doAs")

    add_image_card(slide9, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.8),
                   os.path.join(img_dir, "02_main_dashboard.png"),
                   "Главная панель HDFS Replication: статус задач, скорость (⚡), ETA и фильтры")

    add_image_card(slide9, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.8),
                   os.path.join(img_dir, "03_create_job_modal.png"),
                   "Мастер создания задачи: пути, расписание Cron, UGI doAs имперсонация")

    # =========================================================================
    # СЛАЙД 10: СКРИНШОТ HMS REPLICATION
    # =========================================================================
    slide10 = create_base_slide(prs, 10, "Интерфейс оператора",
                                "Консоль репликации Hive Metastore (HMS Replication)",
                                "Мониторинг стримеров CDC, Event Lag и мастер создания схемы")

    add_image_card(slide10, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.8),
                   os.path.join(img_dir, "06_hms_replication_dashboard.png"),
                   "Консоль HMS Replication: статус CDC-стримеров, Event Lag и список схем")

    add_image_card(slide10, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.8),
                   os.path.join(img_dir, "07_create_hms_modal.png"),
                   "Мастер создания схемы: выбор баз, фильтр таблиц, Reconciliation опции")

    # =========================================================================
    # СЛАЙД 11: СКРИНШОТ DR HUB И KILL-SWITCH
    # =========================================================================
    slide11 = create_base_slide(prs, 11, "Интерфейс Disaster Recovery",
                                "DR Hub, экстренный Kill-Switch и состояние сетевого ограждения",
                                "Интуитивный интерфейс дежурной смены при аварии дата-центра")

    add_image_card(slide11, Inches(0.8), Inches(1.85), Inches(6.2), Inches(4.8),
                   os.path.join(img_dir, "08_disaster_recovery_dashboard.png"),
                   "Консоль DR & Failover Hub: доступность ЦОД, направление потока и суммарный лаг дельты")

    add_image_card(slide11, Inches(7.3), Inches(1.85), Inches(5.2), Inches(2.3),
                   os.path.join(img_dir, "09_emergency_kill_switch_modal.png"),
                   "Модальное окно Kill-Switch: подтверждение останова и сетевое ограждение (0 МБ/с)")

    add_image_card(slide11, Inches(7.3), Inches(4.35), Inches(5.2), Inches(2.3),
                   os.path.join(img_dir, "10_disaster_recovery_fenced_state.png"),
                   "Индикация подавленного кластера: бейдж «ПОДАВЛЕН 🔒» и тревожный баннер")

    # =========================================================================
    # СЛАЙД 12: СКРИНШОТ UNFENCE И REVERSE REPLICATION
    # =========================================================================
    slide12 = create_base_slide(prs, 12, "Интерфейс Disaster Recovery",
                                "Безопасный откат (Unfence) и запуск Reverse Replication (DC2 ➔ DC1)",
                                "Восстановление сети без перезаписи резерва и автоматический разворот потока данных")

    add_image_card(slide12, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.8),
                   os.path.join(img_dir, "11_rollback_unfence_modal.png"),
                   "Модальное окно Unfence: снятие сетевого барьера без запуска старых задач")

    add_image_card(slide12, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.8),
                   os.path.join(img_dir, "12_reverse_replication_modal.png"),
                   "Мастер Reverse Replication: разворот потока данных DC2 ➔ DC1 с подтверждением")

    # =========================================================================
    # СЛАЙД 13: РЕКОМЕНДАЦИИ ПО САЙЗИНГУ И РАЗВЕРТЫВАНИЮ
    # =========================================================================
    slide13 = create_base_slide(prs, 13, "Внедрение в Production",
                                "Рекомендации по сайзингу, развертыванию и фаерволу",
                                "Оптимальные архитектурные конфигурации для промышленного внедрения")

    add_card(slide13, Inches(0.8), Inches(1.85), Inches(3.7), Inches(4.8),
             "🖥️ Сайзинг Orchestrator",
             [
                 "Размещение: Выделенная VM / K8s Pod.",
                 "CPU: 4–8 vCPU.",
                 "RAM: 8–16 GB Heap (Java 21).",
                 "Диск: 50–100 GB NVMe (для PostgreSQL БД истории).",
                 "Сеть: 1 Gbps LAN.",
                 "High Availability: Active-Passive с плавающим IP или K8s Deployment с 1 репликой и persistent volume."
             ], ACCENT_INDIGO)

    add_card(slide13, Inches(4.8), Inches(1.85), Inches(3.7), Inches(4.8),
             "⚡ Сайзинг Replicator Agents",
             [
                 "Вариант 1 (Colocated): На узлах DataNode. Максимальный Zero-Copy перформанс локального HDFS.",
                 "Вариант 2 (Gateway): Выделенные Edge-серверы 2–4 шт на ЦОД с 25G/40G сетевыми картами.",
                 "CPU: 8–16 vCPU на агента.",
                 "RAM: 16–32 GB Heap (Netty off-heap буферы).",
                 "Сеть: 10G / 25G / 40G NIC.",
                 "Масштабирование: Добавление агентов в кластер линейно ускоряет параллельный перенос."
             ], ACCENT_GREEN)

    add_card(slide13, Inches(8.8), Inches(1.85), Inches(3.7), Inches(4.8),
             "🛡️ Чеклист фаервола и ИБ",
             [
                 "WAN межЦОД: Открыть TCP 50051 между пулами агентов DC1 и DC2.",
                 "mTLS: Включить взаимную аутентификацию по X.509 сертификатам.",
                 "Kerberos: Сгенерировать keytab hdfs-replicator с правами Proxy User в core-site.xml.",
                 "SASL: Включить dfs.data.transfer.protection = privacy.",
                 "Ranger: Настроить политики доступа на уровне UGI авторов задач."
             ], ACCENT_CYAN)

    prs.save(output_pptx_path)
    print(f"✅ Презентация успешно сохранена: {output_pptx_path} ({os.path.getsize(output_pptx_path) / 1024:.1f} KB)")

if __name__ == "__main__":
    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "replicator-presentation.pptx")
    if len(sys.argv) > 1:
        out_path = sys.argv[1]
    generate_presentation(out_path)

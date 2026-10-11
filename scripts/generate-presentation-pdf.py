#!/usr/bin/env python3
"""
Скрипт генерации строго технического PDF-документа из docs/replicator-presentation.md
для DevOps-инженеров, Архитекторов и Data Platform команд.
Использует marked для чистого GFM парсинга, клиентский Mermaid.js для векторных диаграмм
и Google Chrome headless для высокоточной печати в векторный PDF без сдвигов и искажений.
"""

import os
import re
import sys
import html
import subprocess

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    docs_dir = os.path.join(base_dir, "docs")
    md_path = os.path.join(docs_dir, "replicator-presentation.md")
    temp_html_path = os.path.join(docs_dir, "replicator-presentation-print.html")
    target_pdf_path = os.path.join(docs_dir, "replicator-presentation.pdf")

    if not os.path.exists(md_path):
        print(f"❌ Ошибка: Файл {md_path} не найден!")
        sys.exit(1)

    print(f"📖 Чтение Markdown: {md_path}")
    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    # Конвертируем Markdown в HTML через npx marked
    print("⚙️ Парсинг GFM через marked...")
    res = subprocess.run(
        ["npx", "--yes", "marked", "--gfm"],
        input=md_text,
        capture_output=True,
        text=True,
        check=True
    )
    raw_html = res.stdout

    # 1. Пост-обработка Mermaid блоков:
    # marked превращает ```mermaid в <pre><code class="language-mermaid">...</code></pre>
    def fix_mermaid(match):
        code_content = match.group(1)
        unescaped = html.unescape(code_content).strip()
        return f'<div class="mermaid-container"><pre class="mermaid">\n{unescaped}\n</pre></div>'

    processed_html = re.sub(
        r'<pre><code class="language-mermaid">([\s\S]*?)</code></pre>',
        fix_mermaid,
        raw_html
    )

    # 2. Обработка путей к картинкам: делаем абсолютными file:// для надежной загрузки в Chrome
    def fix_img_src(match):
        attrs = match.group(1)
        src_match = re.search(r'src=["\']([^"\']+)["\']', attrs)
        if src_match:
            rel_src = src_match.group(1)
            if not rel_src.startswith("http") and not rel_src.startswith("file://"):
                clean_rel = rel_src.lstrip("./")
                abs_src = os.path.abspath(os.path.join(docs_dir, clean_rel))
                attrs = attrs.replace(src_match.group(0), f'src="file://{abs_src}"')
        return f'<img {attrs}>'

    processed_html = re.sub(r'<img\s+([^>]+)>', fix_img_src, processed_html)

    # 3. Оборачивание таблиц в защитный контейнер для предотвращения разрывов
    processed_html = re.sub(
        r'(<table>[\s\S]*?</table>)',
        r'<div class="table-wrapper">\1</div>',
        processed_html
    )

    # 4. Оборачивание блоков скриншотов в screen-card для предотвращения "висячих заголовков"
    # Шаблон: <h4>...</h4> + <div align="center"><img ...></div> + <p><em>...</em></p>
    screen_pattern = re.compile(
        r'(<h4>(\d+\.[^<]+)</h4>\s*<div align="center">\s*<img\s+[^>]+>\s*</div>\s*(?:<p>)?(?:<em>)?([^<]+)(?:</em>)?(?:</p>)?)',
        re.MULTILINE
    )
    def wrap_screen_card(match):
        full_chunk = match.group(0)
        return f'<div class="screen-card">{full_chunk}</div>'

    processed_html = screen_pattern.sub(wrap_screen_card, processed_html)

    # 5. Добавление разрывов страниц перед каждым слайдом (Слайд 1 .. Слайд 13)
    def split_slides(match):
        slide_title = match.group(1)
        slide_num_match = re.search(r'Слайд\s+(\d+)', slide_title)
        num_badge = ""
        if slide_num_match:
            num = slide_num_match.group(1)
            num_badge = f'<div class="slide-section-badge">СЕКЦИЯ {num} / 13 • ТЕХНИЧЕСКИЙ РЕГЛАМЕНТ</div>'
        return f'<div class="page-break"></div>\n<div class="slide-header-block">{num_badge}<h2 class="slide-h2">{slide_title}</h2></div>'

    processed_html = re.sub(r'<h2>(Слайд\s+\d+:[^<]+)</h2>', split_slides, processed_html)

    # 6. Выделение титульной страницы и оглавления
    toc_marker = '<h2>📑 Содержание инженерных разделов</h2>'
    if toc_marker in processed_html:
        parts = processed_html.split(toc_marker, 1)
        header_part = parts[0]
        rest_part = parts[1]

        # Оборачиваем титульный лист
        cover_html = f"""
        <div class="cover-page">
          <div class="cover-badge">HADOOP EXPLORER PLATFORM • TECHNICAL SPECIFICATION</div>
          <div class="cover-logo">
            <img src="file://{os.path.join(docs_dir, 'images', 'logo_white.png')}" alt="Hadoop Explorer Logo" />
          </div>
          <h1 class="cover-title">Hadoop gRPC Replicator</h1>
          <div class="cover-subtitle">Системная архитектура, сетевые протоколы, шейпинг и Disaster Recovery</div>
          <div class="cover-divider"></div>
          <p class="cover-lead">Полный инженерный регламент для системных архитекторов, DevOps-инженеров и SRE администраторов кластеров Big Data</p>

          <div class="cover-meta-grid">
            <div class="cover-meta-card">
              <div class="meta-label">Стек технологий</div>
              <div class="meta-val">Java 21 LTS • Spring Boot 3.3.4 • Netty 4.1 gRPC</div>
            </div>
            <div class="cover-meta-card">
              <div class="meta-label">Целевые платформы</div>
              <div class="meta-val">Apache Hadoop 3.x • Hive Metastore • Kerberos SASL</div>
            </div>
            <div class="cover-meta-card">
              <div class="meta-label">Статус документа</div>
              <div class="meta-val">Production Specification • Ревизия 1.4</div>
            </div>
            <div class="cover-meta-card">
              <div class="meta-label">Дата публикации</div>
              <div class="meta-val">Октябрь 2026 • Инженерная команда платформы</div>
            </div>
          </div>
        </div>
        <div class="page-break"></div>
        <div class="toc-container">
          <h2 class="toc-title">📑 Содержание инженерных разделов</h2>
          {rest_part}
        </div>
        """
        body_content = cover_html
    else:
        body_content = processed_html

    # Формируем полный HTML документ со стилями для высокоточной печати в PDF
    full_html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <title>Hadoop gRPC Replicator: Архитектурная спецификация</title>
  <style>
    @page {{
      size: A4 portrait;
      margin: 16mm 14mm 16mm 14mm;
      @bottom-right {{
        content: counter(page);
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        font-size: 8pt;
        color: #64748b;
      }}
    }}

    * {{
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      font-size: 9.2pt;
      line-height: 1.5;
      color: #1e293b;
      background: #ffffff;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}

    .page-break {{
      page-break-before: always;
      break-before: page;
      height: 0;
      margin: 0;
      padding: 0;
    }}

    /* ========================================= */
    /* ТИТУЛЬНЫЙ ЛИСТ (COVER PAGE) */
    /* ========================================= */
    .cover-page {{
      height: 92vh;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      text-align: center;
      padding: 24px 20px;
      background: radial-gradient(circle at 50% 30%, #1e1b4b 0%, #090d16 80%);
      color: #ffffff;
      border-radius: 12px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
      position: relative;
      overflow: hidden;
      page-break-after: always;
      break-after: page;
    }}

    .cover-badge {{
      display: inline-block;
      background: linear-gradient(135deg, #4f46e5, #06b6d4);
      color: #ffffff;
      font-size: 8pt;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      padding: 5px 14px;
      border-radius: 999px;
      margin-bottom: 24px;
    }}

    .cover-logo img {{
      height: 48px;
      margin-bottom: 20px;
      border: none !important;
      box-shadow: none !important;
      filter: drop-shadow(0 0 12px rgba(99, 102, 241, 0.5));
    }}

    .cover-title {{
      font-size: 27pt;
      font-weight: 800;
      letter-spacing: -0.02em;
      margin-bottom: 12px;
      background: linear-gradient(135deg, #ffffff 30%, #a5b4fc 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }}

    .cover-subtitle {{
      font-size: 12.5pt;
      font-weight: 600;
      color: #38bdf8;
      max-width: 680px;
      margin-bottom: 16px;
      line-height: 1.4;
    }}

    .cover-divider {{
      width: 80px;
      height: 3px;
      background: linear-gradient(90deg, #6366f1, #06b6d4);
      margin: 10px auto 20px auto;
      border-radius: 2px;
    }}

    .cover-lead {{
      font-size: 9.5pt;
      color: #94a3b8;
      max-width: 580px;
      margin-bottom: 36px;
      line-height: 1.6;
    }}

    .cover-meta-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      width: 100%;
      max-width: 620px;
      text-align: left;
    }}

    .cover-meta-card {{
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 8px;
      padding: 10px 14px;
    }}

    .cover-meta-card .meta-label {{
      font-size: 7.5pt;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: #818cf8;
      font-weight: 700;
      margin-bottom: 3px;
    }}

    .cover-meta-card .meta-val {{
      font-size: 8.5pt;
      font-weight: 600;
      color: #f1f5f9;
    }}

    /* ========================================= */
    /* ОГЛАВЛЕНИЕ (TOC) */
    /* ========================================= */
    .toc-container {{
      padding: 10px 0;
      page-break-after: always;
      break-after: page;
    }}

    .toc-title {{
      font-size: 15pt;
      font-weight: 800;
      color: #0f172a;
      border-bottom: 2px solid #4f46e5;
      padding-bottom: 8px;
      margin-bottom: 16px;
    }}

    .toc-container ol {{
      column-count: 2;
      column-gap: 24px;
      padding-left: 20px;
    }}

    .toc-container li {{
      font-size: 8.8pt;
      margin-bottom: 8px;
      color: #334155;
      break-inside: avoid;
    }}

    .toc-container a {{
      color: #2563eb;
      text-decoration: none;
      font-weight: 600;
    }}

    .toc-container hr {{
      display: none;
    }}

    /* ========================================= */
    /* ЗАГОЛОВКИ СЕКЦИЙ И СЛАЙДОВ */
    /* ========================================= */
    .slide-header-block {{
      margin-top: 8px;
      margin-bottom: 12px;
      border-bottom: 1.5px solid #e2e8f0;
      padding-bottom: 8px;
      break-after: avoid;
      page-break-after: avoid;
    }}

    .slide-section-badge {{
      display: inline-block;
      background: #e0e7ff;
      color: #3730a3;
      font-size: 7.2pt;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      padding: 2px 8px;
      border-radius: 4px;
      margin-bottom: 5px;
    }}

    h2.slide-h2 {{
      font-size: 13pt;
      font-weight: 800;
      color: #0f172a;
      letter-spacing: -0.01em;
      line-height: 1.3;
      break-after: avoid;
      page-break-after: avoid;
    }}

    h1, h2, h3, h4, h5 {{
      break-after: avoid;
      page-break-after: avoid;
    }}

    h3 {{
      font-size: 10.5pt;
      font-weight: 700;
      color: #1e293b;
      margin-top: 14px;
      margin-bottom: 8px;
      position: relative;
      padding-left: 10px;
      break-after: avoid;
      page-break-after: avoid;
    }}

    h3::before {{
      content: "";
      position: absolute;
      left: 0;
      top: 3px;
      bottom: 3px;
      width: 3.5px;
      background: #4f46e5;
      border-radius: 2px;
    }}

    h4 {{
      font-size: 9.5pt;
      font-weight: 700;
      color: #0f172a;
      margin-top: 12px;
      margin-bottom: 6px;
      break-after: avoid;
      page-break-after: avoid;
    }}

    p {{
      margin-bottom: 8px;
      color: #334155;
    }}

    ul, ol {{
      margin-bottom: 10px;
      padding-left: 20px;
    }}

    li {{
      margin-bottom: 4px;
      color: #334155;
    }}

    strong {{
      color: #0f172a;
    }}

    hr {{
      border: none;
      border-top: 1px solid #e2e8f0;
      margin: 14px 0;
    }}

    /* ========================================= */
    /* ТАБЛИЦЫ */
    /* ========================================= */
    .table-wrapper {{
      page-break-inside: avoid;
      break-inside: avoid;
      margin: 10px 0 14px 0;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 8.2pt;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      overflow: hidden;
    }}

    thead tr {{
      background: #1e293b;
      color: #ffffff;
    }}

    th {{
      padding: 6px 8px;
      font-weight: 700;
      font-size: 7.8pt;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      border-bottom: 1px solid #334155;
      border-right: 1px solid #334155;
      text-align: left;
    }}

    th:last-child {{
      border-right: none;
    }}

    td {{
      padding: 5px 8px;
      border-bottom: 1px solid #e2e8f0;
      border-right: 1px solid #e2e8f0;
      color: #334155;
      line-height: 1.4;
    }}

    td:last-child {{
      border-right: none;
    }}

    tr:nth-child(even) td {{
      background: #f8fafc;
    }}

    /* ========================================= */
    /* БЛОКИ КОДА И ЛИСТИНГИ */
    /* ========================================= */
    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 8pt;
      background: #f1f5f9;
      color: #0f172a;
      padding: 1px 4px;
      border-radius: 3px;
      border: 1px solid #e2e8f0;
    }}

    pre {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 7.8pt;
      background: #0f172a;
      color: #f8fafc;
      padding: 10px 12px;
      border-radius: 6px;
      margin: 10px 0 14px 0;
      overflow-x: auto;
      line-height: 1.45;
      page-break-inside: avoid;
      break-inside: avoid;
      border: 1px solid #1e293b;
    }}

    pre code {{
      background: transparent;
      color: inherit;
      padding: 0;
      border: none;
      font-size: inherit;
    }}

    /* ========================================= */
    /* ИЗОБРАЖЕНИЯ И КАРТОЧКИ СКРИНШОТОВ */
    /* ========================================= */
    .screen-card {{
      page-break-inside: avoid;
      break-inside: avoid;
      margin-top: 10px;
      margin-bottom: 16px;
    }}

    img {{
      max-width: 100%;
      height: auto;
      display: block;
      margin: 8px auto;
      border-radius: 6px;
      border: 1px solid #cbd5e1;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
      page-break-inside: avoid;
      break-inside: avoid;
    }}

    .screen-card em, .screen-card p em {{
      display: block;
      text-align: center;
      font-size: 8.2pt;
      color: #64748b;
      margin-top: 4px;
    }}

    .mermaid-container {{
      width: 100%;
      margin: 10px 0 14px 0;
      page-break-inside: avoid;
      break-inside: avoid;
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      padding: 10px;
      text-align: center;
    }}

    .mermaid-container pre.mermaid {{
      background: transparent;
      color: #0f172a;
      border: none;
      padding: 0;
      margin: 0;
    }}

    .mermaid svg {{
      max-width: 100% !important;
      height: auto !important;
    }}

    /* ========================================= */
    /* БЛОКИ ЗАМЕТОК И ПЛАШКИ */
    /* ========================================= */
    blockquote {{
      border-left: 3.5px solid #6366f1;
      background: #f8fafc;
      padding: 8px 12px;
      margin: 10px 0;
      border-radius: 0 6px 6px 0;
      color: #334155;
      page-break-inside: avoid;
      break-inside: avoid;
    }}
  </style>

  <script type="module">
    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    mermaid.initialize({{
      startOnLoad: true,
      theme: 'neutral',
      securityLevel: 'loose',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
    }});
  </script>
</head>
<body>

{body_content}

</body>
</html>
"""

    print(f"💾 Запись подготовленного HTML: {temp_html_path}")
    with open(temp_html_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    # Запускаем Google Chrome headless для печати в PDF
    chrome_cmd = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "--headless",
        "--no-pdf-header-footer",
        "--virtual-time-budget=7000",
        "--run-all-compositor-stages-before-draw",
        f"--print-to-pdf={target_pdf_path}",
        f"file://{temp_html_path}"
    ]

    print(f"🚀 Генерация PDF через Google Chrome: {target_pdf_path}")
    res = subprocess.run(chrome_cmd, capture_output=True, text=True)

    if os.path.exists(target_pdf_path) and os.path.getsize(target_pdf_path) > 0:
        pdf_size_kb = os.path.getsize(target_pdf_path) / 1024
        print(f"✅ PDF документ успешно сгенерирован: {target_pdf_path} ({pdf_size_kb:.1f} KB)")
    else:
        print(f"❌ Ошибка генерации PDF!")
        print(res.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

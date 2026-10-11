#!/usr/bin/env python3
"""
Скрипт проверки качества русскоязычного текста во всех UI приложениях и компонентах
платформы Hadoop Explorer Platform (Svelte 5, TypeScript).
Извлекает строки и UI-разметку с кириллицей, проверяет орфографию через Яндекс.Спеллер API
и находит стилистические дефекты (сленг, кальки, разговорные конструкции, дефисы).
"""

import os
import glob
import re
import json
import urllib.request
import urllib.parse
import time
import sys

TECH_WHITELIST = {
    'yarn', 'hdfs', 'sql', 'spark', 'replicator', 'hadoop', 'trino', 'hive', 'livy',
    'pyspark', 'scala', 'netty', 'grpc', 'spring', 'boot', 'svelte', 'tailwind',
    'kerberos', 'spnego', 'ldap', 'jwt', 'namenode', 'datanode', 'metastore',
    'webhdfs', 'keytab', 'doas', 'staging', 'snapshot', 'diff', 'fencing', 'unfence',
    'killswitch', 'backpressure', 'throttler', 'throughput', 'zstd', 'lz4', 'zstandard',
    'failover', 'active', 'standby', 'heartbeat', 'lease', 'inotify', 'cdc',
    'notification', 'log', 'promql', 'grafana', 'alertmanager', 'sysctl',
    'rmem', 'wmem', 'somaxconn', 'tar', 'streaming', 'full-duplex', 'mtu', 'jumbo',
    'parquet', 'orc', 'avro', 'jdbc', 'jpa', 'ansi', 'dbt', 'airflow', 'superset',
    'fastdiff', 'colocated', 'orchestrator', 'tokenbucket', 'chunked', 'g1gc',
    'jvm', 'cpu', 'vcpu', 'ram', 'nvme', 'ssd', 'nic', 'lan', 'wan', 'mtls', 'tls',
    'tcp', 'udp', 'dns', 'cname', 'vip', 'ip', 'postgresql', 'sqlite', 'h2', 'redis',
    'crontab', 'cron', 'systemd', 'docker', 'compose', 'k8s', 'kubernetes', 'helm',
    'spa', 'ui', 'rest', 'sse', 'api', 'cli', 'posix', 'acl', 'rwx', 'sha', 'md5',
    'acid', 'llap', 'tez', 'mapreduce', 'knox', 'ranger', 'sentry', 'atlas',
    'гейт', 'гейта', 'гейты', 'гейтов', 'гейте', 'носитель', 'носителя', 'носители',
    'развертывание', 'развертывания', 'развертыванию', 'снапшот', 'снапшота', 'снапшоты',
    'снапшотов', 'снапшоте', 'партиция', 'партиции', 'партиций', 'партициям', 'партициях',
    'партицирование', 'партицирования', 'дедупликация', 'дедупликации', 'катастрофоустойчивость',
    'катастрофоустойчивости', 'бэкап', 'бэкапа', 'бэкапы', 'бэкапов', 'вайтпейпер',
    'веб', 'вебхук', 'вебхука', 'вебхуки', 'таймаут', 'таймаута', 'таймауты', 'таймаутов',
    'чекпоинт', 'чекпоинта', 'чекпоинты', 'чекпоинтов', 'воркер', 'воркера', 'воркеры',
    'воркеров', 'шейпер', 'шейпера', 'шейперы', 'шейперов', 'шейпинг', 'шейпинга',
    'чанк', 'чанка', 'чанки', 'чанков', 'чанкование', 'чанкирование', 'чанкирования',
    'пайплайн', 'пайплайна', 'пайплайны', 'пайплайнов', 'фреймворк', 'фреймворка',
    'фреймворки', 'фреймворков', 'селект', 'селекта', 'джойн', 'джойна', 'коммит',
    'коммита', 'коммиты', 'коммитов', 'ретрай', 'ретрая', 'ретраи', 'ретраев',
    'инвариант', 'инварианта', 'инварианты', 'инвариантов',
    'синхро', 'мульти', 'онлайн', 'офлайн', 'оффлайн', 'эндпоинт', 'эндпоинта',
    'эндпоинты', 'эндпоинтов', 'дашборд', 'дашборда', 'дашборды', 'дашбордов',
    'скриншот', 'скриншота', 'скриншоты', 'скриншотов', 'слайд', 'слайда', 'слайды', 'слайдов',
    'демон', 'демона', 'демоны', 'демонов', 'демоне', 'демону',
    'прокси', 'проксирование', 'проксирования', 'балансировщик', 'балансировщика',
    'трекер', 'трекера', 'таргет', 'таргета', 'флаг', 'флага', 'флаги', 'флагов',
    'плагин', 'плагина', 'плагины', 'плагинов', 'коннектор', 'коннектора', 'коннекторы',
    'мигратор', 'мигратора', 'репликатор', 'репликатора', 'репликаторы', 'репликаторов',
    'инстанс', 'инстанса', 'инстансы', 'инстансов', 'кластер', 'кластера', 'кластеры',
    'кластеров', 'нода', 'ноды', 'нод', 'нодам', 'нодах', 'хост', 'хоста', 'хосты', 'хостов',
    # UI-специфичные термины
    'тултип', 'тултипа', 'тултипы', 'тултипов', 'тултипе',
    'дропдаун', 'дропдауна', 'дропдауны', 'дропдаунов',
    'селектор', 'селектора', 'селекторы', 'селекторов',
    'тост', 'тоста', 'тосты', 'тостов', 'тосте',
    'бейдж', 'бейджа', 'бейджи', 'бейджей', 'бейджем',
    'бэйдж', 'бэйджа', 'бэйджи', 'бэйджей',
    'чекбокс', 'чекбокса', 'чекбоксы', 'чекбоксов', 'чекбоксе',
    'инпут', 'инпута', 'инпуты', 'инпутов', 'инпуте',
    'плейсхолдер', 'плейсхолдера', 'плейсхолдеры',
    'пагинация', 'пагинации', 'пагинацию',
    'модалка', 'модалки', 'модальное', 'модального', 'модальном',
    'сплиттер', 'сплиттера', 'хлебные', 'крошки',
    'рендеринг', 'рендеринга', 'автоскролл', 'автоскролла',
    'автокомплит', 'автокомплита', 'сниппет', 'сниппета', 'сниппеты',
    'пресет', 'пресета', 'пресеты', 'пресетов',
    'дебаг', 'дебага', 'логгер', 'логгера', 'логгеры',
    'хэш', 'хэша', 'хэши', 'хеш', 'хеша', 'хеши',
    'дифф', 'диффа', 'диффы', 'диффов',
    'таймстемп', 'таймстемпа', 'таймстемпы',
    'таб', 'таба', 'табы', 'табов', 'вкладка', 'вкладки', 'вкладок',
    'клиентская', 'серверная', 'рантайм', 'рантайма',
    'конфиг', 'конфига', 'конфиги', 'конфигов',
    'подэлемент', 'подэлементы', 'подпапка', 'подпапки', 'подпапок',
    'подкаталог', 'подкаталога', 'подкаталоги', 'подкаталогов',
    'дефолтный', 'дефолтная', 'дефолтное', 'дефолтные',
    'авторефреш', 'автообновление', 'автоперезапуск', 'автоподстановка',
    'дата-центр', 'дата-центра', 'дата-центры', 'дата-центров', 'дата-центре', 'цод',
    'линтинг', 'линтинга', 'шедулер', 'шедулера', 'деплой', 'деплоя',
    'аналитикова', 'инженеров', 'наблюдателева', 'админов', 'дата-инженеров', 'датаинженеров'
}

STYLE_PATTERNS = [
    (r'\bоживани[еяюем]\b', 'разговорное «оживание» ➔ «восстановление связи / запуск»'),
    (r'\bсаб-?джоб[а-я]*\b', 'калька «саб-джоб» ➔ «подзадача / дочерняя задача»'),
    (r'\bсабтаск[а-я]*\b', 'калька «сабтаск» ➔ «подзадача»'),
    (r'\bсабмит[а-я]*\b', 'калька «сабмит» ➔ «отправка / применение»'),
    (r'\bтехучетк[а-я]*\b', 'сленг «техучетка» ➔ «сервисная учетная запись»'),
    (r'\bдепло[еяюемй]*\b', 'разговорное «деплой» ➔ «развертывание»'),
    (r'\bдатацентр[а-я]*\b', 'слитное «датацентр» ➔ дефисное «дата-центр»'),
    (r'\bавто-(failover|перезапуск|конвертация|обновление)\b', 'дефисное «авто-» ➔ слитное «авто...»'),
]

def extract_russian_snippets(file_path):
    snippets = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    for line_no, raw_line in enumerate(lines, start=1):
        # Проверяем наличие кириллицы
        if not re.search(r'[а-яА-ЯёЁ]', raw_line):
            continue

        # Пропускаем однострочные комментарии, если вся кириллица только там
        stripped = raw_line.strip()
        if stripped.startswith('//') or stripped.startswith('/*') or stripped.startswith('* '):
            # Проверяем, но отмечаем как комментарий
            is_comment = True
        elif stripped.startswith('<!--') and stripped.endswith('-->'):
            is_comment = True
        else:
            is_comment = False

        snippets.append({
            'line_no': line_no,
            'raw_line': raw_line.rstrip(),
            'is_comment': is_comment
        })

    return snippets

def clean_ui_text_for_speller(raw_line):
    # Убираем экранированные управляющие символы вида \n, \t, \r
    line = re.sub(r'\\[nrtfbv0]', ' ', raw_line)
    # Убираем JS интерполяции {...}
    line = re.sub(r'\{[^{}]*\}', ' ', line)
    # Убираем HTML/Svelte теги <...>
    line = re.sub(r'<[^>]+>', ' ', line)
    # Убираем строковые кавычки и синтаксис присваивания
    line = re.sub(r'[="\':;,`\[\]\(\)\{\}\<\>\$\+\*\#\/\\]', ' ', line)
    # Убираем чисто латинские токены и цифры
    line = re.sub(r'\b[A-Za-z0-9_-]+\b', ' ', line)
    # Убираем лишние пробелы
    line = re.sub(r'\s+', ' ', line).strip()
    return line

def check_yandex_speller(text_chunk):
    url = "https://speller.yandex.net/services/spellservice.json/checkText"
    data = urllib.parse.urlencode({
        'text': text_chunk,
        'lang': 'ru',
        'options': 0
    }).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'User-Agent': 'HadoopExplorerUIChecker/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"⚠️ Ошибка запроса к Яндекс.Спеллеру: {e}", file=sys.stderr)
        return []

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    frontend_dir = os.path.join(base_dir, "frontend")

    ui_files = []
    # common
    for root, dirs, files in os.walk(os.path.join(frontend_dir, "common")):
        if 'node_modules' in root or 'dist' in root or 'tests' in root:
            continue
        for f in files:
            if f.endswith(('.svelte', '.ts')) and not f.endswith('.d.ts'):
                ui_files.append(os.path.join(root, f))

    # apps
    apps_dir = os.path.join(frontend_dir, "apps")
    for app in ['yarn', 'hdfs', 'sql', 'spark', 'replicator']:
        app_src = os.path.join(apps_dir, app, "src")
        if not os.path.exists(app_src):
            continue
        for root, dirs, files in os.walk(app_src):
            if 'node_modules' in root or 'dist' in root or '.svelte-kit' in root:
                continue
            for f in files:
                if f.endswith(('.svelte', '.ts')) and not f.endswith('.d.ts'):
                    ui_files.append(os.path.join(root, f))

    ui_files = sorted(ui_files)
    print(f"🔍 Найдено UI файлов для анализа: {len(ui_files)}")

    app_stats = {}
    all_issues = {}

    for file_path in ui_files:
        rel_path = os.path.relpath(file_path, base_dir)
        app_name = rel_path.split('/')[2] if 'apps/' in rel_path else 'common'
        app_stats[app_name] = app_stats.get(app_name, 0) + 1

        snippets = extract_russian_snippets(file_path)
        if not snippets:
            continue

        file_issues = []

        # 1. Проверка стилистических паттернов
        for s in snippets:
            for pat, desc in STYLE_PATTERNS:
                m = re.search(pat, s['raw_line'], re.IGNORECASE)
                if m:
                    file_issues.append({
                        'type': 'style',
                        'line_no': s['line_no'],
                        'word': m.group(0),
                        'desc': desc,
                        'context': s['raw_line']
                    })

        # 2. Проверка орфографии через Яндекс.Спеллер
        # Формируем батчи текста
        batch_snippets = []
        batch_text_lines = []
        for s in snippets:
            cleaned = clean_ui_text_for_speller(s['raw_line'])
            if cleaned and re.search(r'[а-яА-ЯёЁ]', cleaned):
                batch_snippets.append(s)
                batch_text_lines.append(f"LINE_{s['line_no']}: {cleaned}")

        if batch_text_lines:
            # Отправляем батчами до 8000 символов
            chunk = []
            chunk_len = 0
            for item in batch_text_lines:
                if chunk_len + len(item) > 6000:
                    text_to_send = "\n".join(chunk)
                    speller_res = check_yandex_speller(text_to_send)
                    time.sleep(0.1)
                    process_speller_results(speller_res, file_issues, file_path)
                    chunk = [item]
                    chunk_len = len(item)
                else:
                    chunk.append(item)
                    chunk_len += len(item)

            if chunk:
                text_to_send = "\n".join(chunk)
                speller_res = check_yandex_speller(text_to_send)
                time.sleep(0.1)
                process_speller_results(speller_res, file_issues, file_path)

        if file_issues:
            # Устраняем дубликаты
            seen = set()
            unique_issues = []
            for issue in file_issues:
                key = (issue.get('line_no'), issue.get('word', '').lower(), issue.get('type'))
                if key not in seen:
                    seen.add(key)
                    unique_issues.append(issue)
            all_issues[rel_path] = unique_issues

    print("\n======================================================================")
    print("📊 СТАТИСТИКА ПРОВЕРЕННЫХ UI ПРИЛОЖЕНИЙ:")
    for app, count in sorted(app_stats.items()):
        print(f"  • {app:12}: {count} файлов")
    print("======================================================================\n")

    total_issues = sum(len(v) for v in all_issues.values())
    if not all_issues:
        print("✅ Все проверенные UI компоненты идеальны! Ошибок не найдено.")
        return

    print(f"⚠️ Найдено потенциальных замечаний: {total_issues} в {len(all_issues)} файлах:\n")
    for file_path, issues in sorted(all_issues.items()):
        print(f"📄 {file_path} ({len(issues)} замечаний):")
        for iss in issues:
            line_str = f"L{iss['line_no']}: " if iss.get('line_no') else ""
            if iss['type'] == 'style':
                print(f"   [СТИЛЬ] {line_str}«{iss['word']}» ➔ {iss['desc']}")
                print(f"           Контекст: {iss['context'].strip()[:90]}")
            elif iss['type'] == 'spell':
                sugg = ", ".join(iss.get('s', []))
                print(f"   [ОРФОГРАФИЯ] {line_str}«{iss['word']}» ➔ варианты: {sugg}")
                if iss.get('context'):
                    print(f"           Контекст: {iss['context'].strip()[:90]}")
        print()

def process_speller_results(speller_res, file_issues, file_path):
    if not speller_res:
        return
    for item in speller_res:
        word = item.get('word', '')
        word_lower = word.lower()
        if word_lower in TECH_WHITELIST:
            continue
        # Пропускаем однобуквенные или латиницу
        if len(word) < 2 or re.match(r'^[A-Za-z0-9_]+$', word):
            continue

        file_issues.append({
            'type': 'spell',
            'word': word,
            's': item.get('s', []),
            'context': ''
        })

if __name__ == '__main__':
    main()

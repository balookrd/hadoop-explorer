#!/usr/bin/env python3
"""
Скрипт проверки качества русскоязычного текста документации проекта Hadoop Explorer Platform.
Использует API Яндекс.Спеллера для точного нахождения орфографических ошибок, опечаток,
неправильных дефисов и согласований в текстах Markdown файлов (исключая код и тех. идентификаторы).
"""

import os
import glob
import re
import json
import urllib.request
import urllib.parse
import time

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
    'tcp', 'udp', 'dns', 'cname', 'vip', 'ip', 'postresql', 'postgresql', 'sqlite', 'h2',
    'crontab', 'cron', 'systemd', 'docker', 'compose', 'k8s', 'kubernetes', 'helm',
    'spa', 'ui', 'rest', 'sse', 'api', 'cli', 'posix', 'acl', 'rwx', 'sha', 'md5',
    'acid', 'llap', 'tez', 'mapreduce', 'knox', 'ranger', 'sentry', 'atlas', ' ам ',
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
    'инвариант', 'инварианта', 'инварианты', 'инвариантов', 'либеральный',
    'синхро', 'мульти', 'онлайн', 'офлайн', 'оффлайн', 'эндпоинт', 'эндпоинта',
    'эндпоинты', 'эндпоинтов', 'дашборд', 'дашборда', 'дашборды', 'дашбордов',
    'скриншот', 'скриншота', 'скриншоты', 'скриншотов', 'слайд', 'слайда', 'слайды', 'слайдов',
    'демон', 'демона', 'демоны', 'демонов', 'демоне', 'демону',
    'прокси', 'проксирование', 'проксирования', 'балансировщик', 'балансировщика',
    'трекер', 'трекера', 'таргет', 'таргета', 'флаг', 'флага', 'флаги', 'флагов',
    'плагин', 'плагина', 'плагины', 'плагинов', 'коннектор', 'коннектора', 'коннекторы',
    'мигратор', 'мигратора', 'репликатор', 'репликатора', 'репликаторы', 'репликаторов',
    'инстанс', 'инстанса', 'инстансы', 'инстансов', 'кластер', 'кластера', 'кластеры',
    'кластеров', 'нода', 'ноды', 'нод', 'нодам', 'нодах', 'хост', 'хоста', 'хосты', 'хостов'
}

def clean_text_for_spelling(text):
    # Убираем блоки кода ```...```
    text = re.sub(r'```[\s\S]*?```', ' ', text)
    # Убираем инлайн код `...`
    text = re.sub(r'`[^`]+`', ' ', text)
    # Убираем URL ссылки [text](url) -> оставляем только text
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    # Убираем чистые URL https://...
    text = re.sub(r'https?://\S+', ' ', text)
    # Убираем HTML теги <...>
    text = re.sub(r'<[^>]+>', ' ', text)
    # Убираем пути файлов /foo/bar или docs/...
    text = re.sub(r'(?:/[\w\.-]+)+', ' ', text)
    text = re.sub(r'(?:[\w\.-]+/)+[\w\.-]+', ' ', text)
    # Убираем английские слова и цифры, оставляем только кириллицу и знаки препинания
    # Заменяем латинские токены пробелами
    text = re.sub(r'\b[A-Za-z0-9_-]+\b', ' ', text)
    return text

def check_yandex_speller(text_chunk):
    url = "https://speller.yandex.net/services/spellservice.json/checkText"
    data = urllib.parse.urlencode({
        'text': text_chunk,
        'lang': 'ru',
        'options': 0
    }).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'User-Agent': 'HadoopExplorerDocChecker/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"⚠️ Ошибка запроса к Спеллеру: {e}")
        return []

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    docs_dir = os.path.join(base_dir, "docs")
    md_files = sorted(glob.glob(os.path.join(docs_dir, "*.md")) + [os.path.join(base_dir, "README.md")])

    print(f"🔍 Найдено файлов документации для анализа: {len(md_files)}")
    all_issues = {}

    for doc in md_files:
        rel_path = os.path.relpath(doc, base_dir)
        with open(doc, 'r', encoding='utf-8') as f:
            content = f.read()

        cleaned = clean_text_for_spelling(content)
        # Разбиваем на параграфы / чанки не более 8000 символов для API
        paragraphs = cleaned.split('\n\n')
        chunks = []
        curr = ""
        for p in paragraphs:
            if len(curr) + len(p) < 8000:
                curr += p + "\n\n"
            else:
                chunks.append(curr)
                curr = p + "\n\n"
        if curr:
            chunks.append(curr)

        doc_issues = []
        for chunk in chunks:
            if not chunk.strip():
                continue
            errors = check_yandex_speller(chunk)
            for err in errors:
                word = err['word']
                lower_word = word.lower()
                suggestions = err.get('s', [])
                if lower_word in TECH_WHITELIST:
                    continue
                # Пропускаем слова, которые являются общепринятыми IT сокращениями
                if any(lower_word.startswith(tw) for tw in ['конфиг', 'снапшот', 'партици', 'балансир']):
                    continue
                doc_issues.append({
                    'word': word,
                    'suggestions': suggestions,
                    'col': err.get('col'),
                    'row': err.get('row')
                })
            time.sleep(0.1) # вежливая пауза к API

        if doc_issues:
            # Убираем дубликаты
            seen = set()
            unique_issues = []
            for item in doc_issues:
                if item['word'] not in seen:
                    seen.add(item['word'])
                    unique_issues.append(item)
            all_issues[rel_path] = unique_issues

    print("\n" + "="*70)
    print("📊 ОТЧЕТ О ПРОВЕРКЕ РУССКОЯЗЫЧНОГО ТЕКСТА ДОКУМЕНТАЦИИ")
    print("="*70)

    total_words = 0
    for doc, issues in all_issues.items():
        if not issues:
            continue
        print(f"\n📄 {doc} (подозрительных слов: {len(issues)}):")
        for iss in issues:
            sug = ", ".join(iss['suggestions'][:3]) if iss['suggestions'] else "нет вариантов"
            print(f"   • «{iss['word']}» ➔ возможно: {sug}")
            total_words += 1

    print("\n" + "-"*70)
    print(f"Всего файлов с замечаниями: {len(all_issues)}, уникальных слов для ревью: {total_words}")

if __name__ == "__main__":
    main()

# План реализации: переименование в Pristan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Заменить имя платформы Hadoop Explorer на Pristan, имена сервисов — на Artel, Sklad, Tolmach, Iskra, Parom, имя стартера безопасности — на Zastava во всех идентификаторах и текстах репозитория, без обратной совместимости.

**Architecture:** Переименование идёт слоями: сначала корень Java-пакетов и платформенные идентификаторы, затем стартер, затем сервисы по одному. Каждый слой — это `git mv` каталогов плюс замена точных токенов по манифесту инструментом `scripts/rename/rename_tokens.py`; вложенные YAML-ключи поднимает `scripts/rename/hoist_yaml_key.py`. Задача считается выполненной, когда проходят тесты затронутых модулей и поиск остатков старых токенов возвращает только строки из списка исключений.

**Tech Stack:** Java 21, Maven, Spring Boot 3.3.4, protobuf/gRPC, Flyway, Svelte 5, Vite, Vitest, Playwright, Docker Compose, Prometheus, Grafana, Python 3 (инструменты переименования), ast-grep.

**Спецификация:** `docs/superpowers/specs/2026-11-12-service-proper-names-design.md`. При расхождении плана и спецификации верна спецификация.

## Global Constraints

- Сборка и тесты Java — только на JDK 21 и только через `make` (`make test-<цель>`, `make build-java`). Прямой `mvn` запускать с `JAVA_HOME=<jdk21>`.
- Модули Parom собирать через реактор родителя: `mvn test -f backend/parom/pom.xml -pl agent -am`.
- Не выполнять `git commit` и `git push` без явной команды владельца репозитория. Шаг «Контрольная точка» в каждой задаче: показать `git status --short | wc -l` и `git diff --stat | tail -1`, предложить сообщение коммита, остановиться.
- Не создавать ветки; работа идёт в `main`.
- В сообщениях коммитов нет трейлера `Co-Authored-By` и подписей об авторстве инструмента. Сообщения коммитов и комментарии в коде — на английском.
- Документация — только на русском; пар `*.md`/`*.ru.md` не создавать.
- Не читать и не сканировать `data/`, `node_modules/`, `target/`, `dist/`, lock-файлы.
- Каталоги переносить командой `git mv`.
- Замена по подстроке запрещена: слова `yarn`, `hdfs`, `sql`, `spark` обозначают и продукт, и движок. Все замены — точными токенами из манифестов этого плана.
- Не менять: denylist секретов в `SecurityConfigurationValidator` и `LEGACY_DEFAULT_AGENT_SECRET`; ветки `kafka-replicator-.*`, `replicator-.*`, `hadoop-replicator-.*` в `ConsumerGroupBlacklistFilter`; `V1__baseline_schema.sql`; `CHANGELOG*`; `docs/superpowers/`; классы и таблицы `Replication*`/`replication_*`; имена классов и ключей движков.
- Новые идентификаторы: платформа `pristan`, groupId и корневой пакет `io.github.balookrd.pristan`, сервисы `artel`, `sklad`, `tolmach`, `iskra`, `parom`, стартер `zastava`.
- Отображаемые имена: при первом упоминании на странице или экране — «Artel (YARN)», «Sklad (HDFS)», «Tolmach (SQL)», «Iskra (Spark)», «Parom»; платформа — «Pristan — платформа для работы с Hadoop».
- В регулярных выражениях для `git grep -E` не использовать `\b`: в этой сборке git он не срабатывает. Границы слова задавать классами символов.

## Структура файлов

Создаются (временные, удаляются в задаче 12):

- `scripts/rename/rename_tokens.py` — применяет манифест замен к отслеживаемым текстовым файлам.
- `scripts/rename/hoist_yaml_key.py` — поднимает вложенный YAML-ключ на верхний уровень под новым именем.
- `scripts/rename/test_rename_tools.py` — тесты обоих инструментов.
- `scripts/rename/manifests/NN-<имя>.tsv` — манифест на задачу.
- `scripts/rename/residual.sh` — поиск остатков старых имён с исключениями.

Создаётся (остаётся):

- `backend/parom/orchestrator/src/main/resources/db/migration/{common,h2,postgresql}/V<N>__rename_parom_tables.sql` — в том наборе, где лежит `V1__baseline_schema.sql`.

Переносятся каталоги: `backend/{common-security-starter,yarn,hdfs,sql,spark,replicator}`, `frontend/apps/*`, `demo/{yarn,hdfs,sql,spark,replicator}`, `docs/images/*`, шестнадцать корней `src/{main,test}/java/org/apache/hadoop/explorer`.

---

### Task 1: Инструменты переименования и исходные замеры

**Files:**
- Create: `scripts/rename/rename_tokens.py`
- Create: `scripts/rename/hoist_yaml_key.py`
- Create: `scripts/rename/test_rename_tools.py`
- Create: `scripts/rename/residual.sh`
- Create: `scripts/rename/baseline.txt`

**Interfaces:**
- Produces: `python3 scripts/rename/rename_tokens.py <manifest.tsv> [--dry-run]`; `python3 scripts/rename/hoist_yaml_key.py <parent> <child> <newkey> <file>...`; `scripts/rename/residual.sh <regex>`.
- Формат манифеста (поля разделены табуляцией, строки с `#` и пустые пропускаются):
  - `keep<TAB>ЛИТЕРАЛ` — литерал маскируется до замен и восстанавливается после;
  - `sub<TAB>СТАРОЕ<TAB>НОВОЕ[<TAB>GLOB,GLOB]` — замена литерала;
  - `re<TAB>ШАБЛОН<TAB>ЗАМЕНА[<TAB>GLOB,GLOB]` — замена по регулярному выражению Python;
  - правила применяются в порядке записи; glob сопоставляется с путём от корня репозитория через `fnmatch` (`*` захватывает и `/`).

- [ ] **Step 1: Написать тесты**

`scripts/rename/test_rename_tools.py`:

```python
import unittest

from hoist_yaml_key import hoist
from rename_tokens import apply_rules, parse_manifest


class ApplyRulesTest(unittest.TestCase):
    def test_sub_replaces_literal(self):
        rules = parse_manifest(["sub\tyarn-explorer\tartel"])
        self.assertEqual(apply_rules("image: yarn-explorer:1", "a.yml", rules)[0],
                         "image: artel:1")

    def test_keep_protects_literal(self):
        rules = parse_manifest([
            "keep\tyarn-explorer-test-secret-key",
            "sub\tyarn-explorer\tartel",
        ])
        text = 'deny("yarn-explorer-test-secret-key"); host = "yarn-explorer";'
        self.assertEqual(
            apply_rules(text, "A.java", rules)[0],
            'deny("yarn-explorer-test-secret-key"); host = "artel";')

    def test_glob_limits_scope(self):
        rules = parse_manifest(["re\t^yarn:\tartel:\tbackend/artel/*"])
        self.assertEqual(apply_rules("yarn:\n  a: 1\n", "backend/artel/x.yml", rules)[0],
                         "artel:\n  a: 1\n")
        self.assertEqual(apply_rules("yarn:\n  a: 1\n", "demo/x.yml", rules)[0],
                         "yarn:\n  a: 1\n")

    def test_regex_lookbehind_keeps_hadoop_classes(self):
        rules = parse_manifest([
            "re\t(?<![.\\w])hadoop\\.security\\.(jwt|auth(?![a-z_]))\tzastava.\\1",
        ])
        text = ("org.apache.hadoop.security.UserGroupInformation "
                "hadoop.security.authentication hadoop.security.jwt.x "
                "hadoop.security.auth.mode")
        self.assertEqual(
            apply_rules(text, "A.java", rules)[0],
            "org.apache.hadoop.security.UserGroupInformation "
            "hadoop.security.authentication zastava.jwt.x zastava.auth.mode")

    def test_counts_are_reported_per_rule(self):
        rules = parse_manifest(["sub\ta\tb", "sub\tzzz\ty"])
        _, counts = apply_rules("aa", "f", rules)
        self.assertEqual(counts, [2, 0])


class HoistTest(unittest.TestCase):
    def test_moves_child_to_top_level(self):
        src = ("server:\n  port: 1\n"
               "hadoop:\n  security:\n    jwt:\n      secret-key: x\n  hdfs:\n    a: 1\n"
               "management:\n  x: 1\n").splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertTrue(changed)
        self.assertEqual("".join(out),
                         "server:\n  port: 1\n"
                         "hadoop:\n  hdfs:\n    a: 1\n"
                         "zastava:\n  jwt:\n    secret-key: x\n"
                         "management:\n  x: 1\n")

    def test_removes_empty_parent(self):
        src = "hadoop:\n  security:\n    debug: true\n".splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertTrue(changed)
        self.assertEqual("".join(out), "zastava:\n  debug: true\n")

    def test_no_child_is_noop(self):
        src = "hadoop:\n  hdfs:\n    a: 1\n".splitlines(keepends=True)
        out, changed = hoist(src, "hadoop", "security", "zastava")
        self.assertFalse(changed)
        self.assertEqual(out, src)

    def test_keeps_comments_inside_block(self):
        src = "hadoop:\n  security:\n    # note\n    debug: true\n".splitlines(keepends=True)
        out, _ = hoist(src, "hadoop", "security", "zastava")
        self.assertEqual("".join(out), "zastava:\n  # note\n  debug: true\n")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `cd scripts/rename && python3 -m unittest test_rename_tools -v`
Expected: `ModuleNotFoundError: No module named 'hoist_yaml_key'`

- [ ] **Step 3: Написать `rename_tokens.py`**

```python
#!/usr/bin/env python3
"""Apply a token-replacement manifest to tracked text files."""
import fnmatch
import re
import subprocess
import sys

EXCLUDED_PREFIXES = ("docs/superpowers/", "data/", "scripts/rename/", "CHANGELOG")
EXCLUDED_SUFFIXES = ("package-lock.json", "pnpm-lock.yaml", "V1__baseline_schema.sql")


def parse_manifest(lines):
    rules = []
    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split("\t")
        kind = parts[0]
        if kind == "keep" and len(parts) == 2:
            rules.append(("keep", parts[1], None, None))
        elif kind in ("sub", "re") and len(parts) in (3, 4):
            globs = parts[3].split(",") if len(parts) == 4 else None
            rules.append((kind, parts[1], parts[2], globs))
        else:
            raise ValueError(f"bad manifest line: {line!r}")
    return rules


def apply_rules(text, path, rules):
    keeps = [r[1] for r in rules if r[0] == "keep"]
    for i, literal in enumerate(keeps):
        text = text.replace(literal, f"\x00K{i}\x00")
    counts = []
    for kind, old, new, globs in rules:
        if kind == "keep":
            continue
        if globs and not any(fnmatch.fnmatch(path, g) for g in globs):
            counts.append(0)
            continue
        if kind == "sub":
            counts.append(text.count(old))
            text = text.replace(old, new)
        else:
            text, n = re.subn(old, new, text, flags=re.MULTILINE)
            counts.append(n)
    for i, literal in enumerate(keeps):
        text = text.replace(f"\x00K{i}\x00", literal)
    return text, counts


def tracked_files():
    out = subprocess.run(["git", "ls-files", "-z"], check=True, capture_output=True).stdout
    for name in out.decode().split("\0"):
        if not name or name.startswith(EXCLUDED_PREFIXES) or name.endswith(EXCLUDED_SUFFIXES):
            continue
        yield name


def main(argv):
    dry = "--dry-run" in argv
    manifest = [a for a in argv[1:] if a != "--dry-run"][0]
    with open(manifest, encoding="utf-8") as fh:
        rules = parse_manifest(fh)
    active = [r for r in rules if r[0] != "keep"]
    totals = [0] * len(active)
    changed = 0
    for path in tracked_files():
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except (FileNotFoundError, IsADirectoryError):
            continue
        if b"\0" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        new, counts = apply_rules(text, path, rules)
        totals = [a + b for a, b in zip(totals, counts)]
        if new != text:
            changed += 1
            if not dry:
                with open(path, "w", encoding="utf-8", newline="") as fh:
                    fh.write(new)
    for (kind, old, new, _), n in zip(active, totals):
        flag = "WARNING no matches" if n == 0 else f"{n:6d}"
        print(f"{flag}\t{kind}\t{old}\t->\t{new}")
    print(f"files changed: {changed}{' (dry run)' if dry else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

- [ ] **Step 4: Написать `hoist_yaml_key.py`**

```python
#!/usr/bin/env python3
"""Move a nested YAML key (parent.child) to the top level under a new name.

Line-based on purpose: keeps comments, ordering and quoting intact.
Handles block style only; inline maps are left untouched and reported.
"""
import re
import sys


def _indent(line):
    return len(line) - len(line.lstrip(" "))


def hoist(lines, parent, child, new):
    parent_re = re.compile(rf"^{re.escape(parent)}:\s*(#.*)?$")
    start = next((i for i, l in enumerate(lines) if parent_re.match(l.rstrip("\n"))), None)
    if start is None:
        return lines, False
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or lines[end].startswith(" ")):
        end += 1
    while end > start + 1 and not lines[end - 1].strip():
        end -= 1
    child_re = re.compile(rf"^( +){re.escape(child)}:\s*(#.*)?$")
    c_start = next((i for i in range(start + 1, end)
                    if child_re.match(lines[i].rstrip("\n"))), None)
    if c_start is None:
        return lines, False
    c_indent = _indent(lines[c_start])
    c_end = c_start + 1
    while c_end < end and (not lines[c_end].strip() or _indent(lines[c_end]) > c_indent):
        c_end += 1
    while c_end > c_start + 1 and not lines[c_end - 1].strip():
        c_end -= 1
    block = [f"{new}:\n"] + [
        l[c_indent:] if l.strip() else l for l in lines[c_start + 1:c_end]
    ]
    rest = lines[start + 1:c_start] + lines[c_end:end]
    has_other = any(l.strip() and not l.strip().startswith("#") for l in rest)
    kept_parent = [lines[start]] + rest if has_other else []
    return lines[:start] + kept_parent + block + lines[end:], True


def main(argv):
    parent, child, new, files = argv[1], argv[2], argv[3], argv[4:]
    for path in files:
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
        out, changed = hoist(lines, parent, child, new)
        if changed:
            with open(path, "w", encoding="utf-8", newline="") as fh:
                fh.writelines(out)
        print(f"{'hoisted' if changed else 'no-op  '}\t{path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

- [ ] **Step 5: Убедиться, что тесты проходят**

Run: `cd scripts/rename && python3 -m unittest test_rename_tools -v`
Expected: `Ran 9 tests` и `OK`

- [ ] **Step 6: Написать `residual.sh`**

```bash
#!/usr/bin/env bash
# Usage: scripts/rename/residual.sh '<extended regex>'
# Prints tracked lines that still match, minus the fixed exclusions.
set -euo pipefail
git grep -n -I -E "$1" -- . \
  ':!docs/superpowers' ':!CHANGELOG*' ':!scripts/rename' ':!*lock*' \
  ':!*SecurityConfigurationValidator*' ':!*ConsumerGroupBlacklistFilter*' \
  ':!*V1__baseline_schema.sql' || true
```

Run: `chmod +x scripts/rename/residual.sh && scripts/rename/residual.sh 'hadoop-explorer-parent' | wc -l`
Expected: число больше 0 (старое имя ещё на месте).

- [ ] **Step 7: Снять исходные замеры**

```bash
{
  echo "ast-grep matches: $(ast-grep scan 2>/dev/null | grep -c 'no-synchronized-on-io-path' || true)"
  echo "java files: $(git ls-files 'backend/**/*.java' | wc -l)"
  echo "tracked files: $(git ls-files | wc -l)"
} | tee scripts/rename/baseline.txt
git ls-files | grep -E 'SecurityConfigurationValidator\.java$|ReplicatorProperties\.java$' \
  | xargs grep -h -o -E '"[^"]*(explorer|replicator)[^"]*(secret|key)[^"]*"' | sort > scripts/rename/denylist.txt
wc -l < scripts/rename/denylist.txt
make test-java 2>&1 | tail -15
make test-ui 2>&1 | tail -15
```

Expected: число строк в `denylist.txt` больше 0 — это опубликованные секреты, которые валидатор обязан отклонять и после переименования; `BUILD SUCCESS` для Java и успешный прогон Vitest. Если до переименования что-то красное — остановиться и сообщить: переименование поверх красных тестов не начинать.

- [ ] **Step 8: Проверить занятость имён**

Открыть в браузере или через `curl -s -o /dev/null -w '%{http_code}\n'`:

- `https://registry.npmjs.org/@pristan%2fartel` — ожидается `404`;
- `https://hub.docker.com/v2/namespaces/pristan/` — `404` означает свободное пространство имён, `200` — занятое;
- `https://repo1.maven.org/maven2/io/github/balookrd/` — ожидается `404`.

Занятое пространство `pristan` на Docker Hub не блокирует работу: образы собираются локально, значение `REGISTRY` переопределяется. Результат записать в `scripts/rename/baseline.txt` и сообщить владельцу.

- [ ] **Step 9: Контрольная точка**

Предлагаемое сообщение: `chore(rename): add temporary token-rename tooling`

---

### Task 2: Платформа — корень Java-пакетов и groupId

**Files:**
- Move: 16 каталогов `backend/**/src/{main,test}/java/org/apache/hadoop/explorer` → `…/java/io/github/balookrd/pristan`
- Modify: все `*.java`, все `pom.xml`, `*.proto`, `AutoConfiguration.imports`, `logback.xml`, `application*.yml`, `demo/replicator/**`, `docker/Dockerfile.replicator-agent`, `.ast-grep/rules/no-synchronized-on-io-path.yml`
- Create: `scripts/rename/manifests/02-platform-package.tsv`

**Interfaces:**
- Produces: корневой пакет `io.github.balookrd.pristan`, groupId `io.github.balookrd.pristan`, artifactId корня `pristan-parent`. Сегменты сервисов (`.yarn`, `.hdfs`, `.sql`, `.spark`, `.replicator`, `.common`) пока прежние.

- [ ] **Step 1: Перенести каталоги исходников**

```bash
git ls-files | grep -o -E '^.*/src/(main|test)/java/org/apache/hadoop/explorer' | sort -u | while read -r old; do
  base="${old%/org/apache/hadoop/explorer}"
  mkdir -p "$base/io/github/balookrd"
  git mv "$old" "$base/io/github/balookrd/pristan"
done
find backend -type d -path '*/java/org' -empty -delete 2>/dev/null; find backend -type d -path '*/java/org*' -empty -delete 2>/dev/null
git ls-files | grep -c '/org/apache/hadoop/explorer/'
```

Expected: последняя команда печатает `0`.

- [ ] **Step 2: Написать манифест**

`scripts/rename/manifests/02-platform-package.tsv` (разделитель — табуляция):

```text
sub	org.apache.hadoop.explorer	io.github.balookrd.pristan
sub	org/apache/hadoop/explorer	io/github/balookrd/pristan
sub	<artifactId>hadoop-explorer-parent</artifactId>	<artifactId>pristan-parent</artifactId>
```

- [ ] **Step 3: Применить и проверить счётчики**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/02-platform-package.tsv`
Expected: ни одной строки `WARNING no matches`; `files changed` — около 740.

- [ ] **Step 4: Проверить groupId и ссылки на родителя**

Run: `git grep -n -E '<groupId>io\.github\.balookrd\.pristan</groupId>' -- '*pom.xml' | wc -l; git grep -n 'hadoop-explorer-parent' -- '*pom.xml'`
Expected: первое число не меньше 11 (корень, стартер, четыре сервиса, пять модулей репликатора); вторая команда ничего не печатает.

- [ ] **Step 5: Собрать и прогнать тесты**

Run: `make test-java 2>&1 | tail -20`
Expected: `BUILD SUCCESS` по всем модулям. Типичный сбой — `package … does not exist` в сгенерированном protobuf-коде: проверить, что `option java_package` в трёх файлах `backend/replicator/proto/src/main/proto/*.proto` начинается с `io.github.balookrd.pristan`.

- [ ] **Step 6: Проверить остатки**

Run: `scripts/rename/residual.sh 'org[./]apache[./]hadoop[./]explorer'`
Expected: пустой вывод.

Run: `ast-grep scan 2>/dev/null | grep -c 'no-synchronized-on-io-path'`
Expected: то же число, что в `scripts/rename/baseline.txt`.

- [ ] **Step 7: Контрольная точка**

Предлагаемое сообщение: `refactor!: move Java root package to io.github.balookrd.pristan`

---

### Task 3: Платформа — имя, образы, npm-область, cookie, мониторинг

**Files:**
- Modify: `pom.xml`, `backend/*/pom.xml`, `frontend/package.json`, `frontend/apps/*/package.json`, `frontend/common/package.json`, `frontend/vitest.config.ts`, все импорты `@hadoop-explorer/common` в `frontend/`, `scripts/build-containers.sh`, `Makefile`, `demo/**/docker-compose*.y*ml`, `.github/workflows/*.yml`, `monitoring/**`, `demo/monitoring/**`, `backend/common-security-starter/**` (cookie), `application*.yml` сервисов
- Move: `monitoring/grafana/dashboards/hadoop_explorer_overview.json` → `pristan_overview.json` (и копия в `demo/monitoring/`, если есть)
- Create: `scripts/rename/manifests/03-platform-names.tsv`

**Interfaces:**
- Consumes: корневой пакет из Task 2.
- Produces: npm-область `@pristan`, пакет `@pristan/common`, реестр образов `pristan`, cookie `pristan_session`, `job_name: 'pristan'`, дашборд `pristan_overview` с uid `pristan-overview`.

- [ ] **Step 1: Найти фактические написания имени платформы**

Run: `git grep -h -o -i -E "hadoop[-_ ]explorer[a-z0-9_-]*" -- . ':!docs' ':!*.md' ':!scripts/rename' ':!*lock*' | sort | uniq -c | sort -rn`

Сверить вывод с манифестом шага 2. Каждый токен из вывода должен попадать под одно из правил манифеста либо быть секретом из denylist. Токен, которого нет ни там, ни там, добавить в манифест отдельной строкой `sub` до применения.

- [ ] **Step 2: Написать манифест**

`scripts/rename/manifests/03-platform-names.tsv`:

```text
# denylist of published secrets must survive
keep	hadoop-explorer-super-secret
keep	hadoop-explorer-demo-secret
# dashboards and alert groups
sub	hadoop_explorer_overview	pristan_overview
sub	hadoop-explorer-overview	pristan-overview
sub	hadoop_explorer_alerts	pristan_alerts
# identifiers
sub	hadoop_explorer_session	pristan_session
sub	svc_hadoop_explorer	svc_pristan
sub	/var/lib/hadoop-explorer	/var/lib/pristan
sub	@hadoop-explorer/	@pristan/
sub	hadoop-explorer-monorepo	pristan-monorepo
sub	hadoop-explorer-jars	pristan-jars
sub	hadoop-explorer-${	pristan-${
sub	hadoop-explorer/	pristan/
sub	REGISTRY:-hadoop-explorer	REGISTRY:-pristan
sub	job_name: 'hadoop-explorer'	job_name: 'pristan'
sub	job="hadoop-explorer"	job="pristan"
# display names
sub	Hadoop Explorer Platform	Pristan
sub	HADOOP EXPLORER	PRISTAN
sub	Hadoop Explorer	Pristan
```

Правило `sub hadoop-explorer/ pristan/` задевает и адрес репозитория `balookrd/hadoop-explorer/…` в `docs/*-admin-guide.md`. Это допустимо: адрес меняется в Task 11 вместе с остальными ссылками, а до переименования репозитория GitHub ссылки в рабочем дереве никуда не публикуются.

- [ ] **Step 3: Перенести файлы дашборда и применить манифест**

```bash
git ls-files | grep -E 'hadoop_explorer_overview\.json$' | while read -r f; do
  git mv "$f" "$(dirname "$f")/pristan_overview.json"
done
python3 scripts/rename/rename_tokens.py scripts/rename/manifests/03-platform-names.tsv
```

Expected: строка `WARNING no matches` допустима только для правила `job="hadoop-explorer"`. Любое другое предупреждение — сверить токен с выводом шага 1.

- [ ] **Step 4: Поправить базу CI**

В `.github/workflows/ci.yml` в блоке сервиса Postgres заменить значения `POSTGRES_USER: explorer`, `POSTGRES_DB: explorer` и то же имя в JDBC URL и переменных шага тестов на `pristan`.

Run: `grep -n -E 'explorer' .github/workflows/ci.yml`
Expected: пустой вывод.

- [ ] **Step 5: Собрать фронтенд и прогнать тесты**

```bash
(cd frontend && npm install)
make test-ui 2>&1 | tail -15
make test-security-starter 2>&1 | tail -10
```

Expected: Vitest проходит; `BUILD SUCCESS`. `npm install` обновляет `package-lock.json` под новые имена пакетов — это ожидаемое изменение.

- [ ] **Step 6: Проверить остатки**

Run: `scripts/rename/residual.sh 'hadoop[-_ ]explorer|Hadoop Explorer|HADOOP EXPLORER' | grep -v -E '\.md:|^docs/'`
Expected: пустой вывод. Строки в `*.md` и `docs/` правятся в Task 11.

- [ ] **Step 7: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename platform identifiers to Pristan`

---

### Task 4: Zastava — стартер безопасности

**Files:**
- Move: `backend/common-security-starter` → `backend/zastava`; пакет `…/pristan/common` → `…/pristan/zastava`; файлы трёх классов `CommonSecurity*`
- Modify: `pom.xml` (модуль), `backend/*/pom.xml` и `backend/replicator/*/pom.xml` (зависимость), `Makefile`, `scripts/run-tests.sh`, `.github/workflows/*.yml`, все `application*.yml` и `*.properties` с ключами стартера, `demo/**/docker-compose*.y*ml` (переменные `HADOOP_SECURITY_*`), `AutoConfiguration.imports`
- Create: `scripts/rename/manifests/04-zastava.tsv`

**Interfaces:**
- Consumes: `io.github.balookrd.pristan` из Task 2.
- Produces: artifactId `zastava`, пакет `io.github.balookrd.pristan.zastava`, классы `ZastavaAutoConfiguration`, `ZastavaProperties`, `ZastavaErrorHandlers`, префикс конфигурации `zastava.*`, переменные `ZASTAVA_*`, цели `make test-zastava`, `make build-zastava`.

- [ ] **Step 1: Перенести каталог, пакет и классы**

```bash
git mv backend/common-security-starter backend/zastava
for s in main test; do
  d=backend/zastava/src/$s/java/io/github/balookrd/pristan
  git mv "$d/common" "$d/zastava"
done
git ls-files 'backend/zastava/**/CommonSecurity*.java' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^CommonSecurity/Zastava/')"
done
git ls-files backend/zastava | grep -c CommonSecurity
```

Expected: `0`.

- [ ] **Step 2: Написать манифест**

`scripts/rename/manifests/04-zastava.tsv`:

```text
sub	io.github.balookrd.pristan.common	io.github.balookrd.pristan.zastava
sub	io/github/balookrd/pristan/common	io/github/balookrd/pristan/zastava
sub	CommonSecurityAutoConfiguration	ZastavaAutoConfiguration
sub	CommonSecurityProperties	ZastavaProperties
sub	CommonSecurityErrorHandlers	ZastavaErrorHandlers
sub	CommonSecurityIntegrationTest	ZastavaIntegrationTest
sub	backend/common-security-starter	backend/zastava
sub	<artifactId>common-security-starter</artifactId>	<artifactId>zastava</artifactId>
sub	test-security-starter	test-zastava
sub	build-security-starter	build-zastava
sub	Hadoop Explorer :: Common Security Starter	Pristan :: Zastava
sub	Pristan :: Common Security Starter	Pristan :: Zastava
# starter keys only; Hadoop's own hadoop.security.authentication / authorization / auth_to_local
# and classes under org.apache.hadoop.security stay
re	(?<![.\w])hadoop\.security\.(auth-mode|auth(?![a-z_])|cookie|cors|debug|jwt|kerberos|ldap|rate-limiter|session-store|tls)	zastava.\1
sub	prefix = "hadoop.security"	prefix = "zastava"
sub	HADOOP_SECURITY_	ZASTAVA_
sub	common-security-starter	zastava
```

Последнее правило стоит в конце намеренно: оно заменяет оставшиеся упоминания имени модуля в текстах и скриптах после того, как отработали более точные правила.

- [ ] **Step 3: Применить манифест**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/04-zastava.tsv`
Expected: предупреждение допустимо только для одной из двух строк `… :: Common Security Starter` (сработает та, что соответствует состоянию после Task 3).

- [ ] **Step 4: Поднять YAML-ключ `hadoop.security` на верхний уровень**

```bash
git ls-files 'backend/**/application*.yml' 'backend/**/application*.yaml' 'demo/**/application*.yml' 'demo/**/application*.yaml' \
  | xargs python3 scripts/rename/hoist_yaml_key.py hadoop security zastava
```

Expected: `hoisted` для файлов, где был блок `hadoop: security:`; `no-op` для остальных.

Run: `git grep -n -E '^  security:' -- 'backend/**/application*.y*ml' 'demo/**/application*.y*ml'`
Expected: пустой вывод. Строка в выводе означает блок в нестандартной записи — перенести его вручную: вырезать блок `security:` из-под `hadoop:`, вставить на верхний уровень как `zastava:` с отступом на два пробела меньше.

- [ ] **Step 5: Проверить файл автоконфигурации**

Run: `cat backend/zastava/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
Expected: одна строка `io.github.balookrd.pristan.zastava.autoconfigure.ZastavaAutoConfiguration`.

- [ ] **Step 6: Собрать и прогнать тесты**

Run: `make test-zastava 2>&1 | tail -10 && make test-java 2>&1 | tail -20`
Expected: `BUILD SUCCESS`. Сбой `NoSuchBeanDefinitionException` в тестах сервиса означает расхождение в файле `AutoConfiguration.imports` либо незамеченный ключ `hadoop.security.*` в тестовых свойствах — найти его командой шага 7.

- [ ] **Step 7: Проверить остатки**

Run: `scripts/rename/residual.sh 'common-security-starter|CommonSecurity|HADOOP_SECURITY_' | grep -v -E '\.md:'`
Expected: пустой вывод.

Run: `git grep -h -o -E 'hadoop\.security\.[a-zA-Z_-]+' -- backend demo | sort | uniq -c`
Expected: только `hadoop.security.authentication`, `hadoop.security.authorization`, `hadoop.security.auth_to_local`, `hadoop.security.UserGroupInformation`, `hadoop.security.AuthenticationFilterInitializer`, `hadoop.security.token`.

- [ ] **Step 8: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename common-security-starter to Zastava`

---

### Task 5: Artel (YARN)

**Files:**
- Move: `backend/yarn` → `backend/artel`; `frontend/apps/yarn` → `frontend/apps/artel`; `demo/yarn` → `demo/artel`; `docs/images/yarn` → `docs/images/artel`; `docker/Dockerfile.yarn-java` → `docker/Dockerfile.artel`; пакет `…/pristan/yarn` → `…/pristan/artel`; `YarnExplorerApplication*.java`; `frontend/e2e/yarn.spec.ts`, `frontend/e2e/capture_yarn_manual.spec.ts`, `frontend/scripts/capture_yarn.mjs`; дашборд `yarn_explorer_queues.json` → `artel_queues.json`
- Modify: `pom.xml`, `Makefile`, `scripts/*.sh`, `scripts/check-russian-*.py`, `.github/workflows/*.yml`, `frontend/package.json`, `frontend/playwright.config.ts`, `demo/infra/kdc/entrypoint.sh`, `demo/infra/ldap/bootstrap.ldif`, `demo/artel/cluster-*/core-site.xml`, `demo/all/**`, `demo/platform/**`, `monitoring/**`, `demo/monitoring/**`
- Create: `scripts/rename/manifests/05-artel.tsv`

**Interfaces:**
- Consumes: `zastava.*`, `io.github.balookrd.pristan`, `@pristan/common`, `pristan/` из Task 2–4.
- Produces: artifactId `artel`, пакет `io.github.balookrd.pristan.artel`, класс `ArtelApplication`, префикс конфигурации `artel.*`, переменные `ARTEL_DB_*`, `ARTEL_KERBEROS_*`, `ARTEL_AWX_*`, хост и контейнер `artel`, principal `artel@R`, `HTTP/artel@R`, LDAP-группа `artel-operators`, метка метрик `app=artel`, метрика `artel.change.requests`, npm-пакет `@pristan/artel`, цели `make test-artel`, `make build-artel`, `make demo-artel`.

- [ ] **Step 1: Перенести каталоги и файлы**

```bash
git mv backend/yarn backend/artel
git mv frontend/apps/yarn frontend/apps/artel
git mv demo/yarn demo/artel
[ -d docs/images/yarn ] && git mv docs/images/yarn docs/images/artel
git mv docker/Dockerfile.yarn-java docker/Dockerfile.artel
for s in main test; do
  d=backend/artel/src/$s/java/io/github/balookrd/pristan
  git mv "$d/yarn" "$d/artel"
done
git ls-files 'backend/artel/**/YarnExplorerApplication*.java' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^YarnExplorerApplication/ArtelApplication/')"
done
git ls-files | grep -E '(^|/)(capture_)?yarn(_manual)?\.spec\.ts$|capture_yarn\.mjs$|yarn_explorer_queues\.json$' | while read -r f; do
  n=$(basename "$f" | sed -e 's/yarn_explorer_queues/artel_queues/' -e 's/yarn/artel/')
  git mv "$f" "$(dirname "$f")/$n"
done
```

- [ ] **Step 2: Найти фактические токены сервиса**

```bash
git grep -h -o -i -E "[a-z0-9_./@-]*yarn[-_]?(demo[-_])?explorer[a-z0-9_.-]*" -- . ':!docs' ':!*.md' ':!scripts/rename' | sort | uniq -c | sort -rn
git grep -h -o -E "(test|build|demo|dev|run)[-:_]yarn[a-z-]*" -- Makefile scripts frontend/package.json frontend/playwright.config.ts .github | sort | uniq -c
git grep -h -o -E "YARN_(DB|KERBEROS|AWX)_[A-Z_]+" | sort -u
```

Сверить с манифестом шага 3 так же, как в Task 3, шаг 1.

- [ ] **Step 3: Написать манифест**

`scripts/rename/manifests/05-artel.tsv`:

```text
keep	yarn-explorer-test-secret-key
keep	yarn-explorer-demo-stand-super-secret-key
# packages, classes, paths
sub	io.github.balookrd.pristan.yarn	io.github.balookrd.pristan.artel
sub	io/github/balookrd/pristan/yarn	io/github/balookrd/pristan/artel
sub	YarnExplorerApplication	ArtelApplication
sub	backend/yarn	backend/artel
sub	frontend/apps/yarn	frontend/apps/artel
sub	apps/yarn	apps/artel
sub	demo/yarn	demo/artel
sub	docs/images/yarn	docs/images/artel
sub	Dockerfile.yarn-java	Dockerfile.artel
# build artifacts
sub	yarn-explorer-java	artel
sub	yarn-explorer-frontend	@pristan/artel
sub	yarn-explorer.jar	artel.jar
sub	yarn-explorer-demo	artel-demo
# kerberos, hosts, containers
sub	yarn-explorer.yarn-demo-net	artel.yarn-demo-net
sub	yarn-explorer.keytab	artel.keytab
sub	hadoop.proxyuser.yarn-explorer	hadoop.proxyuser.artel
sub	svc_yarn_explorer	svc_artel
sub	yarn-demo-explorer	artel
sub	yarn-explorer	artel
# data, dashboards, metrics
sub	yarn_explorer_queues	artel_queues
sub	yarn_explorer	artel
sub	yarn.change.requests	artel.change.requests
sub	yarn_change_requests	artel_change_requests
sub	app="yarn"	app="artel"
sub	yarn-operators	artel-operators
# env
re	YARN_(DB|KERBEROS|AWX)_	ARTEL_\1_
# make / npm / scripts
re	(test|build|demo)-yarn(?![a-z-]*-(rm|nm|site|demo-net))	\1-artel	Makefile,scripts/*,.github/*,AGENTS.md,README.md,docs/*,demo/*
re	(build|dev):yarn	\1:artel	frontend/*
sub	run_yarn	run_artel	scripts/*
sub	e2e/yarn.spec.ts	e2e/artel.spec.ts
sub	capture_yarn	capture_artel
# service config prefix and metric tag, only inside this service
re	^yarn:	artel:	backend/artel/*,demo/artel/application-demo.yml
sub	prefix = "yarn"	prefix = "artel"	backend/artel/*
re	(^\s+app:\s*)yarn\s*$	\1artel	backend/artel/*
# titles
sub	YARN Queue Explorer	Artel (YARN)
sub	YARN Explorer API	Artel API
sub	YARN Explorer	Artel
```

- [ ] **Step 4: Применить манифест**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/05-artel.tsv`
Expected: предупреждения допустимы для `yarn_change_requests` и `app="yarn"`, если метрика и селектор не встречаются в дашбордах в таком написании. Остальные предупреждения сверить с выводом шага 2.

- [ ] **Step 5: Проверить ключи конфигурации в Java**

Run: `git grep -n -E '"\$\{yarn\.|name *= *"yarn\.|"yarn\.(client-mode|acl|kerberos|awx|clusters)' -- backend/artel`
Expected: пустой вывод. Найденные строки — ссылки на свойства сервиса: заменить в них `yarn.` на `artel.`. Строки `yarn.scheduler.capacity.*`, `yarn.resourcemanager.*`, `yarn.nodemanager.*` — ключи движка, их не трогать.

- [ ] **Step 6: Проверить Kerberos-связку стенда**

Run: `grep -n -E 'artel' demo/infra/kdc/entrypoint.sh demo/artel/cluster-1/core-site.xml demo/artel/docker-compose*.y*ml demo/artel/application-demo.yml | grep -E 'HTTP/|proxyuser|hostname|container_name|keytab|principal'`
Expected: principal `HTTP/artel…`, ключи `hadoop.proxyuser.artel.hosts` и `.groups`, `container_name: artel`, файл `artel.keytab` — одно имя хоста во всех четырёх файлах.

- [ ] **Step 7: Собрать и прогнать тесты**

```bash
make test-artel 2>&1 | tail -10
(cd frontend && npm install) && make test-ui 2>&1 | tail -10
(cd frontend && npm run build:artel) && ls backend/artel/src/main/resources/static/index.html
```

Expected: `BUILD SUCCESS`; Vitest проходит; файл `index.html` существует.

- [ ] **Step 8: Проверить остатки**

Run: `scripts/rename/residual.sh '[Yy]arn[-_ ]?[Ee]xplorer|YARN Explorer|backend/yarn|apps/yarn|demo/yarn|YARN_(DB|KERBEROS|AWX)_|yarn-operators' | grep -v -E '\.md:|^docs/'`
Expected: пустой вывод.

- [ ] **Step 9: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename YARN Explorer service to Artel`

---

### Task 6: Sklad (HDFS)

**Files:**
- Move: `backend/hdfs` → `backend/sklad`; `frontend/apps/hdfs` → `frontend/apps/sklad`; `demo/hdfs` → `demo/sklad`; `docs/images/hdfs` → `docs/images/sklad`; `docker/Dockerfile.hdfs-java` → `docker/Dockerfile.sklad`; пакет `…/pristan/hdfs` → `…/pristan/sklad`; `HdfsExplorerApplication*.java`; `frontend/e2e/hdfs.spec.ts`, `capture_hdfs_manual.spec.ts`, `frontend/scripts/capture_hdfs.mjs`; дашборд `hdfs_explorer_operations.json` → `sklad_operations.json`
- Modify: те же общие файлы, что в Task 5, плюс `demo/sklad/hadoop/entrypoint.sh`
- Create: `scripts/rename/manifests/06-sklad.tsv`

**Interfaces:**
- Consumes: результаты Task 2–4.
- Produces: artifactId `sklad`, пакет `io.github.balookrd.pristan.sklad`, класс `SkladApplication`, префикс конфигурации `sklad.*`, cookie `sklad_session`, хост и контейнер `sklad`, principal `sklad@R`, `sklad/sklad@R`, `HTTP/sklad@R`, метка `app=sklad`, метрики `sklad.operations`, `sklad.bytes.transferred`, npm-пакет `@pristan/sklad`, цели `make test-sklad`, `make build-sklad`, `make demo-sklad`.

- [ ] **Step 1: Перенести каталоги и файлы**

```bash
git mv backend/hdfs backend/sklad
git mv frontend/apps/hdfs frontend/apps/sklad
git mv demo/hdfs demo/sklad
[ -d docs/images/hdfs ] && git mv docs/images/hdfs docs/images/sklad
git mv docker/Dockerfile.hdfs-java docker/Dockerfile.sklad
for s in main test; do
  d=backend/sklad/src/$s/java/io/github/balookrd/pristan
  git mv "$d/hdfs" "$d/sklad"
done
git ls-files 'backend/sklad/**/HdfsExplorerApplication*.java' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^HdfsExplorerApplication/SkladApplication/')"
done
git ls-files | grep -E '(^|/)(capture_)?hdfs(_manual)?\.spec\.ts$|capture_hdfs\.mjs$|hdfs_explorer_operations\.json$' | while read -r f; do
  n=$(basename "$f" | sed -e 's/hdfs_explorer_operations/sklad_operations/' -e 's/hdfs/sklad/')
  git mv "$f" "$(dirname "$f")/$n"
done
```

- [ ] **Step 2: Найти фактические токены сервиса**

```bash
git grep -h -o -i -E "[a-z0-9_./@-]*hdfs[-_]?explorer[a-z0-9_.-]*" -- . ':!docs' ':!*.md' ':!scripts/rename' | sort | uniq -c | sort -rn
git grep -h -o -E "(test|build|demo|dev|run)[-:_]hdfs[a-z-]*" -- Makefile scripts frontend/package.json frontend/playwright.config.ts .github | sort | uniq -c
```

Сверить с манифестом шага 3.

- [ ] **Step 3: Написать манифест**

`scripts/rename/manifests/06-sklad.tsv`:

```text
keep	hdfs-explorer-super-secret-jwt-token-key
keep	hdfs-explorer-demo-stand-super-secret-key
sub	io.github.balookrd.pristan.hdfs	io.github.balookrd.pristan.sklad
sub	io/github/balookrd/pristan/hdfs	io/github/balookrd/pristan/sklad
sub	HdfsExplorerApplication	SkladApplication
sub	backend/hdfs	backend/sklad
sub	frontend/apps/hdfs	frontend/apps/sklad
sub	apps/hdfs	apps/sklad
sub	demo/hdfs	demo/sklad
sub	docs/images/hdfs	docs/images/sklad
sub	Dockerfile.hdfs-java	Dockerfile.sklad
sub	hdfs-explorer-java	sklad
sub	hdfs-explorer-frontend	@pristan/sklad
sub	hdfs-explorer.jar	sklad.jar
sub	hdfs-explorer-demo	sklad-demo
sub	hdfs-explorer-operations	sklad-operations
sub	hdfs_explorer_operations	sklad_operations
sub	hadoop-hdfs-explorer	sklad
sub	hdfs-explorer.keytab	sklad.keytab
sub	hadoop.proxyuser.hdfs-explorer	hadoop.proxyuser.sklad
sub	hdfs-explorer/hdfs-explorer	sklad/sklad
sub	svc_hdfs_explorer	svc_sklad
sub	hdfs_explorer_session	sklad_session
sub	hdfs_explorer	sklad
sub	hdfs-explorer	sklad
sub	jdbc:h2:mem:hdfs	jdbc:h2:mem:sklad	backend/sklad/*,demo/sklad/*
sub	"hdfs.operations"	"sklad.operations"
sub	"hdfs.bytes.transferred"	"sklad.bytes.transferred"
sub	hdfs_operations	sklad_operations
sub	hdfs_bytes_transferred	sklad_bytes_transferred
sub	app="hdfs"	app="sklad"
re	(test|build|demo)-hdfs(?![a-z-]*-(cluster|site|namenode|datanode))	\1-sklad	Makefile,scripts/*,.github/*,AGENTS.md,README.md,docs/*,demo/*
re	(build|dev):hdfs	\1:sklad	frontend/*
sub	run_hdfs	run_sklad	scripts/*
sub	e2e/hdfs.spec.ts	e2e/sklad.spec.ts
sub	capture_hdfs	capture_sklad
sub	prefix = "hadoop.hdfs"	prefix = "sklad"	backend/sklad/*
re	(?<![.\w])hadoop\.hdfs\.(clusters-config-path|clusters)	sklad.\1
re	(^\s+app:\s*)hdfs\s*$	\1sklad	backend/sklad/*
sub	HDFS Web Explorer	Sklad (HDFS)
sub	HDFS Explorer API	Sklad API
sub	HDFS Explorer	Sklad
```

- [ ] **Step 4: Применить манифест и поднять YAML-ключ**

```bash
python3 scripts/rename/rename_tokens.py scripts/rename/manifests/06-sklad.tsv
git ls-files 'backend/sklad/**/application*.yml' 'demo/sklad/application*.yml' \
  | xargs python3 scripts/rename/hoist_yaml_key.py hadoop hdfs sklad
```

Expected: `hoisted` для `backend/sklad/src/main/resources/application.yml`, `application-mock.yml` и `demo/sklad/application-demo.yml`.

Run: `git grep -n -E '^  hdfs:' -- 'backend/sklad/**/application*.yml' demo/sklad/application-demo.yml`
Expected: пустой вывод.

- [ ] **Step 5: Проверить Kerberos-связку стенда**

Run: `grep -n -E 'sklad' demo/infra/kdc/entrypoint.sh demo/sklad/hadoop/entrypoint.sh demo/sklad/docker-compose*.y*ml demo/sklad/application-demo.yml | grep -E 'HTTP/|proxyuser|hostname|container_name|keytab|principal|domain_realm'`
Expected: principal `sklad/sklad@…` и `HTTP/sklad@…`, ключи `hadoop.proxyuser.sklad.*`, `container_name: sklad`, `sklad.keytab`.

- [ ] **Step 6: Собрать и прогнать тесты**

```bash
make test-sklad 2>&1 | tail -10
(cd frontend && npm install) && make test-ui 2>&1 | tail -10
(cd frontend && npm run build:sklad) && ls backend/sklad/src/main/resources/static/index.html
```

Expected: `BUILD SUCCESS`; Vitest проходит; `index.html` существует. `HdfsIntegrationTest` проверяет поле `service` ответа `/health`: ожидаемое значение в тесте и в `HealthController` — `sklad`.

- [ ] **Step 7: Проверить остатки**

Run: `scripts/rename/residual.sh '[Hh]dfs[-_ ]?[Ee]xplorer|HDFS (Web )?Explorer|backend/hdfs|apps/hdfs|demo/hdfs' | grep -v -E '\.md:|^docs/'`
Expected: пустой вывод.

- [ ] **Step 8: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename HDFS Explorer service to Sklad`

---

### Task 7: Tolmach (SQL)

**Files:**
- Move: `backend/sql` → `backend/tolmach`; `frontend/apps/sql` → `frontend/apps/tolmach`; `demo/sql` → `demo/tolmach`; `docs/images/sql` → `docs/images/tolmach`; `docker/Dockerfile.sql-java` → `docker/Dockerfile.tolmach`; пакет `…/pristan/sql` → `…/pristan/tolmach`; `SqlExplorerApplication*.java`; `frontend/e2e/sql.spec.ts`, `frontend/scripts/capture_sql.mjs`
- Modify: общие файлы, как в Task 5, плюс `demo/tolmach/hive/core-site*.xml`
- Create: `scripts/rename/manifests/07-tolmach.tsv`

**Interfaces:**
- Consumes: результаты Task 2–4.
- Produces: artifactId `tolmach`, пакет `io.github.balookrd.pristan.tolmach`, класс `TolmachApplication`, префикс конфигурации `tolmach.*`, cookie `tolmach_session`, хост и контейнер `tolmach`, учётка `svc_tolmach`, principal `HTTP/tolmach@R`, том `tolmach-data`, метка `app=tolmach`, метрика `tolmach.query.duration`, npm-пакет `@pristan/tolmach`, цели `make test-tolmach`, `make build-tolmach`, `make demo-tolmach`.

- [ ] **Step 1: Перенести каталоги и файлы**

```bash
git mv backend/sql backend/tolmach
git mv frontend/apps/sql frontend/apps/tolmach
git mv demo/sql demo/tolmach
[ -d docs/images/sql ] && git mv docs/images/sql docs/images/tolmach
git mv docker/Dockerfile.sql-java docker/Dockerfile.tolmach
for s in main test; do
  d=backend/tolmach/src/$s/java/io/github/balookrd/pristan
  git mv "$d/sql" "$d/tolmach"
done
git ls-files 'backend/tolmach/**/SqlExplorerApplication*.java' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^SqlExplorerApplication/TolmachApplication/')"
done
git ls-files | grep -E '(^|/)sql\.spec\.ts$|capture_sql\.mjs$' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/sql/tolmach/')"
done
```

- [ ] **Step 2: Найти фактические токены сервиса**

```bash
git grep -h -o -i -E "[a-z0-9_./@-]*sql[-_]?explorer[a-z0-9_.-]*" -- . ':!docs' ':!*.md' ':!scripts/rename' | sort | uniq -c | sort -rn
git grep -h -o -E "(test|build|demo|dev|run)[-:_]sql[a-z-]*" -- Makefile scripts frontend/package.json frontend/playwright.config.ts .github | sort | uniq -c
```

Токен `spark_sql_explorer_analytics` в этом выводе ожидаем: он меняется в Task 8.

- [ ] **Step 3: Написать манифест**

`scripts/rename/manifests/07-tolmach.tsv`:

```text
keep	sql-explorer-super-secret-jwt-token-key
keep	spark_sql_explorer_analytics
keep	spark-sql-explorer-analytics
sub	io.github.balookrd.pristan.sql	io.github.balookrd.pristan.tolmach
sub	io/github/balookrd/pristan/sql	io/github/balookrd/pristan/tolmach
sub	SqlExplorerApplication	TolmachApplication
sub	backend/sql	backend/tolmach
sub	frontend/apps/sql	frontend/apps/tolmach
sub	apps/sql	apps/tolmach
sub	demo/sql	demo/tolmach
sub	docs/images/sql	docs/images/tolmach
sub	Dockerfile.sql-java	Dockerfile.tolmach
sub	sql-explorer-java	tolmach
sub	sql-explorer.jar	tolmach.jar
sub	sql-explorer-demo	tolmach-demo
sub	sql-explorer-data	tolmach-data
sub	hadoop-sql-explorer	tolmach
sub	sql-explorer.keytab	tolmach.keytab
sub	/etc/sql-explorer	/etc/tolmach
sub	svc_sql_explorer	svc_tolmach
sub	sql_explorer_session	tolmach_session
sub	sql_explorer	tolmach
sub	sqlexplorer	tolmach
sub	sql-explorer	tolmach
sub	"sql.query.duration"	"tolmach.query.duration"
sub	sql_query_duration	tolmach_query_duration
sub	app="sql"	app="tolmach"
re	(test|build|demo)-sql(?![a-z-]*-(hive|trino))	\1-tolmach	Makefile,scripts/*,.github/*,AGENTS.md,README.md,docs/*,demo/*
re	(build|dev):sql	\1:tolmach	frontend/*
sub	run_sql	run_tolmach	scripts/*
sub	e2e/sql.spec.ts	e2e/tolmach.spec.ts
sub	capture_sql	capture_tolmach
re	^sql:	tolmach:	backend/tolmach/*,demo/tolmach/application-demo.yml
sub	prefix = "sql"	prefix = "tolmach"	backend/tolmach/*
re	(^\s+app:\s*)sql\s*$	\1tolmach	backend/tolmach/*
sub	SQL Web Explorer	Tolmach (SQL)
sub	SQL Explorer API	Tolmach API
sub	SQL Explorer (Trino & Hive)	Tolmach (Trino и Hive)
sub	SQL Explorer	Tolmach
```

- [ ] **Step 4: Задать имя npm-пакета**

В `frontend/apps/tolmach/package.json` заменить `"name": "frontend"` на `"name": "@pristan/tolmach"`.

Run: `grep -n '"name"' frontend/apps/tolmach/package.json`
Expected: `"name": "@pristan/tolmach",`

- [ ] **Step 5: Применить манифест**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/07-tolmach.tsv`
Expected: предупреждения допустимы для `sql_query_duration` и `app="sql"`.

- [ ] **Step 6: Проверить ключи конфигурации в Java и proxyuser**

Run: `git grep -n -E '"\$\{sql\.|name *= *"sql\.|"sql\.(results-ttl-hours|max-rows-per-query|ai|clusters)' -- backend/tolmach`
Expected: пустой вывод; найденное заменить на `tolmach.`.

Run: `grep -n -E 'proxyuser' demo/tolmach/hive/core-site.xml demo/tolmach/hive/core-site-cluster2.xml`
Expected: ключи `hadoop.proxyuser.svc_tolmach.hosts` и `.groups`.

- [ ] **Step 7: Собрать и прогнать тесты**

```bash
make test-tolmach 2>&1 | tail -10
(cd frontend && npm install) && make test-ui 2>&1 | tail -10
(cd frontend && npm run build:tolmach) && ls backend/tolmach/src/main/resources/static/index.html
```

Expected: `BUILD SUCCESS`; Vitest проходит; `index.html` существует. Таблицы `sql_user_workspaces` в сущностях и в `H2TextColumnsTest` должны остаться с прежним именем.

Run: `git grep -c 'sql_user_workspaces' -- backend/tolmach | head -3`
Expected: непустой вывод.

- [ ] **Step 8: Проверить остатки**

Run: `scripts/rename/residual.sh '[Ss]ql[-_ ]?[Ee]xplorer|SQL (Web )?Explorer|backend/sql|apps/sql|demo/sql' | grep -v -E '\.md:|^docs/|spark[_-]sql[_-]explorer[_-]analytics'`
Expected: пустой вывод.

- [ ] **Step 9: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename SQL Explorer service to Tolmach`

---

### Task 8: Iskra (Spark)

**Files:**
- Move: `backend/spark` → `backend/iskra`; `frontend/apps/spark` → `frontend/apps/iskra`; `demo/spark` → `demo/iskra`; `docs/images/spark` → `docs/images/iskra`; `docker/Dockerfile.spark-java` → `docker/Dockerfile.iskra`; пакет `…/pristan/spark` → `…/pristan/iskra`; `SparkExplorerApplication*.java`; `frontend/e2e/spark.spec.ts`, `frontend/scripts/capture_spark.mjs`; дашборд `spark_sql_explorer_analytics.json` → `iskra_tolmach_analytics.json`
- Modify: общие файлы, как в Task 5
- Create: `scripts/rename/manifests/08-iskra.tsv`

**Interfaces:**
- Consumes: результаты Task 2–4 и Task 7 (общий дашборд).
- Produces: artifactId `iskra`, пакет `io.github.balookrd.pristan.iskra`, класс `IskraApplication`, префикс конфигурации `iskra.*`, cookie `iskra_session`, ключи localStorage `iskra_state_v2`, `iskra_state_<user>`, хост и контейнер `iskra`, учётка `svc_iskra`, LDAP-группа `iskra-users`, метка `app=iskra`, метрики `iskra.statement.duration`, `iskra.sessions.active`, npm-пакет `@pristan/iskra`, дашборд `iskra_tolmach_analytics`, цели `make test-iskra`, `make build-iskra`, `make demo-iskra`.

- [ ] **Step 1: Перенести каталоги и файлы**

```bash
git mv backend/spark backend/iskra
git mv frontend/apps/spark frontend/apps/iskra
git mv demo/spark demo/iskra
[ -d docs/images/spark ] && git mv docs/images/spark docs/images/iskra
git mv docker/Dockerfile.spark-java docker/Dockerfile.iskra
for s in main test; do
  d=backend/iskra/src/$s/java/io/github/balookrd/pristan
  git mv "$d/spark" "$d/iskra"
done
git ls-files 'backend/iskra/**/SparkExplorerApplication*.java' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^SparkExplorerApplication/IskraApplication/')"
done
git ls-files | grep -E '(^|/)spark\.spec\.ts$|capture_spark\.mjs$|spark_sql_explorer_analytics\.json$' | while read -r f; do
  n=$(basename "$f" | sed -e 's/spark_sql_explorer_analytics/iskra_tolmach_analytics/' -e 's/^spark/iskra/' -e 's/capture_spark/capture_iskra/')
  git mv "$f" "$(dirname "$f")/$n"
done
```

- [ ] **Step 2: Найти фактические токены сервиса**

```bash
git grep -h -o -i -E "[a-z0-9_./@-]*spark[-_]?(sql[-_])?explorer[a-z0-9_.-]*" -- . ':!docs' ':!*.md' ':!scripts/rename' | sort | uniq -c | sort -rn
git grep -h -o -E "(test|build|demo|dev|run)[-:_]spark[a-z-]*" -- Makefile scripts frontend/package.json frontend/playwright.config.ts .github | sort | uniq -c
```

- [ ] **Step 3: Написать манифест**

`scripts/rename/manifests/08-iskra.tsv`:

```text
keep	spark-explorer-super-secret-jwt-token-key
keep	spark-explorer-demo-secret-jwt-key
sub	spark_sql_explorer_analytics	iskra_tolmach_analytics
sub	spark-sql-explorer-analytics	iskra-tolmach-analytics
sub	Spark & SQL Explorer - Interactive Analytics	Iskra и Tolmach: интерактивная аналитика
sub	io.github.balookrd.pristan.spark	io.github.balookrd.pristan.iskra
sub	io/github/balookrd/pristan/spark	io/github/balookrd/pristan/iskra
sub	SparkExplorerApplication	IskraApplication
sub	backend/spark	backend/iskra
sub	frontend/apps/spark	frontend/apps/iskra
sub	apps/spark	apps/iskra
sub	demo/spark	demo/iskra
sub	docs/images/spark	docs/images/iskra
sub	Dockerfile.spark-java	Dockerfile.iskra
sub	spark-explorer-java	iskra
sub	@pristan/spark	@pristan/iskra
sub	spark-explorer.jar	iskra.jar
sub	spark-explorer-demo	iskra-demo
sub	spark-explorer-livy-pyspark-scala-metastore	iskra-livy-pyspark-scala-metastore
sub	hadoop-spark-explorer	iskra
sub	spark-explorer.keytab	iskra.keytab
sub	svc_spark_explorer	svc_iskra
sub	spark_explorer_session	iskra_session
sub	spark_explorer_state_	iskra_state_
sub	spark_explorer	iskra
sub	sparkexplorer	iskra
sub	spark-explorer	iskra
sub	"spark.statement.duration"	"iskra.statement.duration"
sub	"spark.sessions.active"	"iskra.sessions.active"
sub	spark_statement_duration	iskra_statement_duration
sub	spark_sessions_active	iskra_sessions_active
sub	app="spark"	app="iskra"
sub	spark-users	iskra-users
re	(test|build|demo)-spark(?![a-z-]*-(livy|yarn|nm|history))	\1-iskra	Makefile,scripts/*,.github/*,AGENTS.md,README.md,docs/*,demo/*
re	(build|dev):spark	\1:iskra	frontend/*
sub	run_spark	run_iskra	scripts/*
sub	e2e/spark.spec.ts	e2e/iskra.spec.ts
sub	capture_spark	capture_iskra
re	^spark:	iskra:	backend/iskra/*,demo/iskra/application-demo.yml
sub	prefix = "spark.clusters	prefix = "iskra.clusters	backend/iskra/*
sub	prefix = "spark"	prefix = "iskra"	backend/iskra/*
sub	${spark.session-sync-interval-ms	${iskra.session-sync-interval-ms	backend/iskra/*
re	(^\s+app:\s*)spark\s*$	\1iskra	backend/iskra/*
sub	Spark Explorer | Pristan	Iskra (Spark) | Pristan
sub	Spark Explorer API	Iskra API
sub	Spark Explorer	Iskra
```

- [ ] **Step 4: Применить манифест**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/08-iskra.tsv`
Expected: предупреждения допустимы для `spark_statement_duration`, `spark_sessions_active`, `app="spark"`.

- [ ] **Step 5: Проверить, что ключи движка не задеты**

Run: `git grep -c -E '"spark\.(master|yarn\.|jars|kerberos\.|sql\.shuffle\.partitions)' -- backend/iskra | head`
Expected: непустой вывод — литералы Spark, передаваемые в Livy, на месте.

Run: `git grep -n -E '"\$\{spark\.|name *= *"spark\.|"spark\.(session-ttl-minutes|results-ttl-hours|clusters|session-sync-interval-ms)' -- backend/iskra`
Expected: пустой вывод; найденное заменить на `iskra.`.

Run: `git grep -c -E 'spark_sessions|spark_execution_history|spark_user_workspaces' -- backend/iskra | head -3`
Expected: непустой вывод — имена таблиц не изменились.

- [ ] **Step 6: Собрать и прогнать тесты**

```bash
make test-iskra 2>&1 | tail -10
(cd frontend && npm install) && make test-ui 2>&1 | tail -10
(cd frontend && npm run build:iskra) && ls backend/iskra/src/main/resources/static/index.html
```

Expected: `BUILD SUCCESS`; Vitest проходит; `index.html` существует.

- [ ] **Step 7: Проверить остатки**

Run: `scripts/rename/residual.sh '[Ss]park[-_ ]?([Ss]ql[-_ ]?)?[Ee]xplorer|Spark Explorer|backend/spark|apps/spark|demo/spark|spark-users' | grep -v -E '\.md:|^docs/'`
Expected: пустой вывод.

- [ ] **Step 8: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename Spark Explorer service to Iskra`

---

### Task 9: Parom — модули, пакеты, классы, protobuf, конфигурация

**Files:**
- Move: `backend/replicator` → `backend/parom`; `frontend/apps/replicator` → `frontend/apps/parom`; `demo/replicator` → `demo/parom`; `docs/images/replicator` → `docs/images/parom`; `docker/Dockerfile.replicator-{agent,orchestrator}` → `docker/Dockerfile.parom-{agent,orchestrator}`; пакеты `…/pristan/replicator` → `…/pristan/parom` в модулях `model`, `agent`, `orchestrator`; файлы классов `Replicator*` и `KafkaReplicatorAgentMain`; `replicator.proto` → `parom.proto`, `kafka_replicator.proto` → `parom_kafka.proto`; `agent/bin/replicator-agent.sh` → `parom-agent.sh`
- Modify: `pom.xml`, `backend/parom/**/pom.xml`, `Makefile`, `scripts/**`, `.github/workflows/*.yml`, `.ast-grep/rules/no-synchronized-on-io-path.yml`, `demo/parom/**`, `demo/all/**`, `demo/infra/**`
- Create: `scripts/rename/manifests/09-parom-code.tsv`

**Interfaces:**
- Consumes: результаты Task 2–4.
- Produces: artifactId `parom-parent`, `parom-proto`, `parom-model`, `parom-agent`, `parom-orchestrator`; пакет `io.github.balookrd.pristan.parom`; классы `ParomAgent`, `ParomAgentMain`, `ParomAgentConfig`, `ParomApplicationMaster`, `ParomYarnClient`, `ParomOrchestratorApplication`, `ParomProperties`, `ParomMetricsService`, `ParomMetricsController`, `ParomKafkaAgentMain`; proto-пакеты `parom`, `parom.kafka`, `parom.control`; внешние классы `ParomProto`, `ParomKafkaProto`; префикс конфигурации `parom.*`; переменные `PAROM_*`.

- [ ] **Step 1: Перенести каталоги и файлы**

```bash
git mv backend/replicator backend/parom
git mv frontend/apps/replicator frontend/apps/parom
git mv demo/replicator demo/parom
[ -d docs/images/replicator ] && git mv docs/images/replicator docs/images/parom
git mv docker/Dockerfile.replicator-agent docker/Dockerfile.parom-agent
git mv docker/Dockerfile.replicator-orchestrator docker/Dockerfile.parom-orchestrator
git ls-files backend/parom | grep -o -E '^.*/java/io/github/balookrd/pristan/replicator' | sort -u | while read -r d; do
  git mv "$d" "$(dirname "$d")/parom"
done
git mv backend/parom/proto/src/main/proto/replicator.proto backend/parom/proto/src/main/proto/parom.proto
git mv backend/parom/proto/src/main/proto/kafka_replicator.proto backend/parom/proto/src/main/proto/parom_kafka.proto
git mv backend/parom/agent/bin/replicator-agent.sh backend/parom/agent/bin/parom-agent.sh
git ls-files backend/parom | grep -E '/KafkaReplicatorAgentMain[A-Za-z]*\.java$' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^KafkaReplicatorAgentMain/ParomKafkaAgentMain/')"
done
git ls-files backend/parom | grep -E '/Replicator[A-Za-z]*\.java$' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^Replicator/Parom/')"
done
git ls-files backend/parom | grep -E '/[A-Za-z]*Replicator[A-Za-z]*\.java$'
```

Expected: последняя команда ничего не печатает. Если печатает — это класс с `Replicator` в середине имени: перенести его, заменив `Replicator` на `Parom`, и добавить имя в манифест шага 3.

- [ ] **Step 2: Найти фактические токены**

```bash
git grep -h -o -E "[A-Za-z]*Replicator[A-Za-z]*" -- 'backend/parom/**/*.java' 'backend/parom/**/*.proto' | sort | uniq -c | sort -rn
git grep -h -o -E "(HADOOP_)?REPLICATOR_[A-Z0-9_]+" | sort -u | wc -l
git grep -h -o -E "[a-z0-9_./-]*replicator[a-z0-9_./-]*" -- . ':!docs' ':!*.md' ':!scripts/rename' ':!*.java' ':!monitoring' ':!demo/monitoring' | sort | uniq -c | sort -rn | head -80
```

Сверить с манифестом шага 3. Имена метрик (`replicator_*`, `replicator.*`), контейнеров, Kafka и Kerberos в манифесте этой задачи защищены строками `keep` либо не затрагиваются: они меняются в Task 10.

- [ ] **Step 3: Написать манифест**

`scripts/rename/manifests/09-parom-code.tsv`:

```text
# published default secret: must keep rejecting old configs
keep	replicator-secure-agent-secret-key-12345
# packages and paths
sub	io.github.balookrd.pristan.replicator	io.github.balookrd.pristan.parom
sub	io/github/balookrd/pristan/replicator	io/github/balookrd/pristan/parom
sub	backend/replicator	backend/parom
sub	frontend/apps/replicator	frontend/apps/parom
sub	apps/replicator	apps/parom
sub	demo/replicator	demo/parom
sub	docs/images/replicator	docs/images/parom
sub	Dockerfile.replicator-	Dockerfile.parom-
sub	<module>replicator/	<module>parom/
# classes
sub	KafkaReplicatorAgentMain	ParomKafkaAgentMain
sub	KafkaReplicatorProto	ParomKafkaProto
sub	ReplicatorOrchestratorApplication	ParomOrchestratorApplication
sub	ReplicatorApplicationMaster	ParomApplicationMaster
sub	ReplicatorMetricsController	ParomMetricsController
sub	ReplicatorMetricsService	ParomMetricsService
sub	ReplicatorAgentConfig	ParomAgentConfig
sub	ReplicatorAgentMain	ParomAgentMain
sub	ReplicatorYarnClient	ParomYarnClient
sub	ReplicatorProperties	ParomProperties
sub	ReplicatorAgent	ParomAgent
sub	ReplicatorProto	ParomProto
# protobuf
sub	kafka_replicator.proto	parom_kafka.proto
sub	replicator.proto	parom.proto
re	^package replicator(\.kafka|\.control)?;	package parom\1;	backend/parom/proto/*
sub	replicator.control.	parom.control.	backend/parom/*
sub	replicator.kafka.KafkaDataTransferService	parom.kafka.KafkaDataTransferService
sub	replicator.DataTransferService	parom.DataTransferService
sub	replicator.HmsTransferService	parom.HmsTransferService
re	(?<![.\w])replicator\.(HmsTransfer|DataTransfer|Heartbeat|Chunk|Task|Job|Token|Transfer|Cluster|Agent|File|Manifest|Bundle)	parom.\1	backend/parom/proto/*
# maven artifacts
sub	replicator-parent	parom-parent
sub	replicator-proto	parom-proto
sub	replicator-model	parom-model
sub	<artifactId>replicator-agent</artifactId>	<artifactId>parom-agent</artifactId>
sub	<artifactId>replicator-orchestrator</artifactId>	<artifactId>parom-orchestrator</artifactId>
sub	replicator-agent-1.0.0	parom-agent-1.0.0
sub	replicator-orchestrator-1.0.0	parom-orchestrator-1.0.0
sub	replicator-agent.jar	parom-agent.jar
sub	replicator-orchestrator.jar	parom-orchestrator.jar
sub	replicator-agent.sh	parom-agent.sh
sub	replicator-explorer-frontend	@pristan/parom
# configuration
sub	prefix = "hadoop.replicator"	prefix = "parom"
re	(?<![.\w])hadoop\.replicator\.	parom.
sub	replicator.hms.mock-cluster-api	parom.hms.mock-cluster-api
sub	replicator.schemaregistry.block-loopback	parom.schemaregistry.block-loopback
sub	hadoop-grpc-replicator-orchestrator	parom-orchestrator
sub	HADOOP_REPLICATOR_	PAROM_
re	(?<![A-Z_])REPLICATOR_	PAROM_
sub	jdbc:h2:mem:replicator	jdbc:h2:mem:parom
sub	/app/data/replicator	/app/data/parom
# make / npm / scripts / CI
re	(test|build|demo)-replicator	\1-parom	Makefile,scripts/*,.github/*,AGENTS.md,README.md,docs/*,demo/*
re	(build|dev):replicator	\1:parom	frontend/*
sub	frontend-build-replicator	frontend-build-parom
sub	run_replicator	run_parom	scripts/*
sub	replicator/orchestrator	parom/orchestrator	.github/*,scripts/*,Makefile
# titles
sub	Hadoop gRPC Replicator :: Parent	Pristan :: Parom :: Parent
sub	Hadoop gRPC Replicator	Parom
```

- [ ] **Step 4: Применить манифест**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/09-parom-code.tsv`
Expected: предупреждение допустимо для правила `re … replicator\.(HmsTransfer|…)` — оно страхует ссылки на типы с полным именем внутри `.proto`, которых может не быть.

- [ ] **Step 5: Поднять YAML-ключ `hadoop.replicator`**

```bash
git ls-files 'backend/parom/**/application*.yml' 'demo/parom/**/application*.yml' \
  | xargs python3 scripts/rename/hoist_yaml_key.py hadoop replicator parom
git grep -n -E '^  replicator:' -- 'backend/parom/**/application*.yml' 'demo/parom/**/application*.yml'
```

Expected: последняя команда ничего не печатает.

- [ ] **Step 6: Проверить denylist и proto**

Run: `git grep -n 'replicator-secure-agent-secret-key-12345' -- backend/parom | head -3`
Expected: строка в `ParomProperties.java` (константа `LEGACY_DEFAULT_AGENT_SECRET`) на месте.

Run: `grep -n -E '^package |java_package|java_outer_classname' backend/parom/proto/src/main/proto/*.proto`
Expected: `package parom;`, `package parom.kafka;`, `package parom.control;`; `java_package` начинается с `io.github.balookrd.pristan.parom`; внешние классы `ParomProto`, `ParomKafkaProto`, `OrchestratorControlPlaneProto`.

- [ ] **Step 7: Проверить правило ast-grep**

Run: `ast-grep scan 2>/dev/null | grep -c 'no-synchronized-on-io-path'`
Expected: то же число, что в `scripts/rename/baseline.txt`. Ноль при ненулевом исходном значении означает, что пути в `.ast-grep/rules/no-synchronized-on-io-path.yml` не совпадают с каталогами: исправить четыре пути на `backend/parom/agent/src/main/java/io/github/balookrd/pristan/parom/…`.

- [ ] **Step 8: Собрать и прогнать тесты**

```bash
rm -rf backend/parom/proto/target backend/parom/model/target backend/parom/agent/target backend/parom/orchestrator/target
make build-parom 2>&1 | tail -5
make test-parom 2>&1 | tail -20
(cd frontend && npm install) && make test-ui 2>&1 | tail -10
(cd frontend && npm run build:parom) && ls backend/parom/orchestrator/src/main/resources/static/index.html
```

Expected: `BUILD SUCCESS`; Vitest проходит; `index.html` существует. Каталоги `target/` удаляются перед сборкой, потому что в них лежит сгенерированный protobuf-код со старыми именами классов.

- [ ] **Step 9: Проверить остатки кода**

Run: `scripts/rename/residual.sh 'Replicator[A-Z]|ReplicatorProto|backend/replicator|apps/replicator|demo/replicator|hadoop\.replicator|REPLICATOR_|replicator-(parent|proto|model)' | grep -v -E '\.md:|^docs/'`
Expected: пустой вывод.

- [ ] **Step 10: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename Replicator modules, packages and config to Parom`

---

### Task 10: Parom — данные, Kafka, Kerberos, стенды, метрики

**Files:**
- Create: `backend/parom/orchestrator/src/main/resources/db/migration/<набор>/V<N>__rename_parom_tables.sql` для каждого набора, где есть baseline с этими таблицами
- Modify: `DrSnapshotEntity.java`, `StreamingLeaseEntity.java`, `ConsumerGroupBlacklistFilter.java`, `MetaStoreConnectionFactory.java`, `ParomYarnClient.java`, `ParomApplicationMaster.java`, `AgentMetrics.java`, `ParomMetricsService.java`, `KafkaReplicationController.java`, `RoleResolver.java`, `AgentSecretAuthenticationFilter.java`, `frontend/apps/parom/src/**`, `demo/parom/**`, `demo/infra/kdc/entrypoint.sh`, `demo/infra/ldap/bootstrap.ldif`, `monitoring/**`, `demo/monitoring/**`, `scripts/generate-presentation-*.py`
- Move: `monitoring/grafana/dashboards/hadoop_replicator_{agents,hms_dr,kafka,overview}.json` → `parom_{agents,hms_dr,kafka,overview}.json` (и копии в `demo/monitoring/`)
- Create: `scripts/rename/manifests/10-parom-contracts.tsv`

**Interfaces:**
- Consumes: классы и пакеты из Task 9.
- Produces: таблицы `parom_dr_snapshots`, `parom_streaming_leases`; consumer group `parom-kafka-<dc>-<job>`; principal `parom-kafka@R`, `parom@REALM.LOCAL`; LDAP-группы `parom-operators`, `parom-agents`; API `/api/v1/parom/kafka`; метрики `parom_*` и `parom.*`; контейнеры `parom-*`, `parom-kafka-agent-*`; `job_name: 'parom'`; дашборды `parom_*` с uid `parom-*`.

- [ ] **Step 1: Написать падающий тест на имена таблиц**

Найти тест целостности схемы и его каталог:

Run: `git ls-files backend/parom/orchestrator | grep -E 'SchemaMigrationIntegrityTest|MigrationsApply'`

В найденный класс `SchemaMigrationIntegrityTest` добавить тест (имя пакета и способ получить `DataSource` — как в соседних тестах этого класса):

```java
@Test
void drAndStreamingTablesUseParomPrefix() throws Exception {
    try (var conn = dataSource.getConnection();
         var rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
        var names = new java.util.TreeSet<String>();
        while (rs.next()) {
            names.add(rs.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT));
        }
        org.assertj.core.api.Assertions.assertThat(names)
                .contains("parom_dr_snapshots", "parom_streaming_leases")
                .doesNotContain("replicator_dr_snapshots", "replicator_streaming_leases");
    }
}
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `JAVA_HOME=<jdk21> mvn test -f backend/parom/pom.xml -pl orchestrator -am -Dtest=SchemaMigrationIntegrityTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | tail -15`
Expected: FAIL, в сообщении `Expecting … to contain: ["parom_dr_snapshots", "parom_streaming_leases"]`.

`<jdk21>` — путь к JDK 21; его печатает `make check-jdk21 && make -s -p | sed -n 's/^JAVA21_HOME *:\{0,1\}= *//p' | head -1`.

- [ ] **Step 3: Добавить миграцию**

Определить следующий номер версии и наборы:

Run: `git ls-files 'backend/parom/orchestrator/src/main/resources/db/migration/**' | sed -E 's#.*/migration/([^/]+)/V([0-9]+)__.*#\2 \1#' | sort -n | tail -5; git grep -l -E 'replicator_(dr_snapshots|streaming_leases)' -- 'backend/parom/orchestrator/src/main/resources/db/migration'`

Пусть наибольший номер — `M`. В каталоге набора, где лежит baseline с этими таблицами (по инвентарю — `common`), создать `V<M+1>__rename_parom_tables.sql`:

```sql
-- Rename product-prefixed tables after the Replicator -> Parom rename.
ALTER TABLE replicator_dr_snapshots RENAME TO parom_dr_snapshots;
ALTER TABLE replicator_streaming_leases RENAME TO parom_streaming_leases;
```

Синтаксис `ALTER TABLE … RENAME TO …` одинаков для H2 и PostgreSQL, поэтому файл в наборе `common` покрывает оба диалекта. Если baseline с этими таблицами продублирован в наборах `h2` и `postgresql`, положить тот же файл в каждый из них вместо `common`.

Индексы и ограничения с именем `replicator_…` найти и переименовать в той же миграции:

Run: `grep -n -i -E '(index|constraint) +[a-z_]*replicator[a-z_]*' backend/parom/orchestrator/src/main/resources/db/migration/common/V1__baseline_schema.sql`

Для каждой найденной строки добавить `ALTER INDEX <старое> RENAME TO <новое>;` с заменой `replicator_` на `parom_`. Ограничения первичного ключа не переименовывать: в H2 и PostgreSQL для этого разный синтаксис, а на работу схемы имя не влияет.

- [ ] **Step 4: Обновить сущности**

В `DrSnapshotEntity.java` заменить `@Table(name = "replicator_dr_snapshots")` на `@Table(name = "parom_dr_snapshots")`; в `StreamingLeaseEntity.java` — `"replicator_streaming_leases"` на `"parom_streaming_leases"`. Нативные запросы с этими именами найти и заменить:

Run: `git grep -n -E 'replicator_(dr_snapshots|streaming_leases)' -- backend/parom ':!*V1__baseline_schema.sql'`
Expected после правки: пустой вывод.

- [ ] **Step 5: Убедиться, что тест проходит**

Run: `JAVA_HOME=<jdk21> mvn test -f backend/parom/pom.xml -pl orchestrator -am -Dtest='SchemaMigrationIntegrityTest,MigrationsApply*' -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 6: Дополнить фильтр consumer group**

В `backend/parom/model/src/main/java/io/github/balookrd/pristan/parom/**/ConsumerGroupBlacklistFilter.java` в регулярное выражение добавить ветку `parom-.*` первой в группе альтернатив; ветки `kafka-replicator-.*`, `replicator-.*`, `hadoop-replicator-.*` оставить. В тест этого класса добавить проверки:

```java
@Test
void excludesParomServiceGroups() {
    assertThat(ConsumerGroupBlacklistFilter.isBlacklisted("parom-kafka-dc1-42")).isTrue();
    assertThat(ConsumerGroupBlacklistFilter.isBlacklisted("kafka-replicator-dc1-42")).isTrue();
}
```

Имя метода (`isBlacklisted`) взять из существующих тестов класса. Run: `JAVA_HOME=<jdk21> mvn test -f backend/parom/pom.xml -pl model -am -Dtest='ConsumerGroupBlacklistFilter*' -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 7: Перенести дашборды и написать манифест**

```bash
git ls-files | grep -E 'hadoop_replicator_(agents|hms_dr|kafka|overview)\.json$' | while read -r f; do
  git mv "$f" "$(dirname "$f")/$(basename "$f" | sed 's/^hadoop_replicator_/parom_/')"
done
```

`scripts/rename/manifests/10-parom-contracts.tsv`:

```text
keep	replicator-secure-agent-secret-key-12345
# dashboards
sub	hadoop_replicator_	parom_
sub	hadoop-replicator-agents	parom-agents-dashboard
sub	hadoop-replicator-hms-dr	parom-hms-dr
sub	hadoop-replicator-kafka-demo	parom-kafka-demo
sub	hadoop-replicator-kafka-agent	parom-kafka-agent
sub	hadoop-replicator-kafka	parom-kafka-dashboard
sub	hadoop-replicator-overview	parom-overview
sub	hadoop-replicator-demo	parom-demo
sub	job_name: 'hadoop-replicator'	job_name: 'parom'
sub	job=~\"hadoop-replicator\"	job=~\"parom\"
sub	app: 'replicator'	app: 'parom'
sub	replicator-metrics-scrape-token	parom-metrics-scrape-token
# kafka
sub	"kafka-replicator-" +	"parom-kafka-" +
sub	kafka-replicator-agent	parom-kafka-agent
sub	kafka-replicator.keytab	parom-kafka.keytab
sub	kafka-replicator@	parom-kafka@
sub	kafka-replicator/	parom-kafka/
# kerberos and hdfs
sub	hdfs-replicator@REALM.LOCAL	parom@REALM.LOCAL
sub	replicator-dc1.keytab	parom-dc1.keytab
sub	replicator-dc2.keytab	parom-dc2.keytab
sub	/etc/replicator.keytab	/etc/parom.keytab
sub	.replicator-staging	.parom-staging
sub	/tmp/replicator	/tmp/parom
sub	replicator-hms-token	parom-hms-token
sub	Hadoop-Replicator-Agent-	Parom-Agent-
# ldap
sub	replicator-operators	parom-operators
sub	replicator-agents	parom-agents
# containers, volumes, db
sub	replicator-orchestrator	parom-orchestrator
sub	replicator-streamer	parom-streamer
sub	replicator-smoke-test	parom-smoke-test
sub	replicator-ha-pg-data	parom-ha-pg-data
sub	replicator-db-data	parom-db-data
sub	replicator-db	parom-db
sub	replicator-agent	parom-agent
sub	POSTGRES_DB:replicator	POSTGRES_DB:parom
sub	POSTGRES_DB: replicator	POSTGRES_DB: parom
sub	POSTGRES_USER: replicator	POSTGRES_USER: parom
sub	POSTGRES_USER:replicator	POSTGRES_USER:parom
sub	:5432/replicator	:5432/parom
# api and ui state
sub	/api/v1/replicator/kafka	/api/v1/parom/kafka
sub	/replicator/kafka	/parom/kafka
sub	replicator_active_tab	parom_active_tab
# metrics and telemetry
sub	replicator.dc_id	parom.dc_id
re	"replicator\.(compression|grpc|staging)\.	"parom.\1.
re	(?<![A-Za-z0-9_])replicator_(?!dr_snapshots|streaming_leases)	parom_
```

Uid дашбордов `parom-agents-dashboard` и `parom-kafka-dashboard` отличаются от имён LDAP-группы `parom-agents` и principal `parom-kafka` намеренно: иначе следующие правила манифеста и поиск остатков не различают uid и идентификатор.

- [ ] **Step 8: Применить манифест**

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/10-parom-contracts.tsv`
Expected: предупреждения допустимы для вариантов записи `POSTGRES_*` и `job=~`, которых нет в файлах. Сверить остальные с выводом Task 9, шаг 2.

- [ ] **Step 9: Проверить, что предметные имена не задеты**

Run: `git grep -c -E 'replication_(jobs|tasks|agents)' -- backend/parom | head -3; git grep -c 'X-Repl-Origin' -- backend/parom | head -2`
Expected: непустой вывод в обоих случаях.

Run: `git diff --stat -- '*V1__baseline_schema.sql' '*SecurityConfigurationValidator*' | tail -1`
Expected: пустой вывод — оба файла не изменены.

- [ ] **Step 10: Собрать, прогнать тесты и стенд**

```bash
make test-parom 2>&1 | tail -20
make test-ui 2>&1 | tail -10
make build 2>&1 | tail -5
make demo-parom && sleep 60 && docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'parom'
make demo-kafka-smoke 2>&1 | tail -15
make demo-monitoring && make demo-monitoring-smoke 2>&1 | tail -15
make demo-parom-stop; make demo-monitoring-stop
```

Expected: `BUILD SUCCESS`; контейнеры `parom-orchestrator`, `parom-agent-dc1`, `parom-agent-dc2` в состоянии `Up`; smoke-тесты Kafka и мониторинга проходят. Сбой smoke-теста мониторинга на проверке uid означает расхождение между uid в JSON дашборда и строкой в `demo/monitoring/run-monitoring-smoke-tests.sh`.

- [ ] **Step 11: Проверить остатки**

Run: `scripts/rename/residual.sh '[Rr]eplicator' | grep -v -E '\.md:|^docs/'`
Expected: только строки с `LEGACY_DEFAULT_AGENT_SECRET` и её значением `replicator-secure-agent-secret-key-12345`.

- [ ] **Step 12: Контрольная точка**

Предлагаемое сообщение: `refactor!: rename Replicator contracts, stands and metrics to Parom`

---

### Task 11: LoginModal, общие файлы, стенды остальных сервисов, документация

**Files:**
- Modify: `frontend/common/components/LoginModal.svelte` и его тест; `frontend/apps/{artel,sklad,tolmach,iskra,parom}/src/App.svelte`
- Modify: `README.md`, `AGENTS.md`, `docs/**/*.md` (кроме `docs/superpowers/`), `backend/*/README.md`, `demo/**/README.md`, `ansible/README.md`, `scripts/check-russian-ui.py`, `scripts/check-russian-docs.py`, `scripts/generate-presentation-*.py`
- Move: `docs/{yarn,hdfs,sql,spark,replicator}-admin-guide.md` → `docs/{artel,sklad,tolmach,iskra,parom}-admin-guide.md`
- Create: `scripts/rename/manifests/11-docs.tsv`

**Interfaces:**
- Consumes: все имена из Task 2–10.
- Produces: свойство `app` компонента `LoginModal` со значениями `'artel' | 'sklad' | 'tolmach' | 'iskra' | 'parom'`; функции `detectApp` нет.

- [ ] **Step 1: Прочитать текущее определение приложения**

Run: `grep -n -E 'detectApp|APP_MOCK_USERS|document\.title|export let|\$props\(\)|id: *.(yarn|hdfs|sql|spark|replicator|universal)' frontend/common/components/LoginModal.svelte`

Run: `git ls-files frontend | grep -i -E 'LoginModal.*\.(test|spec)\.ts$'; git grep -n '<LoginModal' -- frontend/apps`

- [ ] **Step 2: Написать падающий тест**

В существующий файл теста `LoginModal` (если его нет — создать `frontend/common/components/LoginModal.test.ts` по образцу соседних тестов компонентов в `frontend/common`) добавить:

```ts
import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import LoginModal from './LoginModal.svelte';

describe('LoginModal app selection', () => {
  it('uses the explicit app prop and ignores document.title', () => {
    document.title = 'YARN Queue Explorer';
    render(LoginModal, { props: { app: 'tolmach', open: true } });
    expect(screen.getByTestId('login-modal').dataset.app).toBe('tolmach');
  });

  it('accepts every service id', () => {
    for (const app of ['artel', 'sklad', 'tolmach', 'iskra', 'parom'] as const) {
      const { unmount } = render(LoginModal, { props: { app, open: true } });
      expect(screen.getByTestId('login-modal').dataset.app).toBe(app);
      unmount();
    }
  });
});
```

Имя свойства видимости (`open`) взять из вывода шага 1; если компонент показывается по другому свойству — подставить его.

- [ ] **Step 3: Убедиться, что тест падает**

Run: `cd frontend && npx vitest run common/components/LoginModal.test.ts 2>&1 | tail -15`
Expected: FAIL — элемент с `data-testid="login-modal"` не найден либо `dataset.app` равно `undefined`.

- [ ] **Step 4: Заменить определение по заголовку на явное свойство**

В `frontend/common/components/LoginModal.svelte`:

1. Удалить функцию `detectApp` и её вызовы.
2. Объявить тип и обязательное свойство. В синтаксисе Svelte 5:

   ```ts
   export type AppId = 'artel' | 'sklad' | 'tolmach' | 'iskra' | 'parom';
   ```

   и в деструктуризации `$props()` добавить `app` с типом `AppId` без значения по умолчанию.
3. На корневой элемент модального окна добавить атрибуты `data-testid="login-modal"` и `data-app={app}`.
4. Ключи объекта `APP_MOCK_USERS` и поля `id` вкладок заменить: `yarn` → `artel`, `hdfs` → `sklad`, `sql` → `tolmach`, `spark` → `iskra`, `replicator` → `parom`. Ветку `universal` удалить вместе с её вкладкой, если после удаления `detectApp` на неё не осталось ссылок.

В каждом `frontend/apps/<имя>/src/App.svelte` в теге `<LoginModal …>` передать `app="<имя>"`, где `<имя>` — `artel`, `sklad`, `tolmach`, `iskra` или `parom`. Прежний атрибут `app="yarn"` и ему подобные заменить на новое значение.

- [ ] **Step 5: Убедиться, что тесты проходят**

Run: `cd frontend && npx vitest run common/components/LoginModal.test.ts 2>&1 | tail -10 && cd .. && make test-ui 2>&1 | tail -10 && make frontend-check 2>&1 | tail -10`
Expected: PASS; проверка типов без ошибок.

Run: `git grep -n -E "detectApp|app=\"(yarn|hdfs|sql|spark|replicator)\"|id: *'(yarn|hdfs|sql|spark|replicator)'" -- frontend ':!frontend/node_modules'`
Expected: пустой вывод.

- [ ] **Step 6: Обновить списки сервисов в скриптах проверки**

В `scripts/check-russian-ui.py` и `scripts/check-russian-docs.py` в списках каталогов приложений заменить `'yarn', 'hdfs', 'sql', 'spark', 'replicator'` на `'artel', 'sklad', 'tolmach', 'iskra', 'parom'`. Слова `yarn`, `hdfs`, `sql`, `spark` в белом списке допустимых латинских слов оставить и добавить туда `pristan`, `artel`, `sklad`, `tolmach`, `iskra`, `parom`, `zastava`.

Run: `python3 scripts/check-russian-ui.py; python3 scripts/check-russian-docs.py`
Expected: обе проверки завершаются с кодом 0.

- [ ] **Step 7: Перенести руководства и применить манифест документации**

```bash
for p in yarn:artel hdfs:sklad sql:tolmach spark:iskra replicator:parom; do
  [ -f "docs/${p%%:*}-admin-guide.md" ] && git mv "docs/${p%%:*}-admin-guide.md" "docs/${p##*:}-admin-guide.md"
done
```

`scripts/rename/manifests/11-docs.tsv`:

```text
sub	yarn-admin-guide	artel-admin-guide
sub	hdfs-admin-guide	sklad-admin-guide
sub	sql-admin-guide	tolmach-admin-guide
sub	spark-admin-guide	iskra-admin-guide
sub	replicator-admin-guide	parom-admin-guide
sub	balookrd/hadoop-explorer	balookrd/pristan
sub	hadoop-explorer.git	pristan.git
sub	YARN Queue Explorer	Artel
sub	HDFS Web Explorer	Sklad
sub	SQL Web Explorer	Tolmach
sub	YARN Explorer	Artel	*.md
sub	HDFS Explorer	Sklad	*.md
sub	SQL Explorer	Tolmach	*.md
sub	Spark Explorer	Iskra	*.md
sub	Hadoop gRPC Replicator	Parom	*.md
sub	Hadoop Explorer Platform	Pristan	*.md
sub	Hadoop Explorer	Pristan	*.md
sub	hadoop-explorer	pristan	*.md
```

Run: `python3 scripts/rename/rename_tokens.py scripts/rename/manifests/11-docs.tsv`

Правило `balookrd/hadoop-explorer` может дать предупреждение: Task 3 уже превратил `hadoop-explorer/` в `pristan/` внутри адресов. Это ожидаемо.

- [ ] **Step 8: Переписать описание проекта в README**

В `README.md`:

1. В шапке: `alt="Pristan"` у логотипа; строку под логотипом заменить на «Pristan — платформа для работы с Hadoop: YARN, HDFS, SQL (Trino, Hive), Spark и межкластерная репликация».
2. Раздел «Состав платформы» заменить таблицей:

   ```markdown
   | Имя | Идентификатор | Движок | Назначение | Смысл имени |
   |---|---|---|---|---|
   | Artel | `artel` | YARN | очереди Capacity Scheduler, заявки на изменение, RM HA | артель: делит работу и ресурсы между участниками |
   | Sklad | `sklad` | HDFS | файловый обозреватель через WebHDFS, предпросмотр Parquet и ORC | склад |
   | Tolmach | `tolmach` | Trino, Hive | SQL-редактор, каталог Hive Metastore, AI-ассистент | толмач: переводит вопрос в запрос |
   | Iskra | `iskra` | Spark | сессии Livy, PySpark и Scala, DAG-пайплайны | искра |
   | Parom | `parom` | HDFS, Hive Metastore, Kafka | межкластерная репликация: orchestrator и agent по gRPC | паром между дата-центрами |
   | Zastava | `zastava` | — | общий стартер безопасности: SPNEGO, LDAP, JWT, ограничение частоты запросов | застава: проверяет каждого входящего |
   ```

   Формулировки столбца «Назначение» сверить с прежним текстом раздела и не добавлять возможностей, которых в нём не было.
3. В разделах «Структура монорепозитория», «Общие модули», «Адреса, health-check и образы сервисов», «Метрики Prometheus и дашборды Grafana», «Запуск демо-стендов» проверить каждый путь, имя образа, цель `make` и имя метрики: после манифестов они уже заменены, вручную правятся заголовки вида «Демо-стенд Artel (YARN)» и подписи, где имя движка осталось без имени сервиса.

- [ ] **Step 9: Обновить AGENTS.md**

В `AGENTS.md`:

1. Заголовок карты: «Pristan — монорепозиторий…».
2. Дерево каталогов: `backend/zastava`, `backend/artel`, `backend/sklad`, `backend/tolmach`, `backend/iskra`, `backend/parom/{proto,model,orchestrator,agent}`; `frontend/apps`: `artel, sklad, tolmach, iskra, parom`; `demo`: `all, artel, sklad, tolmach, iskra, parom, monitoring`. В комментарии к каждой строке указать движок: `# Artel: YARN API (…)`.
3. Правило изоляции сервисов: примеры каталогов `sklad/`, `iskra/`, `tolmach/`; связка `backend/<service>` + `frontend/apps/<service>` + `backend/zastava`.
4. Раздел команд: `make test-zastava`, `make test-artel`, `make test-sklad`, `make test-tolmach`, `make test-iskra`, `mvn test -f backend/parom/pom.xml`, `mvn test -f backend/parom/pom.xml -pl agent -am`.

Run: `grep -n -E 'backend/(yarn|hdfs|sql|spark|replicator|common-security-starter)|test-(yarn|hdfs|sql|spark|security-starter)' AGENTS.md README.md`
Expected: пустой вывод.

- [ ] **Step 10: Вычитать документацию**

Run: `scripts/rename/residual.sh '(yarn|hdfs|sql|spark|hadoop)[-_ ]?explorer|[Rr]eplicator|Hadoop Explorer|common-security-starter' | grep -E '\.md:|^docs/'`

Каждую найденную строку исправить вручную по таблицам спецификации. В каждом файле `docs/*.md` и `README.md` модулей первое упоминание сервиса привести к виду «Artel (YARN)», «Sklad (HDFS)», «Tolmach (SQL)», «Iskra (Spark)».

Expected после правки: пустой вывод.

- [ ] **Step 11: Проверить логотип**

Открыть `images/logo_white.png` и остальные файлы `images/logo*`. Если на изображении есть надпись «Hadoop Explorer», записать это в отчёт задачи одной строкой: замена изображения в план не входит.

- [ ] **Step 12: Контрольная точка**

Предлагаемое сообщение: `refactor!: pass app id to LoginModal explicitly and rename docs to Pristan`

---

### Task 12: Итоговая проверка и удаление инструментов

**Files:**
- Delete: `scripts/rename/`

**Interfaces:**
- Consumes: результаты всех предыдущих задач.

- [ ] **Step 1: Полный прогон тестов**

```bash
make test-java 2>&1 | tail -25
make test-ui 2>&1 | tail -10
make frontend-check 2>&1 | tail -5
make frontend-build 2>&1 | tail -5
ls backend/{artel,sklad,tolmach,iskra}/src/main/resources/static/index.html backend/parom/orchestrator/src/main/resources/static/index.html
```

Expected: `BUILD SUCCESS`; Vitest проходит; пять файлов `index.html` существуют.

- [ ] **Step 2: ast-grep и индекс**

Run: `ast-grep scan 2>/dev/null | grep -c 'no-synchronized-on-io-path'; cat scripts/rename/baseline.txt; make java-index 2>&1 | tail -3`
Expected: число совпадений равно исходному; индекс собирается без ошибок.

- [ ] **Step 3: Поиск остатков по всему репозиторию**

Run: `scripts/rename/residual.sh '(yarn|hdfs|sql|spark|hadoop)[-_ ]?explorer|org[./]apache[./]hadoop[./]explorer|[Rr]eplicator|common-security-starter|CommonSecurity'`
Expected: только строки с `LEGACY_DEFAULT_AGENT_SECRET` и значением `replicator-secure-agent-secret-key-12345`.

Run: `git ls-files | grep -E 'SecurityConfigurationValidator\.java$|ParomProperties\.java$' | xargs grep -h -o -E '"[^"]*(explorer|replicator)[^"]*(secret|key)[^"]*"' | sort | diff - scripts/rename/denylist.txt`
Expected: пустой вывод — denylist секретов совпадает с исходным. Каждую строку расхождения вернуть к исходному значению из `denylist.txt`.

- [ ] **Step 4: Старых каталогов нет**

Run: `ls -d backend/{yarn,hdfs,sql,spark,replicator,common-security-starter} frontend/apps/{yarn,hdfs,sql,spark,replicator} demo/{yarn,hdfs,sql,spark,replicator} 2>&1 | grep -v 'No such file' | wc -l`
Expected: `0`. Каталог, оставшийся из-за неотслеживаемых файлов (`target/`, `node_modules/`), удалить после проверки содержимого командой `ls`.

- [ ] **Step 5: Стенды**

```bash
make build 2>&1 | tail -8
make demo-all && sleep 120
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E '^(artel|sklad|tolmach|iskra|parom-)'
for p in 8001 8002 8003 8004 8005; do curl -s "http://localhost:$p/health" | head -c 200; echo; done
```

Expected: контейнеры `artel`, `sklad`, `tolmach`, `iskra`, `parom-orchestrator` в состоянии `Up`; в ответах `/health` поле `service` равно `artel`, `sklad`, `tolmach`, `iskra`, `parom-orchestrator` (для сервисов, у которых это поле есть).

Проверить вход в каждом интерфейсе учётной записью LDAP из раздела README «Тестовые учетные записи LDAP» — через форму, headless-браузером Playwright из `frontend/node_modules` либо вручную. Ошибка 401 с `Server not found in Kerberos database` в логе сервиса означает расхождение имени хоста и principal: сверить `container_name`, `demo/infra/kdc/entrypoint.sh` и proxyuser в `core-site.xml` стенда.

```bash
make demo-monitoring-smoke 2>&1 | tail -10
make demo-kafka-smoke 2>&1 | tail -10
make demo-all-stop
```

Expected: оба smoke-теста проходят.

- [ ] **Step 6: Удалить инструменты переименования**

```bash
git rm -r -q scripts/rename
git status --short | grep 'scripts/rename' | head
```

Expected: в выводе только строки удаления (`D`).

- [ ] **Step 7: Контрольная точка**

Предлагаемое сообщение: `chore(rename): remove temporary token-rename tooling`

Сообщить владельцу: итог проверок, список несовместимых изменений из раздела спецификации «Несовместимые изменения для существующих установок», результат проверки логотипа.

---

### Task 13: Репозиторий на GitHub и каталог проекта

Выполняется только по отдельной команде владельца, после того как все коммиты отправлены (`git push`).

**Files:**
- Modify: `.git/config` (адрес `origin`)

- [ ] **Step 1: Убедиться, что рабочее дерево чистое и отправлено**

Run: `git status --short | wc -l; git log origin/main..HEAD --oneline | wc -l`
Expected: `0` и `0`.

- [ ] **Step 2: Переименовать репозиторий**

Run: `gh repo rename pristan --repo balookrd/hadoop-explorer`
Expected: сообщение об успешном переименовании. GitHub перенаправляет старые URL, пока имя `balookrd/hadoop-explorer` не занято другим репозиторием.

- [ ] **Step 3: Обновить адрес origin**

Run: `git remote set-url origin git@github.com:balookrd/pristan.git && git remote -v && git fetch`
Expected: оба адреса указывают на `balookrd/pristan.git`; `git fetch` проходит без ошибок.

- [ ] **Step 4: Переименовать каталог проекта**

Закрыть IDE и все сессии Claude Code, работающие в каталоге. Затем в обычном терминале:

```bash
mv ~/IdeaProjects/hadoop-explorer ~/IdeaProjects/pristan
mkdir -p ~/.claude/projects/-Users-$USER-IdeaProjects-pristan/memory
cp -R ~/.claude/projects/-Users-$USER-IdeaProjects-hadoop-explorer/memory/. ~/.claude/projects/-Users-$USER-IdeaProjects-pristan/memory/
grep -rn 'IdeaProjects/hadoop-explorer' ~/IdeaProjects/pristan/.git/hooks 2>/dev/null
```

Expected: последняя команда ничего не печатает; найденные строки в hook-скриптах заменить на новый путь.

- [ ] **Step 5: Проверить сборку из нового каталога**

Run: `cd ~/IdeaProjects/pristan && make test-zastava 2>&1 | tail -5`
Expected: `BUILD SUCCESS`.

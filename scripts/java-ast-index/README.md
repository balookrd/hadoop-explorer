# ⚡ Легковесный AST-индексатор Java и регламенты Token Efficiency

Автономная инфраструктура синтаксической индексации и структурного поиска Java-кода для разработчиков и ИИ-агентов без добавления внешних библиотек в сборочные файлы `pom.xml` или `build.gradle`.

---

## 1. Назначение и архитектура

В больших Java- и монорепозиториях чтение исходных файлов целиком приводит к перерасходу контекста (Context Window Bloat) и тратит десятки тысяч токенов. 

**Java AST-индексатор решает эту задачу:**
- **Zero-Dependency**: использует исключительно стандартный Java Compiler API (`com.sun.source.tree.*`, `com.sun.source.util.*`, `javax.tools.*`) из JDK (Java 17+).
- **Высокая производительность**: синтаксический AST-парсинг репозитория (70+ файлов, 1000+ символов) выполняется менее чем за 1 секунду.
- **Поддержка многомодульных проектов**: автоматически сканирует все модули `src/main/java` и `src/test/java`, игнорируя каталоги сборки (`target/`, `build/`, `.gradle/`, `.git/`, `.idea/`).
- **Изоляция артефактов**: сохраняет индекс в директорию сборки (`target/java-ast-index` или `build/java-ast-index`), не засоряя Git.
- **Экономия токенов (Token Efficiency)**: ИИ-агент выполняет точечный запрос через CLI (`class`, `method`, `tests`) и получает точные координаты (файл, строка, сигнатура) с расходом менее 100 токенов вместо тысяч.

---

## 2. Структура сгенерированного индекса

Индекс сохраняется в `target/java-ast-index/` (или `build/java-ast-index/`) и содержит:

| Файл | Описание | Колонки |
|------|----------|---------|
| `java-symbols.json` | Полный JSON-индекс с метаданными, классами, полями, методами, вызовами и тестами | JSON-структура |
| `symbols.tsv` | Плоский реестр всех символов (классы, поля, методы, константы) | `kind`, `name`, `qualifiedName`, `owner`, `path`, `line`, `detail` |
| `classes.tsv` | Срез классов, интерфейсов, enum, record и аннотаций | `path`, `line`, `kind`, `qualifiedName`, `extends`, `implements`, `annotations`, `fields` |
| `methods.tsv` | Срез методов и конструкторов | `path`, `line`, `class`, `method`, `returnType`, `parameters`, `annotations`, `calls`, `news` |
| `calls.tsv` | Агрегированная сводка вызовов методов по всей кодовой базе | `call`, `count`, `sampleReferences` |
| `news.tsv` | Агрегированная сводка инстанцирований (`new Type`) | `new`, `count`, `sampleReferences` |
| `affected-tests.tsv` | Эвристическая привязка тестов к прод-классам | `prodClass`, `testClass`, `testPath`, `reasons` |

---

## 3. Сборка индекса (`build-java-ast-index.sh`)

Скрипт автоматически определяет установленный JDK (Java 17+), компилирует `ProjectAstIndex.java` в артефакты сборки и генерирует индекс:

```bash
# Сборка в директорию по умолчанию (target/java-ast-index)
./scripts/build-java-ast-index.sh

# Сборка в произвольную директорию
./scripts/build-java-ast-index.sh build/my-index
```

---

## 4. CLI-запросы (`java-index-query.sh`)

Скрипт быстрого поиска с **автоматической инвалидацией**: если любой Java-файл репозитория изменился с момента последней индексации, индекс будет пересобран автоматически перед выполнением запроса.

### Доступные команды:

```bash
# 1. Поиск классов / интерфейсов / records / enums
./scripts/java-index-query.sh class SimpleCircuitBreaker

# 2. Поиск методов по имени
./scripts/java-index-query.sh method authenticate

# 3. Поиск полей по имени
./scripts/java-index-query.sh field failureThreshold

# 4. Поиск мест вызова метода
./scripts/java-index-query.sh call recordSuccess

# 5. Поиск мест создания объектов (new <Type>)
./scripts/java-index-query.sh new UserSession

# 6. Поиск затронутых тестов для прод-класса
./scripts/java-index-query.sh tests SimpleCircuitBreaker

# 7. Общий поиск по всем символам
./scripts/java-index-query.sh symbol JwtToken

# 8. Статистика индекса
./scripts/java-index-query.sh stats
```

---

## 5. Прямой поиск через `rg` (ripgrep) и `awk`

Благодаря плоскому формату TSV, запросы можно выполнять напрямую через потоковые утилиты:

```bash
# Поиск всех классов, реализующих определенный интерфейс
awk -F'\t' '$6 ~ /SessionStore/ { print $4, "->", $1 ":" $2 }' target/java-ast-index/classes.tsv

# Поиск методов, возвращающих ResponseEntity
awk -F'\t' '$5 ~ /ResponseEntity/ { print $3 "#" $4, "->", $1 ":" $2 }' target/java-ast-index/methods.tsv

# Поиск классов с аннотацией @RestController
rg "RestController" target/java-ast-index/classes.tsv

# Топ-10 самых часто создаваемых классов через new
sort -t$'\t' -k2,2nr target/java-ast-index/news.tsv | head -n 10
```

---

## 6. Makefile команды

В корневой `Makefile` добавлены команды:
- `make java-index` — принудительная пересборка Java AST-индекса.
- `make java-query Q="class Auth"` — быстрый структурный запрос.

# AST-индекс Java-кода

Синтаксический индекс и структурный поиск по Java-коду репозитория для разработчиков и ИИ-агентов. Внешние библиотеки в `pom.xml` или `build.gradle` не добавляются.

---

## Назначение и устройство

Чтение исходных файлов целиком расходует контекст ИИ-агента: один класс — тысячи токенов. Индекс отвечает на запрос `class`, `method`, `tests` строкой с файлом, номером строки и сигнатурой — меньше 100 токенов.

- Зависимости: только стандартный Java Compiler API из JDK 17+ (`com.sun.source.tree.*`, `com.sun.source.util.*`, `javax.tools.*`).
- Скорость: полная пересборка индекса (601 Java-файл, 10 847 символов) командой `./scripts/build-java-ast-index.sh` занимает около 1,6 с вместе со стартом JVM (замер 2026-11-06, Apple M3 Pro).
- Многомодульные проекты: сканируются все `src/main/java` и `src/test/java`; каталоги `target/`, `build/`, `.gradle/`, `.git/`, `.idea/` пропускаются.
- Индекс пишется в каталог сборки (`target/java-ast-index` или `build/java-ast-index`) и не попадает в Git.

---

## Файлы индекса

Индекс сохраняется в `target/java-ast-index/` (или `build/java-ast-index/`) и содержит:

| Файл | Содержимое | Колонки |
|------|----------|---------|
| `java-symbols.json` | JSON-индекс: метаданные, классы, поля, методы, вызовы, тесты | JSON-структура |
| `symbols.tsv` | Все символы (классы, поля, методы, константы) | `kind`, `name`, `qualifiedName`, `owner`, `path`, `line`, `detail` |
| `classes.tsv` | Срез классов, интерфейсов, enum, record и аннотаций | `path`, `line`, `kind`, `qualifiedName`, `extends`, `implements`, `annotations`, `fields` |
| `methods.tsv` | Срез методов и конструкторов | `path`, `line`, `class`, `method`, `returnType`, `parameters`, `annotations`, `calls`, `news` |
| `calls.tsv` | Число вызовов каждого метода по всей кодовой базе | `call`, `count`, `sampleReferences` |
| `news.tsv` | Число созданий экземпляров (`new Type`) по типам | `new`, `count`, `sampleReferences` |
| `affected-tests.tsv` | Привязка тестов к прод-классам по эвристикам | `prodClass`, `testClass`, `testPath`, `reasons` |

---

## Сборка индекса (`build-java-ast-index.sh`)

Скрипт находит установленный JDK 17+, компилирует `ProjectAstIndex.java` в каталог сборки и строит индекс:

```bash
# Сборка в директорию по умолчанию (target/java-ast-index)
./scripts/build-java-ast-index.sh

# Сборка в произвольную директорию
./scripts/build-java-ast-index.sh build/my-index
```

---

## Запросы к индексу (`java-index-query.sh`)

Если какой-либо Java-файл репозитория изменился после последней индексации, скрипт пересобирает индекс перед выполнением запроса.

### Команды

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

## Поиск по TSV через `rg` (ripgrep) и `awk`

Файлы TSV плоские, поэтому их можно разбирать потоковыми утилитами:

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

## Цели Makefile

В корневом `Makefile`:
- `make java-index` — пересборка индекса без проверки изменений.
- `make java-query Q="class Auth"` — запрос к индексу, аргумент `Q` передается в `java-index-query.sh`.

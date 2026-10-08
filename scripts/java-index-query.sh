#!/usr/bin/env bash
# ==============================================================================
# CLI-инструмент быстрых структурных запросов по Java AST-индексу
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Определение выходной директории
if [ -d "$REPO_ROOT/target/java-ast-index" ]; then
    INDEX_DIR="$REPO_ROOT/target/java-ast-index"
elif [ -d "$REPO_ROOT/build/java-ast-index" ]; then
    INDEX_DIR="$REPO_ROOT/build/java-ast-index"
else
    INDEX_DIR="$REPO_ROOT/target/java-ast-index"
fi

INDEX_FILE="$INDEX_DIR/java-symbols.json"
SYMBOLS_TSV="$INDEX_DIR/symbols.tsv"
CLASSES_TSV="$INDEX_DIR/classes.tsv"
METHODS_TSV="$INDEX_DIR/methods.tsv"
CALLS_TSV="$INDEX_DIR/calls.tsv"
NEWS_TSV="$INDEX_DIR/news.tsv"
TESTS_TSV="$INDEX_DIR/affected-tests.tsv"

# Определение утилиты поиска (rg или grep fallback)
if command -v rg >/dev/null 2>&1; then
    SEARCH_CMD="rg"
elif [ -x "/opt/homebrew/bin/rg" ]; then
    SEARCH_CMD="/opt/homebrew/bin/rg"
else
    SEARCH_CMD="grep -E"
fi

# Проверка актуальности индекса и авто-пересборка
ensure_index_fresh() {
    local needs_build=0
    if [ ! -f "$INDEX_FILE" ] || [ ! -f "$SYMBOLS_TSV" ]; then
        needs_build=1
    else
        # Проверяем, есть ли файлы Java новее файла индекса
        local newer_file
        newer_file="$(find "$REPO_ROOT" -type f -name "*.java" \
            -not -path "*/target/*" \
            -not -path "*/build/*" \
            -not -path "*/.gradle/*" \
            -not -path "*/.idea/*" \
            -not -path "*/.git/*" \
            -not -path "*/scripts/*" \
            -newer "$INDEX_FILE" 2>/dev/null | head -n 1 || true)"
        if [ -n "$newer_file" ]; then
            needs_build=1
        fi
    fi

    if [ "$needs_build" -eq 1 ]; then
        echo "⚡ [java-query] Индекс устарел или отсутствует. Авто-пересборка..." >&2
        "$SCRIPT_DIR/build-java-ast-index.sh" "$INDEX_DIR" >&2
    fi
}

show_help() {
    cat << 'EOF'
🔍 Использование: ./scripts/java-index-query.sh <команда> <аргумент>

Команды:
  class  <имя>       Поиск классов, интерфейсов, records, enums (по имени или подстроке)
  method <имя>       Поиск методов по имени
  field  <имя>       Поиск полей по имени
  call   <имя>       Поиск мест вызова метода (сводка вызовов и референсов)
  new    <тип>       Поиск мест создания экземпляров (new <Type>)
  tests  <класс>     Поиск потенциально затронутых тестов для класса
  symbol <имя>       Общий поиск по всем символам (классы, поля, методы)
  stats              Статистика индекса репозитория

Примеры:
  ./scripts/java-index-query.sh class SimpleCircuitBreaker
  ./scripts/java-index-query.sh method authenticate
  ./scripts/java-index-query.sh tests SimpleCircuitBreaker
  ./scripts/java-index-query.sh call recordSuccess
  ./scripts/java-index-query.sh new UserSession
EOF
}

cmd="${1:-}"
query="${2:-}"

if [ -z "$cmd" ] || [ "$cmd" = "-h" ] || [ "$cmd" = "--help" ] || [ "$cmd" = "help" ]; then
    show_help
    exit 0
fi

ensure_index_fresh

case "$cmd" in
    class)
        if [ -z "$query" ]; then
            echo "Укажите имя класса: ./scripts/java-index-query.sh class <ClassName>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            tolower($4) ~ tolower(q) || tolower($3) ~ tolower(q) {
                count++
                printf "🏷️  [%s] %s\n", $3, $4
                printf "   📍 %s:%s\n", $1, $2
                if ($5 != "-") printf "   extends: %s\n", $5
                if ($6 != "-") printf "   implements: %s\n", $6
                if ($7 != "-") printf "   annotations: %s\n", $7
                if ($8 != "-") printf "   fields: %s\n", $8
                printf "\n"
            }
            END {
                if (count == 0) printf "❌ Класс \"%s\" не найден в индексе.\n", q
                else printf "✅ Найдено совпадений: %d\n", count
            }
        ' "$CLASSES_TSV"
        ;;

    method)
        if [ -z "$query" ]; then
            echo "Укажите имя метода: ./scripts/java-index-query.sh method <methodName>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            tolower($4) ~ tolower(q) {
                count++
                printf "⚡ %s#%s(%s) -> %s\n", $3, $4, ($6 == "-" ? "" : $6), $5
                printf "   📍 %s:%s\n", $1, $2
                if ($7 != "-") printf "   annotations: %s\n", $7
                if ($8 != "-") printf "   calls: %s\n", $8
                if ($9 != "-") printf "   news:  %s\n", $9
                printf "\n"
            }
            END {
                if (count == 0) printf "❌ Метод \"%s\" не найден в индексе.\n", q
                else printf "✅ Найдено методов: %d\n", count
            }
        ' "$METHODS_TSV"
        ;;

    field)
        if [ -z "$query" ]; then
            echo "Укажите имя поля: ./scripts/java-index-query.sh field <fieldName>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            ($1 == "FIELD" || $1 == "CONSTANT") && tolower($2) ~ tolower(q) {
                count++
                printf "📦 [%s] %s : %s\n", $1, $3, $7
                printf "   📍 %s:%s (owner: %s)\n\n", $5, $6, $4
            }
            END {
                if (count == 0) printf "❌ Поле \"%s\" не найдено в индексе.\n", q
                else printf "✅ Найдено полей: %d\n", count
            }
        ' "$SYMBOLS_TSV"
        ;;

    call)
        if [ -z "$query" ]; then
            echo "Укажите имя вызова: ./scripts/java-index-query.sh call <methodName>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            tolower($1) == tolower(q) || tolower($1) ~ tolower(q) {
                count++
                printf "📞 Вызов: %s (всего: %s)\n", $1, $2
                printf "   Примеры мест: %s\n\n", $3
            }
            END {
                if (count == 0) printf "❌ Вызовы \"%s\" не найдены в индексе.\n", q
            }
        ' "$CALLS_TSV"
        ;;

    new)
        if [ -z "$query" ]; then
            echo "Укажите тип: ./scripts/java-index-query.sh new <TypeName>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            tolower($1) == tolower(q) || tolower($1) ~ tolower(q) {
                count++
                printf "🔨 new %s (созданий: %s)\n", $1, $2
                printf "   Примеры мест: %s\n\n", $3
            }
            END {
                if (count == 0) printf "❌ Создания экземпляров \"%s\" не найдены в индексе.\n", q
            }
        ' "$NEWS_TSV"
        ;;

    tests)
        if [ -z "$query" ]; then
            echo "Укажите имя класса: ./scripts/java-index-query.sh tests <ClassName>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            tolower($1) ~ tolower(q) || tolower($2) ~ tolower(q) {
                count++
                printf "🧪 Прод-класс: %s\n", $1
                printf "   Тест:       %s\n", $2
                printf "   Путь:       %s\n", $3
                printf "   Причины:    %s\n\n", $4
            }
            END {
                if (count == 0) printf "❌ Связанных тестов для \"%s\" не найдено.\n", q
                else printf "✅ Найдено затронутых тестов: %d\n", count
            }
        ' "$TESTS_TSV"
        ;;

    symbol)
        if [ -z "$query" ]; then
            echo "Укажите имя символа: ./scripts/java-index-query.sh symbol <name>" >&2
            exit 1
        fi
        awk -F'\t' -v q="$query" '
            NR == 1 { next }
            tolower($2) ~ tolower(q) || tolower($3) ~ tolower(q) {
                count++
                printf "🔹 [%s] %s\n", $1, $3
                printf "   📍 %s:%s | %s\n\n", $5, $6, $7
            }
            END {
                if (count == 0) printf "❌ Символ \"%s\" не найден в индексе.\n", q
                else printf "✅ Найдено символов: %d\n", count
            }
        ' "$SYMBOLS_TSV"
        ;;

    stats|summary)
        if [ -f "$INDEX_FILE" ]; then
            echo "📊 Статистика AST-индекса Java ($INDEX_DIR):"
            grep -A 10 '"stats":' "$INDEX_FILE" || head -n 25 "$INDEX_FILE"
        else
            echo "❌ Индекс не найден." >&2
            exit 1
        fi
        ;;

    *)
        echo "❌ Неизвестная команда: $cmd" >&2
        show_help
        exit 1
        ;;
esac

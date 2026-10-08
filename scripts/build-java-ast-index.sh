#!/usr/bin/env bash
# ==============================================================================
# Скрипт сборки и запуска генератора Java AST-индекса (Hadoop Explorer Platform)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Определение выходной директории
if [ -n "${1:-}" ]; then
    INDEX_DIR="$1"
elif [ -f "$REPO_ROOT/pom.xml" ] || [ -d "$REPO_ROOT/backend/common-security-starter" ]; then
    INDEX_DIR="$REPO_ROOT/target/java-ast-index"
elif [ -f "$REPO_ROOT/build.gradle" ] || [ -f "$REPO_ROOT/build.gradle.kts" ]; then
    INDEX_DIR="$REPO_ROOT/build/java-ast-index"
else
    INDEX_DIR="$REPO_ROOT/target/java-ast-index"
fi

OUTPUT_JSON="$INDEX_DIR/java-symbols.json"
CLASSES_DIR="$INDEX_DIR/.classes"

# Поиск JDK / javac / java
find_jdk() {
    # 1. Проверка уже заданного JAVA_HOME
    if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/javac" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        echo "$JAVA_HOME"
        return 0
    fi

    # 2. Проверка стандартного пути brew на macOS (как в Makefile)
    if [ -d "/opt/homebrew/opt/openjdk" ] && [ -x "/opt/homebrew/opt/openjdk/bin/javac" ]; then
        echo "/opt/homebrew/opt/openjdk"
        return 0
    fi

    # 3. Проверка через macOS /usr/libexec/java_home
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        local jh
        jh="$(/usr/libexec/java_home 2>/dev/null || true)"
        if [ -n "$jh" ] && [ -x "$jh/bin/javac" ]; then
            echo "$jh"
            return 0
        fi
    fi

    # 4. Проверка javac в PATH
    if command -v javac >/dev/null 2>&1 && command -v java >/dev/null 2>&1; then
        local javac_path
        javac_path="$(command -v javac)"
        # Пытаемся восстановить JAVA_HOME из пути к javac
        local bin_dir
        bin_dir="$(dirname "$javac_path")"
        local parent_dir
        parent_dir="$(dirname "$bin_dir")"
        if [ -x "$parent_dir/bin/javac" ] && [ -x "$parent_dir/bin/java" ]; then
            echo "$parent_dir"
            return 0
        fi
    fi

    # 5. Другие популярные пути установки
    local candidates=(
        "/opt/homebrew/opt/openjdk@21"
        "/opt/homebrew/opt/openjdk@17"
        "/Library/Java/JavaVirtualMachines"/*/Contents/Home
        "/usr/lib/jvm/default-java"
        "/usr/lib/jvm/java-21-openjdk"
        "/usr/lib/jvm/java-17-openjdk"
        "$HOME/.sdkman/candidates/java/current"
    )

    for cand in "${candidates[@]}"; do
        if [ -d "$cand" ] && [ -x "$cand/bin/javac" ] && [ -x "$cand/bin/java" ]; then
            echo "$cand"
            return 0
        fi
    done

    return 1
}

JDK_DIR="$(find_jdk || true)"
if [ -z "$JDK_DIR" ]; then
    echo "❌ Ошибка: Не найден JDK (требуется Java 17+ с javac)." >&2
    echo "Установите JDK или задайте переменную окружения JAVA_HOME." >&2
    exit 1
fi

export JAVA_HOME="$JDK_DIR"
JAVAC_BIN="$JAVA_HOME/bin/javac"
JAVA_BIN="$JAVA_HOME/bin/java"

mkdir -p "$CLASSES_DIR"

SOURCE_FILE="$SCRIPT_DIR/java-ast-index/ProjectAstIndex.java"
if [ ! -f "$SOURCE_FILE" ]; then
    echo "❌ Исходный файл $SOURCE_FILE не найден." >&2
    exit 1
fi

echo "🔨 [java-ast-index] Компиляция ProjectAstIndex.java..."
"$JAVAC_BIN" -d "$CLASSES_DIR" "$SOURCE_FILE"

echo "🚀 [java-ast-index] Генерация AST-индекса..."
START_TIME=$(date +%s)
"$JAVA_BIN" -cp "$CLASSES_DIR" ProjectAstIndex "$REPO_ROOT" "$OUTPUT_JSON"
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "✨ [java-ast-index] Готово за ${ELAPSED} сек! Индекс сохранен в $INDEX_DIR"

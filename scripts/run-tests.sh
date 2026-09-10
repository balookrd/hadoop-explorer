#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

find_pytest() {
  if [ -n "$VIRTUAL_ENV" ] && [ -x "$VIRTUAL_ENV/bin/pytest" ]; then
    echo "$VIRTUAL_ENV/bin/pytest"
  elif [ -x "$ROOT_DIR/.venv/bin/pytest" ]; then
    echo "$ROOT_DIR/.venv/bin/pytest"
  elif [ -x "$ROOT_DIR/backend/.venv/bin/pytest" ]; then
    echo "$ROOT_DIR/backend/.venv/bin/pytest"
  elif [ -x "$ROOT_DIR/backend/venv/bin/pytest" ]; then
    echo "$ROOT_DIR/backend/venv/bin/pytest"
  elif command -v uv >/dev/null 2>&1; then
    echo "uv run --project $ROOT_DIR pytest"
  else
    which pytest 2>/dev/null || echo "pytest"
  fi
}

APP="${1:-all}"

run_yarn() {
  echo "=========================================="
  echo "🧪 Запуск тестов: YARN Explorer (65 тестов)"
  echo "=========================================="
  local pt
  pt="$(find_pytest yarn)"
  (cd "$ROOT_DIR/backend/yarn" && \
   CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   $pt tests)
}

run_hdfs() {
  echo "=========================================="
  echo "🧪 Запуск тестов: HDFS Explorer (86 тестов)"
  echo "=========================================="
  local pt
  pt="$(find_pytest hdfs)"
  (cd "$ROOT_DIR/backend/hdfs" && \
   HDFS_CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   $pt tests)
}

run_sql() {
  echo "=========================================="
  echo "🧪 Запуск тестов: SQL Explorer (42 теста)"
  echo "=========================================="
  local pt
  pt="$(find_pytest sql)"
  (cd "$ROOT_DIR/backend/sql" && \
   CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   $pt tests)
}

run_spark() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Spark Explorer (24 теста)"
  echo "=========================================="
  local pt
  pt="$(find_pytest spark)"
  (cd "$ROOT_DIR/backend/spark" && \
   CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   $pt tests)
}

run_frontend() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Frontend Static Check & UI Suite (23 теста)"
  echo "=========================================="
  (cd "$ROOT_DIR/frontend" && npm run test:ui)
}

case "$APP" in
  frontend)
    run_frontend
    ;;
  yarn)
    run_yarn
    ;;
  hdfs)
    run_hdfs
    ;;
  sql)
    run_sql
    ;;
  spark)
    run_spark
    ;;
  all)
    run_frontend
    run_yarn
    run_hdfs
    run_sql
    run_spark
    echo ""
    echo "========================================================"
    echo "🎉 ВСЕ ТЕСТЫ ПЛАТФОРМЫ HADOOP EXPLORER ПРОЙДЕНЫ УСПЕШНО!"
    echo "========================================================"
    ;;
  *)
    echo "Использование: $0 [all|frontend|hdfs|spark|sql|yarn]"
    exit 1
    ;;
esac

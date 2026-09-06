#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

find_pytest() {
  local service="$1"
  if [ -x "$ROOT_DIR/backend/venv/bin/pytest" ]; then
    echo "$ROOT_DIR/backend/venv/bin/pytest"
  elif [ -x "$ROOT_DIR/../$service-explorer/backend/venv/bin/pytest" ]; then
    echo "$ROOT_DIR/../$service-explorer/backend/venv/bin/pytest"
  else
    which pytest 2>/dev/null || echo "pytest"
  fi
}

APP="${1:-all}"

run_hdfs() {
  echo "=========================================="
  echo "🧪 Запуск тестов: HDFS Explorer (39 тестов)"
  echo "=========================================="
  local pt
  pt="$(find_pytest hdfs)"
  (cd "$ROOT_DIR/backend/hdfs" && \
   HDFS_CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   "$pt" tests)
}

run_sql() {
  echo "=========================================="
  echo "🧪 Запуск тестов: SQL Explorer (31 тест)"
  echo "=========================================="
  local pt
  pt="$(find_pytest sql)"
  (cd "$ROOT_DIR/backend/sql" && \
   CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   "$pt" tests)
}

run_yarn() {
  echo "=========================================="
  echo "🧪 Запуск тестов: YARN Explorer (42 теста)"
  echo "=========================================="
  local pt
  pt="$(find_pytest yarn)"
  (cd "$ROOT_DIR/backend/yarn" && \
   CONFIG_PATH=config/config.yaml \
   PYTHONPATH=".:$ROOT_DIR" \
   "$pt" tests)
}

case "$APP" in
  hdfs)
    run_hdfs
    ;;
  sql)
    run_sql
    ;;
  yarn)
    run_yarn
    ;;
  all)
    run_hdfs
    run_sql
    run_yarn
    echo ""
    echo "========================================================"
    echo "🎉 ВСЕ 112 ТЕСТОВ ПЛАТФОРМЫ HADOOP EXPLORER ПРОЙДЕНЫ УСПЕШНО!"
    echo "========================================================"
    ;;
  *)
    echo "Использование: $0 [all|hdfs|sql|yarn]"
    exit 1
    ;;
esac

#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

APP="${1:-all}"

run_artel() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Artel (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/artel/pom.xml"
}

run_sklad() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Sklad (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/sklad/pom.xml"
}

run_sql() {
  echo "=========================================="
  echo "🧪 Запуск тестов: SQL Explorer (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/sql/pom.xml"
}

run_spark() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Spark Explorer (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/spark/pom.xml"
}

run_replicator() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Replicator (Java 21 / Spring Boot 3 & Agent)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/replicator/pom.xml"
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
  artel)
    run_artel
    ;;
  sklad)
    run_sklad
    ;;
  sql)
    run_sql
    ;;
  spark)
    run_spark
    ;;
  replicator)
    run_replicator
    ;;
  all)
    run_frontend
    run_artel
    run_sklad
    run_sql
    run_spark
    run_replicator
    echo ""
    echo "========================================================"
    echo "🎉 ВСЕ ТЕСТЫ ПЛАТФОРМЫ PRISTAN ПРОЙДЕНЫ УСПЕШНО!"
    echo "========================================================"
    ;;
  *)
    echo "Использование: $0 [all|frontend|sklad|spark|sql|artel|replicator]"
    exit 1
    ;;
esac

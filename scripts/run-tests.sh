#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

APP="${1:-all}"

run_yarn() {
  echo "=========================================="
  echo "🧪 Запуск тестов: YARN Explorer (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/yarn/pom.xml"
}

run_hdfs() {
  echo "=========================================="
  echo "🧪 Запуск тестов: HDFS Explorer (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/hdfs/pom.xml"
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
  replicator)
    run_replicator
    ;;
  all)
    run_frontend
    run_yarn
    run_hdfs
    run_sql
    run_spark
    run_replicator
    echo ""
    echo "========================================================"
    echo "🎉 ВСЕ ТЕСТЫ ПЛАТФОРМЫ HADOOP EXPLORER ПРОЙДЕНЫ УСПЕШНО!"
    echo "========================================================"
    ;;
  *)
    echo "Использование: $0 [all|frontend|hdfs|spark|sql|yarn|replicator]"
    exit 1
    ;;
esac

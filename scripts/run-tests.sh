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

run_tolmach() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Tolmach (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/tolmach/pom.xml"
}

run_iskra() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Iskra (Java 21 / Spring Boot 3)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/iskra/pom.xml"
}

run_parom() {
  echo "=========================================="
  echo "🧪 Запуск тестов: Replicator (Java 21 / Spring Boot 3 & Agent)"
  echo "=========================================="
  mvn test -f "$ROOT_DIR/backend/parom/pom.xml"
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
  tolmach)
    run_tolmach
    ;;
  iskra)
    run_iskra
    ;;
  parom)
    run_parom
    ;;
  all)
    run_frontend
    run_artel
    run_sklad
    run_tolmach
    run_iskra
    run_parom
    echo ""
    echo "========================================================"
    echo "🎉 ВСЕ ТЕСТЫ ПЛАТФОРМЫ PRISTAN ПРОЙДЕНЫ УСПЕШНО!"
    echo "========================================================"
    ;;
  *)
    echo "Использование: $0 [all|frontend|sklad|iskra|tolmach|artel|parom]"
    exit 1
    ;;
esac

#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TAG="${TAG:-latest}"
REGISTRY="${REGISTRY:-hadoop-explorer}"

build_app() {
  local app="$1"
  echo "=========================================="
  echo "🐳 Сборка Docker-образа: $REGISTRY/$app:$TAG"
  echo "=========================================="
  docker build -t "$REGISTRY/$app:$TAG" -f "$ROOT_DIR/docker/Dockerfile.$app" "$ROOT_DIR"
  echo " Образ $REGISTRY/$app:$TAG успешно собран!"
}

TARGET="${1:-all}"

case "$TARGET" in
  hdfs)
    build_app hdfs
    ;;
  sql)
    build_app sql
    ;;
  yarn)
    build_app yarn
    ;;
  all)
    build_app hdfs
    build_app sql
    build_app yarn
    echo "=========================================="
    echo "🎉 Все контейнеры платформы успешно собраны!"
    echo "=========================================="
    ;;
  *)
    echo "Использование: $0 [all|hdfs|sql|yarn]"
    exit 1
    ;;
esac

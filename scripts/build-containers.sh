#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TAG="${TAG:-latest}"
REGISTRY="${REGISTRY:-hadoop-explorer}"

build_app() {
  local app="$1"
  local dockerfile="$ROOT_DIR/docker/Dockerfile.${app}-java"
  if [ ! -f "$dockerfile" ]; then
    dockerfile="$ROOT_DIR/docker/Dockerfile.$app"
  fi
  echo "=========================================="
  echo "🐳 Сборка Docker-образа: $REGISTRY/$app:$TAG (Dockerfile: $(basename "$dockerfile"))"
  echo "=========================================="
  docker build -t "$REGISTRY/$app:$TAG" -f "$dockerfile" "$ROOT_DIR"
  echo "✅ Образ $REGISTRY/$app:$TAG успешно собран!"
}

TARGET="${1:-all}"

case "$TARGET" in
  yarn)
    build_app yarn
    ;;
  hdfs)
    build_app hdfs
    ;;
  sql)
    build_app sql
    ;;
  spark)
    build_app spark
    ;;
  replicator)
    build_app replicator-orchestrator
    ;;
  all)
    build_app yarn
    build_app hdfs
    build_app sql
    build_app spark
    build_app replicator-orchestrator
    echo "=========================================="
    echo "🎉 Все контейнеры платформы успешно собраны!"
    echo "=========================================="
    ;;
  *)
    echo "Использование: $0 [all|yarn|hdfs|sql|spark|replicator]"
    exit 1
    ;;
esac

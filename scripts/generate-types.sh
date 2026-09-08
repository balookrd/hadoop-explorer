#!/usr/bin/env bash
set -e

# ==============================================================================
# Скрипт автоматической кодогенерации TypeScript-типов из схем OpenAPI (FastAPI)
# ==============================================================================

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
OUTPUT_DIR="${ROOT_DIR}/frontend/common/types/generated"

mkdir -p "${OUTPUT_DIR}"

cleanup() {
    rm -rf "${TEMP_DIR}"
}
trap cleanup EXIT

echo "=== [1/3] Извлечение OpenAPI схем из backend-сервисов ==="

SERVICES=("hdfs" "spark" "sql" "yarn")

for SVC in "${SERVICES[@]}"; do
    echo " -> Генерация OpenAPI JSON для: ${SVC}"
    PYTHONPATH="${ROOT_DIR}/backend/${SVC}:${ROOT_DIR}" \
    CONFIG_PATH="${ROOT_DIR}/backend/${SVC}/config/config.yaml" \
    DEBUG="true" \
    uv run python -c "
import json
from app.main import app
schema = app.openapi()
with open('${TEMP_DIR}/${SVC}-openapi.json', 'w', encoding='utf-8') as f:
    json.dump(schema, f, indent=2, ensure_ascii=False)
"
done

echo "=== [2/3] Генерация TypeScript-интерфейсов через openapi-typescript ==="

cd "${ROOT_DIR}/frontend"

for SVC in "${SERVICES[@]}"; do
    echo " -> Генерация TypeScript типов: ${OUTPUT_DIR}/${SVC}.ts"
    npx openapi-typescript "${TEMP_DIR}/${SVC}-openapi.json" -o "${OUTPUT_DIR}/${SVC}.ts"
done

echo "=== [3/3] Создание единого index.ts с реэкспортом схем ==="

cat << 'TYPES_INDEX' > "${OUTPUT_DIR}/index.ts"
/**
 * Автоматически сгенерированные TypeScript-типы из OpenAPI схем бэкенда.
 * Не редактируйте данный файл вручную — запускайте `make generate-types`!
 */

export * as HdfsApi from './hdfs';
export * as SparkApi from './spark';
export * as SqlApi from './sql';
export * as YarnApi from './yarn';
TYPES_INDEX

echo "=== Генерация типов успешно завершена! ==="

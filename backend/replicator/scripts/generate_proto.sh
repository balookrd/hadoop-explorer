#!/usr/bin/env bash
set -euo pipefail

# Определение корневой директории репозитория
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPLICATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${REPLICATOR_DIR}/../.." && pwd)"

PROTO_DIR="${REPLICATOR_DIR}/proto"
OUTPUT_DIR="${REPLICATOR_DIR}/generated"

echo "==> Генерация Python gRPC-кода из Protobuf контрактов..."
mkdir -p "${OUTPUT_DIR}"
touch "${OUTPUT_DIR}/__init__.py"

# Запуск компилятора protoc через grpc_tools
uv run python -m grpc_tools.protoc \
    -I "${PROTO_DIR}" \
    --python_out="${OUTPUT_DIR}" \
    --grpc_python_out="${OUTPUT_DIR}" \
    "${PROTO_DIR}/replicator.proto"

# Фикс относительного импорта для Python модулей (совместимость с macOS и Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' -e 's/^import replicator_pb2 as/from . import replicator_pb2 as/g' "${OUTPUT_DIR}/replicator_pb2_grpc.py"
else
    sed -i -e 's/^import replicator_pb2 as/from . import replicator_pb2 as/g' "${OUTPUT_DIR}/replicator_pb2_grpc.py"
fi

echo "==> Кодогенерация успешно завершена:"
ls -lh "${OUTPUT_DIR}"

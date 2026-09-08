# ==========================================
# Этап 1: Сборка Frontend (Svelte 5 + Vite + Monaco)
# ==========================================
FROM node:22-alpine AS frontend-builder
WORKDIR /app/frontend

COPY frontend/package*.json ./
COPY frontend/common ./common
COPY frontend/apps/sql/package*.json ./apps/sql/
RUN npm ci --workspace=apps/sql --include-workspace-root

COPY frontend/apps/sql ./apps/sql
RUN npm run build --workspace=apps/sql

# ==========================================
# Этап 2: Сборка зависимостей Backend
# ==========================================
FROM python:3.12-slim AS backend-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libkrb5-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Кэширование установки внешних зависимостей
COPY backend/common/pyproject.toml /tmp/backend/common/pyproject.toml
COPY backend/sql/pyproject.toml /tmp/backend/sql/pyproject.toml
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        fastapi>=0.115.0 uvicorn[standard]>=0.30.0 pydantic>=2.8.0 pydantic-settings>=2.4.0 \
        pyyaml>=6.0.2 pyjwt>=2.9.0 cryptography>=43.0.0 ldap3>=2.9.1 pyspnego>=0.11.1 \
        requests-kerberos>=0.14.0 sqlalchemy>=2.0.32 redis>=5.0.0 psycopg2-binary>=2.9.9 \
        aiosqlite>=0.20.0 asyncpg>=0.29.0 alembic>=1.13.2 passlib[bcrypt]>=1.7.4 \
        python-multipart>=0.0.9 trino>=0.329.0 impyla>=0.24.0 thrift>=0.16.0 \
        thrift-sasl>=0.4.3 pure-sasl>=0.6.2 httpx>=0.27.0 python-dateutil>=2.9.0 \
        sqlglot>=30.0.0 greenlet>=3.0.0 kerberos>=1.3.0

# Установка пакетов платформы без повторного скачивания зависимостей
COPY backend/common /tmp/backend/common
COPY backend/sql /tmp/backend/sql
RUN pip install --no-cache-dir --no-deps /tmp/backend/common /tmp/backend/sql


# ==========================================
# Этап 3: Финальный образ Backend + Static
# ==========================================
FROM python:3.12-slim

# Системные runtime зависимости (без компиляторов и dev-пакетов)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libkrb5-3 \
    libsasl2-2 \
    libsasl2-modules-gssapi-mit \
    krb5-user \
    ldap-utils \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Копирование виртуального окружения с установленными зависимостями
COPY --from=backend-builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Исходный код сервиса, конфигурация и entrypoint
COPY backend/common ./backend/common
COPY backend/sql ./backend/sql
COPY backend/sql/config ./config
COPY docker/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Копирование собранного Frontend из этапа 1
COPY --from=frontend-builder /app/frontend/apps/sql/dist ./frontend/dist

# Создание непривилегированного пользователя appuser (UID 10001)
RUN groupadd -g 10001 appuser && \
    useradd -u 10001 -g appuser -m -s /bin/bash appuser && \
    mkdir -p /app/data /etc/security/keytabs && \
    touch /etc/krb5.conf && \
    chown -R appuser:appuser /app /etc/krb5.conf /etc/security/keytabs

ENV APP_NAME="sql" \
    PYTHONPATH="/app:/app/backend/common:/app/backend/sql" \
    CONFIG_PATH="/app/config/config.yaml" \
    FRONTEND_DIST="/app/frontend/dist"

USER 10001

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]

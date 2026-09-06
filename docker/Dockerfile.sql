# ==========================================
# Этап 1: Сборка Frontend (Svelte 5 + Vite + Monaco)
# ==========================================
FROM node:22-alpine AS frontend-builder
WORKDIR /app/frontend

COPY frontend/apps/sql/package*.json ./
RUN npm ci

COPY frontend/common /app/frontend/common
COPY frontend/apps/sql/ ./
RUN npm run build

# ==========================================
# Этап 2: Финальный образ Backend + Static
# ==========================================
FROM python:3.12-slim

# Системные зависимости для Kerberos, SASL, LDAP
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libkrb5-dev \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    krb5-user \
    ldap-utils \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Установка Python зависимостей
COPY backend/common/requirements-common.txt ./backend/common/requirements-common.txt
COPY backend/sql/requirements.txt ./backend/sql/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r backend/common/requirements-common.txt && \
    pip install --no-cache-dir -r backend/sql/requirements.txt

# Копирование исходного кода backend
COPY backend/common ./backend/common
COPY backend/sql ./backend/sql
COPY backend/sql/config ./config
COPY backend/sql/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Копирование собранного Frontend из этапа 1
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

# Создание непривилегированного пользователя appuser (UID 10001)
RUN groupadd -g 10001 appuser && \
    useradd -u 10001 -g appuser -m -s /bin/bash appuser && \
    mkdir -p /app/data /etc/security/keytabs && \
    touch /etc/krb5.conf && \
    chown -R appuser:appuser /app /etc/krb5.conf /etc/security/keytabs

ENV PYTHONPATH="/app:/app/backend/common:/app/backend/sql" \
    CONFIG_PATH="/app/config/config.yaml" \
    FRONTEND_DIST="/app/frontend/dist"

USER 10001

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]

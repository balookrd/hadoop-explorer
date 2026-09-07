#!/bin/bash
set -e

APP_NAME="${APP_NAME:-hadoop-explorer}"
echo "[entrypoint] Запуск $APP_NAME контейнера..."

# 1. Проверка наличия и монтирования krb5.conf
if [ -n "$KRB5_CONFIG" ] && [ -f "$KRB5_CONFIG" ]; then
    echo "[entrypoint] Использование KRB5_CONFIG: $KRB5_CONFIG"
    if [ "$KRB5_CONFIG" != "/etc/krb5.conf" ]; then
        cp "$KRB5_CONFIG" /etc/krb5.conf 2>/dev/null || true
    fi
elif [ -s "/etc/krb5.conf" ]; then
    echo "[entrypoint] Обнаружен непустой /etc/krb5.conf"
elif [ -f "/etc/security/keytabs/krb5.conf" ]; then
    echo "[entrypoint] Копирование krb5.conf из /etc/security/keytabs..."
    cp /etc/security/keytabs/krb5.conf /etc/krb5.conf 2>/dev/null || true
elif [ -f "/etc/krb5_shared/krb5.conf" ]; then
    echo "[entrypoint] Копирование krb5.conf из /etc/krb5_shared..."
    cp /etc/krb5_shared/krb5.conf /etc/krb5.conf 2>/dev/null || true
fi

# 2. Автоматическая инициализация Kerberos тикета сервисной учетной записи
KEYTAB="${KRB5_KEYTAB:-/etc/security/keytabs/${APP_NAME}.keytab}"
PRINCIPAL="${KRB5_PRINCIPAL:-${APP_NAME}/${APP_NAME}@EXAMPLE.COM}"

if [ -f "$KEYTAB" ]; then
    echo "[entrypoint] Обнаружен keytab: $KEYTAB"
    echo "[entrypoint] Выполнение kinit для $PRINCIPAL..."
    
    # Пытаемся kinit с повторами (если KDC только стартует)
    MAX_RETRIES=15
    RETRY=0
    until kinit -kt "$KEYTAB" "$PRINCIPAL" 2>/dev/null || [ $RETRY -ge $MAX_RETRIES ]; do
        RETRY=$((RETRY + 1))
        echo "[entrypoint] Ожидание доступности KDC (попытка $RETRY/$MAX_RETRIES)..."
        sleep 2
    done

    if klist -s 2>/dev/null; then
        echo "[entrypoint] Kerberos тикет успешно получен:"
        klist
        
        # Фоновый процесс обновления билета каждые 6 часов
        (
            while true; do
                sleep 21600
                kinit -kt "$KEYTAB" "$PRINCIPAL" 2>/dev/null || true
            done
        ) &
    else
        echo "[entrypoint] ВНИМАНИЕ: Не удалось получить Kerberos билет. Сервис продолжит запуск."
    fi
else
    echo "[entrypoint] Keytab $KEYTAB не найден. Kerberos SSO/GSSAPI кэш не инициализирован."
fi

# 3. Запуск веб-сервера
echo "[entrypoint] Запуск веб-сервера..."
if [ "$#" -gt 0 ]; then
    exec "$@"
else
    # Определение модуля запуска по умолчанию
    MODULE="app.main:app"
    if [ -d "/app/backend/$APP_NAME/app" ]; then
        exec python -m uvicorn app.main:app --app-dir "/app/backend/$APP_NAME" --host 0.0.0.0 --port 8000
    elif [ -d "/app/backend/app" ]; then
        exec python -m uvicorn app.main:app --app-dir "/app/backend" --host 0.0.0.0 --port 8000
    else
        exec python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
    fi
fi

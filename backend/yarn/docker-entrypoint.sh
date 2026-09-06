#!/bin/bash
set -e

echo "[entrypoint] Запуск yarn-explorer контейнера..."

# Проверка наличия и монтирования krb5.conf
if [ -f "/etc/krb5.conf" ]; then
    echo "[entrypoint] Обнаружен /etc/krb5.conf"
elif [ -f "/etc/security/keytabs/krb5.conf" ]; then
    echo "[entrypoint] Копирование krb5.conf из /etc/security/keytabs..."
    cp /etc/security/keytabs/krb5.conf /etc/krb5.conf
fi

# Автоматическая инициализация Kerberos тикета сервисной учетной записи
KEYTAB="${KRB5_KEYTAB:-/etc/security/keytabs/yarn-explorer.keytab}"
PRINCIPAL="${KRB5_PRINCIPAL:-svc_yarn_explorer@COMPANY.LOCAL}"

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

    if klist -s; then
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

echo "[entrypoint] Запуск веб-сервера..."
if [ "$#" -gt 0 ]; then
    exec "$@"
else
    exec python -m uvicorn app.main:app --app-dir backend --host 0.0.0.0 --port 8000
fi

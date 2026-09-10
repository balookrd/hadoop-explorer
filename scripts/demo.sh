#!/usr/bin/env bash
# ==============================================================================
# Единый CLI скрипт управления демонстрационными стендами Hadoop Explorer
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ACTION="${1:-help}"
SERVICE="${2:-all}"

usage() {
    echo "Использование: $0 [start|stop|restart|status|logs] [all|yarn|hdfs|sql|spark|monitoring]"
    echo ""
    echo "Команды:"
    echo "  start [service]    Запустить демонстрационный стенд"
    echo "  stop [service]     Остановить демонстрационный стенд"
    echo "  restart [service]  Перезапустить демонстрационный стенд"
    echo "  status [service]   Показать статус контейнеров"
    echo "  logs [service]     Просмотр логов стенда"
    echo ""
    echo "Сервисы: all (по умолчанию), yarn, hdfs, sql, spark, monitoring"
    exit 1
}

get_compose_file() {
    case "$1" in
        all)
            echo "$ROOT_DIR/demo/all/docker-compose.all.yml"
            ;;
        yarn)
            echo "$ROOT_DIR/demo/yarn/docker-compose.yml"
            ;;
        hdfs)
            echo "$ROOT_DIR/demo/hdfs/docker-compose.yml"
            ;;
        sql)
            echo "$ROOT_DIR/demo/sql/docker-compose.yml"
            ;;
        spark)
            echo "$ROOT_DIR/demo/spark/docker-compose.yml"
            ;;
        monitoring)
            echo "$ROOT_DIR/demo/monitoring/docker-compose.monitoring.yml"
            ;;
        *)
            echo ""
            ;;
    esac
}

COMPOSE_FILE="$(get_compose_file "$SERVICE")"
if [ -z "$COMPOSE_FILE" ]; then
    echo "❌ Ошибка: неизвестный сервис '$SERVICE'"
    usage
fi

case "$ACTION" in
    start)
        if [ "$SERVICE" = "all" ]; then
            "$ROOT_DIR/demo/all/start-all-demo.sh"
        elif [ "$SERVICE" = "monitoring" ]; then
            "$ROOT_DIR/demo/monitoring/start-monitoring.sh"
        elif [ -f "$ROOT_DIR/demo/$SERVICE/start-demo.sh" ]; then
            "$ROOT_DIR/demo/$SERVICE/start-demo.sh"
        else
            docker compose -f "$COMPOSE_FILE" up -d --build
        fi
        ;;
    stop)
        if [ "$SERVICE" = "all" ]; then
            "$ROOT_DIR/demo/all/stop-all-demo.sh"
        elif [ "$SERVICE" = "monitoring" ]; then
            "$ROOT_DIR/demo/monitoring/stop-monitoring.sh"
        elif [ -f "$ROOT_DIR/demo/$SERVICE/stop-demo.sh" ]; then
            "$ROOT_DIR/demo/$SERVICE/stop-demo.sh"
        else
            docker compose -f "$COMPOSE_FILE" down -v
        fi
        ;;
    restart)
        "$0" stop "$SERVICE"
        "$0" start "$SERVICE"
        ;;
    status)
        echo "=== Статус стенда '$SERVICE' ==="
        docker compose -f "$COMPOSE_FILE" ps
        ;;
    logs)
        docker compose -f "$COMPOSE_FILE" logs -f
        ;;
    *)
        usage
        ;;
esac

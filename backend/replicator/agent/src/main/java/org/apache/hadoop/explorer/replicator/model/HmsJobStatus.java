package org.apache.hadoop.explorer.replicator.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Формализованный конечный автомат (FSM) жизненного цикла задачи HMS репликации.
 * Обеспечивает строгий контроль допустимости переходов между состояниями.
 */
public enum HmsJobStatus {

    /**
     * Ожидание подключения требуемых агентов (source и/или target).
     */
    WAITING_FOR_AGENTS,

    /**
     * Задача готова к захвату и исполнению агентом источника.
     */
    QUEUED,

    /**
     * Выполняется первичная репликация схемы, DDL таблиц и партиций.
     */
    BOOTSTRAPPING,

    /**
     * Потоковая инкрементальная синхронизация событий CDC (NOTIFICATION_LOG).
     */
    ACTIVE,

    /**
     * Задача временно приостановлена оператором.
     */
    PAUSED,

    /**
     * Временная ошибка репликации (сбой сети, временная недоступность таргета).
     */
    ERROR,

    /**
     * Фатальный сбой, требующий ручного разбора или повторного Bootstrap.
     */
    FAILED,

    /**
     * Задача окончательно остановлена оператором либо вытеснена при Disaster Recovery.
     */
    STOPPED;

    private static final Map<HmsJobStatus, Set<HmsJobStatus>> ALLOWED_TRANSITIONS = Map.of(
            WAITING_FOR_AGENTS, EnumSet.of(QUEUED, STOPPED, FAILED),
            QUEUED, EnumSet.of(BOOTSTRAPPING, ACTIVE, WAITING_FOR_AGENTS, PAUSED, STOPPED, FAILED),
            BOOTSTRAPPING, EnumSet.of(ACTIVE, ERROR, FAILED, PAUSED, STOPPED, QUEUED),
            ACTIVE, EnumSet.of(PAUSED, ERROR, FAILED, STOPPED, BOOTSTRAPPING, QUEUED),
            PAUSED, EnumSet.of(ACTIVE, BOOTSTRAPPING, QUEUED, STOPPED),
            ERROR, EnumSet.of(ACTIVE, BOOTSTRAPPING, QUEUED, PAUSED, STOPPED, FAILED),
            FAILED, EnumSet.of(QUEUED, BOOTSTRAPPING, STOPPED),
            STOPPED, EnumSet.of(QUEUED, BOOTSTRAPPING)
    );

    /**
     * Проверяет допустимость перехода из текущего состояния в целевое.
     */
    public boolean canTransitionTo(HmsJobStatus target) {
        if (target == null) {
            return false;
        }
        if (this == target) {
            return true; // Переход в то же самое состояние всегда допустим (идемпотентность)
        }
        Set<HmsJobStatus> allowed = ALLOWED_TRANSITIONS.get(this);
        return allowed != null && allowed.contains(target);
    }

    /**
     * Валидирует переход в целевое состояние, выбрасывая IllegalStateException при недопустимом переходе.
     */
    public void validateTransition(HmsJobStatus target) {
        if (!canTransitionTo(target)) {
            throw new IllegalStateException(String.format(
                    "Недопустимый переход состояния задачи HMS репликации: '%s' -> '%s'. Допустимые переходы: %s",
                    this.name(), target != null ? target.name() : "null",
                    ALLOWED_TRANSITIONS.getOrDefault(this, Set.of())
            ));
        }
    }

    /**
     * Активно ли выполнение задачи (в процессе начальной или потоковой синхронизации).
     */
    public boolean isRunning() {
        return this == BOOTSTRAPPING || this == ACTIVE;
    }

    /**
     * Находится ли задача в состоянии ошибки.
     */
    public boolean isErrorOrFailed() {
        return this == ERROR || this == FAILED;
    }

    /**
     * Безопасный парсинг из строки с поддержкой регистра и fallback.
     */
    public static HmsJobStatus fromString(String statusStr) {
        if (statusStr == null || statusStr.isBlank()) {
            return QUEUED;
        }
        try {
            return HmsJobStatus.valueOf(statusStr.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return switch (statusStr.trim().toUpperCase()) {
                case "RUNNING", "IN_PROGRESS" -> ACTIVE;
                case "BOOTSTRAP" -> BOOTSTRAPPING;
                case "WAITING" -> WAITING_FOR_AGENTS;
                default -> QUEUED;
            };
        }
    }
}

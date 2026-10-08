package org.apache.hadoop.explorer.common.session;

import org.apache.hadoop.explorer.common.model.UserSession;

import java.time.Instant;
import java.util.Optional;

/**
 * Интерфейс персистентного сессионного хранилища и черного списка токенов.
 */
public interface SessionStore {

    /**
     * Сохраняет активную сессию пользователя.
     */
    void saveSession(String token, UserSession session, Instant expiresAt, String jti);

    /**
     * Возвращает активную сессию по токену.
     */
    Optional<UserSession> getSession(String token);

    /**
     * Помещает токен или JTI в черный список (отзыв).
     */
    void revokeToken(String tokenOrJti, String username, Instant expiresAt);

    /**
     * Проверяет, отозван ли токен или JTI (L1 кэш -> L2 БД).
     */
    boolean isTokenRevoked(String tokenOrJti);

    /**
     * Удаляет активную сессию (Logout).
     */
    void deleteSession(String token);

    /**
     * Очищает устаревшие сессии и отозванные токены.
     */
    void cleanupExpired();

    /**
     * Проверка доступности хранилища (Health check / Readiness probe).
     */
    boolean ping();
}

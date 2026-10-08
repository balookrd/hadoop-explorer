package org.apache.hadoop.explorer.common.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.JwtTokenService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Двухуровневое хранилище сессий:
 * L1: Сверхбыстрый in-memory LRU кэш Caffeine для проверки отзыва токенов.
 * L2: БД через JDBC (SQLite/PostgreSQL) либо локальный ConcurrentHashMap для dev/тестов.
 */
public class DefaultSessionStore implements SessionStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultSessionStore.class);

    private final CommonSecurityProperties properties;
    private final ObjectMapper objectMapper;
    private final Cache<String, Boolean> l1RevokedCache;
    private final JdbcTemplate jdbcTemplate;
    private final boolean hasJdbc;

    // L2 In-memory Fallback (для режима без БД или для тестов)
    private record InMemorySession(UserSession session, Instant expiresAt) {}
    private record InMemoryRevocation(String username, Instant expiresAt) {}

    private final Map<String, InMemorySession> inMemorySessions = new ConcurrentHashMap<>();
    private final Map<String, InMemoryRevocation> inMemoryRevocations = new ConcurrentHashMap<>();

    public DefaultSessionStore(CommonSecurityProperties properties, ObjectMapper objectMapper, DataSource dataSource) {
        this.properties = properties;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();

        int l1Cap = properties.getSessionStore().getL1Capacity();
        int l1Ttl = properties.getSessionStore().getL1TtlSeconds();

        this.l1RevokedCache = Caffeine.newBuilder()
            .maximumSize(l1Cap)
            .expireAfterWrite(l1Ttl, TimeUnit.SECONDS)
            .build();

        if (dataSource != null && !"memory".equalsIgnoreCase(properties.getSessionStore().getType())) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
            this.hasJdbc = true;
            initSchemaIfSupported();
        } else {
            this.jdbcTemplate = null;
            this.hasJdbc = false;
        }
    }

    private void initSchemaIfSupported() {
        if (!hasJdbc) return;
        try {
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS active_sessions (
                    token_hash VARCHAR(64) PRIMARY KEY,
                    username VARCHAR(128) NOT NULL,
                    session_data TEXT NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
            """);

            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS revoked_tokens (
                    token_hash VARCHAR(64) PRIMARY KEY,
                    username VARCHAR(128) NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    revoked_at TIMESTAMP NOT NULL
                )
            """);
            log.info("L2 SessionStore JDBC tables initialized successfully");
        } catch (Exception e) {
            log.warn("Could not auto-create session tables via JDBC: {}", e.getMessage());
        }
    }

    @Override
    public void saveSession(String token, UserSession session, Instant expiresAt, String jti) {
        if (token == null || session == null) return;
        String tokenHash = JwtTokenService.hashToken(token);
        Instant exp = expiresAt != null ? expiresAt : Instant.now().plusSeconds(480 * 60);

        if (hasJdbc) {
            try {
                String jsonData = objectMapper.writeValueAsString(session);
                jdbcTemplate.update("""
                    INSERT INTO active_sessions (token_hash, username, session_data, expires_at, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(token_hash) DO UPDATE SET session_data = EXCLUDED.session_data, expires_at = EXCLUDED.expires_at
                """, tokenHash, session.username(), jsonData, java.sql.Timestamp.from(exp), java.sql.Timestamp.from(Instant.now()));
                return;
            } catch (Exception e) {
                log.error("Failed to save session to JDBC, falling back to in-memory: {}", e.getMessage());
                if (properties.getSessionStore().isFailClosed()) {
                    throw new RuntimeException("L2 storage write failed", e);
                }
            }
        }
        inMemorySessions.put(tokenHash, new InMemorySession(session, exp));
    }

    @Override
    public Optional<UserSession> getSession(String token) {
        if (token == null) return Optional.empty();
        String tokenHash = JwtTokenService.hashToken(token);

        if (isTokenRevoked(token)) {
            return Optional.empty();
        }

        if (hasJdbc) {
            try {
                var list = jdbcTemplate.query(
                    "SELECT session_data, expires_at FROM active_sessions WHERE token_hash = ?",
                    (rs, rowNum) -> {
                        var exp = rs.getTimestamp("expires_at");
                        if (exp != null && exp.toInstant().isBefore(Instant.now())) {
                            return null;
                        }
                        try {
                            return objectMapper.readValue(rs.getString("session_data"), UserSession.class);
                        } catch (Exception ex) {
                            return null;
                        }
                    },
                    tokenHash
                );
                if (!list.isEmpty() && list.get(0) != null) {
                    return Optional.of(list.get(0));
                }
            } catch (Exception e) {
                log.warn("JDBC getSession query failed: {}", e.getMessage());
                if (properties.getSessionStore().isFailClosed()) {
                    return Optional.empty();
                }
            }
        }

        InMemorySession memSession = inMemorySessions.get(tokenHash);
        if (memSession != null) {
            if (memSession.expiresAt().isBefore(Instant.now())) {
                inMemorySessions.remove(tokenHash);
                return Optional.empty();
            }
            return Optional.of(memSession.session());
        }

        return Optional.empty();
    }

    @Override
    public void revokeToken(String tokenOrJti, String username, Instant expiresAt) {
        if (tokenOrJti == null) return;
        String hash = JwtTokenService.hashToken(tokenOrJti);
        Instant exp = expiresAt != null ? expiresAt : Instant.now().plusSeconds(480 * 60);

        // Помещаем в быстрый L1 кэш
        l1RevokedCache.put(hash, true);
        l1RevokedCache.put(tokenOrJti, true);

        // Удаляем из сессий
        inMemorySessions.remove(hash);

        if (hasJdbc) {
            try {
                jdbcTemplate.update("DELETE FROM active_sessions WHERE token_hash = ?", hash);
                jdbcTemplate.update("""
                    INSERT INTO revoked_tokens (token_hash, username, expires_at, revoked_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(token_hash) DO NOTHING
                """, hash, username != null ? username : "unknown", java.sql.Timestamp.from(exp), java.sql.Timestamp.from(Instant.now()));
                return;
            } catch (Exception e) {
                log.error("Failed to revoke token in JDBC: {}", e.getMessage());
            }
        }
        inMemoryRevocations.put(hash, new InMemoryRevocation(username, exp));
    }

    @Override
    public boolean isTokenRevoked(String tokenOrJti) {
        if (tokenOrJti == null) return true;

        // 1. Проверка в L1 Caffeine Cache
        String hash = JwtTokenService.hashToken(tokenOrJti);
        if (Boolean.TRUE.equals(l1RevokedCache.getIfPresent(hash)) ||
            Boolean.TRUE.equals(l1RevokedCache.getIfPresent(tokenOrJti))) {
            return true;
        }

        // 2. Проверка в L2 JDBC
        if (hasJdbc) {
            try {
                Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM revoked_tokens WHERE token_hash = ? OR token_hash = ?",
                    Integer.class,
                    hash, tokenOrJti
                );
                if (count != null && count > 0) {
                    l1RevokedCache.put(hash, true);
                    return true;
                }
            } catch (Exception e) {
                log.warn("JDBC isTokenRevoked query failed: {}", e.getMessage());
                if (properties.getSessionStore().isFailClosed()) {
                    return true; // Безопасный отказ (Fail-Closed)
                }
            }
        }

        // 3. Проверка в локальной памяти
        if (inMemoryRevocations.containsKey(hash) || inMemoryRevocations.containsKey(tokenOrJti)) {
            l1RevokedCache.put(hash, true);
            return true;
        }

        return false;
    }

    @Override
    public void deleteSession(String token) {
        if (token == null) return;
        String hash = JwtTokenService.hashToken(token);
        inMemorySessions.remove(hash);
        if (hasJdbc) {
            try {
                jdbcTemplate.update("DELETE FROM active_sessions WHERE token_hash = ?", hash);
            } catch (Exception e) {
                log.warn("Failed to delete active session in JDBC: {}", e.getMessage());
            }
        }
    }

    @Override
    public void cleanupExpired() {
        Instant now = Instant.now();
        inMemorySessions.entrySet().removeIf(e -> e.getValue().expiresAt().isBefore(now));
        inMemoryRevocations.entrySet().removeIf(e -> e.getValue().expiresAt().isBefore(now));

        if (hasJdbc) {
            try {
                jdbcTemplate.update("DELETE FROM active_sessions WHERE expires_at < ?", java.sql.Timestamp.from(now));
                jdbcTemplate.update("DELETE FROM revoked_tokens WHERE expires_at < ?", java.sql.Timestamp.from(now));
            } catch (Exception e) {
                log.warn("Failed to cleanup expired sessions in JDBC: {}", e.getMessage());
            }
        }
    }

    @Override
    public boolean ping() {
        if (hasJdbc) {
            try {
                jdbcTemplate.queryForObject("SELECT 1", Integer.class);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }
}

package org.apache.hadoop.explorer.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.session.DefaultSessionStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SessionStoreTest {

    @Test
    @DisplayName("Должен сохранять и извлекать активную сессию")
    void shouldSaveAndRetrieveSession() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        DefaultSessionStore store = new DefaultSessionStore(props, new ObjectMapper(), null);

        UserSession session = new UserSession(
            "ivan",
            "Иван Иванов",
            "ivan@corp.ru",
            List.of("engineers"),
            "ldap",
            false,
            Role.WRITER
        );

        String token = "jwt-token-sample-value-123";
        store.saveSession(token, session, Instant.now().plusSeconds(3600), "jti-123");

        var retrievedOpt = store.getSession(token);
        assertTrue(retrievedOpt.isPresent());
        assertEquals("ivan", retrievedOpt.get().username());
        assertEquals(Role.WRITER, retrievedOpt.get().systemRole());
    }

    @Test
    @DisplayName("Должен проверять и кэшировать отзыв токена через L1")
    void shouldRevokeAndDetectRevocation() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        DefaultSessionStore store = new DefaultSessionStore(props, new ObjectMapper(), null);

        String token = "token-to-revoke-999";
        assertFalse(store.isTokenRevoked(token));

        store.revokeToken(token, "ivan", Instant.now().plusSeconds(3600));

        assertTrue(store.isTokenRevoked(token));
        // Сессия по отозванному токену не должна возвращаться
        assertTrue(store.getSession(token).isEmpty());
    }

    @Test
    @DisplayName("Должен удалять сессию при Logout")
    void shouldDeleteSessionOnLogout() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        DefaultSessionStore store = new DefaultSessionStore(props, new ObjectMapper(), null);

        UserSession session = new UserSession("bob", "Bob", null, List.of(), "mock", false, Role.READER);
        String token = "bob-token";
        store.saveSession(token, session, Instant.now().plusSeconds(3600), null);

        assertTrue(store.getSession(token).isPresent());

        store.deleteSession(token);
        assertTrue(store.getSession(token).isEmpty());
    }
}

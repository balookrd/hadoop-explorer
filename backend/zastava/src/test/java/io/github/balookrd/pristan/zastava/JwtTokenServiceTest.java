package io.github.balookrd.pristan.zastava;

import io.github.balookrd.pristan.zastava.config.ZastavaProperties;
import io.github.balookrd.pristan.zastava.model.Role;
import io.github.balookrd.pristan.zastava.model.UserSession;
import io.github.balookrd.pristan.zastava.security.JwtTokenService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JwtTokenServiceTest {

    @Test
    @DisplayName("Должен корректно генерировать и валидировать JWT токен")
    void shouldGenerateAndVerifyJwtToken() {
        ZastavaProperties props = new ZastavaProperties();
        props.getJwt().setSecretKey("my-super-secret-jwt-key-minimum-32-chars-long!");
        props.getJwt().setExpirationMinutes(60);

        JwtTokenService service = new JwtTokenService(props);

        UserSession session = new UserSession(
            "alice",
            "Alice Smith",
            "alice@example.com",
            List.of("hadoop-admins"),
            "ldap",
            true,
            Role.ADMIN
        );

        String token = service.createToken(session);
        assertNotNull(token);
        assertFalse(token.isBlank());

        var parsedOpt = service.parseAndVerifyToken(token);
        assertTrue(parsedOpt.isPresent());

        var payload = parsedOpt.get();
        assertEquals("alice", payload.sub());
        assertEquals("Alice Smith", payload.displayName());
        assertEquals("alice@example.com", payload.email());
        assertEquals(List.of("hadoop-admins"), payload.groups());
        assertTrue(payload.isAdmin());
        assertEquals("admin", payload.systemRole());
        assertNotNull(payload.jti());
    }

    @Test
    @DisplayName("Должен отклонять токен с неверной подписью")
    void shouldRejectTokenWithInvalidSignature() {
        ZastavaProperties props1 = new ZastavaProperties();
        props1.getJwt().setSecretKey("key-1-minimum-32-characters-for-signature-validation");

        ZastavaProperties props2 = new ZastavaProperties();
        props2.getJwt().setSecretKey("key-2-different-secret-key-for-signature-validation");

        JwtTokenService service1 = new JwtTokenService(props1);
        JwtTokenService service2 = new JwtTokenService(props2);

        UserSession session = new UserSession("bob", "Bob", null, List.of(), "mock", false, Role.READER);
        String token = service1.createToken(session);

        var verified = service2.parseAndVerifyToken(token);
        assertTrue(verified.isEmpty(), "Токен с другим ключом должен отклоняться");
    }

    @Test
    @DisplayName("Должен детерминированно считать SHA-256 хэш токена")
    void shouldHashTokenConsistently() {
        String token = "sample-token-string-12345";
        String hash1 = JwtTokenService.hashToken(token);
        String hash2 = JwtTokenService.hashToken(token);

        assertEquals(hash1, hash2);
        assertEquals(64, hash1.length());
    }
}

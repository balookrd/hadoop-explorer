package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.audit.AuditLogger;
import org.apache.hadoop.explorer.common.audit.AuditRecord;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.config.SecurityConfigurationValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Проверка небезопасной конфигурации безопасности на старте")
class SecurityConfigurationValidatorTest {

    private static final String SECRET = "a-real-secret-that-is-at-least-32-characters";

    private static CommonSecurityProperties bind(Map<String, String> props) {
        Binder binder = new Binder(new MapConfigurationPropertySource(props));
        return binder.bind("hadoop.security", CommonSecurityProperties.class).orElseGet(CommonSecurityProperties::new);
    }

    @Test
    @DisplayName("Пустая конфигурация (mock без debug) не стартует")
    void defaultsAreRejected() {
        assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(new CommonSecurityProperties()));
    }

    @Test
    @DisplayName("mock разрешен только вместе с debug=true")
    void mockRequiresDebug() {
        assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "mock"))));
        assertDoesNotThrow(() -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "mock",
                "hadoop.security.debug", "true"))));
    }

    @Test
    @DisplayName("Боевой режим со встроенным JWT-секретом не стартует")
    void productionModeRequiresOwnSecret() {
        assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "kerberos"))));
        assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "kerberos",
                "hadoop.security.jwt.secret-key", "short"))));
        assertDoesNotThrow(() -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "kerberos",
                "hadoop.security.jwt.secret-key", SECRET))));
    }

    @Test
    @DisplayName("Режим ldap требует ldap.enabled=true, неизвестный режим отклоняется")
    void ldapAndUnknownModes() {
        assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "ldap",
                "hadoop.security.jwt.secret-key", SECRET))));
        assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "ldpa",
                "hadoop.security.jwt.secret-key", SECRET))));
    }

    @Test
    @DisplayName("Ключи из старых руководств (auth-mode, server-uri, bind-dn, ...) применяются, а не игнорируются")
    void legacyKeysAreBound() {
        CommonSecurityProperties props = bind(Map.of(
                "hadoop.security.auth-mode", "ldap",
                "hadoop.security.ldap.server-uri", "ldaps://ldap.company.local:636",
                "hadoop.security.ldap.bind-dn", "cn=svc,dc=company,dc=local",
                "hadoop.security.ldap.bind-password", "pw",
                "hadoop.security.ldap.user-base-dn", "ou=users,dc=company,dc=local",
                "hadoop.security.ldap.user-filter", "(uid={username})",
                "hadoop.security.kerberos.keytab-path", "/etc/security/keytabs/svc.keytab",
                "hadoop.security.kerberos.principal", "HTTP/svc@COMPANY.LOCAL"));

        assertEquals("ldap", props.getAuth().getMode());
        assertEquals("ldaps://ldap.company.local:636", props.getLdap().getUrl());
        assertEquals("cn=svc,dc=company,dc=local", props.getLdap().getManagerDn());
        assertEquals("pw", props.getLdap().getManagerPassword());
        assertEquals("ou=users,dc=company,dc=local", props.getLdap().getUserSearchBase());
        assertEquals("(uid={0})", props.getLdap().getUserSearchFilter());
        assertEquals("/etc/security/keytabs/svc.keytab", props.getKerberos().getKeytab());
        assertEquals("HTTP/svc@COMPANY.LOCAL", props.getKerberos().getServicePrincipal());
    }

    @Test
    @DisplayName("Запись аудита с Instant сериализуется в JSON")
    void auditRecordSerializes() throws Exception {
        AuditRecord record = new AuditRecord(Instant.parse("2026-10-29T06:00:00Z"), "yarn", "u", "1.2.3.4",
                "APPROVE", "YARN", "SUCCESS", 1, Map.of());
        String json = new AuditLogger().toJson(record);
        assertTrue(json.contains("\"timestamp\":\"2026-10-29T06:00:00Z\""), json);
    }
}

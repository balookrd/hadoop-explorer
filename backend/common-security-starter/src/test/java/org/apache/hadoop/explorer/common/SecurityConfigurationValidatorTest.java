package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.audit.AuditLogger;
import org.apache.hadoop.explorer.common.audit.AuditRecord;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.config.SecurityConfigurationValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
    @DisplayName("Опубликованные в репозитории JWT-секреты отклоняются вне debug")
    void publishedSecretsAreRejectedOutsideDebug() {
        // default of the removed Helm charts, and the sql mock-profile secret
        for (String published : List.of("replace-with-a-very-long-random-string-in-production",
                "sql-explorer-super-secret-jwt-token-key-32-chars-min!")) {
            IllegalStateException error = assertThrows(IllegalStateException.class, () -> SecurityConfigurationValidator.validate(bind(Map.of(
                    "hadoop.security.auth.mode", "kerberos",
                    "hadoop.security.jwt.secret-key", published))));
            assertTrue(error.getMessage().contains("published"), error.getMessage());
        }
    }

    @Test
    @DisplayName("Демо-стенды (debug=true) запускаются со своими опубликованными секретами")
    void publishedSecretsAreAllowedInDebug() {
        // demo/sql/application-demo.yml: hybrid mode with debug: true
        assertDoesNotThrow(() -> SecurityConfigurationValidator.validate(bind(Map.of(
                "hadoop.security.auth.mode", "hybrid",
                "hadoop.security.debug", "true",
                "hadoop.security.jwt.secret-key", "demo-jwt-secret-key-change-in-production-only-min-32-chars!"))));
    }

    @Test
    @DisplayName("Каждый JWT-секрет из backend/, demo/, docs/ и README.md есть в списке опубликованных")
    void everySecretCommittedToTheRepositoryIsListed() throws IOException {
        // Surefire runs with backend/common-security-starter as working directory
        Path root = Path.of("../..").toAbsolutePath().normalize();
        // YAML "secret-key: \"literal\"", properties "secret-key=literal", "jwt-secret-key: literal",
        // shell/env "JWT_SECRET_KEY=literal"; placeholders (${...}, <...>, "...") do not match
        Pattern secretKey = Pattern.compile(
                "(?:secret-key|JWT_SECRET_KEY)\\s*[:=]\\s*\\\\?[\"']?([A-Za-z0-9!_+/-]{20,})");
        List<String> unlisted = new ArrayList<>();
        int found = 0;
        // a directory missing from the checkout is skipped
        for (String dir : List.of("backend", "demo", "docs", "README.md")) {
            Path start = root.resolve(dir);
            if (!Files.exists(start)) {
                continue;
            }
            List<Path> files;
            try (Stream<Path> walk = Files.walk(start)) {
                files = walk.filter(Files::isRegularFile)
                        .filter(f -> !f.toString().contains("/target/") && !f.toString().contains("/node_modules/"))
                        .filter(f -> f.toString().matches(".*\\.(ya?ml|properties|md|sh)$"))
                        .toList();
            }
            for (Path file : files) {
                Matcher matcher = secretKey.matcher(Files.readString(file));
                while (matcher.find()) {
                    found++;
                    if (!SecurityConfigurationValidator.PUBLISHED_SECRET_KEYS.contains(matcher.group(1))) {
                        unlisted.add(root.relativize(file) + ": \"" + matcher.group(1) + "\"");
                    }
                }
            }
        }
        assertTrue(found > 0, "no secret-key literal found: check the repository root " + root);
        assertTrue(unlisted.isEmpty(),
                "add to SecurityConfigurationValidator.PUBLISHED_SECRET_KEYS or replace with a placeholder: " + unlisted);
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

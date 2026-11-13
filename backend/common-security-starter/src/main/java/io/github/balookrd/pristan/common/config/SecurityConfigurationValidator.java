package io.github.balookrd.pristan.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Set;

/**
 * Fails startup on security settings that are unsafe outside development.
 *
 * <p>A misspelled key silently falls back to the defaults, and the defaults are mock
 * authentication (any password for well-known demo accounts) and a JWT secret that is
 * published in the source code. Refusing to start is the only way such a mistake is noticed
 * before production.
 */
public class SecurityConfigurationValidator implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfigurationValidator.class);

    // "hybrid" is used by the spark/sql stands: CompositeAuthService tries LDAP, Kerberos SSO stays available
    private static final Set<String> KNOWN_MODES = Set.of("mock", "ldap", "kerberos", "hybrid");

    /**
     * JWT secrets published in this repository: mock profiles, demo stands, test resources, defaults of
     * removed chart versions and documentation examples. Anyone who has read the repository can forge
     * session tokens signed with them, so outside debug mode they are refused like the built-in default.
     * Values stay in the list after they disappear from the working tree: they remain in git history.
     */
    public static final Set<String> PUBLISHED_SECRET_KEYS = Set.of(
            // defaults of the removed Helm charts (helm/, deleted in af69592a)
            "replace-with-a-very-long-random-string-in-production",
            "yarn-explorer-super-secret-key-change-in-production-random-hash",
            // application-mock.yml of the services and test resources
            "hdfs-explorer-super-secret-jwt-token-key-32-chars-min!",
            "sql-explorer-super-secret-jwt-token-key-32-chars-min!",
            "spark-explorer-super-secret-jwt-token-key-32-chars-min!",
            "yarn-explorer-test-secret-key-at-least-32-characters",
            "replicator-orchestrator-secret-key-32-chars-minimum!",
            "test-secret-key-that-is-at-least-32-characters-long!",
            // demo/*/application-demo.yml
            "hdfs-explorer-demo-stand-super-secret-key-2026",
            "yarn-explorer-demo-stand-super-secret-key-2026",
            "spark-explorer-demo-secret-jwt-key-32chars-min-safe!",
            "demo-jwt-secret-key-change-in-production-only-min-32-chars!",
            // examples in docs/ and READMEs (current and earlier versions)
            "SuperSecretLongRandomKeyForProductionJWTToken12345",
            "e8b23c91d4e7a8f1029384756cba0192e8b23c91d4e7a8f1029384756cba0192",
            "9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b",
            "SecureJwtTokenSecretMin32CharsLength",
            "Min32CharSecretKeyForHdfsExplorerJwt",
            "Min32CharSecretKeyForReplicatorTokens",
            "CHANGE_TO_SUPER_SECURE_KEY_AT_LEAST_32_CHARS",
            "CHANGE_TO_SUPER_SECURE_RANDOM_KEY_MIN_32_CHARS",
            "your-production-secret-key-at-least-32-chars-long",
            "your-production-secret-key",
            "production-super-secret-key-min-32-chars!");

    private final CommonSecurityProperties properties;

    public SecurityConfigurationValidator(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        validate(properties);
    }

    public static void validate(CommonSecurityProperties properties) {
        String mode = properties.getAuth().getMode() == null ? "" : properties.getAuth().getMode().trim().toLowerCase();

        if (!KNOWN_MODES.contains(mode)) {
            throw new IllegalStateException("hadoop.security.auth.mode='" + properties.getAuth().getMode()
                    + "' is not supported; expected one of " + KNOWN_MODES);
        }

        if ("mock".equals(mode)) {
            if (!properties.isDebug()) {
                throw new IllegalStateException("hadoop.security.auth.mode=mock accepts demo accounts without a real "
                        + "identity check and is allowed only together with hadoop.security.debug=true. "
                        + "For production set hadoop.security.auth.mode to ldap or kerberos.");
            }
            log.warn("SECURITY: mock authentication is enabled (debug mode). Never use this configuration in production.");
            return;
        }

        String secret = properties.getJwt().getSecretKey();
        if (secret == null || secret.isBlank()
                || CommonSecurityProperties.JwtProperties.DEFAULT_SECRET_KEY.equals(secret)) {
            throw new IllegalStateException("hadoop.security.jwt.secret-key is not set: the built-in default is public "
                    + "and lets anyone forge session tokens. Provide a random secret of at least 32 characters.");
        }
        if (secret.length() < 32) {
            throw new IllegalStateException("hadoop.security.jwt.secret-key must be at least 32 characters long");
        }
        if (!properties.isDebug() && PUBLISHED_SECRET_KEYS.contains(secret)) {
            throw new IllegalStateException("hadoop.security.jwt.secret-key is a value published in the Pristan "
                    + "repository (mock profile, demo stand, chart default or documentation example) and lets anyone "
                    + "forge session tokens. Generate a new one, for example with: openssl rand -hex 32");
        }

        if ("ldap".equals(mode) && !properties.getLdap().isEnabled()) {
            throw new IllegalStateException("hadoop.security.auth.mode=ldap requires hadoop.security.ldap.enabled=true");
        }
    }
}

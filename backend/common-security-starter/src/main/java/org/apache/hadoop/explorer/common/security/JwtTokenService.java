package org.apache.hadoop.explorer.common.security;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.TokenPayload;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Сервис создания, валидации и хеширования JWT токенов (Nimbus JOSE).
 */
public class JwtTokenService {

    private static final Logger log = LoggerFactory.getLogger(JwtTokenService.class);
    private final CommonSecurityProperties properties;

    public JwtTokenService(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    /**
     * Возвращает SHA-256 хэш токена для безопасного поиска в БД отозванных токенов (RevokedTokens).
     */
    public static String hashToken(String token) {
        if (token == null) {
            return "";
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(token.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Создает подписанный JWT токен на основе данных сессии пользователя.
     */
    public String createToken(UserSession session) {
        return createToken(session, properties.getJwt().getExpirationMinutes());
    }

    public String createToken(UserSession session, int expirationMinutes) {
        try {
            Instant now = Instant.now();
            Instant exp = now.plus(expirationMinutes, ChronoUnit.MINUTES);
            String jti = UUID.randomUUID().toString().replace("-", "");

            JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                .subject(session.username())
                .claim("sub", session.username())
                .claim("display_name", session.displayName())
                .claim("email", session.email())
                .claim("groups", session.groups())
                .claim("auth_method", session.authMethod())
                .claim("is_admin", session.isAdmin())
                .claim("system_role", session.systemRole().getValue())
                .jwtID(jti)
                .issueTime(Date.from(now))
                .expirationTime(Date.from(exp))
                .build();

            byte[] secretBytes = properties.getJwt().getSecretKey().getBytes(StandardCharsets.UTF_8);
            JWSSigner signer = new MACSigner(secretBytes);

            SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claimsSet);
            signedJWT.sign(signer);

            return signedJWT.serialize();
        } catch (Exception e) {
            log.error("Failed to generate JWT token for user: {}", session.username(), e);
            throw new RuntimeException("JWT generation error", e);
        }
    }

    /**
     * Декодирует и проверяет подпись и срок действия токена.
     */
    public Optional<TokenPayload> parseAndVerifyToken(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            SignedJWT signedJWT = SignedJWT.parse(token);
            byte[] secretBytes = properties.getJwt().getSecretKey().getBytes(StandardCharsets.UTF_8);
            JWSVerifier verifier = new MACVerifier(secretBytes);

            if (!signedJWT.verify(verifier)) {
                log.warn("JWT token signature verification failed");
                return Optional.empty();
            }

            JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
            Date expirationTime = claims.getExpirationTime();
            if (expirationTime == null || expirationTime.before(new Date())) {
                log.debug("JWT token is expired");
                return Optional.empty();
            }

            List<String> groupsList = Collections.emptyList();
            Object rawGroups = claims.getClaim("groups");
            if (rawGroups instanceof List<?> l) {
                groupsList = l.stream().map(Object::toString).toList();
            }

            Boolean isAdmin = claims.getBooleanClaim("is_admin");
            String sub = claims.getSubject() != null ? claims.getSubject() : (String) claims.getClaim("username");
            String displayName = (String) claims.getClaim("display_name");
            String email = (String) claims.getClaim("email");
            String jti = claims.getJWTID();
            String authMethod = (String) claims.getClaim("auth_method");
            String systemRole = (String) claims.getClaim("system_role");

            TokenPayload payload = new TokenPayload(
                sub,
                displayName != null ? displayName : sub,
                email,
                groupsList,
                expirationTime.getTime() / 1000,
                claims.getIssueTime() != null ? claims.getIssueTime().getTime() / 1000 : 0,
                jti,
                authMethod != null ? authMethod : "jwt",
                isAdmin != null && isAdmin,
                systemRole != null ? systemRole : "reader"
            );

            return Optional.of(payload);
        } catch (Exception e) {
            log.debug("JWT token decode failure: {}", e.getMessage());
            return Optional.empty();
        }
    }
}

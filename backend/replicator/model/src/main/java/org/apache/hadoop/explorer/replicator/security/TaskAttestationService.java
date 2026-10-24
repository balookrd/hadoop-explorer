package org.apache.hadoop.explorer.replicator.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Криптографическая аттестация задач репликации (Task Attestation / HMAC Tokens).
 * <p>
 * Обеспечивает 100% Zero-Trust изоляцию прав doAs и защиту от подделки параметров
 * (Blind Privilege Escalation / Path Substitution) при прямых gRPC-вызовах между узлами.
 * </p>
 * Токен подписывает канонические метаданные задачи:
 * <code>v1|taskId|jobId|targetPath|executionPrincipal|runAsServiceAccount|expiresAt</code>
 */
public class TaskAttestationService {

    private static final Logger log = LoggerFactory.getLogger(TaskAttestationService.class);

    public static final String VERSION = "v1";
    public static final String HMAC_ALGORITHM = "HmacSHA256";
    public static final long DEFAULT_TTL_MILLIS = 4 * 3600 * 1000L; // 4 часа
    public static final long CLOCK_SKEW_TOLERANCE_MILLIS = 60_000L; // 60 секунд толерантности часов

    private final String defaultSecret;

    public TaskAttestationService() {
        this(null);
    }

    public TaskAttestationService(String defaultSecret) {
        this.defaultSecret = defaultSecret;
    }

    /**
     * Генерация HMAC-токена аттестации задачи со стандартным TTL (4 часа).
     */
    public static String generateToken(String taskId, String jobId, String targetPath,
                                       String executionPrincipal, boolean runAsServiceAccount,
                                       String secret) {
        return generateToken(taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount, DEFAULT_TTL_MILLIS, secret);
    }

    /**
     * Генерация HMAC-токена аттестации задачи с заданным временем жизни (TTL).
     */
    public static String generateToken(String taskId, String jobId, String targetPath,
                                       String executionPrincipal, boolean runAsServiceAccount,
                                       long ttlMillis, String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("Секретный ключ для подписи токена аттестации не может быть пустым");
        }
        long expiresAt = System.currentTimeMillis() + Math.max(ttlMillis, 1000L);
        String payload = buildCanonicalPayload(taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount, expiresAt);
        byte[] signature = computeHmac(payload, secret);
        String signatureBase64 = Base64.getUrlEncoder().withoutPadding().encodeToString(signature);
        return VERSION + "." + expiresAt + "." + signatureBase64;
    }

    /**
     * Валидация HMAC-токена аттестации задачи.
     *
     * @return true, если токен подлинный, срок его действия не истек, и параметры задачи строго совпадают.
     */
    public static boolean validateToken(String token, String taskId, String jobId, String targetPath,
                                        String executionPrincipal, boolean runAsServiceAccount,
                                        String secret) {
        if (token == null || token.isBlank() || secret == null || secret.isBlank()) {
            return false;
        }

        try {
            String[] parts = token.split("\\.");
            if (parts.length != 3) {
                log.warn("Некорректный формат токена аттестации задачи (ожидалось 3 секции): {}", token);
                return false;
            }

            String version = parts[0];
            if (!VERSION.equals(version)) {
                log.warn("Неподдерживаемая версия токена аттестации задачи: {}", version);
                return false;
            }

            long expiresAt;
            try {
                expiresAt = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                log.warn("Некорректный timestamp истечения токена аттестации: {}", parts[1]);
                return false;
            }

            long now = System.currentTimeMillis();
            if (now > expiresAt + CLOCK_SKEW_TOLERANCE_MILLIS) {
                log.warn("Токен аттестации задачи истек (expiresAt={}, now={}) для task={}, job={}",
                        expiresAt, now, taskId, jobId);
                return false;
            }

            byte[] actualSignature;
            try {
                actualSignature = Base64.getUrlDecoder().decode(parts[2]);
            } catch (IllegalArgumentException e) {
                log.warn("Некорректная Base64-подпись токена аттестации");
                return false;
            }

            String payload = buildCanonicalPayload(taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount, expiresAt);
            byte[] expectedSignature = computeHmac(payload, secret);

            boolean valid = MessageDigest.isEqual(expectedSignature, actualSignature);
            if (!valid) {
                log.warn("Криптографическая сигнатура токена аттестации не совпадает для task={}, job={}, path={}, principal={}, runAsService={}",
                        taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount);
            }
            return valid;
        } catch (Exception e) {
            log.error("Исключение при валидации токена аттестации задачи: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Построение детерминированной канонической строки метаданных для подписи.
     */
    public static String buildCanonicalPayload(String taskId, String jobId, String targetPath,
                                               String executionPrincipal, boolean runAsServiceAccount,
                                               long expiresAt) {
        String safeTaskId = taskId != null ? taskId.trim() : "";
        String safeJobId = jobId != null ? jobId.trim() : "";
        String safeTargetPath = targetPath != null ? targetPath.trim() : "";
        String safePrincipal = executionPrincipal != null ? executionPrincipal.trim() : "";
        return VERSION + "|" + safeTaskId + "|" + safeJobId + "|" + safeTargetPath + "|"
                + safePrincipal + "|" + runAsServiceAccount + "|" + expiresAt;
    }

    private static byte[] computeHmac(String data, String secret) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
            mac.init(keySpec);
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Ошибка вычисления " + HMAC_ALGORITHM + ": " + e.getMessage(), e);
        }
    }

    public String generate(String taskId, String jobId, String targetPath, String executionPrincipal, boolean runAsServiceAccount) {
        return generateToken(taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount, defaultSecret);
    }

    public boolean validate(String token, String taskId, String jobId, String targetPath, String executionPrincipal, boolean runAsServiceAccount) {
        return validateToken(token, taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount, defaultSecret);
    }
}

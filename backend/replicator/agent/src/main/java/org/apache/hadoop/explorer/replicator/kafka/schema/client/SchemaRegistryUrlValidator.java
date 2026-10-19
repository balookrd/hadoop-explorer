package org.apache.hadoop.explorer.replicator.kafka.schema.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Set;

/**
 * Валидатор URL адресов Schema Registry для защиты от SSRF атак,
 * утечек Kerberos SPNEGO токенов и несанкционированного доступа к облачным сервисам метаданных.
 */
public final class SchemaRegistryUrlValidator {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryUrlValidator.class);

    private static final Set<String> ALLOWED_SCHEMES = Set.of("http", "https");
    private static final String CLOUD_METADATA_HOST = "169.254.169.254";
    private static final String GOOGLE_METADATA_HOST = "metadata.google.internal";

    private SchemaRegistryUrlValidator() {
    }

    /**
     * Валидирует и нормализует базовый URL Schema Registry.
     *
     * @param rawUrl исходный URL
     * @return нормализованный URL (без завершающих слэшей)
     * @throws IllegalArgumentException если URL не прошел проверку безопасности
     */
    public static String validate(String rawUrl) {
        if (rawUrl == null || rawUrl.isBlank()) {
            throw new IllegalArgumentException("Schema Registry URL must not be blank");
        }

        if (rawUrl.contains("\r") || rawUrl.contains("\n")) {
            throw new IllegalArgumentException("Schema Registry URL contains invalid newline characters: " + rawUrl);
        }

        URI uri;
        try {
            uri = URI.create(rawUrl.trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Schema Registry URL format: " + rawUrl, e);
        }

        String scheme = uri.getScheme();
        if (scheme == null || !ALLOWED_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Invalid URL scheme '" + scheme +
                    "'. Only http and https are allowed for Schema Registry");
        }

        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Schema Registry URL host must not be empty: " + rawUrl);
        }

        String lowerHost = host.toLowerCase(Locale.ROOT);

        // Блокировка доступа к облачным metadata endpoints (AWS, GCP, Azure, OpenStack)
        if (CLOUD_METADATA_HOST.equals(lowerHost) || GOOGLE_METADATA_HOST.equals(lowerHost) || lowerHost.startsWith("169.254.")) {
            throw new IllegalArgumentException("SSRF blocked: access to cloud metadata service or link-local address is forbidden: " + host);
        }

        // Проверка через резолв IP при числовых адресах или явных link-local
        try {
            if (isLiteralIpAddress(lowerHost)) {
                InetAddress addr = InetAddress.getByName(lowerHost);
                if (addr.isLinkLocalAddress()) {
                    throw new IllegalArgumentException("SSRF blocked: link-local IP addresses are forbidden: " + host);
                }
                if (addr.isMulticastAddress()) {
                    throw new IllegalArgumentException("SSRF blocked: multicast IP addresses are forbidden: " + host);
                }
                if (Boolean.getBoolean("replicator.schemaregistry.block-loopback") && addr.isLoopbackAddress()) {
                    throw new IllegalArgumentException("SSRF blocked: loopback addresses are blocked by configuration: " + host);
                }
            }
        } catch (UnknownHostException ignored) {
            // Если имя не резолвится локально при валидации, пропускаем
        }

        return rawUrl.trim().replaceAll("/+$", "");
    }

    /**
     * Предупреждает об отправке Kerberos SPNEGO тикетов по незашифрованному HTTP.
     */
    public static void checkSpnegoSecurity(String normalizedUrl, boolean hasSpnego) {
        if (!hasSpnego) {
            return;
        }
        try {
            URI uri = URI.create(normalizedUrl);
            if ("http".equalsIgnoreCase(uri.getScheme())) {
                String host = uri.getHost();
                if (!isLoopback(host)) {
                    log.warn("ВНИМАНИЕ: Kerberos SPNEGO аутентификация включена поверх незашифрованного HTTP соединения: {}. " +
                            "Рекомендуется использовать HTTPS во избежание перехвата билетов Kerberos в корпоративной сети.", normalizedUrl);
                }
            }
        } catch (Exception ignored) {
        }
    }

    public static boolean isLoopback(String host) {
        if (host == null) {
            return false;
        }
        String lower = host.toLowerCase(Locale.ROOT);
        return "localhost".equals(lower) || "127.0.0.1".equals(lower) || "::1".equals(lower) || lower.startsWith("127.");
    }

    private static boolean isLiteralIpAddress(String host) {
        return host.matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$") || host.contains(":");
    }
}

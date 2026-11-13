package io.github.balookrd.pristan.iskra.service.engine;

import io.github.balookrd.pristan.iskra.config.SparkProperties;
import io.github.balookrd.pristan.iskra.service.engine.livy.LivyAuthHeaderProvider;
import io.github.balookrd.pristan.iskra.service.engine.livy.LivyClient;
import io.github.balookrd.pristan.iskra.service.engine.livy.LivySessionPayloadBuilder;
import io.github.balookrd.pristan.iskra.service.engine.livy.LivySparkEngine;
import io.github.balookrd.pristan.iskra.service.engine.livy.SpnegoAuthHeaderProvider;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Picks the engine of a cluster: the emulator for mock-storage: true or a non-livy type, Livy otherwise.
 * The Livy settings of every real cluster are checked at startup, so a missing livy-url stops the service
 * with a configuration error instead of failing the first session request.
 */
@Component
public class SparkEngineRegistry {

    private static final Pattern IPV4_LITERAL = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}");

    private final SparkProperties properties;
    private final MockSparkEngine mockEngine = new MockSparkEngine();
    private final Map<String, SparkEngine> engines = new ConcurrentHashMap<>();

    public SparkEngineRegistry(SparkProperties properties) {
        this.properties = properties;
        for (SparkProperties.SparkClusterConfig cluster : properties.getClusters()) {
            if (usesLivy(cluster)) {
                validate(cluster);
            }
        }
    }

    public SparkEngine forCluster(SparkProperties.SparkClusterConfig cluster) {
        if (!usesLivy(cluster)) {
            return mockEngine;
        }
        return engines.computeIfAbsent(cluster.getId(), id -> createLivyEngine(cluster));
    }

    public MockSparkEngine mock() {
        return mockEngine;
    }

    private static boolean usesLivy(SparkProperties.SparkClusterConfig cluster) {
        return !cluster.isMockStorage() && "livy".equalsIgnoreCase(cluster.getType());
    }

    private SparkEngine createLivyEngine(SparkProperties.SparkClusterConfig cluster) {
        validate(cluster);
        SparkProperties.LivyConfig livy = cluster.getLivy();
        LivyClient client = new LivyClient(cluster.getLivyUrl(), authFor(cluster),
                Duration.ofSeconds(livy.getConnectTimeoutSeconds()), Duration.ofSeconds(livy.getReadTimeoutSeconds()));
        return new LivySparkEngine(cluster, client, new LivySessionPayloadBuilder(properties));
    }

    LivyAuthHeaderProvider authFor(SparkProperties.SparkClusterConfig cluster) {
        String auth = cluster.getLivy().getAuth() == null ? "none" : cluster.getLivy().getAuth().toLowerCase(Locale.ROOT);
        return switch (auth) {
            case "none" -> LivyAuthHeaderProvider.NONE;
            case "spnego" -> {
                validateSpnego(cluster);
                yield new SpnegoAuthHeaderProvider(cluster.getLivy().getPrincipal(), cluster.getLivy().getKeytab(),
                        Duration.ofMinutes(cluster.getLivy().getReloginIntervalMinutes()));
            }
            default -> throw new IllegalStateException("iskra.clusters[" + cluster.getId() + "].livy.auth: неизвестное значение " + auth);
        };
    }

    private void validate(SparkProperties.SparkClusterConfig cluster) {
        String prefix = "iskra.clusters[" + cluster.getId() + "].";
        String url = cluster.getLivyUrl();
        if (url == null || url.isBlank()) {
            throw new IllegalStateException(prefix + "livy-url не задан: кластер с mock-storage: false работает через "
                    + "Apache Livy. Укажите URL Livy (http://<livy_host>:8998) или mock-storage: true");
        }
        URI uri;
        try {
            uri = URI.create(url.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(prefix + "livy-url не является URL; ожидается http(s)://<livy_host>:<port>");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!(scheme.equals("http") || scheme.equals("https")) || uri.getHost() == null) {
            throw new IllegalStateException(prefix + "livy-url = " + url + ": ожидается http(s)://<livy_host>:<port>");
        }
        if (uri.getRawUserInfo() != null) {
            // Not echoed: the user info is a credential
            throw new IllegalStateException(prefix + "livy-url содержит учетные данные (user:password@); "
                    + "они не используются, аутентификация к Livy задается в livy.auth");
        }
        SparkProperties.LivyConfig livy = cluster.getLivy();
        requirePositive(prefix + "livy.connect-timeout-seconds", livy.getConnectTimeoutSeconds());
        requirePositive(prefix + "livy.read-timeout-seconds", livy.getReadTimeoutSeconds());
        requirePositive(prefix + "livy.session-start-timeout-seconds", livy.getSessionStartTimeoutSeconds());
        requirePositive(prefix + "livy.statement-timeout-seconds", livy.getStatementTimeoutSeconds());
        requirePositive(prefix + "livy.poll-interval-ms", livy.getPollIntervalMs());
        authFor(cluster);
    }

    /**
     * Keytab and principal are checked here, at startup; the keytab is read only on the first Livy request.
     * krb5.conf is not a setting of its own: the JDK Kerberos configuration is process-wide, so the provider
     * relies on -Djava.security.krb5.conf (or the JDK default) and never sets it.
     */
    private static void validateSpnego(SparkProperties.SparkClusterConfig cluster) {
        String prefix = "iskra.clusters[" + cluster.getId() + "].livy.";
        SparkProperties.LivyConfig livy = cluster.getLivy();
        if (livy.getPrincipal() == null || livy.getPrincipal().isBlank()) {
            throw new IllegalStateException(prefix + "principal не задан: livy.auth: spnego требует principal сервиса "
                    + "(svc_iskra@<REALM>)");
        }
        if (livy.getKeytab() == null || livy.getKeytab().isBlank()) {
            throw new IllegalStateException(prefix + "keytab не задан: livy.auth: spnego требует keytab сервиса");
        }
        if (!new File(livy.getKeytab()).isAbsolute()) {
            throw new IllegalStateException(prefix + "keytab должен быть абсолютным путем: " + livy.getKeytab());
        }
        requirePositive(prefix + "relogin-interval-minutes", livy.getReloginIntervalMinutes());
        // The SPN is HTTP/<host of livy-url>; a KDC issues HTTP principals for host names, an IP address
        // would only fail on the first request with "server not found in Kerberos database"
        String host = URI.create(cluster.getLivyUrl().trim()).getHost();
        if (host.startsWith("[") || IPV4_LITERAL.matcher(host).matches()) {
            throw new IllegalStateException("iskra.clusters[" + cluster.getId() + "].livy-url = " + cluster.getLivyUrl()
                    + ": при livy.auth: spnego нужно имя хоста, на которое выписан principal HTTP/<livy_host> Livy, а не IP-адрес");
        }
    }

    private static void requirePositive(String key, long value) {
        if (value <= 0) {
            throw new IllegalStateException(key + " = " + value + ": значение должно быть больше 0");
        }
    }
}

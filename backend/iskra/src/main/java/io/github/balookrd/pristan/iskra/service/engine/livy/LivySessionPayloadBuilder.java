package io.github.balookrd.pristan.iskra.service.engine.livy;

import io.github.balookrd.pristan.iskra.config.SparkProperties;
import io.github.balookrd.pristan.iskra.dto.CreateSessionRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Body of POST /sessions. Conf precedence (later wins): Spark version, Python env, metastore,
 * packages and custom Python path, then the user's spark_conf (minus denied prefixes).
 */
public final class LivySessionPayloadBuilder {

    /**
     * Denied whatever iskra.denied-spark-conf-prefixes says (lower case, compared with the lower-cased key):
     * - credentials of the Livy account and Livy's own settings: Livy copies session conf into the RSC launcher
     *   config, so livy.rsc.proxy-user would become spark-submit --proxy-user when proxyUser is not sent;
     * - spark.master, spark.submit.deployMode: local or client mode runs user code on the Livy host as livy;
     * - spark.yarn.dist.*, spark.yarn.jars, spark.yarn.archive: spark-submit runs on the Livy host as livy and
     *   uploads these local files into the user's container (a way to read the Livy keytab);
     * - spark.jars.ivy, spark.jars.ivySettings, spark.jars.repositories: Ivy runs on the Livy host as livy, these
     *   would point it at a local settings file, cache directory or arbitrary repository;
     * - spark.yarn.queue: the queue comes from the request field checked against yarn-queues.
     */
    private static final List<String> ALWAYS_DENIED_PREFIXES = List.of(
            "livy.", "spark.yarn.keytab", "spark.yarn.principal", "spark.kerberos.",
            "spark.master", "spark.submit.deploymode",
            "spark.yarn.dist.", "spark.yarn.jars", "spark.yarn.archive",
            "spark.jars.ivy", "spark.jars.repositories",
            // The queue is taken from the request field after the yarn-queues check; a conf key would bypass it
            "spark.yarn.queue");

    /** File lists spark-submit resolves on the Livy host: allowed only with a remote scheme. */
    private static final Set<String> FILE_LIST_KEYS = Set.of("spark.files", "spark.jars", "spark.submit.pyfiles", "spark.archives");

    private final SparkProperties properties;

    public LivySessionPayloadBuilder(SparkProperties properties) {
        this.properties = properties;
    }

    /** Livy session name is iskra-<user>-<8 hex chars>; used to recognise a reused Livy id after a Livy restart. */
    public static String sessionNamePrefix(String username) {
        return "iskra-" + username + "-";
    }

    public Map<String, Object> build(SparkProperties.SparkClusterConfig cluster, String username, CreateSessionRequest req) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("kind", "spark".equals(req.kind()) ? "spark" : "pyspark");
        body.put("name", sessionNamePrefix(username) + UUID.randomUUID().toString().substring(0, 8));
        if (cluster.getLivy().isProxyUser()) {
            body.put("proxyUser", username);
        }
        String queue = notBlank(req.yarnQueue()) ? req.yarnQueue().strip() : cluster.getDefaultQueue();
        if (notBlank(queue)) {
            if (!cluster.getYarnQueues().isEmpty() && !cluster.getYarnQueues().contains(queue)) {
                throw new InvalidSessionRequestException("Очередь YARN " + queue + " не входит в список очередей кластера "
                        + cluster.getId() + ": " + String.join(", ", cluster.getYarnQueues()));
            }
            body.put("queue", queue);
        }

        SparkProperties.ResourceProfileConfig profile = cluster.getResourceProfiles().stream()
                .filter(p -> p.getId().equals(req.resourceProfile()))
                .findFirst()
                .orElseThrow(() -> new InvalidSessionRequestException("Неизвестный профиль ресурсов: " + req.resourceProfile()));
        body.put("driverMemory", profile.getDriverMemory());
        body.put("driverCores", profile.getDriverCores());
        body.put("executorMemory", profile.getExecutorMemory());
        body.put("executorCores", profile.getExecutorCores());
        body.put("numExecutors", profile.getNumExecutors());

        if (req.jars() != null && !req.jars().isEmpty()) {
            req.jars().forEach(path -> requireRemote("jars", path));
            body.put("jars", req.jars());
        }
        if (req.pyFiles() != null && !req.pyFiles().isEmpty()) {
            req.pyFiles().forEach(path -> requireRemote("py_files", path));
            body.put("pyFiles", req.pyFiles());
        }
        if (notBlank(req.customPythonArchive())) {
            requireRemote("custom_python_archive", req.customPythonArchive());
            body.put("archives", List.of(req.customPythonArchive()));
        }

        Map<String, String> conf = new LinkedHashMap<>();
        Optional<SparkProperties.SparkVersionConfig> version = cluster.getSparkVersions().stream()
                .filter(v -> v.getId().equals(req.sparkVersionId())).findFirst();
        version.ifPresent(v -> conf.putAll(v.getSparkConf()));
        version.flatMap(v -> v.getPythonVersions().stream().filter(p -> p.getId().equals(req.pythonEnvId())).findFirst())
                .ifPresent(p -> conf.putAll(p.getSparkConf()));
        cluster.getMetastores().stream().filter(m -> m.getId().equals(req.metastoreId())).findFirst()
                .ifPresent(m -> conf.putAll(m.getSparkConf()));
        if (req.packages() != null && !req.packages().isEmpty()) {
            conf.put("spark.jars.packages", String.join(",", req.packages()));
        }
        if (notBlank(req.customPythonPath())) {
            conf.put("spark.pyspark.python", req.customPythonPath());
        }
        if (req.sparkConf() != null) {
            // Only the user's spark_conf is checked: the admin's spark-conf of versions, envs and metastores is trusted
            for (Map.Entry<String, String> entry : req.sparkConf().entrySet()) {
                String key = entry.getKey();
                if (key == null || key.isEmpty() || key.codePoints().anyMatch(c -> Character.isWhitespace(c) || Character.isISOControl(c))) {
                    // Not echoed: the key may carry control characters into logs
                    throw new InvalidSessionRequestException("Недопустимое имя параметра spark_conf: пустое, с пробелами или управляющими символами");
                }
                String normalized = key.toLowerCase(Locale.ROOT);
                if (isDenied(normalized)) {
                    throw new InvalidSessionRequestException("Параметр spark_conf запрещен администратором: " + key);
                }
                if (FILE_LIST_KEYS.contains(normalized) && entry.getValue() != null) {
                    for (String path : entry.getValue().split(",")) {
                        if (!path.isBlank()) {
                            requireRemote(key, path.strip());
                        }
                    }
                }
                conf.put(key, entry.getValue());
            }
        }
        if (!conf.isEmpty()) {
            body.put("conf", conf);
        }
        return body;
    }

    private boolean isDenied(String normalizedKey) {
        return ALWAYS_DENIED_PREFIXES.stream().anyMatch(normalizedKey::startsWith)
                || properties.getDeniedSparkConfPrefixes().stream()
                        .map(prefix -> prefix.strip().toLowerCase(Locale.ROOT))
                        .anyMatch(prefix -> !prefix.isEmpty() && normalizedKey.startsWith(prefix));
    }

    /** Schemes from iskra.allowed-remote-schemes; file:, local: and plain paths point to the Livy host. */
    private void requireRemote(String parameter, String path) {
        Set<String> allowed = new TreeSet<>();
        properties.getAllowedRemoteSchemes().forEach(scheme -> allowed.add(scheme.strip().toLowerCase(Locale.ROOT)));
        String scheme;
        try {
            scheme = new URI(path.strip()).getScheme();
        } catch (URISyntaxException e) {
            scheme = null;
        }
        if (scheme == null || !allowed.contains(scheme.toLowerCase(Locale.ROOT))) {
            throw new InvalidSessionRequestException("Параметр " + parameter + ": путь " + path
                    + " должен быть в удаленной файловой системе (" + String.join(", ", allowed)
                    + ", iskra.allowed-remote-schemes); локальные пути указывают на хост Livy");
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}

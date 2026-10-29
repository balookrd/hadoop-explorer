package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.security.TlsContextFactory;
import org.apache.hadoop.explorer.yarn.model.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.*;

/**
 * ResourceManager REST client.
 *
 * <ul>
 *   <li>Kerberos-secured RMs are called over SPNEGO ({@link AuthenticatedURL}) with the explorer's
 *       keytab identity; there is no silent fallback to unauthenticated requests.</li>
 *   <li>The active RM is re-discovered when a request fails or lands on a standby RM (which answers
 *       with a redirect), so an HA failover does not leave the client stuck on a dead address.</li>
 * </ul>
 */
public class NativeYarnClient implements YarnClient {

    private static final Logger log = LoggerFactory.getLogger(NativeYarnClient.class);

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 10_000;

    private final ClusterConfig cluster;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final SSLContext sslContext;
    private final KerberosClientLogin kerberosLogin;
    private volatile String cachedActiveRmUrl;

    public NativeYarnClient(ClusterConfig cluster, ObjectMapper objectMapper) {
        this(cluster, objectMapper, null, null);
    }

    public NativeYarnClient(ClusterConfig cluster,
                            ObjectMapper objectMapper,
                            CommonSecurityProperties.TlsProperties tlsProps,
                            KerberosClientLogin kerberosLogin) {
        this.cluster = cluster;
        this.objectMapper = objectMapper;
        this.kerberosLogin = kerberosLogin;
        boolean tls = tlsProps != null && tlsProps.isEnabled();
        this.sslContext = tls ? TlsContextFactory.createSslContext(tlsProps) : null;
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(CONNECT_TIMEOUT_MS))
                // A standby RM redirects to the active one; that is treated as a failover signal
                .followRedirects(HttpClient.Redirect.NEVER);
        if (tls) {
            TlsContextFactory.configureHttpClient(builder, tlsProps);
        }
        this.httpClient = builder.build();
    }

    @Override
    public String getActiveRmUrl() {
        String cached = cachedActiveRmUrl;
        return cached != null ? cached : discoverActiveRm();
    }

    @Override
    public ClusterMetrics getClusterMetrics(String doAs) {
        JsonNode m = getJson("/ws/v1/cluster/metrics", doAs).path("clusterMetrics");

        int totalMem = m.path("totalMB").asInt(cluster.getTotalResources() != null ? cluster.getTotalResources().getMemoryMb() : 0);
        int allocatedMem = m.path("allocatedMB").asInt(0);
        int totalCores = m.path("totalVirtualCores").asInt(cluster.getTotalResources() != null ? cluster.getTotalResources().getVcores() : 0);
        int allocatedCores = m.path("allocatedVirtualCores").asInt(0);

        return ClusterMetrics.builder()
                .totalMemoryMb(totalMem)
                .allocatedMemoryMb(allocatedMem)
                .availableMemoryMb(m.path("availableMB").asInt(totalMem - allocatedMem))
                .totalVcores(totalCores)
                .allocatedVcores(allocatedCores)
                .availableVcores(m.path("availableVirtualCores").asInt(totalCores - allocatedCores))
                .activeNodes(m.path("activeNodes").asInt(0))
                .unhealthyNodes(m.path("unhealthyNodes").asInt(0))
                .totalContainers(m.path("containersAllocated").asInt(0))
                .runningApps(m.path("appsRunning").asInt(0))
                .partitions(cluster.getPartitions())
                .build();
    }

    @Override
    public QueueNode getQueueTree(String doAs) {
        JsonNode schedulerInfo = getJson("/ws/v1/cluster/scheduler", doAs).path("scheduler").path("schedulerInfo");
        String type = schedulerInfo.path("type").asText("");
        if (!type.isEmpty() && !"capacityScheduler".equals(type)) {
            throw new YarnRmException("Кластер " + cluster.getId() + " использует планировщик '" + type
                    + "', поддерживается только Capacity Scheduler");
        }
        return parseQueueTree(schedulerInfo, cluster);
    }

    /**
     * Converts the {@code schedulerInfo} element of {@code /ws/v1/cluster/scheduler} into the queue tree.
     * Only values the RM actually reports are filled in; unknown settings stay {@code null} so that
     * the change-request diff never mistakes a guessed default for the deployed value.
     */
    static QueueNode parseQueueTree(JsonNode schedulerInfo, ClusterConfig cluster) {
        return parseQueueNode(schedulerInfo, null, cluster);
    }

    private static QueueNode parseQueueNode(JsonNode node, String parentPath, ClusterConfig cluster) {
        String queueName = node.path("queueName").asText("root");
        String path = parentPath == null || parentPath.isBlank() ? queueName : parentPath + "." + queueName;

        List<QueueNode> children = new ArrayList<>();
        JsonNode childQueues = node.path("queues").path("queue");
        if (childQueues.isArray()) {
            for (JsonNode childJson : childQueues) {
                children.add(parseQueueNode(childJson, path, cluster));
            }
        } else if (childQueues.isObject()) {
            children.add(parseQueueNode(childQueues, path, cluster));
        }

        QueueState state = QueueState.RUNNING;
        try {
            state = QueueState.valueOf(node.path("state").asText("RUNNING").toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            // keep RUNNING for states this model does not know
        }

        Map<String, PartitionResourceConfig> partitions = new LinkedHashMap<>();
        Double maxAmPercent = null;
        JsonNode capacities = node.path("capacities").path("queueCapacitiesByPartition");
        if (capacities.isArray()) {
            for (JsonNode partNode : capacities) {
                // The RM reports the default partition with an empty name
                String partition = CapacitySchedulerPatchService.normalizePartition(partNode.path("partitionName").asText(""), cluster);
                partitions.put(partition, partition(partition, partNode.path("capacity").asDouble(0.0),
                        partNode.path("maxCapacity").asDouble(100.0)));
                if (CapacitySchedulerPatchService.DEFAULT_PARTITION.equals(partition) && partNode.has("maxAMLimitPercentage")) {
                    maxAmPercent = round(partNode.path("maxAMLimitPercentage").asDouble() / 100.0, 4);
                }
            }
        }
        partitions.putIfAbsent(CapacitySchedulerPatchService.DEFAULT_PARTITION, partition(
                CapacitySchedulerPatchService.DEFAULT_PARTITION,
                node.path("capacity").asDouble(0.0), node.path("maxCapacity").asDouble(100.0)));

        List<String> labels = null;
        JsonNode nodeLabels = node.path("nodeLabels");
        if (nodeLabels.isArray()) {
            labels = new ArrayList<>();
            for (JsonNode l : nodeLabels) {
                labels.add(l.asText());
            }
        } else if (nodeLabels.isTextual()) {
            labels = List.of(nodeLabels.asText());
        }

        JsonNode resourcesUsed = node.path("resourcesUsed");
        return QueueNode.builder()
                .name(queueName)
                .path(path)
                .parentPath(parentPath)
                .leaf(children.isEmpty())
                .state(state)
                .resourceMode(cluster.getResourceMode())
                .partitions(partitions)
                .userLimitFactor(optDouble(node, "userLimitFactor"))
                // Reported by Hadoop 3.3+ only; older RMs do not expose the configured policy
                .orderingPolicy(node.hasNonNull("orderingPolicyInfo") ? node.path("orderingPolicyInfo").asText().toLowerCase(Locale.ROOT) : null)
                .maxApplications(optInt(node, "maxApplications"))
                .maxAmResourcePercent(maxAmPercent)
                .maxParallelApps(optInt(node, "maxParallelApps"))
                .accessibleNodeLabels(labels)
                .defaultNodeLabelExpression(node.hasNonNull("defaultNodeLabelExpression") ? node.path("defaultNodeLabelExpression").asText() : null)
                .currentUsedResources(new ResourceAllocation(resourcesUsed.path("memory").asInt(0), resourcesUsed.path("vCores").asInt(0)))
                .currentUsedPercent(node.path("usedCapacity").asDouble(0.0))
                .numApplications(node.path("numApplications").asInt(0))
                .numActiveApplications(node.path("numActiveApplications").asInt(0))
                .numPendingApplications(node.path("numPendingApplications").asInt(0))
                .children(children)
                .build();
    }

    private static PartitionResourceConfig partition(String name, double capacity, double maxCapacity) {
        double cap = round(capacity, 2);
        double max = round(maxCapacity, 2);
        return PartitionResourceConfig.builder()
                .partitionName(name)
                .capacity(cap)
                .maxCapacity(max)
                .elastic(max > cap)
                .elasticityRatio(cap > 0 ? round(max / cap, 2) : 1.0)
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // HTTP with active-RM discovery and failover
    // ---------------------------------------------------------------------------------------

    private JsonNode getJson(String apiPath, String doAs) {
        String query = buildDoAsParam(doAs);
        String rm = getActiveRmUrl();
        try {
            return objectMapper.readTree(fetchOk(rm, apiPath + query));
        } catch (IOException | StandbyRmException first) {
            // Failover or a dead RM: look for the active one again and retry once
            log.warn("Запрос {} к RM {} кластера {} не удался ({}), повторное определение активного RM",
                    apiPath, rm, cluster.getId(), first.getMessage());
            cachedActiveRmUrl = null;
            String next = discoverActiveRm();
            try {
                return objectMapper.readTree(fetchOk(next, apiPath + query));
            } catch (IOException | StandbyRmException second) {
                throw new YarnRmException("Ошибка связи с YARN ResourceManager кластера " + cluster.getId()
                        + ": " + second.getMessage(), second);
            }
        }
    }

    private String fetchOk(String rm, String pathAndQuery) throws IOException {
        HttpResult r = get(rm + pathAndQuery, "application/json");
        if (r.status() >= 300 && r.status() < 400) {
            throw new StandbyRmException("RM " + rm + " ответил перенаправлением (standby)");
        }
        if (r.status() == 401 || r.status() == 403) {
            throw new YarnRmException("ResourceManager " + rm + " отказал в доступе (HTTP " + r.status() + ")"
                    + (cluster.isKerberosEnabled() ? "" : "; если RM защищен Kerberos, включите kerberos-enabled для кластера"));
        }
        if (r.status() >= 400) {
            throw new IOException("HTTP " + r.status() + " от " + rm + pathAndQuery);
        }
        return r.body();
    }

    private synchronized String discoverActiveRm() {
        if (cachedActiveRmUrl != null) {
            return cachedActiveRmUrl;
        }
        List<String> rmUrls = cluster.getResourceManagerUrls();
        if (rmUrls == null || rmUrls.isEmpty()) {
            throw new YarnRmException("Не указаны адреса ResourceManager для кластера " + cluster.getId());
        }

        String firstReachable = null;
        List<String> failures = new ArrayList<>();
        for (String url : rmUrls) {
            String rm = url.replaceAll("/+$", "");
            try {
                HttpResult r = get(rm + "/ws/v1/cluster/info", "application/json");
                if (r.status() == 401 || r.status() == 403) {
                    failures.add(rm + ": HTTP " + r.status());
                    continue;
                }
                if (r.status() != 200) {
                    failures.add(rm + ": HTTP " + r.status());
                    continue;
                }
                String haState = objectMapper.readTree(r.body()).path("clusterInfo").path("haState").asText("");
                if ("ACTIVE".equalsIgnoreCase(haState)) {
                    log.info("Активный RM кластера {}: {}", cluster.getId(), rm);
                    cachedActiveRmUrl = rm;
                    return rm;
                }
                if (firstReachable == null && haState.isEmpty()) {
                    firstReachable = rm; // pre-HA RMs do not report haState
                }
            } catch (Exception e) {
                failures.add(rm + ": " + e.getMessage());
            }
        }
        if (firstReachable != null) {
            cachedActiveRmUrl = firstReachable;
            return firstReachable;
        }
        throw new YarnRmException("Нет доступного активного ResourceManager для кластера " + cluster.getId()
                + " (" + String.join("; ", failures) + ")");
    }

    private HttpResult get(String url, String accept) throws IOException {
        if (cluster.isKerberosEnabled()) {
            return getSpnego(url, accept);
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", accept)
                .timeout(Duration.ofMillis(READ_TIMEOUT_MS))
                .GET()
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return new HttpResult(response.statusCode(), response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Запрос прерван", e);
        }
    }

    private HttpResult getSpnego(String url, String accept) throws IOException {
        if (kerberosLogin == null) {
            throw new YarnRmException("Kerberos для кластера " + cluster.getId() + " включен, но клиентский логин не настроен");
        }
        UserGroupInformation ugi = kerberosLogin.ugi();
        ConnectionConfigurator configurator = conn -> {
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setInstanceFollowRedirects(false);
            if (sslContext != null && conn instanceof HttpsURLConnection https) {
                https.setSSLSocketFactory(sslContext.getSocketFactory());
            }
            return conn;
        };
        try {
            return ugi.doAs((PrivilegedExceptionAction<HttpResult>) () -> {
                AuthenticatedURL.Token token = new AuthenticatedURL.Token();
                HttpURLConnection conn = new AuthenticatedURL(new KerberosAuthenticator(), configurator)
                        .openConnection(URI.create(url).toURL(), token);
                conn.setRequestProperty("Accept", accept);
                int status = conn.getResponseCode();
                InputStream in = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
                String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
                return new HttpResult(status, body);
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("SPNEGO-запрос к " + url + " не удался: " + e.getMessage(), e);
        }
    }

    private String buildDoAsParam(String doAs) {
        if (cluster.isImpersonationEnabled() && doAs != null && !doAs.isBlank()) {
            return "?doAs=" + URLEncoder.encode(doAs, StandardCharsets.UTF_8);
        }
        return "";
    }

    private static Double optDouble(JsonNode node, String field) {
        return node.hasNonNull(field) ? round(node.path(field).asDouble(), 4) : null;
    }

    private static Integer optInt(JsonNode node, String field) {
        return node.hasNonNull(field) && node.path(field).asInt() > 0 ? node.path(field).asInt() : null;
    }

    private static double round(double v, int scale) {
        double f = Math.pow(10, scale);
        return Math.round(v * f) / f;
    }

    private record HttpResult(int status, String body) {
    }

    private static final class StandbyRmException extends RuntimeException {
        StandbyRmException(String message) {
            super(message);
        }
    }
}

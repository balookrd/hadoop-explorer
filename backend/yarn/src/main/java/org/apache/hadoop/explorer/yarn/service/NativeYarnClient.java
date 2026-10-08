package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.yarn.model.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.*;

public class NativeYarnClient implements YarnClient {

    private static final Logger log = LoggerFactory.getLogger(NativeYarnClient.class);

    private final ClusterConfig cluster;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final MockYarnClient fallbackMockClient;
    private volatile String cachedActiveRmUrl;

    public NativeYarnClient(ClusterConfig cluster, ObjectMapper objectMapper) {
        this.cluster = cluster;
        this.objectMapper = objectMapper;
        this.fallbackMockClient = new MockYarnClient(cluster);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @Override
    public synchronized String getActiveRmUrl() {
        if (cachedActiveRmUrl != null) {
            return cachedActiveRmUrl;
        }

        List<String> rmUrls = cluster.getResourceManagerUrls();
        if (rmUrls == null || rmUrls.isEmpty()) {
            return "http://localhost:8088";
        }

        for (String url : rmUrls) {
            String trimmedUrl = url.replaceAll("/+$", "");
            try {
                String infoUrl = trimmedUrl + "/ws/v1/cluster/info";
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(infoUrl))
                        .header("Accept", "application/json")
                        .timeout(Duration.ofSeconds(3))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    JsonNode root = objectMapper.readTree(response.body());
                    JsonNode clusterInfo = root.path("clusterInfo");
                    String haState = clusterInfo.path("haState").asText("");
                    if ("ACTIVE".equalsIgnoreCase(haState)) {
                        log.info("Обнаружен активный RM для кластера {}: {}", cluster.getId(), trimmedUrl);
                        this.cachedActiveRmUrl = trimmedUrl;
                        return trimmedUrl;
                    }
                }
            } catch (Exception e) {
                log.debug("Не удалось связаться с RM {}: {}", trimmedUrl, e.getMessage());
            }
        }

        // Если активный не определен явно через haState, используем первый
        this.cachedActiveRmUrl = rmUrls.getFirst().replaceAll("/+$", "");
        return this.cachedActiveRmUrl;
    }

    @Override
    public ClusterMetrics getClusterMetrics(String doAs) {
        try {
            String activeRm = getActiveRmUrl();
            String metricsUrl = activeRm + "/ws/v1/cluster/metrics" + buildDoAsParam(doAs);
            String body = executeHttp(metricsUrl, "application/json");

            JsonNode root = objectMapper.readTree(body);
            JsonNode m = root.path("clusterMetrics");

            int totalMem = m.path("totalMB").asInt(cluster.getTotalResources() != null ? cluster.getTotalResources().getMemoryMb() : 0);
            int allocatedMem = m.path("allocatedMB").asInt(0);
            int availableMem = m.path("availableMB").asInt(totalMem - allocatedMem);

            int totalCores = m.path("totalVirtualCores").asInt(cluster.getTotalResources() != null ? cluster.getTotalResources().getVcores() : 0);
            int allocatedCores = m.path("allocatedVirtualCores").asInt(0);
            int availableCores = m.path("availableVirtualCores").asInt(totalCores - allocatedCores);

            return ClusterMetrics.builder()
                    .totalMemoryMb(totalMem)
                    .allocatedMemoryMb(allocatedMem)
                    .availableMemoryMb(availableMem)
                    .totalVcores(totalCores)
                    .allocatedVcores(allocatedCores)
                    .availableVcores(availableCores)
                    .activeNodes(m.path("activeNodes").asInt(0))
                    .unhealthyNodes(m.path("unhealthyNodes").asInt(0))
                    .totalContainers(m.path("containersAllocated").asInt(0))
                    .runningApps(m.path("appsRunning").asInt(0))
                    .partitions(cluster.getPartitions())
                    .build();
        } catch (Exception e) {
            log.warn("Не удалось получить метрики кластера {} из YARN RM, переключение на mock fallback: {}", cluster.getId(), e.getMessage());
            return fallbackMockClient.getClusterMetrics(doAs);
        }
    }

    @Override
    public QueueNode getQueueTree(String doAs) {
        try {
            String activeRm = getActiveRmUrl();
            String schedulerUrl = activeRm + "/ws/v1/cluster/scheduler" + buildDoAsParam(doAs);
            String body = executeHttp(schedulerUrl, "application/json");

            JsonNode root = objectMapper.readTree(body);
            JsonNode schedulerInfo = root.path("scheduler").path("schedulerInfo");

            // Capacity Scheduler представляет корень в schedulerInfo
            return parseQueueNode(schedulerInfo, null);
        } catch (Exception e) {
            log.warn("Не удалось получить дерево очередей {} из YARN RM, переключение на mock fallback: {}", cluster.getId(), e.getMessage());
            return fallbackMockClient.getQueueTree(doAs);
        }
    }

    private QueueNode parseQueueNode(JsonNode node, String parentPath) {
        String queueName = node.path("queueName").asText("root");
        String path = parentPath == null || parentPath.isBlank() ? queueName : parentPath + "." + queueName;

        boolean isLeaf = node.path("queues").isMissingNode() || node.path("queues").path("queue").isMissingNode();
        double capacity = node.path("capacity").asDouble(0.0);
        double maxCapacity = node.path("maxCapacity").asDouble(100.0);
        String stateStr = node.path("status").asText(node.path("state").asText("RUNNING"));

        QueueState state = QueueState.RUNNING;
        try {
            state = QueueState.valueOf(stateStr.toUpperCase());
        } catch (Exception ignored) {
        }

        Map<String, PartitionResourceConfig> partitions = new HashMap<>();
        partitions.put("DEFAULT", PartitionResourceConfig.builder()
                .partitionName("DEFAULT")
                .capacity(capacity)
                .maxCapacity(maxCapacity)
                .elastic(maxCapacity > capacity)
                .elasticityRatio(capacity > 0 ? Math.round((maxCapacity / capacity) * 100.0) / 100.0 : 1.0)
                .build());

        // Проверяем наличие очередей по разделам (node labels)
        JsonNode capacities = node.path("capacities").path("queueCapacitiesByPartition");
        if (capacities.isArray()) {
            for (JsonNode partNode : capacities) {
                String pName = partNode.path("partitionName").asText("DEFAULT");
                double pCap = partNode.path("capacity").asDouble(0.0);
                double pMaxCap = partNode.path("maxCapacity").asDouble(100.0);
                partitions.put(pName, PartitionResourceConfig.builder()
                        .partitionName(pName)
                        .capacity(pCap)
                        .maxCapacity(pMaxCap)
                        .elastic(pMaxCap > pCap)
                        .elasticityRatio(pCap > 0 ? Math.round((pMaxCap / pCap) * 100.0) / 100.0 : 1.0)
                        .build());
            }
        }

        JsonNode resourcesUsed = node.path("resourcesUsed");
        int usedMem = resourcesUsed.path("memory").asInt(0);
        int usedCores = resourcesUsed.path("vCores").asInt(0);

        List<QueueNode> children = new ArrayList<>();
        JsonNode childQueues = node.path("queues").path("queue");
        if (childQueues.isArray()) {
            for (JsonNode childJson : childQueues) {
                children.add(parseQueueNode(childJson, path));
            }
        }

        return QueueNode.builder()
                .name(queueName)
                .path(path)
                .parentPath(parentPath)
                .leaf(isLeaf && children.isEmpty())
                .state(state)
                .resourceMode(cluster.getResourceMode())
                .partitions(partitions)
                .currentUsedResources(new ResourceAllocation(usedMem, usedCores))
                .numApplications(node.path("numApplications").asInt(0))
                .numActiveApplications(node.path("numActiveApplications").asInt(0))
                .numPendingApplications(node.path("numPendingApplications").asInt(0))
                .children(children)
                .build();
    }

    @Override
    public String getCapacitySchedulerXml(String doAs) {
        try {
            String activeRm = getActiveRmUrl();
            String confUrl = activeRm + "/conf" + buildDoAsParam(doAs);
            return executeHttp(confUrl, "application/xml, text/xml");
        } catch (Exception e) {
            log.warn("Не удалось загрузить capacity-scheduler.xml из YARN RM {}: {}", cluster.getId(), e.getMessage());
            return null;
        }
    }

    private String buildDoAsParam(String doAs) {
        if (cluster.isImpersonationEnabled() && doAs != null && !doAs.isBlank()) {
            return "?doAs=" + doAs;
        }
        return "";
    }

    private String executeHttp(String url, String acceptHeader) throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", acceptHeader)
                .timeout(Duration.ofSeconds(10))
                .GET();

        if (cluster.isKerberosEnabled()) {
            try {
                UserGroupInformation ugi = UserGroupInformation.getLoginUser();
                return ugi.doAs((PrivilegedExceptionAction<String>) () -> {
                    HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() >= 400) {
                        throw new IOException("HTTP " + response.statusCode() + " from " + url);
                    }
                    return response.body();
                });
            } catch (Exception e) {
                log.warn("Kerberos doAs ошибка при запросе к {}: {}", url, e.getMessage());
            }
        }

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IOException("HTTP " + response.statusCode() + " from " + url);
        }
        return response.body();
    }
}

package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.*;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockYarnClient implements YarnClient {

    private final ClusterConfig cluster;

    public MockYarnClient(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    @Override
    public String getActiveRmUrl() {
        if (cluster.getResourceManagerUrls() != null && !cluster.getResourceManagerUrls().isEmpty()) {
            return cluster.getResourceManagerUrls().getFirst();
        }
        return "http://rm1." + cluster.getId() + ".company.local:8088";
    }

    @Override
    public ClusterMetrics getClusterMetrics(String doAs) {
        int totalMem = cluster.getTotalResources() != null ? cluster.getTotalResources().getMemoryMb() : 2097152;
        int totalCores = cluster.getTotalResources() != null ? cluster.getTotalResources().getVcores() : 1024;
        int allocatedMem = (int) (totalMem * 0.68);
        int allocatedCores = (int) (totalCores * 0.62);

        int activeNodes = cluster.getId().contains("prod") ? 120 : (cluster.getId().contains("analytics") ? 48 : 24);

        return ClusterMetrics.builder()
                .totalMemoryMb(totalMem)
                .totalVcores(totalCores)
                .allocatedMemoryMb(allocatedMem)
                .allocatedVcores(allocatedCores)
                .availableMemoryMb(totalMem - allocatedMem)
                .availableVcores(totalCores - allocatedCores)
                .activeNodes(activeNodes)
                .unhealthyNodes(0)
                .totalContainers(342)
                .runningApps(45)
                .partitions(cluster.getPartitions())
                .build();
    }

    @Override
    public QueueNode getQueueTree(String doAs) {
        int totalMem = cluster.getTotalResources() != null ? cluster.getTotalResources().getMemoryMb() : 2097152;
        int totalCores = cluster.getTotalResources() != null ? cluster.getTotalResources().getVcores() : 1024;
        List<String> partitions = cluster.getPartitions() != null ? cluster.getPartitions() : List.of("DEFAULT");

        // spark
        Map<String, PartitionResourceConfig> sparkParts = new HashMap<>();
        sparkParts.put("DEFAULT", makePartition("DEFAULT", 40.0, 80.0, totalMem, totalCores));
        if (partitions.contains("GPU")) {
            sparkParts.put("GPU", makePartition("GPU", 60.0, 100.0, totalMem, totalCores));
        }

        QueueNode spark = QueueNode.builder()
                .name("spark")
                .path("root.prod.spark")
                .parentPath("root.prod")
                .leaf(true)
                .state(QueueState.RUNNING)
                .userLimitFactor(2.0)
                .orderingPolicy("fair")
                .maxApplications(10000)
                .maxAmResourcePercent(0.3)
                .maxParallelApps(50)
                .maxApplicationLifetime(86400)
                .partitions(sparkParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.24), (int) (totalCores * 0.22)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.21), (int) (totalCores * 0.19)))
                .currentUsedPercent(87.5)
                .numApplications(18)
                .numActiveApplications(14)
                .numPendingApplications(4)
                .children(new ArrayList<>())
                .build();

        // flink
        Map<String, PartitionResourceConfig> flinkParts = new HashMap<>();
        flinkParts.put("DEFAULT", makePartition("DEFAULT", 35.0, 50.0, totalMem, totalCores));
        QueueNode flink = QueueNode.builder()
                .name("flink")
                .path("root.prod.flink")
                .parentPath("root.prod")
                .leaf(true)
                .state(QueueState.RUNNING)
                .userLimitFactor(1.0)
                .orderingPolicy("fifo")
                .maxApplications(5000)
                .maxAmResourcePercent(0.2)
                .maxParallelApps(20)
                .maxApplicationLifetime(43200)
                .partitions(flinkParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.21), (int) (totalCores * 0.20)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.18), (int) (totalCores * 0.17)))
                .currentUsedPercent(85.7)
                .numApplications(10)
                .numActiveApplications(8)
                .numPendingApplications(2)
                .children(new ArrayList<>())
                .build();

        // hive
        Map<String, PartitionResourceConfig> hiveParts = new HashMap<>();
        hiveParts.put("DEFAULT", makePartition("DEFAULT", 25.0, 50.0, totalMem, totalCores));
        QueueNode hive = QueueNode.builder()
                .name("hive")
                .path("root.prod.hive")
                .parentPath("root.prod")
                .leaf(true)
                .state(QueueState.RUNNING)
                .userLimitFactor(1.5)
                .orderingPolicy("fair")
                .maxApplications(5000)
                .maxAmResourcePercent(0.25)
                .maxParallelApps(30)
                .maxApplicationLifetime(28800)
                .partitions(hiveParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.15), (int) (totalCores * 0.14)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.12), (int) (totalCores * 0.11)))
                .currentUsedPercent(80.0)
                .numApplications(12)
                .numActiveApplications(11)
                .numPendingApplications(1)
                .children(new ArrayList<>())
                .build();

        // prod (родитель: spark, flink, hive -> 40 + 35 + 25 = 100%)
        Map<String, PartitionResourceConfig> prodParts = new HashMap<>();
        prodParts.put("DEFAULT", makePartition("DEFAULT", 60.0, 100.0, totalMem, totalCores));
        if (partitions.contains("GPU")) {
            prodParts.put("GPU", makePartition("GPU", 80.0, 100.0, totalMem, totalCores));
        }

        QueueNode prod = QueueNode.builder()
                .name("prod")
                .path("root.prod")
                .parentPath("root")
                .leaf(false)
                .state(QueueState.RUNNING)
                .partitions(prodParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.60), (int) (totalCores * 0.56)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.51), (int) (totalCores * 0.47)))
                .currentUsedPercent(85.0)
                .numApplications(40)
                .numActiveApplications(33)
                .numPendingApplications(7)
                .children(new ArrayList<>(List.of(spark, flink, hive)))
                .build();

        // dev
        Map<String, PartitionResourceConfig> devParts = new HashMap<>();
        devParts.put("DEFAULT", makePartition("DEFAULT", 25.0, 50.0, totalMem, totalCores));
        QueueNode dev = QueueNode.builder()
                .name("dev")
                .path("root.dev")
                .parentPath("root")
                .leaf(true)
                .state(QueueState.RUNNING)
                .userLimitFactor(1.0)
                .orderingPolicy("fifo")
                .partitions(devParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.20), (int) (totalCores * 0.18)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.14), (int) (totalCores * 0.12)))
                .currentUsedPercent(70.0)
                .numApplications(5)
                .numActiveApplications(5)
                .numPendingApplications(0)
                .children(new ArrayList<>())
                .build();

        // default
        Map<String, PartitionResourceConfig> defaultParts = new HashMap<>();
        defaultParts.put("DEFAULT", makePartition("DEFAULT", 15.0, 100.0, totalMem, totalCores));
        QueueNode defaultQueue = QueueNode.builder()
                .name("default")
                .path("root.default")
                .parentPath("root")
                .leaf(true)
                .state(QueueState.RUNNING)
                .userLimitFactor(1.0)
                .orderingPolicy("fifo")
                .partitions(defaultParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.14), (int) (totalCores * 0.10)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.03), (int) (totalCores * 0.03)))
                .currentUsedPercent(21.4)
                .numApplications(0)
                .numActiveApplications(0)
                .numPendingApplications(0)
                .children(new ArrayList<>())
                .build();

        // root (дети: prod 60%, dev 25%, default 15% -> 100%)
        Map<String, PartitionResourceConfig> rootParts = new HashMap<>();
        rootParts.put("DEFAULT", makePartition("DEFAULT", 100.0, 100.0, totalMem, totalCores));
        if (partitions.contains("GPU")) {
            rootParts.put("GPU", makePartition("GPU", 100.0, 100.0, totalMem, totalCores));
        }

        QueueNode root = QueueNode.builder()
                .name("root")
                .path("root")
                .parentPath(null)
                .leaf(false)
                .state(QueueState.RUNNING)
                .partitions(rootParts)
                .allocatedResources(new ResourceAllocation((int) (totalMem * 0.94), (int) (totalCores * 0.84)))
                .currentUsedResources(new ResourceAllocation((int) (totalMem * 0.68), (int) (totalCores * 0.62)))
                .currentUsedPercent(72.3)
                .numApplications(45)
                .numActiveApplications(38)
                .numPendingApplications(7)
                .children(new ArrayList<>(List.of(prod, dev, defaultQueue)))
                .build();

        applyResourceMode(root, cluster.getResourceMode());
        return root;
    }

    private void applyResourceMode(QueueNode node, String resourceMode) {
        node.setResourceMode(resourceMode);
        for (QueueNode child : node.getChildren()) {
            applyResourceMode(child, resourceMode);
        }
    }

    private PartitionResourceConfig makePartition(String name, double cap, double maxCap, int totalMem, int totalCores) {
        int mem = (int) (totalMem * (cap / 100.0));
        int cores = (int) (totalCores * (cap / 100.0));
        int maxMem = (int) (totalMem * (maxCap / 100.0));
        int maxCores = (int) (totalCores * (maxCap / 100.0));

        return PartitionResourceConfig.builder()
                .partitionName(name)
                .capacity(cap)
                .maxCapacity(maxCap)
                .elastic(maxCap > cap)
                .elasticityRatio(cap > 0 ? Math.round((maxCap / cap) * 100.0) / 100.0 : 1.0)
                .memoryMb(mem)
                .vcores(cores)
                .maxMemoryMb(maxMem)
                .maxVcores(maxCores)
                .memoryPercent(cap)
                .vcorePercent(cap)
                .maxMemoryPercent(maxCap)
                .maxVcorePercent(maxCap)
                .absoluteResources(new ResourceAllocation(mem, cores))
                .absoluteMaxResources(new ResourceAllocation(maxMem, maxCores))
                .build();
    }

    @Override
    public String getCapacitySchedulerXml(String doAs) {
        try {
            ClassPathResource resource = new ClassPathResource("capacity-scheduler-template.xml");
            try (InputStream is = resource.getInputStream()) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            return null;
        }
    }
}

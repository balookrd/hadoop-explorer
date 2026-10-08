package org.apache.hadoop.explorer.hdfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.apache.hadoop.explorer.hdfs.client.HdfsFileSystemClient;
import org.apache.hadoop.explorer.hdfs.client.MockHdfsClient;
import org.apache.hadoop.explorer.hdfs.client.NativeHdfsClient;
import org.apache.hadoop.explorer.hdfs.config.HdfsProperties;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterConfig;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class ClusterRegistry {

    private static final Logger log = LoggerFactory.getLogger(ClusterRegistry.class);

    private final HdfsProperties properties;
    private final ConcurrentMap<String, ClusterConfig> clusterConfigs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, HdfsFileSystemClient> clients = new ConcurrentHashMap<>();

    public ClusterRegistry(HdfsProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        // 1. Загрузка из application.yml
        if (properties.getClusters() != null) {
            for (ClusterConfig cluster : properties.getClusters()) {
                clusterConfigs.put(cluster.getId(), cluster);
                log.info("Registered cluster from properties: id={}, name={}, mock={}",
                    cluster.getId(), cluster.getName(), cluster.isMockStorage());
            }
        }

        // 2. Загрузка из внешнего YAML файла (если задан путь)
        if (properties.getClustersConfigPath() != null) {
            File file = new File(properties.getClustersConfigPath());
            if (file.exists() && file.isFile()) {
                try {
                    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                    Map<?, ?> root = mapper.readValue(file, Map.class);
                    Object listObj = root.get("clusters");
                    if (listObj instanceof List<?> list) {
                        for (Object item : list) {
                            ClusterConfig cfg = mapper.convertValue(item, ClusterConfig.class);
                            clusterConfigs.put(cfg.getId(), cfg);
                            log.info("Registered cluster from file: id={}, name={}", cfg.getId(), cfg.getName());
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to load clusters from yaml: {}", properties.getClustersConfigPath(), e);
                }
            }
        }
    }

    public List<ClusterConfig> getAllClusters() {
        return new ArrayList<>(clusterConfigs.values());
    }

    public Optional<ClusterConfig> getCluster(String clusterId) {
        return Optional.ofNullable(clusterConfigs.get(clusterId));
    }

    public ClusterConfig getClusterOrThrow(String clusterId) {
        return getCluster(clusterId)
            .orElseThrow(() -> new HdfsLocalizedException(
                "Кластер '" + clusterId + "' не найден", HttpStatus.NOT_FOUND, "ClusterNotFoundException"));
    }

    public HdfsFileSystemClient getClient(String clusterId) {
        ClusterConfig config = getClusterOrThrow(clusterId);
        return clients.computeIfAbsent(clusterId, id -> {
            if (config.isMockStorage()) {
                log.info("Creating MockHdfsClient for cluster {}", id);
                return new MockHdfsClient();
            } else {
                log.info("Creating NativeHdfsClient for cluster {}", id);
                return new NativeHdfsClient(config);
            }
        });
    }

    public void registerCluster(ClusterConfig config) {
        clusterConfigs.put(config.getId(), config);
    }
}

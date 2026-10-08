package org.apache.hadoop.explorer.spark.service;

import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.dto.*;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ClusterService {

    private final SparkProperties sparkProperties;

    public ClusterService(SparkProperties sparkProperties) {
        this.sparkProperties = sparkProperties;
    }

    public List<ClusterSummary> listAllowedClusters(String username, Collection<String> groups, boolean isAdmin) {
        List<ClusterSummary> list = new ArrayList<>();
        for (var c : sparkProperties.getClusters()) {
            if (hasAccess(username, groups, isAdmin, c)) {
                list.add(new ClusterSummary(
                        c.getId(),
                        c.getName(),
                        c.getDescription(),
                        c.getType(),
                        c.getLivyUrl(),
                        c.getYarnClusterId()
                ));
            }
        }
        return list;
    }

    public Optional<ClusterDetailResponse> getClusterDetail(String clusterId, String username, Collection<String> groups, boolean isAdmin) {
        return findCluster(clusterId)
                .filter(c -> hasAccess(username, groups, isAdmin, c))
                .map(c -> {
                    List<SparkVersionItem> versions = c.getSparkVersions().stream().map(v ->
                            new SparkVersionItem(
                                    v.getId(),
                                    v.getName(),
                                    v.isDefaultVersion(),
                                    v.getPythonVersions().stream().map(p ->
                                            new PythonEnvItem(p.getId(), p.getName(), p.isDefaultEnv())
                                    ).toList()
                            )
                    ).toList();

                    List<MetastoreItem> metastores = c.getMetastores().stream().map(m ->
                            new MetastoreItem(m.getId(), m.getName(), m.isDefaultMetastore())
                    ).toList();

                    List<ResourceProfileItem> profiles = c.getResourceProfiles().stream().map(r ->
                            new ResourceProfileItem(
                                    r.getId(),
                                    r.getName(),
                                    r.getDriverMemory(),
                                    r.getDriverCores(),
                                    r.getExecutorMemory(),
                                    r.getExecutorCores(),
                                    r.getNumExecutors()
                            )
                    ).toList();

                    return new ClusterDetailResponse(
                            c.getId(),
                            c.getName(),
                            c.getDescription(),
                            c.getType(),
                            c.getYarnClusterId(),
                            versions,
                            metastores,
                            c.getYarnQueues(),
                            c.getDefaultQueue(),
                            profiles,
                            c.getDefaultRepositories()
                    );
                });
    }

    public Optional<SparkProperties.SparkClusterConfig> findCluster(String clusterId) {
        return sparkProperties.getClusters().stream()
                .filter(c -> c.getId().equalsIgnoreCase(clusterId))
                .findFirst();
    }

    public boolean hasAccess(String username, Collection<String> groups, boolean isAdmin, SparkProperties.SparkClusterConfig cluster) {
        if (isAdmin) return true;
        if (cluster.getAllowedUsers().isEmpty() && cluster.getAllowedGroups().isEmpty()) {
            return true;
        }
        if (username != null && cluster.getAllowedUsers().contains(username)) {
            return true;
        }
        if (groups != null) {
            for (String g : groups) {
                if (cluster.getAllowedGroups().contains(g)) return true;
            }
        }
        return false;
    }
}

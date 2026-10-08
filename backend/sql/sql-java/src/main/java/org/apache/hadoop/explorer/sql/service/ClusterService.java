package org.apache.hadoop.explorer.sql.service;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.ClusterSummary;
import org.apache.hadoop.explorer.sql.service.engine.HiveSqlEngine;
import org.apache.hadoop.explorer.sql.service.engine.MockSqlEngine;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngine;
import org.apache.hadoop.explorer.sql.service.engine.TrinoSqlEngine;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ClusterService {

    private final SqlProperties sqlProperties;
    private final Map<String, SqlEngine> engineCache = new ConcurrentHashMap<>();

    public ClusterService(SqlProperties sqlProperties) {
        this.sqlProperties = sqlProperties;
    }

    public List<ClusterSummary> listAllowedClusters(String username, Collection<String> groups, boolean isAdmin) {
        List<ClusterSummary> result = new ArrayList<>();
        for (SqlProperties.ClusterConfig cluster : sqlProperties.getClusters()) {
            if (hasAccess(username, groups, isAdmin, cluster)) {
                result.add(new ClusterSummary(
                        cluster.getId(),
                        cluster.getName(),
                        cluster.getType(),
                        cluster.getHost(),
                        cluster.getPort(),
                        cluster.getImpersonation().isEnabled(),
                        cluster.getImpersonation().getMethod(),
                        cluster.getCatalog(),
                        cluster.getSchema()
                ));
            }
        }
        return result;
    }

    public Optional<SqlProperties.ClusterConfig> findCluster(String clusterId) {
        return sqlProperties.getClusters().stream()
                .filter(c -> c.getId().equalsIgnoreCase(clusterId))
                .findFirst();
    }

    public boolean hasAccess(String username, Collection<String> groups, boolean isAdmin, SqlProperties.ClusterConfig cluster) {
        if (isAdmin) {
            return true;
        }
        if (cluster.getAllowedUsers().isEmpty() && cluster.getAllowedGroups().isEmpty()) {
            return true;
        }
        if (username != null && cluster.getAllowedUsers().contains(username)) {
            return true;
        }
        if (groups != null) {
            for (String group : groups) {
                if (cluster.getAllowedGroups().contains(group)) {
                    return true;
                }
            }
        }
        return false;
    }

    public SqlEngine getEngine(SqlProperties.ClusterConfig cluster) {
        return engineCache.computeIfAbsent(cluster.getId(), id -> {
            if (cluster.isMockStorage()) {
                return new MockSqlEngine(cluster);
            }
            return switch (cluster.getType().toLowerCase()) {
                case "trino" -> new TrinoSqlEngine(cluster);
                case "hive" -> new HiveSqlEngine(cluster);
                default -> new MockSqlEngine(cluster);
            };
        });
    }
}

package org.apache.hadoop.explorer.spark.service;

import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.dto.ColumnMetadata;
import org.apache.hadoop.explorer.spark.service.engine.LivyClient;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SparkCatalogService {

    private final LivyClient livyClient;
    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    public SparkCatalogService(LivyClient livyClient) {
        this.livyClient = livyClient;
    }

    public List<String> getDatabases(SparkProperties.SparkClusterConfig cluster, String metastoreId, boolean refresh) {
        String key = cluster.getId() + ":" + metastoreId + ":dbs";
        if (!refresh && cache.containsKey(key)) {
            @SuppressWarnings("unchecked")
            List<String> cached = (List<String>) cache.get(key);
            return cached;
        }
        List<String> dbs = livyClient.getMockEngine().getDatabases();
        cache.put(key, dbs);
        return dbs;
    }

    public List<String> getTables(SparkProperties.SparkClusterConfig cluster, String database, String metastoreId, boolean refresh) {
        String key = cluster.getId() + ":" + database + ":" + metastoreId + ":tables";
        if (!refresh && cache.containsKey(key)) {
            @SuppressWarnings("unchecked")
            List<String> cached = (List<String>) cache.get(key);
            return cached;
        }
        List<String> tables = livyClient.getMockEngine().getTables(database);
        cache.put(key, tables);
        return tables;
    }

    public List<ColumnMetadata> getColumns(SparkProperties.SparkClusterConfig cluster, String database, String table, String metastoreId, boolean refresh) {
        String key = cluster.getId() + ":" + database + ":" + table + ":" + metastoreId + ":cols";
        if (!refresh && cache.containsKey(key)) {
            @SuppressWarnings("unchecked")
            List<ColumnMetadata> cached = (List<ColumnMetadata>) cache.get(key);
            return cached;
        }
        List<ColumnMetadata> cols = livyClient.getMockEngine().getColumns(database, table);
        cache.put(key, cols);
        return cols;
    }
}

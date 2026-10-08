package org.apache.hadoop.explorer.sql.service;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;
import org.apache.hadoop.explorer.sql.service.engine.SqlEngine;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CatalogService {

    private final ClusterService clusterService;
    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    public CatalogService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public List<String> getCatalogs(SqlProperties.ClusterConfig cluster, String username, boolean refresh) {
        String cacheKey = cluster.getId() + ":" + username + ":catalogs";
        if (!refresh && cache.containsKey(cacheKey)) {
            @SuppressWarnings("unchecked")
            List<String> cached = (List<String>) cache.get(cacheKey);
            return cached;
        }
        SqlEngine engine = clusterService.getEngine(cluster);
        List<String> catalogs = engine.getCatalogs(username);
        cache.put(cacheKey, catalogs);
        return catalogs;
    }

    public List<String> getSchemas(SqlProperties.ClusterConfig cluster, String username, String catalog, boolean refresh) {
        String cacheKey = cluster.getId() + ":" + username + ":" + catalog + ":schemas";
        if (!refresh && cache.containsKey(cacheKey)) {
            @SuppressWarnings("unchecked")
            List<String> cached = (List<String>) cache.get(cacheKey);
            return cached;
        }
        SqlEngine engine = clusterService.getEngine(cluster);
        List<String> schemas = engine.getSchemas(username, catalog);
        cache.put(cacheKey, schemas);
        return schemas;
    }

    public List<String> getTables(SqlProperties.ClusterConfig cluster, String username, String catalog, String schema, boolean refresh) {
        String cacheKey = cluster.getId() + ":" + username + ":" + catalog + ":" + schema + ":tables";
        if (!refresh && cache.containsKey(cacheKey)) {
            @SuppressWarnings("unchecked")
            List<String> cached = (List<String>) cache.get(cacheKey);
            return cached;
        }
        SqlEngine engine = clusterService.getEngine(cluster);
        List<String> tables = engine.getTables(username, catalog, schema);
        cache.put(cacheKey, tables);
        return tables;
    }

    public List<ColumnMetadata> getColumns(SqlProperties.ClusterConfig cluster, String username, String catalog, String schema, String table, boolean refresh) {
        String cacheKey = cluster.getId() + ":" + username + ":" + catalog + ":" + schema + ":" + table + ":columns";
        if (!refresh && cache.containsKey(cacheKey)) {
            @SuppressWarnings("unchecked")
            List<ColumnMetadata> cached = (List<ColumnMetadata>) cache.get(cacheKey);
            return cached;
        }
        SqlEngine engine = clusterService.getEngine(cluster);
        List<ColumnMetadata> columns = engine.getColumns(username, catalog, schema, table);
        cache.put(cacheKey, columns);
        return columns;
    }
}

package org.apache.hadoop.explorer.sql.controller;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.ColumnMetadata;
import org.apache.hadoop.explorer.sql.service.CatalogService;
import org.apache.hadoop.explorer.sql.service.ClusterService;
import org.apache.hadoop.explorer.sql.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/v1/catalog")
public class CatalogController {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-]+$");

    private final ClusterService clusterService;
    private final CatalogService catalogService;

    public CatalogController(ClusterService clusterService, CatalogService catalogService) {
        this.clusterService = clusterService;
        this.catalogService = catalogService;
    }

    private void validateIdentifier(String name, String field) {
        if (name == null || !IDENTIFIER_PATTERN.matcher(name.trim()).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Недопустимые символы в параметре " + field + ": '" + name + "'");
        }
    }

    private SqlProperties.ClusterConfig getClusterOrThrow(String clusterId, Authentication auth) {
        SqlProperties.ClusterConfig cluster = clusterService.findCluster(clusterId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Кластер не найден: " + clusterId));

        String username = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        boolean isAdmin = SecurityUtils.isAdmin(auth);

        if (!clusterService.hasAccess(username, groups, isAdmin, cluster)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Доступ к данному кластеру запрещен ACL");
        }
        return cluster;
    }

    @GetMapping("/{clusterId}/catalogs")
    public List<String> getCatalogs(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        SqlProperties.ClusterConfig cluster = getClusterOrThrow(clusterId, authentication);
        String username = SecurityUtils.getUsername(authentication);
        return catalogService.getCatalogs(cluster, username, refresh);
    }

    @GetMapping("/{clusterId}/schemas")
    public List<String> getSchemas(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "catalog", defaultValue = "hive") String catalog,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        validateIdentifier(catalog, "catalog");
        SqlProperties.ClusterConfig cluster = getClusterOrThrow(clusterId, authentication);
        String username = SecurityUtils.getUsername(authentication);
        return catalogService.getSchemas(cluster, username, catalog, refresh);
    }

    @GetMapping("/{clusterId}/tables")
    public List<String> getTables(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "catalog", defaultValue = "hive") String catalog,
            @RequestParam(name = "schema", defaultValue = "default") String schema,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        validateIdentifier(catalog, "catalog");
        validateIdentifier(schema, "schema");
        SqlProperties.ClusterConfig cluster = getClusterOrThrow(clusterId, authentication);
        String username = SecurityUtils.getUsername(authentication);
        return catalogService.getTables(cluster, username, catalog, schema, refresh);
    }

    @GetMapping("/{clusterId}/columns")
    public List<ColumnMetadata> getColumns(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "catalog", defaultValue = "hive") String catalog,
            @RequestParam(name = "schema", defaultValue = "default") String schema,
            @RequestParam(name = "table") String table,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        validateIdentifier(catalog, "catalog");
        validateIdentifier(schema, "schema");
        validateIdentifier(table, "table");
        SqlProperties.ClusterConfig cluster = getClusterOrThrow(clusterId, authentication);
        String username = SecurityUtils.getUsername(authentication);
        return catalogService.getColumns(cluster, username, catalog, schema, table, refresh);
    }
}

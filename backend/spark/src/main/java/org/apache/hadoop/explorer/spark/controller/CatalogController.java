package org.apache.hadoop.explorer.spark.controller;

import org.apache.hadoop.explorer.spark.config.SparkProperties;
import org.apache.hadoop.explorer.spark.dto.ColumnMetadata;
import org.apache.hadoop.explorer.spark.service.ClusterService;
import org.apache.hadoop.explorer.spark.service.SparkCatalogService;
import org.apache.hadoop.explorer.spark.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/catalog")
public class CatalogController {

    private final ClusterService clusterService;
    private final SparkCatalogService catalogService;

    public CatalogController(ClusterService clusterService, SparkCatalogService catalogService) {
        this.clusterService = clusterService;
        this.catalogService = catalogService;
    }

    /**
     * The reason goes to the client as "detail": the catalog tree shows it, e.g. the 409 "start a session"
     * on a Livy cluster. Without this handler the error page carries no message (server.error.include-message).
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> statusError(ResponseStatusException e) {
        String detail = e.getReason() != null ? e.getReason() : e.getStatusCode().toString();
        return ResponseEntity.status(e.getStatusCode()).body(Map.of("detail", detail, "status", e.getStatusCode().value()));
    }

    private SparkProperties.SparkClusterConfig getClusterOrThrow(String clusterId, Authentication auth) {
        SparkProperties.SparkClusterConfig cluster = clusterService.findCluster(clusterId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Кластер не найден: " + clusterId));

        String username = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        boolean isAdmin = SecurityUtils.isAdmin(auth);

        if (!clusterService.hasAccess(username, groups, isAdmin, cluster)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Доступ к кластеру запрещен");
        }
        return cluster;
    }

    @GetMapping("/{clusterId}/catalogs")
    public List<String> getCatalogs(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        getClusterOrThrow(clusterId, authentication);
        return List.of("spark_catalog");
    }

    @GetMapping("/{clusterId}/databases")
    public List<String> getDatabases(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "metastore_id", required = false) String metastoreId,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        var cluster = getClusterOrThrow(clusterId, authentication);
        return catalogService.getDatabases(cluster, SecurityUtils.getUsername(authentication), metastoreId, refresh);
    }

    @GetMapping("/{clusterId}/tables")
    public List<String> getTables(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "database", defaultValue = "default") String database,
            @RequestParam(name = "metastore_id", required = false) String metastoreId,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        var cluster = getClusterOrThrow(clusterId, authentication);
        return catalogService.getTables(cluster, SecurityUtils.getUsername(authentication), database, metastoreId, refresh);
    }

    @GetMapping("/{clusterId}/columns")
    public List<ColumnMetadata> getColumns(
            @PathVariable("clusterId") String clusterId,
            @RequestParam(name = "database", defaultValue = "default") String database,
            @RequestParam(name = "table", defaultValue = "") String table,
            @RequestParam(name = "metastore_id", required = false) String metastoreId,
            @RequestParam(name = "refresh", defaultValue = "false") boolean refresh,
            Authentication authentication
    ) {
        var cluster = getClusterOrThrow(clusterId, authentication);
        return catalogService.getColumns(cluster, SecurityUtils.getUsername(authentication), database, table, metastoreId, refresh);
    }
}

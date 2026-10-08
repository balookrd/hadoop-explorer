package org.apache.hadoop.explorer.spark.controller;

import org.apache.hadoop.explorer.spark.dto.ClusterDetailResponse;
import org.apache.hadoop.explorer.spark.dto.ClusterSummary;
import org.apache.hadoop.explorer.spark.service.ClusterService;
import org.apache.hadoop.explorer.spark.util.SecurityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/api/v1/clusters")
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping
    public List<ClusterSummary> listClusters(Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        List<String> groups = SecurityUtils.getGroups(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);
        return clusterService.listAllowedClusters(username, groups, isAdmin);
    }

    @GetMapping("/{clusterId}")
    public ClusterDetailResponse getClusterDetail(@PathVariable("clusterId") String clusterId, Authentication authentication) {
        String username = SecurityUtils.getUsername(authentication);
        List<String> groups = SecurityUtils.getGroups(authentication);
        boolean isAdmin = SecurityUtils.isAdmin(authentication);
        return clusterService.getClusterDetail(clusterId, username, groups, isAdmin)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Кластер не найден: " + clusterId));
    }
}

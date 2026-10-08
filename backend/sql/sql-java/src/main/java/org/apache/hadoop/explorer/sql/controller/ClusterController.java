package org.apache.hadoop.explorer.sql.controller;

import org.apache.hadoop.explorer.sql.dto.ClusterSummary;
import org.apache.hadoop.explorer.sql.service.ClusterService;
import org.apache.hadoop.explorer.sql.util.SecurityUtils;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
}

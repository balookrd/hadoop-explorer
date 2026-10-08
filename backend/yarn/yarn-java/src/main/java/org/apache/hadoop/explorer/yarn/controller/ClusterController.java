package org.apache.hadoop.explorer.yarn.controller;

import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.ClusterSummary;
import org.apache.hadoop.explorer.yarn.service.ChangeRequestService;
import org.apache.hadoop.explorer.yarn.service.YarnClient;
import org.apache.hadoop.explorer.yarn.service.YarnClientFactory;
import org.apache.hadoop.explorer.yarn.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/v1/clusters")
public class ClusterController {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    private final YarnProperties yarnProperties;
    private final YarnClientFactory yarnClientFactory;
    private final ChangeRequestService changeRequestService;

    public ClusterController(
            YarnProperties yarnProperties,
            YarnClientFactory yarnClientFactory,
            ChangeRequestService changeRequestService
    ) {
        this.yarnProperties = yarnProperties;
        this.yarnClientFactory = yarnClientFactory;
        this.changeRequestService = changeRequestService;
    }

    @GetMapping
    public ResponseEntity<List<ClusterSummary>> getClusters(Authentication auth) {
        UserSession user = SecurityUtils.getUserSession(auth);
        List<ClusterSummary> result = new ArrayList<>();

        for (ClusterConfig cluster : yarnProperties.getClusters()) {
            Role role = changeRequestService.resolveClusterRole(user, cluster);
            if (role == null) {
                continue;
            }

            String activeRmUrl = null;
            try {
                YarnClient client = yarnClientFactory.getClient(cluster);
                activeRmUrl = client.getActiveRmUrl();
            } catch (Exception e) {
                log.debug("Не удалось получить active RM URL для {}: {}", cluster.getId(), e.getMessage());
                if (cluster.getResourceManagerUrls() != null && !cluster.getResourceManagerUrls().isEmpty()) {
                    activeRmUrl = cluster.getResourceManagerUrls().getFirst();
                }
            }

            result.add(ClusterSummary.builder()
                    .id(cluster.getId())
                    .name(cluster.getName())
                    .description(cluster.getDescription())
                    .activeRmUrl(activeRmUrl)
                    .kerberosEnabled(cluster.isKerberosEnabled())
                    .impersonationEnabled(cluster.isImpersonationEnabled())
                    .partitions(cluster.getPartitions())
                    .defaultPartition(cluster.getDefaultPartition())
                    .resourceMode(cluster.getResourceMode())
                    .totalResources(cluster.getTotalResources())
                    .userRole(role)
                    .canWrite(role == Role.WRITER || role == Role.ADMIN)
                    .canAdmin(role == Role.ADMIN)
                    .build());
        }

        return ResponseEntity.ok(result);
    }
}

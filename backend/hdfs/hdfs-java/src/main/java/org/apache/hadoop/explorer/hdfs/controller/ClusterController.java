package org.apache.hadoop.explorer.hdfs.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterPublicInfo;
import org.apache.hadoop.explorer.hdfs.dto.file.CrossClusterCopyRequest;
import org.apache.hadoop.explorer.hdfs.dto.file.CrossClusterCopyResponse;
import org.apache.hadoop.explorer.hdfs.service.ClusterAclService;
import org.apache.hadoop.explorer.hdfs.service.ClusterRegistry;
import org.apache.hadoop.explorer.hdfs.service.HdfsFileOperationService;
import org.apache.hadoop.explorer.hdfs.util.SecurityUtils;
import org.apache.hadoop.explorer.common.audit.Audited;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/clusters")
public class ClusterController {

    private final ClusterRegistry clusterRegistry;
    private final ClusterAclService aclService;
    private final HdfsFileOperationService fileOperationService;

    public ClusterController(ClusterRegistry clusterRegistry,
                             ClusterAclService aclService,
                             HdfsFileOperationService fileOperationService) {
        this.clusterRegistry = clusterRegistry;
        this.aclService = aclService;
        this.fileOperationService = fileOperationService;
    }

    @GetMapping
    public ResponseEntity<List<ClusterPublicInfo>> listClusters(Authentication auth) {
        String username = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        List<ClusterPublicInfo> visible = aclService.getVisibleClusters(
            clusterRegistry.getAllClusters(), username, groups);
        return ResponseEntity.ok(visible);
    }

    @PostMapping("/cross-copy")
    @Audited(action = "CROSS_CLUSTER_COPY", resource = "HDFS")
    public ResponseEntity<CrossClusterCopyResponse> crossClusterCopy(
            @Valid @RequestBody CrossClusterCopyRequest req,
            Authentication auth) {
        String username = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        CrossClusterCopyResponse resp = fileOperationService.crossClusterCopy(req, username, groups);
        return ResponseEntity.ok(resp);
    }
}

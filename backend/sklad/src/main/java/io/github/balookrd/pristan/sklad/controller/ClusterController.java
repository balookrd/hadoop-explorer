package io.github.balookrd.pristan.sklad.controller;

import jakarta.validation.Valid;
import io.github.balookrd.pristan.sklad.dto.cluster.ClusterPublicInfo;
import io.github.balookrd.pristan.sklad.dto.file.CrossClusterCopyRequest;
import io.github.balookrd.pristan.sklad.dto.file.CrossClusterCopyResponse;
import io.github.balookrd.pristan.sklad.service.ClusterAclService;
import io.github.balookrd.pristan.sklad.service.ClusterRegistry;
import io.github.balookrd.pristan.sklad.service.HdfsFileOperationService;
import io.github.balookrd.pristan.sklad.util.SecurityUtils;
import io.github.balookrd.pristan.zastava.audit.Audited;
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

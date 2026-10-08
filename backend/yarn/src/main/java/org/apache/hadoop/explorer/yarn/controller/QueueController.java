package org.apache.hadoop.explorer.yarn.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.audit.Audited;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.yarn.model.*;
import org.apache.hadoop.explorer.yarn.service.*;
import org.apache.hadoop.explorer.yarn.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/v1/clusters")
public class QueueController {

    private static final Logger log = LoggerFactory.getLogger(QueueController.class);

    private final ChangeRequestService changeRequestService;
    private final YarnClientFactory yarnClientFactory;
    private final CapacitySchedulerService schedulerService;
    private final XmlGeneratorService xmlGeneratorService;
    private final AwxClientService awxClientService;

    public QueueController(
            ChangeRequestService changeRequestService,
            YarnClientFactory yarnClientFactory,
            CapacitySchedulerService schedulerService,
            XmlGeneratorService xmlGeneratorService,
            AwxClientService awxClientService
    ) {
        this.changeRequestService = changeRequestService;
        this.yarnClientFactory = yarnClientFactory;
        this.schedulerService = schedulerService;
        this.xmlGeneratorService = xmlGeneratorService;
        this.awxClientService = awxClientService;
    }

    @GetMapping("/{clusterId}/queues")
    public ResponseEntity<QueueTreeResponse> getQueueTree(
            @PathVariable String clusterId,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ClusterConfig cluster = changeRequestService.findClusterOrThrow(clusterId);
        changeRequestService.checkClusterPermission(user, cluster, Role.READER);

        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode rootQueue = client.getQueueTree(user.username());
        ClusterMetrics metrics = client.getClusterMetrics(user.username());

        List<BranchBalance> balances = schedulerService.computeBalancesFromTree(rootQueue, cluster.getDefaultPartition());

        return ResponseEntity.ok(QueueTreeResponse.builder()
                .clusterId(cluster.getId())
                .clusterName(cluster.getName())
                .resourceMode(cluster.getResourceMode())
                .defaultPartition(cluster.getDefaultPartition())
                .partitions(cluster.getPartitions())
                .rootQueue(rootQueue)
                .clusterMetrics(metrics)
                .balances(balances)
                .queueMappings(cluster.getQueueMappings())
                .queueMappingsOverride(cluster.isQueueMappingsOverride())
                .build());
    }

    @PostMapping("/{clusterId}/validate")
    public ResponseEntity<DraftValidateResponse> validateDraft(
            @PathVariable String clusterId,
            @Valid @RequestBody DraftValidateRequest body,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ClusterConfig cluster = changeRequestService.findClusterOrThrow(clusterId);
        changeRequestService.checkClusterPermission(user, cluster, Role.WRITER);

        List<BranchBalance> balances = schedulerService.validateQueueBalance(
                body.getQueues(),
                cluster.getResourceMode(),
                body.getSelectedPartition()
        );

        List<String> errors = new ArrayList<>();
        for (BranchBalance b : balances) {
            if (!b.isBalanced()) {
                errors.add(b.getMessage());
            }
        }

        return ResponseEntity.ok(DraftValidateResponse.builder()
                .valid(errors.isEmpty())
                .balances(balances)
                .errors(errors)
                .warnings(new ArrayList<>())
                .build());
    }

    @PostMapping("/{clusterId}/diff")
    public ResponseEntity<DraftDiffResponse> getDiff(
            @PathVariable String clusterId,
            @Valid @RequestBody DraftValidateRequest body,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ClusterConfig cluster = changeRequestService.findClusterOrThrow(clusterId);
        changeRequestService.checkClusterPermission(user, cluster, Role.WRITER);

        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());

        DraftDiffResponse diffResponse = schedulerService.computeDiff(
                cluster,
                body.getQueues(),
                liveRoot,
                body.getSelectedPartition(),
                body.getQueueMappings(),
                body.getQueueMappingsOverride()
        );

        return ResponseEntity.ok(diffResponse);
    }

    @PostMapping("/{clusterId}/generate-xml")
    @Audited(action = "GENERATE_XML", resource = "YARN")
    public ResponseEntity<GenerateXmlResponse> generateXml(
            @PathVariable String clusterId,
            @Valid @RequestBody GenerateXmlRequest body,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ClusterConfig cluster = changeRequestService.findClusterOrThrow(clusterId);
        changeRequestService.checkClusterPermission(user, cluster, Role.ADMIN);

        YarnClient client = yarnClientFactory.getClient(cluster);
        String baseXml = client.getCapacitySchedulerXml(user.username());

        String generatedXml = xmlGeneratorService.generateCapacitySchedulerXml(
                body.getQueues(),
                cluster,
                user.username(),
                body.getProposalComment() != null ? body.getProposalComment() : "",
                body.getResourceModeOverride() != null ? body.getResourceModeOverride() : cluster.getResourceMode(),
                body.getQueueMappings(),
                body.getQueueMappingsOverride(),
                baseXml
        );

        String now = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());

        String fileTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());

        String instructions = "Инструкции по применению:\n" +
                "1. Скопируйте файл capacity-scheduler.xml на все RM узлы кластера '" + cluster.getName() + "'\n" +
                "   Путь: /etc/hadoop/conf/capacity-scheduler.xml\n" +
                "2. Выполните на активном ResourceManager:\n" +
                "   yarn rmadmin -refreshQueues\n" +
                "3. Проверьте в YARN UI: " + client.getActiveRmUrl() + "/cluster/scheduler\n";

        return ResponseEntity.ok(GenerateXmlResponse.builder()
                .clusterId(clusterId)
                .filename("capacity-scheduler-" + clusterId + "-" + fileTimestamp + ".xml")
                .xmlContent(generatedXml)
                .appliedBy(user.username())
                .generatedAt(now)
                .instructions(instructions)
                .build());
    }

    @PostMapping("/{clusterId}/deploy-xml")
    @Audited(action = "DEPLOY_DIRECT_XML", resource = "YARN")
    public ResponseEntity<DirectDeployXmlResponse> deployDirectXml(
            @PathVariable String clusterId,
            @Valid @RequestBody DirectDeployXmlRequest body,
            Authentication auth
    ) {
        UserSession user = SecurityUtils.getUserSession(auth);
        ClusterConfig cluster = changeRequestService.findClusterOrThrow(clusterId);
        changeRequestService.checkClusterPermission(user, cluster, Role.ADMIN);

        DeployResponse deployResp = awxClientService.deployXml(
                cluster,
                null,
                body.getXmlContent(),
                body.getComment() != null ? body.getComment() : "Direct XML deploy by " + user.username(),
                true
        );

        return ResponseEntity.ok(DirectDeployXmlResponse.builder()
                .clusterId(clusterId)
                .awxJobId(deployResp.getAwxJobId())
                .status(deployResp.getStatus())
                .message(deployResp.getMessage())
                .deployedAt(deployResp.getDeployedAt())
                .stdout(deployResp.getStdout())
                .build());
    }
}

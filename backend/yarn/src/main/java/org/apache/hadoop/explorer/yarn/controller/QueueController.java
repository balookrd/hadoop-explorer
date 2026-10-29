package org.apache.hadoop.explorer.yarn.controller;

import jakarta.validation.Valid;
import org.apache.hadoop.explorer.common.audit.Audited;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.model.*;
import org.apache.hadoop.explorer.yarn.service.*;
import org.apache.hadoop.explorer.yarn.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

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
    private final CapacitySchedulerPatchService patchService;
    private final YarnProperties yarnProperties;
    private final AwxClientService awxClientService;

    public QueueController(
            ChangeRequestService changeRequestService,
            YarnClientFactory yarnClientFactory,
            CapacitySchedulerService schedulerService,
            CapacitySchedulerPatchService patchService,
            AwxClientService awxClientService,
            YarnProperties yarnProperties
    ) {
        this.changeRequestService = changeRequestService;
        this.yarnClientFactory = yarnClientFactory;
        this.schedulerService = schedulerService;
        this.patchService = patchService;
        this.awxClientService = awxClientService;
        this.yarnProperties = yarnProperties;
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

        // Validated against the live tree with the draft applied, exactly as a change request would be
        QueueNode liveRoot = yarnClientFactory.getClient(cluster).getQueueTree(user.username());
        PatchPlan plan = patchService.buildPlan(cluster, liveRoot, body.getQueues(),
                body.getQueueMappings(), body.getQueueMappingsOverride());

        return ResponseEntity.ok(DraftValidateResponse.builder()
                .valid(plan.isValid())
                .balances(plan.balances())
                .errors(plan.errors())
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
        QueueNode liveRoot = client.getQueueTree(user.username());
        PatchPlan plan = patchService.buildPlan(cluster, liveRoot, body.getQueues(),
                body.getQueueMappings(), body.getQueueMappingsOverride());
        if (!plan.isValid()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Черновик не прошел проверку: " + String.join("; ", plan.errors()));
        }

        String now = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        String fileTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());

        String instructions = "Это перечень правок capacity-scheduler.xml, а не файл целиком: Explorer не может "
                + "прочитать развернутый файл, поэтому меняет только затронутые свойства.\n"
                + "Рекомендуемый путь — оформить заявку на изменение: после согласования AWX применит правки "
                + "к файлу на всех RM и выполнит yarn rmadmin -refreshQueues с автоматическим откатом.\n"
                + "Для ручного применения внесите эти правки в /etc/hadoop/conf/capacity-scheduler.xml на всех "
                + "RM узлах кластера '" + cluster.getName() + "' и выполните yarn rmadmin -refreshQueues; "
                + "результат — " + client.getActiveRmUrl() + "/cluster/scheduler\n";

        return ResponseEntity.ok(GenerateXmlResponse.builder()
                .clusterId(clusterId)
                .filename("capacity-scheduler-patch-" + clusterId + "-" + fileTimestamp + ".xml")
                .xmlContent(patchService.renderPreview(clusterId,
                        body.getProposalComment() != null ? body.getProposalComment() : "Draft by " + user.username(),
                        plan.operations()))
                .operations(plan.operations())
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

        // Replacing the whole file skips review; with four-eyes enforced every change goes through a change request
        if (yarnProperties.getAcl().isEnforceFourEyes()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "Прямое развертывание XML запрещено при yarn.acl.enforce-four-eyes=true: оформите заявку на изменение");
        }
        CapacitySchedulerXmlValidator.validate(body.getXmlContent());

        DeployResponse deployResp = awxClientService.deployXml(
                cluster,
                body.getXmlContent(),
                user.username(),
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

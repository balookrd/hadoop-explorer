package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.model.Role;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.RoleResolver;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.entity.ChangeRequestEntity;
import org.apache.hadoop.explorer.yarn.entity.ChangeRequestStatus;
import org.apache.hadoop.explorer.yarn.model.*;
import org.apache.hadoop.explorer.yarn.repository.ChangeRequestRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class ChangeRequestService {

    private static final Logger log = LoggerFactory.getLogger(ChangeRequestService.class);

    private final ChangeRequestRepository repository;
    private final YarnProperties yarnProperties;
    private final YarnClientFactory yarnClientFactory;
    private final CapacitySchedulerService schedulerService;
    private final CapacitySchedulerPatchService patchService;
    private final AwxClientService awxClientService;
    private final RoleResolver roleResolver;
    private final ObjectMapper objectMapper;

    public ChangeRequestService(
            ChangeRequestRepository repository,
            YarnProperties yarnProperties,
            YarnClientFactory yarnClientFactory,
            CapacitySchedulerService schedulerService,
            CapacitySchedulerPatchService patchService,
            AwxClientService awxClientService,
            RoleResolver roleResolver,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.yarnProperties = yarnProperties;
        this.yarnClientFactory = yarnClientFactory;
        this.schedulerService = schedulerService;
        this.patchService = patchService;
        this.awxClientService = awxClientService;
        this.roleResolver = roleResolver;
        this.objectMapper = objectMapper;
    }

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC);

    @Transactional
    public ChangeRequestResponse createChangeRequest(ChangeRequestCreate request, UserSession user) {
        ClusterConfig cluster = findClusterOrThrow(request.getClusterId());
        checkClusterPermission(user, cluster, Role.WRITER);

        List<QueueDraftItem> changes = request.getChanges() != null ? request.getChanges() : List.of();
        if (changes.isEmpty() && request.getQueueMappings() == null && request.getQueueMappingsOverride() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Заявка не содержит изменений");
        }

        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());

        // Reject at submission what could never be deployed, instead of failing at refreshQueues
        PatchPlan plan = patchService.buildPlan(cluster, liveRoot, copyOf(changes),
                request.getQueueMappings(), request.getQueueMappingsOverride());
        requireValid(plan, "Заявка не прошла проверку");
        if (plan.operations().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Заявка не меняет конфигурацию кластера");
        }

        DraftDiffResponse diffResponse = schedulerService.computeDiff(
                cluster,
                changes,
                liveRoot,
                cluster.getDefaultPartition(),
                request.getQueueMappings(),
                request.getQueueMappingsOverride()
        );

        String changesJson;
        String diffsJson;
        try {
            changesJson = objectMapper.writeValueAsString(changes);
            diffsJson = objectMapper.writeValueAsString(diffResponse.getDiffs());
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Ошибка сериализации данных заявки: " + e.getMessage());
        }

        ChangeRequestEntity entity = ChangeRequestEntity.builder()
                .clusterId(cluster.getId())
                .title(request.getTitle())
                .description(request.getDescription())
                .author(user.username())
                .status(ChangeRequestStatus.SUBMITTED)
                .changesJson(changesJson)
                .diffsJson(diffsJson)
                .queueMappings(request.getQueueMappings())
                .queueMappingsOverride(request.getQueueMappingsOverride())
                .build();

        ChangeRequestEntity saved = repository.save(entity);
        log.info("Создана заявка на изменение #{} для кластера '{}' пользователем '{}'", saved.getId(), cluster.getId(), user.username());
        return mapToResponse(saved);
    }

    @Transactional(readOnly = true)
    public List<ChangeRequestSummary> listChangeRequests(String clusterId, String statusStr, UserSession user) {
        ChangeRequestStatus status = null;
        if (statusStr != null && !statusStr.isBlank()) {
            try {
                status = ChangeRequestStatus.valueOf(statusStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Неизвестный статус заявки: " + statusStr
                        + " (допустимо: " + Arrays.toString(ChangeRequestStatus.values()) + ")");
            }
        }

        List<ChangeRequestEntity> entities;
        if (clusterId != null && !clusterId.isBlank()) {
            ClusterConfig cluster = findClusterOrThrow(clusterId);
            checkClusterPermission(user, cluster, Role.READER);
            if (status != null) {
                entities = repository.findByClusterIdAndStatusOrderByCreatedAtDesc(clusterId, status);
            } else {
                entities = repository.findByClusterIdOrderByCreatedAtDesc(clusterId);
            }
        } else {
            if (status != null) {
                entities = repository.findByStatusOrderByCreatedAtDesc(status);
            } else {
                entities = repository.findAllByOrderByCreatedAtDesc();
            }

            // Фильтруем по доступным кластерам
            Set<String> accessibleClusters = new HashSet<>();
            for (ClusterConfig c : yarnProperties.getClusters()) {
                if (resolveClusterRole(user, c) != null) {
                    accessibleClusters.add(c.getId());
                }
            }
            entities = entities.stream()
                    .filter(e -> accessibleClusters.contains(e.getClusterId()))
                    .toList();
        }

        return entities.stream().map(this::mapToSummary).toList();
    }

    @Transactional(readOnly = true)
    public Map<String, Long> getPendingCount(String clusterId, UserSession user) {
        long count;
        if (clusterId != null && !clusterId.isBlank()) {
            ClusterConfig cluster = findClusterOrThrow(clusterId);
            checkClusterPermission(user, cluster, Role.READER);
            count = repository.countByClusterIdAndStatus(clusterId, ChangeRequestStatus.SUBMITTED);
        } else {
            long total = 0;
            for (ClusterConfig c : yarnProperties.getClusters()) {
                if (resolveClusterRole(user, c) != null) {
                    total += repository.countByClusterIdAndStatus(c.getId(), ChangeRequestStatus.SUBMITTED);
                }
            }
            count = total;
        }
        return Map.of(
                "count", count,
                "pending_count", count
        );
    }

    @Transactional(readOnly = true)
    public ChangeRequestResponse getChangeRequest(Long id, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.READER);
        return mapToResponse(entity);
    }

    @Transactional
    public ChangeRequestResponse approveChangeRequest(Long id, String comment, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.ADMIN);

        // Four-Eyes Principle
        if (yarnProperties.getAcl().isEnforceFourEyes() && entity.getAuthor().equalsIgnoreCase(user.username())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "Принцип разделения обязанностей (Four-Eyes): автор заявки не может самостоятельно одобрить свой запрос");
        }

        if (entity.getStatus() != ChangeRequestStatus.SUBMITTED) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Нельзя одобрить заявку в статусе '" + entity.getStatus() + "' (ожидается SUBMITTED)");
        }

        // The edits are recomputed against the cluster as it is now: the request may have been
        // submitted long ago, and only what still differs is deployed
        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());
        PatchPlan plan = patchService.buildPlan(cluster, liveRoot, deserializeChanges(entity.getChangesJson()),
                entity.getQueueMappings(), entity.getQueueMappingsOverride());
        if (!plan.isValid()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Заявка больше не применима к текущей конфигурации кластера: " + String.join("; ", plan.errors()));
        }
        if (plan.operations().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Изменения заявки уже присутствуют на кластере, развертывать нечего");
        }

        entity.setStatus(ChangeRequestStatus.APPROVED);
        entity.setReviewer(user.username());
        entity.setReviewComment(comment);
        entity.setReviewedAt(Instant.now());
        entity.setPatchJson(writeJson(plan.operations()));
        entity.setXmlContent(patchService.renderPreview(cluster.getId(),
                "CR #" + entity.getId() + ": " + entity.getTitle() + " (author " + entity.getAuthor()
                        + ", approved by " + user.username() + ")",
                plan.operations()));

        ChangeRequestEntity saved = repository.save(entity);
        log.info("Заявка #{} одобрена администратором '{}'", saved.getId(), user.username());
        return mapToResponse(saved);
    }

    @Transactional
    public ChangeRequestResponse rejectChangeRequest(Long id, String comment, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.ADMIN);

        if (entity.getStatus() != ChangeRequestStatus.SUBMITTED) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Нельзя отклонить заявку в статусе '" + entity.getStatus() + "' (ожидается SUBMITTED)");
        }

        entity.setStatus(ChangeRequestStatus.REJECTED);
        entity.setReviewer(user.username());
        entity.setReviewComment((comment != null && !comment.isBlank()) ? comment : "Отклонено администратором");
        entity.setReviewedAt(Instant.now());

        ChangeRequestEntity saved = repository.save(entity);
        log.info("Заявка #{} отклонена администратором '{}'", saved.getId(), user.username());
        return mapToResponse(saved);
    }

    @Transactional
    public ChangeRequestResponse cancelChangeRequest(Long id, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        Role role = resolveClusterRole(user, cluster);

        boolean isAuthor = entity.getAuthor().equalsIgnoreCase(user.username());
        boolean isAdmin = role == Role.ADMIN;

        if (!isAuthor && !isAdmin) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Отозвать заявку может только ее автор или администратор кластера");
        }

        if (entity.getStatus() != ChangeRequestStatus.SUBMITTED) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Нельзя отозвать заявку в статусе '" + entity.getStatus() + "' (ожидается SUBMITTED)");
        }

        entity.setStatus(ChangeRequestStatus.CANCELLED);
        ChangeRequestEntity saved = repository.save(entity);
        log.info("Заявка #{} отозвана пользователем '{}'", saved.getId(), user.username());
        return mapToResponse(saved);
    }

    @Transactional(readOnly = true)
    public Map<String, Object> previewChangeRequestXml(Long id, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.READER);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cr_id", entity.getId());
        result.put("title", entity.getTitle());
        result.put("filename", "capacity-scheduler-patch-" + entity.getClusterId() + "-cr" + entity.getId() + ".xml");

        if (entity.getPatchJson() != null && entity.getStatus() == ChangeRequestStatus.APPROVED) {
            result.put("xml_content", entity.getXmlContent());
            result.put("operations", deserializeOps(entity.getPatchJson()));
            result.put("errors", List.of());
            return result;
        }

        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());
        PatchPlan plan = patchService.buildPlan(cluster, liveRoot, deserializeChanges(entity.getChangesJson()),
                entity.getQueueMappings(), entity.getQueueMappingsOverride());
        result.put("xml_content", patchService.renderPreview(cluster.getId(),
                "Preview CR #" + entity.getId() + ": " + entity.getTitle(), plan.operations()));
        result.put("operations", plan.operations());
        result.put("errors", plan.errors());
        return result;
    }

    /**
     * Not transactional on purpose: the AWX job can run for minutes, and the DEPLOYING mark must be
     * committed before it starts so that a second click cannot launch a parallel job.
     */
    public DeployResponse deployChangeRequest(Long id, boolean wait, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.ADMIN);

        if (entity.getStatus() != ChangeRequestStatus.APPROVED) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Развертывание возможно только для согласованных заявок (текущий статус: " + entity.getStatus() + ")");
        }
        List<ConfigPatchOp> ops = deserializeOps(entity.getPatchJson());
        if (ops.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "У заявки нет рассчитанных изменений: одобрите ее заново");
        }
        if (repository.claimForDeployment(id, ChangeRequestStatus.APPROVED, Instant.now()) == 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Заявка #" + id + " уже развертывается или успешно развернута (статус: " + entity.getDeploymentStatus() + ")");
        }

        DeployResponse deployResp;
        try {
            deployResp = awxClientService.deployPatch(cluster, entity.getId(), ops, user.username(),
                    "Deploy approved Change Request #" + entity.getId() + " by " + user.username(), wait);
        } catch (RuntimeException e) {
            deployResp = DeployResponse.builder()
                    .crId(entity.getId())
                    .clusterId(cluster.getId())
                    .status(AwxClientService.STATUS_FAILED)
                    .message("Сбой развертывания: " + e.getMessage())
                    .build();
        }

        ChangeRequestEntity fresh = repository.findById(id).orElse(entity);
        applyDeployResult(fresh, deployResp);
        repository.save(fresh);
        return deployResp;
    }

    public DeployResponse getDeployStatus(Long id, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.READER);

        // A job launched with wait=false (or one that outlived the poll timeout) is tracked here
        if (AwxClientService.STATUS_RUNNING.equals(entity.getDeploymentStatus()) && entity.getAwxJobId() != null) {
            DeployResponse live = awxClientService.jobStatus(cluster, entity.getId(), entity.getAwxJobId());
            if (!AwxClientService.STATUS_RUNNING.equals(live.getStatus())) {
                applyDeployResult(entity, live);
                repository.save(entity);
            }
            return live;
        }

        String deployedAtStr = entity.getDeployedAt() != null
                ? DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'").withZone(ZoneOffset.UTC).format(entity.getDeployedAt())
                : null;
        String status = entity.getDeploymentStatus() != null ? entity.getDeploymentStatus() : "NOT_DEPLOYED";

        return DeployResponse.builder()
                .crId(entity.getId())
                .clusterId(entity.getClusterId())
                .awxJobId(entity.getAwxJobId())
                .status(status)
                .message(entity.getDeploymentError() != null ? entity.getDeploymentError() : "Status: " + status)
                .deployedAt(deployedAtStr)
                .stdout("AWX Job #" + (entity.getAwxJobId() != null ? entity.getAwxJobId() : "N/A") + " status: " + status)
                .build();
    }

    private void applyDeployResult(ChangeRequestEntity entity, DeployResponse resp) {
        entity.setDeploymentStatus(resp.getStatus());
        if (resp.getAwxJobId() != null) {
            entity.setAwxJobId(resp.getAwxJobId());
        }
        entity.setDeployedAt(Instant.now());
        if (AwxClientService.STATUS_FAILED.equals(resp.getStatus())) {
            String stdout = resp.getStdout();
            entity.setDeploymentError(resp.getMessage()
                    + (stdout != null && !stdout.isBlank() ? "\n" + tail(stdout, 4000) : ""));
        } else {
            entity.setDeploymentError(null);
        }
    }

    private static String tail(String s, int max) {
        return s.length() <= max ? s : s.substring(s.length() - max);
    }

    private void requireValid(PatchPlan plan, String prefix) {
        if (!plan.isValid()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, prefix + ": " + String.join("; ", plan.errors()));
        }
    }

    private List<QueueDraftItem> copyOf(List<QueueDraftItem> changes) {
        return deserializeChanges(writeJson(changes));
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Ошибка сериализации: " + e.getMessage());
        }
    }

    private List<ConfigPatchOp> deserializeOps(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            return objectMapper.readValue(json, new TypeReference<List<ConfigPatchOp>>() {});
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Повреждены сохраненные изменения заявки: " + e.getMessage());
        }
    }

    private List<QueueDraftItem> deserializeChanges(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<QueueDraftItem>>() {});
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Повреждены данные заявки: " + e.getMessage());
        }
    }

    private List<DiffItem> deserializeDiffs(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            return objectMapper.readValue(json, new TypeReference<List<DiffItem>>() {});
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private ChangeRequestResponse mapToResponse(ChangeRequestEntity entity) {
        List<QueueDraftItem> changes = deserializeChanges(entity.getChangesJson());
        List<DiffItem> diffs = deserializeDiffs(entity.getDiffsJson());

        return ChangeRequestResponse.builder()
                .id(entity.getId())
                .clusterId(entity.getClusterId())
                .title(entity.getTitle())
                .description(entity.getDescription())
                .status(entity.getStatus().name())
                .author(entity.getAuthor())
                .createdAt(formatInstant(entity.getCreatedAt()))
                .updatedAt(formatInstant(entity.getUpdatedAt()))
                .reviewer(entity.getReviewer())
                .reviewComment(entity.getReviewComment())
                .reviewedAt(formatInstant(entity.getReviewedAt()))
                .changes(changes)
                .diffs(diffs)
                .xmlContent(entity.getXmlContent())
                .deploymentStatus(entity.getDeploymentStatus())
                .awxJobId(entity.getAwxJobId())
                .deployedAt(formatInstant(entity.getDeployedAt()))
                .deploymentError(entity.getDeploymentError())
                .operations(deserializeOps(entity.getPatchJson()))
                .queueMappings(entity.getQueueMappings())
                .queueMappingsOverride(entity.getQueueMappingsOverride())
                .build();
    }

    private ChangeRequestSummary mapToSummary(ChangeRequestEntity entity) {
        int changesCount = 0;
        try {
            List<QueueDraftItem> changes = deserializeChanges(entity.getChangesJson());
            changesCount = changes.size();
        } catch (Exception ignored) {
        }

        return ChangeRequestSummary.builder()
                .id(entity.getId())
                .clusterId(entity.getClusterId())
                .title(entity.getTitle())
                .status(entity.getStatus().name())
                .author(entity.getAuthor())
                .changesCount(changesCount)
                .createdAt(formatInstant(entity.getCreatedAt()))
                .updatedAt(formatInstant(entity.getUpdatedAt()))
                .reviewer(entity.getReviewer())
                .reviewedAt(formatInstant(entity.getReviewedAt()))
                .deploymentStatus(entity.getDeploymentStatus())
                .awxJobId(entity.getAwxJobId())
                .deployedAt(formatInstant(entity.getDeployedAt()))
                .build();
    }

    private String formatInstant(Instant instant) {
        return instant != null ? ISO_FORMATTER.format(instant) : null;
    }

    public ClusterConfig findClusterOrThrow(String clusterId) {
        ClusterConfig c = yarnProperties.findCluster(clusterId);
        if (c == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Кластер '" + clusterId + "' не найден");
        }
        return c;
    }

    public Role resolveClusterRole(UserSession user, ClusterConfig cluster) {
        if (cluster.getAcl() == null || cluster.getAcl().getRoles() == null) {
            return Role.READER;
        }

        // Проверяем admin
        RoleMapping adminMapping = cluster.getAcl().getRoles().getAdmin();
        if (matchesMapping(user, adminMapping)) {
            return Role.ADMIN;
        }

        // Проверяем writer
        RoleMapping writerMapping = cluster.getAcl().getRoles().getWriter();
        if (matchesMapping(user, writerMapping)) {
            return Role.WRITER;
        }

        // Проверяем reader
        RoleMapping readerMapping = cluster.getAcl().getRoles().getReader();
        if (matchesMapping(user, readerMapping)) {
            return Role.READER;
        }

        return null;
    }

    private boolean matchesMapping(UserSession user, RoleMapping mapping) {
        if (mapping == null) return false;
        if (mapping.getUsers() != null) {
            if (mapping.getUsers().contains("*") || mapping.getUsers().contains(user.username())) {
                return true;
            }
        }
        if (mapping.getGroups() != null && user.groups() != null) {
            if (mapping.getGroups().contains("*")) {
                return true;
            }
            for (String g : user.groups()) {
                if (mapping.getGroups().contains(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void checkClusterPermission(UserSession user, ClusterConfig cluster, Role minRole) {
        Role role = resolveClusterRole(user, cluster);
        if (role == null) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Доступ к кластеру '" + cluster.getId() + "' запрещен");
        }

        if (minRole == Role.ADMIN && role != Role.ADMIN) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Для данного действия требуется роль ADMIN");
        }
        if (minRole == Role.WRITER && (role != Role.WRITER && role != Role.ADMIN)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Для данного действия требуется роль WRITER или ADMIN");
        }
    }
}

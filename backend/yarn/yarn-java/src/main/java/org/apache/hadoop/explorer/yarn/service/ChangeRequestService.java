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
    private final XmlGeneratorService xmlGeneratorService;
    private final AwxClientService awxClientService;
    private final RoleResolver roleResolver;
    private final ObjectMapper objectMapper;

    public ChangeRequestService(
            ChangeRequestRepository repository,
            YarnProperties yarnProperties,
            YarnClientFactory yarnClientFactory,
            CapacitySchedulerService schedulerService,
            XmlGeneratorService xmlGeneratorService,
            AwxClientService awxClientService,
            RoleResolver roleResolver,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.yarnProperties = yarnProperties;
        this.yarnClientFactory = yarnClientFactory;
        this.schedulerService = schedulerService;
        this.xmlGeneratorService = xmlGeneratorService;
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

        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());

        DraftDiffResponse diffResponse = schedulerService.computeDiff(
                cluster,
                request.getChanges(),
                liveRoot,
                cluster.getDefaultPartition(),
                cluster.getQueueMappings(),
                cluster.isQueueMappingsOverride()
        );

        String changesJson;
        String diffsJson;
        try {
            changesJson = objectMapper.writeValueAsString(request.getChanges());
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
            } catch (Exception ignored) {
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
        return Map.of("count", count);
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

        // Генерируем финальный XML
        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());
        String baseXml = client.getCapacitySchedulerXml(user.username());

        List<QueueDraftItem> changes = deserializeChanges(entity.getChangesJson());
        List<QueueDraftItem> mergedQueues = mergeQueues(liveRoot, changes);

        String generatedXml = xmlGeneratorService.generateCapacitySchedulerXml(
                mergedQueues,
                cluster,
                entity.getAuthor() + " (Approved by " + user.username() + ")",
                (comment != null && !comment.isBlank()) ? comment : "Approved Change Request #" + entity.getId() + ": " + entity.getTitle(),
                cluster.getResourceMode(),
                cluster.getQueueMappings(),
                cluster.isQueueMappingsOverride(),
                baseXml
        );

        entity.setStatus(ChangeRequestStatus.APPROVED);
        entity.setReviewer(user.username());
        entity.setReviewComment(comment);
        entity.setReviewedAt(Instant.now());
        entity.setXmlContent(generatedXml);

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

        if (entity.getXmlContent() != null && entity.getStatus() == ChangeRequestStatus.APPROVED) {
            return Map.of(
                    "cr_id", entity.getId(),
                    "title", entity.getTitle(),
                    "filename", "capacity-scheduler-" + entity.getClusterId() + ".xml",
                    "xml_content", entity.getXmlContent()
            );
        }

        YarnClient client = yarnClientFactory.getClient(cluster);
        QueueNode liveRoot = client.getQueueTree(user.username());
        String baseXml = client.getCapacitySchedulerXml(user.username());

        List<QueueDraftItem> changes = deserializeChanges(entity.getChangesJson());
        List<QueueDraftItem> mergedQueues = mergeQueues(liveRoot, changes);

        String generatedXml = xmlGeneratorService.generateCapacitySchedulerXml(
                mergedQueues,
                cluster,
                entity.getAuthor() + " (Preview by " + user.username() + ")",
                "Preview Change Request #" + entity.getId() + ": " + entity.getTitle(),
                cluster.getResourceMode(),
                cluster.getQueueMappings(),
                cluster.isQueueMappingsOverride(),
                baseXml
        );

        return Map.of(
                "cr_id", entity.getId(),
                "title", entity.getTitle(),
                "filename", "capacity-scheduler-" + entity.getClusterId() + ".xml",
                "xml_content", generatedXml
        );
    }

    @Transactional
    public DeployResponse deployChangeRequest(Long id, boolean wait, UserSession user) {
        ChangeRequestEntity entity = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Заявка #" + id + " не найдена"));

        ClusterConfig cluster = findClusterOrThrow(entity.getClusterId());
        checkClusterPermission(user, cluster, Role.ADMIN);

        if (entity.getStatus() != ChangeRequestStatus.APPROVED) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Развертывание возможно только для согласованных заявок (текущий статус: " + entity.getStatus() + ")");
        }

        if (entity.getXmlContent() == null || entity.getXmlContent().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Отсутствует сгенерированный XML для развертывания");
        }

        entity.setDeploymentStatus("DEPLOYING");
        repository.save(entity);

        DeployResponse deployResp = awxClientService.deployXml(
                cluster,
                entity.getId(),
                entity.getXmlContent(),
                "Deploy approved Change Request #" + entity.getId() + " by " + user.username(),
                wait
        );

        entity.setDeploymentStatus(deployResp.getStatus());
        entity.setAwxJobId(deployResp.getAwxJobId());
        entity.setDeployedAt(Instant.now());
        repository.save(entity);

        return deployResp;
    }

    private List<QueueDraftItem> mergeQueues(QueueNode liveRoot, List<QueueDraftItem> changes) {
        Map<String, QueueDraftItem> queueMap = new LinkedHashMap<>();
        indexNodeToDrafts(liveRoot, queueMap);

        for (QueueDraftItem draft : changes) {
            if ("delete".equalsIgnoreCase(draft.getAction())) {
                queueMap.remove(draft.getPath());
            } else {
                queueMap.put(draft.getPath(), draft);
            }
        }
        return new ArrayList<>(queueMap.values());
    }

    private void indexNodeToDrafts(QueueNode node, Map<String, QueueDraftItem> map) {
        if (node == null) return;
        map.put(node.getPath(), QueueDraftItem.builder()
                .name(node.getName())
                .path(node.getPath())
                .parentPath(node.getParentPath())
                .leaf(node.isLeaf())
                .state(node.getState())
                .resourceMode(node.getResourceMode())
                .userLimitFactor(node.getUserLimitFactor())
                .orderingPolicy(node.getOrderingPolicy())
                .maxApplications(node.getMaxApplications())
                .maxAmResourcePercent(node.getMaxAmResourcePercent())
                .partitions(node.getPartitions())
                .action("modify")
                .build());

        if (node.getChildren() != null) {
            for (QueueNode child : node.getChildren()) {
                indexNodeToDrafts(child, map);
            }
        }
    }

    private List<QueueDraftItem> deserializeChanges(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<QueueDraftItem>>() {});
        } catch (Exception e) {
            return Collections.emptyList();
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

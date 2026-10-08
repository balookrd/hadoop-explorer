package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.*;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class CapacitySchedulerService {

    /**
     * Валидирует баланс мощностей очередей (сумма детей = 100% для каждого родителя).
     */
    public List<BranchBalance> validateQueueBalance(List<QueueDraftItem> queues, String resourceMode, String partition) {
        String effectivePartition = (partition == null || partition.isBlank()) ? "DEFAULT" : partition;
        boolean isPercentage = !"absolute".equalsIgnoreCase(resourceMode);

        // Группируем по parentPath
        Map<String, List<QueueDraftItem>> byParent = new HashMap<>();
        for (QueueDraftItem q : queues) {
            if ("delete".equalsIgnoreCase(q.getAction())) {
                continue;
            }
            String parent = q.getParentPath() != null ? q.getParentPath() : "root";
            byParent.computeIfAbsent(parent, k -> new ArrayList<>()).add(q);
        }

        List<BranchBalance> balances = new ArrayList<>();

        for (Map.Entry<String, List<QueueDraftItem>> entry : byParent.entrySet()) {
            String parentPath = entry.getKey();
            List<QueueDraftItem> children = entry.getValue();

            double totalCapacity = 0.0;
            int totalMem = 0;
            int totalVcores = 0;
            boolean hasMem = false;
            boolean hasVcores = false;

            for (QueueDraftItem child : children) {
                if (child.getPartitions() != null && child.getPartitions().containsKey(effectivePartition)) {
                    PartitionResourceConfig cfg = child.getPartitions().get(effectivePartition);
                    totalCapacity += cfg.getCapacity();
                    if (cfg.getMemoryMb() != null) {
                        totalMem += cfg.getMemoryMb();
                        hasMem = true;
                    }
                    if (cfg.getVcores() != null) {
                        totalVcores += cfg.getVcores();
                        hasVcores = true;
                    }
                }
            }

            totalCapacity = Math.round(totalCapacity * 100.0) / 100.0;
            double unallocated = Math.round((100.0 - totalCapacity) * 100.0) / 100.0;

            boolean isBalanced;
            String status;
            String message;

            if (isPercentage) {
                if (Math.abs(unallocated) < 0.01) {
                    isBalanced = true;
                    status = "ok";
                    message = String.format("Баланс соблюден (100.0%%) для раздела '%s'", effectivePartition);
                } else if (totalCapacity > 100.0) {
                    isBalanced = false;
                    status = "overallocated";
                    message = String.format("Сумма емкостей дочерних очередей (%.1f%%) превышает 100%% для раздела '%s'", totalCapacity, effectivePartition);
                } else {
                    isBalanced = false;
                    status = "underallocated";
                    message = String.format("Сумма емкостей дочерних очередей (%.1f%%) меньше 100%% (нераспределено: %.1f%%) для раздела '%s'", totalCapacity, unallocated, effectivePartition);
                }
            } else {
                isBalanced = true;
                status = "ok";
                message = String.format("Абсолютные ресурсы: %d MB RAM, %d vCores для раздела '%s'", totalMem, totalVcores, effectivePartition);
            }

            balances.add(BranchBalance.builder()
                    .parentPath(parentPath)
                    .partition(effectivePartition)
                    .totalChildrenCapacity(totalCapacity)
                    .unallocatedCapacity(unallocated)
                    .balanced(isBalanced)
                    .status(status)
                    .message(message)
                    .totalChildrenMemoryMb(hasMem ? totalMem : null)
                    .totalChildrenVcores(hasVcores ? totalVcores : null)
                    .build());
        }

        return balances;
    }

    /**
     * Вычисляет балансы очередей напрямую из живого дерева очередей.
     */
    public List<BranchBalance> computeBalancesFromTree(QueueNode rootQueue, String defaultPartition) {
        String effectivePartition = (defaultPartition == null || defaultPartition.isBlank()) ? "DEFAULT" : defaultPartition;
        List<BranchBalance> balances = new ArrayList<>();
        collectBalances(rootQueue, effectivePartition, balances);
        return balances;
    }

    private void collectBalances(QueueNode node, String partition, List<BranchBalance> balances) {
        if (node.getChildren() != null && !node.getChildren().isEmpty()) {
            double totalCapacity = 0.0;
            for (QueueNode child : node.getChildren()) {
                if (child.getPartitions() != null && child.getPartitions().containsKey(partition)) {
                    totalCapacity += child.getPartitions().get(partition).getCapacity();
                }
            }

            totalCapacity = Math.round(totalCapacity * 100.0) / 100.0;
            double unallocated = Math.round((100.0 - totalCapacity) * 100.0) / 100.0;

            boolean isBalanced = Math.abs(unallocated) < 0.01;
            String status = isBalanced ? "ok" : (totalCapacity > 100.0 ? "overallocated" : "underallocated");
            String message = isBalanced
                    ? String.format("Баланс соблюден (100.0%%) для раздела '%s'", partition)
                    : (totalCapacity > 100.0
                    ? String.format("Сумма емкостей (%.1f%%) превышает 100%%", totalCapacity)
                    : String.format("Сумма емкостей (%.1f%%) меньше 100%% (нераспределено: %.1f%%)", totalCapacity, unallocated));

            balances.add(BranchBalance.builder()
                    .parentPath(node.getPath())
                    .partition(partition)
                    .totalChildrenCapacity(totalCapacity)
                    .unallocatedCapacity(unallocated)
                    .balanced(isBalanced)
                    .status(status)
                    .message(message)
                    .build());

            for (QueueNode child : node.getChildren()) {
                collectBalances(child, partition, balances);
            }
        }
    }

    /**
     * Вычисляет diff между живым деревом очередей и черновиком.
     */
    public DraftDiffResponse computeDiff(
            ClusterConfig cluster,
            List<QueueDraftItem> draftQueues,
            QueueNode liveRoot,
            String selectedPartition,
            String draftMappings,
            Boolean draftMappingsOverride
    ) {
        Map<String, QueueNode> liveMap = new HashMap<>();
        indexLiveNodes(liveRoot, liveMap);

        String defaultPartition = (selectedPartition != null && !selectedPartition.isBlank())
                ? selectedPartition
                : (cluster.getDefaultPartition() != null ? cluster.getDefaultPartition() : "DEFAULT");

        List<DiffItem> diffs = new ArrayList<>();

        for (QueueDraftItem draft : draftQueues) {
            QueueNode live = liveMap.get(draft.getPath());

            if ("delete".equalsIgnoreCase(draft.getAction())) {
                diffs.add(buildDiffItem(draft, live, defaultPartition, "deleted"));
                continue;
            }

            if (live == null) {
                Set<String> parts = draft.getPartitions() != null ? draft.getPartitions().keySet() : Set.of(defaultPartition);
                for (String p : parts) {
                    diffs.add(buildDiffItem(draft, null, p, "created"));
                }
                continue;
            }

            // Существующая очередь - проверяем изменения
            Set<String> allPartitions = new HashSet<>();
            if (draft.getPartitions() != null) allPartitions.addAll(draft.getPartitions().keySet());
            if (live.getPartitions() != null) allPartitions.addAll(live.getPartitions().keySet());
            if (allPartitions.isEmpty()) allPartitions.add(defaultPartition);

            boolean queuePropsChanged = checkPropsChanged(live, draft);
            List<String> changedParts = new ArrayList<>();

            for (String p : allPartitions) {
                PartitionResourceConfig dPart = draft.getPartitions() != null ? draft.getPartitions().get(p) : null;
                PartitionResourceConfig lPart = live.getPartitions() != null ? live.getPartitions().get(p) : null;

                boolean pChanged = false;
                if ((dPart == null) != (lPart == null)) {
                    pChanged = true;
                } else if (dPart != null && lPart != null) {
                    if (Math.abs(dPart.getCapacity() - lPart.getCapacity()) > 0.01 ||
                            Math.abs(dPart.getMaxCapacity() - lPart.getMaxCapacity()) > 0.01) {
                        pChanged = true;
                    } else if (!Objects.equals(dPart.getMemoryMb(), lPart.getMemoryMb()) ||
                            !Objects.equals(dPart.getVcores(), lPart.getVcores())) {
                        pChanged = true;
                    }
                }

                if (pChanged) {
                    changedParts.add(p);
                }
            }

            if (!changedParts.isEmpty()) {
                for (String p : changedParts) {
                    diffs.add(buildDiffItem(draft, live, p, "modified"));
                }
            } else if (queuePropsChanged) {
                diffs.add(buildDiffItem(draft, live, defaultPartition, "modified"));
            } else {
                diffs.add(buildDiffItem(draft, live, defaultPartition, "unchanged"));
            }
        }

        boolean hasChanges = diffs.stream().anyMatch(d -> !"unchanged".equalsIgnoreCase(d.getAction()));

        Map<String, Object> mappingsDiff = null;
        String liveMappings = cluster.getQueueMappings() != null ? cluster.getQueueMappings() : "";
        boolean liveOverride = cluster.isQueueMappingsOverride();

        if (draftMappings != null && !draftMappings.trim().equals(liveMappings.trim())) {
            mappingsDiff = new HashMap<>();
            mappingsDiff.put("live", liveMappings);
            mappingsDiff.put("draft", draftMappings);
            mappingsDiff.put("override_live", liveOverride);
            mappingsDiff.put("override_draft", draftMappingsOverride != null ? draftMappingsOverride : liveOverride);
            hasChanges = true;
        } else if (draftMappingsOverride != null && draftMappingsOverride != liveOverride) {
            mappingsDiff = new HashMap<>();
            mappingsDiff.put("live", liveMappings);
            mappingsDiff.put("draft", draftMappings != null ? draftMappings : liveMappings);
            mappingsDiff.put("override_live", liveOverride);
            mappingsDiff.put("override_draft", draftMappingsOverride);
            hasChanges = true;
        }

        return DraftDiffResponse.builder()
                .clusterId(cluster.getId())
                .hasChanges(hasChanges)
                .diffs(diffs)
                .queueMappingsDiff(mappingsDiff)
                .build();
    }

    private boolean checkPropsChanged(QueueNode live, QueueDraftItem draft) {
        if (draft.getState() != null && draft.getState() != live.getState()) return true;
        if (draft.getResourceMode() != null && !draft.getResourceMode().equalsIgnoreCase(live.getResourceMode())) return true;
        if (draft.getUserLimitFactor() != null && live.getUserLimitFactor() != null &&
                Math.abs(draft.getUserLimitFactor() - live.getUserLimitFactor()) > 0.001) return true;
        if (draft.getOrderingPolicy() != null && live.getOrderingPolicy() != null &&
                !draft.getOrderingPolicy().equalsIgnoreCase(live.getOrderingPolicy())) return true;
        if (draft.getMaxApplications() != null && !draft.getMaxApplications().equals(live.getMaxApplications())) return true;
        if (draft.getMaxAmResourcePercent() != null && live.getMaxAmResourcePercent() != null &&
                Math.abs(draft.getMaxAmResourcePercent() - live.getMaxAmResourcePercent()) > 0.001) return true;
        if (draft.getMaxParallelApps() != null && !draft.getMaxParallelApps().equals(live.getMaxParallelApps())) return true;
        if (draft.getMaxApplicationLifetime() != null && !draft.getMaxApplicationLifetime().equals(live.getMaxApplicationLifetime())) return true;

        if (draft.getAccessibleNodeLabels() != null) {
            List<String> dLabels = new ArrayList<>(draft.getAccessibleNodeLabels());
            List<String> lLabels = live.getAccessibleNodeLabels() != null ? new ArrayList<>(live.getAccessibleNodeLabels()) : new ArrayList<>();
            Collections.sort(dLabels);
            Collections.sort(lLabels);
            if (!dLabels.equals(lLabels)) return true;
        }

        if (draft.getDefaultNodeLabelExpression() != null || live.getDefaultNodeLabelExpression() != null) {
            String dExp = draft.getDefaultNodeLabelExpression() != null ? draft.getDefaultNodeLabelExpression().trim() : "";
            String lExp = live.getDefaultNodeLabelExpression() != null ? live.getDefaultNodeLabelExpression().trim() : "";
            if (!dExp.equals(lExp)) return true;
        }

        return false;
    }

    private DiffItem buildDiffItem(QueueDraftItem draft, QueueNode live, String partition, String action) {
        PartitionResourceConfig dPart = (draft != null && draft.getPartitions() != null) ? draft.getPartitions().get(partition) : null;
        PartitionResourceConfig lPart = (live != null && live.getPartitions() != null) ? live.getPartitions().get(partition) : null;

        Double liveCap = lPart != null ? lPart.getCapacity() : null;
        Double draftCap = dPart != null ? dPart.getCapacity() : null;
        Double deltaCap = (draftCap != null && liveCap != null) ? Math.round((draftCap - liveCap) * 100.0) / 100.0 : null;

        Double liveMaxCap = lPart != null ? lPart.getMaxCapacity() : null;
        Double draftMaxCap = dPart != null ? dPart.getMaxCapacity() : null;
        Double deltaMaxCap = (draftMaxCap != null && liveMaxCap != null) ? Math.round((draftMaxCap - liveMaxCap) * 100.0) / 100.0 : null;

        Integer liveMem = lPart != null ? lPart.getMemoryMb() : null;
        Integer draftMem = dPart != null ? dPart.getMemoryMb() : null;
        Integer deltaMem = (draftMem != null && liveMem != null) ? draftMem - liveMem : null;

        Integer liveVcores = lPart != null ? lPart.getVcores() : null;
        Integer draftVcores = dPart != null ? dPart.getVcores() : null;
        Integer deltaVcores = (draftVcores != null && liveVcores != null) ? draftVcores - liveVcores : null;

        return DiffItem.builder()
                .path(draft != null ? draft.getPath() : (live != null ? live.getPath() : ""))
                .name(draft != null ? draft.getName() : (live != null ? live.getName() : ""))
                .parentPath(draft != null ? draft.getParentPath() : (live != null ? live.getParentPath() : null))
                .partition(partition)
                .action(action)
                .liveCapacity(liveCap)
                .draftCapacity(draftCap)
                .deltaCapacity(deltaCap)
                .liveMaxCapacity(liveMaxCap)
                .draftMaxCapacity(draftMaxCap)
                .deltaMaxCapacity(deltaMaxCap)
                .liveMemoryMb(liveMem)
                .draftMemoryMb(draftMem)
                .deltaMemoryMb(deltaMem)
                .liveVcores(liveVcores)
                .draftVcores(draftVcores)
                .deltaVcores(deltaVcores)
                .liveState(live != null ? live.getState() : null)
                .draftState(draft != null ? draft.getState() : null)
                .liveResourceMode(live != null ? live.getResourceMode() : null)
                .draftResourceMode(draft != null ? draft.getResourceMode() : null)
                .liveUserLimitFactor(live != null ? live.getUserLimitFactor() : null)
                .draftUserLimitFactor(draft != null ? draft.getUserLimitFactor() : null)
                .liveOrderingPolicy(live != null ? live.getOrderingPolicy() : null)
                .draftOrderingPolicy(draft != null ? draft.getOrderingPolicy() : null)
                .liveMaxApplications(live != null ? live.getMaxApplications() : null)
                .draftMaxApplications(draft != null ? draft.getMaxApplications() : null)
                .liveMaxAmResourcePercent(live != null ? live.getMaxAmResourcePercent() : null)
                .draftMaxAmResourcePercent(draft != null ? draft.getMaxAmResourcePercent() : null)
                .liveMaxParallelApps(live != null ? live.getMaxParallelApps() : null)
                .draftMaxParallelApps(draft != null ? draft.getMaxParallelApps() : null)
                .liveMaxApplicationLifetime(live != null ? live.getMaxApplicationLifetime() : null)
                .draftMaxApplicationLifetime(draft != null ? draft.getMaxApplicationLifetime() : null)
                .liveAccessibleNodeLabels(live != null ? live.getAccessibleNodeLabels() : null)
                .draftAccessibleNodeLabels(draft != null ? draft.getAccessibleNodeLabels() : null)
                .liveDefaultNodeLabelExpression(live != null ? live.getDefaultNodeLabelExpression() : null)
                .draftDefaultNodeLabelExpression(draft != null ? draft.getDefaultNodeLabelExpression() : null)
                .build();
    }

    private void indexLiveNodes(QueueNode node, Map<String, QueueNode> map) {
        if (node == null) return;
        map.put(node.getPath(), node);
        if (node.getChildren() != null) {
            for (QueueNode child : node.getChildren()) {
                indexLiveNodes(child, map);
            }
        }
    }
}

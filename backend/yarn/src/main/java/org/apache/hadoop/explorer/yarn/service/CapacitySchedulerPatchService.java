package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.BranchBalance;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.ConfigPatchOp;
import org.apache.hadoop.explorer.yarn.model.PartitionResourceConfig;
import org.apache.hadoop.explorer.yarn.model.PatchPlan;
import org.apache.hadoop.explorer.yarn.model.QueueDraftItem;
import org.apache.hadoop.explorer.yarn.model.QueueNode;
import org.apache.hadoop.explorer.yarn.model.QueueState;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Turns queue drafts into a list of edits of capacity-scheduler.xml ({@link ConfigPatchOp}).
 *
 * <p>The explorer has no reliable way to read the deployed file: with the default file-based
 * configuration store the ResourceManager does not expose it over REST ({@code /conf} returns the
 * daemon configuration, without any {@code yarn.scheduler.capacity.*} key). So instead of
 * regenerating the whole file, only the properties that a change request actually touches are
 * emitted, and the Ansible role applies them to the file on the ResourceManagers.
 */
@Service
public class CapacitySchedulerPatchService {

    static final String PREFIX = "yarn.scheduler.capacity.";
    static final String QUEUE_MAPPINGS = PREFIX + "queue-mappings";
    static final String QUEUE_MAPPINGS_OVERRIDE = PREFIX + "queue-mappings-override.enable";
    static final String DEFAULT_PARTITION = "DEFAULT";

    private static final Set<String> LEAF_POLICIES = Set.of("fifo", "fair");
    private static final Set<String> PARENT_POLICIES = Set.of("utilization", "priority-utilization");
    private static final double CAPACITY_TOLERANCE = 0.1;

    public PatchPlan buildPlan(ClusterConfig cluster,
                               QueueNode liveRoot,
                               List<QueueDraftItem> drafts,
                               String queueMappings,
                               Boolean queueMappingsOverride) {
        Map<String, QueueNode> live = new LinkedHashMap<>();
        indexLive(liveRoot, live);

        List<String> errors = new ArrayList<>();
        Map<String, QueueDraftItem> changes = normalizeDrafts(drafts, live, errors);

        Map<String, MergedQueue> merged = buildMergedTree(live, changes, cluster);
        validateStructure(changes, live, merged, errors);
        validatePolicies(changes, merged, errors);
        validateCapacities(changes, cluster, errors);
        List<BranchBalance> balances = computeBalances(merged, cluster, errors);

        List<ConfigPatchOp> ops = new ArrayList<>();
        if (errors.isEmpty()) {
            emitDeletes(changes, ops);
            emitCreates(changes, merged, cluster, ops);
            emitModifies(changes, live, merged, cluster, ops);
        }
        emitMappings(cluster, queueMappings, queueMappingsOverride, ops);

        return new PatchPlan(ops, errors, balances);
    }

    // ---------------------------------------------------------------------------------------
    // Draft normalization and structural validation
    // ---------------------------------------------------------------------------------------

    private Map<String, QueueDraftItem> normalizeDrafts(List<QueueDraftItem> drafts,
                                                         Map<String, QueueNode> live,
                                                         List<String> errors) {
        Map<String, QueueDraftItem> changes = new LinkedHashMap<>();
        if (drafts == null) {
            return changes;
        }
        for (QueueDraftItem draft : drafts) {
            String path = draft.getPath();
            if (path == null || path.isBlank()) {
                errors.add("У очереди в черновике не указан путь");
                continue;
            }
            String action = draft.getAction() == null ? "modify" : draft.getAction().toLowerCase(Locale.ROOT);
            if (!Set.of("create", "modify", "delete").contains(action)) {
                errors.add("Очередь " + path + ": неизвестное действие '" + draft.getAction() + "'");
                continue;
            }
            draft.setAction(action);

            String expectedParent = parentOf(path);
            if (!"root".equals(path)) {
                if (draft.getParentPath() != null && !draft.getParentPath().equals(expectedParent)) {
                    errors.add("Очередь " + path + ": parent_path '" + draft.getParentPath()
                            + "' не совпадает с путем очереди");
                    continue;
                }
                if (draft.getName() != null && !path.endsWith("." + draft.getName())) {
                    errors.add("Очередь " + path + ": имя '" + draft.getName() + "' не совпадает с путем очереди");
                    continue;
                }
                draft.setParentPath(expectedParent);
            } else if (!"modify".equals(action)) {
                errors.add("Корневую очередь root нельзя создавать или удалять");
                continue;
            }
            if (draft.getName() == null) {
                draft.setName(path.substring(path.lastIndexOf('.') + 1));
            }

            // The UI sends the whole tree for the XML export; an untouched existing queue is a no-op
            if ("create".equals(action) && live.containsKey(path)) {
                errors.add("Очередь " + path + " уже существует на кластере");
                continue;
            }
            if (!"create".equals(action) && !live.containsKey(path)) {
                errors.add("Очередь " + path + " не найдена на кластере (для новой очереди используйте action=create)");
                continue;
            }
            changes.put(path, draft);
        }
        return changes;
    }

    private void validateStructure(Map<String, QueueDraftItem> changes,
                                   Map<String, QueueNode> live,
                                   Map<String, MergedQueue> merged,
                                   List<String> errors) {
        for (QueueDraftItem draft : changes.values()) {
            String path = draft.getPath();
            switch (draft.getAction()) {
                case "create" -> {
                    if (!merged.containsKey(draft.getParentPath())) {
                        errors.add("Очередь " + path + ": родительская очередь " + draft.getParentPath()
                                + " не существует или удаляется");
                    }
                }
                case "delete" -> {
                    // ResourceManager refuses to drop a queue that is not stopped
                    List<String> running = new ArrayList<>();
                    collectNotStopped(live.get(path), running);
                    if (!running.isEmpty()) {
                        errors.add("Очередь " + path + " нельзя удалить: сначала остановите (STOPPED) и разверните "
                                + "остановку очередей " + String.join(", ", running));
                    }
                }
                default -> {
                }
            }
        }
    }

    private void collectNotStopped(QueueNode node, List<String> out) {
        if (node == null) {
            return;
        }
        if (node.getState() != QueueState.STOPPED) {
            out.add(node.getPath());
        }
        if (node.getChildren() != null) {
            node.getChildren().forEach(c -> collectNotStopped(c, out));
        }
    }

    private void validatePolicies(Map<String, QueueDraftItem> changes, Map<String, MergedQueue> merged, List<String> errors) {
        for (QueueDraftItem draft : changes.values()) {
            if ("delete".equals(draft.getAction()) || isBlank(draft.getOrderingPolicy())) {
                continue;
            }
            MergedQueue q = merged.get(draft.getPath());
            if (q == null) {
                continue;
            }
            String policy = draft.getOrderingPolicy().toLowerCase(Locale.ROOT);
            boolean leaf = q.children.isEmpty();
            Set<String> allowed = leaf ? LEAF_POLICIES : PARENT_POLICIES;
            if (!allowed.contains(policy)) {
                errors.add("Очередь " + draft.getPath() + ": ordering-policy '" + policy + "' недопустима для "
                        + (leaf ? "листовой" : "родительской") + " очереди (допустимо: " + new TreeSet<>(allowed) + ")");
            }
        }
    }

    private void validateCapacities(Map<String, QueueDraftItem> changes, ClusterConfig cluster, List<String> errors) {
        boolean absolute = isAbsolute(cluster);
        for (QueueDraftItem draft : changes.values()) {
            if ("delete".equals(draft.getAction()) || draft.getPartitions() == null) {
                continue;
            }
            for (Map.Entry<String, PartitionResourceConfig> e : draft.getPartitions().entrySet()) {
                PartitionResourceConfig p = e.getValue();
                if (p == null || (absolute && p.getMemoryMb() != null)) {
                    continue;
                }
                String where = "Очередь " + draft.getPath() + " [" + normalizePartition(e.getKey(), cluster) + "]";
                if (p.getCapacity() < 0 || p.getCapacity() > 100) {
                    errors.add(where + ": capacity " + format(p.getCapacity()) + "% вне диапазона 0..100");
                }
                if (p.getMaxCapacity() < 0 || p.getMaxCapacity() > 100) {
                    errors.add(where + ": maximum-capacity " + format(p.getMaxCapacity()) + "% вне диапазона 0..100");
                }
                if (p.getMaxCapacity() + 1e-9 < p.getCapacity()) {
                    errors.add(where + ": maximum-capacity (" + format(p.getMaxCapacity())
                            + "%) меньше capacity (" + format(p.getCapacity()) + "%)");
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Merged tree: live tree with the change request applied
    // ---------------------------------------------------------------------------------------

    private static final class MergedQueue {
        final String path;
        final Map<String, Double> capacities = new TreeMap<>();
        final List<String> children = new ArrayList<>();

        MergedQueue(String path) {
            this.path = path;
        }
    }

    private Map<String, MergedQueue> buildMergedTree(Map<String, QueueNode> live,
                                                     Map<String, QueueDraftItem> changes,
                                                     ClusterConfig cluster) {
        Set<String> deleted = new HashSet<>();
        for (QueueDraftItem d : changes.values()) {
            if ("delete".equals(d.getAction())) {
                deleted.add(d.getPath());
            }
        }

        Map<String, MergedQueue> merged = new LinkedHashMap<>();
        for (QueueNode node : live.values()) {
            if (isUnderAny(node.getPath(), deleted)) {
                continue;
            }
            MergedQueue q = new MergedQueue(node.getPath());
            putCapacities(q, node.getPartitions(), cluster);
            merged.put(q.path, q);
        }

        List<QueueDraftItem> creates = changes.values().stream()
                .filter(d -> "create".equals(d.getAction()))
                .sorted(Comparator.comparingInt(d -> depth(d.getPath())))
                .toList();
        for (QueueDraftItem d : creates) {
            if (isUnderAny(d.getPath(), deleted)) {
                continue;
            }
            merged.put(d.getPath(), new MergedQueue(d.getPath()));
        }
        for (QueueDraftItem d : changes.values()) {
            MergedQueue q = merged.get(d.getPath());
            if (q != null && !"delete".equals(d.getAction())) {
                putCapacities(q, d.getPartitions(), cluster);
            }
        }
        for (MergedQueue q : merged.values()) {
            MergedQueue parent = "root".equals(q.path) ? null : merged.get(parentOf(q.path));
            if (parent != null) {
                parent.children.add(q.path);
            }
        }
        return merged;
    }

    private void putCapacities(MergedQueue q, Map<String, PartitionResourceConfig> partitions, ClusterConfig cluster) {
        if (partitions == null) {
            return;
        }
        partitions.forEach((name, cfg) -> {
            if (cfg != null) {
                q.capacities.put(normalizePartition(name, cluster), cfg.getCapacity());
            }
        });
    }

    private List<BranchBalance> computeBalances(Map<String, MergedQueue> merged, ClusterConfig cluster, List<String> errors) {
        List<BranchBalance> balances = new ArrayList<>();
        if (isAbsolute(cluster)) {
            return balances;
        }
        for (MergedQueue parent : merged.values()) {
            if (parent.children.isEmpty()) {
                continue;
            }
            Set<String> partitions = new TreeSet<>();
            partitions.add(DEFAULT_PARTITION);
            for (String child : parent.children) {
                partitions.addAll(merged.get(child).capacities.keySet());
            }
            for (String partition : partitions) {
                double sum = 0;
                for (String child : parent.children) {
                    sum += merged.get(child).capacities.getOrDefault(partition, 0.0);
                }
                sum = round2(sum);
                // Node-label capacities of children may legitimately sum to 0 (no access to the label)
                boolean ok = Math.abs(sum - 100.0) < CAPACITY_TOLERANCE
                        || (!DEFAULT_PARTITION.equals(partition) && Math.abs(sum) < CAPACITY_TOLERANCE);
                String status = ok ? "ok" : (sum > 100.0 ? "overallocated" : "underallocated");
                String message = ok
                        ? String.format(Locale.ROOT, "Баланс соблюден (%.1f%%) для раздела '%s'", sum, partition)
                        : String.format(Locale.ROOT, "Сумма емкостей дочерних очередей %s (%.1f%%) должна быть 100%% для раздела '%s'",
                        parent.path, sum, partition);
                if (!ok) {
                    errors.add(message);
                }
                balances.add(BranchBalance.builder()
                        .parentPath(parent.path)
                        .partition(partition)
                        .totalChildrenCapacity(sum)
                        .unallocatedCapacity(round2(100.0 - sum))
                        .balanced(ok)
                        .status(status)
                        .message(message)
                        .build());
            }
        }
        return balances;
    }

    // ---------------------------------------------------------------------------------------
    // Emitting edits
    // ---------------------------------------------------------------------------------------

    private void emitDeletes(Map<String, QueueDraftItem> changes, List<ConfigPatchOp> ops) {
        Set<String> deleted = new HashSet<>();
        changes.values().stream().filter(d -> "delete".equals(d.getAction())).forEach(d -> deleted.add(d.getPath()));
        for (String path : new TreeSet<>(deleted)) {
            if (isUnderAny(path, without(deleted, path))) {
                continue; // removed together with its deleted ancestor
            }
            ops.add(ConfigPatchOp.listRemove(PREFIX + parentOf(path) + ".queues", lastSegment(path)));
            ops.add(ConfigPatchOp.removePrefix(PREFIX + path + "."));
        }
    }

    private void emitCreates(Map<String, QueueDraftItem> changes,
                             Map<String, MergedQueue> merged,
                             ClusterConfig cluster,
                             List<ConfigPatchOp> ops) {
        changes.values().stream()
                .filter(d -> "create".equals(d.getAction()))
                .sorted(Comparator.comparingInt((QueueDraftItem d) -> depth(d.getPath())).thenComparing(QueueDraftItem::getPath))
                .forEach(d -> {
                    ops.add(ConfigPatchOp.listAdd(PREFIX + d.getParentPath() + ".queues", d.getName()));
                    emitQueueProperties(d, null, merged.get(d.getPath()), cluster, ops);
                });
    }

    private void emitModifies(Map<String, QueueDraftItem> changes,
                              Map<String, QueueNode> live,
                              Map<String, MergedQueue> merged,
                              ClusterConfig cluster,
                              List<ConfigPatchOp> ops) {
        for (QueueDraftItem d : changes.values()) {
            if ("modify".equals(d.getAction())) {
                emitQueueProperties(d, live.get(d.getPath()), merged.get(d.getPath()), cluster, ops);
            }
        }
    }

    /**
     * Emits a {@code set} for every property of the draft that differs from the live queue
     * ({@code live == null} for a new queue, then every specified property is written).
     */
    private void emitQueueProperties(QueueDraftItem d, QueueNode live, MergedQueue merged, ClusterConfig cluster, List<ConfigPatchOp> ops) {
        String base = PREFIX + d.getPath() + ".";
        boolean absolute = isAbsolute(cluster);

        if (d.getPartitions() != null) {
            Map<String, PartitionResourceConfig> liveParts = normalizedPartitions(live, cluster);
            for (Map.Entry<String, PartitionResourceConfig> e : new TreeMap<>(d.getPartitions()).entrySet()) {
                PartitionResourceConfig p = e.getValue();
                if (p == null) {
                    continue;
                }
                String partition = normalizePartition(e.getKey(), cluster);
                PartitionResourceConfig lp = liveParts.get(partition);
                String capKey = capacityKey(d.getPath(), partition, "capacity");
                String maxKey = capacityKey(d.getPath(), partition, "maximum-capacity");

                if (absolute && p.getMemoryMb() != null && p.getVcores() != null) {
                    ops.add(ConfigPatchOp.set(capKey, "[memory=" + p.getMemoryMb() + ",vcores=" + p.getVcores() + "]"));
                    if (p.getMaxMemoryMb() != null && p.getMaxVcores() != null) {
                        ops.add(ConfigPatchOp.set(maxKey, "[memory=" + p.getMaxMemoryMb() + ",vcores=" + p.getMaxVcores() + "]"));
                    }
                    continue;
                }
                if (lp == null || differs(lp.getCapacity(), p.getCapacity())) {
                    ops.add(ConfigPatchOp.set(capKey, format(p.getCapacity())));
                }
                if (lp == null || differs(lp.getMaxCapacity(), p.getMaxCapacity())) {
                    ops.add(ConfigPatchOp.set(maxKey, format(p.getMaxCapacity())));
                }
            }
        }

        if (d.getState() != null && (live == null || live.getState() != d.getState())) {
            ops.add(ConfigPatchOp.set(base + "state", d.getState().name()));
        }
        if (d.getUserLimitFactor() != null && (live == null || differs(live.getUserLimitFactor(), d.getUserLimitFactor()))) {
            ops.add(ConfigPatchOp.set(base + "user-limit-factor", format(d.getUserLimitFactor())));
        }
        if (!isBlank(d.getOrderingPolicy())
                && (live == null || !d.getOrderingPolicy().equalsIgnoreCase(Objects.toString(live.getOrderingPolicy(), "")))) {
            ops.add(ConfigPatchOp.set(base + "ordering-policy", d.getOrderingPolicy().toLowerCase(Locale.ROOT)));
        }
        if (d.getMaxApplications() != null && (live == null || !d.getMaxApplications().equals(live.getMaxApplications()))) {
            ops.add(ConfigPatchOp.set(base + "maximum-applications", String.valueOf(d.getMaxApplications())));
        }
        if (d.getMaxAmResourcePercent() != null
                && (live == null || differs(live.getMaxAmResourcePercent(), d.getMaxAmResourcePercent(), 0.0001))) {
            ops.add(ConfigPatchOp.set(base + "maximum-am-resource-percent", format(d.getMaxAmResourcePercent(), 4)));
        }
        if (d.getMaxParallelApps() != null && (live == null || !d.getMaxParallelApps().equals(live.getMaxParallelApps()))) {
            ops.add(ConfigPatchOp.set(base + "max-parallel-apps", String.valueOf(d.getMaxParallelApps())));
        }
        if (d.getMaxApplicationLifetime() != null
                && (live == null || !d.getMaxApplicationLifetime().equals(live.getMaxApplicationLifetime()))) {
            ops.add(ConfigPatchOp.set(base + "maximum-application-lifetime", String.valueOf(d.getMaxApplicationLifetime())));
        }
        if (d.getAccessibleNodeLabels() != null) {
            Set<String> draftLabels = new TreeSet<>(d.getAccessibleNodeLabels());
            Set<String> liveLabels = live == null || live.getAccessibleNodeLabels() == null
                    ? null : new TreeSet<>(live.getAccessibleNodeLabels());
            if (!draftLabels.equals(liveLabels)) {
                // A single space is how capacity-scheduler.xml spells "no labels"
                ops.add(ConfigPatchOp.set(base + "accessible-node-labels",
                        draftLabels.isEmpty() ? " " : String.join(",", draftLabels)));
            }
        }
        if (d.getDefaultNodeLabelExpression() != null
                && (live == null || !d.getDefaultNodeLabelExpression().equals(Objects.toString(live.getDefaultNodeLabelExpression(), "")))) {
            ops.add(ConfigPatchOp.set(base + "default-node-label-expression", d.getDefaultNodeLabelExpression()));
        }
    }

    private void emitMappings(ClusterConfig cluster, String queueMappings, Boolean override, List<ConfigPatchOp> ops) {
        // The "current" mappings the UI shows come from the cluster config, so only a real edit is emitted
        if (queueMappings != null) {
            String wanted = queueMappings.trim();
            String current = Objects.toString(cluster.getQueueMappings(), "").trim();
            if (!wanted.equals(current)) {
                ops.add(wanted.isEmpty() ? ConfigPatchOp.remove(QUEUE_MAPPINGS) : ConfigPatchOp.set(QUEUE_MAPPINGS, wanted));
            }
        }
        if (override != null && override != cluster.isQueueMappingsOverride()) {
            ops.add(ConfigPatchOp.set(QUEUE_MAPPINGS_OVERRIDE, String.valueOf(override)));
        }
    }

    // ---------------------------------------------------------------------------------------
    // Rendering
    // ---------------------------------------------------------------------------------------

    /**
     * Human-readable form of the edits, shown in the UI and attached to the change request.
     */
    public String renderPreview(String clusterId, String description, List<ConfigPatchOp> ops) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            Document doc = dbf.newDocumentBuilder().newDocument();
            doc.appendChild(doc.createComment(" Edits applied by Ansible to the deployed capacity-scheduler.xml; "
                    + "properties not listed here are kept as they are "));
            Element root = doc.createElement("capacity-scheduler-patch");
            root.setAttribute("cluster", clusterId);
            if (description != null && !description.isBlank()) {
                root.setAttribute("description", description);
            }
            doc.appendChild(root);
            for (ConfigPatchOp op : ops) {
                Element e = doc.createElement(op.op());
                e.setAttribute("name", op.name());
                if (op.value() != null) {
                    e.setAttribute("value", op.value());
                }
                root.appendChild(e);
            }
            Transformer t = TransformerFactory.newInstance().newTransformer();
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            StringWriter writer = new StringWriter();
            t.transform(new DOMSource(doc), new StreamResult(writer));
            return writer.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Не удалось сформировать описание изменений: " + e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------

    static String normalizePartition(String name, ClusterConfig cluster) {
        if (name == null || name.isBlank() || DEFAULT_PARTITION.equalsIgnoreCase(name)
                || (cluster != null && name.equalsIgnoreCase(cluster.getDefaultPartition()))) {
            return DEFAULT_PARTITION;
        }
        return name;
    }

    static String capacityKey(String path, String partition, String suffix) {
        return DEFAULT_PARTITION.equals(partition)
                ? PREFIX + path + "." + suffix
                : PREFIX + path + ".accessible-node-labels." + partition + "." + suffix;
    }

    private Map<String, PartitionResourceConfig> normalizedPartitions(QueueNode live, ClusterConfig cluster) {
        Map<String, PartitionResourceConfig> out = new LinkedHashMap<>();
        if (live != null && live.getPartitions() != null) {
            live.getPartitions().forEach((k, v) -> out.put(normalizePartition(k, cluster), v));
        }
        return out;
    }

    private static void indexLive(QueueNode node, Map<String, QueueNode> out) {
        if (node == null) {
            return;
        }
        out.put(node.getPath(), node);
        if (node.getChildren() != null) {
            node.getChildren().forEach(c -> indexLive(c, out));
        }
    }

    private static boolean isAbsolute(ClusterConfig cluster) {
        return "absolute".equalsIgnoreCase(cluster.getResourceMode());
    }

    private static boolean differs(Double live, double draft) {
        return differs(live, draft, 0.01);
    }

    private static boolean differs(Double live, double draft, double tolerance) {
        return live == null || Math.abs(live - draft) > tolerance;
    }

    static String format(double v) {
        return format(v, 2);
    }

    static String format(double v, int scale) {
        return BigDecimal.valueOf(v).setScale(scale, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static String parentOf(String path) {
        int i = path.lastIndexOf('.');
        return i < 0 ? null : path.substring(0, i);
    }

    private static String lastSegment(String path) {
        return path.substring(path.lastIndexOf('.') + 1);
    }

    private static int depth(String path) {
        return (int) path.chars().filter(c -> c == '.').count();
    }

    private static boolean isUnderAny(String path, Set<String> roots) {
        for (String r : roots) {
            if (path.equals(r) || path.startsWith(r + ".")) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> without(Set<String> set, String item) {
        Set<String> copy = new LinkedHashSet<>(set);
        copy.remove(item);
        return copy;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}

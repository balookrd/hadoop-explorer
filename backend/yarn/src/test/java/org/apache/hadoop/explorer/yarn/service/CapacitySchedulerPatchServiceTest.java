package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.apache.hadoop.explorer.yarn.model.ConfigPatchOp;
import org.apache.hadoop.explorer.yarn.model.PartitionResourceConfig;
import org.apache.hadoop.explorer.yarn.model.PatchPlan;
import org.apache.hadoop.explorer.yarn.model.QueueDraftItem;
import org.apache.hadoop.explorer.yarn.model.QueueNode;
import org.apache.hadoop.explorer.yarn.model.QueueState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CapacitySchedulerPatchService: правки capacity-scheduler.xml по заявке")
class CapacitySchedulerPatchServiceTest {

    private final CapacitySchedulerPatchService service = new CapacitySchedulerPatchService();
    private ClusterConfig cluster;
    private QueueNode live;

    /**
     * Live tree parsed from a real Hadoop 3.1.3 ResourceManager:
     * root{dev 30/50, prod 70/100 {hive 40 (GPU 0), spark 60 (GPU 100, ulf 3)}}.
     */
    @BeforeEach
    void setUp() throws Exception {
        cluster = ClusterConfig.builder()
                .id("prod-yarn")
                .name("Prod")
                .resourceMode("percentage")
                .defaultPartition("DEFAULT")
                .partitions(List.of("DEFAULT", "GPU"))
                .queueMappingsOverride(false)
                .build();
        try (InputStream in = getClass().getResourceAsStream("/rm/scheduler-hadoop-3.1.3.json")) {
            JsonNode json = new ObjectMapper().readTree(in);
            live = NativeYarnClient.parseQueueTree(json.path("scheduler").path("schedulerInfo"), cluster);
        }
    }

    private static QueueDraftItem modify(String path, double cap, double max) {
        Map<String, PartitionResourceConfig> parts = new LinkedHashMap<>();
        parts.put("DEFAULT", PartitionResourceConfig.builder().capacity(cap).maxCapacity(max).build());
        return QueueDraftItem.builder()
                .path(path)
                .name(path.substring(path.lastIndexOf('.') + 1))
                .parentPath(path.substring(0, path.lastIndexOf('.')))
                .action("modify")
                .partitions(parts)
                .build();
    }

    private static Map<String, String> sets(PatchPlan plan) {
        Map<String, String> out = new LinkedHashMap<>();
        plan.operations().stream().filter(o -> ConfigPatchOp.SET.equals(o.op())).forEach(o -> out.put(o.name(), o.value()));
        return out;
    }

    @Test
    @DisplayName("Изменение max-capacity одной очереди дает ровно одну правку и не трогает остальные свойства")
    void singleChangeProducesSingleEdit() {
        // Same shape the UI sends: the edited queue with its state and current partitions
        QueueDraftItem dev = modify("root.dev", 30.0, 60.0);
        dev.setState(QueueState.RUNNING);

        PatchPlan plan = service.buildPlan(cluster, live, List.of(dev), null, null);

        assertTrue(plan.isValid(), plan.errors().toString());
        assertEquals(List.of(ConfigPatchOp.set("yarn.scheduler.capacity.root.dev.maximum-capacity", "60")), plan.operations());
    }

    @Test
    @DisplayName("Неизмененное дерево целиком (экспорт из UI) не порождает правок, в т.ч. queue-mappings")
    void untouchedTreeIsNoOp() {
        List<QueueDraftItem> all = new ArrayList<>();
        collect(live, all);

        PatchPlan plan = service.buildPlan(cluster, live, all, "", false);

        assertTrue(plan.isValid(), plan.errors().toString());
        assertEquals(List.of(), plan.operations());
    }

    private static void collect(QueueNode n, List<QueueDraftItem> out) {
        out.add(QueueDraftItem.builder()
                .path(n.getPath()).name(n.getName()).parentPath(n.getParentPath())
                .action("modify").leaf(n.isLeaf()).state(n.getState())
                .partitions(n.getPartitions())
                .userLimitFactor(n.getUserLimitFactor())
                .orderingPolicy(n.getOrderingPolicy())
                .maxApplications(n.getMaxApplications())
                .maxAmResourcePercent(n.getMaxAmResourcePercent())
                .build());
        n.getChildren().forEach(c -> collect(c, out));
    }

    @Test
    @DisplayName("Емкость node label пишется в accessible-node-labels.<label>.capacity")
    void nodeLabelCapacityUsesCorrectKey() {
        QueueDraftItem hive = modify("root.prod.hive", 40.0, 100.0);
        hive.getPartitions().put("GPU", PartitionResourceConfig.builder().capacity(30.0).maxCapacity(100.0).build());
        QueueDraftItem spark = modify("root.prod.spark", 60.0, 100.0);
        spark.getPartitions().put("GPU", PartitionResourceConfig.builder().capacity(70.0).maxCapacity(100.0).build());

        PatchPlan plan = service.buildPlan(cluster, live, List.of(hive, spark), null, null);

        assertTrue(plan.isValid(), plan.errors().toString());
        assertEquals(Map.of(
                "yarn.scheduler.capacity.root.prod.hive.accessible-node-labels.GPU.capacity", "30",
                "yarn.scheduler.capacity.root.prod.spark.accessible-node-labels.GPU.capacity", "70"), sets(plan));
    }

    @Test
    @DisplayName("Несбалансированная заявка отклоняется")
    void unbalancedIsRejected() {
        PatchPlan plan = service.buildPlan(cluster, live, List.of(modify("root.dev", 40.0, 50.0)), null, null);

        assertFalse(plan.isValid());
        assertTrue(plan.errors().get(0).contains("root"), plan.errors().toString());
        assertTrue(plan.operations().isEmpty());
    }

    @Test
    @DisplayName("Новая очередь: добавление в <parent>.queues и ее свойства; ребалансировка соседей")
    void createQueue() {
        QueueDraftItem etl = QueueDraftItem.builder()
                .path("root.prod.etl").name("etl").parentPath("root.prod").action("create")
                .state(QueueState.RUNNING).orderingPolicy("fair").userLimitFactor(2.0)
                .partitions(new LinkedHashMap<>(Map.of("DEFAULT",
                        PartitionResourceConfig.builder().capacity(20.0).maxCapacity(50.0).build())))
                .build();
        QueueDraftItem hive = modify("root.prod.hive", 20.0, 100.0);

        PatchPlan plan = service.buildPlan(cluster, live, List.of(etl, hive), null, null);

        assertTrue(plan.isValid(), plan.errors().toString());
        assertTrue(plan.operations().contains(ConfigPatchOp.listAdd("yarn.scheduler.capacity.root.prod.queues", "etl")));
        Map<String, String> sets = sets(plan);
        assertEquals("20", sets.get("yarn.scheduler.capacity.root.prod.etl.capacity"));
        assertEquals("fair", sets.get("yarn.scheduler.capacity.root.prod.etl.ordering-policy"));
        assertEquals("2", sets.get("yarn.scheduler.capacity.root.prod.etl.user-limit-factor"));
        assertEquals("20", sets.get("yarn.scheduler.capacity.root.prod.hive.capacity"));
    }

    @Test
    @DisplayName("Удаление: только остановленной очереди, через list_remove и remove_prefix")
    void deleteQueue() {
        QueueDraftItem deleteDev = QueueDraftItem.builder().path("root.dev").name("dev").parentPath("root").action("delete").build();
        QueueDraftItem prod = modify("root.prod", 100.0, 100.0);

        PatchPlan running = service.buildPlan(cluster, live, List.of(deleteDev, prod), null, null);
        assertFalse(running.isValid());
        assertTrue(running.errors().get(0).contains("STOPPED"), running.errors().toString());

        live.getChildren().stream().filter(c -> "dev".equals(c.getName())).findFirst().orElseThrow()
                .setState(QueueState.STOPPED);
        PatchPlan stopped = service.buildPlan(cluster, live, List.of(deleteDev, prod), null, null);
        assertTrue(stopped.isValid(), stopped.errors().toString());
        assertTrue(stopped.operations().contains(ConfigPatchOp.listRemove("yarn.scheduler.capacity.root.queues", "dev")));
        assertTrue(stopped.operations().contains(ConfigPatchOp.removePrefix("yarn.scheduler.capacity.root.dev.")));
    }

    @Test
    @DisplayName("ordering-policy fifo/fair недопустима для родительской очереди")
    void parentPolicyValidated() {
        QueueDraftItem prod = modify("root.prod", 70.0, 100.0);
        prod.setOrderingPolicy("fifo");

        PatchPlan plan = service.buildPlan(cluster, live, List.of(prod), null, null);

        assertFalse(plan.isValid());
        assertTrue(plan.errors().get(0).contains("родительской"), plan.errors().toString());
    }

    @Test
    @DisplayName("queue-mappings пишутся только при реальном изменении")
    void mappingsOnlyWhenChanged() {
        PatchPlan unchanged = service.buildPlan(cluster, live, List.of(), null, false);
        assertTrue(unchanged.operations().isEmpty());

        PatchPlan changed = service.buildPlan(cluster, live, List.of(), "u:etl:root.prod.spark", true);
        assertEquals(List.of(
                ConfigPatchOp.set("yarn.scheduler.capacity.queue-mappings", "u:etl:root.prod.spark"),
                ConfigPatchOp.set("yarn.scheduler.capacity.queue-mappings-override.enable", "true")), changed.operations());
    }

    @Test
    @DisplayName("Создание существующей и изменение несуществующей очереди — ошибки")
    void wrongActions() {
        QueueDraftItem createExisting = modify("root.dev", 30.0, 50.0);
        createExisting.setAction("create");
        QueueDraftItem modifyMissing = modify("root.ghost", 0.0, 10.0);

        PatchPlan plan = service.buildPlan(cluster, live, List.of(createExisting, modifyMissing), null, null);

        assertEquals(2, plan.errors().size(), plan.errors().toString());
    }

    @Test
    @DisplayName("Превью экранирует значения и перечисляет все правки")
    void previewRendersOps() {
        String xml = service.renderPreview("prod-yarn", "a < b & \"c\"", List.of(
                ConfigPatchOp.set("yarn.scheduler.capacity.root.dev.capacity", "20"),
                ConfigPatchOp.removePrefix("yarn.scheduler.capacity.root.old.")));

        assertTrue(xml.contains("<capacity-scheduler-patch"), xml);
        assertTrue(xml.contains("a &lt; b &amp; &quot;c&quot;"), xml);
        assertTrue(xml.contains("<remove_prefix name=\"yarn.scheduler.capacity.root.old.\"/>"), xml);
    }
}

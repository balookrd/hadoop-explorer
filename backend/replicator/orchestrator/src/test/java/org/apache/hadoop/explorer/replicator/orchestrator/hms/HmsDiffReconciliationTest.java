package org.apache.hadoop.explorer.replicator.orchestrator.hms;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsEventLogEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.entity.HmsReplicationJobEntity;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.service.HmsCoordinatorService;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsEventLogRepository;
import org.apache.hadoop.explorer.replicator.orchestrator.repository.HmsReplicationJobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class HmsDiffReconciliationTest {

    @Autowired
    private HmsCoordinatorService coordinatorService;

    @Autowired
    private HmsReplicationJobRepository hmsJobRepository;

    @Autowired
    private HmsEventLogRepository eventLogRepository;

    @Autowired
    private HmsClientPool hmsClientPool;

    private HmsClient srcClient;
    private HmsClient dstClient;

    @BeforeEach
    void setup() {
        hmsJobRepository.deleteAll();
        eventLogRepository.deleteAll();
        hmsClientPool.initDemoClients();

        srcClient = hmsClientPool.getClient("dc1");
        dstClient = hmsClientPool.getClient("dc2");
    }

    @Test
    @DisplayName("Diff & Reconciliation: Удаление лишних таблиц и идемпотентный alterTable при Bootstrap")
    void testTableDiffAndReconciliation_DropExtraneousTablesEnabled() {
        String dbName = "diff_db_" + UUID.randomUUID().toString().substring(0, 8);
        srcClient.createDatabase(dbName, null);
        dstClient.createDatabase(dbName, null);

        // 1. Создаем на источнике таблицы: tbl_active_1 и tbl_active_2
        HmsTableDto srcTable1 = new HmsTableDto("hive", dbName, "tbl_active_1", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + dbName + "/tbl_active_1",
                Map.of("classification", "confidential"), Collections.emptyList(),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        srcClient.createTable(srcTable1);

        HmsTableDto srcTable2 = new HmsTableDto("hive", dbName, "tbl_active_2", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + dbName + "/tbl_active_2",
                Collections.emptyMap(), Collections.emptyList(),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        srcClient.createTable(srcTable2);

        // 2. На приемнике предварительно создаем tbl_active_1 (со старыми параметрами) и tbl_extraneous_orphan (лишняя таблица)
        HmsTableDto dstOldTable1 = new HmsTableDto("hive", dbName, "tbl_active_1", "EXTERNAL_TABLE",
                "hdfs://ns-hot-dc2:8020/old/location",
                Map.of("classification", "public_old"), Collections.emptyList(),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        dstClient.createTable(dstOldTable1);

        HmsTableDto dstOrphanTable = new HmsTableDto("hive", dbName, "tbl_extraneous_orphan", "MANAGED_TABLE",
                "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/tbl_extraneous_orphan",
                Collections.emptyMap(), Collections.emptyList(),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        dstClient.createTable(dstOrphanTable);

        // Проверяем начальное состояние приемника: есть 2 таблицы
        assertEquals(2, dstClient.getAllTables(dbName).size());

        // 3. Запускаем репликацию с dropExtraneousTables = true
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1", "dc2", dbName, dbName, "*", "qa_engineer", true, false
        );

        assertEquals("ACTIVE", job.getStatus());

        // 4. Проверяем состояние целевого HMS:
        // Лишняя таблица tbl_extraneous_orphan должна быть удалена!
        Optional<HmsTableDto> orphanCheck = dstClient.getTable(dbName, "tbl_extraneous_orphan");
        assertTrue(orphanCheck.isEmpty(), "Лишняя таблица tbl_extraneous_orphan должна быть удалена из целевого HMS");

        // tbl_active_1 должна быть обновлена (alterTable) с актуальным расположением
        Optional<HmsTableDto> active1Opt = dstClient.getTable(dbName, "tbl_active_1");
        assertTrue(active1Opt.isPresent());
        assertEquals("hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/tbl_active_1", active1Opt.get().sdLocation());
        assertEquals("confidential", active1Opt.get().parameters().get("classification"));

        // tbl_active_2 должна быть создана
        Optional<HmsTableDto> active2Opt = dstClient.getTable(dbName, "tbl_active_2");
        assertTrue(active2Opt.isPresent());

        // Всего на целевом кластере ровно 2 таблицы
        assertEquals(2, dstClient.getAllTables(dbName).size());

        // 5. Проверяем аудит-лог событий
        List<HmsEventLogEntity> dropEvents = eventLogRepository.findByHmsJobId(job.getId()).stream()
                .filter(e -> "BOOTSTRAP_DROP_EXTRA_TABLE".equals(e.getEventType()))
                .toList();
        assertEquals(1, dropEvents.size());
        assertEquals("tbl_extraneous_orphan", dropEvents.getFirst().getTableName());
        assertEquals("DROPPED", dropEvents.getFirst().getStatus());
    }

    @Test
    @DisplayName("Diff & Reconciliation: Сохранение сторонних таблиц при dropExtraneousTables = false")
    void testTableDiffAndReconciliation_DropExtraneousTablesDisabled() {
        String dbName = "preserve_db_" + UUID.randomUUID().toString().substring(0, 8);
        srcClient.createDatabase(dbName, null);
        dstClient.createDatabase(dbName, null);

        HmsTableDto srcTable = new HmsTableDto("hive", dbName, "tbl_src", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + dbName + "/tbl_src",
                Collections.emptyMap(), Collections.emptyList(), "inp", "out", "ser");
        srcClient.createTable(srcTable);

        HmsTableDto dstExtra = new HmsTableDto("hive", dbName, "tbl_extra_keep", "EXTERNAL_TABLE",
                "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/tbl_extra_keep",
                Collections.emptyMap(), Collections.emptyList(), "inp", "out", "ser");
        dstClient.createTable(dstExtra);

        // Запуск с dropExtraneousTables = false
        coordinatorService.createAndStartReplication(
                "dc1", "dc2", dbName, dbName, "*", "qa_engineer", false, false
        );

        // Лишняя таблица ДОЛЖНА сохраниться
        assertTrue(dstClient.getTable(dbName, "tbl_extra_keep").isPresent(),
                "Таблица tbl_extra_keep должна остаться, так как dropExtraneousTables=false");
        assertTrue(dstClient.getTable(dbName, "tbl_src").isPresent());
        assertEquals(2, dstClient.getAllTables(dbName).size());
    }

    @Test
    @DisplayName("Diff & Reconciliation: Удаление лишних партиций при dropExtraneousPartitions = true")
    void testPartitionDiffAndReconciliation_DropExtraneousPartitionsEnabled() {
        String dbName = "part_diff_db_" + UUID.randomUUID().toString().substring(0, 8);
        srcClient.createDatabase(dbName, null);
        dstClient.createDatabase(dbName, null);

        List<String> partKeys = List.of("dt");

        // 1. Создаем партиционированную таблицу на источнике с партициями dt=2026-10-09 и dt=2026-10-10
        HmsTableDto srcTable = new HmsTableDto("hive", dbName, "events_log", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + dbName + "/events_log",
                Collections.emptyMap(), partKeys, "inp", "out", "ser");
        srcClient.createTable(srcTable);

        srcClient.addPartitions(dbName, "events_log", List.of(
                new HmsPartitionDto("hive", dbName, "events_log", List.of("2026-10-09"),
                        "hdfs://ns-hot:8020/warehouse/" + dbName + "/events_log/dt=2026-10-09", Collections.emptyMap()),
                new HmsPartitionDto("hive", dbName, "events_log", List.of("2026-10-10"),
                        "hdfs://ns-hot:8020/warehouse/" + dbName + "/events_log/dt=2026-10-10", Collections.emptyMap())
        ));

        // 2. На приемнике предварительно создаем эту же таблицу с устаревшей партицией dt=2026-10-01 и существующей dt=2026-10-09
        HmsTableDto dstTable = new HmsTableDto("hive", dbName, "events_log", "EXTERNAL_TABLE",
                "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/events_log",
                Collections.emptyMap(), partKeys, "inp", "out", "ser");
        dstClient.createTable(dstTable);

        dstClient.addPartitions(dbName, "events_log", List.of(
                new HmsPartitionDto("hive", dbName, "events_log", List.of("2026-10-01"),
                        "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/events_log/dt=2026-10-01", Collections.emptyMap()),
                new HmsPartitionDto("hive", dbName, "events_log", List.of("2026-10-09"),
                        "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/events_log/dt=2026-10-09", Collections.emptyMap())
        ));

        // 3. Запускаем репликацию с dropExtraneousPartitions = true
        HmsReplicationJobEntity job = coordinatorService.createAndStartReplication(
                "dc1", "dc2", dbName, dbName, "*", "qa_engineer", false, true
        );

        assertEquals("ACTIVE", job.getStatus());

        // 4. Проверяем партиции на приемнике:
        List<HmsPartitionDto> dstPartitions = dstClient.getPartitions(dbName, "events_log");
        assertEquals(2, dstPartitions.size(), "Должно остаться ровно 2 партиции");

        Set<String> partValues = new HashSet<>();
        for (HmsPartitionDto p : dstPartitions) {
            partValues.add(p.values().getFirst());
        }

        assertTrue(partValues.contains("2026-10-09"), "Партиция 2026-10-09 должна присутствовать");
        assertTrue(partValues.contains("2026-10-10"), "Партиция 2026-10-10 должна присутствовать");
        assertFalse(partValues.contains("2026-10-01"), "Устаревшая партиция 2026-10-01 должна быть удалена");

        // 5. Проверяем аудит-лог
        List<HmsEventLogEntity> dropPartEvents = eventLogRepository.findByHmsJobId(job.getId()).stream()
                .filter(e -> "BOOTSTRAP_DROP_EXTRA_PARTITION".equals(e.getEventType()))
                .toList();
        assertEquals(1, dropPartEvents.size());
        assertEquals("dt=2026-10-01", dropPartEvents.getFirst().getPartitionName());
        assertEquals("DROPPED", dropPartEvents.getFirst().getStatus());
    }

    @Test
    @DisplayName("Diff & Reconciliation: Сохранение партиций при dropExtraneousPartitions = false")
    void testPartitionDiffAndReconciliation_DropExtraneousPartitionsDisabled() {
        String dbName = "keep_parts_db_" + UUID.randomUUID().toString().substring(0, 8);
        srcClient.createDatabase(dbName, null);
        dstClient.createDatabase(dbName, null);

        List<String> partKeys = List.of("dt");

        HmsTableDto table = new HmsTableDto("hive", dbName, "events_log", "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/" + dbName + "/events_log",
                Collections.emptyMap(), partKeys, "inp", "out", "ser");
        srcClient.createTable(table);
        srcClient.addPartitions(dbName, "events_log", List.of(
                new HmsPartitionDto("hive", dbName, "events_log", List.of("2026-10-10"),
                        "hdfs://ns-hot:8020/warehouse/" + dbName + "/events_log/dt=2026-10-10", Collections.emptyMap())
        ));

        HmsTableDto dstTable = new HmsTableDto("hive", dbName, "events_log", "EXTERNAL_TABLE",
                "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/events_log",
                Collections.emptyMap(), partKeys, "inp", "out", "ser");
        dstClient.createTable(dstTable);
        dstClient.addPartitions(dbName, "events_log", List.of(
                new HmsPartitionDto("hive", dbName, "events_log", List.of("2026-10-01"),
                        "hdfs://ns-hot-dc2:8020/warehouse/" + dbName + "/events_log/dt=2026-10-01", Collections.emptyMap())
        ));

        // dropExtraneousPartitions = false
        coordinatorService.createAndStartReplication(
                "dc1", "dc2", dbName, dbName, "*", "qa_engineer", false, false
        );

        List<HmsPartitionDto> dstPartitions = dstClient.getPartitions(dbName, "events_log");
        assertEquals(2, dstPartitions.size(), "Обе партиции должны остаться, так как флаг dropExtraneousPartitions отключен");
    }
}

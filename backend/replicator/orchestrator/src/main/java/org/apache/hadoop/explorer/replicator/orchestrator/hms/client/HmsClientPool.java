package org.apache.hadoop.explorer.replicator.orchestrator.hms.client;

import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class HmsClientPool {

    private static final Logger log = LoggerFactory.getLogger(HmsClientPool.class);

    private final Map<String, HmsClient> clients = new ConcurrentHashMap<>();

    @PostConstruct
    public void initDemoClients() {
        // Создаем симуляторы для DC1 (HDP 3.1) и DC2 (Apache Hive 3.1.3)
        MockHmsClient dc1Client = new MockHmsClient("dc1", "HDP_3.1");
        MockHmsClient dc2Client = new MockHmsClient("dc2", "APACHE_3.1.3");

        // 1. Сидирование базы 'analytics' в DC1 (HDP 3.1)
        dc1Client.createDatabase("analytics", "hdfs://ns-hot:8020/warehouse/tablespace/external/hive/analytics.db");

        // 1.1. Внешняя таблица с партициями на ns-cold (федерация HDFS)
        HmsTableDto salesTable = new HmsTableDto(
                "hive",
                "analytics",
                "sales_daily",
                "EXTERNAL_TABLE",
                "hdfs://ns-hot:8020/warehouse/tablespace/external/hive/analytics.db/sales_daily",
                Map.of("EXTERNAL", "TRUE", "hdp.version", "3.1.0.0-78"),
                List.of("dt"),
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        );
        dc1Client.createTable(salesTable);

        // Партиция, размещенная в холодном неймсервисе (ns-cold)
        HmsPartitionDto p1 = new HmsPartitionDto(
                "hive",
                "analytics",
                "sales_daily",
                List.of("2026-10-08"),
                "hdfs://ns-cold:8020/warehouse/tablespace/external/hive/analytics.db/sales_daily/dt=2026-10-08",
                Map.of("numRows", "1500000")
        );
        dc1Client.addPartitions("analytics", "sales_daily", List.of(p1));

        // 1.2. Managed Non-Transactional таблица (поддерживается!)
        HmsTableDto managedNonAcidTable = new HmsTableDto(
                "hive",
                "analytics",
                "dim_customers",
                "MANAGED_TABLE",
                "hdfs://ns-hot:8020/warehouse/tablespace/managed/hive/analytics.db/dim_customers",
                Map.of("transactional", "false"),
                List.of(),
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        );
        dc1Client.createTable(managedNonAcidTable);

        // 1.3. Managed ACID таблица (должна быть отфильтрована!)
        HmsTableDto acidTable = new HmsTableDto(
                "hive",
                "analytics",
                "acid_orders_streaming",
                "MANAGED_TABLE",
                "hdfs://ns-hot:8020/warehouse/tablespace/managed/hive/analytics.db/acid_orders_streaming",
                Map.of("transactional", "true"),
                List.of(),
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
        );
        dc1Client.createTable(acidTable);

        clients.put("dc1", dc1Client);
        clients.put("dc2", dc2Client);

        log.info("HMS Client Pool успешно инициализирован: DC1 (HDP 3.1) и DC2 (Apache Hive 3.1.3)");
    }

    public HmsClient getClient(String clusterId) {
        String normalizedId = clusterId;
        if ("demo-cluster".equalsIgnoreCase(normalizedId)) normalizedId = "dc1";
        if ("backup-cluster".equalsIgnoreCase(normalizedId)) normalizedId = "dc2";
        return clients.computeIfAbsent(normalizedId, id -> new MockHmsClient(id, "APACHE_3.1.3"));
    }

    public void registerClient(String clusterId, HmsClient client) {
        clients.put(clusterId, client);
    }
}

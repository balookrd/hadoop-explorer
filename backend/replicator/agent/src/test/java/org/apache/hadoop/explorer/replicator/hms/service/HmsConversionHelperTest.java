package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.generated.HmsPartitionProto;
import org.apache.hadoop.explorer.replicator.hms.client.ThriftHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Тесты сквозного переноса метаданных партиции по маршруту
 * агент-источник → protobuf → агент-приемник → Hive Metastore.
 */
@DisplayName("HmsConversionHelper Tests")
class HmsConversionHelperTest {

    private static final String ORC_INPUT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private static final String ORC_OUTPUT = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    private static final String ORC_SERDE = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

    private static Partition orcPartition() {
        SerDeInfo serde = new SerDeInfo();
        serde.setName("events");
        serde.setSerializationLib(ORC_SERDE);
        serde.setParameters(Map.of("serialization.format", "1"));

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation("hdfs://ns-dc1:8020/warehouse/logs.db/events/dt=2026-10-24");
        sd.setInputFormat(ORC_INPUT);
        sd.setOutputFormat(ORC_OUTPUT);
        sd.setSerdeInfo(serde);
        sd.setCols(List.of(new FieldSchema("event_type", "string", null)));

        Partition p = new Partition();
        p.setCatName("hive");
        p.setDbName("logs");
        p.setTableName("events");
        p.setValues(List.of("2026-10-24"));
        p.setParameters(Map.of());
        p.setSd(sd);
        return p;
    }

    @Test
    @DisplayName("Формат хранения партиции переживает передачу через protobuf")
    void testPartitionStorageFormatSurvivesProtoRoundTrip() {
        HmsPartitionDto source = ThriftHmsClient.toDto(orcPartition());

        HmsPartitionProto proto = HmsConversionHelper.toProto(source);
        HmsPartitionDto restored = HmsConversionHelper.toDto(proto);
        Partition applied = ThriftHmsClient.toThrift(restored);

        assertEquals(ORC_INPUT, applied.getSd().getInputFormat(),
                "inputFormat обязан дойти до приемника через gRPC-контракт");
        assertEquals(ORC_OUTPUT, applied.getSd().getOutputFormat(),
                "outputFormat обязан дойти до приемника через gRPC-контракт");
        assertEquals(ORC_SERDE, applied.getSd().getSerdeInfo().getSerializationLib(),
                "serde обязан дойти до приемника через gRPC-контракт");
    }

    @Test
    @DisplayName("Остальные поля партиции не теряются при конвертации")
    void testPartitionIdentityFieldsSurviveProtoRoundTrip() {
        HmsPartitionDto restored = HmsConversionHelper.toDto(
                HmsConversionHelper.toProto(ThriftHmsClient.toDto(orcPartition())));

        assertEquals("logs", restored.dbName());
        assertEquals("events", restored.tableName());
        assertEquals(List.of("2026-10-24"), restored.values());
        assertEquals("hdfs://ns-dc1:8020/warehouse/logs.db/events/dt=2026-10-24", restored.location());
    }
}

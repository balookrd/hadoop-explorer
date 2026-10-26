package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ThriftHmsClientTest {

    private IMetaStoreClient mockClient1;
    private IMetaStoreClient mockClient2;
    private GenericKeyedObjectPool<String, IMetaStoreClient> pool;
    private ThriftHmsClient thriftHmsClient;

    @BeforeEach
    void setup() {
        mockClient1 = mock(IMetaStoreClient.class);
        mockClient2 = mock(IMetaStoreClient.class);

        BaseKeyedPooledObjectFactory<String, IMetaStoreClient> factory = new BaseKeyedPooledObjectFactory<>() {
            private int count = 0;

            @Override
            public IMetaStoreClient create(String principalKey) {
                count++;
                return (count == 1) ? mockClient1 : mockClient2;
            }

            @Override
            public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient obj) {
                return new DefaultPooledObject<>(obj);
            }
        };

        GenericKeyedObjectPoolConfig<IMetaStoreClient> poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setMaxTotal(5);
        poolConfig.setMaxTotalPerKey(5);
        poolConfig.setMaxWait(Duration.ofSeconds(2));
        pool = new GenericKeyedObjectPool<>(factory, poolConfig);

        thriftHmsClient = new ThriftHmsClient("dc1", "thrift://localhost:9083", pool);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (thriftHmsClient != null) {
            thriftHmsClient.close();
        }
    }

    @Test
    @DisplayName("Проверка возврата соединений в пул после успешных операций")
    void testConnectionPoolBorrowAndReturn() throws Exception {
        when(mockClient1.getAllDatabases()).thenReturn(List.of("db1", "db2"));

        List<String> dbs = thriftHmsClient.getAllDatabases();
        assertEquals(2, dbs.size());
        assertTrue(dbs.contains("db1"));

        // Соединение должно быть возвращено в пул
        assertEquals(0, pool.getNumActive(), "После завершения операции не должно оставаться активных соединений");
        assertEquals(1, pool.getNumIdle(), "Соединение должно быть свободно в пуле");
        verify(mockClient1, times(1)).getAllDatabases();
    }

    @Test
    @DisplayName("Проверка инвалидации и повтора при возникновении MetaException")
    void testRetryAndInvalidationOnMetaException() throws Exception {
        // Первый клиент бросает ошибку сети / MetaException
        when(mockClient1.getAllDatabases()).thenThrow(new MetaException("Connection lost"));
        // Второй клиент отвечает успешно
        when(mockClient2.getAllDatabases()).thenReturn(List.of("recovered_db"));

        List<String> dbs = thriftHmsClient.getAllDatabases();
        assertEquals(1, dbs.size());
        assertEquals("recovered_db", dbs.get(0));

        // Проверяем, что первый клиент был вызван, сфейлился, а второй клиент успешно отработал
        verify(mockClient1, times(1)).getAllDatabases();
        verify(mockClient2, times(1)).getAllDatabases();
        assertEquals(0, pool.getNumActive());
    }

    @Test
    @DisplayName("Проверка инвалидации и повтора при возникновении сетевой ошибки Thrift (TTransportException)")
    void testRetryAndInvalidationOnTTransportException() throws Exception {
        // Первый клиент бросает сетевую ошибку сокета TTransportException
        when(mockClient1.getAllDatabases()).thenThrow(new org.apache.thrift.transport.TTransportException("Socket closed by peer"));
        // Второй клиент отвечает успешно
        when(mockClient2.getAllDatabases()).thenReturn(List.of("recovered_db"));

        List<String> dbs = thriftHmsClient.getAllDatabases();
        assertEquals(1, dbs.size());
        assertEquals("recovered_db", dbs.get(0));

        verify(mockClient1, times(1)).getAllDatabases();
        verify(mockClient2, times(1)).getAllDatabases();
        assertEquals(0, pool.getNumActive(), "Сломанный клиент не должен оставаться активным");
    }

    @Test
    @DisplayName("Проверка создания таблицы: сохранение полной схемы колонок, партиций и параметров SerDe")
    void testCreateTablePreservesSchemaAndSerde() throws Exception {
        ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);

        HmsTableDto tableDto = new HmsTableDto(
                "hive",
                "default",
                "orders",
                "EXTERNAL_TABLE",
                "hdfs://ns1/user/hive/warehouse/orders",
                Map.of("EXTERNAL", "TRUE", "orc.compress", "ZLIB"),
                List.of("dt", "region"),
                "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                List.of(
                        new HmsFieldSchemaDto("order_id", "bigint", "Order ID"),
                        new HmsFieldSchemaDto("customer_id", "string", "Customer UUID"),
                        new HmsFieldSchemaDto("amount", "decimal(18,2)", "Order sum")
                ),
                List.of(
                        new HmsFieldSchemaDto("dt", "string", "Partition Date"),
                        new HmsFieldSchemaDto("region", "string", "Partition Region")
                ),
                Map.of("serialization.format", "1", "field.delim", ",")
        );

        thriftHmsClient.createTable(tableDto);

        verify(mockClient1, times(1)).createTable(tableCaptor.capture());
        Table created = tableCaptor.getValue();

        assertNotNull(created);
        assertEquals("orders", created.getTableName());
        assertEquals("EXTERNAL_TABLE", created.getTableType());
        assertEquals("hdfs://ns1/user/hive/warehouse/orders", created.getSd().getLocation());

        // Проверка колонок
        List<FieldSchema> cols = created.getSd().getCols();
        assertEquals(3, cols.size());
        assertEquals("order_id", cols.get(0).getName());
        assertEquals("bigint", cols.get(0).getType());
        assertEquals("Order ID", cols.get(0).getComment());

        // Проверка partition keys
        List<FieldSchema> partKeys = created.getPartitionKeys();
        assertEquals(2, partKeys.size());
        assertEquals("dt", partKeys.get(0).getName());
        assertEquals("region", partKeys.get(1).getName());

        // Проверка SerDe параметров
        Map<String, String> serdeParams = created.getSd().getSerdeInfo().getParameters();
        assertEquals("1", serdeParams.get("serialization.format"));
        assertEquals(",", serdeParams.get("field.delim"));
    }

    @Test
    @DisplayName("Проверка закрытия пула клиентов")
    void testClosePool() throws Exception {
        assertFalse(pool.isClosed());
        thriftHmsClient.close();
        assertTrue(pool.isClosed(), "Пул должен быть закрыт при закрытии ThriftHmsClient");
    }

    @Test
    @DisplayName("Проверка executeAs с валидным пользователем")
    void testExecuteAsValidUser() throws Exception {
        when(mockClient1.getAllDatabases()).thenReturn(List.of("sales_db"));
        List<String> dbs = thriftHmsClient.executeAs("analyst_user@EXAMPLE.COM", () -> thriftHmsClient.getAllDatabases());
        assertEquals(List.of("sales_db"), dbs);
        verify(mockClient1).getAllDatabases();
    }

    @Test
    @DisplayName("Проверка отклонения запрещенных системных принципалов в executeAs")
    void testExecuteAsRejectsForbiddenPrincipal() {
        assertThrows(SecurityException.class, () ->
                thriftHmsClient.executeAs("root", () -> thriftHmsClient.getAllDatabases()));
        assertThrows(SecurityException.class, () ->
                thriftHmsClient.executeAs("../bad_user", () -> thriftHmsClient.getAllDatabases()));
    }

    private static HmsPartitionDto part(String value, String location) {
        return new HmsPartitionDto("hive", "analytics", "sales_daily", List.of(value), location, Map.of());
    }

    private static final String PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    private static final String PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

    private static Partition parquetSourcePartition() {
        SerDeInfo serde = new SerDeInfo();
        serde.setName("sales_daily");
        serde.setSerializationLib(PARQUET_SERDE);
        serde.setParameters(Map.of("serialization.format", "1"));

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation("hdfs://ns-dc1:8020/warehouse/analytics.db/sales_daily/dt=2026-10-24");
        sd.setInputFormat(PARQUET_INPUT);
        sd.setOutputFormat(PARQUET_OUTPUT);
        sd.setSerdeInfo(serde);
        sd.setCols(List.of(new FieldSchema("amount", "double", null)));

        Partition p = new Partition();
        p.setCatName("hive");
        p.setDbName("analytics");
        p.setTableName("sales_daily");
        p.setValues(List.of("2026-10-24"));
        p.setParameters(Map.of("numRows", "1000"));
        p.setSd(sd);
        return p;
    }

    @Test
    @DisplayName("Формат партиции переносится без подмены на Text/LazySimpleSerDe")
    void testPartitionStorageFormatSurvivesConversion() {
        Partition source = parquetSourcePartition();

        HmsPartitionDto dto = ThriftHmsClient.toDto(source);
        Partition applied = ThriftHmsClient.toThrift(dto);

        assertEquals(PARQUET_INPUT, applied.getSd().getInputFormat(),
                "inputFormat партиции обязан сохраниться, иначе Hive читает Parquet как текст");
        assertEquals(PARQUET_OUTPUT, applied.getSd().getOutputFormat(),
                "outputFormat партиции обязан сохраниться");
        assertEquals(PARQUET_SERDE, applied.getSd().getSerdeInfo().getSerializationLib(),
                "serde партиции обязан сохраниться");
    }

    @Test
    @DisplayName("При отсутствии формата у источника применяется текстовый дефолт (обратная совместимость)")
    void testPartitionFormatFallsBackToTextWhenSourceHasNone() {
        Partition applied = ThriftHmsClient.toThrift(part("2026-10-24", "hdfs://ns/warehouse/sales_daily/dt=2026-10-24"));

        assertEquals("org.apache.hadoop.mapred.TextInputFormat", applied.getSd().getInputFormat());
        assertEquals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                applied.getSd().getSerdeInfo().getSerializationLib());
    }

    @Test
    @DisplayName("Добавление партиций идемпотентно: батч с коллизией применяется целиком (ifNotExists)")
    void testAddPartitionsAppliesWholeBatchDespiteCollision() throws Exception {
        List<HmsPartitionDto> batch = List.of(
                part("2026-10-23", "hdfs://ns/warehouse/sales_daily/dt=2026-10-23"),
                part("2026-10-24", "hdfs://ns/warehouse/sales_daily/dt=2026-10-24")
        );
        // HMS добавил только вторую: первая уже существовала
        when(mockClient1.add_partitions(anyList(), eq(true), eq(true)))
                .thenAnswer(inv -> {
                    List<Partition> requested = inv.getArgument(0);
                    return List.of(requested.get(1));
                });

        thriftHmsClient.addPartitions("analytics", "sales_daily", batch);

        verify(mockClient1).add_partitions(anyList(), eq(true), eq(true));
        verify(mockClient1, never()).add_partitions(anyList());
    }

    @Test
    @DisplayName("Уже существующая партиция обновляется (upsert), а не отбрасывается молча")
    void testAddPartitionsUpsertsExistingPartition() throws Exception {
        List<HmsPartitionDto> batch = List.of(
                part("2026-10-23", "hdfs://ns-new/warehouse/sales_daily/dt=2026-10-23"),
                part("2026-10-24", "hdfs://ns-new/warehouse/sales_daily/dt=2026-10-24")
        );
        // Ни одна партиция не добавлена: обе уже есть в метасторе
        when(mockClient1.add_partitions(anyList(), eq(true), eq(true))).thenReturn(List.of());

        thriftHmsClient.addPartitions("analytics", "sales_daily", batch);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Partition>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockClient1).alter_partitions(eq("analytics"), eq("sales_daily"), captor.capture());
        List<Partition> altered = captor.getValue();
        assertEquals(2, altered.size(), "Обе существующие партиции должны быть обновлены");
        assertEquals("hdfs://ns-new/warehouse/sales_daily/dt=2026-10-23",
                altered.get(0).getSd().getLocation(),
                "Обновление обязано донести новый location, иначе ALTER_PARTITION теряется");
    }

    @Test
    @DisplayName("AlreadyExists в середине батча: партиции применяются, успех не выдается молча")
    void testAddPartitionsRecoversFromAlreadyExistsException() throws Exception {
        List<HmsPartitionDto> batch = List.of(
                part("2026-10-23", "hdfs://ns/warehouse/sales_daily/dt=2026-10-23"),
                part("2026-10-24", "hdfs://ns/warehouse/sales_daily/dt=2026-10-24"),
                part("2026-10-25", "hdfs://ns/warehouse/sales_daily/dt=2026-10-25")
        );
        // Метастор может не вернуть список добавленных, а бросить исключение на первой
        // же коллизии, не применив батч вообще
        when(mockClient1.add_partitions(anyList(), eq(true), eq(true)))
                .thenThrow(new AlreadyExistsException("partition dt=2026-10-24 already exists"));

        thriftHmsClient.addPartitions("analytics", "sales_daily", batch);

        // Ни одна партиция не добавлена, значит все три обязаны быть приведены к состоянию
        // источника через alter_partitions: иначе событие CDC считается примененным, а
        // location и формат партиций остались прежними
        ArgumentCaptor<List<Partition>> altered = ArgumentCaptor.forClass(List.class);
        verify(mockClient1).alter_partitions(eq("analytics"), eq("sales_daily"), altered.capture());
        assertEquals(3, altered.getValue().size(),
                "Обновить обязаны все партиции батча, получено: " + altered.getValue().size());
    }

    @Test
    @DisplayName("Отказ метастора при добавлении партиций не выдается за успех")
    void testAddPartitionsPropagatesMetastoreFailure() throws Exception {
        List<HmsPartitionDto> batch = List.of(part("2026-10-24", "hdfs://ns/warehouse/sales_daily/dt=2026-10-24"));
        when(mockClient1.add_partitions(anyList(), eq(true), eq(true)))
                .thenThrow(new MetaException("metastore is down"));
        when(mockClient2.add_partitions(anyList(), eq(true), eq(true)))
                .thenThrow(new MetaException("metastore is down"));

        assertThrows(RuntimeException.class,
                () -> thriftHmsClient.addPartitions("analytics", "sales_daily", batch),
                "Сбой метастора обязан подниматься наверх, иначе позиция CDC уйдет вперед по непримененному событию");
    }
}

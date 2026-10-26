package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Партиции таблицы читаются порциями.
 *
 * <p>{@code listPartitions(db, tbl, (short) -1)} заставляет метастор собрать в один Thrift-ответ все
 * партиции таблицы: для 100k партиций это сотни мегабайт в одном сообщении — на стороне агента
 * упирается в лимит размера ответа и в таймаут, а на стороне метастора нагружает его целиком.
 */
class PartitionBatchingTest {

    private IMetaStoreClient metaStore;
    private GenericKeyedObjectPool<String, IMetaStoreClient> pool;
    private ThriftHmsClient client;

    @BeforeEach
    void setUp() {
        metaStore = mock(IMetaStoreClient.class);
        BaseKeyedPooledObjectFactory<String, IMetaStoreClient> factory = new BaseKeyedPooledObjectFactory<>() {
            @Override
            public IMetaStoreClient create(String principalKey) {
                return metaStore;
            }

            @Override
            public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient obj) {
                return new DefaultPooledObject<>(obj);
            }
        };
        GenericKeyedObjectPoolConfig<IMetaStoreClient> config = new GenericKeyedObjectPoolConfig<>();
        config.setMaxTotal(2);
        config.setMaxTotalPerKey(2);
        config.setMaxWait(Duration.ofSeconds(2));
        pool = new GenericKeyedObjectPool<>(factory, config);
        client = new ThriftHmsClient("dc1", "thrift://localhost:9083", pool);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    @DisplayName("Партиции большой таблицы запрашиваются порциями, а не одним ответом")
    void partitionsAreFetchedInBatches() throws Exception {
        int total = 2500;
        List<String> names = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            names.add("dt=2026-01-" + i);
        }
        when(metaStore.listPartitionNames(anyString(), anyString(), anyShort())).thenReturn(names);
        when(metaStore.getPartitionsByNames(anyString(), anyString(), anyList()))
                .thenAnswer(invocation -> {
                    List<String> requested = invocation.getArgument(2);
                    List<Partition> batch = new ArrayList<>(requested.size());
                    for (String name : requested) {
                        batch.add(partition(name));
                    }
                    return batch;
                });

        List<HmsPartitionDto> partitions = client.getPartitions("sales", "orders");

        assertEquals(total, partitions.size(), "возвращаются все партиции таблицы");
        verify(metaStore, never()).listPartitions(anyString(), anyString(), anyShort());
        verify(metaStore, times(3)).getPartitionsByNames(anyString(), anyString(), anyList());
    }

    @Test
    @DisplayName("Таблица без партиций не вызывает выборку по именам")
    void emptyTableSkipsBatchFetch() throws Exception {
        when(metaStore.listPartitionNames(anyString(), anyString(), anyShort())).thenReturn(List.of());

        assertEquals(List.of(), client.getPartitions("sales", "orders"));

        verify(metaStore, never()).getPartitionsByNames(anyString(), anyString(), anyList());
    }

    private static Partition partition(String name) {
        Partition p = new Partition();
        p.setDbName("sales");
        p.setTableName("orders");
        p.setValues(List.of(name.substring(name.indexOf('=') + 1)));
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation("hdfs://src/warehouse/sales.db/orders/" + name);
        p.setSd(sd);
        return p;
    }
}

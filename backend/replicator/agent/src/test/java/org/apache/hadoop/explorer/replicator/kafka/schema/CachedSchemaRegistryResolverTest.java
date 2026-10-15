package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CachedSchemaRegistryResolverTest {

    @Test
    @DisplayName("BoundedLruCache должен ограничивать максимальное количество элементов и вытеснять старейшие")
    void testBoundedLruCacheEviction() {
        int maxEntries = 3;
        CachedSchemaRegistryResolver.BoundedLruCache<String, Integer> cache =
                new CachedSchemaRegistryResolver.BoundedLruCache<>(maxEntries);

        cache.put("k1", 1);
        cache.put("k2", 2);
        cache.put("k3", 3);

        assertEquals(3, cache.size());
        assertEquals(1, cache.get("k1"));
        assertEquals(2, cache.get("k2"));
        assertEquals(3, cache.get("k3"));

        // Доступ к k1 делает его недавно использованным, самым старым становится k2
        cache.get("k1");

        // Добавляем 4-й элемент, k2 должен быть вытеснен
        cache.put("k4", 4);

        assertEquals(3, cache.size());
        assertEquals(1, cache.get("k1"));
        assertNull(cache.get("k2"), "k2 должен быть вытеснен по LRU");
        assertEquals(3, cache.get("k3"));
        assertEquals(4, cache.get("k4"));
    }

    @Test
    @DisplayName("CachedSchemaRegistryResolver должен корректно использовать BoundedLruCache для Confluent-To-Confluent")
    void testConfluentToConfluentResolutionWithCache() throws Exception {
        ConfluentRegistryClient sourceClient = Mockito.mock(ConfluentRegistryClient.class);
        ConfluentRegistryClient targetClient = Mockito.mock(ConfluentRegistryClient.class);

        CachedSchemaRegistryResolver resolver = new CachedSchemaRegistryResolver(
                sourceClient, targetClient, null, null, 2
        );

        when(sourceClient.getSchemaById(101)).thenReturn("{\"type\":\"string\"}");
        when(targetClient.registerSchema(eq("test-topic-value"), any(String.class))).thenReturn(201);

        // Первый вызов - промах кэша, обращение к клиентам
        int targetId1 = resolver.resolveConfluentToConfluent("test-topic", 101);
        assertEquals(201, targetId1);

        // Второй вызов с тем же ID - попадание в кэш, клиенты не вызываются повторно
        int targetId2 = resolver.resolveConfluentToConfluent("test-topic", 101);
        assertEquals(201, targetId2);

        verify(sourceClient, times(1)).getSchemaById(101);
        verify(targetClient, times(1)).registerSchema(eq("test-topic-value"), any(String.class));
    }
}

package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    @Test
    @DisplayName("Single-Flight: 30 параллельных потоков вызывают разрешение новой схемы, вызывая клиентов ровно один раз (устранение Thundering Herd)")
    void testSingleFlightConcurrentResolutionAvoidsThunderingHerd() throws Exception {
        ConfluentRegistryClient sourceClient = Mockito.mock(ConfluentRegistryClient.class);
        ConfluentRegistryClient targetClient = Mockito.mock(ConfluentRegistryClient.class);

        CachedSchemaRegistryResolver resolver = new CachedSchemaRegistryResolver(
                sourceClient, targetClient, null, null, 100
        );

        int sourceId = 999;
        int targetId = 888;
        String topic = "high-load-topic";

        // Имитируем сетевую задержку регистрации схемы (50 мс), чтобы все потоки гарантированно зашли параллельно
        when(sourceClient.getSchemaById(sourceId)).thenAnswer(inv -> {
            Thread.sleep(50);
            return "{\"type\":\"record\",\"name\":\"User\"}";
        });
        when(targetClient.registerSchema(eq(topic + "-value"), any(String.class))).thenAnswer(inv -> {
            Thread.sleep(50);
            return targetId;
        });

        int threadCount = 30;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                startGate.await(); // синхронный одновременный старт всех 30 потоков
                return resolver.resolveConfluentToConfluent(topic, sourceId);
            }));
        }

        startGate.countDown(); // открываем стартовый гейт

        for (Future<Integer> f : futures) {
            assertEquals(targetId, f.get(5, TimeUnit.SECONDS), "Все параллельные потоки должны получить единый targetId");
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));

        // Ровно одно сетевое обращение к источнику и целевому реестру для всех 30 потоков
        verify(sourceClient, times(1)).getSchemaById(sourceId);
        verify(targetClient, times(1)).registerSchema(eq(topic + "-value"), any(String.class));
    }

    @Test
    @DisplayName("BoundedLruCache: высокая многопоточная нагрузка на Lock-Free чтение и вытеснение")
    void testLockFreeCacheUnderHeavyConcurrentReadWrite() throws Exception {
        int maxEntries = 50;
        CachedSchemaRegistryResolver.BoundedLruCache<Integer, String> cache =
                new CachedSchemaRegistryResolver.BoundedLruCache<>(maxEntries);

        int threadCount = 16;
        int operationsPerThread = 2_000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successfulReads = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        int key = (threadId * 100) + (i % 70);
                        cache.put(key, "val-" + key);
                        String read = cache.get(key);
                        if (read != null) {
                            successfulReads.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Все потоки должны завершиться вовремя");
        executor.shutdown();

        // Кэш никогда не должен превысить maxSize с запасом
        assertTrue(cache.size() <= maxEntries, "Размер кэша " + cache.size() + " не должен превышать maxEntries " + maxEntries);
        assertTrue(successfulReads.get() > 0, "Должны быть успешные чтения");
    }
}

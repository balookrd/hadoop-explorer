package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Недоступный реестр схем не должен опрашиваться на каждую запись батча.
 *
 * <p>Single-flight снимает только одновременные запросы. Записи внутри одного poll-цикла идут
 * последовательно, поэтому при недоступном реестре каждая из них делала собственный HTTP-запрос
 * с полным таймаутом: 500 записей × 5 с — и брокер исключает консьюмера из группы.
 */
class SchemaResolverNegativeCacheTest {

    private static final long BACKOFF_MS = 10_000L;

    @Test
    @DisplayName("После отказа реестра повторные промахи не ходят в сеть до конца паузы")
    void failedLookupIsNotRetriedForEveryRecord() throws Exception {
        ConfluentRegistryClient source = Mockito.mock(ConfluentRegistryClient.class);
        ConfluentRegistryClient target = Mockito.mock(ConfluentRegistryClient.class);
        when(source.getSchemaById(anyInt())).thenThrow(new IOException("реестр недоступен"));

        AtomicLong nanos = new AtomicLong(0L);
        CachedSchemaRegistryResolver resolver = new CachedSchemaRegistryResolver(
                source, target, null, null, 1000, BACKOFF_MS, nanos::get);

        for (int i = 0; i < 50; i++) {
            assertEquals(42, resolver.resolveConfluentToConfluent("orders", 42),
                    "при недоступном реестре возвращается исходный идентификатор");
        }

        verify(source, times(1)).getSchemaById(42);
    }

    @Test
    @DisplayName("По истечении паузы реестр опрашивается снова")
    void lookupResumesAfterBackoffExpires() throws Exception {
        ConfluentRegistryClient source = Mockito.mock(ConfluentRegistryClient.class);
        ConfluentRegistryClient target = Mockito.mock(ConfluentRegistryClient.class);
        when(source.getSchemaById(anyInt()))
                .thenThrow(new IOException("реестр недоступен"))
                .thenReturn("{\"type\":\"record\"}");
        when(target.registerSchema(anyString(), anyString())).thenReturn(77);

        AtomicLong nanos = new AtomicLong(0L);
        CachedSchemaRegistryResolver resolver = new CachedSchemaRegistryResolver(
                source, target, null, null, 1000, BACKOFF_MS, nanos::get);

        assertEquals(42, resolver.resolveConfluentToConfluent("orders", 42));
        assertEquals(42, resolver.resolveConfluentToConfluent("orders", 42));
        verify(source, times(1)).getSchemaById(42);

        nanos.addAndGet((BACKOFF_MS + 1) * 1_000_000L);

        assertEquals(77, resolver.resolveConfluentToConfluent("orders", 42),
                "после паузы реестр снова опрашивается и схема регистрируется");
        verify(source, times(2)).getSchemaById(42);
    }

    @Test
    @DisplayName("Пауза для одной схемы не мешает разрешать другие")
    void backoffIsPerSchemaId() throws Exception {
        ConfluentRegistryClient source = Mockito.mock(ConfluentRegistryClient.class);
        ConfluentRegistryClient target = Mockito.mock(ConfluentRegistryClient.class);
        when(source.getSchemaById(42)).thenThrow(new IOException("реестр недоступен"));
        when(source.getSchemaById(43)).thenReturn("{\"type\":\"record\"}");
        when(target.registerSchema(anyString(), anyString())).thenReturn(99);

        AtomicLong nanos = new AtomicLong(0L);
        CachedSchemaRegistryResolver resolver = new CachedSchemaRegistryResolver(
                source, target, null, null, 1000, BACKOFF_MS, nanos::get);

        assertEquals(42, resolver.resolveConfluentToConfluent("orders", 42));
        assertEquals(99, resolver.resolveConfluentToConfluent("orders", 43));
        assertEquals(42, resolver.resolveConfluentToConfluent("orders", 42));

        verify(source, times(1)).getSchemaById(42);
        verify(source, times(1)).getSchemaById(43);
    }
}

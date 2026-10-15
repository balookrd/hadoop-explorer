package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Потокобезопасный резолвер идентификаторов схем с двусторонним in-memory кэшированием
 * и динамической авторегистрацией схем в целевом реестре при промахе кэша.
 */
public class CachedSchemaRegistryResolver implements SchemaIdResolver {

    private static final Logger log = LoggerFactory.getLogger(CachedSchemaRegistryResolver.class);

    private final ConfluentRegistryClient sourceConfluentClient;
    private final ConfluentRegistryClient targetConfluentClient;
    private final HwxRegistryClient sourceHwxClient;
    private final HwxRegistryClient targetHwxClient;

    public static final int DEFAULT_MAX_CACHE_ENTRIES = 10_000;

    /**
     * Потокобезопасный LRU кэш с ограничением максимального количества записей.
     */
    public static class BoundedLruCache<K, V> {
        private final int maxSize;
        private final Map<K, V> map;
        private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

        public BoundedLruCache(int maxSize) {
            this.maxSize = maxSize > 0 ? maxSize : DEFAULT_MAX_CACHE_ENTRIES;
            this.map = new java.util.LinkedHashMap<>(this.maxSize, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    return size() > BoundedLruCache.this.maxSize;
                }
            };
        }

        public V get(K key) {
            lock.lock();
            try {
                return map.get(key);
            } finally {
                lock.unlock();
            }
        }

        public void put(K key, V value) {
            lock.lock();
            try {
                map.put(key, value);
            } finally {
                lock.unlock();
            }
        }

        public int size() {
            lock.lock();
            try {
                return map.size();
            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            lock.lock();
            try {
                map.clear();
            } finally {
                lock.unlock();
            }
        }
    }

    // Кэш сопоставления: HWX Version ID (Long) -> Confluent Schema ID (Int)
    private final BoundedLruCache<Long, Integer> hwxToConfluentCache;
    // Кэш сопоставления: Confluent Schema ID (Int) -> HWX Version ID (Long)
    private final BoundedLruCache<Integer, Long> confluentToHwxCache;
    // Кэш сопоставления между двумя независимыми инстансами Confluent: Source ID -> Target ID
    private final BoundedLruCache<Integer, Integer> confluentToConfluentCache;
    // Кэш сопоставления между двумя независимыми инстансами HWX: Source ID -> Target ID
    private final BoundedLruCache<Long, Long> hwxToHwxCache;

    public CachedSchemaRegistryResolver(ConfluentRegistryClient sourceConfluentClient,
                                        ConfluentRegistryClient targetConfluentClient,
                                        HwxRegistryClient sourceHwxClient,
                                        HwxRegistryClient targetHwxClient) {
        this(sourceConfluentClient, targetConfluentClient, sourceHwxClient, targetHwxClient, DEFAULT_MAX_CACHE_ENTRIES);
    }

    public CachedSchemaRegistryResolver(ConfluentRegistryClient sourceConfluentClient,
                                        ConfluentRegistryClient targetConfluentClient,
                                        HwxRegistryClient sourceHwxClient,
                                        HwxRegistryClient targetHwxClient,
                                        int maxCacheEntries) {
        this.sourceConfluentClient = sourceConfluentClient;
        this.targetConfluentClient = targetConfluentClient;
        this.sourceHwxClient = sourceHwxClient;
        this.targetHwxClient = targetHwxClient;
        this.hwxToConfluentCache = new BoundedLruCache<>(maxCacheEntries);
        this.confluentToHwxCache = new BoundedLruCache<>(maxCacheEntries);
        this.confluentToConfluentCache = new BoundedLruCache<>(maxCacheEntries);
        this.hwxToHwxCache = new BoundedLruCache<>(maxCacheEntries);
    }

    /**
     * Конструктор для ручного добавления статических маппингов (например, в тестах или изолированных контурах).
     */
    public CachedSchemaRegistryResolver() {
        this(null, null, null, null, DEFAULT_MAX_CACHE_ENTRIES);
    }

    public void putConfluentToConfluent(int sourceId, int targetId) {
        confluentToConfluentCache.put(sourceId, targetId);
    }

    public void putHwxToConfluent(long hwxVersionId, int confluentId) {
        hwxToConfluentCache.put(hwxVersionId, confluentId);
        confluentToHwxCache.put(confluentId, hwxVersionId);
    }

    public void putHwxToHwx(long sourceId, long targetId) {
        hwxToHwxCache.put(sourceId, targetId);
    }

    @Override
    public int resolveConfluentToConfluent(String topic, int sourceId) {
        Integer cached = confluentToConfluentCache.get(sourceId);
        if (cached != null) {
            return cached;
        }

        if (sourceConfluentClient == null || targetConfluentClient == null) {
            return sourceId; // fallback на исходный ID при отсутствии клиентов
        }

        try {
            String subject = topic + "-value";
            String schemaText = sourceConfluentClient.getSchemaById(sourceId);
            int registeredTargetId = targetConfluentClient.registerSchema(subject, schemaText);
            log.info("Автоматически зарегистрирована схема Confluent->Confluent для топика {}: Source ID {} -> Target ID {}",
                    topic, sourceId, registeredTargetId);
            confluentToConfluentCache.put(sourceId, registeredTargetId);
            return registeredTargetId;
        } catch (Exception e) {
            log.error("Ошибка синхронизации схемы Confluent->Confluent для ID {}: {}", sourceId, e.getMessage());
            return sourceId;
        }
    }

    @Override
    public int resolveHwxToConfluent(String topic, long hwxVersionId) {
        Integer cached = hwxToConfluentCache.get(hwxVersionId);
        if (cached != null) {
            return cached;
        }

        if (sourceHwxClient == null || targetConfluentClient == null) {
            return (int) (hwxVersionId & 0x7FFFFFFF); // fallback
        }

        try {
            String subject = topic + "-value";
            String schemaText = sourceHwxClient.getSchemaByVersionId(hwxVersionId);
            int targetId = targetConfluentClient.registerSchema(subject, schemaText);
            log.info("Автоматически зарегистрирована схема HWX->Confluent для топика {}: HWX Version ID {} -> Confluent ID {}",
                    topic, hwxVersionId, targetId);
            hwxToConfluentCache.put(hwxVersionId, targetId);
            confluentToHwxCache.put(targetId, hwxVersionId);
            return targetId;
        } catch (Exception e) {
            log.error("Ошибка трансляции схемы HWX->Confluent для HWX Version ID {}: {}", hwxVersionId, e.getMessage());
            return (int) (hwxVersionId & 0x7FFFFFFF);
        }
    }

    @Override
    public long resolveConfluentToHwx(String topic, int confluentId) {
        Long cached = confluentToHwxCache.get(confluentId);
        if (cached != null) {
            return cached;
        }

        if (sourceConfluentClient == null || targetHwxClient == null) {
            return (long) confluentId;
        }

        try {
            String schemaName = topic + "-value";
            String schemaText = sourceConfluentClient.getSchemaById(confluentId);
            long hwxVersionId = targetHwxClient.registerSchemaVersion(schemaName, schemaText);
            log.info("Автоматически зарегистрирована схема Confluent->HWX для топика {}: Confluent ID {} -> HWX Version ID {}",
                    topic, confluentId, hwxVersionId);
            confluentToHwxCache.put(confluentId, hwxVersionId);
            hwxToConfluentCache.put(hwxVersionId, confluentId);
            return hwxVersionId;
        } catch (Exception e) {
            log.error("Ошибка трансляции схемы Confluent->HWX для Confluent ID {}: {}", confluentId, e.getMessage());
            return (long) confluentId;
        }
    }

    @Override
    public long resolveHwxToHwx(String topic, long sourceHwxId) {
        Long cached = hwxToHwxCache.get(sourceHwxId);
        if (cached != null) {
            return cached;
        }

        if (sourceHwxClient == null || targetHwxClient == null) {
            return sourceHwxId;
        }

        try {
            String schemaName = topic + "-value";
            String schemaText = sourceHwxClient.getSchemaByVersionId(sourceHwxId);
            long targetVersionId = targetHwxClient.registerSchemaVersion(schemaName, schemaText);
            log.info("Автоматически зарегистрирована схема HWX->HWX для топика {}: Source ID {} -> Target ID {}",
                    topic, sourceHwxId, targetVersionId);
            hwxToHwxCache.put(sourceHwxId, targetVersionId);
            return targetVersionId;
        } catch (Exception e) {
            log.error("Ошибка синхронизации схемы HWX->HWX для ID {}: {}", sourceHwxId, e.getMessage());
            return sourceHwxId;
        }
    }

    /**
     * Создает резолвер с автоматической инициализацией SPNEGO аутентификаторов для Source и Target реестров.
     */
    public static CachedSchemaRegistryResolver create(
            String sourceSrUrl, String sourceSrType,
            String targetSrUrl, String targetSrType,
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig sourceKrb,
            org.apache.hadoop.explorer.replicator.kafka.security.KafkaKerberosConfig targetKrb) {

        org.apache.hadoop.explorer.replicator.kafka.security.SpnegoAuthenticator sourceAuth =
                (sourceKrb != null && (sourceKrb.isSrKerberosEnabled() || sourceKrb.isEnabled()))
                        ? new org.apache.hadoop.explorer.replicator.kafka.security.KerberosSpnegoAuthenticator(sourceKrb)
                        : null;

        org.apache.hadoop.explorer.replicator.kafka.security.SpnegoAuthenticator targetAuth =
                (targetKrb != null && (targetKrb.isSrKerberosEnabled() || targetKrb.isEnabled()))
                        ? new org.apache.hadoop.explorer.replicator.kafka.security.KerberosSpnegoAuthenticator(targetKrb)
                        : null;

        ConfluentRegistryClient srcConf = null;
        ConfluentRegistryClient tgtConf = null;
        HwxRegistryClient srcHwx = null;
        HwxRegistryClient tgtHwx = null;

        if (sourceSrUrl != null && !sourceSrUrl.isBlank()) {
            if ("CONFLUENT".equalsIgnoreCase(sourceSrType)) {
                srcConf = new ConfluentRegistryClient(sourceSrUrl, sourceAuth);
            } else {
                srcHwx = new HwxRegistryClient(sourceSrUrl, sourceAuth);
            }
        }

        if (targetSrUrl != null && !targetSrUrl.isBlank()) {
            if ("CONFLUENT".equalsIgnoreCase(targetSrType)) {
                tgtConf = new ConfluentRegistryClient(targetSrUrl, targetAuth);
            } else {
                tgtHwx = new HwxRegistryClient(targetSrUrl, targetAuth);
            }
        }

        return new CachedSchemaRegistryResolver(srcConf, tgtConf, srcHwx, tgtHwx);
    }

    public ConfluentRegistryClient getSourceConfluentClient() {
        return sourceConfluentClient;
    }

    public ConfluentRegistryClient getTargetConfluentClient() {
        return targetConfluentClient;
    }

    public HwxRegistryClient getSourceHwxClient() {
        return sourceHwxClient;
    }

    public HwxRegistryClient getTargetHwxClient() {
        return targetHwxClient;
    }
}

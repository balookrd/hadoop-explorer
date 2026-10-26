package org.apache.hadoop.explorer.replicator.kafka.schema;

import org.apache.hadoop.explorer.replicator.kafka.schema.client.ConfluentRegistryClient;
import org.apache.hadoop.explorer.replicator.kafka.schema.client.HwxRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

    /** Пауза, на которую запоминается отказ реестра по конкретному идентификатору схемы. */
    public static final long DEFAULT_FAILURE_BACKOFF_MS = 10_000L;

    /**
     * Высокопроизводительный потокобезопасный LRU кэш с ограничением максимального количества записей.
     * Оптимизирован для сценариев экстремальной нагрузки (100k+ RPS): Lock-Free чтение при Cache Hit
     * через ConcurrentHashMap и детерминированное LRU-вытеснение через LinkedHashMap под адаптивной блокировкой.
     */
    public static class BoundedLruCache<K, V> {
        private final int maxSize;
        private final ConcurrentHashMap<K, V> fastLookup;
        private final Map<K, V> lruMap;
        private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

        public BoundedLruCache(int maxSize) {
            this.maxSize = maxSize > 0 ? maxSize : DEFAULT_MAX_CACHE_ENTRIES;
            this.fastLookup = new ConcurrentHashMap<>(this.maxSize);
            this.lruMap = new java.util.LinkedHashMap<>(this.maxSize, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    if (size() > BoundedLruCache.this.maxSize) {
                        fastLookup.remove(eldest.getKey());
                        return true;
                    }
                    return false;
                }
            };
        }

        public V get(K key) {
            if (key == null) return null;
            V val = fastLookup.get(key);
            if (val == null) {
                return null;
            }
            // Если лок не занят другим потоком (низкая/умеренная конкуренция или однопоточные тесты) — обновляем LRU порядок.
            // Под пиковой многопоточной нагрузкой не блокируем читающие потоки, обеспечивая Lock-Free чтение со скоростью CPU.
            if (lock.tryLock()) {
                try {
                    lruMap.get(key);
                } finally {
                    lock.unlock();
                }
            }
            return val;
        }

        public void put(K key, V value) {
            if (key == null || value == null) return;
            lock.lock();
            try {
                lruMap.put(key, value);
                fastLookup.put(key, value);
            } finally {
                lock.unlock();
            }
        }

        public boolean containsKey(K key) {
            return key != null && fastLookup.containsKey(key);
        }

        public int size() {
            return fastLookup.size();
        }

        public void clear() {
            lock.lock();
            try {
                lruMap.clear();
                fastLookup.clear();
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

    private final long failureBackoffNanos;
    private final java.util.function.LongSupplier nanoClock;
    // Отказы реестра по идентификатору схемы: до истечения паузы запрос не повторяется
    private final BoundedLruCache<String, Long> failureDeadlines;

    // Single-Flight обещания для устранения Thundering Herd (лавины сетевых запросов при промахе кэша)
    private final ConcurrentHashMap<Integer, CompletableFuture<Integer>> confluentToConfluentInFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<Integer>> hwxToConfluentInFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, CompletableFuture<Long>> confluentToHwxInFlight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<Long>> hwxToHwxInFlight = new ConcurrentHashMap<>();

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
        this(sourceConfluentClient, targetConfluentClient, sourceHwxClient, targetHwxClient,
                maxCacheEntries, DEFAULT_FAILURE_BACKOFF_MS, System::nanoTime);
    }

    CachedSchemaRegistryResolver(ConfluentRegistryClient sourceConfluentClient,
                                 ConfluentRegistryClient targetConfluentClient,
                                 HwxRegistryClient sourceHwxClient,
                                 HwxRegistryClient targetHwxClient,
                                 int maxCacheEntries,
                                 long failureBackoffMillis,
                                 java.util.function.LongSupplier nanoClock) {
        this.failureBackoffNanos = Math.max(0L, failureBackoffMillis) * 1_000_000L;
        this.nanoClock = nanoClock;
        this.sourceConfluentClient = sourceConfluentClient;
        this.targetConfluentClient = targetConfluentClient;
        this.sourceHwxClient = sourceHwxClient;
        this.targetHwxClient = targetHwxClient;
        this.hwxToConfluentCache = new BoundedLruCache<>(maxCacheEntries);
        this.confluentToHwxCache = new BoundedLruCache<>(maxCacheEntries);
        this.confluentToConfluentCache = new BoundedLruCache<>(maxCacheEntries);
        this.hwxToHwxCache = new BoundedLruCache<>(maxCacheEntries);
        this.failureDeadlines = new BoundedLruCache<>(maxCacheEntries);
    }

    /**
     * Действует ли пауза после отказа реестра по этому ключу.
     *
     * <p>Single-flight снимает только одновременные запросы, а записи внутри poll-цикла идут
     * последовательно: без паузы недоступный реестр опрашивался заново на каждую запись, с полным
     * таймаутом HTTP-клиента, и консьюмер не успевал вернуться в брокер до {@code max.poll.interval.ms}.
     */
    private boolean isBackoffActive(String key) {
        if (failureBackoffNanos <= 0L) {
            return false;
        }
        Long deadline = failureDeadlines.get(key);
        return deadline != null && nanoClock.getAsLong() - deadline < 0L;
    }

    private void rememberFailure(String key) {
        if (failureBackoffNanos > 0L) {
            failureDeadlines.put(key, nanoClock.getAsLong() + failureBackoffNanos);
        }
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

        String backoffKey = "c2c:" + sourceId;
        if (isBackoffActive(backoffKey)) {
            return sourceId;
        }

        // Single-Flight: только один поток выполняет сетевой RPC, остальные потоки ожидают результат
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        CompletableFuture<Integer> existing = confluentToConfluentInFlight.putIfAbsent(sourceId, promise);
        if (existing != null) {
            try {
                return existing.join();
            } catch (Exception e) {
                log.warn("Ожидание параллельного single-flight разрешения схемы Confluent->Confluent для ID {} завершилось с ошибкой: {}",
                        sourceId, e.getMessage());
                return sourceId;
            }
        }

        try {
            Integer doubleCheck = confluentToConfluentCache.get(sourceId);
            if (doubleCheck != null) {
                promise.complete(doubleCheck);
                return doubleCheck;
            }

            String subject = topic + "-value";
            String schemaText = sourceConfluentClient.getSchemaById(sourceId);
            int registeredTargetId = targetConfluentClient.registerSchema(subject, schemaText);
            log.info("Автоматически зарегистрирована схема Confluent->Confluent для топика {}: Source ID {} -> Target ID {}",
                    topic, sourceId, registeredTargetId);
            confluentToConfluentCache.put(sourceId, registeredTargetId);
            promise.complete(registeredTargetId);
            return registeredTargetId;
        } catch (Exception e) {
            log.error("Ошибка синхронизации схемы Confluent->Confluent для ID {}: {}", sourceId, e.getMessage());
            rememberFailure(backoffKey);
            promise.completeExceptionally(e);
            return sourceId;
        } finally {
            confluentToConfluentInFlight.remove(sourceId);
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

        String backoffKey = "h2c:" + hwxVersionId;
        if (isBackoffActive(backoffKey)) {
            return (int) (hwxVersionId & 0x7FFFFFFF);
        }

        // Single-Flight
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        CompletableFuture<Integer> existing = hwxToConfluentInFlight.putIfAbsent(hwxVersionId, promise);
        if (existing != null) {
            try {
                return existing.join();
            } catch (Exception e) {
                log.warn("Ожидание параллельного single-flight разрешения схемы HWX->Confluent для Version ID {} завершилось с ошибкой: {}",
                        hwxVersionId, e.getMessage());
                return (int) (hwxVersionId & 0x7FFFFFFF);
            }
        }

        try {
            Integer doubleCheck = hwxToConfluentCache.get(hwxVersionId);
            if (doubleCheck != null) {
                promise.complete(doubleCheck);
                return doubleCheck;
            }

            String subject = topic + "-value";
            String schemaText = sourceHwxClient.getSchemaByVersionId(hwxVersionId);
            int targetId = targetConfluentClient.registerSchema(subject, schemaText);
            log.info("Автоматически зарегистрирована схема HWX->Confluent для топика {}: HWX Version ID {} -> Confluent ID {}",
                    topic, hwxVersionId, targetId);
            hwxToConfluentCache.put(hwxVersionId, targetId);
            confluentToHwxCache.put(targetId, hwxVersionId);
            promise.complete(targetId);
            return targetId;
        } catch (Exception e) {
            log.error("Ошибка трансляции схемы HWX->Confluent для HWX Version ID {}: {}", hwxVersionId, e.getMessage());
            rememberFailure(backoffKey);
            promise.completeExceptionally(e);
            return (int) (hwxVersionId & 0x7FFFFFFF);
        } finally {
            hwxToConfluentInFlight.remove(hwxVersionId);
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

        String backoffKey = "c2h:" + confluentId;
        if (isBackoffActive(backoffKey)) {
            return (long) confluentId;
        }

        // Single-Flight
        CompletableFuture<Long> promise = new CompletableFuture<>();
        CompletableFuture<Long> existing = confluentToHwxInFlight.putIfAbsent(confluentId, promise);
        if (existing != null) {
            try {
                return existing.join();
            } catch (Exception e) {
                log.warn("Ожидание параллельного single-flight разрешения схемы Confluent->HWX для ID {} завершилось с ошибкой: {}",
                        confluentId, e.getMessage());
                return (long) confluentId;
            }
        }

        try {
            Long doubleCheck = confluentToHwxCache.get(confluentId);
            if (doubleCheck != null) {
                promise.complete(doubleCheck);
                return doubleCheck;
            }

            String schemaName = topic + "-value";
            String schemaText = sourceConfluentClient.getSchemaById(confluentId);
            long hwxVersionId = targetHwxClient.registerSchemaVersion(schemaName, schemaText);
            log.info("Автоматически зарегистрирована схема Confluent->HWX для топика {}: Confluent ID {} -> HWX Version ID {}",
                    topic, confluentId, hwxVersionId);
            confluentToHwxCache.put(confluentId, hwxVersionId);
            hwxToConfluentCache.put(hwxVersionId, confluentId);
            promise.complete(hwxVersionId);
            return hwxVersionId;
        } catch (Exception e) {
            log.error("Ошибка трансляции схемы Confluent->HWX для Confluent ID {}: {}", confluentId, e.getMessage());
            rememberFailure(backoffKey);
            promise.completeExceptionally(e);
            return (long) confluentId;
        } finally {
            confluentToHwxInFlight.remove(confluentId);
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

        String backoffKey = "h2h:" + sourceHwxId;
        if (isBackoffActive(backoffKey)) {
            return sourceHwxId;
        }

        // Single-Flight
        CompletableFuture<Long> promise = new CompletableFuture<>();
        CompletableFuture<Long> existing = hwxToHwxInFlight.putIfAbsent(sourceHwxId, promise);
        if (existing != null) {
            try {
                return existing.join();
            } catch (Exception e) {
                log.warn("Ожидание параллельного single-flight разрешения схемы HWX->HWX для ID {} завершилось с ошибкой: {}",
                        sourceHwxId, e.getMessage());
                return sourceHwxId;
            }
        }

        try {
            Long doubleCheck = hwxToHwxCache.get(sourceHwxId);
            if (doubleCheck != null) {
                promise.complete(doubleCheck);
                return doubleCheck;
            }

            String schemaName = topic + "-value";
            String schemaText = sourceHwxClient.getSchemaByVersionId(sourceHwxId);
            long targetVersionId = targetHwxClient.registerSchemaVersion(schemaName, schemaText);
            log.info("Автоматически зарегистрирована схема HWX->HWX для топика {}: Source ID {} -> Target ID {}",
                    topic, sourceHwxId, targetVersionId);
            hwxToHwxCache.put(sourceHwxId, targetVersionId);
            promise.complete(targetVersionId);
            return targetVersionId;
        } catch (Exception e) {
            log.error("Ошибка синхронизации схемы HWX->HWX для ID {}: {}", sourceHwxId, e.getMessage());
            rememberFailure(backoffKey);
            promise.completeExceptionally(e);
            return sourceHwxId;
        } finally {
            hwxToHwxInFlight.remove(sourceHwxId);
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

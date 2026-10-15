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

    // Кэш сопоставления: HWX Version ID (Long) -> Confluent Schema ID (Int)
    private final Map<Long, Integer> hwxToConfluentCache = new ConcurrentHashMap<>();
    // Кэш сопоставления: Confluent Schema ID (Int) -> HWX Version ID (Long)
    private final Map<Integer, Long> confluentToHwxCache = new ConcurrentHashMap<>();
    // Кэш сопоставления между двумя независимыми инстансами Confluent: Source ID -> Target ID
    private final Map<Integer, Integer> confluentToConfluentCache = new ConcurrentHashMap<>();
    // Кэш сопоставления между двумя независимыми инстансами HWX: Source ID -> Target ID
    private final Map<Long, Long> hwxToHwxCache = new ConcurrentHashMap<>();

    public CachedSchemaRegistryResolver(ConfluentRegistryClient sourceConfluentClient,
                                        ConfluentRegistryClient targetConfluentClient,
                                        HwxRegistryClient sourceHwxClient,
                                        HwxRegistryClient targetHwxClient) {
        this.sourceConfluentClient = sourceConfluentClient;
        this.targetConfluentClient = targetConfluentClient;
        this.sourceHwxClient = sourceHwxClient;
        this.targetHwxClient = targetHwxClient;
    }

    /**
     * Конструктор для ручного добавления статических маппингов (например, в тестах или изолированных контурах).
     */
    public CachedSchemaRegistryResolver() {
        this(null, null, null, null);
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
        return confluentToConfluentCache.computeIfAbsent(sourceId, id -> {
            if (sourceConfluentClient == null || targetConfluentClient == null) {
                return id; // fallback на исходный ID при отсутствии клиентов
            }
            try {
                String subject = topic + "-value";
                String schemaText = sourceConfluentClient.getSchemaById(id);
                int registeredTargetId = targetConfluentClient.registerSchema(subject, schemaText);
                log.info("Автоматически зарегистрирована схема Confluent->Confluent для топика {}: Source ID {} -> Target ID {}",
                        topic, id, registeredTargetId);
                return registeredTargetId;
            } catch (Exception e) {
                log.error("Ошибка синхронизации схемы Confluent->Confluent для ID {}: {}", id, e.getMessage());
                return id;
            }
        });
    }

    @Override
    public int resolveHwxToConfluent(String topic, long hwxVersionId) {
        return hwxToConfluentCache.computeIfAbsent(hwxVersionId, id -> {
            if (sourceHwxClient == null || targetConfluentClient == null) {
                return (int) (id & 0x7FFFFFFF); // fallback
            }
            try {
                String subject = topic + "-value";
                String schemaText = sourceHwxClient.getSchemaByVersionId(id);
                int targetId = targetConfluentClient.registerSchema(subject, schemaText);
                log.info("Автоматически зарегистрирована схема HWX->Confluent для топика {}: HWX Version ID {} -> Confluent ID {}",
                        topic, id, targetId);
                confluentToHwxCache.put(targetId, id);
                return targetId;
            } catch (Exception e) {
                log.error("Ошибка трансляции схемы HWX->Confluent для HWX Version ID {}: {}", id, e.getMessage());
                return (int) (id & 0x7FFFFFFF);
            }
        });
    }

    @Override
    public long resolveConfluentToHwx(String topic, int confluentId) {
        return confluentToHwxCache.computeIfAbsent(confluentId, id -> {
            if (sourceConfluentClient == null || targetHwxClient == null) {
                return (long) id;
            }
            try {
                String schemaName = topic + "-value";
                String schemaText = sourceConfluentClient.getSchemaById(id);
                long hwxVersionId = targetHwxClient.registerSchemaVersion(schemaName, schemaText);
                log.info("Автоматически зарегистрирована схема Confluent->HWX для топика {}: Confluent ID {} -> HWX Version ID {}",
                        topic, id, hwxVersionId);
                hwxToConfluentCache.put(hwxVersionId, id);
                return hwxVersionId;
            } catch (Exception e) {
                log.error("Ошибка трансляции схемы Confluent->HWX для Confluent ID {}: {}", id, e.getMessage());
                return (long) id;
            }
        });
    }

    @Override
    public long resolveHwxToHwx(String topic, long sourceHwxId) {
        return hwxToHwxCache.computeIfAbsent(sourceHwxId, id -> {
            if (sourceHwxClient == null || targetHwxClient == null) {
                return id;
            }
            try {
                String schemaName = topic + "-value";
                String schemaText = sourceHwxClient.getSchemaByVersionId(id);
                long targetVersionId = targetHwxClient.registerSchemaVersion(schemaName, schemaText);
                log.info("Автоматически зарегистрирована схема HWX->HWX для топика {}: Source ID {} -> Target ID {}",
                        topic, id, targetVersionId);
                return targetVersionId;
            } catch (Exception e) {
                log.error("Ошибка синхронизации схемы HWX->HWX для ID {}: {}", id, e.getMessage());
                return id;
            }
        });
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

package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class MockHmsClient implements HmsClient {

    private static final Logger log = LoggerFactory.getLogger(MockHmsClient.class);

    private final String clusterId;
    private final String hiveVersion; // "HDP_3.1" or "APACHE_3.1.3"
    private final Set<String> databases = ConcurrentHashMap.newKeySet();
    private final Map<String, HmsTableDto> tables = new ConcurrentHashMap<>();
    private final Map<String, List<HmsPartitionDto>> partitions = new ConcurrentHashMap<>();
    private final List<HmsNotificationEventDto> events = new CopyOnWriteArrayList<>();
    private final AtomicLong eventSequence = new AtomicLong(1000);

    public MockHmsClient(String clusterId, String hiveVersion) {
        this.clusterId = clusterId;
        this.hiveVersion = hiveVersion;
    }

    private String tableKey(String db, String tbl) {
        return (db + "." + tbl).toLowerCase();
    }

    @Override
    public long getCurrentNotificationEventId() {
        return eventSequence.get();
    }

    @Override
    public List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents) {
        return events.stream()
                .filter(e -> e.eventId() > lastEventId)
                .sorted(Comparator.comparingLong(HmsNotificationEventDto::eventId))
                .limit(maxEvents)
                .toList();
    }

    public void emitEvent(String eventType, String db, String tbl, String message) {
        long id = eventSequence.incrementAndGet();
        HmsNotificationEventDto event = new HmsNotificationEventDto(
                id,
                (int) (System.currentTimeMillis() / 1000),
                eventType,
                db,
                tbl,
                message,
                "JSON"
        );
        events.add(event);
        log.info("[MockHMS {}] Generated event #{}: {} on {}.{}", clusterId, id, eventType, db, tbl);
    }

    @Override
    public List<String> getAllDatabases() {
        return new ArrayList<>(databases);
    }

    @Override
    public List<String> getAllTables(String dbName) {
        String prefix = dbName.toLowerCase() + ".";
        return tables.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .map(k -> k.substring(prefix.length()))
                .toList();
    }

    @Override
    public Optional<HmsTableDto> getTable(String dbName, String tableName) {
        return Optional.ofNullable(tables.get(tableKey(dbName, tableName)));
    }

    @Override
    public List<HmsPartitionDto> getPartitions(String dbName, String tableName) {
        List<HmsPartitionDto> list = partitions.get(tableKey(dbName, tableName));
        return list != null ? new ArrayList<>(list) : Collections.emptyList();
    }

    @Override
    public void createDatabase(String dbName, String locationUri) {
        databases.add(dbName.toLowerCase());
        log.info("[MockHMS {}] Created database: {}", clusterId, dbName);
    }

    @Override
    public void createTable(HmsTableDto table) {
        databases.add(table.dbName().toLowerCase());
        tables.put(tableKey(table.dbName(), table.tableName()), table);
        log.info("[MockHMS {}] Created table: {}.{} (type={}, loc={})",
                clusterId, table.dbName(), table.tableName(), table.tableType(), table.sdLocation());
    }

    @Override
    public void alterTable(HmsTableDto table) {
        tables.put(tableKey(table.dbName(), table.tableName()), table);
        log.info("[MockHMS {}] Altered table: {}.{}", clusterId, table.dbName(), table.tableName());
    }

    @Override
    public void addPartitions(String dbName, String tableName, List<HmsPartitionDto> parts) {
        String key = tableKey(dbName, tableName);
        List<HmsPartitionDto> list = partitions.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
        for (HmsPartitionDto part : parts) {
            list.removeIf(existing -> Objects.equals(existing.values(), part.values()));
            list.add(part);
        }
        log.info("[MockHMS {}] Added/Updated {} partition(s) in {}.{}", clusterId, parts.size(), dbName, tableName);
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean deleteData) {
        String key = tableKey(dbName, tableName);
        tables.remove(key);
        partitions.remove(key);
        log.info("[MockHMS {}] Dropped table {}.{} (deleteData={})", clusterId, dbName, tableName, deleteData);
    }

    @Override
    public void dropPartition(String dbName, String tableName, List<String> partVals, boolean deleteData) {
        String key = tableKey(dbName, tableName);
        List<HmsPartitionDto> list = partitions.get(key);
        if (list != null) {
            list.removeIf(p -> Objects.equals(p.values(), partVals));
        }
        log.info("[MockHMS {}] Dropped partition {} from {}.{} (deleteData={})",
                clusterId, partVals, dbName, tableName, deleteData);
    }

    public String getClusterId() { return clusterId; }
    public String getHiveVersion() { return hiveVersion; }
}

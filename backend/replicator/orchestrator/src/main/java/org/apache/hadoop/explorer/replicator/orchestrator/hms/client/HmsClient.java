package org.apache.hadoop.explorer.replicator.orchestrator.hms.client;

import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.model.HmsTableDto;

import java.util.List;
import java.util.Optional;

public interface HmsClient {

    long getCurrentNotificationEventId();

    List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents);

    List<String> getAllDatabases();

    List<String> getAllTables(String dbName);

    Optional<HmsTableDto> getTable(String dbName, String tableName);

    List<HmsPartitionDto> getPartitions(String dbName, String tableName);

    void createDatabase(String dbName, String locationUri);

    void createTable(HmsTableDto table);

    void alterTable(HmsTableDto table);

    void addPartitions(String dbName, String tableName, List<HmsPartitionDto> partitions);

    void dropTable(String dbName, String tableName, boolean deleteData);

    void dropPartition(String dbName, String tableName, List<String> partVals, boolean deleteData);
}

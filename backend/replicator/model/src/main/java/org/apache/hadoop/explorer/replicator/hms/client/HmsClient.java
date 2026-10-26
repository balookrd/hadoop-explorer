package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;

import java.util.List;
import java.util.Optional;

public interface HmsClient {

    long getCurrentNotificationEventId();

    List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents);

    List<String> getAllDatabases();

    default Optional<String> getDatabaseLocation(String dbName) {
        return Optional.empty();
    }

    List<String> getAllTables(String dbName);

    Optional<HmsTableDto> getTable(String dbName, String tableName);

    List<HmsPartitionDto> getPartitions(String dbName, String tableName);

    default Optional<HmsPartitionDto> getPartition(String dbName, String tableName, List<String> partVals) {
        if (partVals == null || partVals.isEmpty()) {
            return Optional.empty();
        }
        return getPartitions(dbName, tableName).stream()
                .filter(p -> partVals.equals(p.values()))
                .findFirst();
    }

    void createDatabase(String dbName, String locationUri);

    void createTable(HmsTableDto table);

    void alterTable(HmsTableDto table);

    void addPartitions(String dbName, String tableName, List<HmsPartitionDto> partitions);

    void dropTable(String dbName, String tableName, boolean deleteData);

    void dropPartition(String dbName, String tableName, List<String> partVals, boolean deleteData);

    /**
     * Выполнение действия от имени пользователя.
     *
     * <p>Реализация по умолчанию имперсонацию не выполняет: действие идет от имени процесса.
     * Клиент, который не умеет имперсонацию, обязан сказать об этом в своей документации —
     * иначе запись в логе о doAs будет приниматься за свидетельство того, что метастор увидел
     * пользователя. Имперсонацию выполняет транспорт Thrift, открывая соединение под
     * пользователем; HTTP-клиент поверх API оркестратора — нет.
     */
    default <T> T executeAs(String executionPrincipal, java.util.concurrent.Callable<T> action) throws Exception {
        return action.call();
    }
}

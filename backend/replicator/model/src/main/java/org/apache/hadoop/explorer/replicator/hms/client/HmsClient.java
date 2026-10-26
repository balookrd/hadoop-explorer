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

    /**
     * Имена партиций таблицы в формате метастора ({@code k1=v1/k2=v2}, значения экранированы).
     * Значение по умолчанию собирает их из партиций; реальный клиент берет только имена.
     */
    default List<String> getPartitionNames(String dbName, String tableName) {
        List<String> keys = getTable(dbName, tableName).map(HmsTableDto::partitionKeys).orElse(List.of());
        return getPartitions(dbName, tableName).stream().map(p -> p.getPartitionName(keys)).toList();
    }

    /**
     * Получатель порции при обходе с продолжением: {@code total} — партиций у таблицы всего,
     * {@code skipped} — пропущено как уже перенесенные, {@code lastName} — имя последней
     * партиции порции (по нему продолжают в следующий раз).
     */
    @FunctionalInterface
    interface ResumableBatchConsumer {
        void accept(List<HmsPartitionDto> batch, int total, int skipped, String lastName) throws Exception;
    }

    /**
     * Обход партиций в порядке имен, начиная после {@code startAfterName} (или с начала).
     *
     * <p>Порядок задается сортировкой имен в Java, а не базой метастора: collation MySQL,
     * PostgreSQL и Derby различается, и продолжение по имени должно быть однозначным.
     */
    default int forEachPartitionBatchAfter(String dbName, String tableName, int batchSize, String startAfterName,
                                           ResumableBatchConsumer consumer) throws Exception {
        List<String> keys = getTable(dbName, tableName).map(HmsTableDto::partitionKeys).orElse(List.of());
        java.util.TreeMap<String, HmsPartitionDto> byName = new java.util.TreeMap<>();
        for (HmsPartitionDto p : getPartitions(dbName, tableName)) {
            byName.put(p.getPartitionName(keys), p);
        }
        var remaining = startAfterName == null ? byName : byName.tailMap(startAfterName, false);
        int total = byName.size();
        int skipped = total - remaining.size();
        List<String> names = new java.util.ArrayList<>(remaining.keySet());
        int size = Math.max(1, batchSize);
        for (int from = 0; from < names.size(); from += size) {
            List<String> slice = names.subList(from, Math.min(from + size, names.size()));
            consumer.accept(slice.stream().map(byName::get).toList(), total, skipped, slice.get(slice.size() - 1));
        }
        return total;
    }

    /** Получатель порции партиций: {@code total} — сколько партиций у таблицы всего. */
    @FunctionalInterface
    interface PartitionBatchConsumer {
        void accept(List<HmsPartitionDto> batch, int total) throws Exception;
    }

    /**
     * Обход партиций таблицы порциями.
     *
     * <p>Таблица в 200k партиций со столбцами у каждой не помещается в память агента одним
     * списком, тем более при параллельном bootstrap нескольких таблиц. Реализация обязана
     * держать в памяти не больше одной порции; значение по умолчанию годится только для
     * клиентов, у которых партиции и так лежат в памяти.
     *
     * @return сколько партиций обойдено
     */
    default int forEachPartitionBatch(String dbName, String tableName, int batchSize,
                                      PartitionBatchConsumer consumer) throws Exception {
        List<HmsPartitionDto> all = getPartitions(dbName, tableName);
        int size = Math.max(1, batchSize);
        for (int from = 0; from < all.size(); from += size) {
            consumer.accept(all.subList(from, Math.min(from + size, all.size())), all.size());
        }
        return all.size();
    }

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

    /** Переименование таблицы с сохранением ее партиций (alter_table со сменой имени). */
    default void renameTable(String dbName, String oldTableName, HmsTableDto renamed) {
        throw new UnsupportedOperationException("Клиент метастора не поддерживает переименование таблиц");
    }

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

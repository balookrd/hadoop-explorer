package org.apache.hadoop.explorer.replicator.kafka.schema;

/**
 * Интерфейс разрешения и кросс-регистрации идентификаторов схем между независимыми реестрами.
 */
public interface SchemaIdResolver {

    /**
     * Преобразует Confluent Schema ID исходного кластера в локальный Schema ID целевого кластера Confluent.
     */
    int resolveConfluentToConfluent(String topic, int sourceId);

    /**
     * Преобразует Hortonworks Schema Version ID в эквивалентный Confluent Schema ID.
     */
    int resolveHwxToConfluent(String topic, long hwxVersionId);

    /**
     * Преобразует Confluent Schema ID в эквивалентный Hortonworks Schema Version ID.
     */
    long resolveConfluentToHwx(String topic, int confluentId);

    /**
     * Преобразует Hortonworks Version ID исходного кластера в локальный ID целевого кластера Hortonworks.
     */
    long resolveHwxToHwx(String topic, long sourceHwxId);
}

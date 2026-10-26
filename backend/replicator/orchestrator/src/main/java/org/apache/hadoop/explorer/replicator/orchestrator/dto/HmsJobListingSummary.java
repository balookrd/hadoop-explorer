package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Сводка по списку заданий метастора для текущего набора фильтров.
 *
 * <p>Счетчики и авторы описывают все задания, подходящие под отбор, а не загруженную страницу:
 * иначе чипы статусов считали бы сотню записей, а в фильтре по автору оказались бы только те,
 * кто попал на эту сотню.
 */
public record HmsJobListingSummary(
        @JsonProperty("total") long total,
        @JsonProperty("by_status") Map<String, Long> byStatus,
        @JsonProperty("replicated_tables") long replicatedTables,
        @JsonProperty("replicated_partitions") long replicatedPartitions,
        @JsonProperty("authors") List<String> authors
) {
}

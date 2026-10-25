package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;

/**
 * Сводка по списку заданий для текущего набора фильтров.
 *
 * <p>Нужна интерфейсу вместе со страничной выборкой: счетчики по статусам и список авторов
 * описывают все задания, а не загруженную страницу. Иначе фильтр по автору предлагал бы только
 * авторов из случайной сотни записей, а чипы статусов показывали бы счет по странице.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record JobListingSummary(
        long total,
        long running,
        long queued,
        long scheduled,
        long completed,
        long failed,
        long streaming,
        List<String> authors
) {
    public static JobListingSummary empty() {
        return new JobListingSummary(0, 0, 0, 0, 0, 0, 0, List.of());
    }
}

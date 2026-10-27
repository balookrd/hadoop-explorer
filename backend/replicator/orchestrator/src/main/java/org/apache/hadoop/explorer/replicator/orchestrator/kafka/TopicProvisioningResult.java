package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import java.util.Collections;
import java.util.List;

/**
 * Результат предварительной проверки и автосоздания топиков Kafka.
 */
public record TopicProvisioningResult(
        List<String> createdTopics,
        /**
         * Расхождения существующих целевых топиков с источником: меньше партиций, другая
         * политика очистки, меньший предел сообщения. Существующий топик не меняется: на
         * боевом кластере им пользуются и другие продюсеры.
         */
        List<String> conflicts,
        List<String> matchedTopics,
        boolean success,
        String message
) {
    public static TopicProvisioningResult empty() {
        return new TopicProvisioningResult(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true, "Список топиков пуст");
    }

    public static TopicProvisioningResult warning(String msg) {
        return new TopicProvisioningResult(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, msg);
    }

    public String toSummaryString() {
        StringBuilder sb = new StringBuilder();
        if (!createdTopics.isEmpty()) {
            sb.append("Созданы топики: ").append(String.join(", ", createdTopics)).append("; ");
        }
        if (!conflicts.isEmpty()) {
            sb.append("Расхождения с источником: ").append(String.join(", ", conflicts)).append("; ");
        }
        if (!matchedTopics.isEmpty()) {
            sb.append("Уже согласованы: ").append(String.join(", ", matchedTopics)).append("; ");
        }
        if (message != null && !message.isBlank()) {
            sb.append(message);
        }
        return sb.toString().trim();
    }
}

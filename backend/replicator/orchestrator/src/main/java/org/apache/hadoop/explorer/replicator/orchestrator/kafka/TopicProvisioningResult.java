package org.apache.hadoop.explorer.replicator.orchestrator.kafka;

import java.util.Collections;
import java.util.List;

/**
 * Результат предварительной проверки и автосоздания топиков Kafka.
 */
public record TopicProvisioningResult(
        List<String> createdTopics,
        List<String> expandedTopics,
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
        if (!expandedTopics.isEmpty()) {
            sb.append("Расширены партиции: ").append(String.join(", ", expandedTopics)).append("; ");
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

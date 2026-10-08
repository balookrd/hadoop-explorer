package org.apache.hadoop.explorer.common.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Структурированный логгер аудита безопасности платформы Hadoop Explorer.
 */
public class AuditLogger {

    private static final Logger log = LoggerFactory.getLogger("AUDIT_LOGGER");
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void logEvent(AuditRecord record) {
        try {
            String json = objectMapper.writeValueAsString(record);
            log.info("AUDIT_EVENT: {}", json);
        } catch (Exception e) {
            log.error("Failed to serialize audit record: {}", record, e);
        }
    }
}

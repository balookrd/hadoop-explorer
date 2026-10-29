package org.apache.hadoop.explorer.common.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Структурированный логгер аудита безопасности платформы Hadoop Explorer.
 */
public class AuditLogger {

    private static final Logger log = LoggerFactory.getLogger("AUDIT_LOGGER");
    // AuditRecord carries java.time.Instant, which plain ObjectMapper cannot serialize
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public void logEvent(AuditRecord record) {
        try {
            log.info("AUDIT_EVENT: {}", toJson(record));
        } catch (Exception e) {
            log.error("Failed to serialize audit record: {}", record, e);
        }
    }

    public String toJson(AuditRecord record) throws JsonProcessingException {
        return objectMapper.writeValueAsString(record);
    }
}

package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Запрос обновления статуса и прогресса выполнения задачи репликации.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpdateJobRequest {

    @JsonProperty("status")
    private String status;

    @JsonProperty("copied_bytes")
    private Long copiedBytes;

    @JsonProperty("total_bytes")
    private Long totalBytes;

    @JsonProperty("message")
    private String message;

    public UpdateJobRequest() {}

    public UpdateJobRequest(String status, Long copiedBytes, Long totalBytes, String message) {
        this.status = status;
        this.copiedBytes = copiedBytes;
        this.totalBytes = totalBytes;
        this.message = message;
    }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Long getCopiedBytes() { return copiedBytes; }
    public void setCopiedBytes(Long copiedBytes) { this.copiedBytes = copiedBytes; }

    public Long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(Long totalBytes) { this.totalBytes = totalBytes; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}

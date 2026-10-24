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

    @JsonProperty("total_objects")
    private Integer totalObjects;

    @JsonProperty("transferred_objects")
    private Integer transferredObjects;

    @JsonProperty("skipped_objects")
    private Integer skippedObjects;

    @JsonProperty("failed_objects")
    private Integer failedObjects;

    public UpdateJobRequest() {}

    public UpdateJobRequest(String status, Long copiedBytes, Long totalBytes, String message) {
        this.status = status;
        this.copiedBytes = copiedBytes;
        this.totalBytes = totalBytes;
        this.message = message;
    }

    public UpdateJobRequest(String status, Long copiedBytes, Long totalBytes, String message,
                            Integer totalObjects, Integer transferredObjects, Integer skippedObjects, Integer failedObjects) {
        this.status = status;
        this.copiedBytes = copiedBytes;
        this.totalBytes = totalBytes;
        this.message = message;
        this.totalObjects = totalObjects;
        this.transferredObjects = transferredObjects;
        this.skippedObjects = skippedObjects;
        this.failedObjects = failedObjects;
    }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Long getCopiedBytes() { return copiedBytes; }
    public void setCopiedBytes(Long copiedBytes) { this.copiedBytes = copiedBytes; }

    public Long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(Long totalBytes) { this.totalBytes = totalBytes; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Integer getTotalObjects() { return totalObjects; }
    public void setTotalObjects(Integer totalObjects) { this.totalObjects = totalObjects; }

    public Integer getTransferredObjects() { return transferredObjects; }
    public void setTransferredObjects(Integer transferredObjects) { this.transferredObjects = transferredObjects; }

    public Integer getSkippedObjects() { return skippedObjects; }
    public void setSkippedObjects(Integer skippedObjects) { this.skippedObjects = skippedObjects; }

    public Integer getFailedObjects() { return failedObjects; }
    public void setFailedObjects(Integer failedObjects) { this.failedObjects = failedObjects; }
}

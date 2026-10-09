package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Описание задачи репликации данных (Job DTO).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobDto {

    @JsonProperty("id")
    private String id;

    @JsonProperty("source_path")
    private String sourcePath;

    @JsonProperty("target_path")
    private String targetPath;

    @JsonProperty("source_cluster_id")
    private String sourceClusterId;

    @JsonProperty("target_cluster_id")
    private String targetClusterId;

    @JsonProperty("status")
    private String status;

    @JsonProperty("total_bytes")
    private Long totalBytes;

    @JsonProperty("copied_bytes")
    private Long copiedBytes;

    @JsonProperty("run_as_service_account")
    private Boolean runAsServiceAccount;

    @JsonProperty("execution_principal")
    private String executionPrincipal;

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

    public JobDto() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }

    public String getTargetPath() { return targetPath; }
    public void setTargetPath(String targetPath) { this.targetPath = targetPath; }

    public String getSourceClusterId() { return sourceClusterId; }
    public void setSourceClusterId(String sourceClusterId) { this.sourceClusterId = sourceClusterId; }

    public String getTargetClusterId() { return targetClusterId; }
    public void setTargetClusterId(String targetClusterId) { this.targetClusterId = targetClusterId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Long getTotalBytes() { return totalBytes; }
    public void setTotalBytes(Long totalBytes) { this.totalBytes = totalBytes; }

    public Long getCopiedBytes() { return copiedBytes; }
    public void setCopiedBytes(Long copiedBytes) { this.copiedBytes = copiedBytes; }

    public Boolean getRunAsServiceAccount() { return runAsServiceAccount != null ? runAsServiceAccount : Boolean.FALSE; }
    public void setRunAsServiceAccount(Boolean runAsServiceAccount) { this.runAsServiceAccount = runAsServiceAccount; }

    public String getExecutionPrincipal() { return executionPrincipal; }
    public void setExecutionPrincipal(String executionPrincipal) { this.executionPrincipal = executionPrincipal; }

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

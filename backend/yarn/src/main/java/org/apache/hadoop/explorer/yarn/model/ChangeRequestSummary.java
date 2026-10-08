package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ChangeRequestSummary {
    private Long id;

    @JsonProperty("cluster_id")
    private String clusterId;

    private String title;
    private String status;
    private String author;

    @JsonProperty("changes_count")
    private int changesCount;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("updated_at")
    private String updatedAt;

    private String reviewer;

    @JsonProperty("reviewed_at")
    private String reviewedAt;

    @JsonProperty("deployment_status")
    private String deploymentStatus;

    @JsonProperty("awx_job_id")
    private Integer awxJobId;

    @JsonProperty("deployed_at")
    private String deployedAt;

    public ChangeRequestSummary() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final ChangeRequestSummary obj = new ChangeRequestSummary();

        public Builder id(Long id) { obj.id = id; return this; }
        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder title(String title) { obj.title = title; return this; }
        public Builder status(String status) { obj.status = status; return this; }
        public Builder author(String author) { obj.author = author; return this; }
        public Builder changesCount(int changesCount) { obj.changesCount = changesCount; return this; }
        public Builder createdAt(String createdAt) { obj.createdAt = createdAt; return this; }
        public Builder updatedAt(String updatedAt) { obj.updatedAt = updatedAt; return this; }
        public Builder reviewer(String reviewer) { obj.reviewer = reviewer; return this; }
        public Builder reviewedAt(String reviewedAt) { obj.reviewedAt = reviewedAt; return this; }
        public Builder deploymentStatus(String deploymentStatus) { obj.deploymentStatus = deploymentStatus; return this; }
        public Builder awxJobId(Integer awxJobId) { obj.awxJobId = awxJobId; return this; }
        public Builder deployedAt(String deployedAt) { obj.deployedAt = deployedAt; return this; }

        public ChangeRequestSummary build() { return obj; }
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }
    public int getChangesCount() { return changesCount; }
    public void setChangesCount(int changesCount) { this.changesCount = changesCount; }
    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }
    public String getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }
    public String getReviewer() { return reviewer; }
    public void setReviewer(String reviewer) { this.reviewer = reviewer; }
    public String getReviewedAt() { return reviewedAt; }
    public void setReviewedAt(String reviewedAt) { this.reviewedAt = reviewedAt; }
    public String getDeploymentStatus() { return deploymentStatus; }
    public void setDeploymentStatus(String deploymentStatus) { this.deploymentStatus = deploymentStatus; }
    public Integer getAwxJobId() { return awxJobId; }
    public void setAwxJobId(Integer awxJobId) { this.awxJobId = awxJobId; }
    public String getDeployedAt() { return deployedAt; }
    public void setDeployedAt(String deployedAt) { this.deployedAt = deployedAt; }
}

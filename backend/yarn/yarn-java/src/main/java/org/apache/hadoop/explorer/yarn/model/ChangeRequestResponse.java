package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ChangeRequestResponse {
    private Long id;

    @JsonProperty("cluster_id")
    private String clusterId;

    private String title;
    private String description;
    private String status;
    private String author;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("updated_at")
    private String updatedAt;

    private String reviewer;

    @JsonProperty("review_comment")
    private String reviewComment;

    @JsonProperty("reviewed_at")
    private String reviewedAt;

    private List<QueueDraftItem> changes = new ArrayList<>();
    private List<DiffItem> diffs = new ArrayList<>();

    @JsonProperty("xml_content")
    private String xmlContent;

    @JsonProperty("deployment_status")
    private String deploymentStatus;

    @JsonProperty("awx_job_id")
    private Integer awxJobId;

    @JsonProperty("deployed_at")
    private String deployedAt;

    @JsonProperty("deployment_error")
    private String deploymentError;

    public ChangeRequestResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final ChangeRequestResponse obj = new ChangeRequestResponse();

        public Builder id(Long id) { obj.id = id; return this; }
        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder title(String title) { obj.title = title; return this; }
        public Builder description(String description) { obj.description = description; return this; }
        public Builder status(String status) { obj.status = status; return this; }
        public Builder author(String author) { obj.author = author; return this; }
        public Builder createdAt(String createdAt) { obj.createdAt = createdAt; return this; }
        public Builder updatedAt(String updatedAt) { obj.updatedAt = updatedAt; return this; }
        public Builder reviewer(String reviewer) { obj.reviewer = reviewer; return this; }
        public Builder reviewComment(String reviewComment) { obj.reviewComment = reviewComment; return this; }
        public Builder reviewedAt(String reviewedAt) { obj.reviewedAt = reviewedAt; return this; }
        public Builder changes(List<QueueDraftItem> changes) { obj.changes = changes; return this; }
        public Builder diffs(List<DiffItem> diffs) { obj.diffs = diffs; return this; }
        public Builder xmlContent(String xmlContent) { obj.xmlContent = xmlContent; return this; }
        public Builder deploymentStatus(String deploymentStatus) { obj.deploymentStatus = deploymentStatus; return this; }
        public Builder awxJobId(Integer awxJobId) { obj.awxJobId = awxJobId; return this; }
        public Builder deployedAt(String deployedAt) { obj.deployedAt = deployedAt; return this; }
        public Builder deploymentError(String deploymentError) { obj.deploymentError = deploymentError; return this; }

        public ChangeRequestResponse build() { return obj; }
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }
    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }
    public String getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }
    public String getReviewer() { return reviewer; }
    public void setReviewer(String reviewer) { this.reviewer = reviewer; }
    public String getReviewComment() { return reviewComment; }
    public void setReviewComment(String reviewComment) { this.reviewComment = reviewComment; }
    public String getReviewedAt() { return reviewedAt; }
    public void setReviewedAt(String reviewedAt) { this.reviewedAt = reviewedAt; }
    public List<QueueDraftItem> getChanges() { return changes; }
    public void setChanges(List<QueueDraftItem> changes) { this.changes = changes; }
    public List<DiffItem> getDiffs() { return diffs; }
    public void setDiffs(List<DiffItem> diffs) { this.diffs = diffs; }
    public String getXmlContent() { return xmlContent; }
    public void setXmlContent(String xmlContent) { this.xmlContent = xmlContent; }
    public String getDeploymentStatus() { return deploymentStatus; }
    public void setDeploymentStatus(String deploymentStatus) { this.deploymentStatus = deploymentStatus; }
    public Integer getAwxJobId() { return awxJobId; }
    public void setAwxJobId(Integer awxJobId) { this.awxJobId = awxJobId; }
    public String getDeployedAt() { return deployedAt; }
    public void setDeployedAt(String deployedAt) { this.deployedAt = deployedAt; }
    public String getDeploymentError() { return deploymentError; }
    public void setDeploymentError(String deploymentError) { this.deploymentError = deploymentError; }
}

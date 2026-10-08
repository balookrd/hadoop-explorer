package org.apache.hadoop.explorer.yarn.entity;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "change_requests")
public class ChangeRequestEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "cluster_id", nullable = false)
    private String clusterId;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(length = 2000)
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private ChangeRequestStatus status = ChangeRequestStatus.SUBMITTED;

    @Column(nullable = false, length = 100)
    private String author;

    @Lob
    @Column(name = "changes_json", nullable = false, columnDefinition = "TEXT")
    private String changesJson;

    @Lob
    @Column(name = "diffs_json", columnDefinition = "TEXT")
    private String diffsJson;

    @Lob
    @Column(name = "xml_content", columnDefinition = "TEXT")
    private String xmlContent;

    @Column(length = 100)
    private String reviewer;

    @Column(name = "review_comment", length = 2000)
    private String reviewComment;

    @Column(name = "reviewed_at")
    private Instant reviewedAt;

    @Column(name = "deployment_status", length = 50)
    private String deploymentStatus;

    @Column(name = "awx_job_id")
    private Integer awxJobId;

    @Column(name = "deployed_at")
    private Instant deployedAt;

    @Lob
    @Column(name = "deployment_error", columnDefinition = "TEXT")
    private String deploymentError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public ChangeRequestEntity() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final ChangeRequestEntity obj = new ChangeRequestEntity();

        public Builder id(Long id) { obj.id = id; return this; }
        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder title(String title) { obj.title = title; return this; }
        public Builder description(String description) { obj.description = description; return this; }
        public Builder status(ChangeRequestStatus status) { obj.status = status; return this; }
        public Builder author(String author) { obj.author = author; return this; }
        public Builder changesJson(String changesJson) { obj.changesJson = changesJson; return this; }
        public Builder diffsJson(String diffsJson) { obj.diffsJson = diffsJson; return this; }
        public Builder xmlContent(String xmlContent) { obj.xmlContent = xmlContent; return this; }
        public Builder reviewer(String reviewer) { obj.reviewer = reviewer; return this; }
        public Builder reviewComment(String reviewComment) { obj.reviewComment = reviewComment; return this; }
        public Builder reviewedAt(Instant reviewedAt) { obj.reviewedAt = reviewedAt; return this; }
        public Builder deploymentStatus(String deploymentStatus) { obj.deploymentStatus = deploymentStatus; return this; }
        public Builder awxJobId(Integer awxJobId) { obj.awxJobId = awxJobId; return this; }
        public Builder deployedAt(Instant deployedAt) { obj.deployedAt = deployedAt; return this; }
        public Builder deploymentError(String deploymentError) { obj.deploymentError = deploymentError; return this; }
        public Builder createdAt(Instant createdAt) { obj.createdAt = createdAt; return this; }
        public Builder updatedAt(Instant updatedAt) { obj.updatedAt = updatedAt; return this; }

        public ChangeRequestEntity build() { return obj; }
    }

    @PrePersist
    public void onPrePersist() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    public void onPreUpdate() {
        updatedAt = Instant.now();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public ChangeRequestStatus getStatus() { return status; }
    public void setStatus(ChangeRequestStatus status) { this.status = status; }
    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }
    public String getChangesJson() { return changesJson; }
    public void setChangesJson(String changesJson) { this.changesJson = changesJson; }
    public String getDiffsJson() { return diffsJson; }
    public void setDiffsJson(String diffsJson) { this.diffsJson = diffsJson; }
    public String getXmlContent() { return xmlContent; }
    public void setXmlContent(String xmlContent) { this.xmlContent = xmlContent; }
    public String getReviewer() { return reviewer; }
    public void setReviewer(String reviewer) { this.reviewer = reviewer; }
    public String getReviewComment() { return reviewComment; }
    public void setReviewComment(String reviewComment) { this.reviewComment = reviewComment; }
    public Instant getReviewedAt() { return reviewedAt; }
    public void setReviewedAt(Instant reviewedAt) { this.reviewedAt = reviewedAt; }
    public String getDeploymentStatus() { return deploymentStatus; }
    public void setDeploymentStatus(String deploymentStatus) { this.deploymentStatus = deploymentStatus; }
    public Integer getAwxJobId() { return awxJobId; }
    public void setAwxJobId(Integer awxJobId) { this.awxJobId = awxJobId; }
    public Instant getDeployedAt() { return deployedAt; }
    public void setDeployedAt(Instant deployedAt) { this.deployedAt = deployedAt; }
    public String getDeploymentError() { return deploymentError; }
    public void setDeploymentError(String deploymentError) { this.deploymentError = deploymentError; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}

package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "kafka_replication_jobs")
public class KafkaReplicationJobEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "name", nullable = false, length = 128)
    private String name;

    @Column(name = "source_dc", nullable = false, length = 64)
    private String sourceDc = "DC1";

    @Column(name = "target_dc", nullable = false, length = 64)
    private String targetDc = "DC2";

    @Column(name = "direction", nullable = false, length = 32)
    private String direction = "BIDIRECTIONAL"; // BIDIRECTIONAL / UNIDIRECTIONAL

    @Column(name = "topics_json", nullable = false, columnDefinition = "TEXT")
    private String topicsJson; // JSON список топиков и их payload_mode (PASSTHROUGH, AUTO_DETECT, SCHEMA_REGISTRY)

    @Column(name = "consumer_groups_json", columnDefinition = "TEXT")
    private String consumerGroupsJson; // JSON список групп для синхронизации смещений

    @Column(name = "schema_transcoding_enabled", nullable = false)
    private boolean schemaTranscodingEnabled = false;

    @Column(name = "source_sr_type", length = 32)
    private String sourceSrType; // CONFLUENT / HORTONWORKS

    @Column(name = "target_sr_type", length = 32)
    private String targetSrType; // CONFLUENT / HORTONWORKS

    @Column(name = "source_sr_url", length = 256)
    private String sourceSrUrl;

    @Column(name = "target_sr_url", length = 256)
    private String targetSrUrl;

    @Column(name = "source_sr_kerberos_enabled")
    private boolean sourceSrKerberosEnabled = false;

    @Column(name = "target_sr_kerberos_enabled")
    private boolean targetSrKerberosEnabled = false;

    @Column(name = "bandwidth_limit_mb")
    private Double bandwidthLimitMb = 50.0;

    @Column(name = "offset_sync_interval_sec")
    private Integer offsetSyncIntervalSec = 5;

    @Column(name = "consumer_threads")
    private Integer consumerThreads = 8;

    @Column(name = "batch_size")
    private Integer batchSize = 2000;

    @Column(name = "target_endpoints_json", columnDefinition = "TEXT")
    private String targetEndpointsJson;

    // Kerberos SASL / GSSAPI Configuration
    @Column(name = "source_kerberos_enabled")
    private boolean sourceKerberosEnabled = false;

    @Column(name = "source_principal", length = 128)
    private String sourcePrincipal;

    @Column(name = "source_keytab_path", length = 256)
    private String sourceKeytabPath;

    @Column(name = "source_service_name", length = 64)
    private String sourceServiceName = "kafka";

    @Column(name = "target_kerberos_enabled")
    private boolean targetKerberosEnabled = false;

    @Column(name = "target_principal", length = 128)
    private String targetPrincipal;

    @Column(name = "target_keytab_path", length = 256)
    private String targetKeytabPath;

    @Column(name = "target_service_name", length = 64)
    private String targetServiceName = "kafka";

    @Column(name = "source_bootstrap_servers", length = 256)
    private String sourceBootstrapServers;

    @Column(name = "target_bootstrap_servers", length = 256)
    private String targetBootstrapServers;

    @Column(name = "auto_create_topics")
    private boolean autoCreateTopics = true;

    /** С какой позиции новая группа задачи начинает чтение: LATEST или EARLIEST. */
    @Column(name = "start_position", length = 16)
    private String startPosition;

    @Column(name = "topic_provisioning_summary", length = 1024)
    private String topicProvisioningSummary;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "ACTIVE"; // ACTIVE, PAUSED, ERROR, STOPPED

    @Column(name = "messages_replicated")
    private Long messagesReplicated = 0L;

    @Column(name = "bytes_replicated")
    private Long bytesReplicated = 0L;

    @Column(name = "current_lag_ms")
    private Long currentLagMs = 0L;

    @Column(name = "last_error_message", length = 1024)
    private String lastErrorMessage;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public KafkaReplicationJobEntity() {
    }

    @PrePersist
    public void onPrePersist() {
        if (createdAt == null) createdAt = Instant.now();
        if (updatedAt == null) updatedAt = Instant.now();
    }

    @PreUpdate
    public void onPreUpdate() {
        updatedAt = Instant.now();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSourceDc() {
        return sourceDc;
    }

    public void setSourceDc(String sourceDc) {
        this.sourceDc = sourceDc;
    }

    public String getTargetDc() {
        return targetDc;
    }

    public void setTargetDc(String targetDc) {
        this.targetDc = targetDc;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getTopicsJson() {
        return topicsJson;
    }

    public void setTopicsJson(String topicsJson) {
        this.topicsJson = topicsJson;
    }

    public String getConsumerGroupsJson() {
        return consumerGroupsJson;
    }

    public void setConsumerGroupsJson(String consumerGroupsJson) {
        this.consumerGroupsJson = consumerGroupsJson;
    }

    public boolean isSchemaTranscodingEnabled() {
        return schemaTranscodingEnabled;
    }

    public void setSchemaTranscodingEnabled(boolean schemaTranscodingEnabled) {
        this.schemaTranscodingEnabled = schemaTranscodingEnabled;
    }

    public String getSourceSrType() {
        return sourceSrType;
    }

    public void setSourceSrType(String sourceSrType) {
        this.sourceSrType = sourceSrType;
    }

    public String getTargetSrType() {
        return targetSrType;
    }

    public void setTargetSrType(String targetSrType) {
        this.targetSrType = targetSrType;
    }

    public String getSourceSrUrl() {
        return sourceSrUrl;
    }

    public void setSourceSrUrl(String sourceSrUrl) {
        this.sourceSrUrl = sourceSrUrl;
    }

    public String getTargetSrUrl() {
        return targetSrUrl;
    }

    public void setTargetSrUrl(String targetSrUrl) {
        this.targetSrUrl = targetSrUrl;
    }

    public Double getBandwidthLimitMb() {
        return bandwidthLimitMb;
    }

    public void setBandwidthLimitMb(Double bandwidthLimitMb) {
        this.bandwidthLimitMb = bandwidthLimitMb;
    }

    public Integer getOffsetSyncIntervalSec() {
        return offsetSyncIntervalSec;
    }

    public void setOffsetSyncIntervalSec(Integer offsetSyncIntervalSec) {
        this.offsetSyncIntervalSec = offsetSyncIntervalSec;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getMessagesReplicated() {
        return messagesReplicated;
    }

    public void setMessagesReplicated(Long messagesReplicated) {
        this.messagesReplicated = messagesReplicated;
    }

    public Long getBytesReplicated() {
        return bytesReplicated;
    }

    public void setBytesReplicated(Long bytesReplicated) {
        this.bytesReplicated = bytesReplicated;
    }

    public Long getCurrentLagMs() {
        return currentLagMs;
    }

    public void setCurrentLagMs(Long currentLagMs) {
        this.currentLagMs = currentLagMs;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    public void setLastErrorMessage(String lastErrorMessage) {
        this.lastErrorMessage = lastErrorMessage;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public boolean isSourceKerberosEnabled() { return sourceKerberosEnabled; }
    public void setSourceKerberosEnabled(boolean sourceKerberosEnabled) { this.sourceKerberosEnabled = sourceKerberosEnabled; }

    public String getSourcePrincipal() { return sourcePrincipal; }
    public void setSourcePrincipal(String sourcePrincipal) { this.sourcePrincipal = sourcePrincipal; }

    public String getSourceKeytabPath() { return sourceKeytabPath; }
    public void setSourceKeytabPath(String sourceKeytabPath) { this.sourceKeytabPath = sourceKeytabPath; }

    public String getSourceServiceName() { return sourceServiceName; }
    public void setSourceServiceName(String sourceServiceName) { this.sourceServiceName = sourceServiceName; }

    public boolean isTargetKerberosEnabled() { return targetKerberosEnabled; }
    public void setTargetKerberosEnabled(boolean targetKerberosEnabled) { this.targetKerberosEnabled = targetKerberosEnabled; }

    public String getTargetPrincipal() { return targetPrincipal; }
    public void setTargetPrincipal(String targetPrincipal) { this.targetPrincipal = targetPrincipal; }

    public String getTargetKeytabPath() { return targetKeytabPath; }
    public void setTargetKeytabPath(String targetKeytabPath) { this.targetKeytabPath = targetKeytabPath; }

    public String getTargetServiceName() { return targetServiceName; }
    public void setTargetServiceName(String targetServiceName) { this.targetServiceName = targetServiceName; }

    public boolean isSourceSrKerberosEnabled() { return sourceSrKerberosEnabled; }
    public void setSourceSrKerberosEnabled(boolean sourceSrKerberosEnabled) { this.sourceSrKerberosEnabled = sourceSrKerberosEnabled; }

    public boolean isTargetSrKerberosEnabled() { return targetSrKerberosEnabled; }
    public void setTargetSrKerberosEnabled(boolean targetSrKerberosEnabled) { this.targetSrKerberosEnabled = targetSrKerberosEnabled; }

    public String getSourceBootstrapServers() { return sourceBootstrapServers; }
    public void setSourceBootstrapServers(String sourceBootstrapServers) { this.sourceBootstrapServers = sourceBootstrapServers; }

    public String getTargetBootstrapServers() { return targetBootstrapServers; }
    public void setTargetBootstrapServers(String targetBootstrapServers) { this.targetBootstrapServers = targetBootstrapServers; }

    public String getStartPosition() { return startPosition; }
    public void setStartPosition(String startPosition) { this.startPosition = startPosition; }

    public boolean isAutoCreateTopics() { return autoCreateTopics; }
    public void setAutoCreateTopics(boolean autoCreateTopics) { this.autoCreateTopics = autoCreateTopics; }

    public String getTopicProvisioningSummary() { return topicProvisioningSummary; }
    public void setTopicProvisioningSummary(String topicProvisioningSummary) { this.topicProvisioningSummary = topicProvisioningSummary; }

    public Integer getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(Integer consumerThreads) { this.consumerThreads = consumerThreads; }

    public Integer getBatchSize() { return batchSize; }
    public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }

    public String getTargetEndpointsJson() { return targetEndpointsJson; }
    public void setTargetEndpointsJson(String targetEndpointsJson) { this.targetEndpointsJson = targetEndpointsJson; }
}

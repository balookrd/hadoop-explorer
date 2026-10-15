package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import org.apache.hadoop.explorer.replicator.orchestrator.entity.KafkaReplicationJobEntity;

import java.time.Instant;
import java.util.List;

public class KafkaReplicationJobDto {

    private String id;
    private String name;
    private String sourceDc;
    private String targetDc;
    private String direction;
    private List<CreateKafkaJobRequest.TopicSetting> topics;
    private List<String> consumerGroups;
    private boolean schemaTranscodingEnabled;
    private String sourceSrType;
    private String targetSrType;
    private String sourceSrUrl;
    private String targetSrUrl;
    private boolean sourceSrKerberosEnabled;
    private boolean targetSrKerberosEnabled;
    private Double bandwidthLimitMb;
    private Integer offsetSyncIntervalSec;

    // Kerberos
    private boolean sourceKerberosEnabled;
    private String sourcePrincipal;
    private String sourceKeytabPath;
    private String sourceServiceName;

    private boolean targetKerberosEnabled;
    private String targetPrincipal;
    private String targetKeytabPath;
    private String targetServiceName;

    private String sourceBootstrapServers;
    private String targetBootstrapServers;
    private boolean autoCreateTopics = true;
    private String topicProvisioningSummary;

    private String status;
    private Long messagesReplicated;
    private Long bytesReplicated;
    private Long currentLagMs;
    private String lastErrorMessage;
    private Instant createdAt;
    private Instant updatedAt;

    public KafkaReplicationJobDto() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getSourceDc() { return sourceDc; }
    public void setSourceDc(String sourceDc) { this.sourceDc = sourceDc; }

    public String getTargetDc() { return targetDc; }
    public void setTargetDc(String targetDc) { this.targetDc = targetDc; }

    public String getDirection() { return direction; }
    public void setDirection(String direction) { this.direction = direction; }

    public List<CreateKafkaJobRequest.TopicSetting> getTopics() { return topics; }
    public void setTopics(List<CreateKafkaJobRequest.TopicSetting> topics) { this.topics = topics; }

    public List<String> getConsumerGroups() { return consumerGroups; }
    public void setConsumerGroups(List<String> consumerGroups) { this.consumerGroups = consumerGroups; }

    public boolean isSchemaTranscodingEnabled() { return schemaTranscodingEnabled; }
    public void setSchemaTranscodingEnabled(boolean schemaTranscodingEnabled) { this.schemaTranscodingEnabled = schemaTranscodingEnabled; }

    public String getSourceSrType() { return sourceSrType; }
    public void setSourceSrType(String sourceSrType) { this.sourceSrType = sourceSrType; }

    public String getTargetSrType() { return targetSrType; }
    public void setTargetSrType(String targetSrType) { this.targetSrType = targetSrType; }

    public String getSourceSrUrl() { return sourceSrUrl; }
    public void setSourceSrUrl(String sourceSrUrl) { this.sourceSrUrl = sourceSrUrl; }

    public String getTargetSrUrl() { return targetSrUrl; }
    public void setTargetSrUrl(String targetSrUrl) { this.targetSrUrl = targetSrUrl; }

    public Double getBandwidthLimitMb() { return bandwidthLimitMb; }
    public void setBandwidthLimitMb(Double bandwidthLimitMb) { this.bandwidthLimitMb = bandwidthLimitMb; }

    public Integer getOffsetSyncIntervalSec() { return offsetSyncIntervalSec; }
    public void setOffsetSyncIntervalSec(Integer offsetSyncIntervalSec) { this.offsetSyncIntervalSec = offsetSyncIntervalSec; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Long getMessagesReplicated() { return messagesReplicated; }
    public void setMessagesReplicated(Long messagesReplicated) { this.messagesReplicated = messagesReplicated; }

    public Long getBytesReplicated() { return bytesReplicated; }
    public void setBytesReplicated(Long bytesReplicated) { this.bytesReplicated = bytesReplicated; }

    public Long getCurrentLagMs() { return currentLagMs; }
    public void setCurrentLagMs(Long currentLagMs) { this.currentLagMs = currentLagMs; }

    public String getLastErrorMessage() { return lastErrorMessage; }
    public void setLastErrorMessage(String lastErrorMessage) { this.lastErrorMessage = lastErrorMessage; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

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

    public boolean isAutoCreateTopics() { return autoCreateTopics; }
    public void setAutoCreateTopics(boolean autoCreateTopics) { this.autoCreateTopics = autoCreateTopics; }

    public String getTopicProvisioningSummary() { return topicProvisioningSummary; }
    public void setTopicProvisioningSummary(String topicProvisioningSummary) { this.topicProvisioningSummary = topicProvisioningSummary; }
}

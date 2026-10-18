package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import jakarta.validation.constraints.NotBlank;
import java.util.List;

public class CreateKafkaJobRequest {

    @NotBlank(message = "Название задачи репликации не может быть пустым")
    private String name;

    @NotBlank(message = "Исходный ДЦ обязателен")
    private String sourceDc = "DC1";

    @NotBlank(message = "Целевой ДЦ обязателен")
    private String targetDc = "DC2";

    @NotBlank(message = "Направление репликации обязательно")
    private String direction = "BIDIRECTIONAL"; // BIDIRECTIONAL / UNIDIRECTIONAL
    private List<TopicSetting> topics;
    private List<String> consumerGroups;
    private boolean schemaTranscodingEnabled = false;
    private String sourceSrType = "CONFLUENT";
    private String targetSrType = "CONFLUENT";
    private String sourceSrUrl;
    private String targetSrUrl;
    private boolean sourceSrKerberosEnabled = false;
    private boolean targetSrKerberosEnabled = false;
    private Double bandwidthLimitMb = 50.0;
    private Integer offsetSyncIntervalSec = 5;
    private Integer consumerThreads = 8;
    private Integer batchSize = 2000;
    private List<String> targetEndpoints;

    // Kerberos
    private boolean sourceKerberosEnabled = false;
    private String sourcePrincipal;
    private String sourceKeytabPath;
    private String sourceServiceName = "kafka";

    private boolean targetKerberosEnabled = false;
    private String targetPrincipal;
    private String targetKeytabPath;
    private String targetServiceName = "kafka";

    private String sourceBootstrapServers;
    private String targetBootstrapServers;
    private boolean autoCreateTopics = true;

    public static class TopicSetting {
        private String topic;
        private String payloadMode = "AUTO_DETECT"; // PASSTHROUGH, AUTO_DETECT, SCHEMA_REGISTRY

        public TopicSetting() {}

        public TopicSetting(String topic, String payloadMode) {
            this.topic = topic;
            this.payloadMode = payloadMode;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getPayloadMode() {
            return payloadMode;
        }

        public void setPayloadMode(String payloadMode) {
            this.payloadMode = payloadMode;
        }
    }

    public CreateKafkaJobRequest() {}

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

    public List<TopicSetting> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicSetting> topics) {
        this.topics = topics;
    }

    public List<String> getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(List<String> consumerGroups) {
        this.consumerGroups = consumerGroups;
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

    public Integer getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(Integer consumerThreads) { this.consumerThreads = consumerThreads; }

    public Integer getBatchSize() { return batchSize; }
    public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }

    public List<String> getTargetEndpoints() { return targetEndpoints; }
    public void setTargetEndpoints(List<String> targetEndpoints) { this.targetEndpoints = targetEndpoints; }
}

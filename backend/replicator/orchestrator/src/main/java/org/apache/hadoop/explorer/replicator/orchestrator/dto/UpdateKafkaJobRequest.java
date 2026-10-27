package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import java.util.List;

/**
 * Изменение настроек существующей Kafka-задачи. Незаданное поле ({@code null}) не меняется.
 *
 * <p>Направление, ЦОД и учетные данные не редактируются: они определяют консьюмер-группы и
 * маршрут задачи, и их смена — это новая задача.
 */
public class UpdateKafkaJobRequest {
    private String name;
    private List<CreateKafkaJobRequest.TopicSetting> topics;
    private List<String> consumerGroups;
    private Integer consumerThreads;
    private Integer batchSize;
    private Double bandwidthLimitMb;
    private Integer offsetSyncIntervalSec;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public List<CreateKafkaJobRequest.TopicSetting> getTopics() { return topics; }
    public void setTopics(List<CreateKafkaJobRequest.TopicSetting> topics) { this.topics = topics; }
    public List<String> getConsumerGroups() { return consumerGroups; }
    public void setConsumerGroups(List<String> consumerGroups) { this.consumerGroups = consumerGroups; }
    public Integer getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(Integer consumerThreads) { this.consumerThreads = consumerThreads; }
    public Integer getBatchSize() { return batchSize; }
    public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }
    public Double getBandwidthLimitMb() { return bandwidthLimitMb; }
    public void setBandwidthLimitMb(Double bandwidthLimitMb) { this.bandwidthLimitMb = bandwidthLimitMb; }
    public Integer getOffsetSyncIntervalSec() { return offsetSyncIntervalSec; }
    public void setOffsetSyncIntervalSec(Integer offsetSyncIntervalSec) { this.offsetSyncIntervalSec = offsetSyncIntervalSec; }
}

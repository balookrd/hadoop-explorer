package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Запрос регистрации пула пофайловых задач в Оркестраторе после анализа.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchCreateTasksRequest {

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("tasks")
    private List<TaskCreateItem> tasks = new ArrayList<>();

    @JsonProperty("epoch")
    private Long epoch;

    @JsonProperty("last_processed_txid")
    private Long lastProcessedTxid;

    public BatchCreateTasksRequest() {}

    public BatchCreateTasksRequest(List<TaskCreateItem> tasks) {
        this(null, tasks);
    }

    public BatchCreateTasksRequest(String jobId, List<TaskCreateItem> tasks) {
        this(jobId, tasks, null, null);
    }

    public BatchCreateTasksRequest(String jobId, List<TaskCreateItem> tasks, Long epoch, Long lastProcessedTxid) {
        this.jobId = jobId;
        this.tasks = tasks != null ? tasks : new ArrayList<>();
        this.epoch = epoch;
        this.lastProcessedTxid = lastProcessedTxid;
    }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public List<TaskCreateItem> getTasks() { return tasks; }
    public void setTasks(List<TaskCreateItem> tasks) { this.tasks = tasks; }

    public Long getEpoch() { return epoch; }
    public void setEpoch(Long epoch) { this.epoch = epoch; }

    public Long getLastProcessedTxid() { return lastProcessedTxid; }
    public void setLastProcessedTxid(Long lastProcessedTxid) { this.lastProcessedTxid = lastProcessedTxid; }
}

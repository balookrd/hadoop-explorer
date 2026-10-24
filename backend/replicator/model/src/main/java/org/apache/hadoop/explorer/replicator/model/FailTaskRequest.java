package org.apache.hadoop.explorer.replicator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Запрос воркера на фиксацию ошибки при передаче файла.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FailTaskRequest {

    @JsonProperty("task_id")
    private String taskId;

    @JsonProperty("agent_id")
    private String agentId;

    @JsonProperty("error_message")
    private String errorMessage;

    public FailTaskRequest() {}

    public FailTaskRequest(String taskId, String agentId, String errorMessage) {
        this.taskId = taskId;
        this.agentId = agentId;
        this.errorMessage = errorMessage;
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getAgentId() { return agentId; }
    public void setAgentId(String agentId) { this.agentId = agentId; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}

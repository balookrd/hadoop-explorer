package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DirectDeployXmlResponse {
    @JsonProperty("cluster_id")
    private String clusterId;

    @JsonProperty("awx_job_id")
    private Integer awxJobId;

    private String status;
    private String message;

    @JsonProperty("deployed_at")
    private String deployedAt;

    private String stdout;

    public DirectDeployXmlResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final DirectDeployXmlResponse obj = new DirectDeployXmlResponse();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder awxJobId(Integer awxJobId) { obj.awxJobId = awxJobId; return this; }
        public Builder status(String status) { obj.status = status; return this; }
        public Builder message(String message) { obj.message = message; return this; }
        public Builder deployedAt(String deployedAt) { obj.deployedAt = deployedAt; return this; }
        public Builder stdout(String stdout) { obj.stdout = stdout; return this; }

        public DirectDeployXmlResponse build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public Integer getAwxJobId() { return awxJobId; }
    public void setAwxJobId(Integer awxJobId) { this.awxJobId = awxJobId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getDeployedAt() { return deployedAt; }
    public void setDeployedAt(String deployedAt) { this.deployedAt = deployedAt; }
    public String getStdout() { return stdout; }
    public void setStdout(String stdout) { this.stdout = stdout; }
}

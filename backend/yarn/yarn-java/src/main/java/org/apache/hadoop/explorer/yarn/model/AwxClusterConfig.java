package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AwxClusterConfig {
    private boolean enabled = true;

    @JsonProperty("job_template_id")
    private Integer jobTemplateId;

    @JsonProperty("inventory_id")
    private Integer inventoryId;

    public AwxClusterConfig() {}

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public Integer getJobTemplateId() { return jobTemplateId; }
    public void setJobTemplateId(Integer jobTemplateId) { this.jobTemplateId = jobTemplateId; }
    public Integer getInventoryId() { return inventoryId; }
    public void setInventoryId(Integer inventoryId) { this.inventoryId = inventoryId; }
}

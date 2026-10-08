package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GenerateXmlResponse {
    @JsonProperty("cluster_id")
    private String clusterId;

    private String filename;

    @JsonProperty("xml_content")
    private String xmlContent;

    @JsonProperty("applied_by")
    private String appliedBy;

    @JsonProperty("generated_at")
    private String generatedAt;

    private String instructions;

    public GenerateXmlResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final GenerateXmlResponse obj = new GenerateXmlResponse();

        public Builder clusterId(String clusterId) { obj.clusterId = clusterId; return this; }
        public Builder filename(String filename) { obj.filename = filename; return this; }
        public Builder xmlContent(String xmlContent) { obj.xmlContent = xmlContent; return this; }
        public Builder appliedBy(String appliedBy) { obj.appliedBy = appliedBy; return this; }
        public Builder generatedAt(String generatedAt) { obj.generatedAt = generatedAt; return this; }
        public Builder instructions(String instructions) { obj.instructions = instructions; return this; }

        public GenerateXmlResponse build() { return obj; }
    }

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public String getFilename() { return filename; }
    public void setFilename(String filename) { this.filename = filename; }
    public String getXmlContent() { return xmlContent; }
    public void setXmlContent(String xmlContent) { this.xmlContent = xmlContent; }
    public String getAppliedBy() { return appliedBy; }
    public void setAppliedBy(String appliedBy) { this.appliedBy = appliedBy; }
    public String getGeneratedAt() { return generatedAt; }
    public void setGeneratedAt(String generatedAt) { this.generatedAt = generatedAt; }
    public String getInstructions() { return instructions; }
    public void setInstructions(String instructions) { this.instructions = instructions; }
}

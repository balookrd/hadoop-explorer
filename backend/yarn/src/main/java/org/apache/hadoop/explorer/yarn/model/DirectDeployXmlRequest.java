package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class DirectDeployXmlRequest {
    @NotBlank
    @JsonProperty("xml_content")
    private String xmlContent;

    private String comment = "Manual direct XML deployment";

    public DirectDeployXmlRequest() {}

    public DirectDeployXmlRequest(String xmlContent, String comment) {
        this.xmlContent = xmlContent;
        this.comment = comment;
    }

    public String getXmlContent() { return xmlContent; }
    public void setXmlContent(String xmlContent) { this.xmlContent = xmlContent; }
    public String getComment() { return comment; }
    public void setComment(String comment) { this.comment = comment; }
}

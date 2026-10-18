package org.apache.hadoop.explorer.replicator.hms.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record HmsFieldSchemaDto(
        String name,
        String type,
        String comment
) {
    public HmsFieldSchemaDto(String name, String type) {
        this(name, type, null);
    }
}

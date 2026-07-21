package com.company.pipeline.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// GET /connector-plugins 응답 원소.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectorPluginInfo(
        @JsonProperty("class") String className,
        String type,
        String version
) {
}

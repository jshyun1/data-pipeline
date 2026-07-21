package com.company.pipeline.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectorState(
        String state,
        @JsonProperty("worker_id") String workerId
) {
}

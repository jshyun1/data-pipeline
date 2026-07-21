package com.company.pipeline.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectorTaskStatus(
        int id,
        String state,
        @JsonProperty("worker_id") String workerId,
        String trace
) {
}

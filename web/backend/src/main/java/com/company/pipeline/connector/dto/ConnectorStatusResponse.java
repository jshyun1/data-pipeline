package com.company.pipeline.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

// GET /connectors/{name}/status 원본 응답 형태를 그대로 매핑.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectorStatusResponse(
        String name,
        ConnectorState connector,
        List<ConnectorTaskStatus> tasks,
        String type
) {
}

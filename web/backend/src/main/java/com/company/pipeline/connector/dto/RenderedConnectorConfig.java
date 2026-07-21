package com.company.pipeline.connector.dto;

import java.util.Map;

public record RenderedConnectorConfig(
        String connectorName,
        String connectorRole,
        String connectorClass,
        Map<String, Object> config
) {
}

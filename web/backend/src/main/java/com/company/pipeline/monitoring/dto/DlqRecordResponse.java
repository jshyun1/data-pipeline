package com.company.pipeline.monitoring.dto;

import java.time.Instant;

public record DlqRecordResponse(
        Long pipelineId, String pipelineName, String topic, Integer partition, Long offset,
        Instant occurredAt, String connectorName, String errorClass, String errorMessage) {
}

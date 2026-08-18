package com.company.pipeline.monitoring.dto;

import java.time.Instant;

public record DlqRecordDetailResponse(
        Long pipelineId, String topic, Integer partition, Long offset, Instant occurredAt,
        String connectorName, String errorClass, String errorMessage, String key, String payload) {
}

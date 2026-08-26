package com.company.pipeline.pipeline.dto;

import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public record PipelineConnectorSummary(
        Long id,
        String connectorRole,
        String connectorName,
        String connectorClass,
        String status,
        String connectorConfigJson,
        String lastStatusJson
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static PipelineConnectorSummary from(PipelineConnector entity) {
        return new PipelineConnectorSummary(
                entity.getId(),
                entity.getConnectorRole(),
                entity.getConnectorName(),
                entity.getConnectorClass(),
                effectiveStatus(entity),
                entity.getConnectorConfigJson(),
                entity.getLastStatusJson()
        );
    }

    /**
     * 커넥터(envelope) 상태만 보면 task가 죽어도 RUNNING으로 보인다(예: ORA-03113 으로 소스 task 실패).
     * 목록 화면(PipelineRuntimeStatusService.observe)과 동일하게 task 상태를 접어, task 중 하나라도
     * FAILED 면 FAILED 로 표시해 상세 화면과 목록 화면이 같은 판정을 보이게 한다.
     */
    private static String effectiveStatus(PipelineConnector entity) {
        String status = entity.getStatus();
        String json = entity.getLastStatusJson();
        if (json == null || json.isBlank()) {
            return status;
        }
        try {
            ConnectorStatusResponse parsed = MAPPER.readValue(json, ConnectorStatusResponse.class);
            if (parsed.connector() != null && parsed.connector().state() != null) {
                status = parsed.connector().state();
            }
            if (parsed.tasks() != null
                    && parsed.tasks().stream().anyMatch(t -> "FAILED".equalsIgnoreCase(t.state()))) {
                return "FAILED";
            }
        } catch (JsonProcessingException ignored) {
            // 저장된 JSON을 못 읽으면 커넥터 레벨 상태를 그대로 쓴다.
        }
        return status;
    }
}

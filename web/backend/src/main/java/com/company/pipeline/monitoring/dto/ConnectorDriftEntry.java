package com.company.pipeline.monitoring.dto;

/**
 * metadata-db(pipeline_connector)는 존재한다고 알고 있지만 실제 Kafka Connect
 * 레지스트리에는 없는 커넥터 - 컨테이너 재기동 등으로 Kafka Connect 쪽 등록이
 * 리셋됐는데 metadata-db는 그걸 알 방법이 없어서 계속 "RUNNING"이라고 잘못 알고
 * 있는 상태를 뜻한다. 데이터가 조용히 안 들어오는 실제 원인이 된 사례가 있어 추가함.
 */
public record ConnectorDriftEntry(
        Long pipelineId,
        String pipelineName,
        String connectorName,
        String connectorRole
) {
}

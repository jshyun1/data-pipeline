package com.company.pipeline.monitoring.dto;

import java.time.LocalDateTime;

/**
 * 통합 운영 요약에서 사용하는 Kafka 파이프라인의 최신 관측값.
 *
 * throughput은 두 committed offset 스냅샷의 차이를 수집 간격으로 나눈 Sink 소비
 * 추정 처리율이다. 타깃 DB의 실제 커밋 행 수 또는 E2E 지연으로 해석하면 안 된다.
 */
public record RealtimePipelineMetricResponse(
        Long pipelineId,
        LocalDateTime collectedAt,
        Integer partitionCount,
        Long endOffset,
        Long committedOffset,
        Long consumerLag,
        double throughputPerSecond,
        Long estimatedRecoverySeconds,
        LocalDateTime lastProgressAt,
        String collectionStatus
) {
}

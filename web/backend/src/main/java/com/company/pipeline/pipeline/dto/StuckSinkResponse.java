package com.company.pipeline.pipeline.dto;

import java.time.Instant;
import java.util.List;

/**
 * 멈춘 싱크가 «어디서» 멈췄는지. 2026-09-17 실패 정책 전환(errors.tolerance=none)으로 한 건이라도 거부되면 싱크가 멈추는데,
 * 원본을 고칠 수 없는 건(깨진 메시지 등)은 그 한 건을 건너뛰어야 한다 - 그 전에 무엇을 건너뛰는지 보여 주기 위한 정보다.
 *
 * @param stopped       싱크 태스크가 실제로 멈춰 있는지(FAILED). false 면 건너뛸 수 없다
 * @param failureTraces 멈춘 원인(태스크 trace 앞부분)
 * @param positions     아직 읽지 않은 위치. 보통 한 파티션이고, 그 위치의 레코드가 «멈춘 건»이다
 */
public record StuckSinkResponse(
        Long pipelineId,
        String connectorName,
        String consumerGroupId,
        boolean stopped,
        List<String> failureTraces,
        List<StuckPosition> positions) {

    /**
     * @param committedOffset 다음에 읽을 위치 = 멈춘 건의 오프셋
     * @param preview         그 레코드의 내용(개인정보 마스킹 · 길이 제한 적용). 못 읽으면 null
     */
    public record StuckPosition(
            String topic,
            int partition,
            long committedOffset,
            long endOffset,
            long lag,
            Instant recordTimestamp,
            String keyPreview,
            String preview,
            String readError) {
    }
}

package com.company.pipeline.nifi.dto;

import com.company.pipeline.monitoring.NifiProcessorRun;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * ETL 로그 "처리 이력" 한 행.
 *
 * <p>소요시간과 처리량은 저장하지 않고 여기서 계산한다 - 진행 중인 구간도 "지금까지
 * 몇 초 동안 몇 건"으로 보여줘야 하는데, 저장해두면 갱신 시점마다 다시 써야 한다.
 */
public record NifiProcessorRunResponse(
        Long id,
        String processorId,
        String processorName,
        String processorType,
        String groupId,
        String groupName,
        /** 적재 대상 schema.table. 스크립트 기반 적재는 알 수 없어 null. */
        String targetTable,
        LocalDateTime startedAt,
        /** 진행 중이면 null. */
        LocalDateTime endedAt,
        Long durationSeconds,
        Long insertedCount,
        /** 초당 적재 행수. 소요가 0초면(한 폴링 주기 안에 끝남) 계산 불가라 null. */
        Long rowsPerSecond,
        String status
) {

    public static NifiProcessorRunResponse from(NifiProcessorRun run) {
        LocalDateTime end = run.getEndedAt() != null ? run.getEndedAt() : run.getLastSeenAt();
        long seconds = Math.max(0, Duration.between(run.getStartedAt(), end).getSeconds());
        Long rowsPerSecond = seconds > 0 ? run.getInsertedCount() / seconds : null;
        return new NifiProcessorRunResponse(
                run.getId(),
                run.getProcessorId(),
                run.getProcessorName(),
                run.getProcessorType(),
                run.getGroupId(),
                run.getGroupName(),
                run.getTargetTable(),
                run.getStartedAt(),
                run.getEndedAt(),
                seconds,
                run.getInsertedCount(),
                rowsPerSecond,
                run.getStatus()
        );
    }
}

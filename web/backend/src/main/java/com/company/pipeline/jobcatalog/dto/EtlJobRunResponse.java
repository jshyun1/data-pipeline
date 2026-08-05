package com.company.pipeline.jobcatalog.dto;

import com.company.pipeline.jobcatalog.EtlJobRun;
import java.time.Duration;
import java.time.LocalDateTime;

/** 잡 실행 1회. 소요 시간은 화면에서 다시 계산하지 않도록 초 단위로 함께 준다. */
public record EtlJobRunResponse(
        Long id,
        Long jobId,
        String status,
        String triggerSource,
        String airflowDagRunId,
        LocalDateTime startedAt,
        LocalDateTime endedAt,
        Long durationSeconds,
        int stepRunCount,
        long totalInserted,
        int failedStepCount) {

    public static EtlJobRunResponse from(EtlJobRun run) {
        Long duration = run.getEndedAt() == null
                ? null : Duration.between(run.getStartedAt(), run.getEndedAt()).toSeconds();
        return new EtlJobRunResponse(run.getId(), run.getJobId(), run.getStatus(), run.getTriggerSource(),
                run.getAirflowDagRunId(), run.getStartedAt(), run.getEndedAt(), duration,
                run.getStepRunCount(), run.getTotalInserted(), run.getFailedStepCount());
    }
}

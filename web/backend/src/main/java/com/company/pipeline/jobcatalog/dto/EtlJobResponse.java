package com.company.pipeline.jobcatalog.dto;

import com.company.pipeline.jobcatalog.EtlJob;
import java.time.LocalDateTime;

/** 잡 목록 1행. */
public record EtlJobResponse(
        Long id,
        String nifiPgId,
        String jobName,
        String engine,
        String comments,
        String parameterContextName,
        String airflowDagId,
        int stepCount,
        int runningCount,
        int stoppedCount,
        int invalidCount,
        LocalDateTime firstSeenAt,
        LocalDateTime lastSyncedAt) {

    public static EtlJobResponse from(EtlJob job) {
        return new EtlJobResponse(
                job.getId(),
                job.getNifiPgId(),
                job.getJobName(),
                job.getEngine(),
                job.getComments(),
                job.getParameterContextName(),
                job.getAirflowDagId(),
                job.getStepCount(),
                job.getRunningCount(),
                job.getStoppedCount(),
                job.getInvalidCount(),
                job.getFirstSeenAt(),
                job.getLastSyncedAt());
    }
}

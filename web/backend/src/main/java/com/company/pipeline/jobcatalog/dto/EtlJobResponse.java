package com.company.pipeline.jobcatalog.dto;

import com.company.pipeline.jobcatalog.EtlJob;
import java.time.LocalDateTime;

/** 잡 목록 1행. */
public record EtlJobResponse(
        Long id,
        String nifiPgId,
        /** 소속 그룹 id. 체인을 자식 PG로 나누면 이름이 겹치므로(DW/DZ 아래 COM001M 등)
         *  화면이 어느 그룹 것인지 구분할 근거가 필요하다. */
        String parentPgId,
        /** 소속 그룹 이름. 부모 그룹 자체는 프로세서가 없어 잡 목록에 안 나오므로
         *  여기서 같이 내려줘야 화면이 'DW / COM001M'처럼 구분해 보여줄 수 있다. */
        String parentGroupName,
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
        return from(job, null);
    }

    public static EtlJobResponse from(EtlJob job, String parentGroupName) {
        return new EtlJobResponse(
                job.getId(),
                job.getNifiPgId(),
                job.getParentPgId(),
                parentGroupName,
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

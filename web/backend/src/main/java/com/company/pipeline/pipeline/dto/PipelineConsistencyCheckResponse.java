package com.company.pipeline.pipeline.dto;

import com.company.pipeline.pipeline.PipelineConsistencyCheck;
import java.time.LocalDateTime;

public record PipelineConsistencyCheckResponse(
        Long id, Long pipelineId, String checkMode, Long sourceCount, Long targetCount,
        Long difference, String result, String message, LocalDateTime checkedAt) {
    public static PipelineConsistencyCheckResponse from(PipelineConsistencyCheck check) {
        Long difference = check.getSourceCount() == null || check.getTargetCount() == null
                ? null : check.getTargetCount() - check.getSourceCount();
        return new PipelineConsistencyCheckResponse(check.getId(), check.getPipelineId(), check.getCheckMode(),
                check.getSourceCount(), check.getTargetCount(), difference, check.getResult(), check.getMessage(),
                check.getCheckedAt());
    }
}

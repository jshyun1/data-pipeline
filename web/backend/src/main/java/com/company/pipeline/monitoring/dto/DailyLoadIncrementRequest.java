package com.company.pipeline.monitoring.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record DailyLoadIncrementRequest(
        @NotBlank String pipelineSource,
        @NotBlank String pipelineKey,
        @NotBlank String taskKey,
        String pipelineLabel,
        @Positive long count
) {
}

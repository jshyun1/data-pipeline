package com.company.pipeline.jobcatalog.dto;

import java.time.LocalDateTime;

public record CalciteSqlValidationResponse(
        boolean success,
        LocalDateTime testedAt,
        long latencyMs,
        String message
) {
}

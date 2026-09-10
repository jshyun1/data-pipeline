package com.company.pipeline.connection.dto;

import java.time.LocalDateTime;

public record SqlValidationResponse(
        boolean success,
        LocalDateTime testedAt,
        long latencyMs,
        String message
) {
}

package com.company.pipeline.connection.dto;

import java.time.LocalDateTime;

public record ConnectionTestResponse(
        boolean success,
        LocalDateTime testedAt,
        long latencyMs,
        String message
) {
}

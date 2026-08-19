package com.company.pipeline.connection.dto;

import java.util.List;

public record ConnectionUsageResponse(
        Long connectionId,
        long cdcSourceCount,
        long cdcTargetCount,
        long etlJobCount,
        boolean deletable,
        List<ConnectionReferenceResponse> references
) {
}

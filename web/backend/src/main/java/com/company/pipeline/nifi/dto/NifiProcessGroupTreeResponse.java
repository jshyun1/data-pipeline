package com.company.pipeline.nifi.dto;

import java.util.List;

public record NifiProcessGroupTreeResponse(
        String id,
        String name,
        int processorCount,
        int runningCount,
        int stoppedCount,
        int invalidCount,
        int disabledCount,
        List<NifiProcessGroupTreeResponse> children
) {
}

package com.company.pipeline.nifi.dto;

import java.util.List;

public record NifiProcessGroupTreeResponse(
        String id,
        String name,
        String groupType,
        String jobStatus,
        int processorCount,
        int runningCount,
        int stoppedCount,
        int invalidCount,
        int disabledCount,
        int activeThreadCount,
        int flowFilesQueued,
        int sourceInputCount,
        int terminalInputCount,
        List<NifiProcessGroupTreeResponse> children
) {
}

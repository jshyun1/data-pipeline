package com.company.pipeline.monitoring.dto;

import com.company.pipeline.pipeline.dto.PipelineCommandHistoryResponse;
import java.util.List;

public record DashboardSummaryResponse(
        long totalPipelines,
        long runningCount,
        long failedCount,
        long pausedCount,
        boolean kafkaConnectHealthy,
        boolean kafkaBrokerHealthy,
        List<PipelineCommandHistoryResponse> recentErrors,
        List<PipelineCommandHistoryResponse> recentDeployments
) {
}

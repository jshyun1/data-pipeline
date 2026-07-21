import type { PipelineCommandHistoryResponse } from "./pipeline";

// web/backend의 DashboardSummaryResponse와 1:1.
export interface DashboardSummaryResponse {
  totalPipelines: number;
  runningCount: number;
  failedCount: number;
  pausedCount: number;
  kafkaConnectHealthy: boolean;
  kafkaBrokerHealthy: boolean;
  recentErrors: PipelineCommandHistoryResponse[];
  recentDeployments: PipelineCommandHistoryResponse[];
}

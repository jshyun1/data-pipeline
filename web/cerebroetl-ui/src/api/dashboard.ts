import { apiClient, unwrap, type ApiResponse } from "./client";

export interface DailyLoadPointResponse {
  date: string;
  count: number;
}

export interface KeyedLoadPointResponse {
  key: string;
  label: string;
  count: number;
}

export interface DailyLoadSummaryResponse {
  daily: DailyLoadPointResponse[];
  topPipelines: KeyedLoadPointResponse[];
  topTasks: KeyedLoadPointResponse[];
}

export async function getDailyLoadSummary(
  from: string,
  to: string,
  source?: "NIFI" | "KAFKA",
): Promise<DailyLoadSummaryResponse> {
  const res = await apiClient.get<ApiResponse<DailyLoadSummaryResponse>>("/metrics/daily-load/summary", {
    params: { from, to, source },
  });
  return unwrap(res.data);
}

export interface ConnectorDriftEntry {
  pipelineId: number;
  pipelineName: string;
  connectorName: string;
  connectorRole: string;
}

export interface PipelineDashboardSummary {
  kafkaConnectHealthy: boolean;
  connectorDrift: ConnectorDriftEntry[];
  todayCommandTotalCount: number;
  todayCommandSuccessCount: number;
  todayCommandFailedCount: number;
}

// metadata-db는 등록돼 있다고 알고 있는데 실제 Kafka Connect 레지스트리엔 없는 커넥터를
// 조회한다(컨테이너 재기동 등으로 조용히 사라진 경우) - 데이터가 안 들어와서 사람이
// 알아채기 전에 화면에서 먼저 알려주기 위함.
export async function getPipelineDashboardSummary(): Promise<PipelineDashboardSummary> {
  const res = await apiClient.get<ApiResponse<PipelineDashboardSummary>>("/dashboard/summary");
  return unwrap(res.data);
}

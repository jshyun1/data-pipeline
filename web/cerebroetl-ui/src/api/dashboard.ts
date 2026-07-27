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

export interface RealtimePipelineMetricResponse {
  pipelineId: number;
  collectedAt: string | null;
  partitionCount: number | null;
  endOffset: number | null;
  committedOffset: number | null;
  consumerLag: number | null;
  throughputPerSecond: number;
  estimatedRecoverySeconds: number | null;
  lastProgressAt: string | null;
  collectionStatus: "COLLECTED" | "NO_DATA";
}

// committed offset 기반 처리율/미처리량이다. 타깃 DB의 실제 커밋 행 수나 E2E 지연과
// 혼동하지 않도록 화면에서도 "Sink 소비 추정"으로 표기한다.
export async function getRealtimePipelineMetrics(): Promise<RealtimePipelineMetricResponse[]> {
  const res = await apiClient.get<ApiResponse<RealtimePipelineMetricResponse[]>>("/metrics/daily-load/realtime");
  return unwrap(res.data);
}

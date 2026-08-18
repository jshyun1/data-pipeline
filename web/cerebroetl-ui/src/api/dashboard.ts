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

export interface HourlyLoadPointResponse {
  /** YYYY-MM-DD */
  date: string;
  /** 0~23 */
  hour: number;
  count: number;
}

export interface HourlyLoadSummaryResponse {
  hourly: HourlyLoadPointResponse[];
}

// 날짜 × 시간대 조합의 적재 건수. 시각이 남아 있는 원본 관측 테이블(NiFi=카운터 증가분
// 로그, CDC=offset 스냅샷)에서 계산하므로, 그 테이블이 생기기 전 기간은 값이 비어 있다.
// 건수가 0인 조합은 응답에 없으므로 화면에서 빈 칸을 채운다.
export async function getHourlyLoadSummary(
  from: string,
  to: string,
  source?: "NIFI" | "KAFKA",
): Promise<HourlyLoadSummaryResponse> {
  const res = await apiClient.get<ApiResponse<HourlyLoadSummaryResponse>>("/metrics/daily-load/hourly", {
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

/* -------------------------------------------------------------------------
 * 차트 보강 — 원본 문서 5-5 (#4 실패·지연 추이, #6 Top 5 3종)
 * ---------------------------------------------------------------------- */

/** 화면 상단 프리셋 칩과 같은 값. 버킷 입도는 서버가 프리셋에서 유도한다. */
export type TrendPreset = "1h" | "24h" | "7d" | "30d";

export interface TrendPoint {
  at: string;
  count: number;
}

export interface TrendsResponse {
  preset: string;
  bucket: string;
  failures: TrendPoint[];
  lag: TrendPoint[];
}

export async function getDashboardTrends(preset: TrendPreset): Promise<TrendsResponse> {
  const res = await apiClient.get<ApiResponse<TrendsResponse>>("/dashboard/trends", { params: { preset } });
  return unwrap(res.data);
}

export type Top5Metric = "count" | "duration" | "failure";

export interface Top5Item {
  label: string;
  value: number;
  runs: number;
  /** 드릴다운용. null 이면 이동할 화면을 특정할 수 없어 클릭을 막는다. */
  job_id: number | null;
}

export interface Top5Response {
  metric: Top5Metric;
  preset: string;
  items: Top5Item[];
}

export async function getDashboardTop5(metric: Top5Metric, preset: TrendPreset): Promise<Top5Response> {
  const res = await apiClient.get<ApiResponse<Top5Response>>("/dashboard/top5", { params: { metric, preset } });
  return unwrap(res.data);
}

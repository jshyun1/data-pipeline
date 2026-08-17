import { apiClient, unwrap, type ApiResponse } from "./client";

// KPI 롤업(백엔드 /api/dashboard/kpi). summary 는 Map 직렬화라 snake_case,
// timeline 은 record 직렬화라 camelCase 인 점에 주의(백엔드 KpiController 참고).
export interface KpiSummaryRow {
  pipeline_source: string;
  loaded: number;
  obs: number;
  series: number;
}

export interface KpiTimelineBucket {
  source: string;
  bucketStart: string;
  loadedCount: number;
  observationCount: number;
}

export interface KpiTimelineResponse {
  preset: string;
  granularity: string;
  since: string;
  bucketAdjusted: boolean;
  buckets: KpiTimelineBucket[];
}

export type KpiPreset = "1h" | "24h" | "7d" | "30d";

export async function getKpiSummary(preset: KpiPreset): Promise<KpiSummaryRow[]> {
  const res = await apiClient.get<ApiResponse<KpiSummaryRow[]>>("/dashboard/kpi/summary", { params: { preset } });
  return unwrap(res.data);
}

export async function getKpiTimeline(preset: KpiPreset): Promise<KpiTimelineResponse> {
  const res = await apiClient.get<ApiResponse<KpiTimelineResponse>>("/dashboard/kpi/timeline", { params: { preset } });
  return unwrap(res.data);
}

import { apiClient, unwrap, type ApiResponse } from "./client";

// 자가진단(백엔드 /api/admin/self-check). record 직렬화라 camelCase.
export interface CollectorStatus {
  key: string;
  label: string;
  metricSource: string;
  status: string; // UP | PENDING | DOWN | LATE 등
  lastBeatAt: string | null;
  ageSeconds: number | null;
  expectedIntervalSeconds: number;
  observedIntervalSeconds: number | null;
  lastResult: string | null;
  lastError: string | null;
}

export interface SelfCheckResponse {
  checkedAt: string;
  collectors: CollectorStatus[];
  clockSkewSeconds: number;
}

export async function getSelfCheck(): Promise<SelfCheckResponse> {
  const res = await apiClient.get<ApiResponse<SelfCheckResponse>>("/admin/self-check");
  return unwrap(res.data);
}

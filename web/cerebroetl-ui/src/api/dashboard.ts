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

export async function getDailyLoadSummary(from: string, to: string): Promise<DailyLoadSummaryResponse> {
  const res = await apiClient.get<ApiResponse<DailyLoadSummaryResponse>>("/metrics/daily-load/summary", {
    params: { from, to },
  });
  return unwrap(res.data);
}

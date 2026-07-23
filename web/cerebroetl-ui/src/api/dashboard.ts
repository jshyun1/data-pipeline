import { apiClient, unwrap, type ApiResponse } from "./client";
import type { DashboardSummaryResponse } from "../types/dashboard";

export async function getDashboardSummary(): Promise<DashboardSummaryResponse> {
  const res = await apiClient.get<ApiResponse<DashboardSummaryResponse>>("/dashboard/summary");
  return unwrap(res.data);
}

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

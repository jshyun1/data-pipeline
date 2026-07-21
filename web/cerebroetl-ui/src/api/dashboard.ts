import { apiClient, unwrap, type ApiResponse } from "./client";
import type { DashboardSummaryResponse } from "../types/dashboard";

export async function getDashboardSummary(): Promise<DashboardSummaryResponse> {
  const res = await apiClient.get<ApiResponse<DashboardSummaryResponse>>("/dashboard/summary");
  return unwrap(res.data);
}

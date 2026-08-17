import { apiClient, unwrap, type ApiResponse } from "./client";

// 조치 대기열(백엔드 /api/dashboard/queue). items 는 백엔드가 Map 으로 내려 snake_case 다.
export interface QueueItem {
  id: number;
  rule_type_code: string;
  severity: "CRITICAL" | "WARNING" | "INFO";
  state: string;
  kpi_axis: string;
  target_key: string;
  target_label: string | null;
  component_code: string | null;
  summary: string;
  observed_value: number | null;
  threshold_value: number | null;
  duration_seconds: number | null;
  deep_link: string | null;
  acked: boolean;
  notify_count: number;
}

export interface QueueCounts {
  critical: number;
  warning: number;
  info: number;
  unknown: number;
  acked: number;
  snoozed: number;
  suppressed: number;
}

export interface QueueResponse {
  counts: QueueCounts;
  totalOpen: number;
  truncated: boolean;
  items: QueueItem[];
}

export async function getAlertQueue(limit = 20): Promise<QueueResponse> {
  const res = await apiClient.get<ApiResponse<QueueResponse>>("/dashboard/queue", { params: { limit } });
  return unwrap(res.data);
}

export async function ackAlert(id: number, comment?: string): Promise<void> {
  await apiClient.post(`/alerts/${id}/ack`, { comment });
}

export async function snoozeAlert(id: number, minutes = 60): Promise<void> {
  await apiClient.post(`/alerts/${id}/snooze`, { minutes });
}

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

/* -------------------------------------------------------------------------
 * 알림 이력 — 원본 5-7 / PDF 8쪽
 * 대기열은 "지금 열려 있는 것"만 본다. 종료된 알림과 확인 이력은 여기서 본다.
 * ---------------------------------------------------------------------- */

export type HistoryFilter = "all" | "unacked" | "acked";

export interface HistoryItem {
  id: number;
  rule_type_code: string;
  severity: "CRITICAL" | "WARNING" | "INFO";
  state: string;
  target_key: string;
  target_label: string | null;
  summary: string;
  observed_value: number | null;
  threshold_value: number | null;
  deep_link: string | null;
  ack_by: string | null;
  ack_at: string | null;
  ack_comment: string | null;
  acked: boolean;
  closed: boolean;
  resolve_reason: string | null;
  started_at: string | null;
  condition_since: string | null;
  last_transition_at: string | null;
  resolved_at: string | null;
}

export type SeverityFilter = "ALL" | "CRITICAL" | "WARNING" | "INFO";

export interface HistoryQuery {
  filter: HistoryFilter;
  severity?: SeverityFilter;
  q?: string;
  /** 기간: from/to(ISO)가 있으면 그 범위, 없으면 최근 days일. */
  days?: number;
  from?: string;
  to?: string;
  page: number;
  pageSize: number;
}

export interface HistoryResponse {
  filter: string;
  severity: string;
  q: string;
  days: number;
  page: number;
  pageSize: number;
  total: number;
  counts: { total: number; unacked: number; acked: number };
  items: HistoryItem[];
}

export async function getAlertHistory(query: HistoryQuery): Promise<HistoryResponse> {
  const res = await apiClient.get<ApiResponse<HistoryResponse>>("/dashboard/queue/history", {
    params: {
      filter: query.filter,
      severity: query.severity ?? "ALL",
      q: query.q ?? "",
      ...(query.from && query.to ? { from: query.from, to: query.to } : { days: query.days ?? 7 }),
      page: query.page,
      pageSize: query.pageSize,
    },
  });
  return unwrap(res.data);
}

export interface AlertEvent {
  event_type: string;
  from_state: string | null;
  to_state: string | null;
  actor: string | null;
  occurred_at: string;
}

export async function getAlertEvents(id: number): Promise<AlertEvent[]> {
  const res = await apiClient.get<ApiResponse<AlertEvent[]>>(`/dashboard/queue/history/${id}/events`);
  return unwrap(res.data);
}

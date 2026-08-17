import { apiClient, unwrap, type ApiResponse } from "./client";

// 백엔드가 Map/jsonb 로 내리는 필드가 있어 느슨한 타입으로 둔다.
export interface AlertRule {
  id: number;
  name: string;
  type_label: string;
  category: string;
  enabled: boolean;
  severity: string;
  params_json: unknown;
  for_seconds: number;
  clear_seconds: number;
  mandatory: boolean;
  last_eval_error: string | null;
}

export interface ChannelConfig {
  channel_type: "IN_APP" | "EMAIL" | "SMS";
  enabled: boolean;
  secret_set: boolean;
  rate_critical_per_min: number;
  rate_other_per_min: number;
  circuit_state: string;
  last_failure_reason: string | null;
}

export interface Recipient {
  id: number;
  display_name: string;
  email: string | null;
  phone_masked: string | null;
  enabled: boolean;
}

export async function getAlertRules(): Promise<AlertRule[]> {
  const res = await apiClient.get<ApiResponse<AlertRule[]>>("/admin/alert-rules");
  return unwrap(res.data);
}

export async function updateAlertRule(
  id: number,
  body: { enabled?: boolean; severity?: string; paramsJson?: string; forSeconds?: number; clearSeconds?: number },
): Promise<void> {
  await apiClient.put(`/admin/alert-rules/${id}`, body);
}

export async function getChannels(): Promise<ChannelConfig[]> {
  const res = await apiClient.get<ApiResponse<ChannelConfig[]>>("/admin/notification/channels");
  return unwrap(res.data);
}

export async function updateChannel(
  type: string,
  body: { enabled?: boolean; configJson?: string; rateCriticalPerMin?: number; rateOtherPerMin?: number },
): Promise<void> {
  await apiClient.put(`/admin/notification/channels/${type}`, body);
}

export async function testChannel(type: string): Promise<void> {
  await apiClient.post(`/notifications/channels/${type}/test`);
}

export async function getRecipients(): Promise<Recipient[]> {
  const res = await apiClient.get<ApiResponse<Recipient[]>>("/admin/notification/recipients");
  return unwrap(res.data);
}

export async function createRecipient(body: {
  displayName: string;
  email?: string;
  phone?: string;
}): Promise<void> {
  await apiClient.post("/admin/notification/recipients", body);
}

export async function deleteRecipient(id: number): Promise<void> {
  await apiClient.delete(`/admin/notification/recipients/${id}`);
}

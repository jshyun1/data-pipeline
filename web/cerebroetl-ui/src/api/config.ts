import { apiClient, unwrap, type ApiResponse } from "./client";

// 백엔드가 Map/jsonb 로 내리는 필드가 있어 느슨한 타입으로 둔다.
export interface AlertRule {
  id: number;
  rule_type_code: string;
  name: string;
  type_label: string;
  category: string;
  enabled: boolean;
  severity: string;
  params_json: unknown;
  scope_json: string | null;
  renotify_seconds: number;
  for_seconds: number;
  clear_seconds: number;
  mandatory: boolean;
  last_eval_error: string | null;
  // 일별 점검 스케줄. off(기본)면 20초 평가 루프에서 상시 판정한다.
  schedule_enabled: boolean;
  schedule_time: string | null;        // "HH:mm:ss"
  schedule_last_fired_on: string | null;
  // 누가·언제 (목록 컬럼). 서버가 timestamptz 를 ISO 문자열로 내려준다.
  created_by: string | null;
  updated_by: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface ScopeTarget {
  id: number;
  name: string;
}

/** 감시 범위 선택 트리. id 가 null 인 노드는 «묶음»이라 고를 수 없다. */
export interface ScopeTreeNode {
  /** 잎(감시 단위: 적재 테이블 또는 잡). 그룹이면 null. */
  id: number | null;
  /** ETL 그룹 노드의 NiFi 프로세스 그룹 id. 이걸로 «그룹째» 감시할 수 있다. */
  groupPgId?: string | null;
  /** CDC 그룹 노드의 소스 연결정보 id. 그 원천의 파이프라인 전부를 감시한다. */
  groupConnId?: number | null;
  name: string;
  children: ScopeTreeNode[];
}

export async function getScopeTree(category: string): Promise<ScopeTreeNode[]> {
  const res = await apiClient.get<ApiResponse<ScopeTreeNode[]>>("/admin/alert-scope-tree", {
    params: { category },
  });
  return unwrap(res.data);
}

/** 규칙 평가 이력 한 줄. result: FAILED | NO_SIGNAL | FIRED | RESOLVED */
export interface RuleEvalLog {
  id: number;
  occurred_at: string;
  result: string;
  matched_count: number | null;
  duration_ms: number | null;
  message: string | null;
}

export async function getRuleEvalLogs(ruleId: number): Promise<RuleEvalLog[]> {
  const res = await apiClient.get<ApiResponse<RuleEvalLog[]>>(
    `/admin/alert-rules/${ruleId}/eval-logs`,
  );
  return unwrap(res.data);
}

export async function getScopeTargets(category: string): Promise<ScopeTarget[]> {
  const res = await apiClient.get<ApiResponse<ScopeTarget[]>>("/admin/alert-scope-targets", {
    params: { category },
  });
  return unwrap(res.data);
}

export type NotificationChannel = "IN_APP" | "EMAIL" | "SMS" | "KAKAO";

export interface ChannelConfig {
  channel_type: NotificationChannel;
  enabled: boolean;
  /**
   * 서버에 릴레이(SMTP 호스트 / SMS 게이트웨이)가 설정돼 있는지. enabled 는 화면 토글이고
   * 이건 서버 환경변수라 둘이 어긋날 수 있다 - «켰는데 안 온다»를 화면에서 판별하려고 받는다.
   */
  relay_configured: boolean;
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
  /** 마스킹하지 않는다 — 가려진 번호로는 맞는지 확인할 수도, 고칠 수도 없다. */
  phone: string | null;
  enabled: boolean;
  /**
   * 이 사람이 어떤 채널로 받는지. 등록만 해서는 알림이 나가지 않고 이 구독이 있어야 발송된다.
   * 서버가 «켜진 것만» json_agg 로 내려주므로, 없으면 null 이다.
   */
  subscriptions: Array<{ channel: string; minSeverity: string }> | null;
}

/** 수신자의 채널 구독을 켜고 끈다. 연락처가 없는 채널을 켜면 서버가 이유를 담아 거절한다. */
export async function setSubscription(
  recipientId: number,
  channelType: Exclude<NotificationChannel, "IN_APP">,
  enabled: boolean,
): Promise<void> {
  await apiClient.put(`/admin/notification/recipients/${recipientId}/subscriptions`, {
    channelType,
    enabled,
  });
}

/** 조건 입력 폼을 그리기 위한 유형별 파라미터 정의. 서버가 단일 원천이다. */
export interface RuleParamSpec {
  key: string;
  label: string;
  unit: string;
  type: "INT";
  min: number;
  max: number;
  defaultValue: number;
}

export interface AlertRuleType {
  code: string;
  label: string;
  category: string;
  mandatory: boolean;
  default_severity: string;
  min_severity: string;
  description: string | null;
  paramSpec: RuleParamSpec[];
}

export async function getAlertRuleTypes(): Promise<AlertRuleType[]> {
  const res = await apiClient.get<ApiResponse<AlertRuleType[]>>("/admin/alert-rule-types");
  return unwrap(res.data);
}

export async function createAlertRule(body: {
  ruleTypeCode: string;
  name: string;
  severity?: string;
  paramsJson?: string;
  forSeconds?: number;
  clearSeconds?: number;
  scopeJson?: string;
  renotifySeconds?: number;
  scheduleEnabled?: boolean;
  scheduleTime?: string | null;        // "HH:mm"
}): Promise<void> {
  await apiClient.post("/admin/alert-rules", body);
}

export async function deleteAlertRule(id: number): Promise<void> {
  await apiClient.delete(`/admin/alert-rules/${id}`);
}

export async function getAlertRules(): Promise<AlertRule[]> {
  const res = await apiClient.get<ApiResponse<AlertRule[]>>("/admin/alert-rules");
  return unwrap(res.data);
}

export async function updateAlertRule(
  id: number,
  body: {
    enabled?: boolean;
    severity?: string;
    paramsJson?: string;
    forSeconds?: number;
    clearSeconds?: number;
    name?: string;
    scopeJson?: string;
    renotifySeconds?: number;
    scheduleEnabled?: boolean;
    scheduleTime?: string | null;
  },
): Promise<void> {
  await apiClient.put(`/admin/alert-rules/${id}`, body);
}

// 규칙별 수신자. 목록을 한 번도 손대지 않은 규칙은 종전대로 전원에게 발송된다.
export interface RuleRecipient {
  recipient_id: number;
  display_name: string;
  email: string | null;
  phone: string | null;
  recipient_enabled: boolean;   // 수신자 자체의 사용여부
  enabled: boolean;             // 이 규칙을 받는지
}

export async function getRuleRecipients(ruleId: number): Promise<RuleRecipient[]> {
  const res = await apiClient.get<ApiResponse<RuleRecipient[]>>(`/admin/alert-rules/${ruleId}/recipients`);
  return unwrap(res.data);
}

export async function replaceRuleRecipients(
  ruleId: number,
  recipients: { recipientId: number; enabled: boolean }[],
): Promise<void> {
  await apiClient.put(`/admin/alert-rules/${ruleId}/recipients`, recipients);
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

/**
 * 채널 설정(메일 서버 · 문자 게이트웨이 · 카카오 대행사 API). 어느 망에 들어가든 화면에서 넣으면
 * 다음 발송부터 그 값으로 나간다. 비밀값은 내려오지 않고 저장 여부 · 끝자리 힌트만 온다.
 */
export interface ChannelSettings {
  channelType: string;
  /** 채널마다 항목이 다르다(메일: host·port…, 문자·카카오: url·headers…). */
  values: Record<string, string>;
  secretSet: boolean;
  secretHint: string | null;
  /** 화면에서 저장한 적이 없어 서버 환경변수 값으로 돌고 있다는 뜻. */
  fromEnvOnly: boolean;
}

export async function getChannelSettings(type: string): Promise<ChannelSettings> {
  const res = await apiClient.get<ApiResponse<ChannelSettings>>(`/admin/notification/channels/${type}/settings`);
  return unwrap(res.data);
}

/** 비밀값(secret)을 비워 보내면 저장된 값을 그대로 둔다 - GPT 제공자 등록과 같은 규칙이다. */
export async function saveChannelSettings(
  type: string,
  body: { values: Record<string, string>; secret?: string },
): Promise<ChannelSettings> {
  const res = await apiClient.put<ApiResponse<ChannelSettings>>(
    `/admin/notification/channels/${type}/settings`, body);
  return unwrap(res.data);
}

export async function clearChannelSecret(type: string): Promise<ChannelSettings> {
  const res = await apiClient.delete<ApiResponse<ChannelSettings>>(
    `/admin/notification/channels/${type}/settings/secret`);
  return unwrap(res.data);
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

/** 부분 수정. 빈 문자열은 "지운다"는 뜻으로 서버가 NULL 처리한다. */
export async function updateRecipient(
  id: number,
  body: { displayName?: string; email?: string; phone?: string; enabled?: boolean },
): Promise<void> {
  await apiClient.put(`/admin/notification/recipients/${id}`, body);
}

export async function deleteRecipient(id: number): Promise<void> {
  await apiClient.delete(`/admin/notification/recipients/${id}`);
}

/** 이 대상(job/파이프라인)을 감시하고 있는 알림 규칙. 실행 현황 속성창이 읽는다. */
export interface WatchingAlertRule {
  id: number;
  rule_type_code: string;
  type_label: string;
  category: string;
  name: string;
  enabled: boolean;
  severity: string;
  scope_json: string | null;
  schedule_enabled: boolean;
  schedule_time: string | null;
  last_evaluated_at: string | null;
  last_eval_error: string | null;
}

export async function getAlertRulesWatching(
  target: "CDC" | "ETL",
  ids: Array<number | string>,
): Promise<WatchingAlertRule[]> {
  if (!ids.length) return [];
  const res = await apiClient.get<ApiResponse<WatchingAlertRule[]>>("/admin/alert-rules/watching", {
    params: { target, ids: ids.join(",") },
  });
  return unwrap(res.data);
}

import { apiClient, unwrap, type ApiResponse } from "./client";
import { clearSession, getToken } from "../auth/session";

// 백엔드 com.company.pipeline.llm 과 맞춘다. 설계: docs/2026-09-13-gpt-settings-design.md
export type LlmDeployment = "EXTERNAL" | "INTERNAL";
export type LlmProvider = "ANTHROPIC" | "OPENAI" | "GEMINI" | "OPENAI_COMPATIBLE";
export type LlmBillingType = "FREE" | "PAID";

export const PROVIDER_LABEL: Record<LlmProvider, string> = {
  ANTHROPIC: "Claude",
  OPENAI: "ChatGPT",
  GEMINI: "Gemini",
  OPENAI_COMPATIBLE: "OpenAI 호환",
};

export const PROVIDERS_BY_DEPLOYMENT: Record<LlmDeployment, LlmProvider[]> = {
  EXTERNAL: ["ANTHROPIC", "OPENAI", "GEMINI"],
  INTERNAL: ["OPENAI_COMPATIBLE"],
};

export const DEPLOYMENT_LABEL: Record<LlmDeployment, string> = { EXTERNAL: "외부", INTERNAL: "내부" };
export const BILLING_LABEL: Record<LlmBillingType, string> = { FREE: "무료", PAID: "유료" };

/** 서버 LlmProvider 의 기본 입력 길이와 같게 둔다(화면 기본값 제안용 - 서버가 최종값을 정한다). */
export const DEFAULT_MAX_INPUT_CHARS: Record<LlmDeployment, number> = { EXTERNAL: 100_000, INTERNAL: 16_000 };

export interface LlmProviderRequest {
  name: string;
  provider: LlmProvider;
  billingType: LlmBillingType;
  baseUrl?: string | null;
  modelName: string;
  apiKey?: string | null;
  maskSensitive: boolean;
  maxInputChars?: number | null;
  description?: string | null;
}

export interface LlmProviderResponse {
  id: number;
  name: string;
  deployment: LlmDeployment;
  provider: LlmProvider;
  billingType: LlmBillingType;
  baseUrl: string | null;
  effectiveBaseUrl: string | null;
  modelName: string;
  apiKeySet: boolean;
  apiKeyHint: string | null;
  maskSensitive: boolean;
  maxInputChars: number;
  active: boolean;
  lastTestStatus: string | null;
  lastTestMessage: string | null;
  lastTestedAt: string | null;
  description: string | null;
  createdBy: string | null;
  createdAt: string;
  updatedBy: string | null;
  updatedAt: string;
}

export interface LlmTestResult {
  status: string;
  success: boolean;
  message: string;
  httpStatus: number | null;
  durationMs: number;
  detail: string | null;
}

export interface LlmStatus {
  enabled: boolean;
  providerName: string | null;
  deployment: LlmDeployment | null;
  provider: LlmProvider | null;
  modelName: string | null;
  maskSensitive: boolean | null;
}

export async function listLlmProviders(): Promise<LlmProviderResponse[]> {
  const res = await apiClient.get<ApiResponse<LlmProviderResponse[]>>("/llm/providers");
  return unwrap(res.data);
}

export async function getLlmProvider(id: number): Promise<LlmProviderResponse> {
  const res = await apiClient.get<ApiResponse<LlmProviderResponse>>(`/llm/providers/${id}`);
  return unwrap(res.data);
}

export async function createLlmProvider(request: LlmProviderRequest): Promise<LlmProviderResponse> {
  const res = await apiClient.post<ApiResponse<LlmProviderResponse>>("/llm/providers", request);
  return unwrap(res.data);
}

export async function updateLlmProvider(id: number, request: LlmProviderRequest): Promise<LlmProviderResponse> {
  const res = await apiClient.put<ApiResponse<LlmProviderResponse>>(`/llm/providers/${id}`, request);
  return unwrap(res.data);
}

export async function deleteLlmProvider(id: number): Promise<void> {
  const res = await apiClient.delete<ApiResponse<void>>(`/llm/providers/${id}`);
  unwrap(res.data);
}

export async function activateLlmProvider(id: number): Promise<LlmProviderResponse> {
  const res = await apiClient.post<ApiResponse<LlmProviderResponse>>(`/llm/providers/${id}/activate`);
  return unwrap(res.data);
}

export async function deactivateLlmProvider(): Promise<void> {
  const res = await apiClient.post<ApiResponse<void>>("/llm/providers/deactivate");
  unwrap(res.data);
}

/** 저장 전 입력값으로 테스트. 수정 화면은 id 를 넘겨, 키 칸이 비었을 때 저장된 키를 쓰게 한다. */
export async function testLlmDraft(request: LlmProviderRequest, id?: number): Promise<LlmTestResult> {
  const res = await apiClient.post<ApiResponse<LlmTestResult>>("/llm/providers/test", request, {
    params: id ? { id } : undefined,
    // 내부 모델은 첫 호출에 모델을 올리느라 오래 걸린다. 서버 제한(30초)보다 조금 길게.
    timeout: 45_000,
  });
  return unwrap(res.data);
}

export async function testSavedLlmProvider(id: number): Promise<LlmProviderResponse> {
  const res = await apiClient.post<ApiResponse<LlmProviderResponse>>(`/llm/providers/${id}/test`, undefined, {
    timeout: 45_000,
  });
  return unwrap(res.data);
}

/** GPT 기능 사용 가능 여부. enabled=false 면 GPT 탭·분석 버튼을 그리지 않는다(2단계에서 사용). */
export async function getLlmStatus(): Promise<LlmStatus> {
  const res = await apiClient.get<ApiResponse<LlmStatus>>("/llm/status");
  return unwrap(res.data);
}

// ---------------------------------------------------------------- GPT 분석(2단계)

/** 분석 대상 종류. 서버가 요청마다 이 값으로 조회 권한을 다시 확인한다(설계 7절). */
export type LlmTargetType =
  | "ETL_ASSISTANT"
  | "ETL_JOB"
  | "CDC_PIPELINE"
  | "WORKFLOW"
  | "WORKFLOW_DRAFT"
  | "WORKFLOW_NODE"
  | "WORKFLOW_TASK";
export type LlmAgentType = "ERROR_ANALYSIS" | "JOB_ANALYSIS";

export interface LlmChatTurn {
  role: "user" | "assistant";
  content: string;
}

export interface LlmChatRequest {
  agentType: LlmAgentType;
  targetType: LlmTargetType;
  /** WORKFLOW_TASK: dagId · runId · taskId / CDC_PIPELINE: pipelineId */
  target: Record<string, string>;
  /** 화면에 보이는 오류 문구. 대상 정보(이름·SQL·커넥터 상태 등)는 서버가 모은다. */
  errorText?: string | null;
  /** 이번 창에서 이미 오간 대화. 오류 분석은 첫 분석 답부터, 속성창은 첫 질문부터(요약된 부분은 빼고). */
  history?: LlmChatTurn[];
  /** 속성창 대화의 앞부분 요약(서버가 compacted 이벤트로 준 것). */
  summary?: string | null;
  /** WORKFLOW_DRAFT: 화면의 작성 중 설계(JSON - 속성 입력 · 노드 · 연결). 서버에 아직 없는 대상이라 화면이 보낸다. */
  draft?: string | null;
  question?: string | null;
}

/** 대화가 길어 서버가 앞부분을 줄였다. DROPPED: 오래된 문답을 뺌, SUMMARIZED: 요약으로 바꿈(summary 를 다음 요청에 보낸다). */
export interface LlmCompacted {
  mode: "DROPPED" | "SUMMARIZED";
  /** 보낸 대화 목록 앞에서부터 줄인 개수 */
  droppedTurns: number;
  /** 줄인 문답(질문 + 답) 수 */
  droppedPairs: number;
  summary: string | null;
}

/**
 * 실제로 전송될 대상 정보 - 마스킹·자르기가 적용된 모습 그대로. 스트림의 meta 이벤트에서는 text 가 null.
 * maxInputChars 는 대상 정보에 주는 몫(설정의 최대 입력 길이 × 0.6)이다. 나머지는 대화가 쓴다.
 */
export interface LlmContextPreview {
  targetLabel: string;
  providerName: string;
  deployment: LlmDeployment;
  modelName: string;
  masked: boolean;
  truncated: boolean;
  omittedSections: string[];
  inputChars: number;
  maxInputChars: number;
  text: string | null;
}

export async function previewLlmContext(request: LlmChatRequest): Promise<LlmContextPreview> {
  const res = await apiClient.post<ApiResponse<LlmContextPreview>>("/llm/chat/preview", request, { timeout: 30_000 });
  return unwrap(res.data);
}

export interface LlmStreamHandlers {
  onMeta?: (meta: LlmContextPreview) => void;
  onCompacted?: (compacted: LlmCompacted) => void;
  onDelta: (text: string) => void;
  onDone?: (usage: { inputTokens: number | null; outputTokens: number | null; durationMs: number }) => void;
  /** 답변 도중 LLM 쪽에서 난 실패. 시작 전 실패(권한·대상 없음 등)는 Promise 가 reject 된다. */
  onError?: (error: { code: string; message: string }) => void;
}

/**
 * GPT 분석을 SSE 로 받는다. axios 는 응답을 흘려 읽지 못해 fetch 를 쓴다 - 그래서 인증 헤더·401 처리를 여기서 직접 한다
 * (api/client.ts 의 인터셉터와 같은 규칙). {@code signal} 로 [중지]·창 닫기 때 끊는다.
 */
export async function streamLlmChat(request: LlmChatRequest, handlers: LlmStreamHandlers, signal?: AbortSignal): Promise<void> {
  const token = getToken();
  const response = await fetch("/api/llm/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "text/event-stream",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(request),
    signal,
  });
  if (response.status === 401) {
    clearSession();
    if (window.location.pathname !== "/login") {
      window.location.assign("/login");
    }
    throw new Error("인증이 필요합니다.");
  }
  const contentType = response.headers.get("content-type") ?? "";
  if (!response.ok || !contentType.includes("text/event-stream") || !response.body) {
    let message = `GPT 분석 요청이 실패했습니다(HTTP ${response.status}).`;
    try {
      const body = (await response.json()) as ApiResponse<unknown>;
      message = body.error?.message ?? message;
    } catch {
      // 본문이 JSON 이 아니면 기본 문구
    }
    throw new Error(message);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const dispatch = (frame: string) => {
    let event = "message";
    const data: string[] = [];
    for (const line of frame.split("\n")) {
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
    }
    if (data.length === 0) return;
    const payload = JSON.parse(data.join("\n"));
    if (event === "delta") handlers.onDelta(payload.text ?? "");
    else if (event === "meta") handlers.onMeta?.(payload as LlmContextPreview);
    else if (event === "compacted") handlers.onCompacted?.(payload as LlmCompacted);
    else if (event === "done") handlers.onDone?.(payload);
    else if (event === "error") handlers.onError?.(payload);
  };
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
    let index: number;
    while ((index = buffer.indexOf("\n\n")) >= 0) {
      dispatch(buffer.slice(0, index));
      buffer = buffer.slice(index + 2);
    }
  }
  if (buffer.trim()) dispatch(buffer);
}

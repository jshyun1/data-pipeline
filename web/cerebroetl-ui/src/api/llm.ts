import { apiClient, unwrap, type ApiResponse } from "./client";
import { getToken } from "../auth/session";

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

export type LlmTargetType = "GENERAL" | "NIFI_GROUP" | "ETL_JOB" | "CDC_PIPELINE" | "WORKFLOW_NODE" | "WORKFLOW_TASK";
export type LlmChatRole = "user" | "assistant";

export interface LlmChatMessage {
  role: LlmChatRole;
  content: string;
}

export interface LlmChatRequest {
  targetType: LlmTargetType;
  targetId: string;
  question: string;
  messages: LlmChatMessage[];
}

export interface LlmChatResponse {
  answer: string;
  providerName: string;
  modelName: string;
  masked: boolean;
  durationMs: number;
}

export interface LlmContextPreviewResponse {
  targetTitle: string;
  providerName: string;
  masked: boolean;
  context: string;
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

export async function streamLlmChat(request: LlmChatRequest, onDelta: (delta: string) => void): Promise<LlmChatResponse> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "text/event-stream",
  };
  const token = getToken();
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const response = await fetch("/api/llm/chat", {
    method: "POST",
    headers,
    body: JSON.stringify(request),
  });
  if (!response.ok || !response.body) {
    const text = await response.text();
    let message = text || "AI 어시스턴트 요청에 실패했습니다.";
    try {
      const parsed = JSON.parse(text) as ApiResponse<unknown>;
      message = parsed.error?.message ?? message;
    } catch {
      // JSON 오류 본문이 아니면 원문 메시지를 그대로 쓴다.
    }
    throw new Error(message);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let donePayload: LlmChatResponse | null = null;

  const consumeEvent = (raw: string) => {
    const lines = raw.split(/\r?\n/);
    const event = lines.find((line) => line.startsWith("event:"))?.slice("event:".length).trim() ?? "message";
    const data = lines
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice("data:".length).trimStart())
      .join("\n");
    if (!data) {
      return;
    }
    if (event === "delta") {
      onDelta(data);
    } else if (event === "done") {
      donePayload = JSON.parse(data) as LlmChatResponse;
    } else if (event === "error") {
      throw new Error(data);
    }
  };

  for (;;) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    const events = buffer.split(/\r?\n\r?\n/);
    buffer = events.pop() ?? "";
    for (const event of events) {
      consumeEvent(event);
    }
  }
  buffer += decoder.decode();
  if (buffer.trim()) {
    consumeEvent(buffer);
  }
  if (!donePayload) {
    throw new Error("AI 어시스턴트 응답이 완료되지 않았습니다.");
  }
  return donePayload;
}

export async function previewLlmChatContext(request: LlmChatRequest): Promise<LlmContextPreviewResponse> {
  const res = await apiClient.post<ApiResponse<LlmContextPreviewResponse>>("/llm/chat/preview", request);
  return unwrap(res.data);
}

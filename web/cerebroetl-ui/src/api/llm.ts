import { apiClient, unwrap, type ApiResponse } from "./client";

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

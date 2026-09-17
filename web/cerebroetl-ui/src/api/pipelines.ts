import { apiClient, unwrap, type ApiResponse } from "./client";
import type {
  LogPipelineCreateRequest,
  PipelineCommandHistoryResponse,
  PipelineCreateRequest,
  PipelineResponse,
  PipelineRuntimeStatusResponse,
  PipelineConsistencyCheckResponse,
} from "../types/pipeline";

export async function listPipelines(): Promise<PipelineResponse[]> {
  const res = await apiClient.get<ApiResponse<PipelineResponse[]>>("/pipelines");
  return unwrap(res.data);
}

export async function listPipelineRuntimeStatuses(): Promise<PipelineRuntimeStatusResponse[]> {
  const res = await apiClient.get<ApiResponse<PipelineRuntimeStatusResponse[]>>("/pipelines/runtime-statuses");
  return unwrap(res.data);
}

export async function getPipelineHistory(id: number): Promise<PipelineCommandHistoryResponse[]> {
  const res = await apiClient.get<ApiResponse<PipelineCommandHistoryResponse[]>>(`/pipelines/${id}/history`);
  return unwrap(res.data);
}

export async function listPipelineConsistencyChecks(id: number): Promise<PipelineConsistencyCheckResponse[]> {
  const res = await apiClient.get<ApiResponse<PipelineConsistencyCheckResponse[]>>(`/pipelines/${id}/consistency-checks`);
  return unwrap(res.data);
}

export async function checkPipelineConsistency(id: number): Promise<PipelineConsistencyCheckResponse> {
  const res = await apiClient.post<ApiResponse<PipelineConsistencyCheckResponse>>(`/pipelines/${id}/consistency-checks`);
  return unwrap(res.data);
}

export async function createPipeline(request: PipelineCreateRequest): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>("/pipelines", request);
  return unwrap(res.data);
}

export interface PipelineBatchCreateResponse {
  createdCount: number;
  pipelines: PipelineResponse[];
}

export async function createPipelineBatch(requests: PipelineCreateRequest[]): Promise<PipelineBatchCreateResponse> {
  const res = await apiClient.post<ApiResponse<PipelineBatchCreateResponse>>("/pipelines/batch", { pipelines: requests });
  return unwrap(res.data);
}

export async function createLogFilePipeline(request: LogPipelineCreateRequest): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>("/pipelines/log-file", request);
  return unwrap(res.data);
}

export async function deletePipeline(id: number): Promise<void> {
  const res = await apiClient.delete<ApiResponse<void>>(`/pipelines/${id}`);
  unwrap(res.data);
}

// 배포는 커넥터를 최초로 등록하는 생성 절차의 연장이라 포털에 남겨둔다(안 하면 새
// 파이프라인이 활성화될 방법이 없음). start/pause/stop/restart는 Airflow가 전담.
export async function deployPipeline(id: number): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>(`/pipelines/${id}/deploy`);
  return unwrap(res.data);
}

// 커넥터 불일치 경고를 닫는다 - 재배포는 안 하고, 확인했다는 기록만 남긴다.
export async function dismissConnectorDrift(id: number): Promise<void> {
  const res = await apiClient.post<ApiResponse<void>>(`/pipelines/${id}/dismiss-drift`);
  unwrap(res.data);
}

/**
 * 이미 등록된 싱크 커넥터에 지금 설정(실패 정책 등)을 다시 넣는다. 소스는 건드리지 않고, 실행 중이면 싱크 태스크만
 * 새 설정으로 재시작돼 커밋된 위치부터 이어 읽는다. 2026-09-17 실패 정책 전환(«버리고 계속» → «멈춤»)을 기존 파이프라인에 적용할 때 쓴다.
 */
export async function refreshSinkConfig(id: number): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>(`/pipelines/${id}/sink-config/refresh`, undefined, {
    timeout: 120_000,
  });
  return unwrap(res.data);
}

/** 멈춘 싱크가 어디서 멈췄는지(읽기 전용). 2026-09-17 실패 정책 전환으로 생긴 «건너뛰기»의 앞 단계. */
export interface StuckSinkPosition {
  topic: string;
  partition: number;
  /** 다음에 읽을 위치 = 멈춘 건의 오프셋 */
  committedOffset: number;
  endOffset: number;
  lag: number;
  recordTimestamp: string | null;
  keyPreview: string | null;
  /** 레코드 내용(개인정보 마스킹·길이 제한). 못 읽으면 null */
  preview: string | null;
  readError: string | null;
}

export interface StuckSink {
  pipelineId: number;
  connectorName: string;
  consumerGroupId: string;
  /** 싱크 태스크가 실제로 멈춰 있는지. false 면 건너뛸 수 없다 */
  stopped: boolean;
  failureTraces: string[];
  positions: StuckSinkPosition[];
}

export async function getStuckSink(id: number): Promise<StuckSink> {
  const res = await apiClient.get<ApiResponse<StuckSink>>(`/pipelines/${id}/sink-stuck`, { timeout: 30_000 });
  return unwrap(res.data);
}

/** 그 한 건을 건너뛰고 싱크를 다시 띄운다. 넘긴 건은 타깃에 들어가지 않는다 - 사유가 이력에 남는다. */
export async function skipStuckRecord(id: number, body: { topic: string; partition: number; offset: number; reason: string }): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>(`/pipelines/${id}/sink-stuck/skip`, body, { timeout: 120_000 });
  return unwrap(res.data);
}

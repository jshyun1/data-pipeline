import { apiClient, unwrap, type ApiResponse } from "./client";
import type {
  LogPipelineCreateRequest,
  PipelineCommandHistoryResponse,
  PipelineCreateRequest,
  PipelineResponse,
  PipelineRuntimeStatusResponse,
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

export async function createPipeline(request: PipelineCreateRequest): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>("/pipelines", request);
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

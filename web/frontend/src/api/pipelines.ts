import { apiClient, unwrap, type ApiResponse } from "./client";
import type {
  LogPipelineCreateRequest,
  PipelineCommandHistoryResponse,
  PipelineCreateRequest,
  PipelineResponse,
} from "../types/pipeline";

export async function listPipelines(): Promise<PipelineResponse[]> {
  const res = await apiClient.get<ApiResponse<PipelineResponse[]>>("/pipelines");
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

// start/pause/stop/restart는 Airflow DAG(kafka_pipelines_dynamic.py)가 같은 엔드포인트를
// 호출해서 담당한다 - 웹 UI는 생성/배포/삭제까지만 다룬다.
export async function deployPipeline(id: number): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>(`/pipelines/${id}/deploy`);
  return unwrap(res.data);
}

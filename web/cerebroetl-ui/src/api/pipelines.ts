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

// deploy/start/pause/stop/restart는 전부 "커넥터에 액션 하나 실행하고 최신 상태 돌려주는" 같은 모양이라 공용화.
async function invokeLifecycleAction(id: number, action: string): Promise<PipelineResponse> {
  const res = await apiClient.post<ApiResponse<PipelineResponse>>(`/pipelines/${id}/${action}`);
  return unwrap(res.data);
}

export const deployPipeline = (id: number) => invokeLifecycleAction(id, "deploy");
export const startPipeline = (id: number) => invokeLifecycleAction(id, "start");
export const pausePipeline = (id: number) => invokeLifecycleAction(id, "pause");
export const stopPipeline = (id: number) => invokeLifecycleAction(id, "stop");
export const restartPipeline = (id: number) => invokeLifecycleAction(id, "restart");

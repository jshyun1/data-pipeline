import { apiClient, unwrap, type ApiResponse } from "./client";

/**
 * CDC 파이프라인 그룹(관리 화면 트리의 폴더).
 *
 * <p>최상단 «전체 파이프라인»은 서버에 행이 없다 - 화면이 그리는 가상 뿌리다.
 * 그래서 parentId 가 null 인 그룹이 그 바로 아래 칸이 된다.
 */
export interface PipelineGroupResponse {
  id: number;
  parentId: number | null;
  name: string;
}

export interface PipelineGroupRequest {
  /** null 이면 «전체 파이프라인» 바로 아래. 수정 시에도 같은 뜻이라 현재 값을 그대로 실어 보낸다. */
  parentId: number | null;
  name: string;
}

export async function listPipelineGroups(): Promise<PipelineGroupResponse[]> {
  const res = await apiClient.get<ApiResponse<PipelineGroupResponse[]>>("/pipeline-groups");
  return unwrap(res.data);
}

export async function createPipelineGroup(request: PipelineGroupRequest): Promise<PipelineGroupResponse> {
  const res = await apiClient.post<ApiResponse<PipelineGroupResponse>>("/pipeline-groups", request);
  return unwrap(res.data);
}

export async function updatePipelineGroup(
  id: number, request: PipelineGroupRequest,
): Promise<PipelineGroupResponse> {
  const res = await apiClient.put<ApiResponse<PipelineGroupResponse>>(`/pipeline-groups/${id}`, request);
  return unwrap(res.data);
}

export async function deletePipelineGroup(id: number): Promise<void> {
  await apiClient.delete<ApiResponse<void>>(`/pipeline-groups/${id}`);
}

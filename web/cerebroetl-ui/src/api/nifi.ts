import { apiClient, unwrap, type ApiResponse } from "./client";

export interface NifiRevision {
  clientId?: string | null;
  version?: number | null;
}

export interface NifiParameter {
  name: string;
  value?: string | null;
  sensitive?: boolean | null;
  description?: string | null;
}

export interface NifiParameterEntity {
  parameter: NifiParameter;
}

export interface NifiParameterContextComponent {
  id: string;
  name: string;
  description?: string | null;
  parameters?: NifiParameterEntity[] | null;
}

export interface NifiParameterContextResponse {
  id: string;
  revision?: NifiRevision | null;
  component: NifiParameterContextComponent;
}

export interface NifiParameterRequest {
  name: string;
  value?: string | null;
  sensitive: boolean;
  description?: string | null;
}

export interface NifiParameterContextSaveRequest {
  name: string;
  description?: string | null;
  parameters: NifiParameterRequest[];
}

export async function listNifiParameterContexts(): Promise<NifiParameterContextResponse[]> {
  const res = await apiClient.get<ApiResponse<NifiParameterContextResponse[]>>("/nifi/parameter-contexts");
  return unwrap(res.data);
}

export async function createNifiParameterContext(
  request: NifiParameterContextSaveRequest,
): Promise<NifiParameterContextResponse> {
  const res = await apiClient.post<ApiResponse<NifiParameterContextResponse>>("/nifi/parameter-contexts", request);
  return unwrap(res.data);
}

export async function updateNifiParameterContext(
  id: string,
  request: NifiParameterContextSaveRequest,
): Promise<NifiParameterContextResponse> {
  const res = await apiClient.put<ApiResponse<NifiParameterContextResponse>>(
    `/nifi/parameter-contexts/${encodeURIComponent(id)}`,
    request,
  );
  return unwrap(res.data);
}

export async function deleteNifiParameterContext(id: string): Promise<void> {
  unwrap((await apiClient.delete<ApiResponse<void>>(`/nifi/parameter-contexts/${encodeURIComponent(id)}`)).data);
}

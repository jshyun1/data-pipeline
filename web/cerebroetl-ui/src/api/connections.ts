import { apiClient, unwrap, type ApiResponse } from "./client";
import type {
  CdcPrerequisiteResponse,
  ConnectionCreateRequest,
  ConnectionResponse,
  ConnectionTestRequest,
  ConnectionTestResponse,
  ConnectionUpdateRequest,
  ConnectionUsageResponse,
} from "../types/connection";

export async function listConnections(): Promise<ConnectionResponse[]> {
  const res = await apiClient.get<ApiResponse<ConnectionResponse[]>>("/connections");
  return unwrap(res.data);
}

export async function createConnection(request: ConnectionCreateRequest): Promise<ConnectionResponse> {
  const res = await apiClient.post<ApiResponse<ConnectionResponse>>("/connections", request);
  return unwrap(res.data);
}

export async function updateConnection(id: number, request: ConnectionUpdateRequest): Promise<ConnectionResponse> {
  const res = await apiClient.put<ApiResponse<ConnectionResponse>>(`/connections/${id}`, request);
  return unwrap(res.data);
}

// 실제로 접속해봐서 status(SUCCESS/FAILED)를 갱신한다.
export async function testConnection(id: number): Promise<ConnectionResponse> {
  const res = await apiClient.post<ApiResponse<ConnectionResponse>>(`/connections/${id}/test`);
  return unwrap(res.data);
}

export async function validateNewConnection(request: ConnectionTestRequest): Promise<ConnectionTestResponse> {
  const res = await apiClient.post<ApiResponse<ConnectionTestResponse>>("/connections/validate", request);
  return unwrap(res.data);
}

export async function validateConnectionUpdate(
  id: number,
  request: ConnectionUpdateRequest,
): Promise<ConnectionTestResponse> {
  const res = await apiClient.post<ApiResponse<ConnectionTestResponse>>(`/connections/${id}/validate`, request);
  return unwrap(res.data);
}

export async function listConnectionUsages(): Promise<ConnectionUsageResponse[]> {
  const res = await apiClient.get<ApiResponse<ConnectionUsageResponse[]>>("/connections/usages");
  return unwrap(res.data);
}

export async function checkCdcPrerequisites(id: number): Promise<CdcPrerequisiteResponse> {
  const res = await apiClient.post<ApiResponse<CdcPrerequisiteResponse>>(`/connections/${id}/cdc-prerequisites`);
  return unwrap(res.data);
}

export async function deleteConnection(id: number): Promise<void> {
  const res = await apiClient.delete<ApiResponse<void>>(`/connections/${id}`);
  unwrap(res.data);
}

export async function listConnectionSchemas(connectionId: number): Promise<string[]> {
  const res = await apiClient.get<ApiResponse<string[]>>(`/connections/${connectionId}/schemas`);
  return unwrap(res.data);
}

export async function listConnectionTables(connectionId: number, schema: string): Promise<string[]> {
  const res = await apiClient.get<ApiResponse<string[]>>(`/connections/${connectionId}/tables`, {
    params: { schema },
  });
  return unwrap(res.data);
}

export interface ColumnMetadataResponse { name: string; dataType: string; primaryKey: boolean; maskable: boolean }

export async function listConnectionColumns(connectionId: number, schema: string, table: string): Promise<ColumnMetadataResponse[]> {
  const res = await apiClient.get<ApiResponse<ColumnMetadataResponse[]>>(`/connections/${connectionId}/columns`, {
    params: { schema, table },
  });
  return unwrap(res.data);
}

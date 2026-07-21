import { apiClient, unwrap, type ApiResponse } from "./client";
import type { ConnectionCreateRequest, ConnectionResponse } from "../types/connection";

export async function listConnections(): Promise<ConnectionResponse[]> {
  const res = await apiClient.get<ApiResponse<ConnectionResponse[]>>("/connections");
  return unwrap(res.data);
}

export async function createConnection(request: ConnectionCreateRequest): Promise<ConnectionResponse> {
  const res = await apiClient.post<ApiResponse<ConnectionResponse>>("/connections", request);
  return unwrap(res.data);
}

export async function deleteConnection(id: number): Promise<void> {
  const res = await apiClient.delete<ApiResponse<void>>(`/connections/${id}`);
  unwrap(res.data);
}

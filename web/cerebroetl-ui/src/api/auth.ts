import { apiClient, unwrap, type ApiResponse } from "./client";

export interface AppUser {
  userId: string;
  userNm: string;
  email: string | null;
  telNo: string | null;
  hqCd: string;
  positionCd: string;
  admin: boolean;
}

export interface LoginResponse {
  token: string;
  expiresInMinutes: number;
  user: AppUser;
}

export interface RegisterPayload {
  userId: string;
  userNm: string;
  password: string;
  email?: string;
  telNo?: string;
  hqCd: string;
  positionCd: string;
}

export async function login(userId: string, password: string): Promise<LoginResponse> {
  const res = await apiClient.post<ApiResponse<LoginResponse>>("/auth/login", { userId, password });
  return unwrap(res.data);
}

export async function register(payload: RegisterPayload): Promise<void> {
  const res = await apiClient.post<ApiResponse<void>>("/auth/register", payload);
  unwrap(res.data);
}

export async function fetchMe(): Promise<AppUser> {
  const res = await apiClient.get<ApiResponse<AppUser>>("/auth/me");
  return unwrap(res.data);
}

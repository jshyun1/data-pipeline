import { apiClient, unwrap, type ApiResponse } from "./client";

// app_user 프로필. 인증 자체는 통합 계정 테이블(ST_USER)로 이 앱 자신이 검증한다(Keycloak 제거).
export interface AppUser {
  userId: string;
  userNm: string;
  email: string | null;
  telNo: string | null;
  hqCd: string | null;
  positionCd: string | null;
  admin: boolean;
}

export interface LoginResult {
  token: string;
  user: AppUser;
}

export async function login(userId: string, password: string): Promise<LoginResult> {
  const res = await apiClient.post<ApiResponse<LoginResult>>("/auth/login", { userId, password });
  return unwrap(res.data);
}

export async function fetchMe(): Promise<AppUser> {
  const res = await apiClient.get<ApiResponse<AppUser>>("/auth/me");
  return unwrap(res.data);
}

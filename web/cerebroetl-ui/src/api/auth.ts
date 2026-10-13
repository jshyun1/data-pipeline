import { apiClient, unwrap, type ApiResponse } from "./client";

// app_user 프로필. 인증은 이 앱 자신이 한다(Keycloak 제거) - 기본은 자체 계정(app_user), 설정에 따라 사내 ST_USER.
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

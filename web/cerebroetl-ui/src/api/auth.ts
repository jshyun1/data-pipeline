import { apiClient, unwrap, type ApiResponse } from "./client";

// app_user 프로필(백엔드가 Keycloak 신원으로 프로비저닝한 값). 인증 자체는 Keycloak이 담당.
export interface AppUser {
  userId: string;
  userNm: string;
  email: string | null;
  telNo: string | null;
  hqCd: string | null;
  positionCd: string | null;
  admin: boolean;
}

export async function fetchMe(): Promise<AppUser> {
  const res = await apiClient.get<ApiResponse<AppUser>>("/auth/me");
  return unwrap(res.data);
}

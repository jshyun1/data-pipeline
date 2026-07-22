import axios from "axios";
import { getAccessToken, userManager } from "../auth/oidc";

// 상대경로 그대로 둔다: 로컬 개발에서는 vite.config.ts의 dev proxy가,
// 운영(Docker)에서는 nginx.conf의 reverse proxy가 각각 pipeline-api로 넘겨준다.
export const apiClient = axios.create({
  baseURL: "/api",
});

// backend의 ApiResponse<T> 봉투(success/data/error)와 맞춘 타입.
export interface ApiResponse<T> {
  success: boolean;
  data: T | null;
  error: { code: string; message: string } | null;
}

export function unwrap<T>(response: ApiResponse<T>): T {
  if (!response.success) {
    throw new Error(response.error?.message ?? "요청 처리 중 오류가 발생했습니다.");
  }
  // DELETE처럼 반환값이 없는 성공 응답은 data가 null이라 T가 void인 호출부에서만
  // 쓰인다 - data 유무가 아니라 success 플래그만으로 성공/실패를 판단해야 한다.
  return response.data as T;
}

// Keycloak이 발급한 액세스 토큰을 모든 pipeline-api 요청에 Authorization 헤더로 붙인다.
apiClient.interceptors.request.use(async (config) => {
  const token = await getAccessToken();
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// 4xx/5xx 응답도 backend가 ApiResponse{success:false, error:{message}} 형태로 몸통을 채워
// 보내주므로, axios 기본 에러 메시지 대신 그 message를 그대로 꺼내 쓴다.
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    // 토큰 만료 등으로 401이면 Keycloak 로그인으로 다시 보낸다.
    if (error?.response?.status === 401) {
      void userManager.signinRedirect();
    }
    const message = error?.response?.data?.error?.message;
    if (message) {
      return Promise.reject(new Error(message));
    }
    return Promise.reject(error);
  },
);

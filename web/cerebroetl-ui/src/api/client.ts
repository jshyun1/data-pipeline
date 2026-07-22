import axios from "axios";

// 로그인 토큰을 localStorage에 보관한다. auth 컨텍스트와 axios 인터셉터가 공유.
const TOKEN_STORAGE_KEY = "cerebro_token";

export function getStoredToken(): string | null {
  return localStorage.getItem(TOKEN_STORAGE_KEY);
}

export function setStoredToken(token: string | null) {
  if (token) {
    localStorage.setItem(TOKEN_STORAGE_KEY, token);
  } else {
    localStorage.removeItem(TOKEN_STORAGE_KEY);
  }
}

// 상대경로 그대로 둔다: 로컬 개발에서는 vite.config.ts의 dev proxy가,
// 운영(Docker)에서는 nginx.conf의 reverse proxy가 각각 pipeline-api로 넘겨준다.
export const apiClient = axios.create({
  baseURL: "/api",
});

// 저장된 JWT가 있으면 모든 pipeline-api 요청에 Authorization 헤더로 붙인다.
apiClient.interceptors.request.use((config) => {
  const token = getStoredToken();
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
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

// 4xx/5xx 응답도 backend가 ApiResponse{success:false, error:{message}} 형태로 몸통을
// 채워서 보내주므로, axios 기본 에러 메시지 대신 그 message를 그대로 꺼내 쓴다.
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    // 세션 만료(유효 토큰이 있었는데 401): 토큰을 비우고 로그인 화면으로.
    // 단, 로그인 요청 자체의 401(자격증명 오류)은 그 폼에서 처리하게 그냥 통과시킨다.
    const status = error?.response?.status;
    const url: string = error?.config?.url ?? "";
    if (status === 401 && !url.includes("/auth/login") && getStoredToken()) {
      setStoredToken(null);
      if (!window.location.pathname.startsWith("/login")) {
        window.location.assign("/login");
      }
    }
    const message = error?.response?.data?.error?.message;
    if (message) {
      return Promise.reject(new Error(message));
    }
    return Promise.reject(error);
  },
);


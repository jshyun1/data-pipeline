import type { AppUser } from "../api/auth";

// Keycloak 제거 후 계정테이블 기반 자체 로그인 세션. web-client-svr의
// sessionManager.js와 동일한 패턴(JWT를 localStorage에 저장, Authorization 헤더로 첨부).
const TOKEN_KEY = "cerebro_token";
const USER_KEY = "cerebro_user";

export function saveSession(token: string, user: AppUser): void {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, JSON.stringify(user));
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function getStoredUser(): AppUser | null {
  const raw = localStorage.getItem(USER_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as AppUser;
  } catch {
    return null;
  }
}

export function clearSession(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
}

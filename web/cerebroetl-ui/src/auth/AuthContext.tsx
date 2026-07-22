import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { getAccessToken, userManager } from "./oidc";
import { fetchMe, type AppUser } from "../api/auth";

type AuthStatus = "loading" | "authenticated" | "unauthenticated";

interface AuthContextValue {
  status: AuthStatus;
  user: AppUser | null;
  login: () => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<AppUser | null>(null);

  // 앱 로드 시 Keycloak 세션(로컬 저장된 토큰)이 유효하면 /me로 앱 프로필을 가져온다.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const token = await getAccessToken();
      if (!token) {
        if (!cancelled) setStatus("unauthenticated");
        return;
      }
      try {
        const me = await fetchMe();
        if (!cancelled) {
          setUser(me);
          setStatus("authenticated");
        }
      } catch {
        if (!cancelled) setStatus("unauthenticated");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Keycloak 로그인 페이지로 리다이렉트(Authorization Code + PKCE).
  // signinRedirect는 리다이렉트 전에 Keycloak 메타데이터를 fetch하는데, 자체 서명
  // 인증서가 브라우저에 신뢰돼 있지 않으면 여기서 실패한다 → 에러를 던져 호출부가 안내하게 한다.
  const login = async () => {
    await userManager.signinRedirect();
  };

  // Keycloak 로그아웃(SSO 세션 종료) 후 /login으로 복귀.
  const logout = () => {
    void userManager.signoutRedirect();
  };

  return (
    <AuthContext.Provider value={{ status, user, login, logout }}>{children}</AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth는 AuthProvider 안에서만 사용할 수 있습니다.");
  }
  return ctx;
}

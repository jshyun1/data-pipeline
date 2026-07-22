import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { getStoredToken, setStoredToken } from "../api/client";
import { fetchMe, login as loginApi, type AppUser } from "../api/auth";

type AuthStatus = "loading" | "authenticated" | "unauthenticated";

interface AuthContextValue {
  status: AuthStatus;
  user: AppUser | null;
  login: (userId: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<AppUser | null>(null);

  // 앱 로드 시 저장된 토큰이 있으면 /me로 유효성 확인 + 사용자 정보 복원.
  useEffect(() => {
    if (!getStoredToken()) {
      setStatus("unauthenticated");
      return;
    }
    fetchMe()
      .then((me) => {
        setUser(me);
        setStatus("authenticated");
      })
      .catch(() => {
        setStoredToken(null);
        setUser(null);
        setStatus("unauthenticated");
      });
  }, []);

  const login = async (userId: string, password: string) => {
    const res = await loginApi(userId, password);
    setStoredToken(res.token);
    setUser(res.user);
    setStatus("authenticated");
  };

  const logout = () => {
    setStoredToken(null);
    setUser(null);
    setStatus("unauthenticated");
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

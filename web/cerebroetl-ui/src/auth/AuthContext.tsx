import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { clearSession, getStoredUser, getToken, saveSession } from "./session";
import { login as loginApi, type AppUser } from "../api/auth";

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

  // 앱 로드 시 localStorage에 저장된 세션(계정테이블 로그인으로 발급받은 JWT)이 있으면
  // 그대로 인증된 것으로 취급한다 - 만료/무효 여부는 실제 API 호출에서 401로 드러나며,
  // client.ts의 응답 인터셉터가 그 시점에 세션을 지우고 로그인 화면으로 보낸다.
  useEffect(() => {
    const token = getToken();
    const storedUser = getStoredUser();
    if (token && storedUser) {
      setUser(storedUser);
      setStatus("authenticated");
    } else {
      setStatus("unauthenticated");
    }
  }, []);

  const login = async (userId: string, password: string) => {
    const result = await loginApi(userId, password);
    saveSession(result.token, result.user);
    setUser(result.user);
    setStatus("authenticated");
  };

  const logout = () => {
    clearSession();
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

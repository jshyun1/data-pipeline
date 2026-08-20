import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";
import { clearSession, getStoredUser, getToken, saveSession } from "./session";
import { login as loginApi, type AppUser } from "../api/auth";
import { can as canHelper, getMe, type AccessAction, type AuthzMe, type SystemCode } from "../api/authz";

type AuthStatus = "loading" | "authenticated" | "unauthenticated";

interface AuthContextValue {
  status: AuthStatus;
  user: AppUser | null;
  permissions: AuthzMe | null;
  permissionsLoaded: boolean;
  can: (system: SystemCode, action: AccessAction) => boolean;
  refreshPermissions: () => Promise<void>;
  login: (userId: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<AppUser | null>(null);
  const [permissions, setPermissions] = useState<AuthzMe | null>(null);
  const [permissionsLoaded, setPermissionsLoaded] = useState(false);

  const refreshPermissions = useCallback(async () => {
    try {
      const me = await getMe();
      setPermissions(me);
    } catch {
      // 권한 조회 실패(백엔드 미배포/일시 오류)는 치명적이지 않다 - 메뉴 게이팅은 fail-open으로
      // 처리한다(인가 강제는 아직 없음). 화면이 통째로 비어 보이는 상황을 막는다.
      setPermissions(null);
    } finally {
      setPermissionsLoaded(true);
    }
  }, []);

  // 앱 로드 시 저장된 세션(계정테이블 로그인 JWT)이 있으면 인증된 것으로 취급하고 권한을 읽는다.
  useEffect(() => {
    const token = getToken();
    const storedUser = getStoredUser();
    if (token && storedUser) {
      setUser(storedUser);
      setStatus("authenticated");
      void refreshPermissions();
    } else {
      setStatus("unauthenticated");
    }
  }, [refreshPermissions]);

  const login = async (userId: string, password: string) => {
    const result = await loginApi(userId, password);
    saveSession(result.token, result.user);
    setUser(result.user);
    setStatus("authenticated");
    setPermissionsLoaded(false);
    await refreshPermissions();
  };

  const logout = () => {
    clearSession();
    setUser(null);
    setPermissions(null);
    setPermissionsLoaded(false);
    setStatus("unauthenticated");
  };

  const can = useCallback(
    (system: SystemCode, action: AccessAction) => canHelper(permissions, system, action),
    [permissions],
  );

  return (
    <AuthContext.Provider
      value={{ status, user, permissions, permissionsLoaded, can, refreshPermissions, login, logout }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth는 AuthProvider 안에서만 사용할 수 있습니다.");
  }
  return ctx;
}

import { Navigate, Outlet } from "react-router-dom";
import { Spin } from "antd";
import { useAuth } from "./AuthContext";

/**
 * 로그인하지 않았으면 /login으로 보낸다. 토큰 검증 중에는 스피너를 보여준다.
 *
 * Keycloak 제거 이후 Airflow/NiFi는 nginx가 공유 서비스계정으로 대신 인증하므로
 * (PlatformSessionBootstrap이 하던 일), 여기서는 이 앱 자신의 로그인 여부만 확인하면 된다.
 */
export function RequireAuth() {
  const { status } = useAuth();

  if (status === "loading") {
    return (
      <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <Spin size="large" />
      </div>
    );
  }
  if (status === "unauthenticated") {
    return <Navigate to="/login" replace />;
  }
  return <Outlet />;
}

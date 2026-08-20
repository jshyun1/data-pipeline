import { Navigate, Outlet } from "react-router-dom";
import { useAuth } from "./AuthContext";
import type { AccessAction, SystemCode } from "../api/authz";

/**
 * 라우트 가드(설계서 §7.4). 권한이 로드됐고 요구 권한이 없으면 대시보드로 돌려보낸다. 권한이
 * 아직 로드되지 않았으면(fail-open) 통과시킨다 - 인가 강제의 최종 방어선은 서버
 * (@RequirePermission)이고, 이 가드는 UX(권한 없는 URL 직접 진입 차단)용이다.
 */
export function RequirePermissionRoute({
  system,
  action = "READ",
}: {
  system: SystemCode;
  action?: AccessAction;
}) {
  const { permissionsLoaded, can } = useAuth();

  if (permissionsLoaded && !can(system, action)) {
    return <Navigate to="/dashboard" replace />;
  }
  return <Outlet />;
}

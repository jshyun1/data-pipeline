import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Spin } from "antd";
import { userManager } from "../auth/oidc";

/**
 * Keycloak 로그인 후 돌아오는 콜백(/auth/callback). Authorization Code를 토큰으로 교환한 뒤
 * 대시보드로 이동한다. 세션을 방금 저장했으므로 전체 페이지 이동으로 AuthProvider를
 * 다시 초기화시킨다(가장 단순하고 확실).
 */
export function CallbackPage() {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    userManager
      .signinRedirectCallback()
      .then(() => {
        window.location.assign("/dashboard");
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : "로그인 처리 중 오류가 발생했습니다.");
      });
  }, [navigate]);

  if (error) {
    return (
      <div className="login-shell">
        <div className="login-card">
          <div className="login-kicker">MANUAL PORTAL</div>
          <h1 className="login-title">로그인 오류</h1>
          <p className="login-subtitle">{error}</p>
          <a href="/login">로그인 화면으로</a>
        </div>
      </div>
    );
  }

  return (
    <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <Spin size="large" tip="로그인 처리 중..." />
    </div>
  );
}

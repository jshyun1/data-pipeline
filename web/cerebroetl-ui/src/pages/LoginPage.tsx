import { Navigate } from "react-router-dom";
import { Button } from "antd";
import { useAuth } from "../auth/AuthContext";

/**
 * 브랜딩 랜딩 화면. 실제 자격증명 입력은 Keycloak 로그인 페이지에서 이뤄진다
 * (Authorization Code + PKCE 리다이렉트). 이렇게 해야 브라우저가 Keycloak과 세션을
 * 맺어 NiFi/Airflow까지 SSO가 흐른다. "등록신청"도 Keycloak이 담당한다.
 */
export function LoginPage() {
  const { status, login } = useAuth();

  if (status === "authenticated") {
    return <Navigate to="/dashboard" replace />;
  }

  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="login-kicker">MANUAL PORTAL</div>
        <h1 className="login-title">CEREBRO ETL</h1>
        <p className="login-subtitle">권한에 맞는 데이터 파이프라인을 안전하게 관리합니다.</p>

        <Button
          type="primary"
          size="large"
          block
          danger
          onClick={login}
          disabled={status === "loading"}
          style={{ marginTop: 8 }}
        >
          로그인
        </Button>
        <p style={{ color: "#9aa1ab", fontSize: 12, marginTop: 14, marginBottom: 0 }}>
          통합 계정(Keycloak)으로 로그인합니다. 계정이 없으면 관리자에게 등록을 요청하세요.
        </p>
      </div>
    </div>
  );
}

import { useEffect, useRef, useState } from "react";
import { Navigate } from "react-router-dom";
import { Button, Spin } from "antd";
import { useAuth } from "../auth/AuthContext";

const KEYCLOAK_URL =
  import.meta.env.VITE_OIDC_AUTHORITY?.replace(/\/realms\/.*$/, "") ?? "https://localhost:8543";

/**
 * 미인증 사용자는 별도 버튼 클릭 없이 CEREBRO 전용 Keycloak 로그인 테마로 이동한다.
 * 자격증명은 포털이 받지 않고 Keycloak에만 입력되며 Authorization Code + PKCE와
 * 브라우저 SSO 세션을 그대로 유지한다.
 */
export function LoginPage() {
  const { status, login } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const loginStarted = useRef(false);

  const onLogin = async () => {
    setError(null);
    try {
      await login();
    } catch (e) {
      // 대개 Keycloak 자체 서명 인증서를 브라우저가 아직 신뢰하지 않아 메타데이터
      // 조회에 실패한 경우다. 인증서를 한 번 수락하도록 안내한다.
      console.error("signinRedirect 실패:", e);
      setError("cert");
    }
  };

  useEffect(() => {
    if (status !== "unauthenticated" || loginStarted.current) return;
    loginStarted.current = true;
    void onLogin();
  }, [status]);

  if (status === "authenticated") {
    return <Navigate to="/dashboard" replace />;
  }

  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="login-kicker">MANUAL PORTAL</div>
        <h1 className="login-title">CEREBRO ETL</h1>
        <p className="login-subtitle">권한에 맞는 데이터 파이프라인을 안전하게 관리합니다.</p>

        {!error && (
          <div style={{ marginTop: 24, textAlign: "center" }}>
            <Spin />
            <p style={{ color: "#7b8799", fontSize: 13, marginTop: 12 }}>로그인 화면을 준비하고 있습니다.</p>
          </div>
        )}

        {error === "cert" && (
          <div className="login-error" style={{ marginTop: 12 }}>
            Keycloak 서버 인증서를 먼저 신뢰해야 합니다.{" "}
            <a href={KEYCLOAK_URL} target="_blank" rel="noreferrer">
              여기
            </a>
            를 새 탭에서 열어 "계속 진행"으로 인증서를 수락한 뒤 다시 로그인하세요.
            <Button type="primary" danger block onClick={onLogin} style={{ marginTop: 12 }}>
              다시 시도
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}

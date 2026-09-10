import { useState } from "react";
import { Navigate } from "react-router-dom";
import { Button, ConfigProvider, Form, Input } from "antd";
import { useAuth } from "../auth/AuthContext";
import { loginTheme } from "../theme/cerebro";

interface LoginFormValues {
  userId: string;
  password: string;
}

/** 통합 계정 테이블(ST_USER) 기반 자체 로그인 폼 (Keycloak 제거). */
export function LoginPage() {
  const { status, login } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const onFinish = async (values: LoginFormValues) => {
    setError(null);
    setLoading(true);
    try {
      await login(values.userId, values.password);
    } catch (e) {
      setError(e instanceof Error ? e.message : "로그인 중 오류가 발생했습니다.");
    } finally {
      setLoading(false);
    }
  };

  if (status === "authenticated") {
    return <Navigate to="/dashboard" replace />;
  }

  // 디자인 초안(login.html): 로고 + 입력칸 두 개 + 빨간 로그인 버튼 + 시스템 문의.
  // 초안의 «자동 로그인» 체크박스는 뺐다 - 로그인 유지 기능이 없어 눌러도 아무 일이 없는 칸이 된다.
  // 입력칸 제목은 초안처럼 안내 문구(placeholder)로 보여주고, 화면 낭독기용 이름은 aria-label 로 남긴다.
  return (
    <div className="login-shell">
      <ConfigProvider theme={loginTheme}>
        <div className="login-card">
          <h1 className="login-logo">
            CEREBRO <span className="login-logo-accent">ETL</span>
          </h1>
          <Form layout="vertical" onFinish={onFinish} disabled={loading} className="login-form">
            <Form.Item name="userId" rules={[{ required: true, message: "아이디를 입력하세요." }]}>
              <Input size="large" placeholder="아이디" aria-label="아이디" autoFocus autoComplete="username" />
            </Form.Item>
            <Form.Item name="password" rules={[{ required: true, message: "비밀번호를 입력하세요." }]}>
              <Input.Password
                size="large"
                placeholder="비밀번호"
                aria-label="비밀번호"
                autoComplete="current-password"
              />
            </Form.Item>
            {error && <p className="login-error">{error}</p>}
            <Button type="primary" htmlType="submit" size="large" block loading={loading} className="login-submit">
              로그인
            </Button>
          </Form>
          <div className="login-inquiry">
            시스템 문의 <span className="login-inquiry-phone">042-488-9114</span>
          </div>
        </div>
      </ConfigProvider>
    </div>
  );
}

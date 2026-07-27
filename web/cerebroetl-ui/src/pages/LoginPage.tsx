import { useState } from "react";
import { Navigate } from "react-router-dom";
import { Button, Form, Input } from "antd";
import { useAuth } from "../auth/AuthContext";

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

  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="login-kicker">CEREBRO ETL</div>
        <h1 className="login-title">통합 로그인</h1>
        <Form layout="vertical" onFinish={onFinish} disabled={loading} style={{ marginTop: 16 }}>
          <Form.Item name="userId" label="아이디" rules={[{ required: true, message: "아이디를 입력하세요." }]}>
            <Input autoFocus autoComplete="username" />
          </Form.Item>
          <Form.Item name="password" label="비밀번호" rules={[{ required: true, message: "비밀번호를 입력하세요." }]}>
            <Input.Password autoComplete="current-password" />
          </Form.Item>
          {error && (
            <p className="login-error" style={{ marginBottom: 12 }}>
              {error}
            </p>
          )}
          <Button type="primary" htmlType="submit" block loading={loading}>
            로그인
          </Button>
        </Form>
      </div>
    </div>
  );
}

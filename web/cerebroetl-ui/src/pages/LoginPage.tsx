import { useState } from "react";
import { Navigate, useNavigate } from "react-router-dom";
import { Button, Form, Input, Modal, message } from "antd";
import { useAuth } from "../auth/AuthContext";
import { register, type RegisterPayload } from "../api/auth";

export function LoginPage() {
  const { status, login } = useAuth();
  const navigate = useNavigate();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [registerOpen, setRegisterOpen] = useState(false);
  const [registerForm] = Form.useForm<RegisterPayload>();
  const [registering, setRegistering] = useState(false);

  // 이미 로그인돼 있으면 대시보드로.
  if (status === "authenticated") {
    return <Navigate to="/dashboard" replace />;
  }

  const onFinish = async (values: { userId: string; password: string }) => {
    setSubmitting(true);
    setError(null);
    try {
      await login(values.userId, values.password);
      navigate("/dashboard", { replace: true });
    } catch (e) {
      setError(e instanceof Error ? e.message : "로그인에 실패했습니다.");
    } finally {
      setSubmitting(false);
    }
  };

  const onRegister = async (values: RegisterPayload) => {
    setRegistering(true);
    try {
      await register(values);
      message.success("등록신청이 접수되었습니다. 관리자 승인 후 로그인할 수 있습니다.");
      setRegisterOpen(false);
      registerForm.resetFields();
    } catch (e) {
      message.error(e instanceof Error ? e.message : "등록신청에 실패했습니다.");
    } finally {
      setRegistering(false);
    }
  };

  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="login-kicker">MANUAL PORTAL</div>
        <h1 className="login-title">CEREBRO ETL</h1>
        <p className="login-subtitle">권한에 맞는 데이터 파이프라인을 안전하게 관리합니다.</p>

        <Form layout="vertical" onFinish={onFinish} requiredMark={false} className="login-form">
          <Form.Item name="userId" rules={[{ required: true, message: "아이디를 입력하세요" }]}>
            <Input size="large" placeholder="아이디" autoComplete="username" />
          </Form.Item>
          <Form.Item name="password" rules={[{ required: true, message: "비밀번호를 입력하세요" }]}>
            <Input.Password size="large" placeholder="비밀번호" autoComplete="current-password" />
          </Form.Item>
          {error && <div className="login-error">{error}</div>}
          <Button type="primary" htmlType="submit" size="large" block danger loading={submitting}>
            로그인
          </Button>
          <Button size="large" block className="login-register-btn" onClick={() => setRegisterOpen(true)}>
            등록신청
          </Button>
        </Form>
      </div>

      <Modal
        title="등록신청"
        open={registerOpen}
        onCancel={() => setRegisterOpen(false)}
        onOk={() => registerForm.submit()}
        confirmLoading={registering}
        okText="신청"
        cancelText="취소"
        destroyOnHidden
      >
        <p style={{ color: "#6b7280", marginTop: 0 }}>
          신청 후 관리자 승인이 완료되어야 로그인할 수 있습니다.
        </p>
        <Form form={registerForm} layout="vertical" requiredMark={false} onFinish={onRegister}>
          <Form.Item name="userId" label="아이디" rules={[{ required: true, message: "아이디를 입력하세요" }]}>
            <Input placeholder="예: hong.gildong" />
          </Form.Item>
          <Form.Item name="userNm" label="이름" rules={[{ required: true, message: "이름을 입력하세요" }]}>
            <Input placeholder="예: 홍길동" />
          </Form.Item>
          <Form.Item name="password" label="비밀번호" rules={[{ required: true, message: "비밀번호를 입력하세요" }]}>
            <Input.Password placeholder="비밀번호" autoComplete="new-password" />
          </Form.Item>
          <Form.Item name="hqCd" label="본부 코드" rules={[{ required: true, message: "본부 코드를 입력하세요" }]}>
            <Input placeholder="예: HQ01" />
          </Form.Item>
          <Form.Item name="positionCd" label="직함 코드" rules={[{ required: true, message: "직함 코드를 입력하세요" }]}>
            <Input placeholder="예: P03" />
          </Form.Item>
          <Form.Item name="email" label="이메일">
            <Input placeholder="선택" />
          </Form.Item>
          <Form.Item name="telNo" label="전화번호">
            <Input placeholder="선택" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}

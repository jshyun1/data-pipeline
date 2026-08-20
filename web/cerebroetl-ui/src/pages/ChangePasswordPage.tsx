import { useMutation } from "@tanstack/react-query";
import { Alert, Button, Card, Form, Input, message } from "antd";
import { useNavigate } from "react-router-dom";
import { changePassword } from "../api/authz";
import { useAuth } from "../auth/AuthContext";

export function ChangePasswordPage() {
  const [form] = Form.useForm();
  const navigate = useNavigate();
  const { permissions, refreshPermissions } = useAuth();
  const mustChange = permissions?.pwMustChange ?? false;

  const mut = useMutation({
    mutationFn: (v: { currentPassword: string; newPassword: string }) =>
      changePassword(v.currentPassword, v.newPassword),
    onSuccess: async () => {
      message.success("비밀번호를 변경했습니다.");
      form.resetFields();
      await refreshPermissions();
      navigate("/dashboard");
    },
    onError: (e: Error) => message.error(e.message),
  });

  return (
    <div style={{ padding: 16, maxWidth: 480 }}>
      <h2 style={{ marginTop: 0 }}>비밀번호 변경</h2>
      {mustChange && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="비밀번호 변경이 필요합니다"
          description="관리자가 비밀번호를 초기화했습니다. 계속하려면 새 비밀번호로 변경하세요."
        />
      )}
      <Card>
        <Form
          form={form}
          layout="vertical"
          onFinish={(v) => mut.mutate({ currentPassword: v.currentPassword, newPassword: v.newPassword })}
        >
          <Form.Item
            name="currentPassword"
            label="현재 비밀번호"
            rules={[{ required: true, message: "현재 비밀번호를 입력하세요." }]}
          >
            <Input.Password autoComplete="current-password" />
          </Form.Item>
          <Form.Item
            name="newPassword"
            label="새 비밀번호"
            rules={[{ required: true, message: "새 비밀번호를 입력하세요." }, { min: 8, message: "8자 이상 입력하세요." }]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item
            name="confirm"
            label="새 비밀번호 확인"
            dependencies={["newPassword"]}
            rules={[
              { required: true, message: "새 비밀번호를 다시 입력하세요." },
              ({ getFieldValue }) => ({
                validator(_, value) {
                  if (!value || getFieldValue("newPassword") === value) return Promise.resolve();
                  return Promise.reject(new Error("새 비밀번호가 일치하지 않습니다."));
                },
              }),
            ]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Button type="primary" htmlType="submit" loading={mut.isPending}>
            변경
          </Button>
        </Form>
      </Card>
    </div>
  );
}

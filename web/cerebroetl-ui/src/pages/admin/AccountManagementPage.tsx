import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Checkbox, Form, Input, Modal, Popconfirm, Space, Table, Tag, message } from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  createAccount,
  disableAccount,
  enableAccount,
  listAccounts,
  resetAccountPassword,
  unlockAccount,
  updateAccount,
  type AccountView,
} from "../../api/authz";

export function AccountManagementPage() {
  const queryClient = useQueryClient();
  const { data: accounts = [], isLoading } = useQuery({ queryKey: ["admin-accounts"], queryFn: listAccounts });

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<AccountView | null>(null);
  const [pwTarget, setPwTarget] = useState<AccountView | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();
  const [pwForm] = Form.useForm();

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["admin-accounts"] });

  const createMut = useMutation({
    mutationFn: createAccount,
    onSuccess: () => {
      message.success("계정을 추가했습니다.");
      setCreateOpen(false);
      createForm.resetFields();
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const editMut = useMutation({
    mutationFn: (v: { userId: string; userNm: string; email?: string; telNo?: string }) =>
      updateAccount(v.userId, { userNm: v.userNm, email: v.email, telNo: v.telNo }),
    onSuccess: () => {
      message.success("계정을 수정했습니다.");
      setEditTarget(null);
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const pwMut = useMutation({
    mutationFn: (v: { userId: string; password: string }) => resetAccountPassword(v.userId, v.password),
    onSuccess: () => {
      message.success("비밀번호를 초기화했습니다.");
      setPwTarget(null);
      pwForm.resetFields();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const toggleMut = useMutation({
    mutationFn: (v: { userId: string; enable: boolean }) =>
      v.enable ? enableAccount(v.userId) : disableAccount(v.userId),
    onSuccess: () => {
      message.success("상태를 변경했습니다.");
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const unlockMut = useMutation({
    mutationFn: unlockAccount,
    onSuccess: () => {
      message.success("잠금을 해제했습니다.");
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const columns: ColumnsType<AccountView> = [
    { title: "계정", dataIndex: "userId", key: "userId" },
    { title: "이름", dataIndex: "userNm", key: "userNm" },
    { title: "이메일", dataIndex: "email", key: "email", render: (v) => v ?? "-" },
    {
      title: "권한",
      dataIndex: "admin",
      key: "admin",
      render: (admin: boolean) => (admin ? <Tag color="gold">관리자</Tag> : <Tag>일반</Tag>),
    },
    {
      title: "상태",
      dataIndex: "useYn",
      key: "useYn",
      render: (useYn: string) =>
        useYn === "Y" ? <Tag color="green">사용</Tag> : <Tag color="default">비활성</Tag>,
    },
    {
      title: "최종 로그인",
      dataIndex: "lastLoginDt",
      key: "lastLoginDt",
      render: (v: string | null) => (v ? v.replace("T", " ").slice(0, 19) : "-"),
    },
    {
      title: "잠금",
      dataIndex: "locked",
      key: "locked",
      render: (locked: boolean, row) =>
        locked ? <Tag color="red">🔒 {row.loginFailCount}회</Tag> : <span>-</span>,
    },
    {
      title: "작업",
      key: "action",
      width: 320,
      render: (_, row) => (
        <Space size="small" wrap>
          <Button size="small" onClick={() => { setEditTarget(row); editForm.setFieldsValue(row); }}>
            수정
          </Button>
          <Button size="small" onClick={() => setPwTarget(row)}>
            비번 초기화
          </Button>
          {row.locked && (
            <Button size="small" onClick={() => unlockMut.mutate(row.userId)}>
              잠금 해제
            </Button>
          )}
          {row.useYn === "Y" ? (
            <Popconfirm
              title="이 계정을 비활성화하시겠습니까?"
              onConfirm={() => toggleMut.mutate({ userId: row.userId, enable: false })}
            >
              <Button size="small" danger>
                비활성화
              </Button>
            </Popconfirm>
          ) : (
            <Button size="small" onClick={() => toggleMut.mutate({ userId: row.userId, enable: true })}>
              활성화
            </Button>
          )}
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>계정 관리</h2>
      <Card
        extra={
          <Button type="primary" onClick={() => setCreateOpen(true)}>
            계정 추가
          </Button>
        }
      >
        <Table<AccountView>
          rowKey="userId"
          loading={isLoading}
          columns={columns}
          dataSource={accounts}
          size="middle"
          pagination={{ pageSize: 15 }}
        />
        <p style={{ color: "#888", marginTop: 12, marginBottom: 0 }}>
          ⓘ 관리자도 기존 비밀번호를 볼 수 없습니다. 초기화하면 사용자는 다음 로그인 때 변경을 요구받습니다.
          삭제 대신 비활성화를 권장합니다(실행 이력·감사 로그가 계정을 참조).
        </p>
      </Card>

      {/* 계정 추가 */}
      <Modal
        title="계정 추가"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={() => createForm.submit()}
        confirmLoading={createMut.isPending}
        okText="추가"
        cancelText="취소"
      >
        <Form form={createForm} layout="vertical" onFinish={(v) => createMut.mutate(v)}>
          <Form.Item name="userId" label="아이디" rules={[{ required: true, message: "아이디는 필수입니다." }]}>
            <Input autoComplete="off" />
          </Form.Item>
          <Form.Item name="userNm" label="이름" rules={[{ required: true, message: "이름은 필수입니다." }]}>
            <Input />
          </Form.Item>
          <Form.Item name="email" label="이메일">
            <Input />
          </Form.Item>
          <Form.Item name="telNo" label="전화번호">
            <Input />
          </Form.Item>
          <Form.Item name="password" label="초기 비밀번호" rules={[{ required: true, message: "비밀번호는 필수입니다." }]}>
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item name="admin" valuePropName="checked" initialValue={false}>
            <Checkbox>관리자 권한 부여(admin_yn=Y)</Checkbox>
          </Form.Item>
        </Form>
      </Modal>

      {/* 계정 수정 */}
      <Modal
        title={`계정 수정 — ${editTarget?.userId ?? ""}`}
        open={editTarget !== null}
        onCancel={() => setEditTarget(null)}
        onOk={() => editForm.submit()}
        confirmLoading={editMut.isPending}
        okText="저장"
        cancelText="취소"
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={(v) => editTarget && editMut.mutate({ userId: editTarget.userId, ...v })}
        >
          <Form.Item name="userNm" label="이름" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="email" label="이메일">
            <Input />
          </Form.Item>
          <Form.Item name="telNo" label="전화번호">
            <Input />
          </Form.Item>
        </Form>
      </Modal>

      {/* 비밀번호 초기화 */}
      <Modal
        title={`비밀번호 초기화 — ${pwTarget?.userId ?? ""}`}
        open={pwTarget !== null}
        onCancel={() => { setPwTarget(null); pwForm.resetFields(); }}
        onOk={() => pwForm.submit()}
        confirmLoading={pwMut.isPending}
        okText="초기화"
        cancelText="취소"
      >
        <Form form={pwForm} layout="vertical" onFinish={(v) => pwTarget && pwMut.mutate({ userId: pwTarget.userId, password: v.password })}>
          <Form.Item name="password" label="새 비밀번호" rules={[{ required: true, message: "비밀번호는 필수입니다." }]}>
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}

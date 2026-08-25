import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Checkbox, Form, Input, Modal, Popconfirm, Space, Table, Tag, Tooltip, message } from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  createAccount,
  disableAccount,
  enableAccount,
  listAccounts,
  listAssignments,
  listRoles,
  resetAccountPassword,
  setUserRoles,
  syncAllIdentities,
  unlockAccount,
  updateAccount,
  type AccountView,
} from "../../api/authz";

export function AccountManagementPage() {
  const queryClient = useQueryClient();
  const { data: accounts = [], isLoading } = useQuery({ queryKey: ["admin-accounts"], queryFn: listAccounts });
  const { data: assignments = [] } = useQuery({ queryKey: ["admin-assignments"], queryFn: listAssignments });
  const { data: roles = [] } = useQuery({ queryKey: ["admin-roles"], queryFn: listRoles });

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<AccountView | null>(null);
  const [pwTarget, setPwTarget] = useState<AccountView | null>(null);
  const [roleTarget, setRoleTarget] = useState<AccountView | null>(null);
  const [checkedRoles, setCheckedRoles] = useState<string[]>([]);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();
  const [pwForm] = Form.useForm();

  // userId → 배정된 역할 정보. 계정 목록 옆에 역할을 함께 보여주기 위해 배정 조회와 합친다.
  const rolesByUser = useMemo(() => {
    const map = new Map<string, { roleIds: string[]; roleNames: string[] }>();
    for (const a of assignments) map.set(a.userId, { roleIds: a.roleIds, roleNames: a.roleNames });
    return map;
  }, [assignments]);

  // 역할 배정 모달을 열면 현재 배정을 체크 상태로 복원한다.
  useEffect(() => {
    setCheckedRoles(roleTarget ? rolesByUser.get(roleTarget.userId)?.roleIds ?? [] : []);
  }, [roleTarget, rolesByUser]);

  const refresh = () => {
    queryClient.invalidateQueries({ queryKey: ["admin-accounts"] });
    queryClient.invalidateQueries({ queryKey: ["admin-assignments"] });
  };

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

  const roleMut = useMutation({
    mutationFn: (v: { userId: string; roleIds: string[] }) => setUserRoles(v.userId, v.roleIds),
    onSuccess: (r) => {
      // 역할 저장은 됐지만 NiFi/Airflow 개인계정 동기화가 실패했을 수 있다(best-effort).
      // 그대로 두면 그 사용자만 콘솔이 안 열리는데 화면에는 성공으로만 보인다 - 경고를 띄운다.
      if (r.syncWarnings?.length) {
        message.warning(`역할은 저장했지만 콘솔 계정 동기화에 실패했습니다 — ${r.syncWarnings.join(" / ")}`, 10);
      } else {
        message.success("역할을 저장했습니다.");
      }
      setRoleTarget(null);
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const syncMut = useMutation({
    mutationFn: syncAllIdentities,
    onSuccess: (r) =>
      message.success(`전체 동기화 완료 — 대상 ${r.total}명 · ETL ${r.nifiSynced} · Airflow ${r.airflowSynced}`),
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
      title: "역할",
      key: "roles",
      // admin_yn='Y' 는 PermissionService 에서 역할 계산 결과를 전 시스템 최대 권한으로
      // 덮어쓴다(부트스트랩용 백도어). 그래서 관리자에게 배정된 역할은 실제로 아무 효과가
      // 없는데, 파란 태그로 나란히 보이면 "이 역할 때문에 이만큼 되는구나"로 읽힌다.
      // 무시되고 있다는 사실을 그 자리에 그대로 적는다.
      render: (_, row) => {
        const names = rolesByUser.get(row.userId)?.roleNames ?? [];
        if (row.admin) {
          return (
            <Tooltip
              title={
                names.length
                  ? `관리자 권한이 전 시스템을 열어두므로 배정된 역할(${names.join(", ")})은 판정에 쓰이지 않습니다. 역할대로 통제하려면 권한을 '일반'으로 바꾸세요.`
                  : "관리자 권한이 전 시스템을 열어둡니다. 역할대로 통제하려면 권한을 '일반'으로 바꾸세요."
              }
            >
              <Tag>역할 무시됨</Tag>
            </Tooltip>
          );
        }
        return names.length ? (
          names.map((n) => <Tag key={n} color="blue">{n}</Tag>)
        ) : (
          <span style={{ color: "#bbb" }}>(없음)</span>
        );
      },
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
      width: 400,
      render: (_, row) => (
        <Space size="small" wrap>
          <Button size="small" onClick={() => setRoleTarget(row)}>
            역할 배정
          </Button>
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
          <Space>
            <Button
              loading={syncMut.isPending}
              onClick={() => syncMut.mutate()}
              title="배정된 역할 기준으로 전체 사용자를 ETL/Airflow 개인계정으로 일괄 재조정합니다."
            >
              ETL/Airflow 전체 동기화
            </Button>
            <Button type="primary" onClick={() => setCreateOpen(true)}>
              계정 추가
            </Button>
          </Space>
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

      {/* 역할 배정 */}
      <Modal
        title={roleTarget ? `역할 배정 — ${roleTarget.userNm}(${roleTarget.userId})` : "역할 배정"}
        open={roleTarget !== null}
        onCancel={() => setRoleTarget(null)}
        onOk={() => roleTarget && roleMut.mutate({ userId: roleTarget.userId, roleIds: checkedRoles })}
        confirmLoading={roleMut.isPending}
        okText="저장"
        cancelText="취소"
      >
        <Checkbox.Group
          style={{ display: "flex", flexDirection: "column", gap: 8 }}
          value={checkedRoles}
          onChange={(v) => setCheckedRoles(v as string[])}
          options={roles.map((r) => ({ label: `${r.roleNm} (${r.roleId})`, value: r.roleId }))}
        />
        {roleTarget?.admin ? (
          // 여기서 아무리 골라도 판정에 안 쓰인다는 걸 저장 전에 알려준다 - 나중에 권한을
          // '일반'으로 내릴 때를 대비해 미리 배정해 두는 것 자체는 유효하므로 막지는 않는다.
          <p style={{ color: "#b4740f", marginTop: 12, marginBottom: 0 }}>
            ⚠ 이 계정은 <b>권한이 '관리자'</b>라 전 시스템이 이미 열려 있습니다. 여기서 배정한 역할은
            권한을 '일반'으로 바꾸기 전까지 판정에 사용되지 않습니다.
          </p>
        ) : null}
        <p style={{ color: "#888", marginTop: 12, marginBottom: 0 }}>
          ⓘ 역할이 하나도 없는 사용자는 로그인은 되지만 메뉴가 보이지 않습니다(권한 없음이 기본값).
          역할을 바꾸면 ETL/Airflow 개인계정 권한도 함께 조정됩니다.
        </p>
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

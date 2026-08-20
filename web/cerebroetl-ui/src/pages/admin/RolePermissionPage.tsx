import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Checkbox, Form, Input, Modal, Popconfirm, Space, Table, Tag, message } from "antd";
import type { ColumnsType } from "antd/es/table";
import {
  BIT_READ,
  BIT_WRITE,
  SYSTEM_LABELS,
  SYSTEM_ORDER,
  createRole,
  deleteRole,
  getRole,
  listRoles,
  setRolePermissions,
  type RoleView,
  type SystemCode,
} from "../../api/authz";

interface RowState {
  read: boolean;
  write: boolean;
}

export function RolePermissionPage() {
  const queryClient = useQueryClient();
  const { data: roles = [], isLoading } = useQuery({ queryKey: ["admin-roles"], queryFn: listRoles });

  const [selected, setSelected] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [createForm] = Form.useForm();
  const [edited, setEdited] = useState<Record<string, RowState>>({});

  const { data: detail } = useQuery({
    queryKey: ["admin-role", selected],
    queryFn: () => getRole(selected as string),
    enabled: selected !== null,
  });

  // 역할 상세가 로드되면 편집 상태를 초기화한다(bits → 읽기/쓰기 체크박스).
  useEffect(() => {
    if (!detail) return;
    const next: Record<string, RowState> = {};
    for (const sc of SYSTEM_ORDER) {
      const bits = detail.systemBits?.[sc] ?? 0;
      next[sc] = { read: (bits & BIT_READ) === BIT_READ, write: bits === BIT_WRITE };
    }
    setEdited(next);
  }, [detail]);

  const refresh = () => queryClient.invalidateQueries({ queryKey: ["admin-roles"] });

  const createMut = useMutation({
    mutationFn: createRole,
    onSuccess: () => {
      message.success("역할을 추가했습니다.");
      setCreateOpen(false);
      createForm.resetFields();
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: deleteRole,
    onSuccess: () => {
      message.success("역할을 삭제했습니다.");
      setSelected(null);
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const saveMut = useMutation({
    mutationFn: () => {
      const bits: Record<string, number> = {};
      for (const sc of SYSTEM_ORDER) {
        const st = edited[sc] ?? { read: false, write: false };
        bits[sc] = st.write ? BIT_WRITE : st.read ? BIT_READ : 0;
      }
      return setRolePermissions(selected as string, bits);
    },
    onSuccess: () => {
      message.success("시스템 권한을 저장했습니다.");
      queryClient.invalidateQueries({ queryKey: ["admin-role", selected] });
      refresh();
    },
    onError: (e: Error) => message.error(e.message),
  });

  const setCell = (sc: SystemCode, patch: Partial<RowState>) =>
    setEdited((prev) => {
      const cur = prev[sc] ?? { read: false, write: false };
      const next = { ...cur, ...patch };
      // 쓰기를 켜면 읽기도 켜진다(WRITE=7 은 READ 포함). 읽기를 끄면 쓰기도 꺼진다.
      if (patch.write === true) next.read = true;
      if (patch.read === false) next.write = false;
      return { ...prev, [sc]: next };
    });

  const roleColumns: ColumnsType<RoleView> = [
    { title: "역할명", dataIndex: "roleNm", key: "roleNm", render: (v, r) => (
      <span>{v} {r.builtIn && <Tag>기본</Tag>}</span>
    ) },
    { title: "설명", dataIndex: "roleDesc", key: "roleDesc", render: (v) => v ?? "-" },
    { title: "사용자", dataIndex: "userCount", key: "userCount", width: 90 },
    {
      title: "작업",
      key: "action",
      width: 100,
      render: (_, r) =>
        r.builtIn ? (
          <Tag color="default">삭제 불가</Tag>
        ) : (
          <Popconfirm title="이 역할을 삭제하시겠습니까?" onConfirm={() => deleteMut.mutate(r.roleId)}>
            <Button size="small" danger>
              삭제
            </Button>
          </Popconfirm>
        ),
    },
  ];

  const permColumns: ColumnsType<{ system: SystemCode }> = [
    { title: "시스템", key: "system", render: (_, r) => SYSTEM_LABELS[r.system] },
    {
      title: "읽기",
      key: "read",
      width: 80,
      render: (_, r) => (
        <Checkbox
          checked={edited[r.system]?.read ?? false}
          onChange={(e) => setCell(r.system, { read: e.target.checked })}
        />
      ),
    },
    {
      title: "쓰기",
      key: "write",
      width: 80,
      render: (_, r) =>
        r.system === "COMMON" ? (
          <span style={{ color: "#bbb" }}>—</span>
        ) : (
          <Checkbox
            checked={edited[r.system]?.write ?? false}
            onChange={(e) => setCell(r.system, { write: e.target.checked })}
          />
        ),
    },
  ];

  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>역할 및 권한</h2>
      <div style={{ display: "flex", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
        <Card
          title="역할 목록"
          style={{ flex: "1 1 460px" }}
          extra={
            <Button type="primary" onClick={() => setCreateOpen(true)}>
              역할 추가
            </Button>
          }
        >
          <Table<RoleView>
            rowKey="roleId"
            loading={isLoading}
            columns={roleColumns}
            dataSource={roles}
            size="middle"
            pagination={false}
            onRow={(r) => ({ onClick: () => setSelected(r.roleId), style: { cursor: "pointer" } })}
            rowClassName={(r) => (r.roleId === selected ? "ant-table-row-selected" : "")}
          />
        </Card>

        <Card
          title={selected ? `시스템 권한 — ${detail?.roleNm ?? selected}` : "시스템 권한"}
          style={{ flex: "1 1 380px" }}
        >
          {selected ? (
            <>
              <Table<{ system: SystemCode }>
                rowKey="system"
                columns={permColumns}
                dataSource={SYSTEM_ORDER.map((s) => ({ system: s }))}
                size="small"
                pagination={false}
              />
              <p style={{ color: "#888", margin: "10px 0" }}>ⓘ '쓰기'는 변경과 실행을 함께 허용합니다.</p>
              <Space>
                <Button type="primary" loading={saveMut.isPending} onClick={() => saveMut.mutate()}>
                  저장
                </Button>
              </Space>
            </>
          ) : (
            <p style={{ color: "#888" }}>왼쪽에서 역할을 선택하세요.</p>
          )}
        </Card>
      </div>

      <Modal
        title="역할 추가"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={() => createForm.submit()}
        confirmLoading={createMut.isPending}
        okText="추가"
        cancelText="취소"
      >
        <Form form={createForm} layout="vertical" onFinish={(v) => createMut.mutate(v)}>
          <Form.Item name="roleId" label="역할 ID" rules={[{ required: true, message: "역할 ID는 필수입니다." }]}>
            <Input placeholder="ROLE_..." />
          </Form.Item>
          <Form.Item name="roleNm" label="역할명" rules={[{ required: true, message: "역할명은 필수입니다." }]}>
            <Input />
          </Form.Item>
          <Form.Item name="roleDesc" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}

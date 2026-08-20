import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Checkbox, Space, Table, Tag, message } from "antd";
import type { ColumnsType } from "antd/es/table";
import { listAssignments, listRoles, setUserRoles, type AssignmentView } from "../../api/authz";

export function UserRoleAssignPage() {
  const queryClient = useQueryClient();
  const { data: assignments = [], isLoading } = useQuery({
    queryKey: ["admin-assignments"],
    queryFn: listAssignments,
  });
  const { data: roles = [] } = useQuery({ queryKey: ["admin-roles"], queryFn: listRoles });

  const [selected, setSelected] = useState<AssignmentView | null>(null);
  const [checked, setChecked] = useState<string[]>([]);

  useEffect(() => {
    setChecked(selected?.roleIds ?? []);
  }, [selected]);

  const saveMut = useMutation({
    mutationFn: () => setUserRoles(selected!.userId, checked),
    onSuccess: () => {
      message.success("역할을 저장했습니다.");
      queryClient.invalidateQueries({ queryKey: ["admin-assignments"] });
    },
    onError: (e: Error) => message.error(e.message),
  });

  const columns: ColumnsType<AssignmentView> = [
    { title: "계정", dataIndex: "userId", key: "userId" },
    { title: "이름", dataIndex: "userNm", key: "userNm" },
    { title: "이메일", dataIndex: "email", key: "email", render: (v) => v ?? "-" },
    {
      title: "역할",
      dataIndex: "roleNames",
      key: "roleNames",
      render: (names: string[]) =>
        names.length ? names.map((n) => <Tag key={n} color="blue">{n}</Tag>) : <span style={{ color: "#bbb" }}>(없음)</span>,
    },
  ];

  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>사용자 역할 배정</h2>
      <div style={{ display: "flex", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
        <Card title="사용자" style={{ flex: "1 1 560px" }}>
          <Table<AssignmentView>
            rowKey="userId"
            loading={isLoading}
            columns={columns}
            dataSource={assignments}
            size="middle"
            pagination={{ pageSize: 15 }}
            onRow={(r) => ({ onClick: () => setSelected(r), style: { cursor: "pointer" } })}
            rowClassName={(r) => (r.userId === selected?.userId ? "ant-table-row-selected" : "")}
          />
        </Card>

        <Card title={selected ? `역할 배정 — ${selected.userNm}(${selected.userId})` : "역할 배정"} style={{ flex: "1 1 320px" }}>
          {selected ? (
            <>
              <Checkbox.Group
                style={{ display: "flex", flexDirection: "column", gap: 8 }}
                value={checked}
                onChange={(v) => setChecked(v as string[])}
                options={roles.map((r) => ({ label: `${r.roleNm} (${r.roleId})`, value: r.roleId }))}
              />
              <Space style={{ marginTop: 16 }}>
                <Button type="primary" loading={saveMut.isPending} onClick={() => saveMut.mutate()}>
                  저장
                </Button>
              </Space>
              <p style={{ color: "#888", marginTop: 12 }}>
                ⓘ 역할이 하나도 없는 사용자는 로그인은 되지만 메뉴가 보이지 않습니다(권한 없음이 기본값).
              </p>
            </>
          ) : (
            <p style={{ color: "#888" }}>왼쪽에서 사용자를 선택하세요.</p>
          )}
        </Card>
      </div>
    </div>
  );
}

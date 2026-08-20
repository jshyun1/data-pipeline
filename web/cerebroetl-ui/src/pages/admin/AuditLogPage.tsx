import { useQuery } from "@tanstack/react-query";
import { Card, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { listAudit, type AuditView } from "../../api/authz";

const ACTION_COLORS: Record<string, string> = {
  LOGIN_SUCCESS: "green",
  LOGIN_FAIL: "orange",
  ACCOUNT_LOCKED: "red",
  UNLOCK: "blue",
  CREATE_USER: "geekblue",
  UPDATE_USER: "geekblue",
  DISABLE_USER: "volcano",
  ENABLE_USER: "cyan",
  RESET_PASSWORD: "purple",
  CREATE_ROLE: "geekblue",
  UPDATE_ROLE: "geekblue",
  DELETE_ROLE: "volcano",
  GRANT_SYSTEM: "gold",
  SET_MENU: "gold",
  ASSIGN_ROLE: "gold",
  PROVISION_USER: "cyan",
  SYNC_NIFI_USER: "cyan",
  SYNC_AIRFLOW_USER: "cyan",
  DENIED: "red",
};

export function AuditLogPage() {
  const { data: rows = [], isLoading } = useQuery({ queryKey: ["admin-audit"], queryFn: () => listAudit(300) });

  const columns: ColumnsType<AuditView> = [
    {
      title: "시각",
      dataIndex: "occurredAt",
      key: "occurredAt",
      width: 180,
      render: (v: string | null) => (v ? v.replace("T", " ").slice(0, 19) : "-"),
    },
    { title: "수행자", dataIndex: "actorId", key: "actorId", width: 140 },
    {
      title: "동작",
      dataIndex: "action",
      key: "action",
      width: 150,
      render: (a: string) => <Tag color={ACTION_COLORS[a] ?? "default"}>{a}</Tag>,
    },
    {
      title: "대상",
      key: "target",
      width: 180,
      render: (_, r) => (r.targetId ? `${r.targetType ?? ""}:${r.targetId}` : "-"),
    },
    {
      title: "변경",
      key: "change",
      render: (_, r) =>
        r.beforeValue || r.afterValue ? (
          <span style={{ fontSize: 12 }}>
            <span style={{ color: "#999" }}>{r.beforeValue ?? "∅"}</span>
            {" → "}
            <span>{r.afterValue ?? "∅"}</span>
          </span>
        ) : (
          r.detail ?? "-"
        ),
    },
    { title: "IP", dataIndex: "clientIp", key: "clientIp", width: 130, render: (v) => v ?? "-" },
  ];

  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>감사 로그</h2>
      <Card>
        <Table<AuditView>
          rowKey="id"
          loading={isLoading}
          columns={columns}
          dataSource={rows}
          size="small"
          pagination={{ pageSize: 20 }}
        />
      </Card>
    </div>
  );
}

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Input, Select, Space, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs, { type Dayjs } from "dayjs";
import { activeDayPreset, dayPresetRange } from "../../components/dayPreset";
import { listAudit, type AuditView } from "../../api/authz";

const { RangePicker } = DatePicker;

/**
 * 로그가 «어디서» 났는지(구분). action 접두어로 판별한다.
 *
 * 자동 감사(PermissionAspect)는 `<시스템>_<동사>` 형태로 남는다 - NIFI_CREATE·KAFKA_DEPLOY·
 * AIRFLOW_RUN 처럼. 계정/권한·로그인처럼 각 서비스가 직접 남기는 액션은 접두어가 시스템이
 * 아니므로 이름으로 분류한다.
 */
const AREA_BY_PREFIX: Record<string, { label: string; color: string }> = {
  NIFI: { label: "ETL", color: "red" },
  KAFKA: { label: "CDC", color: "blue" },
  AIRFLOW: { label: "워크플로우", color: "purple" },
  COMMON: { label: "공통", color: "default" },
  ADMIN: { label: "계정·권한", color: "gold" },
  PORTAL: { label: "포털", color: "cyan" },
};

const ACCOUNT_ACTIONS = new Set([
  "LOGIN_SUCCESS", "LOGIN_FAIL", "ACCOUNT_LOCKED", "UNLOCK",
  "CREATE_USER", "UPDATE_USER", "DISABLE_USER", "ENABLE_USER", "RESET_PASSWORD",
  "CREATE_ROLE", "UPDATE_ROLE", "DELETE_ROLE", "GRANT_SYSTEM", "SET_MENU", "ASSIGN_ROLE",
  "PROVISION_USER", "SYNC_NIFI_USER", "SYNC_AIRFLOW_USER", "SYNC_ALL_IDENTITIES",
]);

function auditArea(action: string): { label: string; color: string } {
  if (ACCOUNT_ACTIONS.has(action)) return { label: "계정·권한", color: "gold" };
  if (action === "DENIED") return { label: "인가", color: "red" };
  const prefix = action.includes("_") ? action.slice(0, action.indexOf("_")) : action;
  return AREA_BY_PREFIX[prefix] ?? { label: "기타", color: "default" };
}

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
  SYNC_ALL_IDENTITIES: "cyan",
  DENIED: "red",
};

// 운영 자동감사(NIFI_CREATE·KAFKA_DELETE·AIRFLOW_UPDATE 등)는 동사 접미사로 색을 정한다.
function actionColor(action: string): string {
  if (ACTION_COLORS[action]) return ACTION_COLORS[action];
  if (action.startsWith("NIFI_CANVAS_")) {
    if (action.endsWith("_ADD")) return "green";
    if (action.endsWith("_REMOVE")) return "volcano";
    return "geekblue";
  }
  // 세분화 액션(KAFKA_DEPLOY·AIRFLOW_RUN·AIRFLOW_SCHEDULE …)을 동사 접미사로 색 구분.
  const suffix = action.includes("_") ? action.slice(action.indexOf("_") + 1) : action;
  if (["CREATE", "ADD", "DEPLOY", "START", "RUN", "TRIGGER", "RESUME", "ENABLE", "APPROVE"].includes(suffix)) return "green";
  if (["DELETE", "REMOVE", "STOP", "PAUSE", "DISABLE", "CANCEL", "ROLLBACK", "REJECT"].includes(suffix)) return "volcano";
  if (["UPDATE", "SCHEDULE", "CONFIGURE", "RESTART"].includes(suffix)) return "geekblue";
  if (["SYNC", "REPLAY", "RETRY", "UNLOCK"].includes(suffix)) return "cyan";
  return "default";
}

export function AuditLogPage() {
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>([
    dayjs().startOf("day"),
    dayjs().endOf("day"),
  ]);
  const [selectedActions, setSelectedActions] = useState<string[]>([]);
  const [keyword, setKeyword] = useState("");

  // 당일/전일 프리셋: 해당 날짜 하루(00:00~23:59)로 조회기간을 즉시 맞춘다.
  const applyDayPreset = (offsetDays: 0 | 1) => {
    setAppliedRange(dayPresetRange(offsetDays));
  };
  const selectedPreset = activeDayPreset(appliedRange[0], appliedRange[1]);

  const { data: rows = [], isFetching, refetch } = useQuery({
    queryKey: ["admin-audit", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () =>
      listAudit({
        from: appliedRange[0].format("YYYY-MM-DDTHH:mm:ss"),
        to: appliedRange[1].format("YYYY-MM-DDTHH:mm:ss"),
        limit: 3000,
      }),
  });

  // 동작 필터 옵션은 조회된 데이터에 실제로 존재하는 동작만 노출한다.
  const actionOptions = useMemo(
    () => [...new Set(rows.map((r) => r.action))].sort().map((a) => ({ label: a, value: a })),
    [rows],
  );

  // 동작(다중 선택) + 검색어(전 컬럼)를 프런트에서 거른다 - CDC 처리 로그 화면과 동일한 방식.
  const filtered = useMemo(() => {
    const kw = keyword.trim().toLowerCase();
    return rows.filter((r) => {
      if (selectedActions.length > 0 && !selectedActions.includes(r.action)) return false;
      if (!kw) return true;
      const haystack = [r.actorId, r.action, r.targetType, r.targetId, r.beforeValue, r.afterValue, r.detail, r.clientIp]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return haystack.includes(kw);
    });
  }, [rows, selectedActions, keyword]);

  const columns: ColumnsType<AuditView> = [
    {
      title: "시각",
      dataIndex: "occurredAt",
      key: "occurredAt",
      width: 180,
      defaultSortOrder: "descend",
      sorter: (a, b) => (a.occurredAt ?? "").localeCompare(b.occurredAt ?? ""),
      render: (v: string | null) => (v ? v.replace("T", " ").slice(0, 19) : "-"),
    },
    {
      title: "구분",
      dataIndex: "action",
      key: "area",
      width: 110,
      render: (action: string) => {
        const area = auditArea(action);
        return <Tag color={area.color}>{area.label}</Tag>;
      },
    },
    { title: "수행자", dataIndex: "actorId", key: "actorId", width: 140 },
    {
      title: "동작",
      dataIndex: "action",
      key: "action",
      width: 150,
      render: (a: string) => <Tag color={actionColor(a)}>{a}</Tag>,
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
        <Space wrap style={{ marginBottom: 16 }}>
          <Space.Compact>
            <Button type={selectedPreset === 0 ? "primary" : "default"} onClick={() => applyDayPreset(0)}>당일</Button>
            <Button type={selectedPreset === 1 ? "primary" : "default"} onClick={() => applyDayPreset(1)}>전일</Button>
          </Space.Compact>
          <RangePicker
            showTime={{ format: "HH:mm" }}
            format="YYYY-MM-DD HH:mm"
            allowClear={false}
            value={appliedRange}
            onChange={(value) => {
              if (value && value[0] && value[1]) setAppliedRange([value[0], value[1]]);
            }}
          />
          <Select
            mode="multiple"
            allowClear
            placeholder="동작 필터"
            style={{ minWidth: 220 }}
            maxTagCount="responsive"
            value={selectedActions}
            onChange={setSelectedActions}
            options={actionOptions}
          />
          <Input.Search
            allowClear
            placeholder="수행자·대상·내용 검색"
            style={{ width: 260 }}
            value={keyword}
            onChange={(e) => setKeyword(e.target.value)}
          />
          <Button onClick={() => refetch()} loading={isFetching}>
            새로고침
          </Button>
        </Space>
        <Table<AuditView>
          rowKey="id"
          loading={isFetching}
          columns={columns}
          dataSource={filtered}
          size="small"
          pagination={{
            defaultPageSize: 10,
            showSizeChanger: true,
            pageSizeOptions: [10, 20, 50, 100, 200],
            showTotal: (total) => `전체 ${total}건`,
          }}
        />
      </Card>
    </div>
  );
}

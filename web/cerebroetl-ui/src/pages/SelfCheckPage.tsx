import { useQuery } from "@tanstack/react-query";
import { Alert, Card, Empty, Space, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { getSelfCheck, type CollectorStatus } from "../api/selfcheck";

// 여기서는 상태색(정상/지연/중단)을 그대로 쓴다 — 이 화면 자체가 수집기 '상태' 판정이다.
const STATUS_TAG: Record<string, { color: string; text: string }> = {
  UP: { color: "success", text: "정상" },
  PENDING: { color: "default", text: "대기" },
  LATE: { color: "warning", text: "지연" },
  STALE: { color: "warning", text: "지연" },
  DOWN: { color: "error", text: "중단" },
};

function statusTag(status: string) {
  const t = STATUS_TAG[status] ?? { color: "default", text: status };
  return <Tag color={t.color}>{t.text}</Tag>;
}

function formatAge(sec: number | null): string {
  if (sec == null) return "-";
  if (sec < 60) return `${sec}초 전`;
  const m = Math.floor(sec / 60);
  if (m < 60) return `${m}분 전`;
  return `${Math.floor(m / 60)}시간 ${m % 60}분 전`;
}

const columns: ColumnsType<CollectorStatus> = [
  { title: "수집기", dataIndex: "label", width: 180 },
  { title: "지표원", dataIndex: "metricSource", width: 100, render: (v: string) => <Tag>{v}</Tag> },
  { title: "상태", dataIndex: "status", width: 90, render: (v: string) => statusTag(v) },
  { title: "마지막 완주", dataIndex: "ageSeconds", width: 130, render: (v: number | null) => formatAge(v) },
  {
    title: "주기(기대/관측)",
    width: 140,
    render: (_: unknown, r) =>
      `${r.expectedIntervalSeconds}s / ${r.observedIntervalSeconds ?? "-"}${r.observedIntervalSeconds ? "s" : ""}`,
  },
  { title: "결과", dataIndex: "lastResult", width: 90, render: (v: string | null) => v ?? "-" },
  {
    title: "오류",
    dataIndex: "lastError",
    ellipsis: true,
    render: (v: string | null) => (v ? <span style={{ color: "#cf1322" }}>{v}</span> : "-"),
  },
];

export function SelfCheckPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["self-check"],
    queryFn: getSelfCheck,
    refetchInterval: 15000,
    placeholderData: (prev) => prev,
  });

  const collectors = data?.collectors ?? [];
  const down = collectors.filter((c) => c.status === "DOWN").length;
  const late = collectors.filter((c) => c.status === "LATE" || c.status === "STALE").length;
  const skew = data?.clockSkewSeconds;

  return (
    <div style={{ padding: 16 }}>
      <header style={{ marginBottom: 12 }}>
        <h2 style={{ margin: 0 }}>자가진단</h2>
        <p style={{ margin: "4px 0 0", color: "#888" }}>
          수집기(신호원)별 완주 상태를 15초마다 점검합니다. 판정이 아니라 "판정기 자체가 살아있는가"를 봅니다.
        </p>
      </header>

      <Space size="middle" wrap style={{ marginBottom: 12 }}>
        <Tag color="success">정상 {collectors.length - down - late}</Tag>
        <Tag color="warning">지연 {late}</Tag>
        <Tag color="error">중단 {down}</Tag>
        {typeof skew === "number" && (
          <span style={{ color: "#888" }}>시계 오차 {skew}s</span>
        )}
      </Space>

      {skew != null && Math.abs(skew) > 30 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message={`앱↔DB 시계 오차가 ${skew}s 입니다. 지속 시간 계산이 왜곡될 수 있습니다.`}
        />
      )}

      <Card loading={isLoading && !data} styles={{ body: { padding: collectors.length ? 0 : 24 } }}>
        {collectors.length ? (
          <Table<CollectorStatus>
            rowKey="key"
            dataSource={collectors}
            columns={columns}
            pagination={false}
            size="middle"
          />
        ) : (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="등록된 수집기가 없습니다" />
        )}
      </Card>
    </div>
  );
}

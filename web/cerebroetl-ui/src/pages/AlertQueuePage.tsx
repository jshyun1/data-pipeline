import { useQuery } from "@tanstack/react-query";
import { Card, Empty, Space, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { getAlertQueue, type QueueItem } from "../api/alerts";

// 상태색(정상/경고/중단)은 예약색. 심각도만 색으로 표현한다.
const SEVERITY_TAG: Record<string, { color: string; icon: string }> = {
  CRITICAL: { color: "error", icon: "🔴" },
  WARNING: { color: "warning", icon: "🟡" },
  INFO: { color: "default", icon: "⚪" },
};

function formatDuration(sec: number | null): string {
  if (sec == null) return "-";
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return m > 0 ? `${m}분 ${s}초째` : `${s}초째`;
}

const columns: ColumnsType<QueueItem> = [
  {
    title: "심각도",
    dataIndex: "severity",
    width: 96,
    render: (severity: string) => {
      const t = SEVERITY_TAG[severity] ?? SEVERITY_TAG.INFO;
      return (
        <Tag color={t.color}>
          {t.icon} {severity}
        </Tag>
      );
    },
  },
  { title: "대상", dataIndex: "target_label", width: 140, render: (v: string | null) => v ?? "-" },
  { title: "내용", dataIndex: "summary", ellipsis: true },
  { title: "상태", dataIndex: "state", width: 100 },
  {
    title: "지속",
    dataIndex: "duration_seconds",
    width: 120,
    render: (v: number | null) => formatDuration(v),
  },
  {
    title: "확인",
    dataIndex: "acked",
    width: 72,
    render: (acked: boolean) => (acked ? <Tag color="processing">확인됨</Tag> : "-"),
  },
];

export function AlertQueuePage() {
  // 화면은 20초마다 이 한 엔드포인트만 호출한다(설계서 5-2 ①).
  const { data, isLoading } = useQuery({
    queryKey: ["alert-queue"],
    queryFn: () => getAlertQueue(50),
    refetchInterval: 20000,
    placeholderData: (previous) => previous,
  });

  const counts = data?.counts;
  const items = data?.items ?? [];

  return (
    <div style={{ padding: 16 }}>
      <header style={{ marginBottom: 12 }}>
        <h2 style={{ margin: 0 }}>조치 대기열</h2>
        <p style={{ margin: "4px 0 0", color: "#888" }}>판정된 장애를 심각도순으로 모아 바로 조치합니다. 20초마다 갱신.</p>
      </header>

      <Space size="middle" wrap style={{ marginBottom: 12 }}>
        <Tag color="error">🔴 위험 {counts?.critical ?? 0}</Tag>
        <Tag color="warning">🟡 경고 {counts?.warning ?? 0}</Tag>
        <Tag>⚪ 판단불가 {counts?.unknown ?? 0}</Tag>
        <span style={{ color: "#888" }}>
          확인됨 {counts?.acked ?? 0} · 스누즈 {counts?.snoozed ?? 0} · 억제 {counts?.suppressed ?? 0}
        </span>
      </Space>

      <Card loading={isLoading && !data} styles={{ body: { padding: items.length ? 0 : 24 } }}>
        {items.length ? (
          <Table<QueueItem>
            rowKey="id"
            dataSource={items}
            columns={columns}
            pagination={false}
            size="middle"
          />
        ) : (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="조치가 필요한 알림이 없습니다" />
        )}
      </Card>
    </div>
  );
}

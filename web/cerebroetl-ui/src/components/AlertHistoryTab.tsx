import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Button, DatePicker, Input, Segmented, Select, Space, Spin, Table, Tag, Timeline } from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs, { type Dayjs } from "dayjs";

const { RangePicker } = DatePicker;
import {
  getAlertEvents,
  getAlertHistory,
  type HistoryFilter,
  type HistoryItem,
  type SeverityFilter,
} from "../api/alerts";

// 알림 이력(원본 5-7). 대기열은 "지금 열린 것"만 보여줘서, 해소된 알림과 "누가 언제 확인했는지"가
// 화면에 남지 않았다. ETL/CDC 로그처럼 조회조건 + 페이징으로 넓은 이력을 훑는다.

const SEVERITY_META: Record<string, { color: string; label: string }> = {
  CRITICAL: { color: "error", label: "🔴 위험" },
  WARNING: { color: "warning", label: "🟡 경고" },
  INFO: { color: "default", label: "⚪ 정보" },
};

const RULE_LINK: Record<string, { path: string; label: string }> = {
  DATA_FRESHNESS: { path: "/cdc/logs", label: "CDC 처리 로그" },
  CDC_LAG: { path: "/cdc/logs", label: "CDC 처리 로그" },
  JOB_FAILURE: { path: "/airflow/dashboard", label: "Airflow 실행 이력" },
  JOB_CONSECUTIVE_FAILURE: { path: "/etl/logs", label: "ETL 로그" },
  JOB_NOT_RUN: { path: "/airflow/dashboard", label: "Airflow 실행 이력" },
  SERVER_MEMORY: { path: "/dashboard#infra", label: "시스템 상태" },
  SERVER_DISK: { path: "/dashboard#infra", label: "시스템 상태" },
  CONNECTOR_FAILED: { path: "/cdc/pipelines", label: "CDC 파이프라인" },
  SERVICE_UNREACHABLE: { path: "/self-check", label: "자가진단" },
  COLLECTOR_DOWN: { path: "/self-check", label: "자가진단" },
};

function formatWhen(iso: string | null): string {
  if (!iso) return "-";
  // 서버는 UTC(+00:00)로 내려주므로, 문자열을 그냥 자르지 말고 지역시각(KST)으로 변환해 표시한다.
  const d = dayjs(iso);
  return d.isValid() ? d.format("YYYY-MM-DD HH:mm") : iso;
}

// 상세 타임라인·정보를 한글로. 코드값이 그대로 노출되면 무슨 이력인지 알기 어렵다.
const EVENT_LABEL: Record<string, string> = {
  CREATED: "감지 · 알림 생성",
  FIRED: "발화 · 알림 시작",
  NOTIFIED: "발송 · 알림 전송",
  RECURRED: "재발생 · 다시 알림",
  ACKED: "확인",
  UNACKED: "확인 취소",
  RESOLVED: "해소 · 조건 해제",
};
const STATE_LABEL: Record<string, string> = {
  PENDING: "대기",
  FIRING: "발생 중",
  RESOLVED: "해소",
  UNKNOWN: "판단 불가",
};
const RULE_TYPE_LABEL: Record<string, string> = {
  JOB_FAILURE: "ETL Job 실패",
  JOB_CONSECUTIVE_FAILURE: "ETL Job 연속 실패",
  JOB_NOT_RUN: "ETL Job 장기 미실행",
  DATA_FRESHNESS: "적재 정체(신선도)",
  CDC_LAG: "CDC 지연",
  CONNECTOR_FAILED: "커넥터 실패",
  SERVICE_UNREACHABLE: "서비스 응답없음",
  SERVER_MEMORY: "서버 메모리 압박",
  SERVER_DISK: "서버 디스크 부족",
  COLLECTOR_DOWN: "수집기 중단",
};
const label = (map: Record<string, string>, k: string | null) => (k ? map[k] ?? k : "-");

function EventTimeline({ id }: { id: number }) {
  const { data, isLoading } = useQuery({
    queryKey: ["alert-events", id],
    queryFn: () => getAlertEvents(id),
  });
  if (isLoading) return <Spin size="small" />;
  const events = data ?? [];
  if (events.length === 0) return <span style={{ color: "#888" }}>기록된 전이가 없습니다.</span>;
  return (
    <Timeline
      items={events.map((e) => ({
        children: (
          <span>
            {formatWhen(e.occurred_at)} · <strong>{label(EVENT_LABEL, e.event_type)}</strong>
            {e.from_state && e.to_state ? ` (${label(STATE_LABEL, e.from_state)} → ${label(STATE_LABEL, e.to_state)})` : ""}
            {e.actor && e.actor !== "SYSTEM" ? ` · ${e.actor}` : e.actor === "SYSTEM" ? " · 자동" : ""}
          </span>
        ),
      }))}
    />
  );
}

export function AlertHistoryTab() {
  const navigate = useNavigate();
  const [filter, setFilter] = useState<HistoryFilter>("all");
  const [severity, setSeverity] = useState<SeverityFilter>("ALL");
  const [q, setQ] = useState("");
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>([
    dayjs().startOf("day"),
    dayjs().endOf("day"),
  ]);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(10);

  const from = appliedRange[0].toISOString();
  const to = appliedRange[1].toISOString();

  const { data, isLoading } = useQuery({
    queryKey: ["alert-history", filter, severity, q, from, to, page, pageSize],
    queryFn: () => getAlertHistory({ filter, severity, q, from, to, page, pageSize }),
    placeholderData: (prev) => prev,
  });

  // 전일자/당일 프리셋: 해당 날짜 하루로 조회기간을 즉시 맞추고 1페이지로.
  const applyDayPreset = (offsetDays: 0 | 1) => {
    const day = dayjs().subtract(offsetDays, "day");
    setAppliedRange([day.startOf("day"), day.endOf("day")]);
    setPage(0);
  };

  const items = data?.items ?? [];
  const counts = data?.counts;
  const total = data?.total ?? 0;

  // 조회조건이 바뀌면 항상 1페이지로.
  function resetTo<T>(setter: (v: T) => void) {
    return (v: T) => {
      setter(v);
      setPage(0);
    };
  }

  const columns: ColumnsType<HistoryItem> = [
    {
      title: "발생시각",
      width: 140,
      render: (_: unknown, r) => formatWhen(r.started_at ?? r.condition_since),
    },
    {
      title: "심각도",
      dataIndex: "severity",
      width: 96,
      render: (v: string) => {
        const m = SEVERITY_META[v] ?? SEVERITY_META.INFO;
        return <Tag color={m.color}>{m.label}</Tag>;
      },
    },
    { title: "대상", dataIndex: "target_label", width: 150, render: (v: string | null) => v ?? "-" },
    { title: "내용", dataIndex: "summary", ellipsis: true },
    {
      title: "상태",
      width: 150,
      render: (_: unknown, r) => (
        <Space size={4}>
          {r.acked ? <Tag color="processing">확인됨</Tag> : <Tag>미확인</Tag>}
          {r.closed ? <Tag color="success">해소</Tag> : <Tag color="warning">진행 중</Tag>}
        </Space>
      ),
    },
    {
      title: "확인자",
      width: 130,
      render: (_: unknown, r) =>
        r.acked ? (
          <span>
            {r.ack_by ?? "-"}
            <br />
            <span style={{ color: "#888", fontSize: 12 }}>{formatWhen(r.ack_at)}</span>
          </span>
        ) : (
          "-"
        ),
    },
  ];

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <Space wrap size="middle">
        <Space.Compact>
          <Button size="small" onClick={() => applyDayPreset(1)}>전일자</Button>
          <Button size="small" onClick={() => applyDayPreset(0)}>당일</Button>
        </Space.Compact>
        <RangePicker
          size="small"
          allowClear={false}
          value={appliedRange}
          onChange={(value) => {
            if (value && value[0] && value[1]) {
              setAppliedRange([value[0], value[1]]);
              setPage(0);
            }
          }}
        />
        <span>
          심각도{" "}
          <Select
            size="small"
            value={severity}
            style={{ width: 110 }}
            onChange={resetTo(setSeverity)}
            options={[
              { label: "전체", value: "ALL" },
              { label: "🔴 위험", value: "CRITICAL" },
              { label: "🟡 경고", value: "WARNING" },
              { label: "⚪ 정보", value: "INFO" },
            ]}
          />
        </span>
        <Segmented
          size="small"
          value={filter}
          onChange={(v) => resetTo(setFilter)(v as HistoryFilter)}
          options={[
            { label: `전체 ${counts?.total ?? 0}`, value: "all" },
            { label: `미확인 ${counts?.unacked ?? 0}`, value: "unacked" },
            { label: `확인됨 ${counts?.acked ?? 0}`, value: "acked" },
          ]}
        />
        <Input.Search
          size="small"
          allowClear
          placeholder="내용·대상 검색"
          style={{ width: 220 }}
          onSearch={(v) => resetTo(setQ)(v)}
        />
      </Space>

      <Table<HistoryItem>
        rowKey="id"
        size="middle"
        loading={isLoading && !data}
        dataSource={items}
        columns={columns}
        pagination={{
          current: page + 1,
          pageSize,
          total,
          showSizeChanger: true,
          pageSizeOptions: [10, 20, 50, 100],
          showTotal: (t) => `총 ${t}건`,
          onChange: (p, ps) => {
            setPage(p - 1);
            setPageSize(ps);
          },
        }}
        expandable={{
          expandedRowRender: (r) => (
            <div style={{ padding: "4px 8px" }}>
              <div style={{ marginBottom: 10, fontSize: 13, color: "#334155", lineHeight: 1.7 }}>
                <div style={{ fontWeight: 700, color: "#172033" }}>{r.summary}</div>
                <div>
                  대상: {r.target_label ?? "-"} · 유형: {label(RULE_TYPE_LABEL, r.rule_type_code)}
                  {r.observed_value != null ? ` · 관측값 ${r.observed_value.toLocaleString()}` : ""}
                  {r.threshold_value != null ? ` / 임계 ${r.threshold_value.toLocaleString()}` : ""}
                </div>
                <div>
                  상태: {r.closed ? "해소됨" : "진행 중"}
                  {r.acked ? ` · 확인: ${r.ack_by ?? "-"}${r.ack_at ? ` (${formatWhen(r.ack_at)})` : ""}` : " · 미확인"}
                  {r.resolve_reason ? ` · 해소사유: ${r.resolve_reason === "CONDITION_CLEARED" ? "조건 해제" : r.resolve_reason}` : ""}
                </div>
              </div>
              <EventTimeline id={r.id} />
              {(() => {
                const link = r.deep_link?.startsWith("/")
                  ? { path: r.deep_link, label: "관련 화면으로 이동" }
                  : RULE_LINK[r.rule_type_code];
                return link ? (
                  <Button type="link" size="small" style={{ padding: 0 }} onClick={() => navigate(link.path)}>
                    {link.label}
                  </Button>
                ) : null;
              })()}
            </div>
          ),
        }}
      />
    </Space>
  );
}

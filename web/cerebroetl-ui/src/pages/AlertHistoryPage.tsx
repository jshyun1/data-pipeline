import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, Empty, Segmented, Space, Spin, Tag, Timeline } from "antd";
import {
  getAlertEvents,
  getAlertHistory,
  type HistoryFilter,
  type HistoryItem,
} from "../api/alerts";

/**
 * 알림 이력 — 원본 문서 5-7 / PDF 8쪽.
 *
 * <p>조치 대기열은 "지금 열려 있는 것"만 보여준다. 그래서 해소된 알림과 "누가 언제 확인했는지"가
 * 화면 어디에도 남지 않았다. 이 화면이 그 질문에 답한다.
 */

const SEVERITY_META: Record<string, { icon: string; color: string; label: string }> = {
  CRITICAL: { icon: "🔴", color: "error", label: "위험" },
  WARNING: { icon: "🟡", color: "warning", label: "경고" },
  INFO: { icon: "⚪", color: "default", label: "정보" },
};

function formatWhen(iso: string | null): string {
  if (!iso) return "";
  return iso.replace("T", " ").slice(5, 16);
}

/** 규칙 코드로 관련 화면을 정한다. 대기열 패널과 같은 규약을 쓴다. */
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

function EventTimeline({ id }: { id: number }) {
  const { data, isLoading } = useQuery({
    queryKey: ["alert-events", id],
    queryFn: () => getAlertEvents(id),
  });
  if (isLoading) return <Spin size="small" />;
  const events = data ?? [];
  if (events.length === 0) return <span className="alert-history-note">기록된 전이가 없습니다.</span>;
  return (
    <Timeline
      className="alert-history-timeline"
      items={events.map((e) => ({
        children: (
          <span>
            {formatWhen(e.occurred_at)} · <strong>{e.event_type}</strong>
            {e.from_state ? ` (${e.from_state} → ${e.to_state})` : ""}
            {e.actor && e.actor !== "SYSTEM" ? ` · ${e.actor}` : ""}
          </span>
        ),
      }))}
    />
  );
}

function HistoryRow({ item }: { item: HistoryItem }) {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const meta = SEVERITY_META[item.severity] ?? SEVERITY_META.INFO;
  const link = item.deep_link?.startsWith("/")
    ? { path: item.deep_link, label: "이동" }
    : RULE_LINK[item.rule_type_code];

  return (
    <li className={`alert-history-item ${item.closed ? "is-closed" : ""}`}>
      <div className="alert-history-line">
        <span className="alert-history-icon">{meta.icon}</span>
        <span className="alert-history-when">{formatWhen(item.started_at ?? item.condition_since)}</span>
        <span className="alert-history-summary">{item.summary}</span>
        <span className="alert-history-status">
          {item.acked ? <Tag color="processing">확인됨</Tag> : <Tag>미확인</Tag>}
          {item.closed ? <Tag color="success">해소</Tag> : <Tag color={meta.color}>진행 중</Tag>}
        </span>
      </div>
      <div className="alert-history-sub">
        └{" "}
        {item.acked ? (
          <span>
            확인: <strong>{item.ack_by ?? "-"}</strong> · {formatWhen(item.ack_at)}
            {item.ack_comment ? ` · ${item.ack_comment}` : ""}
          </span>
        ) : (
          <span>아직 확인되지 않았습니다</span>
        )}
        {link ? (
          <>
            {" · 관련: "}
            <Button type="link" size="small" style={{ padding: 0 }} onClick={() => navigate(link.path)}>
              {link.label}
            </Button>
          </>
        ) : null}
        <Button type="link" size="small" style={{ padding: 0 }} onClick={() => setOpen(!open)}>
          {open ? "이력 접기" : "이력 보기"}
        </Button>
      </div>
      {open ? (
        <div className="alert-history-detail">
          <EventTimeline id={item.id} />
        </div>
      ) : null}
    </li>
  );
}

export function AlertHistoryPage() {
  const [filter, setFilter] = useState<HistoryFilter>("all");
  const [days, setDays] = useState(7);

  const { data, isLoading, isError } = useQuery({
    queryKey: ["alert-history", filter, days],
    queryFn: () => getAlertHistory(filter, days),
    refetchInterval: 60000,
    placeholderData: (prev) => prev,
  });

  const items = data?.items ?? [];
  const counts = data?.counts;

  return (
    <div style={{ padding: 16 }}>
      <header className="alert-history-head">
        <div>
          <h2 style={{ margin: 0 }}>알림</h2>
          <p style={{ margin: "4px 0 0", color: "#888" }}>
            해소된 알림까지 포함한 이력입니다. 누가 언제 확인했는지 남습니다.
          </p>
        </div>
        <Space size="small" wrap>
          <Segmented
            size="small"
            value={days}
            options={[
              { label: "1일", value: 1 },
              { label: "7일", value: 7 },
              { label: "30일", value: 30 },
            ]}
            onChange={(v) => setDays(Number(v))}
          />
          <Segmented
            value={filter}
            options={[
              { label: `전체 ${counts?.total ?? 0}`, value: "all" },
              { label: `미확인 ${counts?.unacked ?? 0}`, value: "unacked" },
              { label: `확인됨 ${counts?.acked ?? 0}`, value: "acked" },
            ]}
            onChange={(v) => setFilter(v as HistoryFilter)}
          />
        </Space>
      </header>

      <Card loading={isLoading && !data}>
        {isError ? (
          <div className="alert-history-note">이력을 불러오지 못했습니다. 알림이 없다는 뜻은 아닙니다.</div>
        ) : items.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="해당 조건의 알림이 없습니다" />
        ) : (
          <ul className="alert-history-list">
            {items.map((item) => (
              <HistoryRow key={item.id} item={item} />
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}

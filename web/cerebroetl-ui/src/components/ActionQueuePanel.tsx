import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Dropdown, Empty, Spin, Tag, message } from "antd";
import { ackAlert, getAlertQueue, snoozeAlert, type QueueItem } from "../api/alerts";

/**
 * 조치 대기열 — 원본 문서 5-1. 대시보드 최상단(KPI 카드보다 위)에 놓인다.
 *
 * <p>원본이 최우선으로 지목한 항목이고, 요구는 "화면을 보고 무엇을 해야 하는지가 나오게 한다"였다.
 * 그래서 세 가지를 그대로 지킨다 — 정상이면 한 줄로 접히고, 이상이면 자동으로 펼쳐지며,
 * 각 항목은 반드시 해당 화면으로 이동한다(딥링크 없으면 대기열의 의미가 사라진다).
 */

const SEVERITY_META: Record<string, { icon: string; color: string; order: number; label: string }> = {
  CRITICAL: { icon: "🔴", color: "error", order: 0, label: "위험" },
  WARNING: { icon: "🟡", color: "warning", order: 1, label: "경고" },
  INFO: { icon: "⚪", color: "default", order: 2, label: "정보" },
};

/** 원본 5-1 "표시 건수: 상위 5건 + [모두 보기]". */
const VISIBLE_LIMIT = 5;

/** 원본 5-1 "스누즈: 1시간 / 4시간 / 오늘 하루". */
const SNOOZE_OPTIONS = [
  { key: "60", label: "1시간", minutes: 60 },
  { key: "240", label: "4시간", minutes: 240 },
  { key: "today", label: "오늘 하루", minutes: -1 },
];

function minutesUntilEndOfDay(): number {
  const now = new Date();
  const end = new Date(now);
  end.setHours(23, 59, 59, 999);
  return Math.max(1, Math.round((end.getTime() - now.getTime()) / 60000));
}

function formatDuration(sec: number | null): string {
  if (sec == null) return "";
  if (sec < 60) return `${sec}초째`;
  const m = Math.floor(sec / 60);
  if (m < 60) return `${m}분 ${sec % 60}초째`;
  const h = Math.floor(m / 60);
  return `${h}시간 ${m % 60}분째`;
}

/**
 * 서버가 내려준 deep_link 를 앱 내부 경로로 해석한다.
 * 해석할 수 없으면 null 을 돌려 "이동" 버튼 자체를 감춘다 — 눌러도 아무 데도 안 가는 버튼은
 * 없는 것만 못하다.
 */
/**
 * 원본 5-1 판정 규칙표의 "딥링크" 열을 그대로 옮긴 매핑.
 *
 * <p>서버가 deep_link 를 채워주면 그걸 먼저 쓰고, 비어 있으면 규칙 코드로 갈 곳을 정한다.
 * 부분문자열 추측 대신 실제 alert_rule.rule_type_code 를 명시적으로 적는다 —
 * 추측에 기대면 규칙이 하나 늘 때마다 조용히 버튼이 사라진다.
 */
const RULE_DEEP_LINK: Record<string, { path: string; label: string }> = {
  // 적재 N분간 0건 → CDC 처리 로그
  DATA_FRESHNESS: { path: "/cdc/logs", label: "처리 로그" },
  CDC_LAG: { path: "/cdc/logs", label: "처리 로그" },
  // DAG 실패 / 연속 실패 → Airflow 실행 이력
  JOB_FAILURE: { path: "/airflow/dashboard", label: "실행 이력" },
  // 리소스 임계 → 시스템 상태(대시보드 인프라 구역)
  SERVER_MEMORY: { path: "/dashboard#infra", label: "시스템 리소스" },
  SERVER_DISK: { path: "/dashboard#infra", label: "시스템 리소스" },
  // 수집기 중단(데드맨) → 자가진단
  COLLECTOR_DOWN: { path: "/self-check", label: "자가진단" },
};

function resolveDeepLink(item: QueueItem): { path: string; label: string } | null {
  const raw = item.deep_link?.trim();
  if (raw && raw.startsWith("/")) {
    return { path: raw, label: "이동" };
  }
  const code = (item.rule_type_code ?? "").toUpperCase();
  if (RULE_DEEP_LINK[code]) {
    return RULE_DEEP_LINK[code];
  }
  // 알려지지 않은 신규 규칙에 대한 최소 보루. 그래도 못 정하면 버튼을 감춘다 —
  // 눌러도 아무 데도 안 가는 버튼은 없는 것만 못하다.
  if (code.includes("CDC") || code.includes("LAG") || code.includes("FRESH")) {
    return { path: "/cdc/logs", label: "처리 로그" };
  }
  if (code.includes("ETL") || code.includes("NIFI")) return { path: "/etl/logs", label: "ETL 로그" };
  if (code.includes("JOB") || code.includes("DAG") || code.includes("AIRFLOW")) {
    return { path: "/airflow/dashboard", label: "실행 이력" };
  }
  if (code.includes("SERVER") || code.includes("RESOURCE") || code.includes("MEMORY") || code.includes("DISK")) {
    return { path: "/dashboard#infra", label: "시스템 리소스" };
  }
  if (code.includes("COLLECTOR") || code.includes("HEARTBEAT")) return { path: "/self-check", label: "자가진단" };
  return null;
}

export function ActionQueuePanel() {
  const navigate = useNavigate();
  const qc = useQueryClient();
  // 원본 5-1 "화면 갱신 30초". 대기열은 이 한 엔드포인트만 호출한다.
  const { data, isLoading, isError } = useQuery({
    queryKey: ["alert-queue", "dashboard"],
    queryFn: () => getAlertQueue(50),
    refetchInterval: 30000,
    placeholderData: (prev) => prev,
  });
  const [manuallyOpen, setManuallyOpen] = useState<boolean | null>(null);

  const counts = data?.counts;
  const items = data?.items ?? [];

  // 원본 5-1 "정렬: 위험 → 경고 → 정보, 각 그룹 내 최신순".
  // 서버도 같은 순서로 내리지만, 확인된 항목을 뒤로 미는 건 화면 몫이다.
  const sorted = useMemo(() => {
    return [...items].sort((a, b) => {
      if (a.acked !== b.acked) return a.acked ? 1 : -1;
      const oa = SEVERITY_META[a.severity]?.order ?? 9;
      const ob = SEVERITY_META[b.severity]?.order ?? 9;
      if (oa !== ob) return oa - ob;
      return (b.duration_seconds ?? 0) - (a.duration_seconds ?? 0);
    });
  }, [items]);

  const actionable = sorted.filter((i) => !i.acked);
  // 원본 5-1 "기본 상태: 정상 시 접힘 / 이상 발생 시 자동 펼침".
  const autoOpen = actionable.length > 0;
  const open = manuallyOpen ?? autoOpen;
  const visible = sorted.slice(0, VISIBLE_LIMIT);

  async function handleAck(item: QueueItem) {
    try {
      await ackAlert(item.id);
      message.success("확인 처리했습니다");
      qc.invalidateQueries({ queryKey: ["alert-queue"] });
    } catch {
      message.error("확인 처리에 실패했습니다");
    }
  }

  async function handleSnooze(item: QueueItem, minutes: number) {
    try {
      await snoozeAlert(item.id, minutes < 0 ? minutesUntilEndOfDay() : minutes);
      message.success("스누즈했습니다");
      qc.invalidateQueries({ queryKey: ["alert-queue"] });
    } catch {
      message.error("스누즈에 실패했습니다");
    }
  }

  // 원본과 별개로 지키는 것: 백엔드가 죽었을 때 빈 화면(=정상처럼 보임)을 만들지 않는다.
  if (isError) {
    return (
      <section className="action-queue action-queue--error">
        <div className="action-queue-head">
          <span className="action-queue-title">⚠ 조치 대기열을 불러오지 못했습니다</span>
          <span className="action-queue-hint">판정 결과를 알 수 없는 상태입니다 — 정상이라는 뜻이 아닙니다.</span>
        </div>
      </section>
    );
  }

  if (isLoading && !data) {
    return (
      <section className="action-queue">
        <div className="action-queue-head">
          <Spin size="small" /> <span className="action-queue-title">조치 대기열 확인 중…</span>
        </div>
      </section>
    );
  }

  return (
    <section className={`action-queue ${open ? "is-open" : "is-collapsed"}`} id="action-queue">
      <div className="action-queue-head">
        <div className="action-queue-summary">
          {actionable.length === 0 ? (
            <span className="action-queue-ok">✅ 조치 필요 항목 없음</span>
          ) : (
            <>
              <Tag color="error">🔴 위험 {counts?.critical ?? 0}</Tag>
              <Tag color="warning">🟡 경고 {counts?.warning ?? 0}</Tag>
              <Tag>⚪ 정보 {counts?.info ?? 0}</Tag>
            </>
          )}
          <span className="action-queue-hint">
            확인됨 {counts?.acked ?? 0} · 스누즈 {counts?.snoozed ?? 0} · 억제 {counts?.suppressed ?? 0}
          </span>
        </div>
        <div className="action-queue-actions">
          <Button size="small" type="link" onClick={() => setManuallyOpen(!open)}>
            {open ? "접기" : "펼치기"}
          </Button>
          <Button size="small" type="link" onClick={() => navigate("/alerts")}>
            모두 보기
          </Button>
        </div>
      </div>

      {open ? (
        <div className="action-queue-body">
          {visible.length === 0 ? (
            <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="조치가 필요한 알림이 없습니다" />
          ) : (
            <ul className="action-queue-list">
              {visible.map((item) => {
                const meta = SEVERITY_META[item.severity] ?? SEVERITY_META.INFO;
                const link = resolveDeepLink(item);
                return (
                  <li key={item.id} className={`action-queue-item ${item.acked ? "is-acked" : ""}`}>
                    <span className="action-queue-icon" aria-label={meta.label}>
                      {meta.icon}
                    </span>
                    <div className="action-queue-text">
                      <div className="action-queue-line">
                        <span className="action-queue-axis">[{item.kpi_axis === "DAILY" ? "일집계" : "현재"}]</span>
                        <span className="action-queue-summary-text">{item.summary}</span>
                      </div>
                      <div className="action-queue-meta">
                        {item.target_label ? <span>{item.target_label}</span> : null}
                        <span>{formatDuration(item.duration_seconds)}</span>
                        {item.acked ? <Tag color="processing">확인됨</Tag> : null}
                      </div>
                    </div>
                    <div className="action-queue-buttons">
                      {link ? (
                        <Button size="small" onClick={() => navigate(link.path)}>
                          {link.label}
                        </Button>
                      ) : null}
                      <Button size="small" disabled={item.acked} onClick={() => handleAck(item)}>
                        확인
                      </Button>
                      <Dropdown
                        menu={{
                          items: SNOOZE_OPTIONS.map((o) => ({ key: o.key, label: o.label })),
                          onClick: ({ key }) => {
                            const opt = SNOOZE_OPTIONS.find((o) => o.key === key);
                            if (opt) handleSnooze(item, opt.minutes);
                          },
                        }}
                      >
                        <Button size="small">스누즈 ▾</Button>
                      </Dropdown>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
          {data && data.totalOpen > VISIBLE_LIMIT ? (
            <div className="action-queue-more">
              상위 {VISIBLE_LIMIT}건만 표시 · 전체 {data.totalOpen}건
              <Button type="link" size="small" onClick={() => navigate("/alerts")}>
                모두 보기
              </Button>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

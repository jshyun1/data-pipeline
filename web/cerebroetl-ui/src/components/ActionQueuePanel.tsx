import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Empty, Modal, Spin, message } from "antd";
import { ackAlert, getAlertQueue, type QueueItem } from "../api/alerts";

/**
 * 조치 대기열 — 원본 문서 5-1. 대시보드 머리줄(제목 오른쪽 도구 모음)에 놓인다.
 *
 * <p>원본의 요구는 "화면을 보고 무엇을 해야 하는지가 나오게 한다"였다. 디자인 초안(index.html)을
 * 따라 페이지에 목록을 펼치던 패널을 «위험·경고·정보 건수 알약» 한 줄로 접었다 - 이상이 있으면
 * 빨간 알약이 머리줄에서 바로 보이고, 누르면 전체 목록 팝업이 뜬다. 팝업의 각 항목은 반드시
 * 해당 화면으로 이동한다(딥링크 없으면 대기열의 의미가 사라진다).
 */

const SEVERITY_META: Record<string, { tone: "danger" | "warning" | "info"; order: number; label: string }> = {
  CRITICAL: { tone: "danger", order: 0, label: "위험" },
  WARNING: { tone: "warning", order: 1, label: "경고" },
  INFO: { tone: "info", order: 2, label: "정보" },
};

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
  DATA_FRESHNESS: { path: "/cdc/logs", label: "로그" },
  CDC_LAG: { path: "/cdc/logs", label: "로그" },
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
    return { path: "/cdc/logs", label: "로그" };
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
  const [showAll, setShowAll] = useState(false);

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
      // 원본 5-1 "그룹 내 최신순": 지속시간이 짧을수록(=최근 발생) 위로.
      return (a.duration_seconds ?? 0) - (b.duration_seconds ?? 0);
    });
  }, [items]);

  const actionable = sorted.filter((i) => !i.acked);
  const critical = counts?.critical ?? 0;
  const warning = counts?.warning ?? 0;
  const info = counts?.info ?? 0;
  const handledSummary = `확인됨 ${counts?.acked ?? 0} · 억제 ${counts?.suppressed ?? 0}`;

  async function handleAck(item: QueueItem) {
    try {
      await ackAlert(item.id);
      message.success("확인 처리했습니다");
      qc.invalidateQueries({ queryKey: ["alert-queue"] });
    } catch {
      message.error("확인 처리에 실패했습니다");
    }
  }

  const renderItem = (item: QueueItem) => {
    const meta = SEVERITY_META[item.severity] ?? SEVERITY_META.INFO;
    const link = resolveDeepLink(item);
    return (
      <li key={item.id} className={`alert-item-row${item.acked ? " is-acked" : ""}`}>
        <div className="alert-item-left">
          <span className={`alert-dot ${meta.tone}`} aria-hidden="true" />
          <div className="alert-item-content">
            <span className="alert-item-title">
              <span className="tag-status-bracket">[{item.kpi_axis === "DAILY" ? "일집계" : "현재"}]</span>{" "}
              {item.summary}
            </span>
            <div className="alert-item-sub">
              {/* 중요도는 색만으로는 «어느 정도인지»가 안 읽힌다. 외부 발송이 위험만
                  나가도록 바뀌어(2026-09-02) 경고·정보는 여기서만 보이므로 글자로도 적는다. */}
              <span className={`alert-type-text ${meta.tone}`}>{meta.label}</span>
              {item.target_label ? <span>{item.target_label}</span> : null}
              <span>{formatDuration(item.duration_seconds)}</span>
              {item.acked ? <span className="alert-acked-chip">확인됨</span> : null}
            </div>
          </div>
        </div>
        <div className="alert-item-actions">
          {link ? (
            <button type="button" className="btn-alert-action" onClick={() => navigate(link.path)}>
              {link.label}
            </button>
          ) : null}
          <button type="button" className="btn-alert-action" disabled={item.acked} onClick={() => handleAck(item)}>
            확인
          </button>
        </div>
      </li>
    );
  };

  // 원본과 별개로 지키는 것: 백엔드가 죽었을 때 빈 화면(=정상처럼 보임)을 만들지 않는다.
  if (isError) {
    return (
      <div className="alert-compact-trigger alert-compact-trigger--status alert-compact-trigger--error" id="action-queue" role="status">
        <span className="alert-compact-title">⚠ 조치 대기열을 불러오지 못했습니다</span>
        <span className="alert-compact-hint">판정 결과를 알 수 없는 상태입니다 — 정상이라는 뜻이 아닙니다.</span>
      </div>
    );
  }

  if (isLoading && !data) {
    return (
      <div className="alert-compact-trigger alert-compact-trigger--status" id="action-queue" role="status">
        <Spin size="small" /> <span className="alert-compact-title">조치 대기열 확인 중…</span>
      </div>
    );
  }

  return (
    <>
      <button
        type="button"
        className={`alert-compact-trigger${actionable.length === 0 ? " is-clear" : ""}`}
        id="action-queue"
        title={`조치 대기열 상세 보기 (${handledSummary})`}
        aria-haspopup="dialog"
        onClick={() => setShowAll(true)}
      >
        <span className="alert-pills-wrap">
          {actionable.length === 0 ? (
            <span className="pill-badge pill-ok">✓ 조치 필요 항목 없음</span>
          ) : (
            <>
              <span className={`pill-badge pill-danger${critical === 0 ? " is-zero" : ""}`}>
                <span className="pill-dot" />위험 {critical}
              </span>
              <span className={`pill-badge pill-warning${warning === 0 ? " is-zero" : ""}`}>
                <span className="pill-dot" />경고 {warning}
              </span>
              <span className={`pill-badge pill-info${info === 0 ? " is-zero" : ""}`}>
                <span className="pill-dot" />정보 {info}
              </span>
            </>
          )}
        </span>
        <span className="alert-view-btn">상세보기 ▾</span>
      </button>

      <Modal
        open={showAll}
        title={`조치 대기열 — 전체 ${sorted.length}건`}
        onCancel={() => setShowAll(false)}
        footer={null}
        width={760}
        rootClassName="cerebro-modal action-queue-modal"
      >
        <div className="action-queue-modal-meta">{handledSummary}</div>
        {sorted.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="조치가 필요한 알림이 없습니다" />
        ) : (
          <ul className="action-queue-list">{sorted.map(renderItem)}</ul>
        )}
      </Modal>
    </>
  );
}

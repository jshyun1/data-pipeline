import { Tooltip } from "antd";

/**
 * KPI 카드 구성요소 — 원본 문서 5-2.
 *
 * <p>원본이 지적한 "기준 시점 혼재"를 없애기 위한 두 장치가 핵심이다:
 * 카드 우상단의 기간 배지와, 카드 안의 `── 지금 ──` 구분선.
 * 이 둘이 있어야 "10.6만 건"이 기간 누적이고 "지연 78,546"이 현재값이라는 게 읽힌다.
 */

/** 누적형 지표의 기준 기간을 카드 우상단에 못박는다. */
export function TimeBadge({ label }: { label: string }) {
  return (
    <span className="kpi-time-badge" title="이 카드의 누적 지표가 기준으로 삼는 기간">
      ⟲ {label}
    </span>
  );
}

/** 누적형과 진행형 사이를 가르는 선. 이 아래는 전부 "지금" 값이다. */
export function NowDivider() {
  return (
    <div className="kpi-now-divider">
      <span>현재</span>
    </div>
  );
}

/**
 * 전일 대비 증감 (원본 5-2 "추이 부재 → 전일 대비 증감").
 * 비교 대상이 없으면 아무것도 그리지 않는다 — 0%로 표시하면 "변화 없음"이라는 거짓 정보가 된다.
 */
export function ChangeIndicator({ current, previous }: { current: number; previous: number | null }) {
  if (previous == null || previous <= 0) {
    return <span className="kpi-change kpi-change--none" title="비교할 이전 구간이 없습니다">비교 불가</span>;
  }
  const diff = ((current - previous) / previous) * 100;
  if (!Number.isFinite(diff)) return null;
  const up = diff >= 0;
  const cls = Math.abs(diff) < 0.05 ? "kpi-change--flat" : up ? "kpi-change--up" : "kpi-change--down";
  return (
    <span className={`kpi-change ${cls}`} title="직전 동일 길이 구간 대비">
      {Math.abs(diff) < 0.05 ? "—" : up ? "▲" : "▼"} {Math.abs(diff).toFixed(1)}%
    </span>
  );
}

export type FlowTone = "ok" | "warn" | "unknown";

/**
 * "처리율 0"에 맥락을 붙인다 — 원본 5-2의 핵심.
 *
 * <p>원본 표현 그대로: lastProgressAt 이 최근이고 Lag 0 이면 "원천 변경 없음(정상)",
 * lastProgressAt 이 N분 이상 과거면 "N분째 정지 — 확인 필요", 수집이 안 되면 "지표 수집 실패".
 * Debezium 이벤트 스킵 사고 당시 화면은 처리율 0에 전부 초록이었다.
 */
export function FlowStateNote({ tone, text }: { tone: FlowTone; text: string }) {
  return (
    <div className={`kpi-flow-note kpi-flow-note--${tone}`}>
      {tone === "warn" ? "⚠ " : tone === "unknown" ? "⚪ " : ""}
      {text}
    </div>
  );
}

/** 값과 라벨 한 줄. warn 이면 색을 바꿔서 스캔할 때 눈에 걸리게 한다. */
export function MetricRow({
  label,
  value,
  warn,
  hint,
}: {
  label: string;
  value: string;
  warn?: boolean;
  hint?: string;
}) {
  const body = (
    <div className={`kpi-metric-row ${warn ? "is-warn" : ""}`}>
      <span className="kpi-metric-label">{label}</span>
      <span className="kpi-metric-value">
        {value}
        {warn ? " ⚠" : ""}
      </span>
    </div>
  );
  return hint ? <Tooltip title={hint}>{body}</Tooltip> : body;
}

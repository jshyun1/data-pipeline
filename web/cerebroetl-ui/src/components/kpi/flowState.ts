import type { RealtimePipelineMetricResponse } from "../../api/dashboard";
import type { FlowTone } from "./kpiBits";

/** 원본 5-2 "처리율 0에 맥락 부여" 표를 그대로 옮긴 판정. */
export interface FlowState {
  tone: FlowTone;
  text: string;
}

/** 이 시간 넘게 진행이 없으면 "정지"로 본다. 원본의 N분을 10분으로 확정한다. */
const STALL_MINUTES = 10;

export function judgeCdcFlow(metrics: RealtimePipelineMetricResponse[]): FlowState {
  if (metrics.length === 0) {
    return { tone: "unknown", text: "관측 대상 파이프라인이 없습니다" };
  }
  // 하나라도 수집이 끊겼으면 "정상"이라고 말할 수 없다(모름을 정상으로 칠하지 않는다).
  if (metrics.some((m) => m.collectionStatus !== "COLLECTED")) {
    return { tone: "unknown", text: "지표 수집 실패 — 상태를 알 수 없습니다" };
  }
  const lag = metrics.reduce((s, m) => s + Number(m.consumerLag ?? 0), 0);
  const throughput = metrics.reduce((s, m) => s + Number(m.throughputPerSecond ?? 0), 0);

  const lastProgress = metrics
    .map((m) => (m.lastProgressAt ? new Date(m.lastProgressAt).getTime() : null))
    .filter((t): t is number => t != null && Number.isFinite(t));
  const newest = lastProgress.length ? Math.max(...lastProgress) : null;
  const idleMinutes = newest == null ? null : Math.floor((Date.now() - newest) / 60000);

  if (throughput > 0) {
    return { tone: "ok", text: `처리 중 · ${throughput.toFixed(1)} rows/s` };
  }
  // 여기부터는 전부 "처리율 0"이다. 원본이 요구한 세 갈래를 가른다.
  if (idleMinutes != null && idleMinutes >= STALL_MINUTES) {
    return { tone: "warn", text: `${idleMinutes}분째 정지 — 확인 필요` };
  }
  if (lag > 0) {
    return { tone: "warn", text: "미처리가 남았는데 처리율 0 — 확인 필요" };
  }
  return { tone: "ok", text: "원천 변경 없음 (정상)" };
}

/**
 * 해소 예상 시간 (원본 #4).
 * 미처리가 남았는데 처리율이 0이면 나눗셈이 무한대가 된다 — 그건 "해소 불가"로 못박는다.
 */
export function formatRecovery(metrics: RealtimePipelineMetricResponse[]): { text: string; warn: boolean } {
  const lag = metrics.reduce((s, m) => s + Number(m.consumerLag ?? 0), 0);
  if (lag <= 0) return { text: "해소할 지연 없음", warn: false };
  const throughput = metrics.reduce((s, m) => s + Number(m.throughputPerSecond ?? 0), 0);
  if (throughput <= 0) return { text: "해소 불가 (처리율 0)", warn: true };

  const seconds = metrics.reduce((max, m) => {
    const v = m.estimatedRecoverySeconds;
    return v != null && v >= 0 ? Math.max(max, Number(v)) : max;
  }, -1);
  const eff = seconds >= 0 ? seconds : Math.round(lag / throughput);
  if (eff < 60) return { text: `약 ${eff}초`, warn: false };
  if (eff < 3600) return { text: `약 ${Math.round(eff / 60)}분`, warn: false };
  return { text: `약 ${(eff / 3600).toFixed(1)}시간`, warn: true };
}

import { useEffect, useMemo, useRef, useState } from "react";
import { Card, Tag, Tooltip } from "antd";
import dayjs from "dayjs";
import type { ProcessGroup, ProcessItem, ProcessStatus } from "../api/infra";

/**
 * 주요 프로세스 현황 — 원본 문서 5-4.
 *
 * <p>원본 진단은 "구성은 양호하나 전부 초록이면 스캔하지 않게 된다"였다. 그래서 목록을
 * 갈아엎지 않고 네 가지만 바꾼다: 이상 항목을 위로 올리고, 정상은 접고, "미사용"을 장애와
 * 구분하고, "정상"의 판정 기준을 툴팁으로 밝힌다.
 */

const STATUS_LABEL: Record<ProcessStatus, string> = {
  UP: "정상",
  STOPPED: "중지",
  DEGRADED: "경고",
  DOWN: "중단",
  UNKNOWN: "확인 불가",
};

const STATUS_TAG_COLOR: Record<ProcessStatus, string> = {
  UP: "success",
  STOPPED: "default",
  DEGRADED: "warning",
  DOWN: "error",
  UNKNOWN: "default",
};

const STATUS_MARK: Record<ProcessStatus, string> = {
  UP: "●",
  STOPPED: "■",
  DEGRADED: "◆",
  DOWN: "▲",
  UNKNOWN: "○",
};

/**
 * "봐야 할 순서"는 "그룹 대표 색을 물들일 것인가"와 다른 질문이다.
 * 서버의 severity()(UP=STOPPED=0)를 고치면 그룹 색 판정이 통째로 바뀌므로 UI 정렬용을 따로 둔다.
 */
const UI_ORDER: Record<ProcessStatus, number> = {
  DOWN: 4,
  DEGRADED: 3,
  UNKNOWN: 2,
  STOPPED: 1,
  UP: 0,
};

/** 계획 정지가 길어질 때 이상 항목이 아래로 밀리는 것을 막는다. */
const STOPPED_COLLAPSE_THRESHOLD = 6;

function isUnconfigured(p: ProcessItem): boolean {
  return p.configured === false;
}

/** 이상 = 조사해야 하는 것. 미사용과 사용자가 일부러 멈춘 것은 이상이 아니다. */
function isAbnormal(p: ProcessItem): boolean {
  if (isUnconfigured(p)) return false;
  return p.status === "DOWN" || p.status === "DEGRADED" || p.status === "UNKNOWN";
}

function formatSeconds(sec: number): string {
  if (sec < 60) return `${sec}초`;
  if (sec % 60 === 0) return `${sec / 60}분`;
  return `${Math.floor(sec / 60)}분 ${sec % 60}초`;
}

function formatAgo(iso: string): string {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return "";
  const sec = Math.max(0, Math.round((Date.now() - t) / 1000));
  return `${formatSeconds(sec)} 전`;
}

/**
 * 판정 기준 툴팁. 원본 5-4 "정상 판정 기준 불명 → heartbeat 허용 지연 시간을 툴팁으로 노출".
 * 임계값은 반드시 응답에서 읽는다 — 화면에 숫자를 박으면 설정 변경 순간 화면이 거짓말을 한다.
 */
function ProcessTooltip({ item }: { item: ProcessItem }) {
  const lines: Array<[string, string]> = [];
  if (item.lastHeartbeatAt) {
    lines.push([
      "마지막 성공",
      `${dayjs(item.lastHeartbeatAt).format("YYYY-MM-DD HH:mm:ss")} (${formatAgo(item.lastHeartbeatAt)})`,
    ]);
  }
  if (item.staleAfterSeconds != null) {
    lines.push(["판정 기준", `${formatSeconds(item.staleAfterSeconds)} 초과 → 경고`]);
  }
  if (item.downAfterSeconds != null) {
    lines.push(["", `${formatSeconds(item.downAfterSeconds)} 초과 → 중단`]);
  }
  if (isUnconfigured(item)) {
    lines.push(["", "이 설치에 구성되지 않아 감시하지 않습니다."]);
  }
  if (lines.length === 0) {
    lines.push(["", "이 항목은 생존 여부만 확인하며 별도 허용 지연이 없습니다."]);
  }
  return (
    <div className="process-tooltip">
      <div className="process-tooltip-title">{item.name}</div>
      {lines.map(([k, v], i) => (
        <div key={i} className="process-tooltip-row">
          <span>{k}</span>
          <strong>{v}</strong>
        </div>
      ))}
    </div>
  );
}

function ProcessRow({ item }: { item: ProcessItem }) {
  const unconfigured = isUnconfigured(item);
  const label = unconfigured ? "미사용" : STATUS_LABEL[item.status];
  const mark = unconfigured ? "○" : STATUS_MARK[item.status];
  return (
    <Tooltip title={<ProcessTooltip item={item} />} placement="left">
      <div className={`infra-process-row ${unconfigured ? "is-unconfigured" : ""}`}>
        <span className={`process-mark process-mark--${unconfigured ? "unconfigured" : item.status.toLowerCase()}`}>
          {mark}
        </span>
        <span className="infra-process-name">{item.name}</span>
        <span className="infra-process-detail">{item.detail ?? "-"}</span>
        <Tag color={unconfigured ? "default" : STATUS_TAG_COLOR[item.status]}>{label}</Tag>
      </div>
    </Tooltip>
  );
}

interface ProcessHealthPanelProps {
  groups?: ProcessGroup[];
  loading: boolean;
  /**
   * 데이터 흐름 헬스(원본 5-4 "프로세스 정상 ≠ 데이터 정상").
   * 여기서 다시 판정하지 않는다 — KPI 카드가 이미 내린 판정을 그대로 받아 축만 나눠 보여준다.
   * 같은 판정을 두 곳에 구현하면 언젠가 두 화면이 서로 다른 말을 한다.
   */
  dataFlow?: { tone: "ok" | "warn" | "unknown"; text: string };
}

export function ProcessHealthPanel({ groups, loading, dataFlow }: ProcessHealthPanelProps) {
  const all = useMemo(() => (groups ?? []).flatMap((g) => g.processes.map((p) => ({ group: g, item: p }))), [groups]);

  const abnormal = all.filter(({ item }) => isAbnormal(item));
  const stopped = all.filter(({ item }) => item.status === "STOPPED" && !isUnconfigured(item));
  const unconfigured = all.filter(({ item }) => isUnconfigured(item));
  const normal = all.filter(({ item }) => item.status === "UP");

  // STOPPED 는 원칙적으로 접지 않는다(사람이 멈춘 것도 관제 대상이다).
  // 다만 계획 정지가 6건 이상이면 목록이 길어져 이상 항목이 밀리므로 함께 접는다.
  const collapseStopped = stopped.length >= STOPPED_COLLAPSE_THRESHOLD;
  const alwaysVisible = [
    ...abnormal.sort((a, b) => UI_ORDER[b.item.status] - UI_ORDER[a.item.status]),
    ...(collapseStopped ? [] : stopped),
    ...unconfigured,
  ];
  const collapsed = [...(collapseStopped ? stopped : []), ...normal];

  const [manuallyOpen, setManuallyOpen] = useState<boolean | null>(null);
  // 사용자가 접어놨더라도 새 이상이 생기면 무조건 펼친다 —
  // 관제 화면에서 이전 선택이 새 장애를 가려서는 안 된다.
  const prevAbnormal = useRef(0);
  useEffect(() => {
    if (abnormal.length > prevAbnormal.current) {
      setManuallyOpen(true);
    }
    prevAbnormal.current = abnormal.length;
  }, [abnormal.length]);

  const showCollapsed = manuallyOpen ?? false;

  return (
    <Card
      className="dashboard-panel infra-process-card"
      title={
        <div className="process-card-head">
          <span>주요 프로세스 현황</span>
          <span className="process-card-count">
            {abnormal.length > 0 ? (
              <Tag color="error">이상 {abnormal.length}</Tag>
            ) : (
              <Tag color="success">이상 없음</Tag>
            )}
            <span className="process-card-total">전체 {all.length}</span>
          </span>
        </div>
      }
      loading={loading && !groups}
    >
      {/* 원본 5-4 "데이터 흐름 헬스를 별도 지표로 분리". 프로세스 생존과 다른 축임을 화면에서 가른다. */}
      {dataFlow ? (
        <div className={`process-dataflow process-dataflow--${dataFlow.tone}`}>
          <span className="process-dataflow-axis">데이터 흐름</span>
          <span className="process-dataflow-text">{dataFlow.text}</span>
        </div>
      ) : null}

      {alwaysVisible.length === 0 && collapsed.length === 0 ? (
        <div className="infra-tile-unavailable">상태를 불러올 수 없습니다.</div>
      ) : null}

      {alwaysVisible.map(({ group, item }) => (
        <div key={`${group.key}-${item.name}`} className="infra-process-line">
          <span className="infra-process-group-tag">{group.label}</span>
          <ProcessRow item={item} />
        </div>
      ))}

      {collapsed.length > 0 ? (
        <>
          <div className="process-collapse-bar">
            <span>
              ● 정상 {normal.length}건{collapseStopped && stopped.length > 0 ? ` · 중지 ${stopped.length}건` : ""}
            </span>
            <button type="button" className="btn-proc-toggle" onClick={() => setManuallyOpen(!showCollapsed)}>
              {showCollapsed ? "접기 ▴" : "펼치기 ▾"}
            </button>
          </div>
          {showCollapsed
            ? collapsed.map(({ group, item }) => (
                <div key={`${group.key}-${item.name}`} className="infra-process-line">
                  <span className="infra-process-group-tag">{group.label}</span>
                  <ProcessRow item={item} />
                </div>
              ))
            : null}
        </>
      ) : null}

      {/* 사고 1의 교훈을 화면에 문장으로 남긴다. Debezium 스킵 당시 이 목록은 전부 초록이었다. */}
      <div className="process-boundary-note">
        ⓘ 이 목록은 프로세스 생존만 봅니다. 데이터가 실제로 흐르는지는 위 KPI 카드와 조치 대기열이 판정합니다.
      </div>
    </Card>
  );
}

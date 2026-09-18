import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DatePicker, Spin } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import { listAllAirflowDagRuns, type AirflowDagRun } from "../../api/platform";
import type { WorkflowSummary } from "../../api/workflows";
import { TaskFailureLogModal, type FailureLogTarget } from "../../components/TaskFailureLogModal";
import { WfIcon } from "../../components/WorkflowPageHeader";
import {
  childRunIdOf,
  displayName,
  durationSeconds,
  parentOfRun,
  periodRange,
  runAt,
  selectionKey,
  type DashboardDag,
  type PeriodKey,
  type Selection,
} from "../workflowLogV2/logV2Data";
import { GridHead, RunBody, RunRow, type GridCtx } from "../workflowLogV2/LogV2Grid";

const PERIODS: Array<[PeriodKey, string]> = [
  ["today", "오늘"], ["yesterday", "어제"], ["7d", "최근 7일"], ["30d", "최근 30일"], ["custom", "📅 기간 지정"],
];

function toggleIn(set: Set<string>, key: string) {
  const next = new Set(set);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

/**
 * 워크플로우 «로그» 화면 가운데(ETL 범위) - 로그2 화면의 실행 목록을 옮겼다(2026-09-18 요청).
 *
 * <ul>
 *   <li>위: 범위 이름 · 기간(오늘 · 어제 · 7일 · 30일 · 지정). 로그2 의 실행 목록/타임라인 전환 · 상태 칩 · 검색은 뺐다.</li>
 *   <li>가운데: 실행 → job → 단계를 한 표에서 펼친다. 하위 워크플로우 실행은 부른 job 아래로 들어간다(run id 규칙).</li>
 *   <li>아래: 고른 실행의 워크플로우 «실행 이력»만(기간 필터 없음, 대시보드가 받아 둔 최근 실행 전부). 끌어서 높이 조절 · 접기.</li>
 * </ul>
 *
 * <p>행을 고르면 그 실행의 워크플로우를 onSelectDag 로 올려 오른쪽 속성창이 따라온다(CDC 워크플로우면 파이프라인별 상태가 뜬다).
 */
export function EtlRunsPane({
  title,
  cdcWorkflow = false,
  includesChildren,
  scopeDagIds,
  dags,
  workflows,
  dashboardRuns,
  refreshSeconds,
  onSelectDag,
  sync,
}: {
  title: string;
  /** 범위가 CDC 워크플로우 하나면 머리줄 칩을 CDC 로(속성창 «CDC 워크플로우»와 맞춘다). */
  cdcWorkflow?: boolean;
  includesChildren: boolean;
  scopeDagIds: string[];
  dags: DashboardDag[];
  workflows: WorkflowSummary[];
  /** 대시보드가 받아 둔 최근 실행(기간 무관). 실행 이력 · 하위 실행 찾기에 쓴다. */
  dashboardRuns: AirflowDagRun[];
  refreshSeconds: number;
  onSelectDag: (dagId: string) => void;
  /**
   * DAG 카탈로그 동기화(2026-09-18 요청으로 화면 위 조작 띠에서 여기 «기간» 왼쪽으로 내려왔다).
   * 쓰기 권한이 없으면 화면에서 아예 뺀다 - 눌러도 403 인 버튼을 남기지 않는다.
   */
  sync?: { busy: boolean; onSync: () => void };
}) {
  const [period, setPeriod] = useState<PeriodKey>("today");
  const [customRange, setCustomRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day").startOf("day"), dayjs().endOf("day")]);
  const [selection, setSelection] = useState<Selection>();
  const [historyExpanded, setHistoryExpanded] = useState<Set<string>>(new Set());
  const [historySelectedKey, setHistorySelectedKey] = useState<string>();
  const [failureLog, setFailureLog] = useState<FailureLogTarget>();
  const [historyOpen, setHistoryOpen] = useState(true);
  const [historyHeight, setHistoryHeight] = useState(300);
  const autoSelect = useRef(true);
  const splitRef = useRef<HTMLDivElement>(null);
  const scopeKey = scopeDagIds.join(",");

  const [rangeFrom, rangeTo] = periodRange(period, customRange);
  const rangeIncludesNow = !dayjs().isAfter(rangeTo);
  const periodRunsQuery = useQuery({
    // 로그2 화면과 같은 캐시 키 - 두 화면을 오가도 한 번만 받는다.
    queryKey: ["logv2-period-runs", rangeFrom.toISOString(), rangeTo.toISOString()],
    queryFn: () => listAllAirflowDagRuns({
      startDateGte: rangeFrom.toISOString(),
      startDateLte: rangeTo.toISOString(),
      maxRuns: 5000,
    }),
    refetchInterval: rangeIncludesNow ? refreshSeconds * 1000 : false,
  });

  const dagsById = useMemo(() => new Map(dags.map((dag) => [dag.dag_id, dag] as const)), [dags]);
  const runsById = useMemo(() => {
    const map = new Map<string, AirflowDagRun[]>();
    const seen = new Set<string>();
    [...(periodRunsQuery.data ?? []), ...dashboardRuns].forEach((run) => {
      const key = `${run.dag_id}|${run.dag_run_id}`;
      if (seen.has(key)) return;
      seen.add(key);
      map.set(run.dag_run_id, [...(map.get(run.dag_run_id) ?? []), run]);
    });
    return map;
  }, [periodRunsQuery.data, dashboardRuns]);

  const findChildRun = (parentDagId: string, parentRunId: string, nodeKey: string) => {
    const candidates = runsById.get(childRunIdOf(parentRunId, nodeKey)) ?? [];
    const parent = workflows.find((workflow) => workflow.dagId === parentDagId);
    const childDagIds = new Set(workflows
      .filter((workflow) => parent?.childWorkflowIds.includes(workflow.id))
      .map((workflow) => workflow.dagId));
    return candidates.find((run) => run.dag_id && childDagIds.has(run.dag_id))
      ?? candidates.find((run) => run.dag_id !== parentDagId);
  };

  const topRuns = useMemo(() => {
    const scope = new Set(scopeKey ? scopeKey.split(",") : []);
    const periodRuns = periodRunsQuery.data ?? [];
    const periodById = new Map<string, AirflowDagRun[]>();
    periodRuns.forEach((run) => periodById.set(run.dag_run_id, [...(periodById.get(run.dag_run_id) ?? []), run]));
    return periodRuns
      .filter((run) => run.dag_id && scope.has(run.dag_id))
      // 범위 안의 상위 실행이 부른 하위 실행은 맨 위에 따로 두지 않는다 - 부른 job 아래에서 펼친다.
      .filter((run) => {
        const parent = parentOfRun(run.dag_run_id);
        if (!parent) return true;
        return !(periodById.get(parent.parentRunId) ?? [])
          .some((candidate) => candidate.dag_id && candidate.dag_id !== run.dag_id && scope.has(candidate.dag_id));
      })
      .sort((a, b) => dayjs(runAt(b) ?? 0).valueOf() - dayjs(runAt(a) ?? 0).valueOf());
  }, [periodRunsQuery.data, scopeKey]);

  // 범위 · 기간이 바뀌면 맨 위 실행을 골라 펼친다. 갱신 주기마다 다시 고르지는 않는다.
  useEffect(() => {
    autoSelect.current = true;
  }, [scopeKey, period, customRange]);
  useEffect(() => {
    if (!autoSelect.current || !periodRunsQuery.isFetched) return;
    autoSelect.current = false;
    const first = topRuns[0];
    if (!first?.dag_id) {
      setSelection(undefined);
      return;
    }
    const sel: Selection = { kind: "run", dagId: first.dag_id, runId: first.dag_run_id };
    setSelection(sel);
    setHistoryExpanded(new Set());
    setHistorySelectedKey(undefined);
  }, [topRuns, periodRunsQuery.isFetched]);

  // 가운데는 워크플로우 실행 줄만 있다. 고르면 그 실행의 task 들이 아래 «실행 이력»에, 그 워크플로우가 속성창에 나온다.
  const select = (sel: Selection) => {
    if (!selection || selectionKey(selection) !== selectionKey(sel)) {
      setHistoryExpanded(new Set());
      setHistorySelectedKey(undefined);
    }
    setSelection(sel);
    setHistoryOpen(true);
    onSelectDag(sel.dagId);
  };

  const ctx: GridCtx = {
    dagsById,
    findChildRun,
    expanded: new Set(),
    toggle: () => undefined,
    selectedKey: selection ? selectionKey(selection) : undefined,
    select,
    openLog: setFailureLog,
    // 행의 «실행 이력» · «로그» 버튼: 그 실행을 고르고 아래 실행 이력을 연다(로그는 실패 로그 창으로 연다).
    openTab: (sel) => select(sel),
    refreshSeconds,
  };

  // 아래 실행 이력: 가운데에서 고른 실행 한 건의 task(job → 단계) 들을 순서대로. 워크플로우 줄은 없다.
  const selectedRun = selection
    ? topRuns.find((run) => run.dag_id === selection.dagId && run.dag_run_id === selection.runId)
    : undefined;
  const historyDag = selection ? dagsById.get(selection.dagId) : undefined;
  const historyStarted = selectedRun ? runAt(selectedRun) : undefined;
  const historyCtx: GridCtx = {
    ...ctx,
    expanded: historyExpanded,
    toggle: (key) => setHistoryExpanded((current) => toggleIn(current, key)),
    selectedKey: historySelectedKey,
    select: (sel) => {
      const key = selectionKey(sel);
      setHistorySelectedKey(key);
      if (sel.kind !== "step") setHistoryExpanded((current) => toggleIn(current, key));
    },
    openTab: (sel) => {
      const key = selectionKey(sel);
      setHistorySelectedKey(key);
      setHistoryExpanded((current) => new Set(current).add(key));
    },
  };

  const scaleOf = (runs: AirflowDagRun[]) => Math.max(...runs.map((run) =>
    durationSeconds(runAt(run), run.end_date ?? (run.state === "running" ? dayjs().toISOString() : undefined))), 1);
  const multiDay = !rangeFrom.isSame(rangeTo, "day");

  const startResize = (event: React.MouseEvent) => {
    event.preventDefault();
    const startY = event.clientY;
    const startHeight = historyOpen ? historyHeight : 42;
    const limit = (splitRef.current?.clientHeight ?? 700) - 140;
    setHistoryOpen(true);
    const move = (ev: MouseEvent) => setHistoryHeight(Math.min(limit, Math.max(160, startHeight - (ev.clientY - startY))));
    const up = () => {
      document.removeEventListener("mousemove", move);
      document.removeEventListener("mouseup", up);
      document.body.classList.remove("lv2-resizing");
    };
    document.body.classList.add("lv2-resizing");
    document.addEventListener("mousemove", move);
    document.addEventListener("mouseup", up);
  };

  let grid: React.ReactNode;
  if (periodRunsQuery.isLoading) {
    grid = <div className="lv2-empty"><Spin size="small" /> 실행 목록을 불러오는 중입니다.</div>;
  } else if (periodRunsQuery.isError) {
    grid = <div className="lv2-empty">실행 목록을 불러오지 못했습니다.</div>;
  } else if (!scopeKey) {
    grid = <div className="lv2-empty">이 범위에 워크플로우가 없습니다.</div>;
  } else if (!topRuns.length) {
    grid = <div className="lv2-empty">이 범위·기간에 실행이 없습니다. 기간을 넓혀 보세요.</div>;
  } else {
    const scale = scaleOf(topRuns);
    grid = (
      <table className="lv2-grid">
        <GridHead />
        <tbody>
          {topRuns.map((run) => <RunRow key={`${run.dag_id}|${run.dag_run_id}`} run={run} ctx={ctx} scale={scale} showDate={multiDay} expandable={false} />)}
        </tbody>
      </table>
    );
  }

  return (
    <div className="etl-runs-pane">
      <div className="lv2-ctx">
        <div className="lv2-crumb">
          {cdcWorkflow ? <span className="lv2-dom cdc"><i />CDC</span> : <span className="lv2-dom etl"><i />ETL</span>}
          <span className="lv2-crumb-cur" title={title}>{title}</span>
          {includesChildren && <span className="lv2-crumb-note">· 하위 워크플로우 포함</span>}
        </div>
        <div className="lv2-ctx-right">
          {sync && (
            // antd 버튼의 loading 과 같게: 동기화 중에는 누름을 무시하고 아이콘만 돈다.
            <button type="button" className={sync.busy ? "btn-wf-sync spinning" : "btn-wf-sync"}
                    aria-busy={sync.busy}
                    title="ETL 에서 만든 job 을 기다리지 않고 바로 가져옵니다"
                    onClick={() => {
                      if (!sync.busy) {
                        sync.onSync();
                      }
                    }}>
              <WfIcon name="sync" size={15} strokeWidth={2.2} />
              동기화
            </button>
          )}
          <span className="lv2-ctx-label">기간</span>
          <div className="lv2-seg">
            {PERIODS.map(([key, label]) => (
              <button key={key} type="button" className={period === key ? "on" : ""} onClick={() => setPeriod(key)}>{label}</button>
            ))}
          </div>
          {period === "custom" && (
            <DatePicker.RangePicker size="small" value={customRange} allowClear={false}
                                    onChange={(value) => {
                                      if (value?.[0] && value?.[1]) setCustomRange([value[0].startOf("day"), value[1].endOf("day")]);
                                    }} />
          )}
        </div>
      </div>

      <div ref={splitRef} className={`lv2-split${historyOpen ? "" : " detail-closed"}`}>
        <div className="lv2-grid-area">{grid}</div>
        <div className="lv2-split-handle" title="끌어서 실행 이력 높이 조절" onMouseDown={startResize} />
        <section className="lv2-detail" style={historyOpen ? { height: historyHeight } : undefined}>
          <div className="lv2-detail-head">
            <div className="lv2-detail-title">
              <span className="lv2-kind run">실행 이력</span>
              <b>{historyDag ? displayName(historyDag) : selection?.dagId ?? "-"}</b>
              {historyStarted && <small>{dayjs(historyStarted).format("MM-DD HH:mm:ss")} 실행</small>}
            </div>
            <div className="lv2-detail-acts">
              <button type="button" className="lv2-btn ghost sm" onClick={() => setHistoryOpen((open) => !open)}>
                {historyOpen ? "▾ 접기" : "▴ 펼치기"}
              </button>
            </div>
          </div>
          {historyOpen && (
            <div className="lv2-detail-body">
              {!selectedRun ? <div className="lv2-empty">위에서 워크플로우 실행을 고르면 그 실행의 태스크가 순서대로 나옵니다.</div> : (
                <table className="lv2-grid">
                  <GridHead />
                  <tbody>
                    <RunBody key={`${selectedRun.dag_id}|${selectedRun.dag_run_id}`} dagId={selectedRun.dag_id ?? ""}
                             runId={selectedRun.dag_run_id} depth={0} ctx={historyCtx}
                             scale={durationSeconds(historyStarted, selectedRun.end_date ?? (selectedRun.state === "running" ? dayjs().toISOString() : undefined))} />
                  </tbody>
                </table>
              )}
            </div>
          )}
        </section>
      </div>
      <TaskFailureLogModal target={failureLog} onClose={() => setFailureLog(undefined)} />
    </div>
  );
}

import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, DatePicker, Space, Spin } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import { listAllAirflowDagRuns, type AirflowDagRun } from "../../api/platform";
import type { WorkflowSummary } from "../../api/workflows";
import { TaskFailureLogModal, type FailureLogTarget } from "../../components/TaskFailureLogModal";
import { activeDayPresetByDate, type DayPreset } from "../../components/dayPreset";
import { WfIcon } from "../../components/WorkflowPageHeader";
import {
  childRunIdOf,
  displayName,
  durationSeconds,
  parentOfRun,
  runAt,
  selectionKey,
  type DashboardDag,
  type Selection,
} from "../workflowLogV2/logV2Data";
import { GridHead, RunBody, RunRow, type GridCtx } from "../workflowLogV2/LogV2Grid";


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
  activeRuns: liveActiveRuns,
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
  /**
   * 대기 · 실행 중인 실행(3초/5초 주기). 화면(AirflowDashboardPage)이 받아 현황 카드와 같이 쓴다(2026-10-11 - 예전에는 여기서 따로 받았다).
   * 아직 못 받았으면 undefined.
   */
  activeRuns: AirflowDagRun[] | undefined;
  refreshSeconds: number;
  onSelectDag: (dagId: string) => void;
  /**
   * DAG 카탈로그 동기화(2026-09-18 요청으로 화면 위 조작 띠에서 여기 «기간» 왼쪽으로 내려왔다).
   * 쓰기 권한이 없으면 화면에서 아예 뺀다 - 눌러도 403 인 버튼을 남기지 않는다.
   */
  sync?: { busy: boolean; onSync: () => void };
}) {
  // 조회기간은 ETL 로그 화면과 같은 모양으로 맞췄다(2026-09-22 요청) - «당일 · 전일» 버튼 + 날짜 달력, 기본은 당일.
  // 예전의 오늘/어제/7일/30일/기간지정 칩은 같은 일을 다른 모양으로 하고 있었다.
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs(), dayjs()]);
  const [selection, setSelection] = useState<Selection>();
  const [historyExpanded, setHistoryExpanded] = useState<Set<string>>(new Set());
  const [historySelectedKey, setHistorySelectedKey] = useState<string>();
  const [failureLog, setFailureLog] = useState<FailureLogTarget>();
  const [historyOpen, setHistoryOpen] = useState(true);
  const [historyHeight, setHistoryHeight] = useState(300);
  const autoSelect = useRef(true);
  const splitRef = useRef<HTMLDivElement>(null);
  const scopeKey = scopeDagIds.join(",");

  const rangeFrom = dateRange[0].startOf("day");
  const rangeTo = dateRange[1].endOf("day");
  const rangeKey = `${rangeFrom.valueOf()}|${rangeTo.valueOf()}`;
  // 달력은 날짜만 다루므로 판정도 날짜로 한다(ETL 로그와 같은 헬퍼).
  const selectedPreset = activeDayPresetByDate(dateRange[0], dateRange[1]);
  const applyDayPreset = (offset: DayPreset) => {
    const day = dayjs().subtract(offset, "day");
    setDateRange([day, day]);
  };
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

  /*
   * 대기 · 실행 중인 실행(2026-10-06 요청 - 수동 실행이 15~20초 뒤에야 보였다). 있으면 3초, 없으면 5초마다 화면이 받는다(결과가
   * 비면 100바이트 남짓). 기간 내 실행 전체(최대 5,000건)는 화면 주기 그대로 두고, 실행이 끝나 이 목록에서 빠지는 순간에만 한 번 더
   * 받는다. 실행을 건 화면은 그 캐시 키를 무효화해 바로 다시 받게 한다(refreshRunsAfterTrigger). 지금을 포함한 기간에만 덮어쓴다.
   */
  const activeRuns = rangeIncludesNow ? liveActiveRuns : undefined;
  const activeKey = (activeRuns ?? []).map((run) => `${run.dag_id}|${run.dag_run_id}`).sort().join(",");
  const previousActiveKey = useRef<string | undefined>(undefined);
  const refetchPeriodRuns = periodRunsQuery.refetch;
  useEffect(() => {
    const before = previousActiveKey.current;
    previousActiveKey.current = activeKey;
    if (!before) return;
    const stillActive = new Set(activeKey ? activeKey.split(",") : []);
    if (before.split(",").some((key) => !stillActive.has(key))) void refetchPeriodRuns();
  }, [activeKey, refetchPeriodRuns]);

  /** 기간 목록에 대기 · 실행 중 목록을 덮어쓴 것 - 도는 실행의 상태는 3초 전 것, 방금 건(아직 시작 전) 실행도 들어간다. */
  const periodRuns = useMemo(() => {
    const merged = new Map<string, AirflowDagRun>();
    (periodRunsQuery.data ?? []).forEach((run) => merged.set(`${run.dag_id}|${run.dag_run_id}`, run));
    (activeRuns ?? []).forEach((run) => {
      const key = `${run.dag_id}|${run.dag_run_id}`;
      const at = runAt(run);
      if (merged.has(key) || (at && !dayjs(at).isBefore(rangeFrom) && !dayjs(at).isAfter(rangeTo))) {
        merged.set(key, { ...merged.get(key), ...run });
      }
    });
    return [...merged.values()];
    // rangeFrom · rangeTo 는 rangeKey 로 대신한다(렌더마다 새 dayjs 객체라 그대로 넣으면 매번 다시 계산된다).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [periodRunsQuery.data, activeRuns, rangeKey]);

  const dagsById = useMemo(() => new Map(dags.map((dag) => [dag.dag_id, dag] as const)), [dags]);
  const runsById = useMemo(() => {
    const map = new Map<string, AirflowDagRun[]>();
    const seen = new Set<string>();
    [...periodRuns, ...dashboardRuns].forEach((run) => {
      const key = `${run.dag_id}|${run.dag_run_id}`;
      if (seen.has(key)) return;
      seen.add(key);
      map.set(run.dag_run_id, [...(map.get(run.dag_run_id) ?? []), run]);
    });
    return map;
  }, [periodRuns, dashboardRuns]);

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
  }, [periodRuns, scopeKey]);

  /**
   * 실행 차수 - 그날 이 워크플로우의 몇 번째 실행인지다(2026-09-21 요청).
   * 목록에 워크플로우 이름 대신 «09-18 10:54:59 · 2차»로 적는다. 고른 기간 안의 실행만 세므로,
   * 기간을 하루 중간부터 잡으면 그 앞의 실행은 세지 않는다.
   */
  const ordinalByRun = useMemo(() => {
    const byDay = new Map<string, AirflowDagRun[]>();
    topRuns.forEach((run) => {
      const day = `${run.dag_id}|${dayjs(runAt(run) ?? 0).format("YYYY-MM-DD")}`;
      byDay.set(day, [...(byDay.get(day) ?? []), run]);
    });
    const map = new Map<string, number>();
    byDay.forEach((runs) => {
      [...runs]
        .sort((a, b) => dayjs(runAt(a) ?? 0).valueOf() - dayjs(runAt(b) ?? 0).valueOf())
        .forEach((run, index) => map.set(`${run.dag_id}|${run.dag_run_id}`, index + 1));
    });
    return map;
  }, [topRuns]);
  /** 한 워크플로우만 보는 범위면 목록에서 이름을 뺀다 - 머리줄에 이미 그 이름이 있다. */
  const singleWorkflow = scopeDagIds.length <= 1;
  const runStamp = (run?: AirflowDagRun) => {
    if (!run) return "-";
    const at = runAt(run);
    const ordinal = ordinalByRun.get(`${run.dag_id}|${run.dag_run_id}`);
    return `${at ? dayjs(at).format("MM-DD HH:mm:ss") : "-"}${ordinal ? ` · ${ordinal}차` : ""}`;
  };

  // 범위 · 기간이 바뀌면 맨 위 실행을 골라 펼친다. 갱신 주기마다 다시 고르지는 않는다.
  useEffect(() => {
    autoSelect.current = true;
  }, [scopeKey, rangeKey]);
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
    runLabel: (dagId, runId) => {
      const run = topRuns.find((row) => row.dag_id === dagId && row.dag_run_id === runId);
      if (!run) return undefined;
      const dag = dagsById.get(dagId);
      return `${dag ? displayName(dag) : dagId} · ${runStamp(run)}`;
    },
    workflowIdOf: (dagId) => workflows.find((workflow) => workflow.dagId === dagId)?.id,
    refreshSeconds,
  };

  // 아래 실행 이력: 가운데에서 고른 실행 한 건의 task(job → 단계) 들을 순서대로. 워크플로우 줄은 없다.
  const selectedRun = selection
    ? topRuns.find((run) => run.dag_id === selection.dagId && run.dag_run_id === selection.runId)
    : undefined;
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
        <GridHead nameLabel="실행 차수" runList />
        <tbody>
          {topRuns.map((run) => (
            <RunRow key={`${run.dag_id}|${run.dag_run_id}`} run={run} ctx={ctx} scale={scale} showDate={multiDay}
                    expandable={false}
                    ordinal={ordinalByRun.get(`${run.dag_id}|${run.dag_run_id}`)}
                    hideName={singleWorkflow} />
          ))}
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
          <Space.Compact>
            <Button size="small" type={selectedPreset === 0 ? "primary" : "default"} onClick={() => applyDayPreset(0)}>당일</Button>
            <Button size="small" type={selectedPreset === 1 ? "primary" : "default"} onClick={() => applyDayPreset(1)}>전일</Button>
          </Space.Compact>
          <DatePicker.RangePicker size="small" value={dateRange} allowClear={false}
                                  onChange={(value) => {
                                    if (value?.[0] && value?.[1]) setDateRange([value[0], value[1]]);
                                  }} />
        </div>
      </div>

      <div ref={splitRef} className={`lv2-split${historyOpen ? "" : " detail-closed"}`}>
        <div className="lv2-grid-area">{grid}</div>
        <div className="lv2-split-handle" title="끌어서 실행 이력 높이 조절" onMouseDown={startResize} />
        <section className="lv2-detail" style={historyOpen ? { height: historyHeight } : undefined}>
          <div className="lv2-detail-head">
            <div className="lv2-detail-title">
              {/* 워크플로우명은 위 머리줄에 이미 있다 - 여기는 «어느 차수의 실행인지»만 적는다(2026-09-21 요청). */}
              <span className="lv2-kind run">상세 실행이력</span>
              <b>{selectedRun ? runStamp(selectedRun) : "-"}</b>
              {selectedRun && <small>실행</small>}
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
                             runId={selectedRun.dag_run_id} runState={selectedRun.state} depth={0} ctx={historyCtx}
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

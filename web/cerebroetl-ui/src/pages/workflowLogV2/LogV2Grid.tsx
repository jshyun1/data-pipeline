import { Fragment, useCallback, useEffect, useState } from "react";
import { useQueries } from "@tanstack/react-query";
import dayjs from "dayjs";
import { listAirflowTaskInstances, type AirflowDagRun } from "../../api/platform";
import type { FailureLogTarget } from "../../components/TaskFailureLogModal";
import {
  countValue,
  displayName,
  duration,
  durationSeconds,
  formatAt,
  groupJobs,
  jobOutcome,
  jobProgress,
  loadCountOf,
  runAt,
  runTypeLabel,
  isActiveState,
  selectionKey,
  stepName,
  type DashboardDag,
  type DetailTab,
  type JobGroup,
  type Selection,
} from "./logV2Data";
import { useRerun, useRunTasks } from "./logV2Hooks";
import { StateBadge } from "./LogV2Parts";

/** 목록 행들이 함께 쓰는 상태·동작. 가운데 목록과 상세 창 «실행 이력»이 각자 하나씩 만든다. */
export interface GridCtx {
  dagsById: Map<string, DashboardDag>;
  findChildRun: (parentDagId: string, parentRunId: string, nodeKey: string) => AirflowDagRun | undefined;
  expanded: Set<string>;
  toggle: (key: string) => void;
  selectedKey?: string;
  select: (sel: Selection) => void;
  openLog: (target: FailureLogTarget) => void;
  openTab: (sel: Selection, tab: DetailTab) => void;
  /** 실행 한 건을 한 줄로 부르는 이름(예: «total · 09-18 10:54:59 · 2차»). 실패 로그 창 머리에 적는다. */
  runLabel?: (dagId: string, runId: string) => string | undefined;
  refreshSeconds: number;
}

const COLS = 10;

/**
 * 표 머리줄. runList 는 워크플로우 로그 위쪽 «실행 목록»(RunRow) - 유형 · 동작 열이 없다(2026-10-02 요청:
 * 줄이 전부 «워크플로우»라 유형이 뜻이 없고, 줄을 누르면 아래에 상세 실행이력이 나와 «실행 이력» 단추도 필요 없다).
 */
export function GridHead({ nameLabel = "이름", runList = false }: { nameLabel?: string; runList?: boolean } = {}) {
  return (
    <thead>
      <tr>
        <th style={{ width: "30%" }}>{nameLabel}</th>{!runList && <th>유형</th>}<th>상태</th><th>시작</th><th>종료</th><th>소요</th>
        <th>적재 / 결과</th><th>재시도</th><th>실행 유형</th>{!runList && <th style={{ width: "1%" }}>동작</th>}
      </tr>
    </thead>
  );
}

function Toggle({ open, empty, onToggle }: { open: boolean; empty?: boolean; onToggle: () => void }) {
  return (
    <button type="button" className={`lv2-tg${empty ? " empty" : open ? " open" : ""}`}
            aria-label={open ? "접기" : "펼치기"} tabIndex={empty ? -1 : 0}
            onClick={(event) => { event.stopPropagation(); if (!empty) onToggle(); }}>
      ▶
    </button>
  );
}

function DurCell({ seconds, text, scale }: { seconds: number; text: string; scale: number }) {
  return (
    <div className="lv2-dur">
      <span className="lv2-num">{text}</span>
      {seconds > 0 && scale > 0 && (
        <span className="lv2-dur-bar"><i style={{ width: `${Math.max(4, Math.min(100, (seconds / scale) * 100))}%` }} /></span>
      )}
    </div>
  );
}

/**
 * 적재 합계를 숫자로 들고 다니는 꼴. 하위 워크플로우 실행의 합계를 상위에 더하려고 글자(«N행»)로 바꾸기 전 단계를 따로 뒀다(2026-10-11).
 * pending: 아직 받는 중(«…») · known: 셀 수 있는 건수가 하나라도 있음(없으면 «-») · live: 아직 도는 job 의 진행 건수가 섞임.
 */
interface RowsSum {
  pending: boolean;
  known: boolean;
  rows: number;
  live: boolean;
}

const NO_ROWS: RowsSum = { pending: false, known: false, rows: 0, live: false };
const PENDING_ROWS: RowsSum = { ...NO_ROWS, pending: true };

function addRows(a: RowsSum, b: RowsSum): RowsSum {
  return { pending: a.pending || b.pending, known: a.known || b.known, rows: a.rows + b.rows, live: a.live || b.live };
}

function rowsText(sum: RowsSum) {
  return sum.pending ? "…" : sum.known ? `${sum.rows.toLocaleString()}행` : "-";
}

/**
 * 실행 한 건이 «자기 job 으로» 적재한 합계. 하위 워크플로우 호출은 여기서 세지 않는다(useRunRows 가 하위 실행 합계를 더한다).
 * job 로그를 다 받기 전에는 pending. 실행 중이면 끝난 job 의 건수에 도는 job 의 «지금까지 적재 건수»를 더하고 live 로 알린다(2026-10-06).
 */
function ownRows(jobs: JobGroup[], logs?: Record<string, string>, progress?: Record<string, number>): RowsSum {
  const doneJobs = jobs.filter((job) => job.kind === "job" && job.state === "success");
  const liveRows = jobs.map((job) => jobProgress(job, progress)).filter((rows): rows is number => rows != null);
  if (!doneJobs.length && !liveRows.length) return NO_ROWS;
  const outcomes = doneJobs.map((job) => jobOutcome(job, logs, progress));
  if (!logs || outcomes.includes("…")) return PENDING_ROWS;
  let total = liveRows.reduce((sum, rows) => sum + rows, 0);
  let known = liveRows.length > 0;
  for (const outcome of outcomes) {
    const value = countValue(outcome);
    if (value != null) {
      total += value;
      known = true;
    }
  }
  return { pending: false, known, rows: total, live: liveRows.length > 0 };
}

/** 실행 한 건의 재시도 합계(자기 job 만). */
function retryText(jobs?: JobGroup[]) {
  return jobs ? `${jobs.reduce((sum, job) => sum + job.retryCount, 0)}회` : "-";
}

/**
 * 실행 한 건의 적재 합계 - 자기 job + 하위 워크플로우 실행(그 아래 하위까지)(2026-10-11 요청: 상위 실행 줄이 하위 호출 117,655행을
 * 빼고 자기 job 18,048행만 적었다).
 *
 * <p>하위 실행마다 {@link RowsProbe} 가 같은 계산을 하고 결과를 올려 보낸다. 부르는 쪽은 돌려준 {@code probes} 를 화면 어디든 그려 둬야
 * 한다(DOM 은 만들지 않는다). 하위 실행을 아직 못 받았으면 합계도 «…»로 둔다 - 상위 건수만 먼저 보였다가 커지는 일이 없게.
 */
function useRunRows(dagId: string, runId: string, runState: string | undefined, ctx: GridCtx, enabled: boolean) {
  const { tasksQuery, jobs, logs, progress } = useRunTasks(dagId, runId, { enabled, refreshSeconds: ctx.refreshSeconds, runState });
  const [childSums, setChildSums] = useState<Record<string, RowsSum>>({});
  const report = useCallback((key: string, sum: RowsSum) => {
    setChildSums((previous) => {
      const before = previous[key];
      return before && before.pending === sum.pending && before.known === sum.known
        && before.rows === sum.rows && before.live === sum.live ? previous : { ...previous, [key]: sum };
    });
  }, []);
  const children = (jobs ?? []).flatMap((job) => {
    if (job.kind !== "call") return [];
    const child = ctx.findChildRun(dagId, runId, job.key);
    return child?.dag_id ? [{ key: `${child.dag_id}|${child.dag_run_id}`, run: child, dagId: child.dag_id }] : [];
  });
  const own = jobs ? ownRows(jobs, logs, progress) : enabled && tasksQuery.isLoading ? PENDING_ROWS : NO_ROWS;
  const sum = children.reduce((total, child) => addRows(total, childSums[child.key] ?? (enabled ? PENDING_ROWS : NO_ROWS)), own);
  const probes = children.map((child) => (
    <RowsProbe key={child.key} probeKey={child.key} dagId={child.dagId} run={child.run} ctx={ctx} enabled={enabled} onRows={report} />
  ));
  return { jobs, sum, probes };
}

/** 하위 워크플로우 실행 하나의 적재 합계를 위로 올려 보낸다. 화면에 그리는 것은 없다. */
function RowsProbe({ probeKey, dagId, run, ctx, enabled, onRows }: {
  probeKey: string;
  dagId: string;
  run: AirflowDagRun;
  ctx: GridCtx;
  enabled: boolean;
  onRows: (key: string, sum: RowsSum) => void;
}) {
  const { sum, probes } = useRunRows(dagId, run.dag_run_id, run.state, ctx, enabled);
  useEffect(() => {
    onRows(probeKey, sum);
    // sum 은 렌더마다 새 객체라 값으로 본다.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [probeKey, onRows, sum.pending, sum.known, sum.rows, sum.live]);
  return <>{probes}</>;
}

/** 적재 건수 칸 - 실행 중이면 «· 진행 중»을 덧붙인다. */
function RowsText({ text, live, muted = false }: { text: string; live: boolean; muted?: boolean }) {
  return (
    <>
      <span className={muted ? "lv2-muted" : "lv2-num"}>{text}</span>
      {live && <span className="lv2-muted lv2-live"> · 진행 중</span>}
    </>
  );
}

/**
 * 워크플로우 실행 한 줄(+ 펼치면 그 아래 job 들). 목록의 맨 위 단위다.
 * 펼치거나 고른 실행만 task 를 받는다 - 기간을 30일로 넓혀도 줄마다 요청이 나가지 않게.
 */
export function RunRow({ run, ctx, scale, showDate, expandable = true, ordinal, hideName = false }: {
  run: AirflowDagRun;
  ctx: GridCtx;
  scale: number;
  showDate: boolean;
  /** false 면 펼치지 않는 한 줄(워크플로우 «로그» 화면 가운데 - job 은 아래 실행 이력에서 본다). */
  expandable?: boolean;
  /** 그날 이 워크플로우의 몇 번째 실행인지. 주면 «실행 차수»로 적는다(2026-09-21 요청). */
  ordinal?: number;
  /** 한 워크플로우만 보는 목록이면 이름을 뺀다 - 머리줄에 이미 그 이름이 있다. */
  hideName?: boolean;
}) {
  const dagId = run.dag_id ?? "";
  const dag = ctx.dagsById.get(dagId);
  const sel: Selection = { kind: "run", dagId, runId: run.dag_run_id };
  const key = selectionKey(sel);
  const expanded = expandable && ctx.expanded.has(key);
  const selected = ctx.selectedKey === key;
  const { jobs, sum, probes } = useRunRows(dagId, run.dag_run_id, run.state, ctx, expanded || selected);
  const name = dag ? displayName(dag) : dagId;
  const started = runAt(run);
  const end = run.end_date ?? (run.state === "running" ? dayjs().toISOString() : undefined);
  // 실행 차수를 적는 목록은 날짜까지 항상 보여 준다(«몇일 몇시 몇분 몇초» 요청).
  const stamp = started ? dayjs(started).format(showDate || ordinal ? "MM-DD HH:mm:ss" : "HH:mm:ss") : "-";
  return (
    <>
      <tr className={`lv2-r k-run f-${run.state ?? "none"}${selected ? " sel" : ""}`} onClick={() => ctx.select(sel)}>
        <td>
          <div className="lv2-nm">
            {expandable && <Toggle open={expanded} onToggle={() => ctx.toggle(key)} />}
            <span className="lv2-kind run">실행</span>
            <span className="lv2-nm-text" title={`${name} (${dagId})`}>
              {hideName ? stamp : name}
              <span className="lv2-muted lv2-nm-sub">
                {hideName ? "" : ` · ${stamp}`}{ordinal ? ` · ${ordinal}차` : ""}
              </span>
            </span>
          </div>
        </td>
        <td><StateBadge state={run.state} /></td>
        <td className="lv2-num">{formatAt(started)}</td>
        <td className="lv2-num">{formatAt(run.end_date)}</td>
        <td><DurCell seconds={durationSeconds(started, end)} text={duration(started, run.end_date ?? end)} scale={scale} /></td>
        {/* 실패한 실행도 «로그» 단추 없이 적재 합계만 적는다(2026-10-02 요청) - 실패 로그는 아래 상세 실행이력의 job 줄에서 연다. */}
        <td><RowsText text={rowsText(sum)} live={sum.live} />{probes}</td>
        <td className="lv2-num">{retryText(jobs)}</td>
        <td className="lv2-muted">{runTypeLabel(run)}</td>
      </tr>
      {expanded && <RunBody dagId={dagId} runId={run.dag_run_id} runState={run.state} depth={1} ctx={ctx}
                            scale={durationSeconds(started, end)} />}
    </>
  );
}

/** 실행 하나의 job 줄들. 하위 워크플로우 호출 job 은 펼치면 그 하위 실행의 job 들이 한 칸 더 들여 나온다. */
export function RunBody({ dagId, runId, runState, depth, ctx, scale }: {
  dagId: string;
  runId: string;
  /** 실행 상태(끝났는지). 실행 중이면 3초마다 받고 진행 건수를 보여 준다. */
  runState?: string;
  depth: number;
  ctx: GridCtx;
  scale: number;
}) {
  const { tasksQuery, tasks, jobs, logs, progress } = useRunTasks(dagId, runId,
    { enabled: true, refreshSeconds: ctx.refreshSeconds, runState });
  const { busyKey, rerun } = useRerun(dagId, runId, tasks, () => tasksQuery.refetch());
  const pad = { paddingLeft: depth * 20 };
  if (tasksQuery.isLoading) {
    return <tr className="lv2-r k-info"><td colSpan={COLS}><div style={pad} className="lv2-muted">실행 단계를 불러오는 중입니다.</div></td></tr>;
  }
  if (!jobs?.length) {
    return <tr className="lv2-r k-info"><td colSpan={COLS}><div style={pad} className="lv2-muted">실행 단계가 없습니다.</div></td></tr>;
  }
  const jobScale = scale || Math.max(...jobs.map((job) => durationSeconds(job.startedAt, job.endedAt)), 1);
  return (
    <>
      {jobs.map((job, index) => (
        <JobRow key={job.key} dagId={dagId} runId={runId} job={job} index={index} depth={depth} ctx={ctx}
                logs={logs} progress={progress} scale={jobScale} busyKey={busyKey} rerun={rerun} />
      ))}
    </>
  );
}

function JobRow({
  dagId, runId, job, index, depth, ctx, logs, progress, scale, busyKey, rerun,
}: {
  dagId: string;
  runId: string;
  job: JobGroup;
  index: number;
  depth: number;
  ctx: GridCtx;
  logs?: Record<string, string>;
  progress?: Record<string, number>;
  scale: number;
  busyKey?: string;
  rerun: (jobKey: string, taskIds: string[], withDownstream: boolean) => Promise<void>;
}) {
  const sel: Selection = { kind: "job", dagId, runId, jobKey: job.key };
  const key = selectionKey(sel);
  const expanded = ctx.expanded.has(key);
  const no = String(index + 1).padStart(2, "0");
  const child = job.kind === "call" ? ctx.findChildRun(dagId, runId, job.key) : undefined;
  const childDag = child?.dag_id ? ctx.dagsById.get(child.dag_id) : undefined;
  const canExpand = job.kind === "job" || (job.kind === "call" && Boolean(child));
  const isRerunnable = job.kind !== "end";
  const failed = job.state === "failed";
  const started = child ? runAt(child) : job.startedAt;
  const ended = child ? child.end_date : job.endedAt;
  const typeLabel = job.kind === "job" ? `JOB (${job.steps.length}단계)`
    : job.kind === "call" ? "하위 워크플로우"
    : job.kind === "cdc" ? "CDC 지시"
    : job.kind === "marker" ? "표시 노드"
    : "워크플로우 종단";
  const outcome = jobOutcome(job, logs, progress);
  const inProgress = jobProgress(job, progress) != null;
  const logTarget: FailureLogTarget = {
    title: `${no} ${job.key}`,
    dagId,
    runId,
    runLabel: ctx.runLabel?.(dagId, runId),
    tasks: job.steps.filter((step) => step.state === "failed"),
  };
  const busy = Boolean(busyKey);
  return (
    <>
      <tr className={`lv2-r k-${job.kind === "call" ? "call" : "job"} f-${job.state ?? "none"}${ctx.selectedKey === key ? " sel" : ""}`}
          onClick={() => ctx.select(sel)}>
        <td>
          <div className="lv2-nm" style={{ paddingLeft: depth * 20 }}>
            <Toggle open={expanded} empty={!canExpand} onToggle={() => ctx.toggle(key)} />
            {job.kind === "call" && <span className="lv2-kind call">호출</span>}
            <span className="lv2-nm-text" title={job.key}>
              {no} {job.kind === "call" && childDag ? displayName(childDag) : job.key}
              {job.kind === "call" && childDag && <span className="lv2-muted lv2-nm-sub"> · {job.key}</span>}
            </span>
          </div>
        </td>
        <td className="lv2-muted">{typeLabel}</td>
        <td><StateBadge state={job.state} /></td>
        <td className="lv2-num">{formatAt(started)}</td>
        <td className="lv2-num">{formatAt(ended)}</td>
        <td><DurCell seconds={durationSeconds(started, ended)} text={duration(started, ended)} scale={scale} /></td>
        <td title={failed ? undefined : outcome}>
          {failed ? (
            <button type="button" className="lv2-btn sm danger"
                    onClick={(event) => { event.stopPropagation(); ctx.openLog(logTarget); }}>로그</button>
          ) : job.kind === "call" && child ? (
            <CallResult child={child} ctx={ctx} />
          ) : <RowsText text={outcome} live={inProgress} muted={job.state === "upstream_failed"} />}
        </td>
        <td className="lv2-num">{job.retryCount}회</td>
        <td className="lv2-muted">{child ? runTypeLabel(child) : "-"}</td>
        <td>
          {isRerunnable && (
            <div className="lv2-acts">
              <button type="button" className="lv2-btn sm" disabled={busy}
                      onClick={(event) => { event.stopPropagation(); void rerun(job.key, job.steps.map((step) => step.task_id), false); }}>
                {busyKey === job.key ? "실행 중…" : "이 작업만 실행"}
              </button>
              <button type="button" className={`lv2-btn sm${failed ? " primary" : ""}`} disabled={busy}
                      onClick={(event) => { event.stopPropagation(); void rerun(job.key, job.steps.map((step) => step.task_id), true); }}>
                이 작업부터 실행
              </button>
            </div>
          )}
        </td>
      </tr>
      {expanded && job.kind === "job" && job.steps.map((step) => {
        const stepSel: Selection = { kind: "step", dagId, runId, taskId: step.task_id };
        const stepKey = selectionKey(stepSel);
        const stepOutcome = step.state === "success" ? (logs && step.task_id in logs ? loadCountOf(logs[step.task_id]) : "…")
          : step.state === "upstream_failed" ? "선행 작업 실패로 미실행" : "-";
        return (
          <tr key={step.task_id} className={`lv2-r k-step f-${step.state ?? "none"}${ctx.selectedKey === stepKey ? " sel" : ""}`}
              onClick={() => ctx.select(stepSel)}>
            <td><div className="lv2-nm" style={{ paddingLeft: (depth + 1) * 20 + 16 }}>{stepName(step.task_id)}</div></td>
            <td className="lv2-muted">단계</td>
            <td><StateBadge state={step.state} /></td>
            <td className="lv2-num">{formatAt(step.start_date)}</td>
            <td className="lv2-num">{formatAt(step.end_date)}</td>
            <td><DurCell seconds={durationSeconds(step.start_date, step.end_date)} text={duration(step.start_date, step.end_date)} scale={durationSeconds(job.startedAt, job.endedAt)} /></td>
            <td>
              {step.state === "failed" ? (
                <button type="button" className="lv2-btn sm danger"
                        onClick={(event) => {
                          event.stopPropagation();
                          ctx.openLog({ title: `${job.key} · ${stepName(step.task_id)}`, dagId, runId,
                                        runLabel: ctx.runLabel?.(dagId, runId), tasks: [step] });
                        }}>로그</button>
              ) : <span className={step.state === "upstream_failed" ? "lv2-muted" : "lv2-num"}>{stepOutcome}</span>}
            </td>
            <td className="lv2-num">{Math.max(0, (step.try_number ?? 1) - 1)}회</td>
            <td className="lv2-muted">-</td>
            <td />
          </tr>
        );
      })}
      {expanded && job.kind === "call" && child?.dag_id && (
        <RunBody dagId={child.dag_id} runId={child.dag_run_id} runState={child.state} depth={depth + 1} ctx={ctx}
                 scale={durationSeconds(runAt(child), child.end_date)} />
      )}
    </>
  );
}

/** 하위 워크플로우 호출 줄의 적재 합계 - 그 하위 실행의 합계(그 아래 하위 포함, useRunRows). */
function CallResult({ child, ctx }: { child: AirflowDagRun; ctx: GridCtx }) {
  const { sum, probes } = useRunRows(child.dag_id ?? "", child.dag_run_id, child.state, ctx, true);
  return <><RowsText text={rowsText(sum)} live={sum.live} />{probes}</>;
}

/* ================================================================== 타임라인 */

const TICK_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 180, 360];

/**
 * 하루치 실행을 시간축에 늘어놓는다. 무엇이 겹쳐 돌았고 어디서 오래 걸렸는지를 표보다 빨리 읽기 위한 보기다.
 * 실행마다 job 막대를 한 줄씩 붙인다(하위 워크플로우 호출은 하위 실행 전체 구간으로 그린다).
 */
export function TimelineView({ runs, ctx }: { runs: AirflowDagRun[]; ctx: GridCtx }) {
  const day = runs.length ? dayjs(runAt(runs[0])).startOf("day") : undefined;
  const dayRuns = day ? runs.filter((run) => dayjs(runAt(run)).isSame(day, "day")).slice(0, 30) : [];
  const taskQueries = useQueries({
    queries: dayRuns.map((run) => ({
      queryKey: ["logv2-tasks", run.dag_id, run.dag_run_id],
      queryFn: () => listAirflowTaskInstances(run.dag_id!, run.dag_run_id),
      refetchInterval: ctx.refreshSeconds * 1000,
    })),
  });
  if (!day || !dayRuns.length) {
    return <div className="lv2-empty">표시할 실행이 없습니다. 기간을 넓혀 보세요.</div>;
  }
  const now = dayjs();
  type Row = { key: string; sel: Selection; depth: number; label: React.ReactNode; state?: string; start?: string; end?: string; isRun: boolean };
  const rows: Row[] = [];
  dayRuns.forEach((run, runIndex) => {
    const dagId = run.dag_id ?? "";
    const dag = ctx.dagsById.get(dagId);
    const sel: Selection = { kind: "run", dagId, runId: run.dag_run_id };
    rows.push({ key: selectionKey(sel), sel, depth: 0, label: <b>{dag ? displayName(dag) : dagId}</b>, state: run.state, start: runAt(run), end: run.end_date, isRun: true });
    const tasks = taskQueries[runIndex]?.data;
    (tasks ? groupJobs(tasks) : []).filter((job) => job.kind !== "end" && job.kind !== "marker").forEach((job, index) => {
      const child = job.kind === "call" ? ctx.findChildRun(dagId, run.dag_run_id, job.key) : undefined;
      const childDag = child?.dag_id ? ctx.dagsById.get(child.dag_id) : undefined;
      const jobSel: Selection = child?.dag_id
        ? { kind: "run", dagId: child.dag_id, runId: child.dag_run_id }
        : { kind: "job", dagId, runId: run.dag_run_id, jobKey: job.key };
      rows.push({
        key: `${selectionKey(sel)}::${job.key}`,
        sel: jobSel,
        depth: 1,
        label: <>{String(index + 1).padStart(2, "0")} {childDag ? <><span className="lv2-kind call">호출</span> {displayName(childDag)}</> : job.key}</>,
        state: job.state,
        start: child ? runAt(child) : job.startedAt,
        end: child ? child.end_date : job.endedAt,
        isRun: false,
      });
    });
  });
  const times = rows.flatMap((row) => [row.start, row.end ?? (isActiveState(row.state) ? now.toISOString() : undefined)])
    .filter(Boolean).map((value) => dayjs(value).valueOf());
  const lo = Math.min(...times);
  const hiRaw = Math.max(...times);
  const pad = Math.max(60_000, (hiRaw - lo) * 0.03);
  const from = lo - pad;
  const to = hiRaw + pad;
  const span = Math.max(to - from, 60_000);
  const pct = (value: number) => ((value - from) / span) * 100;
  const spanMinutes = span / 60_000;
  const stepMinutes = TICK_STEPS.find((step) => spanMinutes / step <= 10) ?? 720;
  const ticks: number[] = [];
  const firstTick = Math.ceil(from / (stepMinutes * 60_000)) * stepMinutes * 60_000;
  for (let at = firstTick; at <= to; at += stepMinutes * 60_000) ticks.push(at);
  const nowAt = now.valueOf();
  const showNow = nowAt >= from && nowAt <= to;
  const runEndOf = (index: number) => {
    for (let i = index; i >= 0; i -= 1) if (rows[i].isRun) return rows[i].end;
    return undefined;
  };
  return (
    <div className="lv2-tl">
      <div className="lv2-tl-head">
        <div className="lab">워크플로우 / Job <span className="lv2-muted">({day.format("YYYY-MM-DD")})</span></div>
        <div className="axis">
          {ticks.map((at) => <span key={at} className="tick" style={{ left: `${pct(at)}%` }}>{dayjs(at).format("HH:mm")}</span>)}
        </div>
      </div>
      {rows.map((row, index) => {
        const end = row.end ?? (isActiveState(row.state) ? now.toISOString() : undefined);
        let bar: React.ReactNode = null;
        if (row.start) {
          const a = dayjs(row.start).valueOf();
          const b = end ? dayjs(end).valueOf() : a;
          bar = (
            <span className={`bar ${row.state ?? "none"}${row.isRun ? " run" : ""}`}
                  style={{ left: `${pct(a)}%`, width: `${Math.max(0.6, ((b - a) / span) * 100)}%` }}
                  title={`${formatAt(row.start)} – ${formatAt(row.end)} (${duration(row.start, end)})`}>
              {duration(row.start, end)}
            </span>
          );
        } else if (row.state === "upstream_failed") {
          const at = dayjs(runEndOf(index) ?? now).valueOf();
          bar = <span className="bar upstream_failed" style={{ left: `${Math.min(94, pct(at))}%`, width: "6%" }}>미실행</span>;
        }
        const selKey = selectionKey(row.sel);
        return (
          <Fragment key={row.key}>
            <div className={`lv2-tl-row${ctx.selectedKey === selKey ? " sel" : ""}`} onClick={() => ctx.select(row.sel)}>
              <div className="lab" style={{ paddingLeft: 12 + row.depth * 18 }}>
                <StateBadge state={row.state} /> <span className="lv2-nm-text">{row.label}</span>
              </div>
              <div className="track">
                {bar}
                {showNow && <i className="now" style={{ left: `${pct(nowAt)}%` }} />}
              </div>
            </div>
          </Fragment>
        );
      })}
      <div className="lv2-tl-legend">
        <span><i style={{ background: "#22c55e" }} />성공</span>
        <span><i style={{ background: "#ef4444" }} />실패</span>
        <span><i style={{ background: "#2563eb" }} />실행 중</span>
        <span><i className="stripe" />선행 실패로 미실행</span>
        {showNow && <span style={{ color: "#ff3b30" }}>┆ 지금</span>}
        {runs.length > dayRuns.length && <span className="lv2-muted">가장 최근 날짜의 실행만 표시합니다(최대 30건).</span>}
      </div>
    </div>
  );
}

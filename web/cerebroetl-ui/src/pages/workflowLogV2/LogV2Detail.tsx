import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DatePicker } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import { Link } from "react-router-dom";
import type { AirflowDagRun } from "../../api/platform";
import { getWorkflow, type WorkflowSummary } from "../../api/workflows";
import type { RealtimePipelineMetricResponse } from "../../api/dashboard";
import type { PipelineResponse, PipelineRuntimeStatusResponse } from "../../types/pipeline";
import { loadModeLabel } from "../../types/pipeline";
import type { FailureLogTarget } from "../../components/TaskFailureLogModal";
import { formatRelativeTime, renderLag, renderRuntimeStatus, sourceLabel, targetLabel } from "../../utils/cdcPresentation";
import { scheduleDescription } from "../../utils/schedulePreset";
import {
  categoryOf,
  displayName,
  duration,
  folderName,
  formatAt,
  jobOutcome,
  loadCountOf,
  runAt,
  runTypeLabel,
  selectionKey,
  stateLabel,
  stepName,
  type DashboardDag,
  type DetailTab,
  type JobGroup,
  type Selection,
} from "./logV2Data";
import { useRerun, useRunTasks } from "./logV2Hooks";
import { InlineFailureLog, StateBadge, Tile, WatchingRules } from "./LogV2Parts";
import { GridHead, RunRow, type GridCtx } from "./LogV2Grid";

interface CommonProps {
  workflows: WorkflowSummary[];
  dagsById: Map<string, DashboardDag>;
  runsByDag: Map<string, AirflowDagRun[]>;
  findRun: (dagId: string, runId: string) => AirflowDagRun | undefined;
  findChildRun: GridCtx["findChildRun"];
  tab: DetailTab;
  onTab: (tab: DetailTab) => void;
  open: boolean;
  onToggleOpen: () => void;
  refreshSeconds: number;
  canRun: boolean;
  openLog: (target: FailureLogTarget) => void;
  onExecution: (dag: DashboardDag) => void;
}

function CollapseButton({ open, onToggle }: { open: boolean; onToggle: () => void }) {
  return (
    <button type="button" className="lv2-btn ghost sm" title="상세 창 접기/펼치기" onClick={onToggle}>
      {open ? "▾ 접기" : "▴ 펼치기"}
    </button>
  );
}

type TabDef = { key: DetailTab; label: string; count?: number; disabled?: boolean };

function Tabs({ tabs, active, onTab }: { tabs: TabDef[]; active: DetailTab; onTab: (tab: DetailTab) => void }) {
  return (
    <div className="lv2-detail-tabs" role="tablist">
      {tabs.map((tab) => (
        <button key={tab.key} type="button" role="tab" aria-selected={active === tab.key}
                className={active === tab.key ? "on" : ""} disabled={tab.disabled}
                onClick={() => onTab(tab.key)}>
          {tab.label}{tab.count ? <em>{tab.count}</em> : null}
        </button>
      ))}
    </div>
  );
}

/** 탭이 비활성이면 요약으로 돌린다(다른 행을 고르면 그 탭이 없을 수 있다). */
function effectiveTab(tabs: TabDef[], tab: DetailTab): DetailTab {
  return tabs.some((item) => item.key === tab && !item.disabled) ? tab : "summary";
}

/* ================================================================== 실행 이력 */

type HistoryPeriod = "today" | "7d" | "30d" | "custom";

/**
 * 실행 이력 탭. 기본은 오늘 하루(기존 실행 이력 창과 같음)이고, 줄을 펼치면 목록과 같은 job·단계가 나온다.
 * 이 표의 펼침은 가운데 목록과 따로 논다 - 이력을 뒤지는 동안 위 목록의 선택이 바뀌지 않게.
 */
function RunHistory({ dagId, runs, common }: { dagId?: string; runs: AirflowDagRun[]; common: CommonProps }) {
  const [period, setPeriod] = useState<HistoryPeriod>("today");
  const [custom, setCustom] = useState<[Dayjs, Dayjs]>([dayjs().startOf("day"), dayjs().endOf("day")]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selectedKey, setSelectedKey] = useState<string>();
  const toggle = (key: string) => setExpanded((current) => {
    const next = new Set(current);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    return next;
  });
  const from = period === "7d" ? dayjs().subtract(6, "day").startOf("day")
    : period === "30d" ? dayjs().subtract(29, "day").startOf("day")
    : period === "custom" ? custom[0] : dayjs().startOf("day");
  const to = period === "custom" ? custom[1] : dayjs().endOf("day");
  const visible = runs.filter((run) => {
    const at = runAt(run);
    return Boolean(at) && !dayjs(at).isBefore(from) && !dayjs(at).isAfter(to);
  });
  const ctx: GridCtx = {
    dagsById: common.dagsById,
    findChildRun: common.findChildRun,
    expanded,
    toggle,
    selectedKey,
    select: (sel) => {
      const key = selectionKey(sel);
      setSelectedKey(key);
      if (sel.kind !== "step") toggle(key);
    },
    openLog: common.openLog,
    openTab: (sel, tab) => {
      if (tab === "log" && sel.kind === "run") {
        const key = selectionKey(sel);
        setSelectedKey(key);
        setExpanded((current) => new Set(current).add(key));
      }
    },
    refreshSeconds: common.refreshSeconds,
  };
  const scale = Math.max(...visible.map((run) => (run.start_date && run.end_date ? dayjs(run.end_date).diff(run.start_date, "second") : 0)), 1);
  if (!dagId) {
    return <div className="lv2-empty">실행 이력이 없습니다.</div>;
  }
  return (
    <div>
      <div className="lv2-history-bar">
        <span className="lv2-ctx-label">기간</span>
        <div className="lv2-seg">
          {([["today", "오늘"], ["7d", "최근 7일"], ["30d", "최근 30일"], ["custom", "📅 지정"]] as const).map(([key, label]) => (
            <button key={key} type="button" className={period === key ? "on" : ""} onClick={() => setPeriod(key)}>{label}</button>
          ))}
        </div>
        {period === "custom" && (
          <DatePicker.RangePicker size="small" value={custom} allowClear={false}
                                  onChange={(value) => { if (value?.[0] && value?.[1]) setCustom([value[0].startOf("day"), value[1].endOf("day")]); }} />
        )}
        <span className="lv2-muted">{visible.length}건</span>
      </div>
      {visible.length === 0 ? <div className="lv2-empty">이 기간에 실행 이력이 없습니다.</div> : (
        <table className="lv2-grid">
          <GridHead />
          <tbody>
            {visible.map((run) => <RunRow key={run.dag_run_id} run={run} ctx={ctx} scale={scale} showDate />)}
          </tbody>
        </table>
      )}
    </div>
  );
}

/* ================================================================== ETL */

function jobTypeLabel(job: JobGroup) {
  return job.kind === "job" ? `JOB (${job.steps.length}단계)`
    : job.kind === "call" ? "하위 워크플로우"
    : job.kind === "marker" ? "표시 노드" : "워크플로우 종단";
}

function EtlProps({ dag }: { dag?: DashboardDag }) {
  if (!dag) return <div className="lv2-empty">DAG 정보를 찾을 수 없습니다.</div>;
  return (
    <div>
      <dl className="lv2-props">
        <dt>이름</dt><dd>{displayName(dag)}</dd>
        <dt>DAG ID</dt><dd>{dag.dag_id}</dd>
        <dt>업무 구분</dt><dd>{categoryOf(dag)}</dd>
        <dt>업무 폴더</dt><dd>{folderName(dag)}</dd>
        <dt>스케줄</dt><dd title={dag.timetable_summary}>{scheduleDescription(dag.timetable_summary)}</dd>
        <dt>상태</dt><dd>{dag.is_paused ? "일시 중지" : dag.is_active === false ? "비활성" : "활성"}</dd>
        <dt>다음 실행</dt><dd>{formatAt(dag.next_dagrun ?? dag.next_dagrun_run_after, "YYYY-MM-DD HH:mm")}</dd>
        <dt>설명</dt><dd>{dag.description || "-"}</dd>
      </dl>
      <EtlLinks dag={dag} />
    </div>
  );
}

function EtlLinks({ dag }: { dag: DashboardDag }) {
  return (
    <div className="lv2-links">
      <Link className="lv2-btn sm" to={`/airflow/manage?dagId=${encodeURIComponent(dag.dag_id)}`}>상세보기 ↗</Link>
      <Link className="lv2-btn sm" to={dag.nifi_process_group_id ? `/etl/manage?processGroupId=${encodeURIComponent(dag.nifi_process_group_id)}` : "/etl/manage"}>ETL 캔버스 ↗</Link>
    </div>
  );
}

function EtlDetail({ selection, common, onSelect }: { selection: Selection; common: CommonProps; onSelect: (sel: Selection) => void }) {
  const { dagId, runId } = selection;
  const dag = common.dagsById.get(dagId);
  const run = common.findRun(dagId, runId);
  const { tasksQuery, tasks, jobs, logs } = useRunTasks(dagId, runId, { enabled: true, refreshSeconds: common.refreshSeconds });
  const { busyKey, rerun } = useRerun(dagId, runId, tasks, () => tasksQuery.refetch());
  const workflow = common.workflows.find((item) => item.dagId === dagId);
  const workflowDetail = useQuery({
    queryKey: ["workflow-detail", workflow?.id],
    queryFn: () => getWorkflow(workflow!.id),
    enabled: Boolean(workflow) && common.tab === "rules",
  });
  const jobIds = (workflowDetail.data?.nodes ?? []).map((node) => node.jobId).filter((id): id is number => typeof id === "number");

  const job = selection.kind === "job" ? jobs?.find((item) => item.key === selection.jobKey)
    : selection.kind === "step" ? jobs?.find((item) => item.steps.some((step) => step.task_id === selection.taskId))
    : undefined;
  const step = selection.kind === "step" ? tasks?.find((task) => task.task_id === selection.taskId) : undefined;
  const child = job?.kind === "call" ? common.findChildRun(dagId, runId, job.key) : undefined;
  const wfName = dag ? displayName(dag) : dagId;
  const runStart = runAt(run);
  const jobIndex = job && jobs ? jobs.indexOf(job) : -1;
  const jobNo = jobIndex >= 0 ? `${String(jobIndex + 1).padStart(2, "0")} ` : "";

  const failedTasks = selection.kind === "run" ? (tasks ?? []).filter((task) => task.state === "failed")
    : selection.kind === "job" ? (job?.steps ?? []).filter((task) => task.state === "failed")
    : step?.state === "failed" ? [step] : [];
  const kids = selection.kind === "run" ? (jobs?.length ?? 0) : selection.kind === "job" && job?.kind === "job" ? job.steps.length : 0;
  const isStep = selection.kind === "step";
  const tabs: TabDef[] = [
    { key: "summary", label: "요약" },
    { key: "children", label: selection.kind === "job" ? "단계" : "작업", count: kids, disabled: !kids },
    { key: "log", label: "로그", count: failedTasks.length, disabled: !failedTasks.length },
    { key: "history", label: "실행 이력", count: (common.runsByDag.get(dagId) ?? []).length, disabled: isStep },
    { key: "rules", label: "알림 규칙", disabled: isStep },
    { key: "props", label: "속성", disabled: isStep },
  ];
  const tab = effectiveTab(tabs, common.tab);

  const kindLabel = selection.kind === "run" ? (run && runTypeLabel(run) === "상위 워크플로우 호출" ? "하위 워크플로우 실행" : "워크플로우 실행")
    : selection.kind === "step" ? "단계"
    : job ? { job: "Job", call: "하위 워크플로우 호출", marker: "표시 노드", end: "워크플로우 종단" }[job.kind] : "Job";
  const title = selection.kind === "run" ? `${wfName} · ${formatAt(runStart, "YYYY-MM-DD HH:mm:ss")}`
    : selection.kind === "job" ? `${jobNo}${selection.jobKey}`
    : `${job?.key ?? ""} · ${stepName(selection.taskId)}`;
  const state = selection.kind === "run" ? run?.state : selection.kind === "job" ? job?.state : step?.state;
  const sub = selection.kind === "run" ? dagId : `${wfName} · ${formatAt(runStart, "YYYY-MM-DD HH:mm:ss")} 실행`;
  const logTitle = selection.kind === "run" ? wfName : title;

  const rerunButtons = job && job.kind !== "end" && selection.kind === "job" && (
    <>
      <button type="button" className="lv2-btn sm" disabled={Boolean(busyKey)}
              onClick={() => void rerun(job.key, job.steps.map((item) => item.task_id), false)}>
        {busyKey === job.key ? "실행 중…" : "이 작업만 실행"}
      </button>
      <button type="button" className={`lv2-btn sm${job.state === "failed" ? " primary" : ""}`} disabled={Boolean(busyKey)}
              onClick={() => void rerun(job.key, job.steps.map((item) => item.task_id), true)}>
        이 작업부터 실행
      </button>
    </>
  );

  const head = (
    <div className="lv2-detail-head">
      <div className="lv2-detail-title">
        <span className={`lv2-kind${selection.kind === "run" ? " run" : job?.kind === "call" ? " call" : ""}`}>{kindLabel}</span>
        <b title={title}>{title}</b>
        <StateBadge state={state} />
        <small>{sub}</small>
      </div>
      <div className="lv2-detail-acts">
        {failedTasks.length > 0 && <button type="button" className="lv2-btn sm danger" onClick={() => common.onTab("log")}>로그</button>}
        {rerunButtons}
        {selection.kind === "run" && dag && common.canRun && (
          <button type="button" className="lv2-btn sm" onClick={() => common.onExecution(dag)}>실행 설정</button>
        )}
        {dag && <Link className="lv2-btn sm" to={`/airflow/manage?dagId=${encodeURIComponent(dag.dag_id)}`}>상세보기 ↗</Link>}
        {dag && <Link className="lv2-btn sm" to={dag.nifi_process_group_id ? `/etl/manage?processGroupId=${encodeURIComponent(dag.nifi_process_group_id)}` : "/etl/manage"}>ETL 캔버스 ↗</Link>}
        <CollapseButton open={common.open} onToggle={common.onToggleOpen} />
      </div>
    </div>
  );

  let body: React.ReactNode = null;
  if (tab === "summary") {
    if (selection.kind === "run") {
      const loadJobs = (jobs ?? []).filter((item) => item.kind === "job");
      const okJobs = (jobs ?? []).filter((item) => item.kind !== "end" && item.state === "success").length;
      const badJobs = (jobs ?? []).filter((item) => item.state === "failed");
      let total = 0;
      let known = false;
      loadJobs.forEach((item) => {
        const text = jobOutcome(item, logs);
        const match = text.match(/^([\d,]+)행$/);
        if (match) { total += Number(match[1].replace(/,/g, "")); known = true; }
      });
      const upstream = (jobs ?? []).filter((item) => item.state === "upstream_failed");
      body = (
        <>
          <div className="lv2-tiles">
            <Tile label="상태" value={stateLabel(run?.state)} bad={run?.state === "failed"} />
            <Tile label="실행 유형" value={run ? runTypeLabel(run) : "-"} />
            <Tile label="시작" value={formatAt(runStart, "MM-DD HH:mm:ss")} />
            <Tile label="종료" value={formatAt(run?.end_date, "MM-DD HH:mm:ss")} />
            <Tile label="소요" value={duration(runStart, run?.end_date)} />
            <Tile label="작업 성공 / 실패" value={jobs ? `${okJobs} / ${badJobs.length}` : "…"} bad={badJobs.length > 0} />
            <Tile label="적재 합계" value={!jobs || (loadJobs.length && !logs) ? "…" : known ? `${total.toLocaleString()}행` : "-"} />
            <Tile label="재시도" value={jobs ? `${jobs.reduce((sum, item) => sum + item.retryCount, 0)}회` : "…"} />
            <Tile label="다음 실행" value={formatAt(dag?.next_dagrun ?? dag?.next_dagrun_run_after, "MM-DD HH:mm")} />
          </div>
          {badJobs.length > 0 && (
            <div className="lv2-errbox">
              <div className="t">실패 {badJobs.length}건 — {badJobs.map((item) => item.key).join(", ")}</div>
              {upstream.length > 0 && <code>선행 실패로 미실행: {upstream.map((item) => item.key).join(", ")}</code>}
              <div className="lv2-errbox-acts">
                <button type="button" className="lv2-btn sm danger" onClick={() => common.onTab("log")}>로그 보기</button>
                {badJobs.map((item) => (
                  <button key={item.key} type="button" className="lv2-btn sm"
                          onClick={() => onSelect({ kind: "job", dagId, runId, jobKey: item.key })}>{item.key} 보기</button>
                ))}
              </div>
            </div>
          )}
        </>
      );
    } else if (selection.kind === "job") {
      if (!job) {
        body = <div className="lv2-empty">{tasksQuery.isLoading ? "불러오는 중입니다." : "이 실행에서 작업을 찾을 수 없습니다."}</div>;
      } else {
        const failedSteps = job.steps.filter((item) => item.state === "failed");
        const outcome = jobOutcome(job, logs);
        body = (
          <>
            <div className="lv2-tiles">
              <Tile label="상태" value={stateLabel(job.state)} bad={job.state === "failed"} />
              <Tile label="유형" value={jobTypeLabel(job)} />
              <Tile label="시작" value={formatAt(child ? runAt(child) : job.startedAt)} />
              <Tile label="종료" value={formatAt(child ? child.end_date : job.endedAt)} />
              <Tile label="소요" value={child ? duration(runAt(child), child.end_date) : duration(job.startedAt, job.endedAt)} />
              {job.kind === "job" && <Tile label="적재 건수" value={outcome || "-"} />}
              <Tile label="재시도" value={`${job.retryCount}회`} />
              {job.kind === "job" && <Tile label="실패 단계" value={failedSteps.map((item) => stepName(item.task_id)).join(", ") || "-"} bad={failedSteps.length > 0} />}
              {child?.dag_id && <Tile label="하위 워크플로우" value={common.dagsById.get(child.dag_id) ? displayName(common.dagsById.get(child.dag_id)!) : child.dag_id} />}
            </div>
            {job.kind === "call" && child?.dag_id && (
              <div className="lv2-links">
                <button type="button" className="lv2-btn sm" onClick={() => onSelect({ kind: "run", dagId: child.dag_id!, runId: child.dag_run_id })}>
                  하위 실행 보기
                </button>
              </div>
            )}
            {job.state === "failed" && (
              <div className="lv2-errbox">
                <div className="t">실패 원인</div>
                <code>{failedSteps.map((item) => `${stepName(item.task_id)} 단계 실패 (시도 ${item.try_number ?? 1})`).join("\n")}</code>
                <div className="lv2-errbox-acts">
                  <button type="button" className="lv2-btn sm danger" onClick={() => common.onTab("log")}>전체 로그 보기</button>
                  <button type="button" className="lv2-btn sm primary" disabled={Boolean(busyKey)}
                          onClick={() => void rerun(job.key, job.steps.map((item) => item.task_id), true)}>이 작업부터 실행</button>
                </div>
              </div>
            )}
            {job.state === "upstream_failed" && (
              <div className="lv2-errbox is-upstream">
                <div className="t">선행 작업 실패로 미실행</div>
                <code>앞 작업이 실패해 이 작업은 돌지 않았습니다.</code>
              </div>
            )}
          </>
        );
      }
    } else {
      body = !step ? <div className="lv2-empty">{tasksQuery.isLoading ? "불러오는 중입니다." : "단계를 찾을 수 없습니다."}</div> : (
        <>
          <div className="lv2-tiles">
            <Tile label="상태" value={stateLabel(step.state)} bad={step.state === "failed"} />
            <Tile label="시작" value={formatAt(step.start_date)} />
            <Tile label="종료" value={formatAt(step.end_date)} />
            <Tile label="소요" value={duration(step.start_date, step.end_date)} />
            <Tile label="결과" value={step.state === "success" ? (logs ? loadCountOf(logs[step.task_id]) : "…") : step.state === "upstream_failed" ? "선행 작업 실패로 미실행" : "-"} />
            <Tile label="시도" value={`${step.try_number ?? 1}${step.max_tries != null ? ` / ${step.max_tries + 1}` : ""}`} />
            <Tile label="재시도" value={`${Math.max(0, (step.try_number ?? 1) - 1)}회`} />
          </div>
          {step.state === "failed" && (
            <div className="lv2-errbox">
              <div className="t">이 단계가 실패했습니다</div>
              <div className="lv2-errbox-acts"><button type="button" className="lv2-btn sm danger" onClick={() => common.onTab("log")}>로그 보기</button></div>
            </div>
          )}
        </>
      );
    }
  } else if (tab === "children") {
    if (selection.kind === "run") {
      body = (
        <table className="lv2-grid">
          <thead><tr><th>작업(Task)</th><th>유형</th><th>시작</th><th>상태</th><th>적재/실패</th><th>소요</th><th>재시도</th></tr></thead>
          <tbody>
            {(jobs ?? []).map((item, index) => (
              <tr key={item.key} className={`lv2-r f-${item.state ?? "none"}`} onClick={() => onSelect({ kind: "job", dagId, runId, jobKey: item.key })}>
                <td>{String(index + 1).padStart(2, "0")} {item.key}</td>
                <td className="lv2-muted">{jobTypeLabel(item)}</td>
                <td className="lv2-num">{formatAt(item.startedAt)}</td>
                <td><StateBadge state={item.state} /></td>
                <td>{item.state === "failed" ? (
                  <button type="button" className="lv2-btn sm danger"
                          onClick={(event) => { event.stopPropagation(); common.openLog({ title: `${String(index + 1).padStart(2, "0")} ${item.key}`, dagId, runId, tasks: item.steps.filter((s) => s.state === "failed") }); }}>로그</button>
                ) : jobOutcome(item, logs)}</td>
                <td className="lv2-num">{duration(item.startedAt, item.endedAt)}</td>
                <td className="lv2-num">{item.retryCount}회</td>
              </tr>
            ))}
          </tbody>
        </table>
      );
    } else if (job) {
      body = (
        <table className="lv2-grid">
          <thead><tr><th>단계</th><th>시작</th><th>종료</th><th>상태</th><th>적재/실패</th><th>소요</th><th>재시도</th></tr></thead>
          <tbody>
            {job.steps.map((item) => (
              <tr key={item.task_id} className={`lv2-r f-${item.state ?? "none"}`} onClick={() => onSelect({ kind: "step", dagId, runId, taskId: item.task_id })}>
                <td>{stepName(item.task_id)}</td>
                <td className="lv2-num">{formatAt(item.start_date)}</td>
                <td className="lv2-num">{formatAt(item.end_date)}</td>
                <td><StateBadge state={item.state} /></td>
                <td>{item.state === "failed" ? (
                  <button type="button" className="lv2-btn sm danger"
                          onClick={(event) => { event.stopPropagation(); common.openLog({ title: `${job.key} · ${stepName(item.task_id)}`, dagId, runId, tasks: [item] }); }}>로그</button>
                ) : item.state === "success" ? (logs ? loadCountOf(logs[item.task_id]) : "…") : item.state === "upstream_failed" ? "선행 작업 실패로 미실행" : "-"}</td>
                <td className="lv2-num">{duration(item.start_date, item.end_date)}</td>
                <td className="lv2-num">{Math.max(0, (item.try_number ?? 1) - 1)}회</td>
              </tr>
            ))}
          </tbody>
        </table>
      );
    }
  } else if (tab === "log") {
    body = <InlineFailureLog key={selectionKey(selection)} title={logTitle} dagId={dagId} runId={runId} tasks={failedTasks} onOpenFull={common.openLog} />;
  } else if (tab === "history") {
    body = <RunHistory dagId={dagId} runs={common.runsByDag.get(dagId) ?? []} common={common} />;
  } else if (tab === "rules") {
    body = workflowDetail.isLoading ? <div className="lv2-empty">불러오는 중입니다.</div> : <WatchingRules target="ETL" ids={jobIds} />;
  } else {
    body = <EtlProps dag={dag} />;
  }

  return (
    <>
      {head}
      {common.open && <Tabs tabs={tabs} active={tab} onTab={common.onTab} />}
      {common.open && <div className="lv2-detail-body">{body}</div>}
    </>
  );
}

/* ================================================================== CDC */

function CdcDetail({
  pipeline, controlDag, runtime, metric, common,
}: {
  pipeline: PipelineResponse;
  controlDag?: DashboardDag;
  runtime?: PipelineRuntimeStatusResponse;
  metric?: RealtimePipelineMetricResponse;
  common: CommonProps;
}) {
  const controlRuns = controlDag ? (common.runsByDag.get(controlDag.dag_id) ?? []) : [];
  const latest = controlRuns[0];
  const tabs: TabDef[] = [
    { key: "summary", label: "요약" },
    { key: "history", label: "실행 이력", count: controlRuns.length, disabled: !controlDag },
    { key: "rules", label: "알림 규칙" },
    { key: "props", label: "속성" },
  ];
  const tab = effectiveTab(tabs, common.tab);
  const links = (
    <>
      {controlDag && <Link className="lv2-btn sm" to={`/airflow/manage?dagId=${encodeURIComponent(controlDag.dag_id)}`}>상세보기 ↗</Link>}
      <Link className="lv2-btn sm" to="/cdc/pipelines">CDC 파이프라인 ↗</Link>
    </>
  );
  let body: React.ReactNode;
  if (tab === "summary") {
    const lagging = (metric?.consumerLag ?? 0) > 0;
    body = (
      <>
        <div className="lv2-tiles">
          <Tile label="제어 DAG 상태" value={stateLabel(latest?.state)} bad={latest?.state === "failed"} />
          <Tile label="최근 제어 실행" value={latest ? `${formatAt(runAt(latest), "MM-DD HH:mm:ss")} · ${latest.conf?.action ?? runTypeLabel(latest)}` : "-"} />
          <Tile label="커넥터 상태" value={renderRuntimeStatus(pipeline, runtime)} bad={Boolean(runtime && runtime.runtimeStatus !== "RUNNING")} />
          <Tile label="지연" value={renderLag(metric)} bad={lagging} />
          <Tile label="처리율" value={metric ? `${metric.throughputPerSecond.toLocaleString()} /s` : "-"} />
          <Tile label="마지막 처리" value={formatRelativeTime(metric?.lastProgressAt ?? null)} />
          <Tile label="Topic" value={pipeline.topicName} />
          <Tile label="상태 확인 시각" value={formatAt(runtime?.runtimeCheckedAt ?? undefined, "MM-DD HH:mm:ss")} />
        </div>
        {runtime?.runtimeStatusReason && (
          <div className="lv2-errbox"><div className="t">커넥터 상태 사유</div><code>{runtime.runtimeStatusReason}</code></div>
        )}
      </>
    );
  } else if (tab === "history") {
    body = <RunHistory dagId={controlDag?.dag_id} runs={controlRuns} common={common} />;
  } else if (tab === "rules") {
    body = <WatchingRules target="CDC" ids={[pipeline.id]} />;
  } else {
    body = (
      <div>
        <dl className="lv2-props">
          <dt>이름</dt><dd>{pipeline.name}</dd>
          <dt>유형</dt><dd>{pipeline.pipelineType}</dd>
          <dt>소스</dt><dd>{sourceLabel(pipeline)}</dd>
          <dt>타겟</dt><dd>{targetLabel(pipeline)}</dd>
          <dt>Topic</dt><dd>{pipeline.topicName}</dd>
          {pipeline.pipelineType === "TABLE_CDC" && <>
            <dt>스냅샷 모드</dt><dd>{pipeline.snapshotMode === "NO_DATA" ? "기존 데이터 미적재 · 이후 CDC" : "초기 적재 후 CDC"}</dd>
            <dt>적재 방식</dt><dd>{loadModeLabel(pipeline.loadMode, pipeline.deltaOpColumn)}</dd>
          </>}
          <dt>커넥터 상태</dt><dd>{renderRuntimeStatus(pipeline, runtime)}</dd>
          <dt>제어 DAG</dt><dd>{controlDag?.dag_id ?? "-"}</dd>
          <dt>설명</dt><dd>{pipeline.description ?? "-"}</dd>
          <dt>생성 시각</dt><dd>{pipeline.createdAt}</dd>
          <dt>수정 시각</dt><dd>{pipeline.updatedAt}</dd>
        </dl>
        <div className="lv2-links">{links}</div>
      </div>
    );
  }
  return (
    <>
      <div className="lv2-detail-head">
        <div className="lv2-detail-title">
          <span className="lv2-dom cdc"><i />CDC</span>
          <b title={pipeline.name}>{pipeline.name}</b>
          <StateBadge state={latest?.state} />
          <small>{controlDag ? `제어 DAG ${controlDag.dag_id}` : "제어 DAG 없음"}</small>
        </div>
        <div className="lv2-detail-acts">
          {controlDag && common.canRun && (
            <button type="button" className="lv2-btn sm" onClick={() => common.onExecution(controlDag)}>실행 설정</button>
          )}
          {links}
          <CollapseButton open={common.open} onToggle={common.onToggleOpen} />
        </div>
      </div>
      {common.open && <Tabs tabs={tabs} active={tab} onTab={common.onTab} />}
      {common.open && <div className="lv2-detail-body">{body}</div>}
    </>
  );
}

/* ================================================================== 상세 창 */

export function LogV2Detail({
  mode,
  selection,
  onSelect,
  pipeline,
  controlDag,
  runtime,
  metric,
  ...common
}: CommonProps & {
  mode: "etl" | "cdc";
  selection?: Selection;
  onSelect: (sel: Selection) => void;
  pipeline?: PipelineResponse;
  controlDag?: DashboardDag;
  runtime?: PipelineRuntimeStatusResponse;
  metric?: RealtimePipelineMetricResponse;
}) {
  if (mode === "cdc") {
    return pipeline
      ? <CdcDetail pipeline={pipeline} controlDag={controlDag} runtime={runtime} metric={metric} common={common} />
      : <div className="lv2-detail-head"><div className="lv2-detail-title"><b className="lv2-muted">파이프라인을 선택하세요</b></div><div className="lv2-detail-acts"><CollapseButton open={common.open} onToggle={common.onToggleOpen} /></div></div>;
  }
  return selection
    ? <EtlDetail key={selectionKey(selection)} selection={selection} common={common} onSelect={onSelect} />
    : <div className="lv2-detail-head"><div className="lv2-detail-title"><b className="lv2-muted">목록에서 실행·작업을 선택하세요</b></div><div className="lv2-detail-acts"><CollapseButton open={common.open} onToggle={common.onToggleOpen} /></div></div>;
}

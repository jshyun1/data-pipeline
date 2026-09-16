import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ConfigProvider, DatePicker, Empty, message, Select, Spin, Table } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import { useSearchParams } from "react-router-dom";
import { listAllAirflowDagRuns, syncAirflowDagCatalog, type AirflowDagRun } from "../../api/platform";
import { listWorkflows } from "../../api/workflows";
import { listConnections } from "../../api/connections";
import { getRealtimePipelineMetrics } from "../../api/dashboard";
import { listPipelineRuntimeStatuses, listPipelines } from "../../api/pipelines";
import type { PipelineResponse } from "../../types/pipeline";
import { cdcPipelineColumns } from "../../utils/cdcPresentation";
import { extractKafkaPipelineId } from "../../utils/dagHistory";
import { parentWorkflowPathOf } from "../../utils/workflowTree";
import { useAuth } from "../../auth/AuthContext";
import { WfIcon, WorkflowPageHeader, type WfIconName } from "../../components/WorkflowPageHeader";
import { TaskFailureLogModal, type FailureLogTarget } from "../../components/TaskFailureLogModal";
import { cerebroBrandTheme } from "../../theme/cerebro";
import {
  CATEGORY_ORDER,
  REFRESH_OPTIONS,
  STATUS_METRIC_LABEL,
  categoryOf,
  childRunIdOf,
  displayName,
  durationSeconds,
  groupRuns,
  loadDashboard,
  matchesMetric,
  parentOfRun,
  periodRange,
  runAt,
  selectionKey,
  type BusinessCategory,
  type DashboardDag,
  type DetailTab,
  type ListScope,
  type PeriodKey,
  type Selection,
  type StatusMetric,
} from "./logV2Data";
import { LogV2Tree } from "./LogV2Tree";
import { GridHead, RunRow, TimelineView, type GridCtx } from "./LogV2Grid";
import { LogV2Detail } from "./LogV2Detail";
import { ExecutionSettingsModal, StateBadge } from "./LogV2Parts";

type StateFilter = "all" | "running" | "success" | "failed" | "queued";

const CURTAIN_KEY = "cerebro.wfLogV2.curtain";
const DETAIL_HEIGHT_KEY = "cerebro.wfLogV2.detailHeight";

function readStorage(key: string) {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStorage(key: string, value: string) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // 저장이 막힌 브라우저(사생활 보호 모드 등)에서는 기억만 못 할 뿐 화면은 그대로 동작한다.
  }
}

interface MetricCounts {
  running: number;
  success: number;
  failed: number;
  waiting: number;
  fresh: number;
  total: number;
}

function countMetrics(dags: DashboardDag[], runsByDag: Map<string, AirflowDagRun[]>): MetricCounts {
  const today = dayjs().startOf("day");
  const counts: MetricCounts = { running: 0, success: 0, failed: 0, waiting: 0, fresh: 0, total: dags.length };
  for (const dag of dags) {
    const runs = runsByDag.get(dag.dag_id) ?? [];
    if (matchesMetric("running", dag, runs, today)) counts.running += 1;
    if (matchesMetric("success", dag, runs, today)) counts.success += 1;
    if (matchesMetric("failed", dag, runs, today)) counts.failed += 1;
    if (matchesMetric("waiting", dag, runs, today)) counts.waiting += 1;
    if (matchesMetric("new", dag, runs, today)) counts.fresh += 1;
  }
  return counts;
}

/** 업무별 상태 카드 - 기존 로그 화면과 같은 마크업·같은 숫자(당일 · 작업 기준)다. */
function StatusCard({
  category, counts, activeMetric, onMetricClick,
}: {
  category: BusinessCategory;
  counts: MetricCounts;
  activeMetric?: StatusMetric;
  onMetricClick: (metric: StatusMetric) => void;
}) {
  const domain = category === "CDC" ? "cdc" : "etl";
  const metric = (key: StatusMetric, icon: WfIconName, label: string, value: number, tone: string) => (
    <button type="button" className={`airflow-business-metric${activeMetric === key ? " active" : ""}`}
            aria-pressed={activeMetric === key} onClick={() => onMetricClick(key)}>
      <span className="airflow-business-metric-label">
        <WfIcon name={icon} size={12} strokeWidth={key === "all" ? 2.5 : 3} className={key === "all" ? undefined : `wf-tone-${tone}`} />
        {label}
      </span>
      <strong className={`airflow-business-metric-value wf-tone-${value === 0 && key !== "all" ? "muted" : tone}`}>{value}</strong>
    </button>
  );
  const metrics = [
    metric("all", "grid", "전체 작업", counts.total, domain === "cdc" ? "blue" : "red"),
    metric("running", "play", "실행 중", counts.running, "blue"),
    ...(category === "ETL" ? [metric("success", "check", "성공", counts.success, "green")] : []),
    metric("failed", "x", "실패", counts.failed, "red"),
    ...(category === "CDC" ? [metric("waiting", "clock", "대기", counts.waiting, "amber")] : []),
    metric("new", "plus", "신규", counts.fresh, "purple"),
  ];
  return (
    <section className={`airflow-business-card airflow-business-card--${domain}`}>
      <strong className={`airflow-business-name airflow-business-name--${domain}`}><span className="domain-dot" />{category}</strong>
      <div className="airflow-business-metrics">
        {metrics.map((item, index) => (
          <Fragment key={index}>
            {index > 0 && <span className="airflow-business-sep" aria-hidden="true" />}
            {item}
          </Fragment>
        ))}
      </div>
    </section>
  );
}

function matchesState(filter: StateFilter, state?: string) {
  if (filter === "all") return true;
  if (filter === "queued") return state === "queued" || state === "scheduled" || state === "up_for_retry";
  return state === filter;
}

/**
 * 워크플로우 로그2 (2026-09-16).
 *
 * <p>기존 «로그»(AirflowDashboardPage)와 같은 정보를 인포매티카 Monitor 식 배치로 다시 놓은 시안이다. 관리자와 둘을
 * 비교해 하나만 남기기로 해서 기존 화면은 그대로 두고 메뉴를 하나 더 만들었다.
 * <ul>
 *   <li>① 상단 현황 카드는 커튼처럼 접는다(접으면 한 줄 요약, 상태는 브라우저에 기억).</li>
 *   <li>② 왼쪽 트리는 그대로 두고 줄마다 오늘 마지막 실행 상태 점을 붙였다.</li>
 *   <li>③ 가운데 위: 지금 보는 범위 · 실행 목록/타임라인 · 기간 · 상태 · 검색.</li>
 *   <li>④ 실행 → job → 단계를 한 표에서 펼친다. 하위 워크플로우 실행은 부른 job 아래로 들어간다
 *       (run id 규칙 manual__{상위 run}__{노드키}로 정확히 잇는다).</li>
 *   <li>⑤ 타임라인: 하루치 실행·job 을 시간축 막대로.</li>
 *   <li>⑥ 아래 상세 창(높이 조절·접기): 요약 / 작업·단계 / 로그 / 실행 이력 / 알림 규칙 / 속성.
 *       예전의 오른쪽 속성창·실행 이력 창·DAG 상세 창이 여기로 모였다.</li>
 * </ul>
 */
export function WorkflowLogV2Page() {
  const [searchParams] = useSearchParams();
  const initialDagId = searchParams.get("dagId") ?? undefined;
  const [refreshSeconds, setRefreshSeconds] = useState(30);
  const [treeSearch, setTreeSearch] = useState("");
  const [scope, setScope] = useState<ListScope>(
    initialDagId ? { kind: "dag", dagId: initialDagId } : { kind: "cdc", scope: "all" });
  const [view, setView] = useState<"list" | "timeline">("list");
  const [period, setPeriod] = useState<PeriodKey>("today");
  const [customRange, setCustomRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day").startOf("day"), dayjs().endOf("day")]);
  const [stateFilter, setStateFilter] = useState<StateFilter>("all");
  const [gridSearch, setGridSearch] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selection, setSelection] = useState<Selection>();
  const [selectedPipelineId, setSelectedPipelineId] = useState<number>();
  const [detailTab, setDetailTab] = useState<DetailTab>(searchParams.get("detail") === "1" ? "props" : "summary");
  const [detailOpen, setDetailOpen] = useState(true);
  const [detailHeight, setDetailHeight] = useState(() => Number(readStorage(DETAIL_HEIGHT_KEY)) || 330);
  const [curtainOpen, setCurtainOpen] = useState(() => readStorage(CURTAIN_KEY) !== "closed");
  const [executionDag, setExecutionDag] = useState<DashboardDag>();
  const [failureLog, setFailureLog] = useState<FailureLogTarget>();
  const [synchronizing, setSynchronizing] = useState(false);
  const autoSelect = useRef(true);
  const splitRef = useRef<HTMLDivElement>(null);

  const { can, permissionsLoaded } = useAuth();
  const canRun = !permissionsLoaded || can("AIRFLOW", "WRITE");

  // 진입 시 카탈로그 동기화·대시보드 조회는 기존 로그 화면과 같은 키를 쓴다 - 두 화면을 오가도 한 번만 받는다.
  const initialSyncQuery = useQuery({
    queryKey: ["airflow-dag-catalog-sync"],
    queryFn: syncAirflowDagCatalog,
    enabled: canRun,
    retry: 1,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  });
  const dashboardQuery = useQuery({
    queryKey: ["logv2-dashboard"],
    queryFn: loadDashboard,
    refetchInterval: refreshSeconds * 1000,
    enabled: !canRun || initialSyncQuery.isFetched,
  });
  const workflowsQuery = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  const pipelinesQuery = useQuery({ queryKey: ["pipelines"], queryFn: listPipelines });
  const connectionsQuery = useQuery({ queryKey: ["connections"], queryFn: listConnections });
  const runtimeQuery = useQuery({
    queryKey: ["pipeline-runtime-statuses"],
    queryFn: listPipelineRuntimeStatuses,
    refetchInterval: refreshSeconds * 1000,
  });
  const cdcMetricsQuery = useQuery({
    queryKey: ["realtime-pipeline-metrics"],
    queryFn: getRealtimePipelineMetrics,
    refetchInterval: refreshSeconds * 1000,
  });

  const [rangeFrom, rangeTo] = periodRange(period, customRange);
  const rangeIncludesNow = !dayjs().isAfter(rangeTo);
  const periodRunsQuery = useQuery({
    queryKey: ["logv2-period-runs", rangeFrom.toISOString(), rangeTo.toISOString()],
    queryFn: () => listAllAirflowDagRuns({
      startDateGte: rangeFrom.toISOString(),
      startDateLte: rangeTo.toISOString(),
      maxRuns: 5000,
    }),
    refetchInterval: rangeIncludesNow ? refreshSeconds * 1000 : false,
  });

  useEffect(() => {
    if (canRun && initialSyncQuery.isError) {
      message.warning("DAG 자동 동기화에 실패해 기존 DB 정보로 대시보드를 표시합니다.");
    }
  }, [canRun, initialSyncQuery.isError]);

  const dags = useMemo(
    () => (dashboardQuery.data?.dags ?? []).filter((dag) => categoryOf(dag) !== "기타"),
    [dashboardQuery.data?.dags],
  );
  const dagsById = useMemo(() => new Map(dags.map((dag) => [dag.dag_id, dag] as const)), [dags]);
  const runsByDag = useMemo(() => groupRuns(dashboardQuery.data?.runs ?? []), [dashboardQuery.data?.runs]);
  const workflows = useMemo(() => workflowsQuery.data ?? [], [workflowsQuery.data]);
  const pipelines = pipelinesQuery.data ?? [];
  const connectionNames = new Map((connectionsQuery.data ?? []).map((c) => [c.id, c.name] as const));
  const runtimeByPipeline = new Map((runtimeQuery.data ?? []).map((r) => [r.pipelineId, r] as const));
  const metricByPipeline = new Map((cdcMetricsQuery.data ?? []).map((m) => [m.pipelineId, m] as const));
  const treeDags = useMemo(() => {
    const needle = treeSearch.trim().toLowerCase();
    return dags.filter((dag) => !needle || `${displayName(dag)} ${dag.dag_id}`.toLowerCase().includes(needle));
  }, [dags, treeSearch]);

  /** run id → 실행들(같은 run id 가 DAG 마다 있을 수 있어 목록으로 둔다). 기간 조회분 + 대시보드 최근분. */
  const runsById = useMemo(() => {
    const map = new Map<string, AirflowDagRun[]>();
    const seen = new Set<string>();
    [...(periodRunsQuery.data ?? []), ...(dashboardQuery.data?.runs ?? [])].forEach((run) => {
      const key = `${run.dag_id}|${run.dag_run_id}`;
      if (seen.has(key)) return;
      seen.add(key);
      map.set(run.dag_run_id, [...(map.get(run.dag_run_id) ?? []), run]);
    });
    return map;
  }, [periodRunsQuery.data, dashboardQuery.data?.runs]);

  const findRun = (dagId: string, runId: string) => runsById.get(runId)?.find((run) => run.dag_id === dagId);
  const findChildRun = (parentDagId: string, parentRunId: string, nodeKey: string) => {
    const candidates = runsById.get(childRunIdOf(parentRunId, nodeKey)) ?? [];
    const parent = workflows.find((workflow) => workflow.dagId === parentDagId);
    const childDagIds = new Set(workflows
      .filter((workflow) => parent?.childWorkflowIds.includes(workflow.id))
      .map((workflow) => workflow.dagId));
    return candidates.find((run) => run.dag_id && childDagIds.has(run.dag_id))
      ?? candidates.find((run) => run.dag_id !== parentDagId);
  };

  /* ---------------------------------------------------------------- 범위 */

  const controlDagPipelineId = (dagId: string) =>
    /^kafka_pipeline_\d+_control$/.test(dagId) ? Number(extractKafkaPipelineId(dagId)) : undefined;
  const mode: "etl" | "cdc" = scope.kind === "cdc" ? "cdc"
    : scope.kind === "metric" ? (scope.category === "CDC" ? "cdc" : "etl")
    : scope.kind === "dag" && controlDagPipelineId(scope.dagId) != null ? "cdc"
    : "etl";

  const scopeDagIds = useMemo(() => {
    if (scope.kind === "etl-all") return new Set(dags.filter((dag) => categoryOf(dag) === "ETL").map((dag) => dag.dag_id));
    if (scope.kind === "dag") return new Set([scope.dagId]);
    if (scope.kind === "metric") {
      return new Set(dags
        .filter((dag) => categoryOf(dag) === scope.category && matchesMetric(scope.metric, dag, runsByDag.get(dag.dag_id) ?? [], dayjs().startOf("day")))
        .map((dag) => dag.dag_id));
    }
    return new Set<string>();
  }, [scope, dags, runsByDag]);

  const cdcRows: PipelineResponse[] = mode !== "cdc" ? [] : pipelines.filter((pipeline) => {
    const needle = gridSearch.trim().toLowerCase();
    if (needle && !`${pipeline.name} ${pipeline.sourceTable ?? ""} ${pipeline.targetTable ?? ""} ${pipeline.topicName}`.toLowerCase().includes(needle)) {
      return false;
    }
    if (scope.kind === "metric") return scopeDagIds.has(`kafka_pipeline_${pipeline.id}_control`);
    if (scope.kind === "dag") return pipeline.id === controlDagPipelineId(scope.dagId);
    if (scope.kind !== "cdc" || scope.scope === "all") return true;
    if (scope.scope === "logfile") return pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null;
    if (scope.scope === "connection") return pipeline.sourceConnectionId === scope.connectionId;
    if (scope.scope === "schema") {
      return pipeline.sourceConnectionId === scope.connectionId && (pipeline.sourceSchema ?? "스키마 없음") === scope.schema;
    }
    return pipeline.id === scope.pipelineId;
  });

  const topRuns = useMemo(() => {
    if (mode !== "etl") return [];
    const periodRuns = periodRunsQuery.data ?? [];
    const periodById = new Map<string, AirflowDagRun[]>();
    periodRuns.forEach((run) => periodById.set(run.dag_run_id, [...(periodById.get(run.dag_run_id) ?? []), run]));
    const needle = gridSearch.trim().toLowerCase();
    return periodRuns
      .filter((run) => run.dag_id && scopeDagIds.has(run.dag_id))
      // 범위 안의 상위 실행이 부른 하위 실행은 맨 위에 따로 두지 않는다 - 부른 job 아래에서 펼친다.
      .filter((run) => {
        const parent = parentOfRun(run.dag_run_id);
        if (!parent) return true;
        return !(periodById.get(parent.parentRunId) ?? [])
          .some((candidate) => candidate.dag_id && candidate.dag_id !== run.dag_id && scopeDagIds.has(candidate.dag_id));
      })
      .filter((run) => matchesState(stateFilter, run.state))
      .filter((run) => {
        if (!needle) return true;
        const dag = run.dag_id ? dagsById.get(run.dag_id) : undefined;
        return `${dag ? displayName(dag) : ""} ${run.dag_id} ${run.dag_run_id}`.toLowerCase().includes(needle);
      })
      .sort((a, b) => dayjs(runAt(b) ?? 0).valueOf() - dayjs(runAt(a) ?? 0).valueOf());
  }, [mode, periodRunsQuery.data, scopeDagIds, stateFilter, gridSearch, dagsById]);

  // 범위·기간을 바꾸면 맨 위 실행을 골라 펼쳐 둔다(목업과 같이). 갱신 주기마다 다시 고르지는 않는다.
  useEffect(() => {
    if (!autoSelect.current || mode !== "etl" || !periodRunsQuery.isFetched) return;
    autoSelect.current = false;
    const first = topRuns[0];
    if (!first?.dag_id) {
      setSelection(undefined);
      return;
    }
    const sel: Selection = { kind: "run", dagId: first.dag_id, runId: first.dag_run_id };
    setSelection(sel);
    setExpanded(new Set([selectionKey(sel)]));
  }, [mode, topRuns, periodRunsQuery.isFetched]);

  // 목록에 없는 파이프라인이 골라져 있으면 첫 줄로 옮긴다(상세 창이 목록과 어긋나지 않게).
  const cdcRowIds = cdcRows.map((pipeline) => pipeline.id).join(",");
  useEffect(() => {
    if (mode !== "cdc" || !cdcRowIds) return;
    const ids = cdcRowIds.split(",").map(Number);
    if (selectedPipelineId == null || !ids.includes(selectedPipelineId)) {
      setSelectedPipelineId(ids[0]);
    }
  }, [mode, cdcRowIds, selectedPipelineId]);

  const changeScope = (next: ListScope) => {
    setScope(next);
    autoSelect.current = true;
    if (next.kind === "cdc" && next.scope === "pipeline") {
      setSelectedPipelineId(next.pipelineId);
    }
    if (next.kind === "metric") {
      // 카드 숫자는 «오늘» 기준이라 목록도 오늘로 맞춘다.
      setPeriod("today");
      setStateFilter("all");
    }
    if (next.kind === "cdc" || next.kind === "metric") {
      setView("list");
    }
  };
  const changePeriod = (next: PeriodKey) => {
    setPeriod(next);
    autoSelect.current = true;
  };
  const selectRow = (sel: Selection) => {
    setSelection(sel);
    setDetailOpen(true);
  };
  const toggleExpanded = (key: string) => setExpanded((current) => {
    const next = new Set(current);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    return next;
  });

  const ctx: GridCtx = {
    dagsById,
    findChildRun,
    expanded,
    toggle: toggleExpanded,
    selectedKey: selection ? selectionKey(selection) : undefined,
    select: selectRow,
    openLog: setFailureLog,
    openTab: (sel, tab) => {
      setSelection(sel);
      setDetailTab(tab);
      setDetailOpen(true);
    },
    refreshSeconds,
  };

  /* ---------------------------------------------------------------- 동작 */

  const synchronizeDags = async () => {
    setSynchronizing(true);
    try {
      const result = await initialSyncQuery.refetch();
      if (result.error || !result.data) {
        message.error("DAG 동기화에 실패했습니다. 기존 대시보드 정보를 갱신합니다.");
      } else {
        message.success(result.data.createdCount || result.data.disabledCount
          ? `동기화 완료: 신규 ${result.data.createdCount}건, 삭제 ${result.data.disabledCount}건`
          : "동기화할 DAG 변경사항이 없습니다.");
      }
      await Promise.all([dashboardQuery.refetch(), periodRunsQuery.refetch()]);
    } finally {
      setSynchronizing(false);
    }
  };

  /** 트리 우클릭 «상세»: 그 대상으로 범위를 옮기고 상세 창 «속성»을 연다(기존의 DAG 상세 창 자리). */
  const showDagDetail = (dag: DashboardDag) => {
    const pipelineId = controlDagPipelineId(dag.dag_id);
    changeScope(pipelineId != null ? { kind: "cdc", scope: "pipeline", pipelineId } : { kind: "dag", dagId: dag.dag_id });
    setDetailTab("props");
    setDetailOpen(true);
  };

  const toggleCurtain = () => {
    setCurtainOpen((current) => {
      writeStorage(CURTAIN_KEY, current ? "closed" : "open");
      return !current;
    });
  };

  const startResize = (event: React.MouseEvent) => {
    event.preventDefault();
    const startY = event.clientY;
    const startHeight = detailOpen ? detailHeight : 42;
    const limit = (splitRef.current?.clientHeight ?? 700) - 140;
    let latest = startHeight;
    setDetailOpen(true);
    const move = (ev: MouseEvent) => {
      latest = Math.min(limit, Math.max(160, startHeight - (ev.clientY - startY)));
      setDetailHeight(latest);
    };
    const up = () => {
      document.removeEventListener("mousemove", move);
      document.removeEventListener("mouseup", up);
      document.body.classList.remove("lv2-resizing");
      writeStorage(DETAIL_HEIGHT_KEY, String(Math.round(latest)));
    };
    document.body.classList.add("lv2-resizing");
    document.addEventListener("mousemove", move);
    document.addEventListener("mouseup", up);
  };

  /* ---------------------------------------------------------------- 화면 */

  const cdcCounts = countMetrics(dags.filter((dag) => categoryOf(dag) === "CDC"), runsByDag);
  const etlCounts = countMetrics(dags.filter((dag) => categoryOf(dag) === "ETL"), runsByDag);
  const countsOf = (category: BusinessCategory) => (category === "CDC" ? cdcCounts : etlCounts);

  const selectedPipeline = pipelines.find((pipeline) => pipeline.id === selectedPipelineId);
  const selectedControlDag = selectedPipeline ? dagsById.get(`kafka_pipeline_${selectedPipeline.id}_control`) : undefined;
  const treeSelectedDagId = mode === "etl" ? (scope.kind === "dag" ? scope.dagId : selection?.dagId) : undefined;

  const crumb = (() => {
    const dom = mode === "cdc"
      ? <span className="lv2-dom cdc"><i />CDC</span>
      : <span className="lv2-dom etl"><i />ETL</span>;
    let path: string[] = [];
    let current = "";
    let note: string | undefined;
    if (scope.kind === "metric") {
      current = `${STATUS_METRIC_LABEL[scope.metric]}`;
      note = "오늘 기준";
    } else if (scope.kind === "etl-all") {
      current = "전체 워크플로우";
    } else if (scope.kind === "dag") {
      const dag = dagsById.get(scope.dagId);
      const workflow = workflows.find((item) => item.dagId === scope.dagId);
      path = workflow ? parentWorkflowPathOf(workflows, workflow.id) : [];
      current = dag ? displayName(dag) : scope.dagId;
      if (workflow?.childWorkflowIds.length) note = "하위 워크플로우 포함";
    } else if (scope.kind === "cdc") {
      if (scope.scope === "all") current = "전체 파이프라인";
      else if (scope.scope === "logfile") { path = ["전체 파이프라인"]; current = "로그파일"; }
      else if (scope.scope === "connection") { path = ["전체 파이프라인"]; current = connectionNames.get(scope.connectionId) ?? `연결 ${scope.connectionId}`; }
      else if (scope.scope === "schema") { path = [connectionNames.get(scope.connectionId) ?? `연결 ${scope.connectionId}`]; current = scope.schema; }
      else {
        const pipeline = pipelines.find((row) => row.id === scope.pipelineId);
        if (pipeline?.sourceConnectionId != null) path = [connectionNames.get(pipeline.sourceConnectionId) ?? "", pipeline.sourceSchema ?? "스키마 없음"];
        current = pipeline?.name ?? "파이프라인";
      }
    }
    return (
      <div className="lv2-crumb">
        {dom}
        {path.filter(Boolean).map((name) => <span key={name} className="lv2-crumb-path">{name} ›</span>)}
        <span className="lv2-crumb-cur" title={current}>{current}</span>
        {note && <span className="lv2-crumb-note">· {note}</span>}
      </div>
    );
  })();

  const runScale = Math.max(...topRuns.map((run) => durationSeconds(runAt(run), run.end_date ?? (run.state === "running" ? dayjs().toISOString() : undefined))), 1);
  const multiDay = !rangeFrom.isSame(rangeTo, "day");

  let grid: React.ReactNode;
  if (mode === "cdc") {
    grid = (
      <Table<PipelineResponse>
        rowKey="id"
        size="small"
        dataSource={cdcRows}
        scroll={{ x: 900 }}
        columns={[
          ...cdcPipelineColumns(runtimeByPipeline, metricByPipeline, {
            render: (pipeline) => <StateBadge state={(runsByDag.get(`kafka_pipeline_${pipeline.id}_control`) ?? [])[0]?.state} />,
          }),
          {
            title: "동작",
            width: 170,
            render: (_: unknown, pipeline: PipelineResponse) => {
              const controlDag = dagsById.get(`kafka_pipeline_${pipeline.id}_control`);
              return (
                <div className="lv2-acts">
                  {controlDag && canRun && (
                    <button type="button" className="lv2-btn sm" onClick={(event) => { event.stopPropagation(); setExecutionDag(controlDag); }}>실행 설정</button>
                  )}
                  {controlDag && (
                    <button type="button" className="lv2-btn sm"
                            onClick={(event) => { event.stopPropagation(); setSelectedPipelineId(pipeline.id); setDetailTab("history"); setDetailOpen(true); }}>
                      실행 이력
                    </button>
                  )}
                </div>
              );
            },
          },
        ]}
        pagination={{ pageSize: 20, hideOnSinglePage: true, showTotal: (total) => `전체 ${total}건` }}
        rowClassName={(row) => (row.id === selectedPipelineId ? "lv2-row-selected" : "")}
        onRow={(row) => ({ onClick: () => { setSelectedPipelineId(row.id); setDetailOpen(true); } })}
        locale={{ emptyText: <Empty description="이 범위에 파이프라인이 없습니다." /> }}
      />
    );
  } else if (periodRunsQuery.isLoading) {
    grid = <div className="lv2-empty"><Spin size="small" /> 실행 목록을 불러오는 중입니다.</div>;
  } else if (periodRunsQuery.isError) {
    grid = <div className="lv2-empty">실행 목록을 불러오지 못했습니다.</div>;
  } else if (scopeDagIds.size === 0) {
    grid = <div className="lv2-empty">이 범위에 워크플로우가 없습니다.</div>;
  } else if (view === "timeline") {
    grid = <TimelineView runs={topRuns} ctx={ctx} />;
  } else if (!topRuns.length) {
    grid = <div className="lv2-empty">이 범위·기간에 실행이 없습니다. 기간을 넓혀 보세요.</div>;
  } else {
    grid = (
      <table className="lv2-grid">
        <GridHead />
        <tbody>
          {topRuns.map((run) => <RunRow key={`${run.dag_id}|${run.dag_run_id}`} run={run} ctx={ctx} scale={runScale} showDate={multiDay} />)}
        </tbody>
      </table>
    );
  }

  const syncBusy = synchronizing || initialSyncQuery.isFetching;
  const initialLoading = (canRun && !initialSyncQuery.isFetched) || dashboardQuery.isLoading;
  const miniCount = (label: string, value: number, bad = false) => (
    <span className="lv2-mini-item">{label} {bad && value > 0 ? <b className="bad">{value}</b> : <em>{value}</em>}</span>
  );

  return (
    <ConfigProvider theme={cerebroBrandTheme}>
      <div className="lv2-page">
        <WorkflowPageHeader
          title="워크플로우 로그2"
          icon={<WfIcon name="pulse" size={22} strokeWidth={2.2} />}
          actions={
            <div className="airflow-refresh-controls">
              {canRun && (
                <button type="button" className={syncBusy ? "btn-wf-sync spinning" : "btn-wf-sync"} aria-busy={syncBusy}
                        onClick={() => { if (!syncBusy) void synchronizeDags(); }}>
                  <WfIcon name="sync" size={15} strokeWidth={2.2} />
                  동기화
                </button>
              )}
              <span className="refresh-label">갱신 주기</span>
              <Select value={refreshSeconds} options={REFRESH_OPTIONS} onChange={setRefreshSeconds} aria-label="갱신 주기" />
              <span className="last-refresh-time">
                마지막 갱신 {dashboardQuery.dataUpdatedAt ? dayjs(dashboardQuery.dataUpdatedAt).format("HH:mm:ss") : "-"}
              </span>
            </div>
          }
        />

        {initialLoading ? (
          <div className="airflow-dashboard-loading"><Spin size="large" /><span>{canRun && !initialSyncQuery.isFetched ? "DAG 동기화 중..." : "대시보드 로딩 중..."}</span></div>
        ) : dashboardQuery.isError ? (
          <Empty description="Airflow 현황을 불러올 수 없습니다." />
        ) : (
          <>
            {/* ① 커튼형 현황 */}
            <section className={`lv2-curtain ${curtainOpen ? "open" : "closed"}`}>
              <div className="lv2-curtain-body" aria-hidden={!curtainOpen}>
                <div className="airflow-business-status">
                  {CATEGORY_ORDER.map((category) => (
                    <StatusCard
                      key={category}
                      category={category}
                      counts={countsOf(category)}
                      activeMetric={scope.kind === "metric" && scope.category === category ? scope.metric : undefined}
                      onMetricClick={(metric) => changeScope(
                        scope.kind === "metric" && scope.category === category && scope.metric === metric
                          ? { kind: "cdc", scope: "all" }
                          : { kind: "metric", category, metric })}
                    />
                  ))}
                </div>
              </div>
              <button type="button" className="lv2-curtain-handle" aria-expanded={curtainOpen} onClick={toggleCurtain}>
                {!curtainOpen && (
                  <span className="lv2-mini">
                    <span className="lv2-dom cdc"><i />CDC</span>
                    {miniCount("실행 중", cdcCounts.running)}<span className="dotsep">·</span>
                    {miniCount("실패", cdcCounts.failed, true)}<span className="dotsep">·</span>
                    {miniCount("대기", cdcCounts.waiting)}
                    <span className="dotsep">│</span>
                    <span className="lv2-dom etl"><i />ETL</span>
                    {miniCount("성공", etlCounts.success)}<span className="dotsep">·</span>
                    {miniCount("실패", etlCounts.failed, true)}<span className="dotsep">·</span>
                    {miniCount("실행 중", etlCounts.running)}
                  </span>
                )}
                <span className="lv2-handle-label">{curtainOpen ? "접기 ▴" : "현황 펼치기 ▾"}</span>
              </button>
            </section>

            <div className="lv2-workspace">
              {/* ② 트리 */}
              <LogV2Tree
                dags={treeDags}
                runsByDag={runsByDag}
                workflows={workflows}
                pipelines={pipelines}
                connectionNames={connectionNames}
                scope={scope}
                selectedDagId={treeSelectedDagId}
                selectedPipelineId={mode === "cdc" ? selectedPipelineId : undefined}
                search={treeSearch}
                onSearch={setTreeSearch}
                onScope={changeScope}
                onDetail={showDagDetail}
                onExecution={setExecutionDag}
              />

              <section className={`lv2-card lv2-main${mode === "cdc" ? " cdc-mode" : ""}`}>
                {/* ③ 컨텍스트 바 */}
                <div className="lv2-ctx">
                  {crumb}
                  <div className="lv2-seg">
                    <button type="button" className={view === "list" || mode === "cdc" ? "on" : ""} onClick={() => setView("list")}>☰ 실행 목록</button>
                    <button type="button" className={view === "timeline" && mode === "etl" ? "on" : ""} disabled={mode === "cdc"}
                            onClick={() => setView("timeline")}>▭ 타임라인</button>
                  </div>
                  <div className="lv2-ctx-right">
                    {mode === "etl" && (
                      <>
                        <span className="lv2-ctx-label">기간</span>
                        <div className="lv2-seg">
                          {([["today", "오늘"], ["yesterday", "어제"], ["7d", "최근 7일"], ["30d", "최근 30일"], ["custom", "📅 기간 지정"]] as const).map(([key, label]) => (
                            <button key={key} type="button" className={period === key ? "on" : ""} onClick={() => changePeriod(key)}>{label}</button>
                          ))}
                        </div>
                        {period === "custom" && (
                          <DatePicker.RangePicker size="small" value={customRange} allowClear={false}
                                                  onChange={(value) => {
                                                    if (value?.[0] && value?.[1]) {
                                                      setCustomRange([value[0].startOf("day"), value[1].endOf("day")]);
                                                      autoSelect.current = true;
                                                    }
                                                  }} />
                        )}
                        <span className="lv2-ctx-label">상태</span>
                        <div className="lv2-chips">
                          {([["all", "전체", ""], ["running", "실행 중", "#2563eb"], ["success", "성공", "#16a34a"], ["failed", "실패", "#ef4444"], ["queued", "대기", "#d97706"]] as const).map(([key, label, color]) => (
                            <button key={key} type="button" className={`lv2-chip${stateFilter === key ? " on" : ""}`}
                                    onClick={() => { setStateFilter(key); autoSelect.current = true; }}>
                              {color && <i style={{ background: color }} />}{label}
                            </button>
                          ))}
                        </div>
                      </>
                    )}
                    <input className="lv2-search" placeholder={mode === "cdc" ? "파이프라인 검색" : "워크플로우·실행 검색"}
                           value={gridSearch} onChange={(event) => setGridSearch(event.target.value)} aria-label="목록 검색" />
                  </div>
                </div>

                <div ref={splitRef} className={`lv2-split${detailOpen ? "" : " detail-closed"}`}>
                  {/* ④ 실행 목록 / ⑤ 타임라인 */}
                  <div className="lv2-grid-area">{grid}</div>
                  <div className="lv2-split-handle" title="끌어서 상세 창 높이 조절" onMouseDown={startResize} />
                  {/* ⑥ 상세 창 */}
                  <section className="lv2-detail" style={detailOpen ? { height: detailHeight } : undefined}>
                    <LogV2Detail
                      mode={mode}
                      selection={selection}
                      onSelect={(sel) => {
                        selectRow(sel);
                        setDetailTab("summary");
                      }}
                      pipeline={selectedPipeline}
                      controlDag={selectedControlDag}
                      runtime={selectedPipeline ? runtimeByPipeline.get(selectedPipeline.id) : undefined}
                      metric={selectedPipeline ? metricByPipeline.get(selectedPipeline.id) : undefined}
                      workflows={workflows}
                      dagsById={dagsById}
                      runsByDag={runsByDag}
                      findRun={findRun}
                      findChildRun={findChildRun}
                      tab={detailTab}
                      onTab={(tab) => { setDetailTab(tab); setDetailOpen(true); }}
                      open={detailOpen}
                      onToggleOpen={() => setDetailOpen((current) => !current)}
                      refreshSeconds={refreshSeconds}
                      canRun={canRun}
                      openLog={setFailureLog}
                      onExecution={setExecutionDag}
                    />
                  </section>
                </div>
              </section>
            </div>
          </>
        )}

        <ExecutionSettingsModal
          open={Boolean(executionDag)}
          dag={executionDag}
          runs={executionDag ? runsByDag.get(executionDag.dag_id) ?? [] : []}
          onClose={() => setExecutionDag(undefined)}
          onExecuted={() => {
            setDetailTab("history");
            setDetailOpen(true);
            void dashboardQuery.refetch();
            void periodRunsQuery.refetch();
          }}
        />
        <TaskFailureLogModal target={failureLog} onClose={() => setFailureLog(undefined)} />
      </div>
    </ConfigProvider>
  );
}

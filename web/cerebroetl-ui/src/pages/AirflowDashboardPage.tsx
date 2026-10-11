import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, ConfigProvider, DatePicker, Descriptions, Dropdown, Empty, Input, message, Modal, Popconfirm, Radio, Select, Space, Spin, Table, Tag, Typography } from "antd";
import type { Dayjs } from "dayjs";
import {
  DeploymentUnitOutlined,
  DownOutlined,
  InfoCircleOutlined,
  PlayCircleOutlined,
  ProfileOutlined,
  RightOutlined,
} from "@ant-design/icons";
import dayjs from "dayjs";
import { Link, useSearchParams } from "react-router-dom";
import {
  getAirflowTaskLog,
  listActiveAirflowDagRuns,
  listAirflowDagCatalog,
  listAirflowDags,
  listAirflowTaskInstances,
  listAllAirflowDagRuns,
  retryAirflowTasks,
  markAirflowTaskSuccess,
  syncAirflowDagCatalog,
  triggerAirflowDag,
  type AirflowDag,
  type AirflowDagRun,
  setAirflowDagPaused,
} from "../api/platform";
import { getWorkflow, listWorkflows, type WorkflowSummary } from "../api/workflows";
import { getAlertRulesWatching } from "../api/config";
import { listPipelineRuntimeStatuses, listPipelines } from "../api/pipelines";
import type { PipelineResponse, PipelineRuntimeStatusResponse } from "../types/pipeline";
import { renderRuntimeStatus } from "../utils/cdcPresentation";
import { buildWorkflowHierarchy, sortFoldersFirst, type WorkflowTreeItem } from "../utils/workflowTree";
import { getEtlJob, listEtlJobs } from "../api/etlJobs";
import { categorizeDag, type DagCategory } from "../utils/dagHistory";
import { scheduleDescription } from "../utils/schedulePreset";
import { useAuth } from "../auth/AuthContext";
import { WfIcon, WorkflowPageHeader, type WfIconName } from "../components/WorkflowPageHeader";
import { cerebroBrandTheme } from "../theme/cerebro";
import { TaskFailureLogModal, type FailureLogTarget } from "../components/TaskFailureLogModal";
import { ACTIVE_RUNS_IDLE_MS, ACTIVE_RUNS_KEY, LIVE_REFRESH_MS, refreshRunsAfterTrigger } from "./workflowLogV2/logV2Live";
import { GptChatPanel } from "../components/gpt/GptChatPanel";
import { useGptEnabled } from "../components/gpt/useGptEnabled";
import { EtlRunsPane } from "./workflowLog/EtlRunsPane";
import { SideDock } from "../components/SideDock";
import { TreeHead } from "../components/TreeHead";
import { useSideDock } from "../components/useSideDock";

type BusinessCategory = Exclude<DagCategory, "기타">;

interface DashboardDag extends AirflowDag {
  dag_display_name?: string;
  description?: string;
  owners?: string[];
  tags?: Array<string | { name?: string }>;
  next_dagrun?: string;
  next_dagrun_run_after?: string;
  timetable_summary?: string;
  business_category?: BusinessCategory;
  business_folder?: string;
  monitoring_enabled?: boolean;
  consecutive_failure_threshold?: number;
  stale_days_threshold?: number;
  duration_multiplier?: number;
  sla_minutes?: number | null;
  etl_job_id?: number;
  nifi_process_group_id?: string;
  /** 카탈로그에 처음 잡힌 시각. "오늘 신규" 판정에 쓴다. */
  created_at?: string;
}

interface DashboardData {
  dags: DashboardDag[];
  runs: AirflowDagRun[];
}

// 트리 최상단 가지. CDC 가지(연결 → 스키마 → 파이프라인)는 2026-10-10 에 뺐다 - CDC 는 ETL 가지의 CDC 워크플로우로 제어하고,
// 파이프라인 단위 상태 · 지연은 CDC 관리 화면이 보여 준다(«소속 워크플로우» 열도 거기 있다).
const CATEGORY_ORDER: BusinessCategory[] = ["ETL"];
const REFRESH_OPTIONS = [
  { label: "10초", value: 10 },
  { label: "30초", value: 30 },
  { label: "1분", value: 60 },
];

type ExecutionAction = "deploy" | "start" | "stop" | "monitor" | "restart";

/** DAG 에 붙은 태그 이름들(Airflow 가 문자열 또는 {name} 으로 준다). */
function tagNames(dag: DashboardDag): string[] {
  return (dag.tags ?? []).map((tag) => (typeof tag === "string" ? tag : tag.name ?? ""));
}

/**
 * CDC 워크플로우인가 - 캔버스에 CDC 노드만 얹은 워크플로우다(2026-09-17).
 *
 * <p>dag_id 는 여느 워크플로우와 같은 etl_wf_* 라서 접두사로는 갈리지 않는다. DAG 팩토리가
 * 붙이는 «cdc-workflow» 태그로 알아본다. 실행할 때 파이프라인 지시(시작·중지·재시작·배포)를 고르게 해야 한다.
 */
function isCdcWorkflowDag(dag: DashboardDag): boolean {
  // «cdc» 가 아니라 «cdc-workflow» 로 본다 - 워크플로우 키도 태그로 붙어서, 키가 cdc 인 ETL 워크플로우와 겹친다.
  return dag.dag_id.startsWith("etl_wf_") && tagNames(dag).includes("cdc-workflow");
}

/** CDC 워크플로우에 얹힌 파이프라인 하나. 실행 현황에서 «어느 것이 실패했는지» 짚어주기 위한 것. */
interface CdcMember {
  pipelineId: number;
  pipeline?: PipelineResponse;
  runtime?: PipelineRuntimeStatusResponse;
}

/**
 * CDC 워크플로우의 «상태». DAG Run 이 아니라 소속 파이프라인들의 실제 상태를 모아서 본다.
 *
 * <p>워크플로우 Run 의 success 는 «지시를 보냈다»는 뜻이지 «CDC 가 돌고 있다»가 아니다
 * (2026-09-10 설계 방안 B). Run 상태를 그대로 보여주면 CDC 가 멈춰 있어도 초록으로 보인다.
 */
function cdcAggregate(members: CdcMember[]): { label: string; color: string } {
  const states = members.map((member) =>
    member.runtime?.runtimeStatus ?? member.pipeline?.status ?? "UNKNOWN");
  if (states.length === 0) return { label: "파이프라인 없음", color: "default" };
  if (states.some((state) => state === "FAILED")) return { label: "실패", color: "error" };
  if (states.every((state) => state === "RUNNING" || state === "DEPLOYED")) {
    return { label: "실행 중", color: "success" };
  }
  if (states.every((state) => state === "STOPPED" || state === "READY" || state === "CREATED")) {
    return { label: "중지", color: "default" };
  }
  return { label: "일부 실행", color: "warning" };
}

/** CDC 워크플로우 노드의 태스크 묶음인가(제어 DAG 에 지시를 보내는 send 한 단계). */
function isCdcSendStep(steps: Array<{ task_id: string }>): boolean {
  return steps.length === 1 && steps[0].task_id.endsWith(".send");
}

function runAt(run?: AirflowDagRun) {
  // 아직 대기 중(queued)이면 시작 시각이 없다 - 대기열에 들어간 시각으로 본다(실행 목록 logV2Data.runAt 과 같은 규칙).
  // 이게 없으면 방금 건 실행이 «오늘 실행»에 안 잡혀 현황 «대기»에서 빠졌다(2026-10-11).
  return run?.start_date ?? run?.queued_at ?? run?.run_after ?? run?.execution_date;
}

/**
 * 오늘 실행분 중 가장 최근 것. 없으면 undefined.
 *
 * <p>실시간 모니터링은 «오늘 무엇이 돌았나»를 보는 화면이다. 그런데 목록이 날짜를 가리지
 * 않고 «마지막 실행»을 집으면, 며칠 전에 돈 워크플로우가 오늘 성공/실패한 것처럼 보인다.
 * 어제 성공하고 오늘 아직 안 돈 작업과, 오늘 성공한 작업이 같은 «성공»으로 보이는 것이
 * 특히 문제였다.
 *
 * <p>runs 는 최신순이라 첫 일치가 곧 오늘의 마지막 실행이다. 이전 일자는 속성창의
 * «실행 이력»에서 기간을 넓혀 본다(RunHistoryModal).
 */
function latestRunToday(runs: AirflowDagRun[], today: Dayjs = dayjs().startOf("day")) {
  return runs.find((run) => {
    const startedAt = runAt(run);
    return Boolean(startedAt && dayjs(startedAt).isAfter(today));
  });
}

function duration(start?: string, end?: string) {
  if (!start || !end) {
    return "-";
  }
  const seconds = Math.max(0, dayjs(end).diff(dayjs(start), "second"));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const rest = seconds % 60;
  return hours > 0
    ? `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(rest).padStart(2, "0")}`
    : `${String(minutes).padStart(2, "0")}:${String(rest).padStart(2, "0")}`;
}

/**
 * 태스크가 «돌고 있는» 상태. deferred 는 하위 워크플로우 노드가 자식이 끝나길 트리거러에 맡겨 두고 슬롯을 비운 상태,
 * up_for_reschedule 은 센서(CDC 제어 DAG)가 다음 확인 시각을 기다리는 상태다 - 둘 다 사용자에겐 «실행 중»이다(2026-09-17).
 */
const ACTIVE_TASK_STATES = ["running", "deferred", "up_for_reschedule"];

function stateLabel(state?: string) {
  if (state === "success") return "성공";
  if (state === "failed") return "실패";
  if (state && ACTIVE_TASK_STATES.includes(state)) return "실행 중";
  if (state === "queued" || state === "scheduled") return "대기";
  if (state === "upstream_failed") return "선행 실패";
  if (state === "skipped") return "건너뜀";
  if (state === "up_for_retry") return "재시도 대기";
  return state ?? "실행 없음";
}

function stateColor(state?: string) {
  if (state === "success") return "success";
  if (state === "failed" || state === "upstream_failed") return "error";
  if (state && ACTIVE_TASK_STATES.includes(state)) return "processing";
  if (state === "queued" || state === "scheduled" || state === "up_for_retry") return "warning";
  return "default";
}

function displayName(dag: DashboardDag) {
  return dag.dag_display_name || dag.dag_id;
}

function categoryOf(dag: DashboardDag): DagCategory {
  return dag.business_category ?? categorizeDag(dag.dag_id);
}

function folderName(dag: DashboardDag) {
  if (dag.business_folder) return dag.business_folder;
  const tags = tagNames(dag);
  const folder = tags
    .map((tag) => tag.match(/^(?:folder|business|업무)[:=](.+)$/i)?.[1]?.trim())
    .find(Boolean);
  return folder || "미분류";
}

function groupRuns(runs: AirflowDagRun[]) {
  const grouped = new Map<string, AirflowDagRun[]>();
  runs.forEach((run) => {
    if (!run.dag_id) return;
    const bucket = grouped.get(run.dag_id) ?? [];
    bucket.push(run);
    grouped.set(run.dag_id, bucket);
  });
  grouped.forEach((bucket) =>
    bucket.sort((a, b) => dayjs(runAt(b) ?? 0).valueOf() - dayjs(runAt(a) ?? 0).valueOf()),
  );
  return grouped;
}

/**
 * 최근 실행(30초 주기)에 대기 · 실행 중 실행(3초/5초 주기)을 덮어쓴 것(2026-10-11 요청 - 현황 카드도 실행 목록처럼 빨리).
 * 도는 실행은 몇 초 전 상태로, 방금 건(아직 시작 전) 실행은 새로 들어간다. 실행 목록(EtlRunsPane)과 같은 덮어쓰기다.
 */
function overlayActiveRuns(runs: AirflowDagRun[], activeRuns: AirflowDagRun[] | undefined) {
  if (!activeRuns?.length) return runs;
  const merged = new Map<string, AirflowDagRun>();
  runs.forEach((run) => merged.set(`${run.dag_id}|${run.dag_run_id}`, run));
  activeRuns.forEach((run) => {
    const key = `${run.dag_id}|${run.dag_run_id}`;
    merged.set(key, { ...merged.get(key), ...run });
  });
  return [...merged.values()];
}

/**
 * 화면이 쓰는 DAG 목록 · 최근 실행. 서버 상태 점검(/infra/processes)은 받지 않는다 - 이 화면은 그 결과를 쓰지 않는데,
 * Kafka · 커넥터별 · NiFi · Airflow 를 차례로 확인하느라 첫 조회가 그만큼 기다렸다(2026-10-11 뺌).
 */
async function loadDashboard(): Promise<DashboardData> {
  const [airflowDags, catalog, runs, etlJobs] = await Promise.all([
    listAirflowDags(),
    listAirflowDagCatalog(),
    listAllAirflowDagRuns({ maxRuns: 2000 }),
    listEtlJobs().catch(() => []),
  ]);
  const etlJobsByDagId = new Map(etlJobs.flatMap((job) => job.airflowDagId ? [[job.airflowDagId, job] as const] : []));
  const withEtlJob = (dag: DashboardDag): DashboardDag => {
    const job = etlJobsByDagId.get(dag.dag_id);
    return job ? { ...dag, etl_job_id: job.id, nifi_process_group_id: job.nifiPgId } : dag;
  };
  const catalogDagIds = new Set(catalog.map((entry) => entry.dagId));
  const catalogDags = catalog.map((entry) => {
      const airflowDag = airflowDags.find((dag) => dag.dag_id === entry.dagId);
      return {
        ...airflowDag,
        dag_id: entry.dagId,
        dag_display_name: entry.displayName,
        description: airflowDag && "description" in airflowDag
          ? (airflowDag as DashboardDag).description
          : entry.description,
        business_category: entry.businessGroup,
        business_folder: entry.businessFolder,
        monitoring_enabled: entry.monitoringEnabled,
        consecutive_failure_threshold: entry.consecutiveFailureThreshold,
        stale_days_threshold: entry.staleDaysThreshold,
        created_at: entry.createdAt,
        duration_multiplier: entry.durationMultiplier,
        sla_minutes: entry.slaMinutes,
      };
    }).map(withEtlJob);
  const discoveredDags = airflowDags
    .filter((dag) => !catalogDagIds.has(dag.dag_id))
    .map((dag) => ({ ...dag, business_category: categorizeDag(dag.dag_id), business_folder: "ETL" } as DashboardDag))
    .map(withEtlJob);
  return {
    dags: [...catalogDags, ...discoveredDags],
    runs,
  };
}

function DagIcon() {
  return <DeploymentUnitOutlined className="airflow-dashboard-dag-icon" />;
}

/** 상단 카드에서 고른 지표. 가운데 목록이 이 조건으로 좁혀진다. */
type StatusMetric = "all" | "running" | "success" | "failed" | "waiting" | "new";

/**
 * 가운데 목록이 무엇을 보여줄지. 트리에서 고르거나 상단 지표를 누르면 바뀐다.
 * 하나의 선택값으로 묶어 두면 "지금 무엇을 보고 있는지"가 한 군데에만 있게 된다.
 */
const STATUS_METRIC_LABEL: Record<StatusMetric, string> = {
  all: "전체 작업",
  running: "실행 중",
  success: "성공",
  failed: "실패",
  waiting: "대기",
  new: "신규",
};

type ListScope =
  | { kind: "dag"; dagId: string }
  // 트리 최상단 폴더(ETL)를 누른 것. 그 가지 «안에 있는 전부»(워크플로우 실행 전체)를 가운데에 편다(2026-09-21 요청).
  // 예전에는 최상단만 접기/펼치기라, 전체를 보려면 «전체 파이프라인»을 따로 눌러야 했고 ETL 은 방법이 아예 없었다.
  | { kind: "category"; category: BusinessCategory }
  | { kind: "metric"; category: BusinessCategory; metric: StatusMetric };

/**
 * 지표 하나에 대한 판정. 카드 숫자와 가운데 목록이 <b>같은 기준</b>을 쓰도록 한 곳에 둔다.
 *
 * <p>예전에는 StatusCard와 ScopeList가 같은 조건을 각자 다시 써서, 한쪽만 고치면
 * "카드는 3건인데 목록엔 2건"이 되기 쉬웠다.
 *
 * <p>«실행 중»만 날짜와 무관하다(어제 시작해 아직 도는 것도 포함). 나머지는 «오늘 마지막
 * 실행»이 무엇이었는지로 판정한다 - runsByDag가 최신순이라 첫 항목이 그것이다.
 */
function matchesMetric(
  metric: StatusMetric,
  dag: DashboardDag,
  runs: AirflowDagRun[],
  today: Dayjs,
  waitingDagIds?: Set<string>,
): boolean {
  if (metric === "all") return true;
  if (metric === "new") return Boolean(dag.created_at && dayjs(dag.created_at).isAfter(today));
  if (metric === "running") return runs.some((run) => run.state === "running");
  const latestToday = latestRunToday(runs, today);
  if (metric === "success") return latestToday?.state === "success";
  if (metric === "failed") return latestToday?.state === "failed";
  // 대기: 큐에 올라 시작을 기다리는 실행 + 지금 실행 중인 워크플로우의 후행(waitingDagIds, 2026-10-02 요청).
  return latestToday?.state === "queued" || latestToday?.state === "scheduled"
    || Boolean(waitingDagIds?.has(dag.dag_id));
}

/**
 * «대기» 워크플로우 - 지금 실행 중인 워크플로우가 끝나면 이어서 돌 워크플로우(2026-10-02 요청).
 *
 * <ul>
 *   <li>선행 연결(Asset): 선행이 실행 중이고 자기는 아직 안 돌고 있는 워크플로우</li>
 *   <li>하위 워크플로우: 상위가 실행 중인데 그 상위의 이번 실행에서 아직 시작하지 않은 하위</li>
 * </ul>
 * 이미 돌고 있는 것은 «실행 중»이라 대기가 아니다.
 */
function computeWaitingDagIds(workflows: WorkflowSummary[], runsByDag: Map<string, AirflowDagRun[]>) {
  const byId = new Map(workflows.map((w) => [w.id, w] as const));
  const runningRun = (dagId: string) => (runsByDag.get(dagId) ?? []).find((run) => run.state === "running");
  const waiting = new Set<string>();
  for (const workflow of workflows) {
    if (!workflow.published || runningRun(workflow.dagId)) continue;
    const upstreamRunning = (workflow.upstreamWorkflowIds ?? [])
      .some((id) => { const up = byId.get(id); return Boolean(up && runningRun(up.dagId)); });
    if (upstreamRunning) waiting.add(workflow.dagId);
  }
  for (const parent of workflows) {
    const parentRun = runningRun(parent.dagId);
    if (!parentRun) continue;
    const parentStart = dayjs(runAt(parentRun) ?? 0);
    for (const childId of parent.childWorkflowIds) {
      const child = byId.get(childId);
      if (!child || !child.published || runningRun(child.dagId)) continue;
      const latest = (runsByDag.get(child.dagId) ?? [])[0];
      // 상위의 이번 실행 뒤에 시작한 적이 없으면 아직 차례를 기다리는 중이다.
      if (!latest || dayjs(runAt(latest) ?? 0).isBefore(parentStart)) waiting.add(child.dagId);
    }
  }
  return waiting;
}

/**
 * 워크플로우 현황 카드(2026-10-02 - CDC · ETL 두 장을 한 장으로 합쳤다).
 *
 * <p>CDC 도 워크플로우로만 제어하므로(2026-10-01) 나눌 이유가 없다. 모든 숫자가 <b>당일 · 워크플로우(DAG) 기준</b>이고,
 * 각 워크플로우의 <b>오늘 마지막 실행</b> 상태로 한 번씩만 센다 - 오늘 세 번 실패해도 «실패 1»이다.
 * «실행 중»은 날짜와 무관하게 지금 돌고 있는 것, «대기»는 큐에 오른 실행 + 실행 중인 워크플로우의 후행이다.
 *
 * <p>숫자를 누르면 가운데 목록이 그 대상으로 좁혀진다.
 */
function StatusCard({
  dags,
  runsByDag,
  waitingDagIds,
  activeMetric,
  onMetricClick,
}: {
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  waitingDagIds: Set<string>;
  activeMetric?: StatusMetric;
  onMetricClick: (metric: StatusMetric) => void;
}) {
  const today = dayjs().startOf("day");
  const count = (key: StatusMetric) =>
    dags.filter((dag) => matchesMetric(key, dag, runsByDag.get(dag.dag_id) ?? [], today, waitingDagIds)).length;

  // 0 인 칸은 숫자를 흐리게 눌러 둬서 값이 있는 칸이 먼저 눈에 들어오게 한다.
  const metric = (key: StatusMetric, icon: WfIconName, label: string, tone: string) => {
    const value = count(key);
    return (
      <button
        type="button"
        className={`airflow-business-metric${activeMetric === key ? " active" : ""}`}
        aria-pressed={activeMetric === key}
        onClick={() => onMetricClick(key)}
      >
        <span className="airflow-business-metric-label">
          <WfIcon name={icon} size={12} strokeWidth={3} className={`wf-tone-${tone}`} />
          {label}
        </span>
        <strong className={`airflow-business-metric-value wf-tone-${value === 0 ? "muted" : tone}`}>
          {value}
        </strong>
      </button>
    );
  };
  const metrics = [
    metric("running", "play", "실행 중", "blue"),
    metric("success", "check", "성공", "green"),
    metric("failed", "x", "실패", "red"),
    metric("waiting", "clock", "대기", "amber"),
  ];

  return (
    <section className="airflow-business-card airflow-business-card--etl">
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

/**
 * 트리 줄의 화살표. 잎(없음) / 줄을 누르면 같이 접히는 폴더(row) / 줄과 따로 눌리는 접기 버튼(button).
 * 마지막은 하위가 있는 게시 워크플로우 - 줄은 «고르기», 화살표는 «접기»라 둘을 떼어 둔다.
 */
type TreeArrow =
  | { kind: "none" }
  | { kind: "row"; open: boolean }
  | { kind: "button"; open: boolean; onToggle: () => void };

function BusinessTree({
  dags,
  selectedDagId,
  search,
  onSearch,
  workflows,
  scope,
  onScope,
  onSelect,
  onDetail,
  onExecution,
  onRefresh,
  refreshing,
}: {
  dags: DashboardDag[];
  /** 워크플로우 목록. ETL 가지는 이 상하 관계로 그린다(스케줄링 화면 트리와 같은 모양). */
  workflows: WorkflowSummary[];
  scope?: ListScope;
  onScope: (scope: ListScope) => void;
  selectedDagId?: string;
  search: string;
  onSearch: (value: string) => void;
  onSelect: (dagId: string) => void;
  onDetail: (dag: DashboardDag) => void;
  onExecution: (dag: DashboardDag) => void;
  /** 트리 머리 «새로고침» - 목록을 다시 불러온다. */
  onRefresh: () => void;
  refreshing: boolean;
}) {
  const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(new Set());
  /**
   * 지금까지 그려 본 «하위가 있는 칸»의 키. «전체 접기»가 쓴다 - 접힌 칸의 자손은 그려지지 않아서,
   * 그 순간 보이는 것만 모으면 다시 펼쳤을 때 속이 펼쳐진 채로 나온다. 한 번 본 키는 지우지 않는다.
   */
  const branchKeys = useRef<Set<string>>(new Set());
  const rememberBranch = (key: string) => {
    branchKeys.current.add(key);
    return key;
  };
  const seeded = useRef(false);
  // 실행(트리거)은 Airflow 쓰기 권한이 있어야 한다. 권한이 아직 안 실렸으면 사이드바와
  // 같은 규칙으로 fail-open 한다(실제 차단은 백엔드가 한다).
  const { can, permissionsLoaded } = useAuth();
  const canRun = !permissionsLoaded || can("AIRFLOW", "WRITE");

  /**
   * 처음 열었을 때의 트리 모양.
   *
   * <p>기본값이 «전부 펼침»이라 들어오자마자 워크플로우가 다 쏟아졌다.
   * 최상단 워크플로우까지만 보이게 접어두고, 필요한 가지를 사용자가 펼치게 한다.
   */
  useEffect(() => {
    if (seeded.current || workflows.length === 0) {
      return;
    }
    seeded.current = true;
    const initial = new Set<string>();
    // 최상단 워크플로우까지만 보이게 하고 하위는 접어 둔다.
    const collapseChildren = (items: WorkflowTreeItem[]) => {
      items.forEach((item) => {
        if (item.children.length) {
          initial.add(`wf:${item.workflow.id}`);
        }
        collapseChildren(item.children);
      });
    };
    collapseChildren(buildWorkflowHierarchy(workflows.filter((w) => w.published)));
    setCollapsedNodes(initial);
  }, [workflows]);
  /**
   * 가운데 목록에서 고른 항목이 트리에서 보이도록 조상 가지를 편다.
   *
   * 트리를 최상단 워크플로우까지만 펼쳐 두다 보니, 목록에서 워크플로우를 눌러도
   * 왼쪽에서는 어디에 있는 것인지 보이지 않았다. 고른 항목까지의 길만 열어준다.
   */
  useEffect(() => {
    const open = new Set<string>();
    if (selectedDagId) {
      // 고른 워크플로우까지 내려오는 «조상»만 편다(형제 가지는 접힌 채로 둔다).
      const trail: number[] = [];
      const find = (items: WorkflowTreeItem[], path: number[]): boolean => items.some((item) => {
        const next = [...path, item.workflow.id];
        if (item.workflow.dagId === selectedDagId) {
          trail.push(...next);
          return true;
        }
        return find(item.children, next);
      });
      if (find(buildWorkflowHierarchy(workflows.filter((w) => w.published)), [])) {
        open.add("category:ETL");
        trail.forEach((id) => open.add(`wf:${id}`));
      }
    }
    if (open.size === 0) {
      return;
    }
    setCollapsedNodes((current) => {
      if (![...open].some((key) => current.has(key))) {
        return current;      // 이미 다 열려 있으면 상태를 건드리지 않는다
      }
      const next = new Set(current);
      open.forEach((key) => next.delete(key));
      return next;
    });
  }, [selectedDagId, workflows, dags]);

  const toggleNode = (node: string) => {
    setCollapsedNodes((current) => {
      const next = new Set(current);
      if (next.has(node)) next.delete(node);
      else next.add(node);
      return next;
    });
  };

  /**
   * 트리 한 줄. 스케줄링 화면 트리와 같은 마크업이다(tree-item-row · ▶ · 폴더/파일 아이콘 · 개수 배지).
   *
   * <p>줄 전체가 눌리는 자리라 div + role=button 으로 두고 Enter/Space 도 받는다 - 하위가 있는
   * 워크플로우 줄에는 접기 버튼이 따로 들어가서 button 안에 button 을 넣을 수 없다.
   * 이름은 초안처럼 한 줄로 자르고, 전체 이름은 마우스를 올리면(title) 보인다.
   */
  const treeRow = ({
    active = false,
    pending = false,
    icon,
    label,
    title,
    arrow,
    trailing,
    onClick,
    onContextMenu,
  }: {
    active?: boolean;
    pending?: boolean;
    icon: WfIconName;
    label: React.ReactNode;
    title?: string;
    arrow: TreeArrow;
    trailing?: React.ReactNode;
    onClick: () => void;
    onContextMenu?: () => void;
  }) => (
    <div
      className={`tree-item-row${active ? " active" : ""}${pending ? " is-pending" : ""}`}
      role="button"
      tabIndex={0}
      aria-expanded={arrow.kind === "none" ? undefined : arrow.open}
      onClick={onClick}
      onContextMenu={onContextMenu}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onClick();
        }
      }}
    >
      {arrow.kind === "button" ? (
        <button
          type="button"
          className={`tree-toggle-arrow${arrow.open ? " open" : ""}`}
          aria-label={arrow.open ? "하위 워크플로우 접기" : "하위 워크플로우 펼치기"}
          onClick={(event) => {
            // 접기만 한다 - 줄 선택까지 같이 되면 접으려다 화면이 바뀐다.
            event.stopPropagation();
            arrow.onToggle();
          }}
          onKeyDown={(event) => event.stopPropagation()}
        >
          ▶
        </button>
      ) : (
        <span
          className={`tree-toggle-arrow${arrow.kind === "row" ? (arrow.open ? " open" : "") : " empty"}`}
          aria-hidden="true"
        >
          ▶
        </span>
      )}
      <WfIcon name={icon} size={15} className="tree-node-icon" />
      <span className="tree-node-label" title={title}>{label}</span>
      {trailing}
    </div>
  );

  /** DAG 하나를 트리 항목으로. 우클릭 메뉴(상세/실행설정/삭제)는 기존과 동일하다. */
  const renderDagButton = (dag: DashboardDag, arrow: TreeArrow = { kind: "none" }, icon: WfIconName = "file") => (
    <Dropdown
      key={dag.dag_id}
      trigger={["contextMenu"]}
      menu={{
        // 삭제는 여기서 뺐다. nifi_pipeline_* 삭제는 NiFi 프로세스 그룹까지 지우는
        // 되돌릴 수 없는 동작이라, 트리 우클릭처럼 스치듯 눌리는 자리에 둘 것이 아니다.
        items: [
          { key: "detail", icon: <InfoCircleOutlined />, label: "상세" },
          // 쓰기 권한이 없으면 실행 항목 자체를 빼서 «눌렀더니 403» 을 만들지 않는다.
          ...(canRun ? [{ key: "execution", icon: <PlayCircleOutlined />, label: "실행 설정" }] : []),
        ],
        onClick: ({ key }) => {
          if (key === "detail") onDetail(dag);
          else onExecution(dag);
        },
      }}
    >
      {treeRow({
        active: selectedDagId === dag.dag_id,
        icon,
        label: displayName(dag),
        title: displayName(dag),
        arrow,
        onClick: () => { onSelect(dag.dag_id); onScope({ kind: "dag", dagId: dag.dag_id }); },
        onContextMenu: () => onSelect(dag.dag_id),
      })}
    </Dropdown>
  );

  /**
   * ETL 가지는 «워크플로우 계층»으로 그린다 - 스케줄링 화면 왼쪽 트리와 같은 모양이다.
   *
   * <p>예전에는 NiFi 업무 그룹(DW/DZ) 아래에 워크플로우를 붙였다. 워크플로우에서 업무 그룹
   * 개념을 걷어내면서 붙일 자리가 없어졌고, 애초에 여기서 알고 싶은 것은 «total 을 돌리면
   * 무엇이 같이 도는가»라 그룹보다 상하 관계가 맞다.
   *
   * <p>게시한 것만 보여준다. 이 화면은 «지금 도는 것»을 보는 자리라 Airflow 에 DAG 가 없는
   * 워크플로우는 볼 것도 누를 것도 없다(알림 감시대상 트리도 같은 기준이다).
   * 미게시 상위 밑에 게시된 하위가 있으면 그 하위는 최상단으로 올라온다 - 빌더가 목록에 없는
   * 부모를 «없는 것»으로 보기 때문이라, 게시된 워크플로우가 트리에서 사라지지는 않는다.
   */
  const publishedWorkflows = workflows.filter((workflow) => workflow.published);
  const workflowHierarchy = sortFoldersFirst(buildWorkflowHierarchy(publishedWorkflows));

  /**
   * 트리에 자리를 갖는 DAG. 워크플로우 계층에 올라간 것이 전부다.
   *
   * <p>여기 없는 ETL DAG(워크플로우로 아직 안 옮긴 옛 팩토리 DAG 등)는 트리 맨 아래에
   * 따로 늘어놓는다 - 어디에도 안 보여서 존재를 모르는 일이 없도록.
   */
  const placedDagIds = new Set(workflows.map((workflow) => workflow.dagId));

  /**
   * 워크플로우 한 칸과 그 하위. DAG 가 있으면 그 자리에서 바로 고르고 우클릭 메뉴도 붙는다
   * (게시 전이라 DAG 가 없으면 이름만 보여준다 - 있는데 안 보이는 것보다 낫다).
   */
  const renderWorkflowTree = (item: WorkflowTreeItem, etlDags: DashboardDag[]): React.ReactNode => {
    const needle = search.trim().toLowerCase();
    const nodeKey = `wf:${item.workflow.id}`;
    const collapsed = collapsedNodes.has(item.children.length > 0 ? rememberBranch(nodeKey) : nodeKey);
    const renderedChildren = collapsed ? [] : item.children
      .map((child) => renderWorkflowTree(child, etlDags))
      .filter(Boolean);
    // 검색 중이면 자신 또는 자손이 걸리는 가지만 남긴다.
    const selfMatches = !needle || item.workflow.name.toLowerCase().includes(needle);
    if (needle && !selfMatches && renderedChildren.length === 0) {
      return null;
    }
    const dag = etlDags.find((d) => d.dag_id === item.workflow.dagId);
    const hasChildren = item.children.length > 0;
    return (
      <div key={item.workflow.id} className="tree-node">
        {/*
          게시된 워크플로우는 DAG 줄 그대로 둔다(선택·우클릭 메뉴가 붙어야 한다). 다만
          하위가 있으면 접기 화살표를 따로 눌리게 한다 - 줄과 같이 눌리면 누를 때마다
          선택까지 같이 돼서 접으려다 화면이 바뀐다.
        */}
        {dag
          ? renderDagButton(
              dag,
              hasChildren ? { kind: "button", open: !collapsed, onToggle: () => toggleNode(nodeKey) } : { kind: "none" },
              hasChildren ? "folder" : "file",
            )
          : treeRow({
              pending: true,
              icon: hasChildren ? "folder" : "file",
              // 게시했는데 Airflow 가 아직 DAG 를 안 읽은 짧은 구간. 이름만 자리를 지킨다.
              label: <>{item.workflow.name}<span className="tree-node-note"> (DAG 준비 중)</span></>,
              title: `${item.workflow.name} (DAG 준비 중)`,
              arrow: hasChildren ? { kind: "row", open: !collapsed } : { kind: "none" },
              onClick: () => {
                if (hasChildren) {
                  toggleNode(nodeKey);
                }
              },
            })}
        {renderedChildren.length > 0 && <div className="tree-sub-group" role="group">{renderedChildren}</div>}
      </div>
    );
  };

  return (
    <aside className="airflow-dashboard-panel airflow-business-tree">
      {/* 트리 제목 줄(«업무별 트리»)은 뺐다(사용자 요청) - 스케줄링 트리와 같이 검색칸부터 시작한다.
          머리 단추(새로고침 · 전체 접기)는 ETL 관리 트리와 같다(2026-10-01 요청). */}
      <TreeHead onRefresh={onRefresh} refreshing={refreshing}
                refreshTitle="트리를 다시 불러옵니다"
                onCollapseAll={() => setCollapsedNodes(new Set(branchKeys.current))}
                onExpandAll={() => setCollapsedNodes(new Set())} />
      <input className="nifi-tree-search-input" aria-label="트리 검색" placeholder="이름"
             value={search} onChange={(event) => onSearch(event.target.value)} />
      <div className="airflow-tree-body" role="tree" aria-label="업무별 트리">
        {CATEGORY_ORDER.map((category) => {
          const categoryDags = dags.filter((dag) => categoryOf(dag) === category);
          const categoryNode = rememberBranch(`category:${category}`);
          const categoryCollapsed = collapsedNodes.has(categoryNode);
          return (
            <section key={category} className={`tree-node airflow-tree-category airflow-tree-category--${category.toLowerCase()}`}>
              {treeRow({
                active: scope?.kind === "category" && scope.category === category,
                icon: "folder",
                // 최상단 이름은 CDC · ETL 관리 트리(«CDC Root» · «ETL Root»)와 같은 꼴(2026-10-11 요청). 내부 분류 값은 ETL 그대로다.
                label: category === "ETL" ? "Workflow Root" : category,
                arrow: { kind: "row", open: !categoryCollapsed },
                trailing: <span className="tree-node-badge">{categoryDags.length}</span>,
                // 누르면 «그 가지 전부»를 가운데에 편다. 접혀 있으면 함께 펼치고, 이미 보고 있는 가지를
                // 다시 누를 때만 접는다 - 목록을 보려다 하위가 사라지지 않게 한다.
                onClick: () => {
                  const showing = scope?.kind === "category" && scope.category === category;
                  if (categoryCollapsed || showing) toggleNode(categoryNode);
                  onScope({ kind: "category", category });
                },
              })}
              {!categoryCollapsed && (
                <div className="tree-sub-group" role="group">
                  {category === "ETL" && workflowHierarchy.map((item) => renderWorkflowTree(item, categoryDags))}
                  {category === "ETL" && categoryDags
                    // 워크플로우 계층에 이미 붙은 DAG 는 여기서 또 보여주지 않는다.
                    .filter((dag) => !placedDagIds.has(dag.dag_id))
                    .map((dag) => <div key={dag.dag_id} className="tree-node">{renderDagButton(dag)}</div>)}
                </div>
              )}
            </section>
          );
        })}
      </div>
    </aside>
  );
}

function TaskRows({
  dag,
  run,
  refreshSeconds,
  configured = false,
  auxColumn = true,
}: {
  dag: DashboardDag;
  run?: AirflowDagRun;
  refreshSeconds: number;
  configured?: boolean;
  /**
   * 7번째 «보조» 열을 그릴지. 이 표를 쓰는 화면마다 그 자리의 머리글이 다른데
   * (다음 실행 / 스케줄 / 없음) task 행에는 넣을 값이 없어 늘 "-" 였다.
   * 열이 아예 없는 표(실행 이력)에서는 꺼야 칸이 밀리지 않는다.
   */
  auxColumn?: boolean;
}) {
  const [retryingTaskId, setRetryingTaskId] = useState<string>();
  const [expandedJobKey, setExpandedJobKey] = useState<string>();
  // «적재/실패» 칸의 «로그» 버튼으로 여는 실패 로그 창의 대상.
  const [failureLog, setFailureLog] = useState<FailureLogTarget>();
  const tasksQuery = useQuery({
    queryKey: ["airflow-dashboard-tasks", dag.dag_id, run?.dag_run_id],
    queryFn: () => listAirflowTaskInstances(dag.dag_id, run!.dag_run_id),
    enabled: Boolean(run) && !configured,
    refetchInterval: refreshSeconds * 1000,
  });
  const etlJobQuery = useQuery({
    queryKey: ["airflow-dashboard-etl-job", dag.etl_job_id],
    queryFn: () => getEtlJob(dag.etl_job_id!),
    enabled: configured && Boolean(dag.etl_job_id),
    refetchInterval: refreshSeconds * 1000,
  });
  // 적재 건수를 읽으려고 성공한 task 로그만 미리 받는다. 실패 로그는 «로그» 버튼을 누를 때 창에서 따로 받는다
  // (예전에는 실패 사유 한 줄을 뽑으려고 실패 task 로그까지 전부 받았다).
  const completedTasks = (tasksQuery.data ?? []).filter((task) => task.state === "success");
  const logsQuery = useQuery({
    queryKey: ["airflow-dashboard-task-logs", dag.dag_id, run?.dag_run_id, completedTasks.map((task) => `${task.task_id}:${task.try_number}`).join(",")],
    queryFn: async () => Object.fromEntries(await Promise.all(completedTasks.map(async (task): Promise<[string, string]> => [
      task.task_id,
      await getAirflowTaskLog(dag.dag_id, run!.dag_run_id, task.task_id, task.try_number || 1).catch(() => ""),
    ]))),
    enabled: Boolean(run && completedTasks.length) && !configured,
    staleTime: Infinity,
  });

  const outcomeDetail = (taskId: string, state?: string) => {
    const log = logsQuery.data?.[taskId] ?? "";
    if (state === "success") {
      // await 로그의 "이번 실행 적재 건수 (총 99,594행)"에서 합계를 읽는다.
      // 뒤따르는 프로세서별 줄도 같은 모양이라 «마지막»을 집으면 합계가 아닌 한 줄이 잡힌다.
      const total = log.match(/총\s*([\d,]+)\s*행/);
      if (total) {
        return `${total[1]}행`;
      }
      if (/적재 건수: 없음/.test(log)) {
        return "0행";
      }
      const counts = [...log.matchAll(/([\d,]+)\s*(?:행|건)/g)];
      return counts.at(-1) ? `${counts.at(-1)![1]}행` : "-";
    }
    if (state === "upstream_failed") return "선행 작업 실패로 미실행";
    // 실패는 사유 문구 대신 «로그» 버튼을 그린다(2026-09-11 요청). 로그 한 줄을 180자로 잘라 보여 주던
    // 예전 방식은 원인이 잘리거나 엉뚱한 줄을 집었다 - 실패 원인은 TaskFailureLogModal 이 원문으로 보여 준다.
    return "-";
  };
  /** job 키(= TaskGroup 이름). 화면에 보이는 단위와 같게 맞춘다. */
  const jobKeyOf = (taskId: string) => {
    const dot = taskId.lastIndexOf(".");
    return dot > 0 ? taskId.slice(0, dot) : taskId;
  };

  const doRerun = async (
    jobKey: string, taskIds: string[], withDownstream: boolean, markBlockers: string[] = [],
  ) => {
    if (!run) return;
    setRetryingTaskId(jobKey);
    try {
      // 선행이 failed 로 남아 있으면 clear 해봐야 스케줄러가 곧바로 upstream_failed 로
      // 되돌린다. 먼저 선행을 성공으로 표시해야 이 job 이 실제로 돈다.
      for (const taskId of markBlockers) {
        await markAirflowTaskSuccess(dag.dag_id, run.dag_run_id, taskId);
      }
      // job 안의 단계를 한꺼번에 지운다. 한 단계만 지우면 그 job이 반쪽만 다시 돈다.
      await retryAirflowTasks(dag.dag_id, run.dag_run_id, taskIds, withDownstream);
      message.success(withDownstream
        ? `${jobKey}부터 후행까지 재실행했습니다.`
        : `${jobKey}만 재실행했습니다.`);
      await tasksQuery.refetch();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "작업 재실행에 실패했습니다.");
    } finally {
      setRetryingTaskId(undefined);
    }
  };

  const rerunJob = async (jobKey: string, taskIds: string[], withDownstream: boolean) => {
    if (!run) return;
    const all = tasksQuery.data ?? [];
    const own = all.filter((task) => taskIds.includes(task.task_id));
    /*
     * «선행 때문에 막힌» job 은 자기 단계 중 실패가 <b>하나도 없고</b> upstream_failed 만
     * 있는 경우다. 자기가 실패한 job 도 뒷단계는 upstream_failed 로 남으므로
     * (예: await 실패 → verify 는 upstream_failed), upstream_failed 만 보고 판단하면
     * 자기 자신의 실패를 «선행»으로 착각해 성공 처리해 버린다.
     */
    const blocked = own.some((task) => task.state === "upstream_failed")
      && !own.some((task) => task.state === "failed");
    if (!blocked) {
      await doRerun(jobKey, taskIds, withDownstream);
      return;
    }
    /*
     * 선행이 실패로 남아 있는 job 을 그냥 clear 하면 «성공했습니다» 만 뜨고 아무 일도
     * 일어나지 않는다 - Airflow 가 의존성을 다시 보고 즉시 upstream_failed 로 되돌리기
     * 때문이다(실측 2026-09-04: wf_postgresql_dm_order_daily 를 눌러도 상태 그대로).
     * 그래서 여기서 막고, 선행을 성공으로 표시할지 확인을 받는다.
     *
     * 선행을 «다시 돌리고» 싶은 경우는 그 행의 «이 작업부터 실행» 이 이미 해 주므로
     * 여기서는 다루지 않는다. 여기서 필요한 건 «다시 돌려도 또 실패하는 선행을 건너뛰는»
     * 길이다(어제 사례: 선행이 외래키 위반이라 재실행해도 결과가 같았다).
     */
    const blockers = all.filter((task) => task.state === "failed" && !taskIds.includes(task.task_id));
    const blockerJobs = [...new Set(blockers.map((task) => jobKeyOf(task.task_id)))];
    if (!blockerJobs.length) {
      message.warning("선행 작업이 끝나지 않아 아직 실행할 수 없습니다.");
      return;
    }
    Modal.confirm({
      title: "선행 작업이 실패로 남아 있습니다",
      okText: "선행을 성공 처리하고 실행",
      okButtonProps: { danger: true },
      cancelText: "취소",
      width: 560,
      content: (
        <div>
          <p>
            <b>{blockerJobs.join(", ")}</b> 이(가) 실패 상태여서, <b>{jobKey}</b> 만 다시 눌러도
            실행되지 않고 «선행 실패»로 되돌아갑니다.
          </p>
          <p>
            계속하면 위 선행 작업을 <b>성공으로 표시한 뒤</b> {jobKey} 을(를) 실행합니다.
            상태만 바꾸는 것이라 <b>선행의 적재는 일어나지 않습니다</b> - 빠진 데이터는 그대로입니다.
          </p>
          <p style={{ marginBottom: 0 }}>
            선행을 <b>다시 돌리려면</b> 취소하고 해당 행의 «이 작업부터 실행»을 쓰십시오.
          </p>
        </div>
      ),
      onOk: () => doRerun(jobKey, taskIds, withDownstream, blockers.map((task) => task.task_id)),
    });
  };

  if (configured && etlJobQuery.isLoading) {
    return <tr className="airflow-task-loading"><td colSpan={8}><Spin size="small" /> ETL 실행 단계를 불러오는 중입니다.</td></tr>;
  }
  if (configured) {
    const steps = etlJobQuery.data?.steps ?? [];
    if (!steps.length) {
      return <tr className="airflow-task-loading"><td colSpan={8}>등록된 ETL 실행 단계가 없습니다.</td></tr>;
    }
    // NiFi 프로세서(trigger/extract/truncate/load 등)를 각각 나열하지 않고, 같은 체인에 속한 프로세서를
    // 하나의 작업(job)으로 묶는다. 체인 키 = step 이름의 마지막 '-' 구획(예: extract-tb-COM001M → COM001M).
    // ETL 관리 트리가 "체인 1개 = job 1개"로 보는 것과 표기를 맞춘다.
    type Step = (typeof steps)[number];
    const chainOrder: string[] = [];
    const byChain = new Map<string, Step[]>();
    for (const step of steps) {
      const parts = (step.stepName ?? "").split("-");
      const key = (parts[parts.length - 1] || step.stepName || `#${step.id}`).trim();
      if (!byChain.has(key)) {
        byChain.set(key, []);
        chainOrder.push(key);
      }
      byChain.get(key)!.push(step);
    }
    return chainOrder.map((key, index) => {
      const chainSteps = byChain.get(key)!;
      const statuses = chainSteps.map((s) => s.runStatus?.toUpperCase());
      const chainStatus = statuses.includes("INVALID") ? "INVALID"
        : statuses.includes("RUNNING") ? "RUNNING" : "STOPPED";
      const targetTable = chainSteps.find((s) => s.targetTable)?.targetTable;
      const trigger = chainSteps.find((s) => s.stepType === "GenerateFlowFile");
      const schedule = (trigger ?? chainSteps[0])?.schedulingPeriod;
      return <tr key={key} className="airflow-task-row">
        <td><span className="airflow-task-name">{String(index + 1).padStart(2, "0")} {key}</span></td>
        <td><ProfileOutlined /> 체인 ({chainSteps.length}단계)</td>
        <td>-</td>
        <td><Tag color={chainStatus === "RUNNING" ? "processing" : chainStatus === "INVALID" ? "error" : "default"}>{chainStatus === "RUNNING" ? "실행 중" : chainStatus === "INVALID" ? "오류" : "중지"}</Tag></td>
        <td>{targetTable ?? "-"}</td>
        <td>-</td>
        <td>{schedule ?? "-"}</td>
        <td>-</td>
      </tr>;
    });
  }
  if (tasksQuery.isLoading) {
    return <tr className="airflow-task-loading"><td colSpan={8}><Spin size="small" /> 실행 단계를 불러오는 중입니다.</td></tr>;
  }
  if (!run || !tasksQuery.data?.length) {
    return <tr className="airflow-task-loading"><td colSpan={8}>실행 단계가 없습니다.</td></tr>;
  }
  const runId = run.dag_run_id;
  /**
   * job 1개 = 한 줄. TaskGroup(=job) 안의 단계(open_run/start_pg/await/stop_pg/verify)를
   * 하나로 접는다. 5개 job이면 21줄이 아니라 5줄이다 - 사용자가 보고 싶은 단위는 job이다.
   * 줄을 누르면 단계별로 펼쳐지고, 실패 단계의 사유가 거기서 보인다.
   */
  const order: string[] = [];
  const byJob = new Map<string, typeof tasksQuery.data>();
  for (const task of tasksQuery.data) {
    // TaskGroup의 태스크 id는 "그룹키.단계"다. 점이 없으면 그룹 밖 태스크(workflow_done).
    const dot = task.task_id.lastIndexOf(".");
    const key = dot > 0 ? task.task_id.slice(0, dot) : task.task_id;
    if (!byJob.has(key)) {
      byJob.set(key, []);
      order.push(key);
    }
    byJob.get(key)!.push(task);
  }

  /*
   * 줄 앞의 01·02·03 은 «실행 순서»로 읽힌다. 그런데 Airflow 가 태스크를 돌려주는 순서는
   * 설계 순서가 아니라서, 직렬로 이어 그린 워크플로우인데도 번호가 뒤죽박죽으로 찍혔다
   * (실측 2026-09-03: customer→product→order→store→order_item 으로 돈 워크플로우가
   * 01 customer / 02 order / 03 order_item / 04 product / 05 store 로 보여, 맨 마지막
   * job 인 order_item 이 03 으로 찍혔다).
   *
   * 그래서 job 이 «처음 시작한 시각» 순으로 세운다. 병렬로 뜬 job 끼리는 시작 순서가 곧
   * 표시 순서다. 아직 시작 전이라 시각이 없는 job 은 뒤로 보내되 원래 순서를 지킨다
   * (정렬이 안정적이어야 실행 중에 줄이 튀지 않는다).
   */
  const firstStartOf = (key: string) => {
    // 문자열 비교가 아니라 시각으로 본다(오프셋 표기가 섞여도 안전하다).
    const starts = byJob.get(key)!
      .map((step) => (step.start_date ? new Date(step.start_date).getTime() : Number.NaN))
      .filter((at) => Number.isFinite(at));
    return starts.length ? Math.min(...starts) : undefined;
  };
  const apiOrder = new Map(order.map((key, index) => [key, index] as const));
  const startedAtOf = new Map(order.map((key) => [key, firstStartOf(key)] as const));
  order.sort((a, b) => {
    const left = startedAtOf.get(a);
    const right = startedAtOf.get(b);
    if (left !== undefined && right !== undefined && left !== right) return left - right;
    if (left !== undefined && right === undefined) return -1;
    if (left === undefined && right !== undefined) return 1;
    return apiOrder.get(a)! - apiOrder.get(b)!;
  });

  /** 단계 상태를 job 하나의 상태로 접는다. 하나라도 실패면 실패다. */
  const rollUpState = (steps: NonNullable<typeof tasksQuery.data>) => {
    const states = steps.map((step) => step.state);
    if (states.includes("failed")) return "failed";
    if (states.some((state) => state && ACTIVE_TASK_STATES.includes(state))) return "running";
    if (states.includes("upstream_failed")) return "upstream_failed";
    if (states.every((state) => state === "success")) return "success";
    return states.find(Boolean) ?? undefined;
  };

  const rows = order.flatMap((key, index) => {
    const steps = byJob.get(key)!;
    const state = rollUpState(steps);
    const failed = steps.find((step) => step.state === "failed");
    const starts = steps.map((step) => step.start_date).filter(Boolean) as string[];
    const ends = steps.map((step) => step.end_date).filter(Boolean) as string[];
    const startedAt = starts.sort()[0];
    const endedAt = ends.sort().at(-1);
    const retryCount = steps.reduce((sum, step) => sum + Math.max(0, (step.try_number ?? 1) - 1), 0);
    // 적재 건수는 await 단계 로그에 찍힌다("이번 실행 적재 건수 (총 N건)").
    // 마지막 단계(verify/stop_pg) 로그를 보면 건수가 없어 늘 0건으로 나온다.
    const counted = steps.find((step) => step.task_id.endsWith(".await")) ?? steps.at(-1)!;
    // 실패한 job 은 문구 대신 «로그» 버튼을 그린다(아래 셀) - 실패한 단계들의 로그를 창으로 연다.
    const detail = failed ? ""
      : isCdcSendStep(steps) && state === "success" ? "지시 보냄"
      : state === "upstream_failed" ? "선행 작업 실패로 미실행"
      : state === "success" ? outcomeDetail(counted.task_id, "success")
      : "-";
    const expanded = expandedJobKey === key;
    const busy = Boolean(retryingTaskId && retryingTaskId !== key);
    // 그룹 밖 태스크(workflow_done)는 job이 아니라 워크플로우 종단 신호다.
    const isJob = steps.length > 1 || steps[0].task_id.includes(".");
    // CDC 워크플로우의 노드는 제어 DAG 에 지시만 보내는 한 단계다(send). «성공»은 «지시를 보냈다»는 뜻이라
    // JOB·적재 건수처럼 보이면 CDC 가 돌았다고 읽힌다 - 실제 상태는 오른쪽 속성창의 파이프라인 상태를 본다.
    const isCdcSend = isCdcSendStep(steps);

    const jobRow = (
      <tr key={key} className={`airflow-task-row${expanded ? " selected" : ""}`}>
        <td>
          {isJob ? (
            <button type="button" className="airflow-task-name"
                    onClick={() => setExpandedJobKey(expanded ? undefined : key)}>
              {expanded ? <DownOutlined /> : <RightOutlined />} {String(index + 1).padStart(2, "0")} {key}
            </button>
          ) : (
            <span className="airflow-task-name">{String(index + 1).padStart(2, "0")} {key}</span>
          )}
        </td>
        <td><ProfileOutlined /> {isCdcSend ? "CDC 지시" : isJob ? `JOB (${steps.length}단계)` : "워크플로우 종단"}</td>
        <td>{startedAt ? dayjs(startedAt).format("YYYY-MM-DD HH:mm") : "-"}</td>
        <td><Tag color={stateColor(state)}>{stateLabel(state)}</Tag></td>
        {/* 성공이면 적재 건수, 실패면 «로그» 버튼(2026-09-11 요청). 열 폭을 넘는 문구는 셀 title 로 남긴다. */}
        <td title={failed ? undefined : detail}>
          {failed ? (
            <Button size="small" danger className="airflow-task-log-button"
                    onClick={() => setFailureLog({
                      title: `${String(index + 1).padStart(2, "0")} ${key}`,
                      dagId: dag.dag_id,
                      runId,
                      tasks: steps.filter((step) => step.state === "failed"),
                    })}>
              로그
            </Button>
          ) : detail}
        </td>
        <td>{duration(startedAt, endedAt)}</td>
        {auxColumn && <td>-</td>}
        <td>
          {retryCount}회{" "}
          {/* job 통째로 다시 돌린다. 값 하나만 고쳐 다시 넣을 때와, 중간이 막혀
              뒤까지 다시 돌려야 할 때가 다르다. */}
          {isJob && (
            <Space size={4}>
              <Button size="small" loading={retryingTaskId === key} disabled={busy}
                      onClick={() => void rerunJob(key, steps.map((step) => step.task_id), false)}>
                이 작업만 실행
              </Button>
              <Button type={state === "failed" ? "primary" : "default"} size="small"
                      loading={retryingTaskId === key} disabled={busy}
                      onClick={() => void rerunJob(key, steps.map((step) => step.task_id), true)}>
                이 작업부터 실행
              </Button>
            </Space>
          )}
        </td>
      </tr>
    );
    if (!expanded || !isJob) {
      return [jobRow];
    }
    return [jobRow, ...steps.map((step) => {
      const stepDetail = outcomeDetail(step.task_id, step.state);
      return (
        <tr key={`${key}::${step.task_id}`} className="airflow-task-row airflow-task-step">
          <td style={{ paddingLeft: 32 }}>{step.task_id.split(".").at(-1)}</td>
          <td>단계</td>
          <td>{step.start_date ? dayjs(step.start_date).format("HH:mm:ss") : "-"}</td>
          <td><Tag color={stateColor(step.state)}>{stateLabel(step.state)}</Tag></td>
          <td>
            {step.state === "failed" ? (
              <Button size="small" danger className="airflow-task-log-button"
                      onClick={() => setFailureLog({
                        title: `${key} · ${step.task_id.split(".").at(-1)}`,
                        dagId: dag.dag_id,
                        runId,
                        tasks: [step],
                      })}>
                로그
              </Button>
            ) : stepDetail}
          </td>
          <td>{duration(step.start_date, step.end_date)}</td>
          {auxColumn && <td>-</td>}
          <td>{Math.max(0, (step.try_number ?? 1) - 1)}회</td>
        </tr>
      );
    })];
  });
  return [
    ...rows,
    // 실패 로그 창. antd Modal 은 body 로 포털되므로 표(tbody) 안에 두어도 DOM 은 표 밖에 생긴다.
    <TaskFailureLogModal key="failure-log" target={failureLog} onClose={() => setFailureLog(undefined)} />,
  ];
}

function ScopeList({
  scope,
  dags,
  runsByDag,
  waitingDagIds,
  selectedDagId,
  onSelectDag,
}: {
  scope: ListScope;
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  waitingDagIds: Set<string>;
  selectedDagId?: string;
  onSelectDag: (dagId: string) => void;
}) {
  const today = dayjs().startOf("day");

  // ETL 그룹 / ETL 지표 선택 -> DAG(워크플로우) 목록
  // 당일 실행분만 본다. 오늘 안 돈 워크플로우는 «실행 없음»으로 비워 두는 편이,
  // 며칠 전 결과를 오늘 것처럼 보여주는 것보다 정확하다.
  const latestRun = (dagId: string) => latestRunToday(runsByDag.get(dagId) ?? [], today);
  let rows: DashboardDag[] = [];
  if (scope.kind === "metric") {
    rows = dags.filter((dag) => categoryOf(dag) === scope.category
      && matchesMetric(scope.metric, dag, runsByDag.get(dag.dag_id) ?? [], today, waitingDagIds));
  } else if (scope.kind === "dag") {
    // 그 DAG 한 줄만.
    rows = dags.filter((dag) => dag.dag_id === scope.dagId);
  }

  return (
    <Table<DashboardDag>
      rowKey="dag_id"
      size="small"
      dataSource={rows}
      scroll={{ x: 820 }}
      pagination={{ pageSize: 20, hideOnSinglePage: true, showTotal: (total) => `총 ${total}건` }}
      rowClassName={(row) => row.dag_id === selectedDagId ? "airflow-row-selected" : ""}
      onRow={(row) => ({ onClick: () => onSelectDag(row.dag_id) })}
      locale={{ emptyText: <Empty description="이 범위에 워크플로우가 없습니다." /> }}
      columns={[
        {
          title: "워크플로우",
          render: (_, row) => (
            <Space direction="vertical" size={0}>
              <strong>{displayName(row)}</strong>
              <span style={{ color: "#888", fontSize: 12 }}>{row.dag_id}</span>
            </Space>
          ),
        },
        {
          title: "시작 시간",
          width: 150,
          render: (_, row) => {
            const at = runAt(latestRun(row.dag_id));
            return at ? dayjs(at).format("YYYY-MM-DD HH:mm") : "-";
          },
        },
        {
          title: "종료 시간",
          width: 150,
          render: (_, row) => {
            const end = latestRun(row.dag_id)?.end_date;
            return end ? dayjs(end).format("YYYY-MM-DD HH:mm") : "-";
          },
        },
        {
          title: "소요 시간",
          width: 100,
          render: (_, row) => {
            const run = latestRun(row.dag_id);
            return duration(runAt(run), run?.end_date);
          },
        },
        {
          title: "상태",
          width: 100,
          render: (_, row) => {
            const run = latestRun(row.dag_id);
            return <Tag color={stateColor(run?.state)}>{stateLabel(run?.state)}</Tag>;
          },
        },
      ]}
    />
  );
}

function JobsTable({
  dags,
  runsByDag,
  selectedDagId,
  refreshSeconds,
  onSelect,
}: {
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  selectedDagId?: string;
  refreshSeconds: number;
  onSelect: (dagId: string) => void;
}) {
  if (!dags.length) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="표시할 작업이 없습니다." />;
  }
  return (
    <div className="airflow-job-table-wrap">
      <table className="airflow-job-table">
        <thead><tr><th>작업명</th><th>유형</th><th>최근 실행</th><th>최종 결과</th><th>소요</th><th>다음 실행</th><th>재시도/재시작</th></tr></thead>
        <tbody>
          {CATEGORY_ORDER.map((category) => {
            const categoryDags = dags.filter((dag) => categoryOf(dag) === category);
            if (!categoryDags.length) return null;
            return [
              <tr key={`${category}-heading`} className="airflow-job-group"><td colSpan={7}>{category}</td></tr>,
              ...categoryDags.flatMap((dag) => {
                const runs = runsByDag.get(dag.dag_id) ?? [];
                const latest = runs[0];
                const selected = dag.dag_id === selectedDagId;
                return [
                  <tr key={dag.dag_id} className={selected ? "airflow-dag-row selected" : "airflow-dag-row"} onClick={() => onSelect(dag.dag_id)}>
                    <td><button type="button">{displayName(dag)}</button></td>
                    <td><DagIcon /> DAG</td>
                    <td>{runAt(latest) ? dayjs(runAt(latest)).format("YYYY-MM-DD HH:mm") : "-"}</td>
                    <td><Tag color={stateColor(latest?.state)}>{stateLabel(latest?.state)}</Tag></td>
                    <td>{duration(latest?.start_date, latest?.end_date)}</td>
                    <td>{dag.next_dagrun ?? dag.next_dagrun_run_after ? dayjs(dag.next_dagrun ?? dag.next_dagrun_run_after).format("YYYY-MM-DD HH:mm") : "-"}</td>
                    <td>{selected ? <DownOutlined /> : "-"}</td>
                  </tr>,
                  ...(selected ? [<TaskRows key={`${dag.dag_id}-tasks`} dag={dag} run={latest} refreshSeconds={refreshSeconds} configured />] : []),
                ];
              }),
            ];
          })}
        </tbody>
      </table>
    </div>
  );
}

function RunHistoryTable({ dag, runs, refreshSeconds }: { dag?: DashboardDag; runs: AirflowDagRun[]; refreshSeconds: number }) {
  const [expandedRunId, setExpandedRunId] = useState<string>();
  if (!dag || !runs.length) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="실행 이력이 없습니다." />;
  }
  return (
    <div className="airflow-job-table-wrap">
      <table className="airflow-job-table">
        {/* 실행 ID(dag_run_id)는 사람이 쓸 일이 없고 문자열이 길어 표를 가로로 밀어냈다. */}
        <thead><tr><th>작업명</th><th>실행 유형</th><th>실행 일시</th><th>최종 결과</th><th>적재/실패</th><th>소요</th><th>재시도/재시작</th></tr></thead>
        <tbody>
          {runs.flatMap((run) => {
            const expanded = expandedRunId === run.dag_run_id;
            return [
            <tr key={run.dag_run_id} className={`airflow-dag-row${expanded ? " selected" : ""}`} onClick={() => setExpandedRunId(expanded ? undefined : run.dag_run_id)}>
              <td><button type="button">{expanded ? <DownOutlined /> : <RightOutlined />} DAG 실행</button></td>
              <td>{run.run_type === "scheduled" ? "스케줄" : "수동 실행"}</td>
              <td>{runAt(run) ? dayjs(runAt(run)).format("YYYY-MM-DD HH:mm:ss") : "-"}</td>
              <td><Tag color={stateColor(run.state)}>{stateLabel(run.state)}</Tag></td>
              <td>-</td>
              <td>{duration(run.start_date, run.end_date)}</td>
              <td>{expanded ? <DownOutlined /> : <RightOutlined />}</td>
            </tr>,
            ...(expanded ? [<TaskRows key={`${run.dag_run_id}-tasks`} dag={dag} run={run} refreshSeconds={refreshSeconds} auxColumn={false} />] : []),
          ];})}
        </tbody>
      </table>
    </div>
  );
}

/**
 * 실행 이력 창. 기본은 오늘 하루만 본다.
 *
 * 예전에는 가운데 탭에서 전체 이력을 늘 펼쳐 보여줬는데, 며칠치가 섞여 "오늘 뭐가
 * 돌았나"를 세기 어려웠다. 기본을 당일로 두고 기간은 직접 넓히게 한다.
 */
function RunHistoryModal({
  open,
  dag,
  runs,
  refreshSeconds,
  onClose,
}: {
  open: boolean;
  dag?: DashboardDag;
  runs: AirflowDagRun[];
  refreshSeconds: number;
  onClose: () => void;
}) {
  const [range, setRange] = useState<[Dayjs, Dayjs]>([dayjs().startOf("day"), dayjs().endOf("day")]);
  useEffect(() => {
    if (open) {
      setRange([dayjs().startOf("day"), dayjs().endOf("day")]);
    }
  }, [open, dag?.dag_id]);

  const visible = runs.filter((run) => {
    const at = runAt(run);
    if (!at) return false;
    const moment = dayjs(at);
    return !moment.isBefore(range[0]) && !moment.isAfter(range[1]);
  });

  return (
    <Modal title={`${dag ? displayName(dag) : "작업"} 실행 이력`} open={open} onCancel={onClose}
           footer={null} width={1080} destroyOnHidden rootClassName="cerebro-modal">
      <Space style={{ marginBottom: 12 }} wrap>
        <span>기간</span>
        <DatePicker.RangePicker
          value={range}
          allowClear={false}
          onChange={(value) => {
            if (value?.[0] && value?.[1]) {
              setRange([value[0].startOf("day"), value[1].endOf("day")]);
            }
          }}
        />
        <Button size="small" onClick={() => setRange([dayjs().startOf("day"), dayjs().endOf("day")])}>오늘</Button>
        <Button size="small" onClick={() => setRange([dayjs().subtract(6, "day").startOf("day"), dayjs().endOf("day")])}>최근 7일</Button>
        <Button size="small" onClick={() => setRange([dayjs().subtract(29, "day").startOf("day"), dayjs().endOf("day")])}>최근 30일</Button>
        <span style={{ color: "#888" }}>{visible.length}건</span>
      </Space>
      <RunHistoryTable dag={dag} runs={visible} refreshSeconds={refreshSeconds} />
    </Modal>
  );
}

/** 이 대상을 감시하는 알림 규칙 목록. 예전의 «이상 감지 설정»을 대신한다. */
/** 알림 규칙 심각도 - 설정 화면 «알림 규칙»과 같은 한글 이름(2026-10-12 요청: 영어 CRITICAL · WARNING · INFO 대신). */
const RULE_SEVERITY: Record<string, { label: string; tone: string }> = {
  CRITICAL: { label: "위험", tone: "critical" },
  WARNING: { label: "경고", tone: "warning" },
  INFO: { label: "정보", tone: "info" },
};

function WatchingRules({ target, ids }: { target: "CDC" | "ETL"; ids: Array<number | string> }) {
  const query = useQuery({
    queryKey: ["alert-rules-watching", target, ids.join(",")],
    queryFn: () => getAlertRulesWatching(target, ids),
    enabled: ids.length > 0,
  });
  const rules = query.data ?? [];
  return (
    <section>
      <strong>알림 규칙 {rules.length}건</strong>
      {query.isLoading ? <Spin size="small" /> : rules.length === 0 ? (
        <p>이 작업을 감시 중인 규칙이 없습니다.</p>
      ) : (
        <div className="airflow-alert-list">
          {rules.map((rule) => {
            const severity = RULE_SEVERITY[rule.severity] ?? { label: rule.severity, tone: "info" };
            // 규칙 이름은 유형 이름과 다를 때만 적는다 - 기본 이름이 유형과 같아 «ETL Job 실패 ETL Job 실패»로 두 번 보였다(2026-10-12 요청).
            const extra = [
              rule.name && rule.name !== rule.type_label ? rule.name : null,
              rule.schedule_enabled && rule.schedule_time ? `매일 ${rule.schedule_time.slice(0, 5)}` : null,
            ].filter(Boolean).join(" · ");
            return (
              <article key={rule.id}>
                {/* 백엔드가 «사용 중 + 이 대상을 감시»하는 규칙만 준다. 전부 사용 중이므로
                    «사용/미사용» 문구는 더 이상 정보가 아니다. */}
                <div>
                  <Tag color="blue">{rule.type_label}</Tag>
                  <span className={`airflow-alert-severity is-${severity.tone}`}>{severity.label}</span>
                </div>
                {extra && <p>{extra}</p>}
                {rule.last_eval_error && <small style={{ color: "#dc2626" }}>{rule.last_eval_error}</small>}
              </article>
            );
          })}
        </div>
      )}
      <Link to="/settings?tab=rules">알림 규칙 관리 바로가기</Link>
    </section>
  );
}

/**
 * 오른쪽 속성창.
 *
 * <p>기본정보 + 바로가기 + 실행 설정/실행 이력 버튼 + 이 작업을 감시하는 알림 규칙.
 * 스케줄 현황과 «이상 감지 설정»은 뺐다 - 스케줄은 실행 설정에서 다루고, 이상 감지는
 * 알림 규칙과 판정 기준이 두 벌이 되어 어느 쪽이 실제로 울리는지 알 수 없었다.
 */
function PropertyPanel({
  dag,
  etlJobIds,
  cdcMembers,
  inline = false,
  onOpenExecution,
  onOpenHistory,
}: {
  dag?: DashboardDag;
  etlJobIds?: number[];
  /** CDC 워크플로우일 때 소속 파이프라인들. 비어 있지 않으면 «어느 것이 실패했는지» 칸을 그린다. */
  cdcMembers?: CdcMember[];
  inline?: boolean;
  onOpenExecution?: () => void;
  onOpenHistory?: () => void;
}) {
  // 실행 설정(트리거·스케줄 변경)은 Airflow 쓰기 권한이 있어야 한다.
  const { can, permissionsLoaded } = useAuth();
  const canRun = !permissionsLoaded || can("AIRFLOW", "WRITE");
  // 일시 중지 해제. 성공하면 대시보드(카탈로그 · DAG 목록)를 다시 받아 상태 · 다음 실행이 바로 반영되게 한다.
  const queryClient = useQueryClient();
  const unpause = useMutation({
    mutationFn: () => setAirflowDagPaused(dag!.dag_id, false),
    onSuccess: async () => {
      message.success("일시 중지를 해제했습니다. 밀린 회차가 곧 시작됩니다.");
      await queryClient.invalidateQueries({ queryKey: ["airflow-dashboard"] });
    },
    onError: (error: unknown) =>
      message.error(`해제하지 못했습니다: ${error instanceof Error ? error.message : String(error)}`),
  });
  if (!dag) {
    return <aside className="airflow-dashboard-panel airflow-property-panel"><Empty description="작업을 선택하세요." /></aside>;
  }
  const category = categoryOf(dag);
  return (
    <aside className={`airflow-dashboard-panel airflow-property-panel${inline ? " airflow-property-panel--inline" : ""}`}>
      {!inline && <section>
        <strong>기본정보</strong>
        <dl>
          <dt>DAG ID</dt><dd>{dag.dag_id}</dd>
          <dt>업무 구분</dt><dd>{isCdcWorkflowDag(dag) ? "CDC 워크플로우" : category}</dd>
          <dt>업무 폴더</dt><dd>{folderName(dag)}</dd>
          <dt>스케줄</dt><dd title={dag.timetable_summary}>{scheduleDescription(dag.timetable_summary)}</dd>
          <dt>상태</dt>
          <dd>
            {dag.is_paused ? "일시 중지" : dag.is_active === false ? "비활성" : "활성"}
            {/* 일시 중지된 스케줄은 화면에서 바로 풀 수 있게 한다(2026-09-27 요청).
                푸는 순간 밀린 회차가 한 번 돌기 때문에, 무엇이 돌지 적어 두고 확인을 받는다. */}
            {dag.is_paused && canRun && (
              <Popconfirm
                title="일시 중지를 해제할까요?"
                description={
                  <span style={{ display: "inline-block", maxWidth: 280 }}>
                    해제하면 <b>밀린 마지막 회차가 곧바로 한 번</b> 실행됩니다
                    {dag.timetable_summary && dag.timetable_summary !== "None"
                      ? ` (스케줄 ${scheduleDescription(dag.timetable_summary)}).`
                      : "."}
                    {" "}그 뒤로는 정상 스케줄로 돕니다.
                  </span>
                }
                okText="해제"
                cancelText="취소"
                onConfirm={() => unpause.mutate()}
              >
                <Button size="small" type="link" loading={unpause.isPending} style={{ paddingLeft: 8 }}>
                  해제
                </Button>
              </Popconfirm>
            )}
          </dd>
          <dt>설명</dt><dd>{dag.description || "-"}</dd>
        </dl>
        <div className="airflow-schedule-actions">
          <Button type="primary" disabled={!canRun} onClick={onOpenExecution}
            title={canRun ? undefined : "Airflow 쓰기 권한이 없습니다"}>실행 설정</Button>
          <Button onClick={onOpenHistory}>실행 이력</Button>
        </div>
      </section>}
      {!inline && isCdcWorkflowDag(dag) && (
        <section>
          {/* 워크플로우 Run 은 «쏘고 끝»이라 초록으로 남는다. 실제 상태는 여기서 본다. */}
          <strong>CDC 파이프라인 ({cdcMembers?.length ?? 0})</strong>
          <div style={{ margin: "6px 0" }}>
            실행 상태 <Tag color={cdcAggregate(cdcMembers ?? []).color}>{cdcAggregate(cdcMembers ?? []).label}</Tag>
            <Typography.Text type="secondary" style={{ fontSize: 12, display: "block", marginTop: 4 }}>
              워크플로우는 지시를 보낼 뿐입니다. 아래는 각 파이프라인의 실제 상태입니다.
            </Typography.Text>
          </div>
          {/* 이름 · 상태 두 칸. 상태 칸 시작을 모든 줄에서 맞춘다(2026-10-11 요청 - 상태 글이 짧은 줄은 오른쪽으로 밀려 사이가 벌어졌다). */}
          <div className="airflow-cdc-members">
            {(cdcMembers ?? []).map((member) => (
              <div key={member.pipelineId} className="airflow-cdc-member">
                <Link to={`/cdc/pipelines?pipelineId=${member.pipelineId}`}>
                  {member.pipeline?.name ?? `파이프라인 ${member.pipelineId}`}
                </Link>
                <div>
                  {member.pipeline
                    ? renderRuntimeStatus(member.pipeline, member.runtime)
                    : <Tag color="error">삭제됨</Tag>}
                </div>
              </div>
            ))}
          </div>
        </section>
      )}
      {!inline && category === "ETL" && !isCdcWorkflowDag(dag) && <WatchingRules target="ETL" ids={etlJobIds ?? []} />}
      {/* CDC 워크플로우는 소속 파이프라인을 감시하는 규칙을 보여 준다 - 예전 CDC 가지에서 파이프라인을 고르면 보이던 것(2026-10-10). */}
      {!inline && isCdcWorkflowDag(dag) && (cdcMembers ?? []).length > 0
        && <WatchingRules target="CDC" ids={(cdcMembers ?? []).map((member) => member.pipelineId)} />}
      {!inline && <section><strong>바로가기</strong><div className="airflow-resource-links"><Link to={`/airflow/manage?dagId=${encodeURIComponent(dag.dag_id)}`}>상세보기</Link>{category === "ETL" && !isCdcWorkflowDag(dag) ? <Link to={dag.nifi_process_group_id ? `/etl/manage?processGroupId=${encodeURIComponent(dag.nifi_process_group_id)}` : "/etl/manage"}>ETL 캔버스</Link> : <Link to="/cdc/pipelines">CDC 파이프라인</Link>}</div></section>}
    </aside>
  );
}

type CdcNodeChoice = ExecutionAction | "none";

/** CDC 노드 한 줄에 고를 수 있는 지시. «유지»는 이번 실행에서 그 노드를 건너뛴다(지금 상태 그대로). */
const CDC_NODE_CHOICES: Array<{ label: string; value: CdcNodeChoice }> = [
  { label: "유지", value: "none" },
  { label: "시작", value: "start" },
  { label: "중지", value: "stop" },
  { label: "재시작", value: "restart" },
  { label: "배포", value: "deploy" },
];

function ExecutionSettingsModal({
  open,
  dag,
  runs,
  workflows,
  pipelines,
  runtimeByPipeline,
  onClose,
  onExecuted,
}: {
  open: boolean;
  dag?: DashboardDag;
  runs: AirflowDagRun[];
  workflows: WorkflowSummary[];
  pipelines: PipelineResponse[];
  runtimeByPipeline: Map<number, PipelineRuntimeStatusResponse>;
  onClose: () => void;
  onExecuted: () => void;
}) {
  const [action, setAction] = useState<ExecutionAction | undefined>("start");
  const [nodeChoices, setNodeChoices] = useState<Record<string, CdcNodeChoice>>({});
  const queryClient = useQueryClient();
  const [triggering, setTriggering] = useState(false);
  // 표 필터(2026-10-02 요청) - 파이프라인 이름 · 현재 상태. «전체 적용»은 걸러진(보이는) 줄에만 적용한다.
  const [nameFilter, setNameFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState<string | undefined>();
  void runs;

  const cdcWorkflow = dag && isCdcWorkflowDag(dag) ? workflows.find((w) => w.dagId === dag.dag_id) : undefined;
  const cdcNodes = cdcWorkflow?.cdcNodes ?? [];
  const pipelineName = (id?: number | null) => pipelines.find((p) => p.id === id)?.name ?? (id ? `파이프라인 ${id}` : "(없음)");
  const statusOf = (id?: number | null) =>
    (id != null ? runtimeByPipeline.get(id)?.runtimeStatus : undefined)
    ?? pipelines.find((p) => p.id === id)?.status ?? "-";
  const statusOptions = [...new Set(cdcNodes.map((node) => statusOf(node.pipelineId)))].sort()
    .map((status) => ({ label: status, value: status }));
  const visibleNodes = cdcNodes.filter((node) =>
    (!nameFilter.trim() || pipelineName(node.pipelineId).toLowerCase().includes(nameFilter.trim().toLowerCase()))
    && (!statusFilter || statusOf(node.pipelineId) === statusFilter));

  useEffect(() => {
    if (!open) return;
    setAction("start");
    setNameFilter("");
    setStatusFilter(undefined);
    // CDC 워크플로우: 전부 «시작»으로 연다. 노드마다 바꿀 수 있다(예전 CDC 가지에서 파이프라인을 골라 들어오던 길은 2026-10-10 에 없앴다).
    setNodeChoices(Object.fromEntries(cdcNodes.map((n) => [n.nodeKey, "start"] as const)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, dag?.dag_id]);

  const options = [
    { label: "시작 (Start)", value: "start" },
    { label: "중지 (Stop)", value: "stop" },
  ];
  const chosen = Object.entries(nodeChoices).filter(([, v]) => v !== "none");

  const execute = async () => {
    if (!dag) return;
    setTriggering(true);
    try {
      if (cdcWorkflow) {
        if (chosen.length === 0) {
          message.warning("지시할 파이프라인을 하나 이상 고르세요.");
          return;
        }
        // 노드별 지시. 여기에 없는 노드는 이번 실행에서 건너뛴다(airflow/dags/cdc_runtime.py).
        await triggerAirflowDag(dag.dag_id, { node_actions: Object.fromEntries(chosen) });
        message.success(`${displayName(dag)} - 파이프라인 ${chosen.length}개에 지시를 보냈습니다. 상태 확인까지 끝나야 성공으로 표시됩니다.`);
      } else {
        if (!action) return;
        await triggerAirflowDag(dag.dag_id, { action });
        message.success(`${displayName(dag)} ${options.find((option) => option.value === action)?.label} 요청을 전송했습니다.`);
      }
      // 로그 화면이 다음 주기를 기다리지 않고 방금 건 실행을 바로 받는다(2026-10-06).
      refreshRunsAfterTrigger(queryClient);
      onClose();
      onExecuted();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "DAG 실행 요청에 실패했습니다.");
    } finally {
      setTriggering(false);
    }
  };

  return (
    <Modal
      title={`${dag ? displayName(dag) : "DAG"} 실행 설정`}
      open={open}
      onCancel={onClose}
      footer={<>
        <Button type="primary" loading={triggering} disabled={cdcWorkflow ? chosen.length === 0 : !action}
                onClick={() => void execute()}>실행</Button>
        <Button disabled={triggering} onClick={onClose}>취소</Button>
      </>}
      destroyOnHidden
      rootClassName="cerebro-modal"
      width={cdcWorkflow ? 640 : undefined}
    >
      {cdcWorkflow ? (
        <>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8 }}>
            <Input size="small" allowClear placeholder="파이프라인 이름" style={{ width: 200 }}
                   value={nameFilter} onChange={(event) => setNameFilter(event.target.value)} />
            <Select size="small" allowClear placeholder="현재 상태" style={{ width: 140 }}
                    options={statusOptions} value={statusFilter} onChange={(value) => setStatusFilter(value)} />
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8 }}>
            <span style={{ fontSize: 12, color: "#888" }} title="필터로 보이는 파이프라인에만 적용합니다">전체 적용</span>
            {CDC_NODE_CHOICES.map((choice) => (
              <Button key={choice.value} size="small"
                      onClick={() => setNodeChoices((current) => ({
                        ...current,
                        ...Object.fromEntries(visibleNodes.map((n) => [n.nodeKey, choice.value] as const)),
                      }))}>
                {choice.label}
              </Button>
            ))}
          </div>
          <Table
            rowKey="nodeKey"
            size="small"
            pagination={false}
            dataSource={visibleNodes}
            locale={{ emptyText: cdcNodes.length ? "조건에 맞는 파이프라인이 없습니다" : "이 워크플로우에 CDC 노드가 없습니다" }}
            columns={[
              { title: "파이프라인", render: (_, node) => pipelineName(node.pipelineId) },
              { title: "현재 상태", width: 110, render: (_, node) => statusOf(node.pipelineId) },
              { title: "지시", width: 300, render: (_, node) => (
                <Radio.Group size="small" optionType="button" buttonStyle="solid"
                             value={nodeChoices[node.nodeKey] ?? "none"} options={CDC_NODE_CHOICES}
                             onChange={(event) => setNodeChoices((current) => ({ ...current, [node.nodeKey]: event.target.value }))} />
              ) },
            ]}
          />
        </>
      ) : (
        <>
          <p>Airflow DAG에 전달할 실행 동작을 선택하세요.</p>
          <Radio.Group
            block
            className="airflow-execution-actions"
            optionType="button"
            buttonStyle="solid"
            value={action}
            options={options}
            onChange={(event) => setAction(event.target.value as ExecutionAction)}
          />
        </>
      )}
    </Modal>
  );
}

export function AirflowDashboardPage() {
  const [searchParams] = useSearchParams();
  const initialDagId = searchParams.get("dagId") ?? undefined;
  const openInitialDetail = searchParams.get("detail") === "1";
  const [refreshSeconds, setRefreshSeconds] = useState(30);
  const [selectedDagId, setSelectedDagId] = useState<string | undefined>(initialDagId);
  const [search, setSearch] = useState("");
  // 처음 열면 무엇을 보여줄지. 마지막으로 고른 DAG가 덩그러니 뜨는 것보다 «ETL 전체 워크플로우»에서 시작한다
  // (예전에는 «CDC 전체» - CDC 가지는 2026-10-10 에 뺐다).
  const [scope, setScope] = useState<ListScope | undefined>(
    initialDagId ? { kind: "dag", dagId: initialDagId } : { kind: "category", category: "ETL" });
  // 옆 패널 기본 폭은 예전 격자 열과 같다(노트북 폭 220 · 280, 넓은 화면 230 · 320).
  const narrowScreen = typeof window !== "undefined" && window.innerWidth <= 1440;
  const treeDock = useSideDock("left", narrowScreen ? 220 : 230, 180, 520);
  const propertyDock = useSideDock("right", narrowScreen ? 280 : 320, 260, 720);
  // 실행 이력(하단 task 표)을 펼쳐 둔 DAG. 같은 DAG를 다시 누르면 접히고,
  // 다른 것을 고르면(그룹·지표·다른 DAG·CDC) 닫힌다 - 이전 선택의 이력이 남아 있으면
  // 지금 보고 있는 게 무엇인지 헷갈린다.
  const [expandedDagId, setExpandedDagId] = useState<string>();
  const [historyOpen, setHistoryOpen] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  // 상단 CDC · ETL 현황 카드 접기(로그2 화면과 같은 커튼, 2026-09-18 요청). 처음엔 접혀 있다.
  const [curtainOpen, setCurtainOpen] = useState(false);
  // 오른쪽 속성창 «상세정보 | AI 어시스턴트» 탭.
  const [propertyTab, setPropertyTab] = useState<"detail" | "ai">("detail");
  const gptEnabled = useGptEnabled();
  const [executionOpen, setExecutionOpen] = useState(false);
  const [synchronizing, setSynchronizing] = useState(false);
  /** 가운데 실행 목록에서 고른 실행 차수(EtlRunsPane). 속성창 AI 어시스턴트의 대상이다. */
  const [selectedRun, setSelectedRun] = useState<{ dagId: string; runId: string }>();
  // 이 화면의 «쓰기» 동작(카탈로그 동기화·실행 설정)은 Airflow 쓰기 권한이 있어야 한다.
  const { can: canTop, permissionsLoaded: permsLoadedTop } = useAuth();
  const canRunTop = !permsLoadedTop || canTop("AIRFLOW", "WRITE");
  // 진입 시 카탈로그 동기화는 POST 라 AIRFLOW 쓰기를 요구한다. 조회자가 화면을 열기만 해도
  // 403 이 나면서 «동기화 실패» 경고가 떴다. 백엔드에 30초 주기 자동 동기화
  // (AirflowDagCatalogSyncService)가 이미 돌고 있으므로, 쓰기 권한이 없으면 그냥 건너뛰고
  // DB 에 있는 현황을 보여준다 - 조회자는 «조회만» 되면 된다.
  const initialSyncQuery = useQuery({
    queryKey: ["airflow-dag-catalog-sync"],
    queryFn: syncAirflowDagCatalog,
    enabled: canRunTop,
    retry: 1,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  });
  // 진입 동기화를 기다리지 않고 바로 받는다(2026-10-11 요청 - 첫 화면이 «동기화 → 조회» 두 번을 차례로 기다렸다).
  // 백엔드가 30초마다 같은 동기화를 하므로 DB 카탈로그는 이미 거의 최신이다. 진입 동기화로 DAG 가 새로 잡히거나
  // 빠졌을 때만 아래에서 한 번 더 받는다.
  const dashboardQuery = useQuery({
    queryKey: ["airflow-dashboard"],
    queryFn: loadDashboard,
    refetchInterval: refreshSeconds * 1000,
  });
  const entrySyncApplied = useRef(false);
  useEffect(() => {
    const result = initialSyncQuery.data;
    // 진입 때 한 번만 본다. «동기화» 단추(synchronizeDags)는 끝나고 스스로 다시 받는다.
    if (!result || entrySyncApplied.current) return;
    entrySyncApplied.current = true;
    if (result.createdCount > 0 || result.disabledCount > 0) void dashboardQuery.refetch();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialSyncQuery.data]);
  /*
   * 대기 · 실행 중인 실행만(3초/5초, 2026-10-06 실행 목록에 넣은 것을 화면 전체로 올렸다 - 2026-10-11 요청).
   * 현황 카드 · 트리 · 실행 목록이 같은 조회를 쓴다(예전에는 실행 목록만 이걸 받고 카드는 30초 주기 조회로만 셌다 -
   * 실행을 걸면 목록엔 바로 뜨는데 카드 «실행 중»은 최대 30초 늦었다). 결과가 비면 100바이트 남짓이다.
   */
  const activeRunsQuery = useQuery({
    queryKey: ACTIVE_RUNS_KEY,
    queryFn: listActiveAirflowDagRuns,
    refetchInterval: (query) => (query.state.data?.length ? LIVE_REFRESH_MS : ACTIVE_RUNS_IDLE_MS),
  });
  // 실행이 끝나 «대기 · 실행 중»에서 빠지면 최근 실행을 한 번 더 받아 끝난 상태(성공 · 실패)를 바로 반영한다.
  const activeRunKey = (activeRunsQuery.data ?? []).map((run) => `${run.dag_id}|${run.dag_run_id}`).sort().join(",");
  const previousActiveRunKey = useRef<string | undefined>(undefined);
  const refetchDashboard = dashboardQuery.refetch;
  useEffect(() => {
    const before = previousActiveRunKey.current;
    previousActiveRunKey.current = activeRunKey;
    if (!before) return;
    const stillActive = new Set(activeRunKey ? activeRunKey.split(",") : []);
    if (before.split(",").some((key) => !stillActive.has(key))) void refetchDashboard();
  }, [activeRunKey, refetchDashboard]);
  // ETL 트리를 NiFi 그룹 계층으로 보여주기 위해(ETL 관리 화면과 동일한 구조)
  // 트리에 워크플로우를 그룹별로 붙이려면 dag_id -> 그룹 매핑이 필요하다.
  const workflowsQuery = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  // 알림 규칙은 etl_job.id를 감시 대상으로 갖는다. 선택한 워크플로우가 어떤 job을
  // 참조하는지 알아야 "이 작업을 감시하는 규칙"을 찾을 수 있다.
  const selectedWorkflowId = (workflowsQuery.data ?? [])
    .find((workflow) => workflow.dagId === selectedDagId)?.id;
  const workflowDetailQuery = useQuery({
    queryKey: ["workflow-detail", selectedWorkflowId],
    queryFn: () => getWorkflow(selectedWorkflowId!),
    enabled: Boolean(selectedWorkflowId),
  });
  // CDC 워크플로우의 소속 파이프라인 상태(속성창 · 실행 설정 창)는 CDC 관리 화면과 같은 원본을 읽는다.
  const pipelinesQuery = useQuery({ queryKey: ["pipelines"], queryFn: listPipelines });
  const runtimeQuery = useQuery({
    queryKey: ["pipeline-runtime-statuses"],
    queryFn: listPipelineRuntimeStatuses,
    refetchInterval: refreshSeconds * 1000,
  });
  const runsByDag = useMemo(
    () => groupRuns(overlayActiveRuns(dashboardQuery.data?.runs ?? [], activeRunsQuery.data)),
    [dashboardQuery.data?.runs, activeRunsQuery.data],
  );
  const businessDags = useMemo(
    () => (dashboardQuery.data?.dags ?? []).filter((dag) => categoryOf(dag) !== "기타"),
    [dashboardQuery.data?.dags],
  );
  // 실행 중인 워크플로우의 후행(«대기» 카드 · 목록이 같이 쓴다).
  const waitingDagIds = useMemo(
    () => computeWaitingDagIds(workflowsQuery.data ?? [], runsByDag),
    [workflowsQuery.data, runsByDag],
  );
  const filteredDags = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return businessDags.filter((dag) => {
      return !needle || `${displayName(dag)} ${dag.dag_id} ${folderName(dag)}`.toLowerCase().includes(needle);
    });
  }, [businessDags, search]);

  useEffect(() => {
    if (initialDagId && filteredDags.some((dag) => dag.dag_id === initialDagId)) {
      setSelectedDagId(initialDagId);
      return;
    }
    if (!filteredDags.some((dag) => dag.dag_id === selectedDagId)) {
      setSelectedDagId(filteredDags[0]?.dag_id);
    }
  }, [filteredDags, initialDagId, selectedDagId]);

  useEffect(() => {
    if (openInitialDetail && selectedDagId === initialDagId && selectedDagId) {
      setDetailOpen(true);
    }
  }, [initialDagId, openInitialDetail, selectedDagId]);

  useEffect(() => {
    if (canRunTop && initialSyncQuery.isError) {
      message.warning("DAG 자동 동기화에 실패해 기존 DB 정보로 대시보드를 표시합니다.");
    }
  }, [initialSyncQuery.isError]);

  const pipelines = pipelinesQuery.data ?? [];
  const runtimeByPipeline = new Map((runtimeQuery.data ?? []).map((r) => [r.pipelineId, r] as const));

  // ETL 그룹 -> 그 그룹(하위 포함)에 속한 워크플로우 DAG id.
  /** DAG를 고른다. 같은 DAG를 다시 고르면 이력이 접힌다. */
  const selectDag = (dagId: string) => {
    setSelectedDagId(dagId);
    setExpandedDagId((current) => (current === dagId ? undefined : dagId));
  };

  /** 트리·지표에서 목록 범위를 바꾼다. DAG를 고른 게 아니면 열려 있던 이력을 닫는다. */
  const changeScope = (next: ListScope | undefined) => {
    setScope(next);
    if (next?.kind !== "dag") {
      setExpandedDagId(undefined);
    }
  };

  const selectedDag = businessDags.find((dag) => dag.dag_id === selectedDagId);

  /** 가운데 목록의 제목. 지금 무엇을 보고 있는지 한 줄로 말해준다. */
  const scopeTitle = !scope
    ? (selectedDag ? displayName(selectedDag) : "선택 작업")
    : scope.kind === "dag" ? (selectedDag ? displayName(selectedDag) : scope.dagId)
      : scope.kind === "category" ? "ETL 전체 워크플로우"
      : `${scope.category} · ${STATUS_METRIC_LABEL[scope.metric]}`;

  /** 선택한 ETL 워크플로우가 참조하는 job id들. 감시 중인 알림 규칙을 찾는 열쇠다. */
  const etlJobIdsOfSelected = selectedDag
    ? (workflowDetailQuery.data?.nodes ?? [])
        .map((node) => node.jobId)
        .filter((id): id is number => typeof id === "number")
    : [];
  /**
   * 선택한 것이 CDC 워크플로우면 소속 파이프라인들의 실제 상태를 모은다.
   * 워크플로우 Run 은 «지시를 보냈다»로 끝나므로, 하나가 실패했는지는 여기서만 드러난다.
   */
  const cdcMembersOfSelected: CdcMember[] = selectedDag && isCdcWorkflowDag(selectedDag)
    ? (workflowDetailQuery.data?.nodes ?? [])
        .filter((node) => node.nodeType === "CDC" && typeof node.pipelineId === "number")
        .map((node) => {
          const pipelineId = node.pipelineId as number;
          return {
            pipelineId,
            pipeline: (pipelinesQuery.data ?? []).find((item) => item.id === pipelineId),
            runtime: runtimeByPipeline.get(pipelineId),
          };
        })
    : [];
  const selectedRuns = selectedDagId ? (runsByDag.get(selectedDagId) ?? []) : [];
  // 목록과 같은 기준으로 맞춘다. 목록이 «실행 없음»인데 하단에 어제 실행의 단계가
  // 펼쳐지면 둘이 어긋나 보인다.
  const selectedRunToday = latestRunToday(selectedRuns);
  const historyDagId = selectedDagId;
  const historyRuns = historyDagId ? (runsByDag.get(historyDagId) ?? []) : [];
  const showExecutionSettings = (dag: DashboardDag) => {
    setSelectedDagId(dag.dag_id);
    setExecutionOpen(true);
  };
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
      await dashboardQuery.refetch();
    } finally {
      setSynchronizing(false);
    }
  };
  const showDagDetail = (dag: DashboardDag) => {
    setSelectedDagId(dag.dag_id);
    setDetailOpen(true);
  };
  // 진입 동기화는 기다리지 않는다(위 dashboardQuery 설명). 조회만 끝나면 화면을 그린다.
  const initialLoading = dashboardQuery.isLoading;

  const syncBusy = synchronizing || initialSyncQuery.isFetching;
  // 가운데 목록이 CDC(파이프라인)인지 ETL(워크플로우)인지. 제목·선택 행의 색만 가른다(초안: CDC 파랑 · ETL 빨강).
  /** 트리 · 지표에서 고른 범위가 ETL 이면 가운데를 실행 목록(EtlRunsPane)으로 그린다. CDC 범위는 예전 파이프라인 표. */
  const etlRunsScope = (() => {
    const today = dayjs().startOf("day");
    if (scope?.kind === "dag") {
      const dag = businessDags.find((row) => row.dag_id === scope.dagId);
      if (!dag || categoryOf(dag) !== "ETL") return undefined;
      const workflow = (workflowsQuery.data ?? []).find((row) => row.dagId === scope.dagId);
      return { dagIds: [scope.dagId], includesChildren: Boolean(workflow?.childWorkflowIds.length) };
    }
    if (scope?.kind === "category" && scope.category === "ETL") {
      // ETL 가지 전체. 하위 워크플로우도 범위 안에 있으니 부른 job 아래로 접힌다(머리줄에 «하위 포함»을 적는다).
      return {
        dagIds: businessDags.filter((dag) => categoryOf(dag) === "ETL").map((dag) => dag.dag_id),
        includesChildren: true,
      };
    }
    if (scope?.kind === "metric" && scope.category === "ETL") {
      return {
        dagIds: businessDags
          .filter((dag) => categoryOf(dag) === "ETL" && matchesMetric(scope.metric, dag, runsByDag.get(dag.dag_id) ?? [], today, waitingDagIds))
          .map((dag) => dag.dag_id),
        includesChildren: false,
      };
    }
    return undefined;
  })();

  /** 상단 커튼을 접었을 때의 한 줄 요약 숫자(카드와 같은 판정). */
  const metricCounts = (category: BusinessCategory) => {
    const today = dayjs().startOf("day");
    const counts = { running: 0, success: 0, failed: 0, waiting: 0 };
    businessDags.filter((dag) => categoryOf(dag) === category).forEach((dag) => {
      const runs = runsByDag.get(dag.dag_id) ?? [];
      if (matchesMetric("running", dag, runs, today)) counts.running += 1;
      if (matchesMetric("success", dag, runs, today)) counts.success += 1;
      if (matchesMetric("failed", dag, runs, today)) counts.failed += 1;
      if (matchesMetric("waiting", dag, runs, today, waitingDagIds)) counts.waiting += 1;
    });
    return counts;
  };

  /**
   * AI 어시스턴트 대상: 가운데 목록에서 고른 실행 차수 한 건(2026-10-11 요청 - 워크플로우 설명이 아니라 그 회차 기준으로 답한다).
   * 서버가 그 차수의 노드별 결과 · 적재 건수 · 실패 로그 · 직전 실행을 모은다(WorkflowRunContextCollector).
   * 기간 안에 실행이 없어 고른 차수가 없으면 예전처럼 워크플로우 전체를 대상으로 한다.
   * 차수마다 대화를 새로 연다(key) - 다른 차수를 고르면 그 차수 요약부터 다시 나온다.
   */
  const aiTarget: { key: string; targetType: "WORKFLOW" | "WORKFLOW_RUN"; target: Record<string, string> } | undefined = !gptEnabled ? undefined
    : selectedRun && selectedRun.dagId === selectedDagId
      ? { key: `run:${selectedRun.dagId}|${selectedRun.runId}`, targetType: "WORKFLOW_RUN" as const,
          target: { dagId: selectedRun.dagId, runId: selectedRun.runId } }
    : selectedWorkflowId
      ? { key: `wf:${selectedWorkflowId}`, targetType: "WORKFLOW" as const, target: { workflowId: String(selectedWorkflowId) } }
      : undefined;

  return (
    <ConfigProvider theme={cerebroBrandTheme}>
    <div className="airflow-dashboard-page">
      {/* 동기화·갱신 주기는 아래 현황 줄(커튼 손잡이) 오른쪽으로 옮겼다(2026-09-18 요청).
          actions 를 주지 않으면 이 머리줄은 아무것도 그리지 않는다 - 별도의 조작 띠가 사라진다. */}
      <WorkflowPageHeader
        title="워크플로우 로그"
        icon={<WfIcon name="pulse" size={22} strokeWidth={2.2} />}
      />

      {initialLoading ? <div className="airflow-dashboard-loading"><Spin size="large" /><span>대시보드 로딩 중...</span></div> : dashboardQuery.isError ? <Empty description="Airflow 현황을 불러올 수 없습니다." /> : (
        <>
          <section className={`lv2-curtain airflow-status-curtain ${curtainOpen ? "open" : "closed"}`}>
          <div className="lv2-curtain-body" aria-hidden={!curtainOpen}>
          <div className="airflow-business-status">
            {/* CDC · ETL 구분 없이 한 장(2026-10-02). 워크플로우 DAG 는 전부 ETL 분류에 있다(CDC 워크플로우 포함). */}
            <StatusCard
              dags={businessDags.filter((dag) => categoryOf(dag) === "ETL")}
              runsByDag={runsByDag}
              waitingDagIds={waitingDagIds}
              activeMetric={scope?.kind === "metric" ? scope.metric : undefined}
              onMetricClick={(metric) => changeScope(
                scope?.kind === "metric" && scope.metric === metric
                  ? { kind: "category", category: "ETL" }
                  : { kind: "metric", category: "ETL", metric })}
            />
          </div>
          </div>
          {/* 현황 줄: 왼쪽 손잡이(접힘일 때 요약) · 오른쪽 동기화·갱신 주기.
              펼쳐도 이 줄은 그대로 있으므로 조작이 화면에서 사라지지 않는다. */}
          <div className="lv2-curtain-bar">
          <button type="button" className="lv2-curtain-handle" aria-expanded={curtainOpen} onClick={() => setCurtainOpen((open) => !open)}>
            {!curtainOpen && (
              <span className="lv2-mini">
                {(() => {
                  const counts = metricCounts("ETL");
                  return (
                    <>
                      <span className="lv2-mini-item">실행 중 <em>{counts.running}</em></span><span className="dotsep">·</span>
                      <span className="lv2-mini-item">성공 <em>{counts.success}</em></span><span className="dotsep">·</span>
                      <span className="lv2-mini-item">실패 {counts.failed > 0 ? <b className="bad">{counts.failed}</b> : <em>0</em>}</span><span className="dotsep">·</span>
                      <span className="lv2-mini-item">대기 <em>{counts.waiting}</em></span>
                    </>
                  );
                })()}
              </span>
            )}
            <span className="lv2-handle-label">{curtainOpen ? "접기 ▴" : "현황 펼치기 ▾"}</span>
          </button>
          {/* 동기화는 아래 실행 목록의 «기간» 왼쪽으로 옮겼다(2026-09-18 요청). 여기는 갱신 주기만 남는다. */}
          <div className="airflow-refresh-controls">
            <span className="refresh-label">갱신 주기</span>
            <Select value={refreshSeconds} options={REFRESH_OPTIONS} onChange={setRefreshSeconds} aria-label="갱신 주기" />
            <span className="last-refresh-time">
              마지막 갱신 {dashboardQuery.dataUpdatedAt ? dayjs(dashboardQuery.dataUpdatedAt).format("HH:mm:ss") : "-"}
            </span>
          </div>
          </div>
          </section>
          {/* 양옆 패널은 ETL 관리 화면처럼 접고 너비를 끌어 바꾼다(2026-09-18 요청). 가운데 열이 남은 폭을 가져간다. */}
          <div className="airflow-dashboard-workspace"
               style={{ gridTemplateColumns: `${treeDock.column} minmax(0, 1fr) ${propertyDock.column}` }}>
            <SideDock side="left" dock={treeDock} label="트리 패널">
            <BusinessTree dags={filteredDags} workflows={workflowsQuery.data ?? []}
                          scope={scope} onScope={changeScope}
                          selectedDagId={selectedDagId} search={search} onSearch={setSearch}
                          onSelect={selectDag} onDetail={showDagDetail} onExecution={showExecutionSettings}
                          onRefresh={() => { void dashboardQuery.refetch(); void workflowsQuery.refetch(); }}
                          refreshing={dashboardQuery.isFetching || workflowsQuery.isFetching} />
            </SideDock>
            {etlRunsScope ? (
              // ETL 범위(워크플로우 선택 · ETL 지표): 로그2 식 실행 목록 + 실행 이력(2026-09-18 요청).
              <main className="airflow-dashboard-panel airflow-jobs-panel airflow-jobs-panel--etl airflow-jobs-panel--runs">
                <EtlRunsPane
                  title={scopeTitle}
                  cdcWorkflow={etlRunsScope.dagIds.length === 1 && etlRunsScope.dagIds.every((id) => {
                    const dag = businessDags.find((item) => item.dag_id === id);
                    return Boolean(dag && isCdcWorkflowDag(dag));
                  })}
                  includesChildren={etlRunsScope.includesChildren}
                  scopeDagIds={etlRunsScope.dagIds}
                  dags={businessDags}
                  workflows={workflowsQuery.data ?? []}
                  dashboardRuns={dashboardQuery.data?.runs ?? []}
                  activeRuns={activeRunsQuery.data}
                  refreshSeconds={refreshSeconds}
                  onSelectDag={(dagId) => setSelectedDagId(dagId)}
                  onSelectRun={setSelectedRun}
                  sync={canRunTop ? { busy: syncBusy, onSync: () => { void synchronizeDags(); } } : undefined}
                />
              </main>
            ) : (
            <main className="airflow-dashboard-panel airflow-jobs-panel airflow-jobs-panel--etl">
              <div className="airflow-jobs-heading">
                <h3>
                  <WfIcon name="filter" size={16} />
                  {scopeTitle}
                </h3>
              </div>
              {scope
                ? <ScopeList scope={scope} dags={businessDags} runsByDag={runsByDag} waitingDagIds={waitingDagIds}
                             selectedDagId={selectedDagId}
                             onSelectDag={selectDag} />
                : <JobsTable dags={selectedDag ? [selectedDag] : []} runsByDag={runsByDag} selectedDagId={selectedDagId} refreshSeconds={refreshSeconds} onSelect={setSelectedDagId} />}
              {/* ETL은 각 task(job)이 중요하다. DAG를 고르면 최근 실행의 task를 바로 펼친다. */}
              {selectedDag && expandedDagId === selectedDag.dag_id && categoryOf(selectedDag) === "ETL" && (
                <div className="airflow-job-table-wrap" style={{ marginTop: 12 }}>
                  <table className="airflow-job-table">
                    <thead><tr><th>작업(Task)</th><th>유형</th><th>시작</th><th>상태</th><th>적재/실패</th><th>소요</th><th>스케줄</th><th>재실행</th></tr></thead>
                    <tbody>
                      <TaskRows dag={selectedDag} run={selectedRunToday} refreshSeconds={refreshSeconds} />
                    </tbody>
                  </table>
                </div>
              )}
            </main>
            )}
            {/* 오른쪽 속성창: 워크플로우 설계 화면과 같은 «상세정보 | AI 어시스턴트» 탭(2026-09-18 요청). 상세정보는 예전 속성창 그대로. */}
            <SideDock side="right" dock={propertyDock} label="속성 패널">
            <div className={`airflow-property-shell${aiTarget && propertyTab === "ai" ? " is-ai" : ""}`}>
              {/* 탭 위 이름 줄. 워크플로우 설계 화면 속성창과 같은 자리다(2026-09-18 요청).
                  아래 «속성 / 이름» 이 하던 역할을 여기로 올렸으므로 그쪽에서는 지웠다 - 같은 이름을 두 번 적지 않는다. */}
              <div className="nifi-detail-header">
                <div className="nifi-detail-title">
                  {selectedDag ? `워크플로우: ${displayName(selectedDag)}` : "속성"}
                </div>
              </div>
              <div className="nifi-detail-tabs" role="tablist" aria-label="속성 패널 탭">
                <button type="button" role="tab" aria-selected={!(aiTarget && propertyTab === "ai")}
                        className={`nifi-detail-tab${aiTarget && propertyTab === "ai" ? "" : " active"}`}
                        onClick={() => setPropertyTab("detail")}>
                  상세정보
                </button>
                {aiTarget ? (
                  <button type="button" role="tab" aria-selected={propertyTab === "ai"}
                          className={`nifi-detail-tab${propertyTab === "ai" ? " active" : ""}`}
                          onClick={() => setPropertyTab("ai")}>
                    AI 어시스턴트
                  </button>
                ) : null}
              </div>
              {aiTarget && propertyTab === "ai" ? (
                <div className="airflow-property-ai">
                  <GptChatPanel key={aiTarget.key} targetType={aiTarget.targetType} target={aiTarget.target} />
                </div>
              ) : (
                <PropertyPanel dag={selectedDag}
                               etlJobIds={etlJobIdsOfSelected}
                               cdcMembers={cdcMembersOfSelected}
                               onOpenExecution={() => setExecutionOpen(true)}
                               onOpenHistory={() => setHistoryOpen(true)} />
              )}
            </div>
            </SideDock>
          </div>
          <ExecutionSettingsModal open={executionOpen} dag={selectedDag}
                                  runs={selectedDag ? runsByDag.get(selectedDag.dag_id) ?? [] : []}
                                  workflows={workflowsQuery.data ?? []} pipelines={pipelines}
                                  runtimeByPipeline={runtimeByPipeline}
                                  onClose={() => setExecutionOpen(false)} onExecuted={() => { setHistoryOpen(true); void dashboardQuery.refetch(); }} />
          <RunHistoryModal open={historyOpen} dag={selectedDag}
                           runs={historyRuns} refreshSeconds={refreshSeconds} onClose={() => setHistoryOpen(false)} />
          <Modal title="DAG 상세" open={detailOpen} footer={null} onCancel={() => setDetailOpen(false)} rootClassName="cerebro-modal">
            {selectedDag && <Descriptions column={1} size="small" bordered>
              <Descriptions.Item label="작업명">{displayName(selectedDag)}</Descriptions.Item>
              <Descriptions.Item label="DAG ID">{selectedDag.dag_id}</Descriptions.Item>
              <Descriptions.Item label="업무 구분">{categoryOf(selectedDag)}</Descriptions.Item>
              <Descriptions.Item label="업무 폴더">{folderName(selectedDag)}</Descriptions.Item>
              <Descriptions.Item label="스케줄">{scheduleDescription(selectedDag.timetable_summary)}</Descriptions.Item>
              <Descriptions.Item label="설명">{selectedDag.description || "-"}</Descriptions.Item>
            </Descriptions>}
          </Modal>
        </>
      )}
    </div>
    </ConfigProvider>
  );
}

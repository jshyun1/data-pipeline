import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, DatePicker, Descriptions, Dropdown, Empty, Input, message, Modal, Radio, Select, Space, Spin, Table, Tag } from "antd";
import type { Dayjs } from "dayjs";
import {
  CheckCircleFilled,
  PlusCircleFilled,
  ClockCircleFilled,
  CloseCircleFilled,
  DeploymentUnitOutlined,
  DeleteOutlined,
  DownOutlined,
  FolderOpenOutlined,
  FolderOutlined,
  InfoCircleOutlined,
  PlayCircleOutlined,
  ProfileOutlined,
  RightOutlined,
  SyncOutlined,
} from "@ant-design/icons";
import dayjs from "dayjs";
import { Link, useSearchParams } from "react-router-dom";
import {
  getAirflowTaskLog,
  acknowledgeAirflowDagAlert,
  deleteAirflowDagCatalog,
  listAirflowDagAlertHistory,
  listAirflowDagAlerts,
  listAirflowDagCatalog,
  listAirflowDags,
  listAirflowTaskInstances,
  listAllAirflowDagRuns,
  retryAirflowTasks,
  syncAirflowDagCatalog,
  triggerAirflowDag,
  type AirflowDag,
  type AirflowDagAlert,
  type AirflowDagRun,
} from "../api/platform";
import { getProcessHealth, type ProcessGroup, type ProcessStatus } from "../api/infra";
import { getWorkflow, listWorkflows, type WorkflowSummary } from "../api/workflows";
import { getAlertRulesWatching } from "../api/config";
import { listConnections } from "../api/connections";
import { getRealtimePipelineMetrics, type RealtimePipelineMetricResponse } from "../api/dashboard";
import { listPipelineRuntimeStatuses, listPipelines } from "../api/pipelines";
import type { PipelineResponse, PipelineRuntimeStatusResponse } from "../types/pipeline";
import {
  cdcPipelineColumns,
  renderRuntimeStatus,
  sourceLabel,
  targetLabel,
} from "../utils/cdcPresentation";
import { getNifiProcessGroupTree, type NifiProcessGroupTreeNode } from "../api/platform";
import { getEtlJob, listEtlJobs } from "../api/etlJobs";
import { categorizeDag, extractKafkaPipelineId, type DagCategory } from "../utils/dagHistory";
import { scheduleDescription } from "../utils/schedulePreset";

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
  alerts: AirflowDagAlert[];
  alertHistory: AirflowDagAlert[];
  processGroups: ProcessGroup[];
}

const CATEGORY_ORDER: BusinessCategory[] = ["CDC", "ETL"];
const REFRESH_OPTIONS = [
  { label: "10초", value: 10 },
  { label: "30초", value: 30 },
  { label: "1분", value: 60 },
];

type ExecutionAction = "deploy" | "start" | "stop" | "monitor";

function runAt(run?: AirflowDagRun) {
  return run?.start_date ?? run?.execution_date;
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

function stateLabel(state?: string) {
  if (state === "success") return "성공";
  if (state === "failed") return "실패";
  if (state === "running") return "실행 중";
  if (state === "queued" || state === "scheduled") return "대기";
  if (state === "upstream_failed") return "선행 실패";
  if (state === "skipped") return "건너뜀";
  if (state === "up_for_retry") return "재시도 대기";
  return state ?? "실행 없음";
}

function stateColor(state?: string) {
  if (state === "success") return "success";
  if (state === "failed" || state === "upstream_failed") return "error";
  if (state === "running") return "processing";
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
  const tags = (dag.tags ?? []).map((tag) => (typeof tag === "string" ? tag : tag.name ?? ""));
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

const ALERT_RULE_LABEL: Record<AirflowDagAlert["ruleType"], string> = {
  CONSECUTIVE_FAILURE: "연속 실패",
  STALE: "정체",
  DURATION_ANOMALY: "실행시간 이상",
  SLA_EXCEEDED: "SLA 초과",
};

const ALERT_SEVERITY_LABEL: Record<AirflowDagAlert["severity"], string> = {
  DANGER: "위험",
  WARNING: "경고",
  INFO: "정보",
};

function alertColor(severity: AirflowDagAlert["severity"]) {
  if (severity === "DANGER") return "error";
  if (severity === "WARNING") return "warning";
  return "processing";
}

async function loadDashboard(): Promise<DashboardData> {
  const [airflowDags, catalog, runs, alerts, alertHistory, processHealth, etlJobs] = await Promise.all([
    listAirflowDags(),
    listAirflowDagCatalog(),
    listAllAirflowDagRuns({ maxRuns: 2000 }),
    listAirflowDagAlerts(),
    listAirflowDagAlertHistory(),
    getProcessHealth().catch(() => ({ collectedAt: "", groups: [] })),
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
    alerts,
    alertHistory,
    processGroups: processHealth.groups,
  };
}

function DagIcon() {
  return <DeploymentUnitOutlined className="airflow-dashboard-dag-icon" />;
}

/** 상단 카드에서 고른 지표. 가운데 목록이 이 조건으로 좁혀진다. */
type StatusMetric = "all" | "running" | "success" | "failed" | "waiting" | "new";

/**
 * 가운데 목록이 무엇을 보여줄지. 트리에서 고르거나 상단 지표를 누르면 바뀐다.
 *
 * CDC는 파이프라인이, ETL은 워크플로우가 실행 단위라 목록의 성격이 다르다.
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
  | { kind: "cdc"; scope: "all" }
  | { kind: "cdc"; scope: "connection"; connectionId: number }
  | { kind: "cdc"; scope: "schema"; connectionId: number; schema: string }
  | { kind: "cdc"; scope: "logfile" }
  | { kind: "cdc"; scope: "pipeline"; pipelineId: number }
  | { kind: "etl"; groupPgId: string; groupName: string }
  | { kind: "dag"; dagId: string }
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
): boolean {
  if (metric === "all") return true;
  if (metric === "new") return Boolean(dag.created_at && dayjs(dag.created_at).isAfter(today));
  if (metric === "running") return runs.some((run) => run.state === "running");
  const latestToday = runs.find((run) => {
    const startedAt = runAt(run);
    return Boolean(startedAt && dayjs(startedAt).isAfter(today));
  });
  if (metric === "success") return latestToday?.state === "success";
  if (metric === "failed") return latestToday?.state === "failed";
  return latestToday?.state === "queued" || latestToday?.state === "scheduled";
}

/**
 * 업무별 상태 카드.
 *
 * <p>모든 숫자가 <b>당일 · 작업(DAG) 기준</b>이다. «전체 작업»이 DAG 수인데 나머지만 실행
 * 건수면 단위가 섞여 읽히지 않는다. 그래서 각 DAG의 <b>오늘 마지막 실행</b>이 어떤 상태인지로
 * 한 번씩만 센다 - 한 작업이 오늘 세 번 실패해도 «실패 1»이다.
 *
 * <p>«실행 중»만 날짜와 무관하게 지금 돌고 있는 작업을 센다(어제 시작해 아직 도는 것도 포함).
 *
 * <p>숫자를 누르면 가운데 목록이 그 대상으로 좁혀진다.
 */
function StatusCard({
  category,
  dags,
  runsByDag,
  activeMetric,
  onMetricClick,
}: {
  category: BusinessCategory;
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  activeMetric?: StatusMetric;
  onMetricClick: (metric: StatusMetric) => void;
}) {
  const today = dayjs().startOf("day");
  let active = 0;
  let waiting = 0;
  let success = 0;
  let failed = 0;
  let fresh = 0;
  for (const dag of dags) {
    const runs = runsByDag.get(dag.dag_id) ?? [];
    if (matchesMetric("running", dag, runs, today)) active += 1;
    if (matchesMetric("success", dag, runs, today)) success += 1;
    if (matchesMetric("failed", dag, runs, today)) failed += 1;
    if (matchesMetric("waiting", dag, runs, today)) waiting += 1;
    if (matchesMetric("new", dag, runs, today)) fresh += 1;
  }

  const metric = (key: StatusMetric, icon: React.ReactNode | null, label: string, value: number, modifier = "") => (
    <button
      type="button"
      className={`airflow-business-metric${modifier}${activeMetric === key ? " active" : ""}`}
      onClick={() => onMetricClick(key)}
    >
      {icon}
      <span>{label}<strong>{value}</strong></span>
    </button>
  );

  return (
    <section className="airflow-business-card">
      <strong className="airflow-business-name">{category}</strong>
      {metric("all", null, "전체 작업", dags.length)}
      {metric("running", <RightOutlined />, "실행 중", active)}
      {category === "ETL" && metric("success", <CheckCircleFilled />, "성공", success, " airflow-business-metric--success")}
      {metric("failed", <CloseCircleFilled />, "실패", failed, " airflow-business-metric--danger")}
      {category === "CDC" && metric("waiting", <ClockCircleFilled />, "대기", waiting, " airflow-business-metric--waiting")}
      {metric("new", <PlusCircleFilled />, "신규", fresh, " airflow-business-metric--new")}
    </section>
  );
}

function BusinessTree({
  dags,
  groupTree,
  selectedDagId,
  search,
  alertsByDag,
  onSearch,
  workflows,
  pipelines,
  connectionNames,
  selectedPipelineId,
  scope,
  onScope,
  onSelect,
  onDetail,
  onExecution,
  onDelete,
}: {
  dags: DashboardDag[];
  /** NiFi 프로세스 그룹 계층. ETL은 이 구조 그대로 보여준다(ETL 관리 화면과 동일). */
  groupTree?: NifiProcessGroupTreeNode | null;
  /** 워크플로우 목록. DAG를 어느 그룹에 붙일지는 여기의 nifiGroupPgId가 정한다. */
  workflows: WorkflowSummary[];
  /** CDC 가지는 CDC 관리 화면과 같은 계층(연결 → 스키마 → 파이프라인)으로 그린다. */
  pipelines: PipelineResponse[];
  connectionNames: Map<number, string>;
  /** 가운데 목록에서 고른 파이프라인. 트리 범위를 바꾸지 않고 행만 고를 수 있어 따로 받는다. */
  selectedPipelineId?: number;
  scope?: ListScope;
  onScope: (scope: ListScope) => void;
  selectedDagId?: string;
  search: string;
  alertsByDag: Map<string, AirflowDagAlert[]>;
  onSearch: (value: string) => void;
  onSelect: (dagId: string) => void;
  onDetail: (dag: DashboardDag) => void;
  onExecution: (dag: DashboardDag) => void;
  onDelete: (dag: DashboardDag) => void;
}) {
  const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(new Set());
  const seeded = useRef(false);

  /**
   * 처음 열었을 때의 트리 모양.
   *
   * <p>기본값이 «전부 펼침»이라 들어오자마자 파이프라인·워크플로우가 다 쏟아졌다.
   * CDC는 «전체 파이프라인», ETL은 «ETL Root»까지만 보이게 접어두고, 필요한 가지를
   * 사용자가 펼치게 한다.
   */
  useEffect(() => {
    if (seeded.current || !groupTree) {
      return;
    }
    seeded.current = true;
    const initial = new Set<string>(["cdc:all", "cdc:logfile"]);
    pipelines.forEach((pipeline) => {
      if (pipeline.sourceConnectionId != null) {
        initial.add(`cdc:connection:${pipeline.sourceConnectionId}`);
        initial.add(`cdc:schema:${pipeline.sourceConnectionId}:${pipeline.sourceSchema ?? "스키마 없음"}`);
      }
    });
    const collapseChildren = (node: NifiProcessGroupTreeNode) => {
      (node.children ?? []).forEach((child) => {
        initial.add(`group:${child.id}`);
        collapseChildren(child);
      });
    };
    initial.add(`group:${groupTree.id}`);
    collapseChildren(groupTree);
    setCollapsedNodes(initial);
  }, [groupTree, pipelines]);
  /**
   * 가운데 목록에서 고른 항목이 트리에서 보이도록 조상 가지를 편다.
   *
   * 트리를 «전체 파이프라인 / ETL Root»까지만 펼쳐 두다 보니, 목록에서 워크플로우를 눌러도
   * 왼쪽에서는 어디에 있는 것인지 보이지 않았다. 고른 항목까지의 길만 열어준다.
   */
  const revealPipelineId = selectedPipelineId
    ?? (scope?.kind === "cdc" && scope.scope === "pipeline" ? scope.pipelineId : undefined);
  useEffect(() => {
    const open = new Set<string>();
    if (selectedDagId) {
      const groupPgId = groupOfWorkflowDag.get(selectedDagId)
        // 아직 워크플로우로 안 옮긴 옛 DAG는 잡 카탈로그의 프로세스 그룹으로 찾는다.
        ?? dags.find((dag) => dag.dag_id === selectedDagId)?.nifi_process_group_id;
      if (groupPgId && groupTree) {
        const path: string[] = [];
        const find = (node: NifiProcessGroupTreeNode, trail: string[]): boolean => {
          const next = [...trail, node.id];
          if (node.id === groupPgId) {
            path.push(...next);
            return true;
          }
          return (node.children ?? []).some((child) => find(child, next));
        };
        if (find(groupTree, [])) {
          open.add("category:ETL");
          path.forEach((id) => open.add(`group:${id}`));
        }
      }
    }
    if (revealPipelineId != null) {
      const pipeline = pipelines.find((row) => row.id === revealPipelineId);
      if (pipeline) {
        open.add("category:CDC");
        open.add("cdc:all");
        if (pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null) {
          open.add("cdc:logfile");
        } else {
          open.add(`cdc:connection:${pipeline.sourceConnectionId}`);
          open.add(`cdc:schema:${pipeline.sourceConnectionId}:${pipeline.sourceSchema ?? "스키마 없음"}`);
        }
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
  }, [selectedDagId, revealPipelineId, groupTree, pipelines, dags]);

  const toggleNode = (node: string) => {
    setCollapsedNodes((current) => {
      const next = new Set(current);
      if (next.has(node)) next.delete(node);
      else next.add(node);
      return next;
    });
  };

  /** DAG 하나를 트리 항목으로. 우클릭 메뉴(상세/실행설정/삭제)는 기존과 동일하다. */
  const renderDagButton = (dag: DashboardDag, depth = 0) => (
    <Dropdown
      key={dag.dag_id}
      trigger={["contextMenu"]}
      menu={{
        items: [
          { key: "detail", icon: <InfoCircleOutlined />, label: "상세" },
          { key: "execution", icon: <PlayCircleOutlined />, label: "실행 설정" },
          { key: "delete", icon: <DeleteOutlined />, label: "삭제", danger: true },
        ],
        onClick: ({ key }) => {
          if (key === "detail") onDetail(dag);
          else if (key === "execution") onExecution(dag);
          else onDelete(dag);
        },
      }}
    >
      <button
        type="button"
        className={selectedDagId === dag.dag_id ? "airflow-tree-dag active" : "airflow-tree-dag"}
        style={{ paddingLeft: 12 + depth * 14 }}
        onClick={() => { onSelect(dag.dag_id); onScope({ kind: "dag", dagId: dag.dag_id }); }}
        onContextMenu={() => onSelect(dag.dag_id)}
      >
        <span>{displayName(dag)}</span>
        {alertsByDag.has(dag.dag_id) && <Tag color="error">조치</Tag>}
      </button>
    </Dropdown>
  );

  /**
   * NiFi 그룹 계층을 그대로 그린다(ETL 관리 화면과 같은 모양).
   *
   * 그룹에 대응하는 DAG가 있으면 그 자리에서 바로 선택할 수 있고, DAG가 없는 그룹
   * (예: 체인을 담고 있는 DW/DZ, 템플릿 보관용 그룹)은 위치만 보여준다.
   */
  const groupOfWorkflowDag = new Map<string, string>();
  workflows.forEach((workflow) => {
    if (workflow.nifiGroupPgId) {
      groupOfWorkflowDag.set(workflow.dagId, workflow.nifiGroupPgId);
    }
  });
  /** 이 그룹과 하위 전체에 걸린 워크플로우 수. 트리 오른쪽 숫자로 쓴다. */
  const countWorkflowsUnder = (node: NifiProcessGroupTreeNode): number => {
    const own = workflows.filter((w) => w.nifiGroupPgId === node.id).length;
    return own + (node.children ?? []).reduce((sum, child) => sum + countWorkflowsUnder(child), 0);
  };

  /** 이 그룹이나 하위에 돌릴 것(워크플로우 또는 아직 안 옮긴 옛 DAG)이 있는지. */
  const hasRunnable = (node: NifiProcessGroupTreeNode): boolean => {
    const own = dags.some((d) =>
      groupOfWorkflowDag.get(d.dag_id) === node.id
      || d.nifi_process_group_id === node.id
      || d.dag_id === `nifi_pipeline_${node.id.slice(0, 8)}_control`);
    return own || (node.children ?? []).some(hasRunnable);
  };

  /**
   * CDC 가지. CDC 관리 화면과 같은 계층으로 그린다 - 연결 → 스키마 → 파이프라인.
   * 예전에는 DAG(kafka_pipeline_N_control)를 평평하게 늘어놓아서 어느 원천인지 알 수 없었다.
   */
  const renderCdcTree = () => {
    const byConnection = new Map<number, Map<string, PipelineResponse[]>>();
    const logFiles: PipelineResponse[] = [];
    const needle = search.trim().toLowerCase();
    for (const pipeline of pipelines) {
      const matches = !needle
        || pipeline.name.toLowerCase().includes(needle)
        || (pipeline.sourceTable ?? "").toLowerCase().includes(needle)
        || (pipeline.targetTable ?? "").toLowerCase().includes(needle);
      if (!matches) {
        continue;
      }
      if (pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null) {
        logFiles.push(pipeline);
        continue;
      }
      const schemas = byConnection.get(pipeline.sourceConnectionId) ?? new Map<string, PipelineResponse[]>();
      const schema = pipeline.sourceSchema ?? "스키마 없음";
      schemas.set(schema, [...(schemas.get(schema) ?? []), pipeline]);
      byConnection.set(pipeline.sourceConnectionId, schemas);
    }

    const folder = (
      key: string, label: string, count: number, depth: number,
      selected: boolean, onClick: () => void, children: React.ReactNode,
    ) => {
      const collapsed = collapsedNodes.has(key);
      return (
        <div key={key}>
          <button type="button"
                  className={selected ? "airflow-tree-label active" : "airflow-tree-label"}
                  style={{ paddingLeft: 8 + depth * 14 }}
                  aria-expanded={!collapsed}
                  onClick={() => { toggleNode(key); onClick(); }}>
            {collapsed ? <RightOutlined /> : <DownOutlined />}
            {collapsed ? <FolderOutlined /> : <FolderOpenOutlined />}
            {label}
            <span style={{ color: "#888" }}> ({count})</span>
          </button>
          {!collapsed && children}
        </div>
      );
    };

    const total = pipelines.length;
    return folder(
      "cdc:all", "전체 파이프라인", total, 0,
      scope?.kind === "cdc" && scope.scope === "all",
      () => onScope({ kind: "cdc", scope: "all" }),
      <>
        {[...byConnection.entries()].map(([connectionId, schemas]) => {
          const count = [...schemas.values()].reduce((sum, rows) => sum + rows.length, 0);
          return folder(
            `cdc:connection:${connectionId}`, connectionNames.get(connectionId) ?? `연결 ${connectionId}`,
            count, 1,
            scope?.kind === "cdc" && scope.scope === "connection" && scope.connectionId === connectionId,
            () => onScope({ kind: "cdc", scope: "connection", connectionId }),
            <>
              {[...schemas.entries()].map(([schema, rows]) => folder(
                `cdc:schema:${connectionId}:${schema}`, schema, rows.length, 2,
                scope?.kind === "cdc" && scope.scope === "schema"
                  && scope.connectionId === connectionId && scope.schema === schema,
                () => onScope({ kind: "cdc", scope: "schema", connectionId, schema }),
                <>
                  {rows.map((pipeline) => (
                    <button key={pipeline.id} type="button"
                            className={revealPipelineId === pipeline.id
                              ? "airflow-tree-dag active" : "airflow-tree-dag"}
                            style={{ paddingLeft: 12 + 3 * 14 }}
                            onClick={() => onScope({ kind: "cdc", scope: "pipeline", pipelineId: pipeline.id })}>
                      <span>{pipeline.name}</span>
                    </button>
                  ))}
                </>,
              ))}
            </>,
          );
        })}
        {logFiles.length > 0 && folder(
          "cdc:logfile", "로그파일", logFiles.length, 1,
          scope?.kind === "cdc" && scope.scope === "logfile",
          () => onScope({ kind: "cdc", scope: "logfile" }),
          <>
            {logFiles.map((pipeline) => (
              <button key={pipeline.id} type="button"
                      className={revealPipelineId === pipeline.id
                        ? "airflow-tree-dag active" : "airflow-tree-dag"}
                      style={{ paddingLeft: 12 + 2 * 14 }}
                      onClick={() => onScope({ kind: "cdc", scope: "pipeline", pipelineId: pipeline.id })}>
                <span>{pipeline.name}</span>
              </button>
            ))}
          </>,
        )}
      </>,
    );
  };

  /**
   * 트리 어딘가에 자리를 갖는 DAG. 접기와 무관하게 원본 트리 전체를 훑는다.
   *
   * 그려진 노드에서만 모으면, 접힌 가지에 속한 워크플로우가 «아직 배치 안 됨»으로 남아
   * 트리 맨 아래 고아 목록에 뜬다 - 그룹을 접었는데 DAG만 그룹 밖에 나와 보이는 증상.
   */
  const placedDagIds = new Set<string>();
  const collectPlaced = (node: NifiProcessGroupTreeNode, etlDags: DashboardDag[]) => {
    const own = etlDags.filter((d) => groupOfWorkflowDag.get(d.dag_id) === node.id);
    own.forEach((d) => placedDagIds.add(d.dag_id));
    if (own.length === 0) {
      const legacy = etlDags.find((d) =>
        d.nifi_process_group_id === node.id
        || d.dag_id === `nifi_pipeline_${node.id.slice(0, 8)}_control`);
      if (legacy) {
        placedDagIds.add(legacy.dag_id);
      }
    }
    (node.children ?? []).forEach((child) => collectPlaced(child, etlDags));
  };

  const renderGroupTree = (node: NifiProcessGroupTreeNode, etlDags: DashboardDag[], depth: number): React.ReactNode => {
    // 실행 단위는 워크플로우(DAG)다. job 그룹은 워크플로우 안의 작업이라 트리에 두지 않는다
    // - 눌러도 아무 일이 없는 항목이 절반을 차지했다. job은 DAG 내 하위 작업 탭에서 본다.
    if (node.groupType === "JOB") {
      return null;
    }
    // 이 그룹에 속한 워크플로우 DAG들. 워크플로우는 nifi_group_pg_id로 그룹을 안다.
    const workflowDags = etlDags.filter((d) => groupOfWorkflowDag.get(d.dag_id) === node.id);
    // 아직 워크플로우로 옮기지 않은 그룹은 옛 팩토리 DAG가 그 자리를 지킨다(점진 컷오버).
    // dag_id 자체가 프로세스 그룹 id의 앞 8자를 담고 있다(nifi_pipeline_{8자}_control).
    const dag = workflowDags.length ? undefined : etlDags.find((d) =>
      d.nifi_process_group_id === node.id
      || d.dag_id === `nifi_pipeline_${node.id.slice(0, 8)}_control`);
    const children = node.children ?? [];
    const nodeKey = `group:${node.id}`;
    const collapsed = collapsedNodes.has(nodeKey);
    const needle = search.trim().toLowerCase();
    // 검색 중이면 자신 또는 자손이 걸리는 가지만 남긴다.
    const selfMatches = !needle || node.name.toLowerCase().includes(needle);
    const renderedChildren = collapsed ? [] : children
      .map((child) => renderGroupTree(child, etlDags, depth + 1))
      .filter(Boolean);
    if (needle && !selfMatches && renderedChildren.length === 0 && workflowDags.length === 0) {
      return null;
    }
    // 실행할 것이 하나도 없는 그룹(워크플로우도, 아직 안 옮긴 옛 DAG도 없음)은 감춘다.
    // 실행 현황은 "무엇을 돌릴 수 있나"를 보는 화면이라 빈 폴더는 잡음이다.
    // 접힘 여부와 무관해야 하므로 renderedChildren이 아니라 원본 트리를 본다.
    if (!hasRunnable(node)) {
      return null;
    }
    const workflowCount = countWorkflowsUnder(node);

    return (
      <div key={node.id}>
        {dag ? renderDagButton(dag, depth) : (
          <button type="button"
                  className={scope?.kind === "etl" && scope.groupPgId === node.id
                    ? "airflow-tree-label active" : "airflow-tree-label"}
                  style={{ paddingLeft: 8 + depth * 14, opacity: children.length ? 1 : 0.5 }}
                  aria-expanded={!collapsed}
                  onClick={() => {
                    // 그룹을 누르면 접히면서 그 그룹의 워크플로우 목록이 가운데에 뜬다.
                    if (children.length) {
                      toggleNode(nodeKey);
                    }
                    onScope({ kind: "etl", groupPgId: node.id, groupName: node.name });
                  }}>
            {children.length ? (collapsed ? <RightOutlined /> : <DownOutlined />) : null}
            {children.length ? (collapsed ? <FolderOutlined /> : <FolderOpenOutlined />) : null}
            {node.name}
            {/* 숫자는 이 그룹과 하위에 있는 워크플로우 수다(실행할 게 몇 개인지). */}
            {workflowCount ? <span style={{ color: "#888" }}> ({workflowCount})</span> : null}
          </button>
        )}
        {!collapsed && workflowDags.map((workflowDag) => renderDagButton(workflowDag, depth + 1))}
        {renderedChildren}
      </div>
    );
  };

  return (
    <aside className="airflow-dashboard-panel airflow-business-tree">
      <h3>업무별 트리</h3>
      <Input.Search allowClear placeholder="검색" value={search} onChange={(event) => onSearch(event.target.value)} />
      <div className="airflow-tree-body">
        {CATEGORY_ORDER.map((category) => {
          const categoryDags = dags.filter((dag) => categoryOf(dag) === category);
          // 접힘과 무관하게 «트리에 자리가 있는 DAG»를 먼저 모은다(고아 목록 판정용).
          if (category === "ETL" && groupTree) {
            collectPlaced(groupTree, categoryDags);
          }
          const categoryNode = `category:${category}`;
          const categoryCollapsed = collapsedNodes.has(categoryNode);
          return (
            <section key={category} className="airflow-tree-category">
              <button type="button" className="airflow-tree-label" aria-expanded={!categoryCollapsed} onClick={() => toggleNode(categoryNode)}>
                {categoryCollapsed ? <RightOutlined /> : <DownOutlined />}
                {categoryCollapsed ? <FolderOutlined /> : <FolderOpenOutlined />}
                {category}
              </button>
              {!categoryCollapsed && category === "CDC" && renderCdcTree()}
              {!categoryCollapsed && category === "ETL" && groupTree
                ? renderGroupTree(groupTree, categoryDags, 0)
                : null}
              {!categoryCollapsed && category === "ETL" && !groupTree && categoryDags.map((dag) => (
                <Dropdown
                  key={dag.dag_id}
                  trigger={["contextMenu"]}
                  menu={{
                    items: [
                      { key: "detail", icon: <InfoCircleOutlined />, label: "상세" },
                      { key: "execution", icon: <PlayCircleOutlined />, label: "실행 설정" },
                      { key: "delete", icon: <DeleteOutlined />, label: "삭제", danger: true },
                    ],
                    onClick: ({ key }) => {
                      if (key === "detail") onDetail(dag);
                      else if (key === "execution") onExecution(dag);
                      else onDelete(dag);
                    },
                  }}
                >
                  <button
                    type="button"
                    className={selectedDagId === dag.dag_id ? "airflow-tree-dag active" : "airflow-tree-dag"}
                    onClick={() => onSelect(dag.dag_id)}
                    onContextMenu={() => onSelect(dag.dag_id)}
                  >
                    <span>{displayName(dag)}</span>
                    {alertsByDag.has(dag.dag_id) && <Tag color="error">조치</Tag>}
                  </button>
                </Dropdown>
              ))}
              {!categoryCollapsed && category === "ETL" && groupTree
                ? categoryDags
                    // 트리 어딘가에 이미 붙은 DAG는 여기서 또 보여주지 않는다.
                    .filter((dag) => !placedDagIds.has(dag.dag_id))
                    .map((dag) => renderDagButton(dag))
                : null}
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
}: {
  dag: DashboardDag;
  run?: AirflowDagRun;
  refreshSeconds: number;
  configured?: boolean;
}) {
  const [retryingTaskId, setRetryingTaskId] = useState<string>();
  const [expandedJobKey, setExpandedJobKey] = useState<string>();
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
  const completedTasks = (tasksQuery.data ?? []).filter((task) => task.state === "success" || task.state === "failed");
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
    if (state !== "failed") return "-";
    const lines = log.split("\n").map((line) => line.trim()).filter(Boolean);
    // 도움말 URL(https://docs.oracle.com/error-help/db/ora-12170/)만 있는 줄은 사유가 아니다.
    // 예전 규칙은 대소문자 무시로 "ora-12170"을 원인 코드로 착각해 URL을 사유로 띄웠다.
    const meaningful = lines.filter((line) => !/^https?:\/\//.test(line));
    // 1순위: 원인 코드가 붙은 줄(ORA-01234: …). 2순위: Exception/Error 줄. 3순위: 마지막 줄.
    // Airflow 로그는 JSON 한 줄에 메시지 전체가 담긴다. 줄 단위로 고르면 원인 코드가
    // 180자 뒤로 밀려 잘린다. 코드가 있으면 그 지점부터 잘라 원인이 앞에 오게 한다.
    const code = meaningful.map((line) => {
      const hit = line.match(/\b[A-Z]{2,5}-\d{3,5}:.*/);
      return hit ? hit[0] : "";
    }).find(Boolean);
    const reason = code
      ?? [...meaningful].reverse().find((line) => /(?:Exception|Error):|실패/.test(line))
      ?? meaningful.at(-1);
    return reason?.replace(/^\[[^\]]+\]\s*/, "").replace(/^-\s*Caused by:\s*/, "").slice(0, 180)
      || "실패 사유를 확인할 수 없습니다.";
  };
  const rerunJob = async (jobKey: string, taskIds: string[], withDownstream: boolean) => {
    if (!run) return;
    setRetryingTaskId(jobKey);
    try {
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

  /** 단계 상태를 job 하나의 상태로 접는다. 하나라도 실패면 실패다. */
  const rollUpState = (steps: NonNullable<typeof tasksQuery.data>) => {
    const states = steps.map((step) => step.state);
    if (states.includes("failed")) return "failed";
    if (states.includes("running")) return "running";
    if (states.includes("upstream_failed")) return "upstream_failed";
    if (states.every((state) => state === "success")) return "success";
    return states.find(Boolean) ?? undefined;
  };

  return order.flatMap((key, index) => {
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
    const detail = failed ? outcomeDetail(failed.task_id, "failed")
      : state === "upstream_failed" ? "선행 작업 실패로 미실행"
      : state === "success" ? outcomeDetail(counted.task_id, "success")
      : "-";
    const expanded = expandedJobKey === key;
    const busy = Boolean(retryingTaskId && retryingTaskId !== key);
    // 그룹 밖 태스크(workflow_done)는 job이 아니라 워크플로우 종단 신호다.
    const isJob = steps.length > 1 || steps[0].task_id.includes(".");

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
        <td><ProfileOutlined /> {isJob ? `JOB (${steps.length}단계)` : "워크플로우 종단"}</td>
        <td>{startedAt ? dayjs(startedAt).format("YYYY-MM-DD HH:mm") : "-"}</td>
        <td><Tag color={stateColor(state)}>{stateLabel(state)}</Tag></td>
        <td>{state === "failed" ? <small title={detail}>{detail}</small> : detail}</td>
        <td>{duration(startedAt, endedAt)}</td>
        <td>-</td>
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
          <td>{step.state === "failed" ? <small title={stepDetail}>{stepDetail}</small> : stepDetail}</td>
          <td>{duration(step.start_date, step.end_date)}</td>
          <td>-</td>
          <td>{Math.max(0, (step.try_number ?? 1) - 1)}회</td>
        </tr>
      );
    })];
  });
}

function ScopeList({
  scope,
  dags,
  runsByDag,
  pipelines,
  runtimeByPipeline,
  metricByPipeline,
  workflowsByGroup,
  selectedDagId,
  selectedPipelineId,
  onSelectDag,
  onSelectPipeline,
}: {
  scope: ListScope;
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  pipelines: PipelineResponse[];
  runtimeByPipeline: Map<number, PipelineRuntimeStatusResponse>;
  metricByPipeline: Map<number, RealtimePipelineMetricResponse>;
  workflowsByGroup: Map<string, Set<string>>;
  selectedDagId?: string;
  selectedPipelineId?: number;
  onSelectDag: (dagId: string) => void;
  onSelectPipeline: (pipelineId: number) => void;
}) {
  const today = dayjs().startOf("day");

  // CDC는 파이프라인이 실행 단위다. 트리에서 고르든 상단 지표를 누르든 같은 표를 그린다 -
  // 예전에는 지표를 누르면 ETL과 같은 DAG 표가 나와서, 같은 대상을 두 가지 모양으로
  // 보여주고 있었다(scope.kind만 보고 category를 안 봤다).
  let cdcRows: PipelineResponse[] | null = null;
  if (scope.kind === "cdc") {
    cdcRows = pipelines.filter((pipeline) => {
      if (scope.scope === "all") return true;
      if (scope.scope === "logfile") return pipeline.pipelineType === "LOG_FILE" || pipeline.sourceConnectionId == null;
      if (scope.scope === "connection") return pipeline.sourceConnectionId === scope.connectionId;
      if (scope.scope === "schema") {
        return pipeline.sourceConnectionId === scope.connectionId
          && (pipeline.sourceSchema ?? "스키마 없음") === scope.schema;
      }
      return pipeline.id === scope.pipelineId;
    });
  } else if (scope.kind === "metric" && scope.category === "CDC") {
    // 지표는 DAG 기준으로 세므로 걸러내기도 DAG로 하고, 그 뒤에 파이프라인으로 되돌린다.
    // 이렇게 해야 카드 숫자와 목록 건수가 어긋나지 않는다.
    const pipelineIds = new Set(
      dags
        .filter((dag) => categoryOf(dag) === "CDC"
          && matchesMetric(scope.metric, dag, runsByDag.get(dag.dag_id) ?? [], today))
        .map((dag) => extractKafkaPipelineId(dag.dag_id)),
    );
    cdcRows = pipelines.filter((pipeline) => pipelineIds.has(String(pipeline.id)));
  }

  if (cdcRows) {
    // 파이프라인 -> 그 파이프라인 제어 DAG의 최근 실행. "워크플로우 상태" 열에 쓴다.
    const latestRunOfPipeline = (pipelineId: number) =>
      (runsByDag.get(`kafka_pipeline_${pipelineId}_control`) ?? [])[0];
    return (
      <Table<PipelineResponse>
        rowKey="id"
        size="small"
        dataSource={cdcRows}
        scroll={{ x: 1130 }}
        columns={cdcPipelineColumns(runtimeByPipeline, metricByPipeline, {
          render: (pipeline) => {
            const run = latestRunOfPipeline(pipeline.id);
            return <Tag color={stateColor(run?.state)}>{stateLabel(run?.state)}</Tag>;
          },
        })}
        pagination={{ pageSize: 20, hideOnSinglePage: true, showTotal: (total) => `전체 ${total}건` }}
        rowClassName={(row) => row.id === selectedPipelineId ? "airflow-row-selected" : ""}
        onRow={(row) => ({ onClick: () => onSelectPipeline(row.id) })}
        locale={{ emptyText: <Empty description="이 범위에 파이프라인이 없습니다." /> }}
      />
    );
  }

  // ETL 그룹 / ETL 지표 선택 -> DAG(워크플로우) 목록
  const latestRun = (dagId: string) => (runsByDag.get(dagId) ?? [])[0];
  // cdc 범위는 위에서 이미 반환했으므로 여기서는 남은 세 가지만 다룬다.
  let rows: DashboardDag[] = [];
  if (scope.kind === "etl") {
    const dagIds = workflowsByGroup.get(scope.groupPgId) ?? new Set<string>();
    rows = dags.filter((dag) => dagIds.has(dag.dag_id));
  } else if (scope.kind === "metric") {
    // CDC 지표는 위에서 파이프라인 표로 처리했으므로 여기 오는 건 ETL뿐이다.
    rows = dags.filter((dag) => categoryOf(dag) === scope.category
      && matchesMetric(scope.metric, dag, runsByDag.get(dag.dag_id) ?? [], today));
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
      pagination={{ pageSize: 20, hideOnSinglePage: true, showTotal: (total) => `전체 ${total}건` }}
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
  alertsByDag,
  selectedDagId,
  refreshSeconds,
  onSelect,
}: {
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  alertsByDag: Map<string, AirflowDagAlert[]>;
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
        <thead><tr><th>작업명</th><th>유형</th><th>최근 실행</th><th>최종 결과</th><th>적재/실패 사유</th><th>소요</th><th>다음 실행</th><th>재시도/재시작</th></tr></thead>
        <tbody>
          {CATEGORY_ORDER.map((category) => {
            const categoryDags = dags.filter((dag) => categoryOf(dag) === category);
            if (!categoryDags.length) return null;
            return [
              <tr key={`${category}-heading`} className="airflow-job-group"><td colSpan={8}>{category}</td></tr>,
              ...categoryDags.flatMap((dag) => {
                const runs = runsByDag.get(dag.dag_id) ?? [];
                const latest = runs[0];
                const alert = alertsByDag.get(dag.dag_id)?.[0];
                const selected = dag.dag_id === selectedDagId;
                return [
                  <tr key={dag.dag_id} className={selected ? "airflow-dag-row selected" : "airflow-dag-row"} onClick={() => onSelect(dag.dag_id)}>
                    <td><button type="button">{displayName(dag)}</button></td>
                    <td><DagIcon /> DAG</td>
                    <td>{runAt(latest) ? dayjs(runAt(latest)).format("YYYY-MM-DD HH:mm") : "-"}</td>
                    <td><Tag color={stateColor(latest?.state)}>{stateLabel(latest?.state)}</Tag></td>
                    <td>{alert ? <span className="airflow-alert-reason"><Tag color={alertColor(alert.severity)}>{ALERT_RULE_LABEL[alert.ruleType]}</Tag>{alert.message}</span> : "-"}</td>
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
        <thead><tr><th>작업명</th><th>실행 유형</th><th>실행 일시</th><th>최종 결과</th><th>적재/실패 사유</th><th>소요</th><th>실행 ID</th><th>재시도/재시작</th></tr></thead>
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
              <td title={run.dag_run_id}>{run.dag_run_id}</td>
              <td>{expanded ? <DownOutlined /> : <RightOutlined />}</td>
            </tr>,
            ...(expanded ? [<TaskRows key={`${run.dag_run_id}-tasks`} dag={dag} run={run} refreshSeconds={refreshSeconds} />] : []),
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
           footer={null} width={1080} destroyOnHidden>
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
        <p>이 작업을 감시하는 규칙이 없습니다.</p>
      ) : (
        <div className="airflow-alert-list">
          {rules.map((rule) => (
            <article key={rule.id}>
              <div>
                <Tag color={rule.enabled ? "blue" : "default"}>{rule.type_label}</Tag>
                <b>{rule.name}</b>
              </div>
              <p>
                {rule.enabled ? "사용" : "미사용"} · 심각도 {rule.severity}
                {rule.schedule_enabled && rule.schedule_time ? ` · 매일 ${rule.schedule_time.slice(0, 5)}` : ""}
              </p>
              {rule.last_eval_error && <small style={{ color: "#dc2626" }}>{rule.last_eval_error}</small>}
            </article>
          ))}
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
  pipeline,
  runtime,
  etlJobIds,
  alerts = [],
  inline = false,
  onOpenExecution,
  onOpenHistory,
  onChanged,
}: {
  dag?: DashboardDag;
  pipeline?: PipelineResponse;
  runtime?: PipelineRuntimeStatusResponse;
  etlJobIds?: number[];
  alerts?: AirflowDagAlert[];
  inline?: boolean;
  onOpenExecution?: () => void;
  onOpenHistory?: () => void;
  onChanged?: () => void;
}) {
  const [acknowledgingId, setAcknowledgingId] = useState<number>();

  const acknowledge = async (alert: AirflowDagAlert) => {
    setAcknowledgingId(alert.id);
    try {
      await acknowledgeAirflowDagAlert(alert.id);
      message.success("경보를 확인 처리했습니다.");
      onChanged?.();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "경보 확인 처리에 실패했습니다.");
    } finally {
      setAcknowledgingId(undefined);
    }
  };

  // CDC 파이프라인을 골랐을 때. CDC 관리 화면의 «기본정보»와 같은 항목을 보여준다.
  if (!inline && pipeline) {
    return (
      <aside className="airflow-dashboard-panel airflow-property-panel">
        <h3>속성</h3><h2>{pipeline.name}</h2>
        <section>
          <strong>기본정보</strong>
          <dl>
            <dt>이름</dt><dd>{pipeline.name}</dd>
            <dt>유형</dt><dd>{pipeline.pipelineType}</dd>
            <dt>소스</dt><dd>{sourceLabel(pipeline)}</dd>
            <dt>타겟</dt><dd>{targetLabel(pipeline)}</dd>
            <dt>Topic</dt><dd>{pipeline.topicName}</dd>
            {pipeline.pipelineType === "TABLE_CDC" && <>
              <dt>스냅샷 모드</dt>
              <dd>{pipeline.snapshotMode === "NO_DATA" ? "기존 데이터 미적재 · 이후 CDC" : "초기 적재 후 CDC"}</dd>
            </>}
            <dt>상태</dt><dd>{renderRuntimeStatus(pipeline, runtime)}</dd>
            <dt>설명</dt><dd>{pipeline.description ?? "-"}</dd>
            <dt>생성 시각</dt><dd>{pipeline.createdAt}</dd>
            <dt>수정 시각</dt><dd>{pipeline.updatedAt}</dd>
          </dl>
          <div className="airflow-schedule-actions">
            <Button type="primary" disabled={!dag} onClick={onOpenExecution}>실행 설정</Button>
            <Button disabled={!dag} onClick={onOpenHistory}>실행 이력</Button>
          </div>
        </section>
        <WatchingRules target="CDC" ids={[pipeline.id]} />
        <section>
          <strong>바로가기</strong>
          <div className="airflow-resource-links">
            {dag && <Link to={`/airflow/manage?dagId=${encodeURIComponent(dag.dag_id)}`}>상세보기</Link>}
            <Link to="/cdc/pipelines">CDC 파이프라인</Link>
          </div>
        </section>
      </aside>
    );
  }

  if (!dag) {
    return <aside className="airflow-dashboard-panel airflow-property-panel"><Empty description="작업을 선택하세요." /></aside>;
  }
  const category = categoryOf(dag);
  return (
    <aside className={`airflow-dashboard-panel airflow-property-panel${inline ? " airflow-property-panel--inline" : ""}`}>
      {!inline && <><h3>속성</h3><h2>{displayName(dag)}</h2></>}
      {!inline && <section>
        <strong>기본정보</strong>
        <dl>
          <dt>이름</dt><dd>{displayName(dag)}</dd>
          <dt>DAG ID</dt><dd>{dag.dag_id}</dd>
          <dt>업무 구분</dt><dd>{category}</dd>
          <dt>업무 폴더</dt><dd>{folderName(dag)}</dd>
          <dt>스케줄</dt><dd title={dag.timetable_summary}>{scheduleDescription(dag.timetable_summary)}</dd>
          <dt>상태</dt><dd>{dag.is_paused ? "일시 중지" : dag.is_active === false ? "비활성" : "활성"}</dd>
          <dt>설명</dt><dd>{dag.description || "-"}</dd>
        </dl>
        <div className="airflow-schedule-actions">
          <Button type="primary" onClick={onOpenExecution}>실행 설정</Button>
          <Button onClick={onOpenHistory}>실행 이력</Button>
        </div>
      </section>}
      {!inline && category === "ETL" && <WatchingRules target="ETL" ids={etlJobIds ?? []} />}
      {inline && <section>
        <strong>조치 필요 {alerts.length}건</strong>
        {alerts.length ? <div className="airflow-alert-list">{alerts.map((alert) => (
          <article key={alert.id}>
            <div><Tag color={alertColor(alert.severity)}>{ALERT_SEVERITY_LABEL[alert.severity]}</Tag><b>{ALERT_RULE_LABEL[alert.ruleType]}</b></div>
            <p>{alert.message}</p>
            <small>{dayjs(alert.lastDetectedAt).format("YYYY-MM-DD HH:mm:ss")}</small>
            <Button size="small" disabled={alert.status === "ACKNOWLEDGED"} loading={acknowledgingId === alert.id} onClick={() => void acknowledge(alert)}>{alert.status === "ACKNOWLEDGED" ? "확인됨" : "확인 처리"}</Button>
          </article>
        ))}</div> : <p>현재 감지된 이상이 없습니다.</p>}
      </section>}
      {!inline && <section><strong>바로가기</strong><div className="airflow-resource-links"><Link to={`/airflow/manage?dagId=${encodeURIComponent(dag.dag_id)}`}>상세보기</Link>{category === "ETL" ? <Link to={dag.nifi_process_group_id ? `/etl/manage?processGroupId=${encodeURIComponent(dag.nifi_process_group_id)}` : "/etl/manage"}>ETL 캔버스</Link> : <Link to="/cdc/pipelines">CDC 파이프라인</Link>}</div></section>}
    </aside>
  );
}

function ExecutionSettingsModal({
  open,
  dag,
  runs,
  onClose,
  onExecuted,
}: {
  open: boolean;
  dag?: DashboardDag;
  runs: AirflowDagRun[];
  onClose: () => void;
  onExecuted: () => void;
}) {
  const [action, setAction] = useState<ExecutionAction>("start");
  const [triggering, setTriggering] = useState(false);

  useEffect(() => {
    if (open) setAction("start");
  }, [dag?.dag_id, open]);

  // 감시 센서까지 도달하는 동작(start/monitor)의 Run이 살아 있으면 이미 감시 중이다.
  // deploy/stop은 센서가 즉시 통과하므로 감시 Run이 아니다.
  const watching = runs.some((run) => run.state === "running"
    && (run.conf?.action === "monitor" || run.conf?.action === "start"));

  const options = dag && categoryOf(dag) === "CDC"
    ? [
        { label: "배포 (Deploy)", value: "deploy" },
        { label: "시작 (Start)", value: "start" },
        { label: "중지 (Stop)", value: "stop" },
        // 이미 돌고 있는 CDC의 감시 Run만 잃었을 때. 커넥터에는 아무 지시도 하지 않는다.
        // 예전에는 이걸 되살리려면 stop -> start 밖에 없어서 Sink를 한 번 멈춰야 했다.
        // 감시 Run이 살아 있는데 또 누르면 같은 파이프라인을 감시하는 Run이 둘이 되고,
        // 그 둘이 max_active_runs=2를 다 차지해 정작 중지 지시가 막힌다. 그래서 잠근다.
        { label: "감시 재개 (Monitor)", value: "monitor", disabled: watching },
      ]
    : [
        { label: "시작 (Start)", value: "start" },
        { label: "중지 (Stop)", value: "stop" },
      ];

  const execute = async () => {
    if (!dag) return;
    if (action === "monitor" && watching) {
      message.warning("이미 감시 중인 실행이 있습니다.");
      return;
    }
    setTriggering(true);
    try {
      await triggerAirflowDag(dag.dag_id, { action });
      message.success(`${displayName(dag)} ${options.find((option) => option.value === action)?.label} 요청을 전송했습니다.`);
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
        <Button type="primary" loading={triggering} onClick={() => void execute()}>실행</Button>
        <Button disabled={triggering} onClick={onClose}>취소</Button>
      </>}
      destroyOnHidden
    >
      <p>Airflow DAG에 전달할 실행 동작을 선택하세요.</p>
      {watching && (
        <p style={{ color: "#888", fontSize: 12 }}>
          이미 감시 중인 실행이 있어 «감시 재개»는 선택할 수 없습니다.
        </p>
      )}
      <Radio.Group
        block
        optionType="button"
        buttonStyle="solid"
        value={action}
        options={options}
        onChange={(event) => setAction(event.target.value as ExecutionAction)}
      />
    </Modal>
  );
}

const PROCESS_STATUS: Record<ProcessStatus, { label: string; className: string }> = {
  UP: { label: "정상", className: "ok" },
  STOPPED: { label: "미사용", className: "unused" },
  DEGRADED: { label: "경고", className: "warn" },
  DOWN: { label: "이상", className: "danger" },
  UNKNOWN: { label: "확인 불가", className: "unused" },
};

function InfrastructureBar({ groups }: { groups: ProcessGroup[] }) {
  const airflow = groups.find((group) => group.key.toLowerCase().includes("airflow") || group.label.toLowerCase().includes("airflow"));
  return (
    <footer className="airflow-infra-bar">
      <strong>인프라 상태</strong>
      {airflow?.processes.length ? airflow.processes.map((process) => {
        const status = PROCESS_STATUS[process.status];
        return <span key={process.name}><i className={status.className} />{process.name} {status.label}</span>;
      }) : <span><i className="unused" />Airflow 상태 확인 불가</span>}
    </footer>
  );
}

export function AirflowDashboardPage() {
  const [searchParams] = useSearchParams();
  const initialDagId = searchParams.get("dagId") ?? undefined;
  const openInitialDetail = searchParams.get("detail") === "1";
  const [refreshSeconds, setRefreshSeconds] = useState(30);
  const [selectedDagId, setSelectedDagId] = useState<string | undefined>(initialDagId);
  const [search, setSearch] = useState("");
  // 처음 열면 무엇을 보여줄지. 마지막으로 고른 DAG가 덩그러니 뜨는 것보다
  // "CDC 전체"에서 시작하는 편이 화면의 성격에 맞는다.
  const [scope, setScope] = useState<ListScope | undefined>(
    initialDagId ? { kind: "dag", dagId: initialDagId } : { kind: "cdc", scope: "all" });
  const [selectedPipelineId, setSelectedPipelineId] = useState<number>();
  // 실행 이력(하단 task 표)을 펼쳐 둔 DAG. 같은 DAG를 다시 누르면 접히고,
  // 다른 것을 고르면(그룹·지표·다른 DAG·CDC) 닫힌다 - 이전 선택의 이력이 남아 있으면
  // 지금 보고 있는 게 무엇인지 헷갈린다.
  const [expandedDagId, setExpandedDagId] = useState<string>();
  const [historyOpen, setHistoryOpen] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [executionOpen, setExecutionOpen] = useState(false);
  const [synchronizing, setSynchronizing] = useState(false);
  const initialSyncQuery = useQuery({
    queryKey: ["airflow-dag-catalog-sync"],
    queryFn: syncAirflowDagCatalog,
    retry: 1,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  });
  const dashboardQuery = useQuery({
    queryKey: ["airflow-dashboard"],
    queryFn: loadDashboard,
    refetchInterval: refreshSeconds * 1000,
    enabled: initialSyncQuery.isFetched,
  });
  // ETL 트리를 NiFi 그룹 계층으로 보여주기 위해(ETL 관리 화면과 동일한 구조)
  const groupTreeQuery = useQuery({ queryKey: ["nifi-pg-tree"], queryFn: getNifiProcessGroupTree });
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
  // CDC 가지는 CDC 관리 화면과 같은 원본을 읽는다(파이프라인·연결·커넥터 상태·지연).
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
  const runsByDag = useMemo(() => groupRuns(dashboardQuery.data?.runs ?? []), [dashboardQuery.data?.runs]);
  const alertsByDag = useMemo(() => {
    const grouped = new Map<string, AirflowDagAlert[]>();
    (dashboardQuery.data?.alerts ?? []).forEach((alert) => grouped.set(alert.dagId, [...(grouped.get(alert.dagId) ?? []), alert]));
    return grouped;
  }, [dashboardQuery.data?.alerts]);
  const businessDags = useMemo(
    () => (dashboardQuery.data?.dags ?? []).filter((dag) => categoryOf(dag) !== "기타"),
    [dashboardQuery.data?.dags],
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
    if (initialSyncQuery.isError) {
      message.warning("DAG 자동 동기화에 실패해 기존 DB 정보로 대시보드를 표시합니다.");
    }
  }, [initialSyncQuery.isError]);

  const pipelines = pipelinesQuery.data ?? [];
  const connectionNames = new Map((connectionsQuery.data ?? []).map((c) => [c.id, c.name] as const));
  const runtimeByPipeline = new Map((runtimeQuery.data ?? []).map((r) => [r.pipelineId, r] as const));
  const metricByPipeline = new Map((cdcMetricsQuery.data ?? []).map((m) => [m.pipelineId, m] as const));
  const selectedPipeline = pipelines.find((pipeline) => pipeline.id === selectedPipelineId);
  // CDC 파이프라인 하나가 DAG 하나에 대응한다(kafka_pipeline_{id}_control).
  const pipelineDag = selectedPipeline
    ? businessDags.find((dag) => dag.dag_id === `kafka_pipeline_${selectedPipeline.id}_control`)
    : undefined;

  // ETL 그룹 -> 그 그룹(하위 포함)에 속한 워크플로우 DAG id.
  const workflowsByGroup = new Map<string, Set<string>>();
  (workflowsQuery.data ?? []).forEach((workflow) => {
    if (!workflow.nifiGroupPgId) {
      return;
    }
    const bucket = workflowsByGroup.get(workflow.nifiGroupPgId) ?? new Set<string>();
    bucket.add(workflow.dagId);
    workflowsByGroup.set(workflow.nifiGroupPgId, bucket);
  });
  // 상위 그룹을 고르면 하위 그룹 워크플로우까지 함께 보여준다.
  const rollUp = (node: NifiProcessGroupTreeNode): Set<string> => {
    const own = new Set(workflowsByGroup.get(node.id) ?? []);
    (node.children ?? []).forEach((child) => rollUp(child).forEach((id) => own.add(id)));
    workflowsByGroup.set(node.id, own);
    return own;
  };
  if (groupTreeQuery.data) {
    rollUp(groupTreeQuery.data);
  }

  /** DAG를 고른다. 같은 DAG를 다시 고르면 이력이 접힌다. */
  const selectDag = (dagId: string) => {
    setSelectedDagId(dagId);
    setSelectedPipelineId(undefined);
    setExpandedDagId((current) => (current === dagId ? undefined : dagId));
  };

  /** 트리·지표에서 목록 범위를 바꾼다. DAG를 고른 게 아니면 열려 있던 이력을 닫는다. */
  const changeScope = (next: ListScope | undefined) => {
    setScope(next);
    if (next?.kind !== "dag") {
      setExpandedDagId(undefined);
    }
    if (next?.kind === "cdc" && next.scope === "pipeline") {
      setSelectedPipelineId(next.pipelineId);
      setSelectedDagId(undefined);
    }
  };

  const selectedDag = businessDags.find((dag) => dag.dag_id === selectedDagId);

  /** 가운데 목록의 제목. 지금 무엇을 보고 있는지 한 줄로 말해준다. */
  const scopeTitle = !scope
    ? (selectedDag ? displayName(selectedDag) : "선택 작업")
    : scope.kind === "cdc"
      ? scope.scope === "all" ? "전체 파이프라인"
        : scope.scope === "connection" ? `${connectionNames.get(scope.connectionId) ?? "연결"} 파이프라인`
        : scope.scope === "schema" ? `${scope.schema} 파이프라인`
        : scope.scope === "logfile" ? "로그파일 파이프라인"
        : selectedPipeline?.name ?? "파이프라인"
      : scope.kind === "etl" ? `${scope.groupName} 워크플로우`
      : scope.kind === "dag" ? (selectedDag ? displayName(selectedDag) : scope.dagId)
      : `${scope.category} · ${STATUS_METRIC_LABEL[scope.metric]}`;

  /** 선택한 ETL 워크플로우가 참조하는 job id들. 감시 중인 알림 규칙을 찾는 열쇠다. */
  const etlJobIdsOfSelected = selectedDag
    ? (workflowDetailQuery.data?.nodes ?? [])
        .map((node) => node.jobId)
        .filter((id): id is number => typeof id === "number")
    : [];
  const selectedRuns = selectedDagId ? (runsByDag.get(selectedDagId) ?? []) : [];
  const selectedAlerts = selectedDagId ? (alertsByDag.get(selectedDagId) ?? []) : [];
  const historyDagId = selectedPipeline ? pipelineDag?.dag_id : selectedDagId;
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
  const confirmDeleteDag = (dag: DashboardDag) => {
    Modal.confirm({
      title: `${displayName(dag)} 삭제`,
      content: "원본 파이프라인과 Airflow DAG, 실행 이력 및 대시보드 등록정보가 함께 삭제됩니다.",
      okText: "삭제",
      cancelText: "취소",
      okButtonProps: { danger: true },
      onOk: async () => {
        await deleteAirflowDagCatalog(dag.dag_id);
        message.success(`${displayName(dag)}을(를) 삭제했습니다.`);
        setDetailOpen(false);
        setSelectedDagId(undefined);
        await dashboardQuery.refetch();
      },
    });
  };

  const initialLoading = !initialSyncQuery.isFetched || dashboardQuery.isLoading;

  return (
    <div className="airflow-dashboard-page">
      <header className="airflow-dashboard-heading">
        <div><h1>실시간 모니터링</h1><p>작업 상태와 실행 이력을 한 화면에서 확인하고 조치합니다.</p></div>
        <div className="airflow-refresh-controls"><Button icon={<SyncOutlined />} loading={synchronizing || initialSyncQuery.isFetching} onClick={() => void synchronizeDags()}>동기화</Button><span>갱신 주기</span><Select value={refreshSeconds} options={REFRESH_OPTIONS} onChange={setRefreshSeconds} /><span>마지막 갱신 {dashboardQuery.dataUpdatedAt ? dayjs(dashboardQuery.dataUpdatedAt).format("HH:mm:ss") : "-"}</span></div>
      </header>

      {initialLoading ? <div className="airflow-dashboard-loading"><Spin size="large" /><span>{!initialSyncQuery.isFetched ? "DAG 동기화 중..." : "대시보드 로딩 중..."}</span></div> : dashboardQuery.isError ? <Empty description="Airflow 현황을 불러올 수 없습니다." /> : (
        <>
          <div className="airflow-business-status">
            {CATEGORY_ORDER.map((category) => (
              <StatusCard
                key={category}
                category={category}
                dags={businessDags.filter((dag) => categoryOf(dag) === category)}
                runsByDag={runsByDag}
                activeMetric={scope?.kind === "metric" && scope.category === category ? scope.metric : undefined}
                onMetricClick={(metric) => changeScope(
                  scope?.kind === "metric" && scope.category === category && scope.metric === metric
                    ? { kind: "cdc", scope: "all" }
                    : { kind: "metric", category, metric })}
              />
            ))}
          </div>
          <div className="airflow-dashboard-workspace">
            <BusinessTree dags={filteredDags} groupTree={groupTreeQuery.data} workflows={workflowsQuery.data ?? []}
                          pipelines={pipelines} connectionNames={connectionNames} selectedPipelineId={selectedPipelineId}
                          scope={scope} onScope={changeScope}
                          selectedDagId={selectedDagId} search={search} alertsByDag={alertsByDag} onSearch={setSearch}
                          onSelect={selectDag} onDetail={showDagDetail} onExecution={showExecutionSettings} onDelete={confirmDeleteDag} />
            <main className="airflow-dashboard-panel airflow-jobs-panel">
              <div className="airflow-jobs-heading">
                <div>
                  <h3>{scopeTitle}</h3>
                </div>
              </div>
              {scope
                ? <ScopeList scope={scope} dags={businessDags} runsByDag={runsByDag} pipelines={pipelines}
                             runtimeByPipeline={runtimeByPipeline} metricByPipeline={metricByPipeline}
                             workflowsByGroup={workflowsByGroup} selectedDagId={selectedDagId}
                             selectedPipelineId={selectedPipelineId}
                             onSelectDag={selectDag}
                             onSelectPipeline={(id) => {
                               setSelectedPipelineId(id);
                               setSelectedDagId(undefined);
                               setExpandedDagId(undefined);
                             }} />
                : <JobsTable dags={selectedDag ? [selectedDag] : []} runsByDag={runsByDag} alertsByDag={alertsByDag} selectedDagId={selectedDagId} refreshSeconds={refreshSeconds} onSelect={setSelectedDagId} />}
              {/* ETL은 각 task(job)이 중요하다. DAG를 고르면 최근 실행의 task를 바로 펼친다. */}
              {selectedDag && expandedDagId === selectedDag.dag_id && categoryOf(selectedDag) === "ETL" && (
                <div className="airflow-job-table-wrap" style={{ marginTop: 12 }}>
                  <table className="airflow-job-table">
                    <thead><tr><th>작업(Task)</th><th>유형</th><th>시작</th><th>상태</th><th>적재/실패 사유</th><th>소요</th><th>스케줄</th><th>재실행</th></tr></thead>
                    <tbody>
                      <TaskRows dag={selectedDag} run={selectedRuns[0]} refreshSeconds={refreshSeconds} />
                    </tbody>
                  </table>
                </div>
              )}
            </main>
            <PropertyPanel dag={selectedPipeline ? pipelineDag : selectedDag} pipeline={selectedPipeline}
                           runtime={selectedPipeline ? runtimeByPipeline.get(selectedPipeline.id) : undefined}
                           etlJobIds={etlJobIdsOfSelected} alerts={selectedAlerts}
                           onOpenExecution={() => setExecutionOpen(true)}
                           onOpenHistory={() => setHistoryOpen(true)}
                           onChanged={() => void dashboardQuery.refetch()} />
          </div>
          <InfrastructureBar groups={dashboardQuery.data?.processGroups ?? []} />
          <ExecutionSettingsModal open={executionOpen} dag={selectedDag}
                                  runs={selectedDag ? runsByDag.get(selectedDag.dag_id) ?? [] : []}
                                  onClose={() => setExecutionOpen(false)} onExecuted={() => { setHistoryOpen(true); void dashboardQuery.refetch(); }} />
          <RunHistoryModal open={historyOpen} dag={selectedPipeline ? pipelineDag : selectedDag}
                           runs={historyRuns} refreshSeconds={refreshSeconds} onClose={() => setHistoryOpen(false)} />
          <Modal title="DAG 상세" open={detailOpen} footer={null} onCancel={() => setDetailOpen(false)}>
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
  );
}

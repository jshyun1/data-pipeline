import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Descriptions, Dropdown, Empty, Input, InputNumber, message, Modal, Radio, Select, Spin, Switch, Tag } from "antd";
import {
  ApartmentOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  DeploymentUnitOutlined,
  DeleteOutlined,
  DownOutlined,
  FolderOpenOutlined,
  FolderOutlined,
  InfoCircleOutlined,
  ProfileOutlined,
  ReloadOutlined,
  RightOutlined,
  SyncOutlined,
} from "@ant-design/icons";
import dayjs from "dayjs";
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
  retryAirflowTaskFrom,
  saveAirflowDagSchedule,
  saveAirflowMonitoringSettings,
  syncAirflowDagCatalog,
  triggerAirflowDag,
  type AirflowDag,
  type AirflowDagAlert,
  type AirflowDagRun,
} from "../api/platform";
import { getProcessHealth, type ProcessGroup, type ProcessStatus } from "../api/infra";
import { categorizeDag, type DagCategory } from "../utils/dagHistory";

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

type DetailTab = "tasks" | "history";
type ScheduleMode = "manual" | "recurring";
type SchedulePreset = "hourly" | "daily" | "weekly";

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

function groupDagsByFolder(dags: DashboardDag[]) {
  const folders = new Map<string, DashboardDag[]>();
  dags.forEach((dag) => {
    const name = folderName(dag);
    folders.set(name, [...(folders.get(name) ?? []), dag]);
  });
  return folders;
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
  const [airflowDags, catalog, runs, alerts, alertHistory, processHealth] = await Promise.all([
    listAirflowDags(),
    listAirflowDagCatalog(),
    listAllAirflowDagRuns({ maxRuns: 2000 }),
    listAirflowDagAlerts(),
    listAirflowDagAlertHistory(),
    getProcessHealth().catch(() => ({ collectedAt: "", groups: [] })),
  ]);
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
        duration_multiplier: entry.durationMultiplier,
        sla_minutes: entry.slaMinutes,
      };
    });
  const discoveredDags = airflowDags
    .filter((dag) => !catalogDagIds.has(dag.dag_id))
    .map((dag) => ({ ...dag, business_category: categorizeDag(dag.dag_id), business_folder: "NiFi" } as DashboardDag));
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

function StatusCard({
  category,
  dags,
  runsByDag,
  alerts,
  onActionClick,
}: {
  category: BusinessCategory;
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
  alerts: AirflowDagAlert[];
  onActionClick: () => void;
}) {
  const dagIds = new Set(dags.map((dag) => dag.dag_id));
  const today = dayjs().startOf("day");
  const runs = [...runsByDag.values()].flat().filter((run) => run.dag_id && dagIds.has(run.dag_id));
  const active = runs.filter((run) => run.state === "running" || run.state === "queued").length;
  const todayRuns = runs.filter((run) => {
    const startedAt = runAt(run);
    return Boolean(startedAt && dayjs(startedAt).isAfter(today));
  });
  const success = todayRuns.filter((run) => run.state === "success").length;
  const failed = todayRuns.filter((run) => run.state === "failed").length;
  const categoryAlerts = alerts.filter((alert) => dagIds.has(alert.dagId));
  const actionCount = categoryAlerts.length;
  const severityCounts = Object.fromEntries(["DANGER", "WARNING", "INFO"].map((severity) => [
    severity,
    categoryAlerts.filter((alert) => alert.severity === severity).length,
  ]));

  return (
    <section className="airflow-business-card">
      <strong className="airflow-business-name">{category}</strong>
      <div className="airflow-business-metric">
        <ApartmentOutlined />
        <span>전체 작업<strong>{dags.length}</strong></span>
      </div>
      <div className="airflow-business-metric">
        <RightOutlined />
        <span>실행 중<strong>{active}</strong></span>
      </div>
      <div className="airflow-business-metric airflow-business-metric--success">
        <CheckCircleFilled />
        <span>오늘 성공<strong>{success}</strong></span>
      </div>
      <div className="airflow-business-metric airflow-business-metric--danger">
        <CloseCircleFilled />
        <span>실패<strong>{failed}</strong></span>
      </div>
      <Button danger={actionCount > 0} disabled={actionCount === 0} onClick={onActionClick}>
        조치 필요 {actionCount}건{actionCount > 0 ? ` · 위험 ${severityCounts.DANGER} · 경고 ${severityCounts.WARNING} · 정보 ${severityCounts.INFO}` : ""}
      </Button>
    </section>
  );
}

function BusinessTree({
  dags,
  selectedDagId,
  search,
  alertsByDag,
  onSearch,
  onSelect,
  onDetail,
  onDelete,
}: {
  dags: DashboardDag[];
  selectedDagId?: string;
  search: string;
  alertsByDag: Map<string, AirflowDagAlert[]>;
  onSearch: (value: string) => void;
  onSelect: (dagId: string) => void;
  onDetail: (dag: DashboardDag) => void;
  onDelete: (dag: DashboardDag) => void;
}) {
  const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(new Set());
  const toggleNode = (node: string) => {
    setCollapsedNodes((current) => {
      const next = new Set(current);
      if (next.has(node)) next.delete(node);
      else next.add(node);
      return next;
    });
  };

  return (
    <aside className="airflow-dashboard-panel airflow-business-tree">
      <h3>업무별 트리</h3>
      <Input.Search allowClear placeholder="검색" value={search} onChange={(event) => onSearch(event.target.value)} />
      <div className="airflow-tree-body">
        {CATEGORY_ORDER.map((category) => {
          const categoryDags = dags.filter((dag) => categoryOf(dag) === category);
          const folders = groupDagsByFolder(categoryDags);
          const categoryNode = `category:${category}`;
          const categoryCollapsed = collapsedNodes.has(categoryNode);
          return (
            <section key={category} className="airflow-tree-category">
              <button type="button" className="airflow-tree-label" aria-expanded={!categoryCollapsed} onClick={() => toggleNode(categoryNode)}>
                {categoryCollapsed ? <RightOutlined /> : <DownOutlined />}
                {categoryCollapsed ? <FolderOutlined /> : <FolderOpenOutlined />}
                {category}
              </button>
              {!categoryCollapsed && [...folders.entries()].map(([folder, folderDags]) => {
                const folderNode = `folder:${category}:${folder}`;
                const folderCollapsed = collapsedNodes.has(folderNode);
                return (
                  <div key={folder} className="airflow-tree-folder">
                    <button type="button" className="airflow-tree-label" aria-expanded={!folderCollapsed} onClick={() => toggleNode(folderNode)}>
                      {folderCollapsed ? <RightOutlined /> : <DownOutlined />}
                      {folderCollapsed ? <FolderOutlined /> : <FolderOpenOutlined />}
                      {folder}
                    </button>
                    {!folderCollapsed && folderDags.map((dag) => (
                      <Dropdown
                        key={dag.dag_id}
                        trigger={["contextMenu"]}
                        menu={{
                          items: [
                            { key: "detail", icon: <InfoCircleOutlined />, label: "상세" },
                            { key: "delete", icon: <DeleteOutlined />, label: "삭제", danger: true },
                          ],
                          onClick: ({ key }) => key === "detail" ? onDetail(dag) : onDelete(dag),
                        }}
                      >
                        <button
                          type="button"
                          className={selectedDagId === dag.dag_id ? "airflow-tree-dag active" : "airflow-tree-dag"}
                          onClick={() => onSelect(dag.dag_id)}
                          onContextMenu={() => onSelect(dag.dag_id)}
                        >
                          <DagIcon />
                          <span>{displayName(dag)}</span>
                          {alertsByDag.has(dag.dag_id) && <Tag color="error">조치</Tag>}
                        </button>
                      </Dropdown>
                    ))}
                  </div>
                );
              })}
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
}: {
  dag: DashboardDag;
  run?: AirflowDagRun;
  refreshSeconds: number;
}) {
  const [retryingTaskId, setRetryingTaskId] = useState<string>();
  const tasksQuery = useQuery({
    queryKey: ["airflow-dashboard-tasks", dag.dag_id, run?.dag_run_id],
    queryFn: () => listAirflowTaskInstances(dag.dag_id, run!.dag_run_id),
    enabled: Boolean(run),
    refetchInterval: refreshSeconds * 1000,
  });
  const completedTasks = (tasksQuery.data ?? []).filter((task) => task.state === "success" || task.state === "failed");
  const logsQuery = useQuery({
    queryKey: ["airflow-dashboard-task-logs", dag.dag_id, run?.dag_run_id, completedTasks.map((task) => `${task.task_id}:${task.try_number}`).join(",")],
    queryFn: async () => Object.fromEntries(await Promise.all(completedTasks.map(async (task): Promise<[string, string]> => [
      task.task_id,
      await getAirflowTaskLog(dag.dag_id, run!.dag_run_id, task.task_id, task.try_number || 1).catch(() => ""),
    ]))),
    enabled: Boolean(run && completedTasks.length),
    staleTime: Infinity,
  });

  const outcomeDetail = (taskId: string, state?: string) => {
    const log = logsQuery.data?.[taskId] ?? "";
    if (state === "success") {
      const counts = [...log.matchAll(/([\d,]+)\s*건/g)];
      return `${counts.at(-1)?.[1] ?? "0"}건`;
    }
    if (state === "upstream_failed") return "선행 작업 실패로 미실행";
    if (state !== "failed") return "-";
    const lines = log.split("\n").map((line) => line.trim()).filter(Boolean);
    const reason = [...lines].reverse().find((line) => /(?:ORA-\d+|(?:Error|Exception):|실패)/i.test(line)) ?? lines.at(-1);
    return reason?.replace(/^\[[^\]]+\]\s*/, "").slice(0, 180) || "실패 사유를 확인할 수 없습니다.";
  };
  const retryFrom = async (taskId: string) => {
    if (!run) return;
    setRetryingTaskId(taskId);
    try {
      await retryAirflowTaskFrom(dag.dag_id, run.dag_run_id, taskId);
      message.success(`${taskId}부터 재시작했습니다.`);
      await tasksQuery.refetch();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "실패 작업 재시작에 실패했습니다.");
    } finally {
      setRetryingTaskId(undefined);
    }
  };

  if (tasksQuery.isLoading) {
    return <tr className="airflow-task-loading"><td colSpan={8}><Spin size="small" /> 실행 단계를 불러오는 중입니다.</td></tr>;
  }
  if (!run || !tasksQuery.data?.length) {
    return <tr className="airflow-task-loading"><td colSpan={8}>실행 단계가 없습니다.</td></tr>;
  }
  return tasksQuery.data.map((task, index) => {
    const detail = outcomeDetail(task.task_id, task.state);
    const retryCount = Math.max(0, (task.try_number ?? 1) - 1);
    return <tr key={task.task_id} className="airflow-task-row">
      <td>
        <span className="airflow-task-name">{String(index + 1).padStart(2, "0")} {task.task_id}</span>
      </td>
      <td><ProfileOutlined /> TASK</td>
      <td>{task.start_date ? dayjs(task.start_date).format("YYYY-MM-DD HH:mm") : "-"}</td>
      <td><Tag color={stateColor(task.state)}>{stateLabel(task.state)}</Tag></td>
      <td>{task.state === "failed" ? <small title={detail}>{detail}</small> : detail}</td>
      <td>{duration(task.start_date, task.end_date)}</td>
      <td>-</td>
      <td>
        {retryCount}회{" "}
        <Button
          type={task.state === "failed" ? "primary" : "default"}
          size="small"
          loading={retryingTaskId === task.task_id}
          disabled={Boolean(retryingTaskId && retryingTaskId !== task.task_id)}
          onClick={() => void retryFrom(task.task_id)}
        >
          이 지점부터 재시작
        </Button>
      </td>
    </tr>;
  });
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
                  ...(selected ? [<TaskRows key={`${dag.dag_id}-tasks`} dag={dag} run={latest} refreshSeconds={refreshSeconds} />] : []),
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

function AlertHistory({ alerts }: { alerts: AirflowDagAlert[] }) {
  if (!alerts.length) return null;
  return (
    <section className="airflow-alert-history">
      <strong>감지 이력</strong>
      <table className="airflow-job-table">
        <thead><tr><th>감지 시각</th><th>규칙</th><th>심각도</th><th>상태</th><th>감지 근거</th><th>해소 시각</th></tr></thead>
        <tbody>{alerts.map((alert) => (
          <tr key={alert.id}>
            <td>{dayjs(alert.detectedAt).format("YYYY-MM-DD HH:mm:ss")}</td>
            <td>{ALERT_RULE_LABEL[alert.ruleType]}</td>
            <td><Tag color={alertColor(alert.severity)}>{ALERT_SEVERITY_LABEL[alert.severity]}</Tag></td>
            <td>{alert.status === "OPEN" ? "조치 필요" : alert.status === "ACKNOWLEDGED" ? "확인" : "해소"}</td>
            <td>{alert.message}</td>
            <td>{alert.resolvedAt ? dayjs(alert.resolvedAt).format("YYYY-MM-DD HH:mm:ss") : "-"}</td>
          </tr>
        ))}</tbody>
      </table>
    </section>
  );
}

function presetCron(preset: SchedulePreset, hour: number, minute: number, weekday: number) {
  if (preset === "hourly") return `${minute} * * * *`;
  if (preset === "weekly") return `${minute} ${hour} * * ${weekday}`;
  return `${minute} ${hour} * * *`;
}

function presetDescription(preset: SchedulePreset, hour: number, minute: number, weekday: number) {
  const time = `${String(hour).padStart(2, "0")}시 ${String(minute).padStart(2, "0")}분`;
  if (preset === "hourly") return `매시간 ${String(minute).padStart(2, "0")}분`;
  if (preset === "weekly") return `매주 ${["일", "월", "화", "수", "목", "금", "토"][weekday]}요일 ${time}`;
  return `매일 ${time}`;
}

function nextPresetRuns(preset: SchedulePreset, hour: number, minute: number, weekday: number) {
  const now = dayjs();
  let next = now.second(0).millisecond(0);
  if (preset === "hourly") {
    next = next.minute(minute);
    if (!next.isAfter(now)) next = next.add(1, "hour");
    return Array.from({ length: 5 }, (_, index) => next.add(index, "hour"));
  }
  next = next.hour(hour).minute(minute);
  if (preset === "daily") {
    if (!next.isAfter(now)) next = next.add(1, "day");
    return Array.from({ length: 5 }, (_, index) => next.add(index, "day"));
  }
  next = next.day(weekday);
  if (!next.isAfter(now)) next = next.add(1, "week");
  return Array.from({ length: 5 }, (_, index) => next.add(index, "week"));
}

function validCronField(field: string, min: number, max: number) {
  return field.split(",").every((part) => {
    const match = part.match(/^(\*|\d+|\d+-\d+)(?:\/(\d+))?$/);
    if (!match || (match[2] && Number(match[2]) < 1)) return false;
    if (match[1] === "*") return true;
    const values = match[1].split("-").map(Number);
    return values.every((value) => value >= min && value <= max) && (values.length === 1 || values[0] <= values[1]);
  });
}

function validCron(cron: string) {
  const ranges = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 7]] as const;
  const fields = cron.trim().split(/\s+/);
  return fields.length === 5 && fields.every((field, index) => {
    const [min, max] = ranges[index];
    return validCronField(field, min, max);
  });
}

function scheduleDescription(cron?: string) {
  if (!cron) return "수동 실행 전용";
  const [minute, hour, day, month, weekday, ...rest] = cron.trim().split(/\s+/);
  if (rest.length || month !== "*" || !/^\d+$/.test(minute) || !/^\d+$/.test(hour)) return cron;
  const time = `${Number(hour)}:${minute.padStart(2, "0")}`;
  if (/^\d+$/.test(day) && weekday === "*") return `매월 ${Number(day)}일 ${time}`;
  if (day === "*" && weekday === "*") return `매일 ${time}`;
  if (day === "*" && /^[0-7]$/.test(weekday)) {
    return `매주 ${["일", "월", "화", "수", "목", "금", "토", "일"][Number(weekday)]}요일 ${time}`;
  }
  return cron;
}

function ScheduleWizard({
  open,
  dag,
  onClose,
  onSaved,
}: {
  open: boolean;
  dag?: DashboardDag;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [mode, setMode] = useState<ScheduleMode>("manual");
  const [preset, setPreset] = useState<SchedulePreset>("daily");
  const [hour, setHour] = useState(12);
  const [minute, setMinute] = useState(56);
  const [weekday, setWeekday] = useState(1);
  const [customCron, setCustomCron] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) {
      setMode(dag?.timetable_summary ? "recurring" : "manual");
      setPreset("daily");
      setHour(12);
      setMinute(56);
      setWeekday(1);
      setCustomCron(dag?.timetable_summary ?? "");
    }
  }, [dag?.dag_id, dag?.timetable_summary, open]);

  const generatedCron = presetCron(preset, hour, minute, weekday);
  const cron = customCron.trim() || generatedCron;
  const upcoming = customCron.trim() ? [] : nextPresetRuns(preset, hour, minute, weekday);
  const save = async () => {
    if (!dag) return;
    if (mode === "recurring" && !validCron(cron)) {
      message.error("올바른 5개 항목 크론 표현식을 입력해 주세요.");
      return;
    }
    setSaving(true);
    try {
      await saveAirflowDagSchedule(dag.dag_id, mode === "recurring" ? cron : undefined);
      message.success(mode === "recurring" ? "스케줄을 저장했습니다." : "수동 실행 전용으로 변경했습니다.");
      onSaved();
      onClose();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "스케줄 저장에 실패했습니다.");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal title={`${dag ? displayName(dag) : "DAG"} 실행 주기`} open={open} onCancel={onClose} onOk={save} okText="저장" cancelText="취소" confirmLoading={saving} width={680} destroyOnHidden>
      <div className="airflow-schedule-wizard">
        <Radio.Group value={mode} onChange={(event) => setMode(event.target.value)}>
          <Radio value="manual">수동 실행 전용</Radio>
          <Radio value="recurring">정기 실행</Radio>
        </Radio.Group>
        {mode === "recurring" && (
          <>
            <div className="airflow-schedule-row">
              <span>프리셋</span>
              <Select value={preset} onChange={setPreset} options={[{ label: "매시간", value: "hourly" }, { label: "매일", value: "daily" }, { label: "매주", value: "weekly" }]} />
              {preset === "weekly" && <Select value={weekday} onChange={setWeekday} options={["일", "월", "화", "수", "목", "금", "토"].map((label, value) => ({ label: `${label}요일`, value }))} />}
              {preset !== "hourly" && <><span>시각</span><InputNumber min={0} max={23} value={hour} onChange={(value) => setHour(value ?? 0)} /></>}
              <InputNumber min={0} max={59} value={minute} onChange={(value) => setMinute(value ?? 0)} addonAfter="분" />
            </div>
            <div className="airflow-schedule-row"><span>또는 직접 입력</span><Input value={customCron} onChange={(event) => setCustomCron(event.target.value)} placeholder={generatedCron} /></div>
            <div className="airflow-schedule-preview">▶ {customCron.trim() ? `${cron} (Asia/Seoul)` : `${presetDescription(preset, hour, minute, weekday)} (Asia/Seoul)`}</div>
            <div className="airflow-schedule-next"><strong>다음 5회 실행</strong>{upcoming.length ? upcoming.map((date) => <span key={date.valueOf()}>{date.format("YYYY-MM-DD HH:mm")}</span>) : <span>직접 입력한 일정은 저장 후 Airflow에서 계산됩니다.</span>}</div>
            <p className="airflow-schedule-note">저장 후 Airflow DAG 재파싱에 최대 5분이 걸릴 수 있습니다.</p>
          </>
        )}
      </div>
    </Modal>
  );
}

function MonitoringSettingsModal({
  open,
  dag,
  onClose,
  onSaved,
}: {
  open: boolean;
  dag?: DashboardDag;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [monitoringEnabled, setMonitoringEnabled] = useState(true);
  const [failureThreshold, setFailureThreshold] = useState(3);
  const [staleDays, setStaleDays] = useState(7);
  const [durationMultiplier, setDurationMultiplier] = useState(3);
  const [slaMinutes, setSlaMinutes] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setMonitoringEnabled(dag?.monitoring_enabled ?? true);
    setFailureThreshold(dag?.consecutive_failure_threshold ?? 3);
    setStaleDays(dag?.stale_days_threshold ?? 7);
    setDurationMultiplier(dag?.duration_multiplier ?? 3);
    setSlaMinutes(dag?.sla_minutes ?? null);
  }, [dag, open]);

  const save = async () => {
    if (!dag || dag.monitoring_enabled === undefined) return;
    setSaving(true);
    try {
      await saveAirflowMonitoringSettings(dag.dag_id, {
        monitoringEnabled,
        consecutiveFailureThreshold: failureThreshold,
        staleDaysThreshold: staleDays,
        durationMultiplier,
        slaMinutes,
      });
      message.success("감지 설정을 저장했습니다.");
      onSaved();
      onClose();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "감지 설정 저장에 실패했습니다.");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal title={`${dag ? displayName(dag) : "DAG"} 이상 감지 설정`} open={open} onCancel={onClose} onOk={save} okText="저장" cancelText="취소" confirmLoading={saving} width={480} destroyOnHidden>
      <div className="airflow-monitoring-form">
        <label><span>감지 사용</span><Switch checked={monitoringEnabled} onChange={setMonitoringEnabled} /></label>
        <label><span>연속 실패</span><InputNumber min={1} value={failureThreshold} onChange={(value) => setFailureThreshold(value ?? 1)} addonAfter="회" /></label>
        <label><span>미실행 정체</span><InputNumber min={1} value={staleDays} onChange={(value) => setStaleDays(value ?? 1)} addonAfter="일" /></label>
        <label><span>실행시간 이상</span><InputNumber min={1} step={0.1} value={durationMultiplier} onChange={(value) => setDurationMultiplier(value ?? 1)} addonAfter="배" /></label>
        <label><span>SLA</span><InputNumber min={1} value={slaMinutes} onChange={(value) => setSlaMinutes(value)} placeholder="미설정" addonAfter="분" /></label>
      </div>
    </Modal>
  );
}

function PropertyPanel({
  dag,
  alerts = [],
  inline = false,
  triggering = false,
  onManualRun,
  onOpenSchedule,
  onOpenMonitoring,
  onChanged,
}: {
  dag?: DashboardDag;
  alerts?: AirflowDagAlert[];
  inline?: boolean;
  triggering?: boolean;
  onManualRun?: () => void;
  onOpenSchedule?: () => void;
  onOpenMonitoring?: () => void;
  onChanged?: () => void;
}) {
  const [acknowledgingId, setAcknowledgingId] = useState<number>();

  if (!dag) {
    return <aside className="airflow-dashboard-panel airflow-property-panel"><Empty description="작업을 선택하세요." /></aside>;
  }
  const monitorable = dag.monitoring_enabled !== undefined;
  const category = categoryOf(dag);
  const scheduleStatus = dag.is_paused ? "일시 중지" : dag.is_active === false ? "비활성" : "활성";
  const nextRun = dag.next_dagrun ?? dag.next_dagrun_run_after;
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
  return (
    <aside className={`airflow-dashboard-panel airflow-property-panel${inline ? " airflow-property-panel--inline" : ""}`}>
      {!inline && <><h3>속성</h3><h2>{displayName(dag)}</h2></>}
      <section>
        <strong>스케줄 현황</strong>
        <dl><dt>상태</dt><dd>{scheduleStatus}</dd><dt>스케줄</dt><dd title={dag.timetable_summary}>{scheduleDescription(dag.timetable_summary)}</dd><dt>다음 실행</dt><dd>{nextRun ? dayjs(nextRun).format("YYYY-MM-DD HH:mm") : "-"}</dd><dt>시간대</dt><dd>Asia/Seoul</dd></dl>
        {!inline && <div className="airflow-schedule-actions"><Button type="primary" loading={triggering} onClick={onManualRun}>수동 실행</Button><Button disabled={!dag.dag_id.startsWith("nifi_pipeline_")} onClick={onOpenSchedule}>스케줄 설정</Button></div>}
      </section>
      {!inline && <section>
        <strong>이상 감지 설정</strong>
        {monitorable
          ? <dl><dt>감지 사용</dt><dd>{dag.monitoring_enabled ? "사용" : "미사용"}</dd><dt>연속 실패</dt><dd>{dag.consecutive_failure_threshold ?? 3}회</dd><dt>미실행 정체</dt><dd>{dag.stale_days_threshold ?? 7}일</dd><dt>실행시간 이상</dt><dd>{Number(dag.duration_multiplier ?? 3).toFixed(1)}배</dd><dt>SLA</dt><dd>{dag.sla_minutes == null ? "미설정" : `${dag.sla_minutes}분`}</dd></dl>
          : <p>DB에 등록된 DAG만 감지 설정을 변경할 수 있습니다.</p>}
        <Button block disabled={!monitorable} onClick={onOpenMonitoring}>이상 감지 설정</Button>
      </section>}
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
      <section><strong>연결 리소스</strong><div className="airflow-resource-links"><a href={`/airflow/dags/${encodeURIComponent(dag.dag_id)}`} target="_blank" rel="noreferrer">DAG 보기</a>{category === "ETL" ? <a href="/etl/manage">NiFi 캔버스</a> : <a href="/cdc/pipelines">CDC 파이프라인</a>}</div></section>
    </aside>
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
  const [refreshSeconds, setRefreshSeconds] = useState(30);
  const [selectedDagId, setSelectedDagId] = useState<string>();
  const [search, setSearch] = useState("");
  const [actionCategory, setActionCategory] = useState<BusinessCategory>();
  const [activeDetailTab, setActiveDetailTab] = useState<DetailTab>("tasks");
  const [scheduleOpen, setScheduleOpen] = useState(false);
  const [monitoringOpen, setMonitoringOpen] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [triggering, setTriggering] = useState(false);
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
      const category = categoryOf(dag) as BusinessCategory;
      if (actionCategory && category !== actionCategory) return false;
      if (actionCategory && !alertsByDag.has(dag.dag_id)) return false;
      return !needle || `${displayName(dag)} ${dag.dag_id} ${folderName(dag)}`.toLowerCase().includes(needle);
    });
  }, [actionCategory, alertsByDag, businessDags, search]);

  useEffect(() => {
    if (!filteredDags.some((dag) => dag.dag_id === selectedDagId)) {
      setSelectedDagId(filteredDags[0]?.dag_id);
    }
  }, [filteredDags, selectedDagId]);

  useEffect(() => {
    if (initialSyncQuery.isError) {
      message.warning("DAG 자동 동기화에 실패해 기존 DB 정보로 대시보드를 표시합니다.");
    }
  }, [initialSyncQuery.isError]);

  const selectedDag = businessDags.find((dag) => dag.dag_id === selectedDagId);
  const selectedRuns = selectedDagId ? (runsByDag.get(selectedDagId) ?? []) : [];
  const selectedAlerts = selectedDagId ? (alertsByDag.get(selectedDagId) ?? []) : [];
  const selectedAlertHistory = selectedDagId
    ? (dashboardQuery.data?.alertHistory ?? []).filter((alert) => alert.dagId === selectedDagId)
    : [];
  const triggerSelectedDag = async () => {
    if (!selectedDag) return;
    setTriggering(true);
    try {
      await triggerAirflowDag(selectedDag.dag_id, selectedDag.dag_id.startsWith("nifi_pipeline_") ? { action: "start" } : {});
      message.success(`${displayName(selectedDag)} 실행을 요청했습니다.`);
      setActiveDetailTab("history");
      await dashboardQuery.refetch();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "DAG 실행 요청에 실패했습니다.");
    } finally {
      setTriggering(false);
    }
  };
  const synchronizeDags = async () => {
    const result = await initialSyncQuery.refetch();
    if (result.error || !result.data) {
      message.error("DAG 동기화에 실패했습니다.");
      return;
    }
    message.success(result.data.createdCount || result.data.disabledCount
      ? `DAG 동기화 완료: 신규 ${result.data.createdCount}건, 삭제 ${result.data.disabledCount}건`
      : "동기화할 DAG 변경사항이 없습니다.");
    await dashboardQuery.refetch();
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
        <div><h1>AirFlow 대시보드</h1><p>작업 상태와 실행 이력을 한 화면에서 확인하고 조치합니다.</p></div>
        <div className="airflow-refresh-controls"><Button icon={<SyncOutlined />} loading={initialSyncQuery.isFetching} onClick={() => void synchronizeDags()}>DAG 동기화</Button><span>갱신 주기</span><Select value={refreshSeconds} options={REFRESH_OPTIONS} onChange={setRefreshSeconds} /><Button type="text" icon={<ReloadOutlined />} loading={dashboardQuery.isFetching} onClick={() => dashboardQuery.refetch()} aria-label="새로고침" /><span>마지막 갱신 {dashboardQuery.dataUpdatedAt ? dayjs(dashboardQuery.dataUpdatedAt).format("HH:mm:ss") : "-"}</span></div>
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
                alerts={dashboardQuery.data?.alerts ?? []}
                onActionClick={() => setActionCategory((current) => current === category ? undefined : category)}
              />
            ))}
          </div>
          <div className="airflow-dashboard-workspace">
            <BusinessTree dags={filteredDags} selectedDagId={selectedDagId} search={search} alertsByDag={alertsByDag} onSearch={setSearch} onSelect={setSelectedDagId} onDetail={showDagDetail} onDelete={confirmDeleteDag} />
            <main className="airflow-dashboard-panel airflow-jobs-panel">
              <div className="airflow-jobs-heading"><div><h3>{selectedDag ? displayName(selectedDag) : "선택 작업"}</h3><button type="button" className={`airflow-jobs-tab${activeDetailTab === "tasks" ? " active" : ""}`} onClick={() => setActiveDetailTab("tasks")}>DAG 내 하위 작업</button><button type="button" className={`airflow-jobs-tab${activeDetailTab === "history" ? " active" : ""}`} onClick={() => setActiveDetailTab("history")}>실행 이력</button></div>{actionCategory ? <Button size="small" onClick={() => setActionCategory(undefined)}>{actionCategory} 조치 필터 해제</Button> : null}</div>
              {activeDetailTab === "tasks"
                ? <JobsTable dags={selectedDag ? [selectedDag] : []} runsByDag={runsByDag} alertsByDag={alertsByDag} selectedDagId={selectedDagId} refreshSeconds={refreshSeconds} onSelect={setSelectedDagId} />
                : <><RunHistoryTable dag={selectedDag} runs={selectedRuns} refreshSeconds={refreshSeconds} /><AlertHistory alerts={selectedAlertHistory} /></>}
              <PropertyPanel dag={selectedDag} alerts={selectedAlerts} inline onChanged={() => void dashboardQuery.refetch()} />
            </main>
            <PropertyPanel dag={selectedDag} alerts={selectedAlerts} triggering={triggering} onManualRun={triggerSelectedDag} onOpenSchedule={() => setScheduleOpen(true)} onOpenMonitoring={() => setMonitoringOpen(true)} onChanged={() => void dashboardQuery.refetch()} />
          </div>
          <InfrastructureBar groups={dashboardQuery.data?.processGroups ?? []} />
          <ScheduleWizard open={scheduleOpen} dag={selectedDag} onClose={() => setScheduleOpen(false)} onSaved={() => void dashboardQuery.refetch()} />
          <MonitoringSettingsModal open={monitoringOpen} dag={selectedDag} onClose={() => setMonitoringOpen(false)} onSaved={() => void dashboardQuery.refetch()} />
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

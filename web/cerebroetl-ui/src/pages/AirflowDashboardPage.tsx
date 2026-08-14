import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Empty, Input, Select, Spin, Tag } from "antd";
import {
  ApartmentOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  DeploymentUnitOutlined,
  DownOutlined,
  FolderOpenOutlined,
  FolderOutlined,
  ProfileOutlined,
  ReloadOutlined,
  RightOutlined,
} from "@ant-design/icons";
import dayjs from "dayjs";
import {
  getAirflowTaskLog,
  listAirflowDagCatalog,
  listAirflowDags,
  listAirflowTaskInstances,
  listAllAirflowDagRuns,
  type AirflowDag,
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
  timetable_summary?: string;
  business_category?: BusinessCategory;
  business_folder?: string;
}

interface DashboardData {
  dags: DashboardDag[];
  runs: AirflowDagRun[];
  processGroups: ProcessGroup[];
}

const CATEGORY_ORDER: BusinessCategory[] = ["CDC", "ETL"];
const REFRESH_OPTIONS = [
  { label: "10초", value: 10 },
  { label: "30초", value: 30 },
  { label: "1분", value: 60 },
];

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

function consecutiveFailures(runs: AirflowDagRun[]) {
  let count = 0;
  for (const run of runs) {
    if (run.state !== "failed") break;
    count += 1;
  }
  return count;
}

function requiresAction(runs: AirflowDagRun[]) {
  const latest = runs[0];
  const latestAt = runAt(latest);
  return latest?.state === "failed" || !latestAt || dayjs(latestAt).isBefore(dayjs().subtract(7, "day"));
}

async function loadDashboard(): Promise<DashboardData> {
  const [airflowDags, catalog, runs, processHealth] = await Promise.all([
    listAirflowDags(),
    listAirflowDagCatalog(),
    listAllAirflowDagRuns({ maxRuns: 2000 }),
    getProcessHealth().catch(() => ({ collectedAt: "", groups: [] })),
  ]);
  return {
    dags: catalog.map((entry) => {
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
      };
    }),
    runs,
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
  onActionClick,
}: {
  category: BusinessCategory;
  dags: DashboardDag[];
  runsByDag: Map<string, AirflowDagRun[]>;
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
  const actionCount = dags.filter((dag) => requiresAction(runsByDag.get(dag.dag_id) ?? [])).length;

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
        조치 필요 {actionCount}건
      </Button>
    </section>
  );
}

function BusinessTree({
  dags,
  selectedDagId,
  search,
  onSearch,
  onSelect,
}: {
  dags: DashboardDag[];
  selectedDagId?: string;
  search: string;
  onSearch: (value: string) => void;
  onSelect: (dagId: string) => void;
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
                      <button
                        key={dag.dag_id}
                        type="button"
                        className={selectedDagId === dag.dag_id ? "airflow-tree-dag active" : "airflow-tree-dag"}
                        onClick={() => onSelect(dag.dag_id)}
                      >
                        <DagIcon />
                        <span>{displayName(dag)}</span>
                      </button>
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
  const tasksQuery = useQuery({
    queryKey: ["airflow-dashboard-tasks", dag.dag_id, run?.dag_run_id],
    queryFn: () => listAirflowTaskInstances(dag.dag_id, run!.dag_run_id),
    enabled: Boolean(run),
    refetchInterval: refreshSeconds * 1000,
  });
  const failedTask = tasksQuery.data?.find((task) => task.state === "failed");
  const logQuery = useQuery({
    queryKey: ["airflow-dashboard-task-log", dag.dag_id, run?.dag_run_id, failedTask?.task_id],
    queryFn: () => getAirflowTaskLog(dag.dag_id, run!.dag_run_id, failedTask!.task_id, failedTask!.try_number || 1),
    enabled: Boolean(run && failedTask),
    staleTime: Infinity,
  });
  const errorLine = useMemo(() => {
    const lines = logQuery.data?.split("\n").map((line) => line.trim()).filter(Boolean) ?? [];
    return lines.at(-1)?.slice(0, 140);
  }, [logQuery.data]);

  if (tasksQuery.isLoading) {
    return <tr className="airflow-task-loading"><td colSpan={7}><Spin size="small" /> 실행 단계를 불러오는 중입니다.</td></tr>;
  }
  if (!run || !tasksQuery.data?.length) {
    return <tr className="airflow-task-loading"><td colSpan={7}>실행 단계가 없습니다.</td></tr>;
  }
  return tasksQuery.data.map((task, index) => (
    <tr key={task.task_id} className="airflow-task-row">
      <td>
        <span className="airflow-task-name">{String(index + 1).padStart(2, "0")} {task.task_id}</span>
        {task.task_id === failedTask?.task_id && errorLine ? <small title={errorLine}>{errorLine}</small> : null}
      </td>
      <td><ProfileOutlined /> TASK</td>
      <td>{task.start_date ? dayjs(task.start_date).format("YYYY-MM-DD HH:mm") : "-"}</td>
      <td><Tag color={stateColor(task.state)}>{stateLabel(task.state)}</Tag></td>
      <td>{duration(task.start_date, task.end_date)}</td>
      <td>-</td>
      <td>
        {task.task_id === failedTask?.task_id ? (
          <Button
            type="primary"
            size="small"
            onClick={() => window.open(`/airflow/dags/${encodeURIComponent(dag.dag_id)}`, "_blank")}
          >
            이 지점부터 재개
          </Button>
        ) : "-"}
      </td>
    </tr>
  ));
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
        <thead><tr><th>작업명</th><th>유형</th><th>최근 실행</th><th>결과</th><th>소요</th><th>다음 실행</th><th>연속 실패</th></tr></thead>
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
                    <td>{dag.next_dagrun ? dayjs(dag.next_dagrun).format("YYYY-MM-DD HH:mm") : "-"}</td>
                    <td>{consecutiveFailures(runs)} {selected ? <DownOutlined /> : null}</td>
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

function PropertyPanel({ dag }: { dag?: DashboardDag }) {
  if (!dag) {
    return <aside className="airflow-dashboard-panel airflow-property-panel"><Empty description="작업을 선택하세요." /></aside>;
  }
  const category = categoryOf(dag);
  const policy = category === "CDC" ? "재시도 1회 · 제한시간 5분" : "재시도 2회 · 제한시간 2시간";
  return (
    <aside className="airflow-dashboard-panel airflow-property-panel">
      <h3>속성 패널</h3>
      <h2>{displayName(dag)}</h2>
      <section><strong>기본 정보</strong><dl><dt>유형</dt><dd>DAG</dd><dt>DAG ID</dt><dd title={dag.dag_id}>{dag.dag_id}</dd><dt>소유자</dt><dd>{dag.owners?.join(", ") || "-"}</dd></dl></section>
      <section><strong>스케줄</strong><p>{dag.timetable_summary || "수동 실행 전용"}</p><p>Asia/Seoul</p></section>
      <section><strong>실패 정책</strong><p>{policy}</p></section>
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
  const dashboardQuery = useQuery({
    queryKey: ["airflow-dashboard"],
    queryFn: loadDashboard,
    refetchInterval: refreshSeconds * 1000,
  });
  const runsByDag = useMemo(() => groupRuns(dashboardQuery.data?.runs ?? []), [dashboardQuery.data?.runs]);
  const businessDags = useMemo(
    () => (dashboardQuery.data?.dags ?? []).filter((dag) => categoryOf(dag) !== "기타"),
    [dashboardQuery.data?.dags],
  );
  const filteredDags = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return businessDags.filter((dag) => {
      const category = categoryOf(dag) as BusinessCategory;
      if (actionCategory && category !== actionCategory) return false;
      if (actionCategory && !requiresAction(runsByDag.get(dag.dag_id) ?? [])) return false;
      return !needle || `${displayName(dag)} ${dag.dag_id} ${folderName(dag)}`.toLowerCase().includes(needle);
    });
  }, [actionCategory, businessDags, runsByDag, search]);

  useEffect(() => {
    if (!selectedDagId && businessDags[0]) setSelectedDagId(businessDags[0].dag_id);
  }, [businessDags, selectedDagId]);

  const selectedDag = businessDags.find((dag) => dag.dag_id === selectedDagId);

  return (
    <div className="airflow-dashboard-page">
      <header className="airflow-dashboard-heading">
        <div><h1>AirFlow 대시보드</h1><p>작업 상태와 실행 이력을 한 화면에서 확인하고 조치합니다.</p></div>
        <div className="airflow-refresh-controls"><span>갱신 주기</span><Select value={refreshSeconds} options={REFRESH_OPTIONS} onChange={setRefreshSeconds} /><Button type="text" icon={<ReloadOutlined />} loading={dashboardQuery.isFetching} onClick={() => dashboardQuery.refetch()} aria-label="새로고침" /><span>마지막 갱신 {dashboardQuery.dataUpdatedAt ? dayjs(dashboardQuery.dataUpdatedAt).format("HH:mm:ss") : "-"}</span></div>
      </header>

      {dashboardQuery.isLoading ? <div className="airflow-dashboard-loading"><Spin size="large" /></div> : dashboardQuery.isError ? <Empty description="Airflow 현황을 불러올 수 없습니다." /> : (
        <>
          <div className="airflow-business-status">
            {CATEGORY_ORDER.map((category) => (
              <StatusCard
                key={category}
                category={category}
                dags={businessDags.filter((dag) => categoryOf(dag) === category)}
                runsByDag={runsByDag}
                onActionClick={() => setActionCategory((current) => current === category ? undefined : category)}
              />
            ))}
          </div>
          <div className="airflow-dashboard-workspace">
            <BusinessTree dags={businessDags} selectedDagId={selectedDagId} search={search} onSearch={setSearch} onSelect={setSelectedDagId} />
            <main className="airflow-dashboard-panel airflow-jobs-panel"><div className="airflow-jobs-heading"><div><h3>작업 목록</h3><span className="active">작업 목록</span><span>실행 이력</span></div>{actionCategory ? <Button size="small" onClick={() => setActionCategory(undefined)}>{actionCategory} 조치 필터 해제</Button> : null}</div><JobsTable dags={filteredDags} runsByDag={runsByDag} selectedDagId={selectedDagId} refreshSeconds={refreshSeconds} onSelect={setSelectedDagId} /></main>
            <PropertyPanel dag={selectedDag} />
          </div>
          <InfrastructureBar groups={dashboardQuery.data?.processGroups ?? []} />
        </>
      )}
    </div>
  );
}

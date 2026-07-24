import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Space, Statistic } from "antd";
import { Column, Line } from "@ant-design/plots";
import dayjs, { type Dayjs } from "dayjs";
import { getDailyLoadSummary } from "../api/dashboard";
import {
  getAirflowTaskLog,
  getNifiRootStatus,
  listAirflowDagRuns,
  listAirflowDagTasks,
  listAirflowDags,
  listAirflowTaskInstances,
} from "../api/platform";
import type { AirflowTaskInstance } from "../api/platform";
import { listPipelines } from "../api/pipelines";
import type { PipelineResponse } from "../types/pipeline";
import { collectNifiJobs } from "../utils/nifiJobs";
import { categorizeDag, NIFI_METRICS_COLLECTOR_DAG_ID, resolveDagDisplayName, type DagCategory } from "../utils/dagHistory";
import { HistoryModal, type HistoryEntry } from "../components/HistoryModal";

const { RangePicker } = DatePicker;

interface DailyLoadPoint {
  date: string;
  count: number;
}

interface DagLoadPoint {
  dagId: string;
  count: number;
}

interface TaskLoadPoint {
  taskKey: string;
  count: number;
}

interface DurationTaskPoint {
  taskKey: string;
  minutes: number;
}

interface DagInfo {
  dagId: string;
  category: DagCategory;
  displayName: string;
  isActive: boolean;
}

interface DashboardHistoryData {
  dagCount: number;
  taskCount: number;
  daily: DailyLoadPoint[];
  topDags: DagLoadPoint[];
  topTasks: TaskLoadPoint[];
  topDurationTasks: DurationTaskPoint[];
  dagCatalog: HistoryEntry[];
  taskCatalog: HistoryEntry[];
  runningEntries: HistoryEntry[];
  successEntries: HistoryEntry[];
  failedEntries: HistoryEntry[];
  totalLoadCount: number;
}

const EMPTY_DASHBOARD_HISTORY: DashboardHistoryData = {
  dagCount: 0,
  taskCount: 0,
  daily: [],
  topDags: [],
  topTasks: [],
  topDurationTasks: [],
  dagCatalog: [],
  taskCatalog: [],
  runningEntries: [],
  successEntries: [],
  failedEntries: [],
  totalLoadCount: 0,
};

async function fetchTaskLogText(dagId: string, runId: string, taskId: string, tryNumber?: number): Promise<string> {
  try {
    return await getAirflowTaskLog(dagId, runId, taskId, tryNumber && tryNumber > 0 ? tryNumber : 1);
  } catch {
    return "로그를 불러올 수 없습니다.";
  }
}

async function fetchRunLogText(dagId: string, runId: string): Promise<string> {
  const instances = await listAirflowTaskInstances(dagId, runId).catch(() => []);
  if (instances.length === 0) {
    return "로그를 불러올 수 없습니다.";
  }
  const parts = await Promise.all(
    instances.map(async (instance) => {
      const text = await fetchTaskLogText(dagId, runId, instance.task_id, instance.try_number);
      return `=== ${instance.task_id} ===\n${text}`;
    }),
  );
  return parts.join("\n\n");
}

// 대시보드 상단 6개 타일(DAG/태스크/실행중/성공/실패/총 적재건수)과 그 클릭 시 뜨는 상세
// 내역 모달, 그리고 적재 건수/수행시간 차트의 데이터를 한 번에 만든다. 적재 건수는
// pipeline_daily_load_metric 롤업을 그대로 읽고, DAG/태스크/실행중/성공/실패는
// Airflow의 실제 DAG/실행 이력을 ETL(NiFi)/CDC(Kafka)/기타로 분류해서 집계한다.
// 실행중/성공/실패 3개 타일은 "DAG 실행(dag run)" 단위 집계다 - 하나의 DAG 실행이
// 여러 태스크로 구성돼 있어도 그 실행 전체의 최종 상태 하나로만 집계되고, 개별 태스크
// 성공/실패는 집계하지 않는다(태스크 타일은 이와 별개로 "정의된 태스크 종류가 몇 개인지"
// 세는 카탈로그).
async function buildDashboardHistory(range: [Dayjs, Dayjs]): Promise<DashboardHistoryData> {
  const [dags, nifiStatusResult, kafkaPipelines, summary] = await Promise.all([
    listAirflowDags(),
    getNifiRootStatus().catch(() => undefined),
    listPipelines().catch(() => [] as PipelineResponse[]),
    getDailyLoadSummary(range[0].format("YYYY-MM-DD"), range[1].format("YYYY-MM-DD")),
  ]);

  const nifiJobs = collectNifiJobs(nifiStatusResult?.processGroupStatus?.aggregateSnapshot?.processGroupStatusSnapshots);

  const dagInfos: DagInfo[] = dags.map((dag) => {
    const category = categorizeDag(dag.dag_id);
    return {
      dagId: dag.dag_id,
      category,
      displayName: resolveDagDisplayName(dag.dag_id, category, nifiJobs, kafkaPipelines),
      isActive: dag.is_active !== false && dag.is_paused !== true,
    };
  });

  const latestRunResults = await Promise.allSettled(
    dagInfos.map(async (info) => ({
      dagId: info.dagId,
      run: (await listAirflowDagRuns(info.dagId, { limit: 1 }))[0],
    })),
  );
  const latestRunByDag = new Map(
    latestRunResults
      .filter((result) => result.status === "fulfilled")
      .map((result) => [result.value.dagId, result.value.run]),
  );

  const taskListResults = await Promise.allSettled(
    dagInfos.map(async (info) => ({ dagId: info.dagId, tasks: await listAirflowDagTasks(info.dagId) })),
  );

  const latestRunTaskInstances = new Map<string, AirflowTaskInstance[]>();
  await Promise.all(
    dagInfos.map(async (info) => {
      const run = latestRunByDag.get(info.dagId);
      if (!run) {
        return;
      }
      const instances = await listAirflowTaskInstances(info.dagId, run.dag_run_id).catch(() => []);
      latestRunTaskInstances.set(info.dagId, instances);
    }),
  );

  const dagCatalog: HistoryEntry[] = dagInfos.map((info) => {
    const run = latestRunByDag.get(info.dagId);
    return {
      id: info.dagId,
      basicContent: info.displayName,
      category: info.category,
      datetime: run?.start_date ?? run?.execution_date,
      fetchLog: run ? () => fetchRunLogText(info.dagId, run.dag_run_id) : undefined,
    };
  });

  const taskCatalog: HistoryEntry[] = taskListResults.flatMap((result) => {
    if (result.status !== "fulfilled") {
      return [];
    }
    const { dagId, tasks } = result.value;
    const info = dagInfos.find((candidate) => candidate.dagId === dagId);
    if (!info) {
      return [];
    }
    const run = latestRunByDag.get(dagId);
    const instances = latestRunTaskInstances.get(dagId) ?? [];
    return tasks.map((task) => {
      const instance = instances.find((candidate) => candidate.task_id === task.task_id);
      return {
        id: `${dagId}.${task.task_id}`,
        basicContent: `${info.displayName} / ${task.task_id}`,
        category: info.category,
        datetime: instance?.start_date,
        fetchLog: run ? () => fetchTaskLogText(dagId, run.dag_run_id, task.task_id, instance?.try_number) : undefined,
      };
    });
  });

  // 1분마다 도는 nifi_pipelines_metrics_collector는 알려진 executor 버그로 매번
  // failed로 찍히는 노이즈라 성공/실패/지연 집계에서는 제외한다(DAG/태스크 목록에는
  // 실제로 존재하는 DAG이니 그대로 포함).
  const historyDagInfos = dagInfos.filter((info) => info.dagId !== NIFI_METRICS_COLLECTOR_DAG_ID);
  const runGroupResults = await Promise.allSettled(
    historyDagInfos.map(async (info) => ({
      info,
      runs: await listAirflowDagRuns(info.dagId, {
        limit: 200,
        startDateGte: range[0].startOf("day").toISOString(),
        startDateLte: range[1].endOf("day").toISOString(),
      }),
    })),
  );
  const runGroups = runGroupResults.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []));

  const runningEntries: HistoryEntry[] = [];
  const successEntries: HistoryEntry[] = [];
  const failedEntries: HistoryEntry[] = [];

  runGroups.forEach(({ info, runs }) => {
    runs.forEach((run) => {
      const entry: HistoryEntry = {
        id: `${info.dagId}:${run.dag_run_id}`,
        basicContent: info.displayName,
        category: info.category,
        datetime: run.start_date ?? run.execution_date,
        fetchLog: () => fetchRunLogText(info.dagId, run.dag_run_id),
      };
      if (run.state === "success") {
        successEntries.push(entry);
      } else if (run.state === "failed") {
        failedEntries.push(entry);
      } else if (run.state === "running" || run.state === "queued") {
        runningEntries.push(entry);
      }
    });
  });

  const byDatetimeDesc = (a: HistoryEntry, b: HistoryEntry) =>
    new Date(b.datetime ?? 0).getTime() - new Date(a.datetime ?? 0).getTime();
  runningEntries.sort(byDatetimeDesc);
  successEntries.sort(byDatetimeDesc);
  failedEntries.sort(byDatetimeDesc);

  // Top 5 수행시간 태스크: 선택한 기간 안의 모든 실행에서 태스크별 소요시간(종료-시작)을
  // 합산해 가장 오래 걸린 태스크 5개를 뽑는다(적재 건수 Top5/10과 같은 "합계 후 정렬" 방식).
  const taskInstanceResults = await Promise.allSettled(
    runGroups.flatMap((group) =>
      group.runs.map(async (run) => ({
        info: group.info,
        instances: await listAirflowTaskInstances(group.info.dagId, run.dag_run_id).catch(() => []),
      })),
    ),
  );
  const durationTotals = new Map<string, { label: string; seconds: number }>();
  taskInstanceResults.forEach((result) => {
    if (result.status !== "fulfilled") {
      return;
    }
    const { info, instances } = result.value;
    instances.forEach((instance) => {
      if (!instance.start_date || !instance.end_date) {
        return;
      }
      const seconds = (new Date(instance.end_date).getTime() - new Date(instance.start_date).getTime()) / 1000;
      if (seconds <= 0) {
        return;
      }
      const key = `${info.dagId}.${instance.task_id}`;
      const label = `${info.displayName} / ${instance.task_id}`;
      const existing = durationTotals.get(key);
      durationTotals.set(key, { label, seconds: (existing?.seconds ?? 0) + seconds });
    });
  });
  const topDurationTasks = Array.from(durationTotals.values())
    .map((entry) => ({ taskKey: entry.label, minutes: Math.round((entry.seconds / 60) * 10) / 10 }))
    .sort((a, b) => b.minutes - a.minutes)
    .slice(0, 5);

  const daily = summary.daily
    .map((point) => ({ date: dayjs(point.date).format("YYYY.MM.DD"), count: point.count }))
    .sort((a, b) => a.date.localeCompare(b.date));
  const topDags = summary.topPipelines.map((point) => ({ dagId: point.label, count: point.count }));
  const topTasks = summary.topTasks.map((point) => ({ taskKey: point.label, count: point.count }));
  const totalLoadCount = daily.reduce((sum, point) => sum + point.count, 0);

  return {
    dagCount: dagInfos.length,
    taskCount: taskCatalog.length,
    daily,
    topDags,
    topTasks,
    topDurationTasks,
    dagCatalog,
    taskCatalog,
    runningEntries,
    successEntries,
    failedEntries,
    totalLoadCount,
  };
}

type TileKind = "dag" | "task" | "running" | "success" | "failed";

const TILE_TITLE: Record<TileKind, string> = {
  dag: "DAG 목록",
  task: "태스크 목록",
  running: "실행중 내역",
  success: "성공 내역",
  failed: "실패 내역",
};

export function DashboardPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const [activeTile, setActiveTile] = useState<TileKind | null>(null);

  const historyQuery = useQuery({
    queryKey: ["dashboard-history", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => buildDashboardHistory(appliedRange),
    placeholderData: (previousData) => previousData,
  });

  const history = historyQuery.data ?? EMPTY_DASHBOARD_HISTORY;
  const showInitialLoading = historyQuery.isLoading && !historyQuery.data;

  const modalRows = useMemo(() => {
    switch (activeTile) {
      case "dag":
        return history.dagCatalog;
      case "task":
        return history.taskCatalog;
      case "running":
        return history.runningEntries;
      case "success":
        return history.successEntries;
      case "failed":
        return history.failedEntries;
      default:
        return [];
    }
  }, [activeTile, history]);

  return (
    <div>
      <div className="dashboard-stack">
        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <h3>데이터 로드 현황</h3>
            </div>
            <Space>
              <RangePicker
                value={dateRange}
                onChange={(value) => {
                  if (value && value[0] && value[1]) {
                    setDateRange([value[0], value[1]]);
                  }
                }}
                allowClear={false}
              />
              <Button type="primary" onClick={() => setAppliedRange(dateRange)}>
                검색
              </Button>
            </Space>
          </div>
          <div className="dashboard-grid-stack">
            <div className="dashboard-tile-grid">
              <Card className="metric-card" loading={showInitialLoading} onClick={() => setActiveTile("dag")}>
                <Statistic title="DAG" value={history.dagCount} valueStyle={{ color: "#08979c" }} />
              </Card>
              <Card className="metric-card" loading={showInitialLoading} onClick={() => setActiveTile("task")}>
                <Statistic title="태스크" value={history.taskCount} valueStyle={{ color: "#5cdbd3" }} />
              </Card>
              <Card className="metric-card" loading={showInitialLoading} onClick={() => setActiveTile("running")}>
                <Statistic title="실행중" value={history.runningEntries.length} valueStyle={{ color: "#1677ff" }} />
              </Card>
              <Card className="metric-card" loading={showInitialLoading} onClick={() => setActiveTile("success")}>
                <Statistic title="성공" value={history.successEntries.length} valueStyle={{ color: "#2f7d32" }} />
              </Card>
              <Card className="metric-card" loading={showInitialLoading} onClick={() => setActiveTile("failed")}>
                <Statistic title="실패" value={history.failedEntries.length} valueStyle={{ color: "#c62828" }} />
              </Card>
              <Card className="metric-card metric-card--static" loading={showInitialLoading}>
                <Statistic title="총 적재건수" value={history.totalLoadCount} valueStyle={{ color: "#d4380d" }} />
              </Card>
            </div>
            <div className="dashboard-chart-grid">
              <Card title="일별 적재 건수" size="small" loading={showInitialLoading}>
                {history.daily.length > 0 ? (
                  <Line data={history.daily} xField="date" yField="count" height={240} />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
              <Card title="Top 5 수행시간 태스크" size="small" loading={showInitialLoading}>
                {history.topDurationTasks.length > 0 ? (
                  <Column
                    data={history.topDurationTasks}
                    xField="taskKey"
                    yField="minutes"
                    height={240}
                    axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                  />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </div>
            <div className="dashboard-chart-grid">
              <Card title="Top 5 데이터 로드 DAG" size="small" loading={showInitialLoading}>
                {history.topDags.length > 0 ? (
                  <Column
                    data={history.topDags}
                    xField="dagId"
                    yField="count"
                    height={240}
                    axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                  />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
              <Card title="Top 10 데이터 로드 태스크" size="small" loading={showInitialLoading}>
                {history.topTasks.length > 0 ? (
                  <Column
                    data={history.topTasks}
                    xField="taskKey"
                    yField="count"
                    height={240}
                    axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                  />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </div>
          </div>
        </section>
      </div>

      <HistoryModal
        open={activeTile !== null}
        onClose={() => setActiveTile(null)}
        title={activeTile ? TILE_TITLE[activeTile] : ""}
        loading={showInitialLoading}
        rows={modalRows}
      />
    </div>
  );
}

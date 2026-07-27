import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Space, Statistic } from "antd";
import { Column, Line } from "@ant-design/plots";
import dayjs, { type Dayjs } from "dayjs";
import {
  getDailyLoadSummary,
  getPipelineDashboardSummary,
  getRealtimePipelineMetrics,
} from "../api/dashboard";
import {
  getAirflowTaskLog,
  getKafkaConnectorTrace,
  getNifiBulletins,
  getNifiRootStatus,
  listAirflowDagRuns,
  listAirflowDagTasks,
  listAirflowDags,
  listAirflowTaskInstances,
  listKafkaConnectorsWithStatus,
  listNifiExecutionLogs,
} from "../api/platform";
import type { AirflowTaskInstance } from "../api/platform";
import { listPipelines } from "../api/pipelines";
import type { PipelineResponse } from "../types/pipeline";
import { collectNifiJobs, extractErrorGroupMessages } from "../utils/nifiJobs";
import { summarizeKafkaConnectors } from "../utils/kafkaConnectors";
import { categorizeDag, NIFI_METRICS_COLLECTOR_DAG_ID, resolveDagDisplayName, type DagCategory } from "../utils/dagHistory";
import { HistoryModal, type HistoryEntry } from "../components/HistoryModal";
import { OperationsOverview } from "../components/OperationsOverview";

const { RangePicker } = DatePicker;

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

interface AirflowHistoryData {
  dagCount: number;
  taskCount: number;
  topDurationTasks: DurationTaskPoint[];
  dagCatalog: HistoryEntry[];
  taskCatalog: HistoryEntry[];
  runningEntries: HistoryEntry[];
  successEntries: HistoryEntry[];
  failedEntries: HistoryEntry[];
}

const EMPTY_AIRFLOW_HISTORY: AirflowHistoryData = {
  dagCount: 0,
  taskCount: 0,
  topDurationTasks: [],
  dagCatalog: [],
  taskCatalog: [],
  runningEntries: [],
  successEntries: [],
  failedEntries: [],
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

// Airflow 제어 DAG의 실행 이력(성공/실패/실행중)과 태스크 수행시간 - 이건 "Airflow가
// 보낸 시작/중지 명령 자체가 잘 처리됐는지"를 보여줄 뿐, 그 뒤 NiFi/Kafka가 실제로 데이터를
// 잘 처리했는지는 반영하지 않는다(그건 아래 NiFi/Kafka 섹션에서 각자 직접 조회한다).
async function buildAirflowHistory(range: [Dayjs, Dayjs]): Promise<AirflowHistoryData> {
  const [dags, nifiStatusResult, kafkaPipelines] = await Promise.all([
    listAirflowDags(),
    getNifiRootStatus().catch(() => undefined),
    listPipelines().catch(() => [] as PipelineResponse[]),
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
  // 합산해 가장 오래 걸린 태스크 5개를 뽑는다.
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

  return {
    dagCount: dagInfos.length,
    taskCount: taskCatalog.length,
    topDurationTasks,
    dagCatalog,
    taskCatalog,
    runningEntries,
    successEntries,
    failedEntries,
  };
}

type AirflowTileKind = "dag" | "task" | "running" | "success" | "failed";

const AIRFLOW_TILE_TITLE: Record<AirflowTileKind, string> = {
  dag: "DAG 목록",
  task: "태스크 목록",
  running: "실행중 내역",
  success: "성공 내역",
  failed: "실패 내역",
};

function formatLoadDate(date: string) {
  return dayjs(date).format("YYYY.MM.DD");
}

export function DashboardPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const [airflowActiveTile, setAirflowActiveTile] = useState<AirflowTileKind | null>(null);
  const [nifiFailedOpen, setNifiFailedOpen] = useState(false);
  const [kafkaFailedOpen, setKafkaFailedOpen] = useState(false);

  // NiFi/Kafka는 Airflow DAG 실행 이력을 거치지 않고 각자의 API를 직접 조회한다 -
  // 이게 실제 처리 상태(대기열, 오류)를 그대로 반영하는 값이라 짧은 주기로 새로고침한다.
  const nifiJobsQuery = useQuery({
    queryKey: ["dashboard-nifi-jobs"],
    queryFn: async () => {
      const [statusResult, bulletinResult] = await Promise.all([getNifiRootStatus(), getNifiBulletins()]);
      const errorMessages = extractErrorGroupMessages(bulletinResult.bulletinBoard?.bulletins);
      return collectNifiJobs(
        statusResult.processGroupStatus?.aggregateSnapshot?.processGroupStatusSnapshots,
        errorMessages,
      );
    },
    refetchInterval: 15000,
    placeholderData: (previousData) => previousData,
  });

  const kafkaConnectorsQuery = useQuery({
    queryKey: ["dashboard-kafka-connectors"],
    queryFn: async () => summarizeKafkaConnectors(await listKafkaConnectorsWithStatus()),
    refetchInterval: 15000,
    placeholderData: (previousData) => previousData,
  });

  const commandSummaryQuery = useQuery({
    queryKey: ["dashboard-command-summary"],
    queryFn: getPipelineDashboardSummary,
    refetchInterval: 30000,
    placeholderData: (previousData) => previousData,
  });

  const nifiLoadQuery = useQuery({
    queryKey: ["dashboard-nifi-load", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => getDailyLoadSummary(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD"), "NIFI"),
    placeholderData: (previousData) => previousData,
  });

  const kafkaLoadQuery = useQuery({
    queryKey: ["dashboard-kafka-load", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => getDailyLoadSummary(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD"), "KAFKA"),
    placeholderData: (previousData) => previousData,
  });

  const airflowHistoryQuery = useQuery({
    queryKey: ["dashboard-airflow-history", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => buildAirflowHistory(appliedRange),
    placeholderData: (previousData) => previousData,
  });

  const operationsPipelineQuery = useQuery({
    queryKey: ["dashboard-operations-pipelines"],
    queryFn: async () => {
      const [pipelines, metrics] = await Promise.all([listPipelines(), getRealtimePipelineMetrics()]);
      return { pipelines, metrics };
    },
    refetchInterval: 20000,
    placeholderData: (previousData) => previousData,
  });

  const nifiExecutionLogsQuery = useQuery({
    queryKey: ["dashboard-operations-nifi-logs", dayjs().format("YYYY-MM-DD")],
    queryFn: () => listNifiExecutionLogs(dayjs().format("YYYY-MM-DD"), dayjs().format("YYYY-MM-DD")),
    refetchInterval: 30000,
    placeholderData: (previousData) => previousData,
  });

  const nifiJobs = nifiJobsQuery.data ?? [];
  const kafkaConnectors = kafkaConnectorsQuery.data ?? [];
  const commandSummary = commandSummaryQuery.data;
  const airflowHistory = airflowHistoryQuery.data ?? EMPTY_AIRFLOW_HISTORY;
  const showAirflowInitialLoading = airflowHistoryQuery.isLoading && !airflowHistoryQuery.data;

  const nifiRunning = nifiJobs.filter((job) => job.status === "RUNNING").length;
  const nifiFailed = nifiJobs.filter((job) => job.status === "FAILED").length;
  const nifiStopped = nifiJobs.filter((job) => job.status === "STOPPED").length;

  const kafkaRunning = kafkaConnectors.filter((job) => job.status === "RUNNING").length;
  const kafkaFailed = kafkaConnectors.filter((job) => job.status === "FAILED").length;
  const kafkaPaused = kafkaConnectors.filter((job) => job.status === "PAUSED").length;

  const nifiQueueChartData = useMemo(
    () =>
      [...nifiJobs]
        .sort((a, b) => b.flowFilesQueued - a.flowFilesQueued)
        .slice(0, 5)
        .map((job) => ({ name: job.name, queued: job.flowFilesQueued })),
    [nifiJobs],
  );

  const nifiLoadDaily = useMemo(
    () => (nifiLoadQuery.data?.daily ?? []).map((point) => ({ date: formatLoadDate(point.date), count: point.count })),
    [nifiLoadQuery.data],
  );
  const nifiLoadTop = useMemo(
    () => (nifiLoadQuery.data?.topPipelines ?? []).map((point) => ({ label: point.label, count: point.count })),
    [nifiLoadQuery.data],
  );
  const kafkaLoadTop = useMemo(
    () => (kafkaLoadQuery.data?.topPipelines ?? []).map((point) => ({ label: point.label, count: point.count })),
    [kafkaLoadQuery.data],
  );
  const kafkaLoadDaily = useMemo(
    () => (kafkaLoadQuery.data?.daily ?? []).map((point) => ({ date: formatLoadDate(point.date), count: point.count })),
    [kafkaLoadQuery.data],
  );

  const nifiFailedRows: HistoryEntry[] = useMemo(
    () =>
      nifiJobs
        .filter((job) => job.status === "FAILED")
        .map((job) => ({
          id: job.id,
          basicContent: job.name,
          category: "ETL" as DagCategory,
          fetchLog: async () => job.errorMessage ?? "오류 메시지를 찾을 수 없습니다.",
        })),
    [nifiJobs],
  );

  const kafkaFailedRows: HistoryEntry[] = useMemo(
    () =>
      kafkaConnectors
        .filter((job) => job.status === "FAILED")
        .map((job) => ({
          id: job.name,
          basicContent: `${job.name} (${job.role})`,
          category: "CDC" as DagCategory,
          fetchLog: async () => {
            const trace = await getKafkaConnectorTrace(job.name).catch(() => undefined);
            const connectorTrace = trace?.connector?.trace;
            const taskTraces = (trace?.tasks ?? [])
              .filter((task) => task.trace)
              .map((task) => `[task ${task.id}]\n${task.trace}`)
              .join("\n\n");
            return connectorTrace || taskTraces || "오류 트레이스를 찾을 수 없습니다.";
          },
        })),
    [kafkaConnectors],
  );

  const airflowModalRows = useMemo(() => {
    switch (airflowActiveTile) {
      case "dag":
        return airflowHistory.dagCatalog;
      case "task":
        return airflowHistory.taskCatalog;
      case "running":
        return airflowHistory.runningEntries;
      case "success":
        return airflowHistory.successEntries;
      case "failed":
        return airflowHistory.failedEntries;
      default:
        return [];
    }
  }, [airflowActiveTile, airflowHistory]);

  return (
    <div>
      <div className="dashboard-stack">
        <OperationsOverview
          pipelines={operationsPipelineQuery.data?.pipelines ?? []}
          realtimeMetrics={operationsPipelineQuery.data?.metrics ?? []}
          kafkaConnectors={kafkaConnectors}
          nifiJobs={nifiJobs}
          nifiExecutionLogs={nifiExecutionLogsQuery.data ?? []}
          nifiDaily={nifiLoadDaily}
          kafkaDaily={kafkaLoadDaily}
          nifiTop={nifiLoadTop}
          kafkaTop={kafkaLoadTop}
          headerExtra={
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
          }
          loading={
            (operationsPipelineQuery.isLoading && !operationsPipelineQuery.data) ||
            (nifiJobsQuery.isLoading && !nifiJobsQuery.data) ||
            (kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data)
          }
        />

        {/* NiFi는 Airflow를 거치지 않고 자신의 REST API(프로세스 그룹 상태, bulletin board)를
            직접 조회한 실시간 값이다 - 실행중/실패는 지금 이 순간의 상태, 적재 건수 차트만
            위에서 고른 기간을 따른다. */}
        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <h3>NiFi 실시간 현황</h3>
            </div>
          </div>
          <div className="dashboard-grid-stack">
            <div className="dashboard-tile-grid dashboard-tile-grid--4">
              <Card className="metric-card metric-card--static" loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}>
                <Statistic title="실행중 Job" value={nifiRunning} valueStyle={{ color: "#1677ff" }} />
              </Card>
              <Card
                className="metric-card"
                loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}
                onClick={() => setNifiFailedOpen(true)}
              >
                <Statistic title="실패 Job" value={nifiFailed} valueStyle={{ color: "#c62828" }} />
              </Card>
              <Card className="metric-card metric-card--static" loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}>
                <Statistic title="중지 Job" value={nifiStopped} valueStyle={{ color: "#8c8c8c" }} />
              </Card>
              <Card className="metric-card metric-card--static" loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}>
                <Statistic title="전체 Job" value={nifiJobs.length} valueStyle={{ color: "#08979c" }} />
              </Card>
            </div>
            <div className="dashboard-chart-grid">
              <Card title="Job별 대기 중 FlowFile Top 5" size="small" loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}>
                {nifiQueueChartData.length > 0 ? (
                  <Column
                    data={nifiQueueChartData}
                    xField="name"
                    yField="queued"
                    height={240}
                    axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                  />
                ) : (
                  <div className="empty-chart-placeholder">대기 중인 FlowFile이 없습니다.</div>
                )}
              </Card>
              <Card title="NiFi 일별 적재 건수" size="small" loading={nifiLoadQuery.isLoading && !nifiLoadQuery.data}>
                {nifiLoadDaily.length > 0 ? (
                  <Line data={nifiLoadDaily} xField="date" yField="count" height={240} />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </div>
          </div>
        </section>

        {/* Kafka도 마찬가지로 Kafka Connect REST API(/connectors?expand=status)를 직접
            조회한다 - 커넥터 상태가 RUNNING이어도 태스크 하나가 FAILED면 실패로 잡는다. */}
        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <h3>Kafka 실시간 현황</h3>
            </div>
          </div>
          <div className="dashboard-grid-stack">
            <div className="dashboard-tile-grid dashboard-tile-grid--4">
              <Card
                className="metric-card metric-card--static"
                loading={kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data}
              >
                <Statistic title="실행중 Connector" value={kafkaRunning} valueStyle={{ color: "#1677ff" }} />
              </Card>
              <Card
                className="metric-card"
                loading={kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data}
                onClick={() => setKafkaFailedOpen(true)}
              >
                <Statistic title="실패 Connector" value={kafkaFailed} valueStyle={{ color: "#c62828" }} />
              </Card>
              <Card
                className="metric-card metric-card--static"
                loading={kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data}
              >
                <Statistic title="일시정지 Connector" value={kafkaPaused} valueStyle={{ color: "#8c8c8c" }} />
              </Card>
              <Card
                className="metric-card metric-card--static"
                loading={kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data}
              >
                <Statistic title="전체 Connector" value={kafkaConnectors.length} valueStyle={{ color: "#08979c" }} />
              </Card>
            </div>
            <div className="dashboard-chart-grid">
              <Card title="Kafka 적재 건수 Top 5 파이프라인" size="small" loading={kafkaLoadQuery.isLoading && !kafkaLoadQuery.data}>
                {kafkaLoadTop.length > 0 ? (
                  <Column
                    data={kafkaLoadTop}
                    xField="label"
                    yField="count"
                    height={240}
                    axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                  />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
              <Card title="Kafka 일별 적재 건수" size="small" loading={kafkaLoadQuery.isLoading && !kafkaLoadQuery.data}>
                {kafkaLoadDaily.length > 0 ? (
                  <Line data={kafkaLoadDaily} xField="date" yField="count" height={240} />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </div>
          </div>
        </section>

        {/* Airflow 섹션은 "제어 명령(DAG) 실행 이력"이다 - DAG 실행 성공/실패는 시작/중지
            명령 자체가 잘 전달됐는지를 뜻할 뿐, 그 뒤 NiFi/Kafka가 실제로 데이터를 어떻게
            처리했는지는 위 두 섹션을 봐야 한다. */}
        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <h3>Airflow 제어 현황</h3>
            </div>
          </div>
          <div className="dashboard-grid-stack">
            <div className="dashboard-tile-grid">
              <Card className="metric-card" loading={showAirflowInitialLoading} onClick={() => setAirflowActiveTile("dag")}>
                <Statistic title="DAG" value={airflowHistory.dagCount} valueStyle={{ color: "#08979c" }} />
              </Card>
              <Card className="metric-card" loading={showAirflowInitialLoading} onClick={() => setAirflowActiveTile("task")}>
                <Statistic title="태스크" value={airflowHistory.taskCount} valueStyle={{ color: "#5cdbd3" }} />
              </Card>
              <Card
                className="metric-card"
                loading={showAirflowInitialLoading}
                onClick={() => setAirflowActiveTile("running")}
              >
                <Statistic
                  title="제어 명령 실행중"
                  value={airflowHistory.runningEntries.length}
                  valueStyle={{ color: "#1677ff" }}
                />
              </Card>
              <Card
                className="metric-card"
                loading={showAirflowInitialLoading}
                onClick={() => setAirflowActiveTile("success")}
              >
                <Statistic
                  title="제어 명령 성공"
                  value={airflowHistory.successEntries.length}
                  valueStyle={{ color: "#2f7d32" }}
                />
              </Card>
              <Card
                className="metric-card"
                loading={showAirflowInitialLoading}
                onClick={() => setAirflowActiveTile("failed")}
              >
                <Statistic
                  title="제어 명령 실패"
                  value={airflowHistory.failedEntries.length}
                  valueStyle={{ color: "#c62828" }}
                />
              </Card>
              <Card className="metric-card metric-card--static" loading={commandSummaryQuery.isLoading}>
                <Statistic
                  title="오늘 명령 성공/실패"
                  value={commandSummary?.todayCommandSuccessCount ?? 0}
                  suffix={`/ ${commandSummary?.todayCommandFailedCount ?? 0} 실패`}
                  valueStyle={{ color: "#d4380d" }}
                />
              </Card>
            </div>
            <div className="dashboard-chart-grid">
              <Card title="Top 5 수행시간 태스크" size="small" loading={showAirflowInitialLoading}>
                {airflowHistory.topDurationTasks.length > 0 ? (
                  <Column
                    data={airflowHistory.topDurationTasks}
                    xField="taskKey"
                    yField="minutes"
                    height={240}
                    axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                  />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 실행 이력이 없습니다.</div>
                )}
              </Card>
            </div>
          </div>
        </section>
      </div>

      <HistoryModal
        open={airflowActiveTile !== null}
        onClose={() => setAirflowActiveTile(null)}
        title={airflowActiveTile ? AIRFLOW_TILE_TITLE[airflowActiveTile] : ""}
        loading={showAirflowInitialLoading}
        rows={airflowModalRows}
      />
      <HistoryModal
        open={nifiFailedOpen}
        onClose={() => setNifiFailedOpen(false)}
        title="NiFi 실패 Job 상세"
        loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}
        rows={nifiFailedRows}
      />
      <HistoryModal
        open={kafkaFailedOpen}
        onClose={() => setKafkaFailedOpen(false)}
        title="Kafka 실패 Connector 상세"
        loading={kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data}
        rows={kafkaFailedRows}
      />
    </div>
  );
}

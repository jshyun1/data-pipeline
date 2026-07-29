import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Empty, Space, Tag } from "antd";
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

const EMPTY_NIFI_JOBS: ReturnType<typeof collectNifiJobs> = [];
const EMPTY_KAFKA_CONNECTORS: ReturnType<typeof summarizeKafkaConnectors> = [];

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

  // Top 5 수행시간 태스크: 조회 기간 안의 모든 실행에서 태스크별 소요시간(종료-시작)을
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

function formatCount(value: number) {
  return new Intl.NumberFormat("ko-KR").format(value);
}

function formatCompactCount(value: number) {
  return new Intl.NumberFormat("ko-KR", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatDashboardDateTime(value?: string) {
  return value ? dayjs(value).format("MM.DD HH:mm:ss") : "-";
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

  // 상단 요약 카드(스트림/플로우/배치)의 집계값. 조회 기간 기반이라 원래는 조회 버튼을
  // 눌러야만 갱신됐는데, 오늘이 기간에 포함되면 "지금 쌓이는 건수"처럼 보이면서 실제로는
  // 멈춰 있어 오해를 샀다. 아래 상태 카드들과 같은 주기로 맞춰 자동 갱신한다.
  const SUMMARY_REFETCH_INTERVAL = 30000;

  const nifiLoadQuery = useQuery({
    queryKey: ["dashboard-nifi-load", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => getDailyLoadSummary(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD"), "NIFI"),
    refetchInterval: SUMMARY_REFETCH_INTERVAL,
    placeholderData: (previousData) => previousData,
  });

  const kafkaLoadQuery = useQuery({
    queryKey: ["dashboard-kafka-load", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => getDailyLoadSummary(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD"), "KAFKA"),
    refetchInterval: SUMMARY_REFETCH_INTERVAL,
    placeholderData: (previousData) => previousData,
  });

  const airflowHistoryQuery = useQuery({
    queryKey: ["dashboard-airflow-history", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => buildAirflowHistory(appliedRange),
    refetchInterval: SUMMARY_REFETCH_INTERVAL,
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
    queryKey: [
      "dashboard-operations-nifi-logs",
      appliedRange[0].format("YYYY-MM-DD"),
      appliedRange[1].format("YYYY-MM-DD"),
    ],
    queryFn: () =>
      listNifiExecutionLogs(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")),
    refetchInterval: 30000,
    placeholderData: (previousData) => previousData,
  });

  const nifiJobs = nifiJobsQuery.data ?? EMPTY_NIFI_JOBS;
  const kafkaConnectors = kafkaConnectorsQuery.data ?? EMPTY_KAFKA_CONNECTORS;
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
    () => [
      ...(nifiExecutionLogsQuery.data ?? [])
        .filter((entry) => entry.status === "FAILED")
        .map((entry) => ({
          id: `log-${entry.id}`,
          basicContent: `${entry.processorName}: ${entry.message ?? "NiFi 처리 오류"}`,
          category: "ETL" as DagCategory,
          datetime: entry.occurredAt,
          fetchLog: async () => entry.message ?? "오류 메시지를 찾을 수 없습니다.",
        })),
      ...nifiJobs
        .filter((job) => job.status === "FAILED")
        .map((job) => ({
          id: `current-${job.id}`,
          basicContent: `${job.name} (현재 오류)`,
          category: "ETL" as DagCategory,
          fetchLog: async () => job.errorMessage ?? "오류 메시지를 찾을 수 없습니다.",
        })),
    ],
    [nifiExecutionLogsQuery.data, nifiJobs],
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

  const selectedPeriodLabel = `${appliedRange[0].format("YYYY.MM.DD")} – ${appliedRange[1].format("YYYY.MM.DD")}`;
  const nifiPeriodTotal = nifiLoadDaily.reduce((sum, point) => sum + point.count, 0);
  const kafkaPeriodTotal = kafkaLoadDaily.reduce((sum, point) => sum + point.count, 0);
  const realtimeMetrics = operationsPipelineQuery.data?.metrics ?? [];
  const kafkaThroughput = realtimeMetrics.reduce((sum, metric) => sum + Number(metric.throughputPerSecond ?? 0), 0);
  const kafkaBacklog = realtimeMetrics.reduce((sum, metric) => sum + Number(metric.consumerLag ?? 0), 0);
  const nifiQueued = nifiJobs.reduce((sum, job) => sum + job.flowFilesQueued, 0);
  const nifiActiveThreads = nifiJobs.reduce((sum, job) => sum + job.activeThreadCount, 0);
  const airflowCompleted = airflowHistory.successEntries.length + airflowHistory.failedEntries.length;
  const airflowSuccessRate =
    airflowCompleted > 0 ? Math.round((airflowHistory.successEntries.length / airflowCompleted) * 1000) / 10 : 0;

  const dailyTrendChart = [
    ...nifiLoadDaily.map((point) => ({ ...point, engine: "NIFI" })),
    ...kafkaLoadDaily.map((point) => ({ ...point, engine: "KAFKA" })),
  ];

  const loadTop5 = [
    ...nifiLoadTop.map((point) => ({ name: point.label, value: point.count, engine: "NIFI" })),
    ...kafkaLoadTop.map((point) => ({ name: point.label, value: point.count, engine: "KAFKA" })),
  ]
    .sort((a, b) => b.value - a.value)
    .slice(0, 5);

  const recentAirflowRuns = [
    ...airflowHistory.runningEntries.map((entry) => ({ ...entry, state: "RUNNING" as const })),
    ...airflowHistory.failedEntries.map((entry) => ({ ...entry, state: "FAILED" as const })),
    ...airflowHistory.successEntries.map((entry) => ({ ...entry, state: "SUCCESS" as const })),
  ]
    .sort((a, b) => new Date(b.datetime ?? 0).getTime() - new Date(a.datetime ?? 0).getTime())
    .slice(0, 10);

  const recentFailureEvents = [
    ...(nifiExecutionLogsQuery.data ?? [])
      .filter((entry) => entry.status === "FAILED")
      .map((entry) => ({
        id: `nifi-${entry.id}`,
        source: "NIFI",
        datetime: entry.occurredAt,
        title: entry.processorName,
        message: entry.message ?? "NiFi 처리 오류",
      })),
    ...airflowHistory.failedEntries.map((entry) => ({
      id: `airflow-${entry.id}`,
      source: "AIRFLOW",
      datetime: entry.datetime,
      title: entry.basicContent,
      message: "Airflow 실행 실패",
    })),
  ]
    .sort((a, b) => new Date(b.datetime ?? 0).getTime() - new Date(a.datetime ?? 0).getTime())
    .slice(0, 10);

  const selectedDataLoading =
    (nifiLoadQuery.isLoading && !nifiLoadQuery.data) ||
    (kafkaLoadQuery.isLoading && !kafkaLoadQuery.data) ||
    showAirflowInitialLoading;

  return (
    <div className="dashboard-page">
      <header className="dashboard-filter-bar">
        <div>
          <h2>통합 운영 대시보드</h2>
          <p>
            처리·실행 이력은 <strong>{selectedPeriodLabel}</strong> 기준이며, 큐와 실행 상태는 현재 기준입니다.
          </p>
        </div>
        <Space className="dashboard-date-filter" wrap>
          <RangePicker
            value={dateRange}
            onChange={(value) => {
              if (value && value[0] && value[1]) {
                setDateRange([value[0], value[1]]);
              }
            }}
            allowClear={false}
          />
          <Button type="primary" loading={selectedDataLoading} onClick={() => setAppliedRange(dateRange)}>
            검색
          </Button>
        </Space>
      </header>

      <section className="dashboard-summary-grid">
        <Card
          className="dashboard-summary-card dashboard-summary-card--cdc"
          loading={kafkaLoadQuery.isLoading && !kafkaLoadQuery.data}
        >
          <div className="summary-card-heading">
            <span>스트림 · CDC</span>
            <button type="button" className="summary-status-button" onClick={() => setKafkaFailedOpen(true)}>
              <Tag color={kafkaFailed > 0 ? "error" : "success"}>
                {kafkaFailed > 0 ? `장애 ${kafkaFailed}` : `정상 ${kafkaRunning}/${kafkaConnectors.length}`}
              </Tag>
            </button>
          </div>
          <div className="summary-card-value" title={`${formatCount(kafkaPeriodTotal)}건`}>
            {formatCompactCount(kafkaPeriodTotal)}
            <small>건</small>
          </div>
          <div className="summary-card-caption">적재 건수</div>
          <div className="summary-card-details">
            <span>현재 처리율 <strong>{formatCompactCount(Math.round(kafkaThroughput))}</strong> rows/s</span>
            <span>미처리 <strong>{formatCompactCount(kafkaBacklog)}</strong></span>
            <span>일시정지 <strong>{kafkaPaused}</strong></span>
          </div>
        </Card>

        <Card
          className="dashboard-summary-card dashboard-summary-card--nifi"
          loading={nifiLoadQuery.isLoading && !nifiLoadQuery.data}
        >
          <div className="summary-card-heading">
            <span>플로우 · NiFi</span>
            <button type="button" className="summary-status-button" onClick={() => setNifiFailedOpen(true)}>
              <Tag color={nifiFailed > 0 ? "error" : "success"}>
                {nifiFailed > 0 ? `오류 그룹 ${nifiFailed}` : "정상"}
              </Tag>
            </button>
          </div>
          <div className="summary-card-value" title={`${formatCount(nifiPeriodTotal)}건`}>
            {formatCompactCount(nifiPeriodTotal)}
            <small>건</small>
          </div>
          <div className="summary-card-caption">적재 건수</div>
          <div className="summary-card-details">
            <span>활성 스레드 <strong>{nifiActiveThreads}</strong></span>
            <span>대기 <strong>{formatCompactCount(nifiQueued)}</strong> FlowFiles</span>
            <span>실행 그룹 <strong>{nifiRunning}</strong></span>
            <span>중지 <strong>{nifiStopped}</strong></span>
          </div>
        </Card>

        <Card className="dashboard-summary-card dashboard-summary-card--airflow" loading={showAirflowInitialLoading}>
          <div className="summary-card-heading">
            <span>배치 · Airflow</span>
            <Tag color={airflowHistory.failedEntries.length > 0 ? "warning" : "success"}>
              실패 {airflowHistory.failedEntries.length}
            </Tag>
          </div>
          <div className="summary-card-value">
            {airflowSuccessRate}
            <small>% 성공</small>
          </div>
          <div className="summary-card-caption">완료 실행 기준</div>
          <div className="summary-card-details">
            <button type="button" onClick={() => setAirflowActiveTile("success")}>
              성공 <strong>{airflowHistory.successEntries.length}</strong>
            </button>
            <button type="button" onClick={() => setAirflowActiveTile("running")}>
              실행 중 <strong>{airflowHistory.runningEntries.length}</strong>
            </button>
            <button type="button" onClick={() => setAirflowActiveTile("failed")}>
              실패 <strong>{airflowHistory.failedEntries.length}</strong>
            </button>
          </div>
          <div className="airflow-run-strip" aria-label="최근 Airflow 실행 상태">
            {recentAirflowRuns.slice(0, 10).map((entry) => (
              <span key={entry.id} className={`airflow-run-block airflow-run-block--${entry.state.toLowerCase()}`} />
            ))}
          </div>
        </Card>
      </section>

      <div className="dashboard-main-grid">
        <main className="dashboard-primary-column">
          <Card
            className="dashboard-panel dashboard-panel--wide"
            title={`일별 처리 건수 추이 · ${selectedPeriodLabel}`}
            loading={selectedDataLoading}
          >
            {dailyTrendChart.length > 0 ? (
              <Line data={dailyTrendChart} xField="date" yField="count" colorField="engine" height={270} />
            ) : (
              <div className="empty-chart-placeholder">
                <Empty description="조회 기간에 적재 이력이 없습니다." />
              </div>
            )}
          </Card>

          <div className="dashboard-chart-pair">
            <Card className="dashboard-panel" title="파이프라인별 적재 건수 Top 5" loading={selectedDataLoading}>
              {loadTop5.length > 0 ? (
                <Column
                  data={loadTop5}
                  xField="name"
                  yField="value"
                  colorField="engine"
                  height={245}
                  axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                />
              ) : (
                <div className="empty-chart-placeholder">
                  <Empty description="조회 기간에 적재 이력이 없습니다." />
                </div>
              )}
            </Card>

            <Card
              className="dashboard-panel"
              title="현재 Job별 대기 FlowFile Top 5"
              loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}
            >
              {nifiQueueChartData.some((point) => point.queued > 0) ? (
                <Column
                  data={nifiQueueChartData}
                  xField="name"
                  yField="queued"
                  height={245}
                  axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
                />
              ) : (
                <div className="empty-chart-placeholder">
                  <Empty description="현재 대기 중인 FlowFile이 없습니다." />
                </div>
              )}
            </Card>
          </div>

          <Card className="dashboard-panel" title="Airflow 태스크 수행시간 Top 5" loading={showAirflowInitialLoading}>
            {airflowHistory.topDurationTasks.length > 0 ? (
              <Column
                data={airflowHistory.topDurationTasks}
                xField="taskKey"
                yField="minutes"
                height={235}
                axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
              />
            ) : (
              <div className="empty-chart-placeholder">
                <Empty description="조회 기간에 실행 이력이 없습니다." />
              </div>
            )}
          </Card>
        </main>

        <aside className="dashboard-side-column">
          <Card
            className="dashboard-feed-card"
            title="배치 · 최근 실행"
            extra={
              <Button type="link" size="small" onClick={() => setAirflowActiveTile("dag")}>
                DAG {airflowHistory.dagCount}개
              </Button>
            }
            loading={showAirflowInitialLoading}
          >
            {recentAirflowRuns.length > 0 ? (
              <div className="dashboard-feed-list">
                {recentAirflowRuns.map((entry) => (
                  <button
                    type="button"
                    key={entry.id}
                    className="dashboard-feed-row"
                    onClick={() =>
                      setAirflowActiveTile(
                        entry.state === "FAILED" ? "failed" : entry.state === "RUNNING" ? "running" : "success",
                      )
                    }
                  >
                    <span className={`feed-state-dot feed-state-dot--${entry.state.toLowerCase()}`} />
                    <span className="feed-row-content">
                      <strong>{entry.basicContent}</strong>
                      <small>{formatDashboardDateTime(entry.datetime)}</small>
                    </span>
                    <Tag color={entry.state === "FAILED" ? "error" : entry.state === "RUNNING" ? "processing" : "success"}>
                      {entry.state === "FAILED" ? "실패" : entry.state === "RUNNING" ? "실행중" : "성공"}
                    </Tag>
                  </button>
                ))}
              </div>
            ) : (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="조회 기간에 실행 이력이 없습니다." />
            )}
          </Card>

          <Card
            className="dashboard-feed-card dashboard-feed-card--issues"
            title="장애 이벤트"
            extra={
              commandSummary && commandSummary.connectorDrift.length > 0 ? (
                <Tag color="warning">커넥터 불일치 {commandSummary.connectorDrift.length}</Tag>
              ) : null
            }
            loading={(nifiExecutionLogsQuery.isLoading && !nifiExecutionLogsQuery.data) || showAirflowInitialLoading}
          >
            {recentFailureEvents.length > 0 ? (
              <div className="dashboard-feed-list">
                {recentFailureEvents.map((event) => (
                  <button
                    type="button"
                    key={event.id}
                    className="dashboard-event-row"
                    onClick={() => (event.source === "NIFI" ? setNifiFailedOpen(true) : setAirflowActiveTile("failed"))}
                  >
                    <span className="event-time">{formatDashboardDateTime(event.datetime)}</span>
                    <Tag color="error">{event.source}</Tag>
                    <span className="event-content">
                      <strong>{event.title}</strong>
                      <small>{event.message}</small>
                    </span>
                  </button>
                ))}
              </div>
            ) : (
              <div className="dashboard-no-issues">
                <span>✓</span>
                <strong>조회 기간에 수집된 장애가 없습니다.</strong>
              </div>
            )}
          </Card>
        </aside>
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
        title={`NiFi 오류 상세 · ${selectedPeriodLabel}`}
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

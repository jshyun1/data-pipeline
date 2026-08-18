import { useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, DatePicker, Empty, Space, Tooltip } from "antd";
import { useNavigate } from "react-router-dom";
import { Column } from "@ant-design/plots";
import dayjs, { type Dayjs } from "dayjs";
import {
  getDailyLoadSummary,
  getHourlyLoadSummary,
  getPipelineDashboardSummary,
  getRealtimePipelineMetrics,
  type HourlyLoadPointResponse,
} from "../api/dashboard";
import { getHostResources, getProcessHealth } from "../api/infra";
import {
  getAirflowTaskLog,
  getKafkaConnectorTrace,
  getNifiBulletins,
  getNifiRootStatus,
  listAirflowDags,
  listAirflowTaskInstances,
  listAllAirflowDagRuns,
  listKafkaConnectorsWithStatus,
  listNifiExecutionLogs,
} from "../api/platform";
import type { AirflowDagRun } from "../api/platform";
import { listPipelines } from "../api/pipelines";
import type { PipelineResponse } from "../types/pipeline";
import { collectNifiJobs, extractErrorGroupMessages } from "../utils/nifiJobs";
import { summarizeKafkaConnectors } from "../utils/kafkaConnectors";
import { categorizeDag, NIFI_METRICS_COLLECTOR_DAG_ID, resolveDagDisplayName, type DagCategory } from "../utils/dagHistory";
import { HistoryModal, type HistoryEntry } from "../components/HistoryModal";
import { InfraRegion } from "../components/InfraRegion";
import { ActionQueuePanel } from "../components/ActionQueuePanel";
import { JobTop5Card } from "../components/JobTop5Card";
import { ChangeIndicator, FlowStateNote, MetricRow, NowDivider, TimeBadge } from "../components/kpi/kpiBits";
import { formatRecovery, judgeCdcFlow } from "../components/kpi/flowState";

const { RangePicker } = DatePicker;

// 계열색: 상단 요약 카드의 강조색과 같은 값을 쓴다(같은 엔진이면 화면 어디서나 같은 색).
// 상태색(정상/경고/중단)은 예약색이라 계열색으로 절대 쓰지 않는다.
const CDC_COLOR = "#2878d0";

interface DagInfo {
  dagId: string;
  category: DagCategory;
  displayName: string;
  isActive: boolean;
}

interface AirflowHistoryData {
  dagCount: number;
  dagCatalog: HistoryEntry[];
  runningEntries: HistoryEntry[];
  successEntries: HistoryEntry[];
  failedEntries: HistoryEntry[];
}

const EMPTY_AIRFLOW_HISTORY: AirflowHistoryData = {
  dagCount: 0,
  dagCatalog: [],
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

// Airflow 제어 DAG의 실행 이력(성공/실패/실행중) - 이건 "Airflow가 보낸 시작/중지 명령
// 자체가 잘 처리됐는지"를 보여줄 뿐, 그 뒤 NiFi/Kafka가 실제로 데이터를 잘 처리했는지는
// 반영하지 않는다(그건 통계 구역의 적재 건수와 인프라 구역의 프로세스 현황이 담당).
async function buildAirflowHistory(range: [Dayjs, Dayjs]): Promise<AirflowHistoryData> {
  const [dags, allRuns, nifiStatusResult, kafkaPipelines] = await Promise.all([
    listAirflowDags(),
    // 전체 DAG의 기간 내 실행을 한 번에 받는다. 예전에는 DAG마다 따로 물어봐서
    // 파이프라인 수에 비례해 요청이 늘었다(DAG 12개 기준 30초마다 48회 -> 2회).
    listAllAirflowDagRuns({
      startDateGte: range[0].startOf("day").toISOString(),
      startDateLte: range[1].endOf("day").toISOString(),
    }).catch(() => [] as AirflowDagRun[]),
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
  const displayNameByDag = new Map(dagInfos.map((info) => [info.dagId, info]));

  // 응답이 이미 start_date 내림차순이라, DAG별 첫 항목이 그 DAG의 최근 실행이다.
  const runsByDag = new Map<string, AirflowDagRun[]>();
  allRuns.forEach((run) => {
    const dagId = run.dag_id;
    if (!dagId) {
      return;
    }
    const bucket = runsByDag.get(dagId);
    if (bucket) {
      bucket.push(run);
    } else {
      runsByDag.set(dagId, [run]);
    }
  });

  const dagCatalog: HistoryEntry[] = dagInfos.map((info) => {
    const latest = runsByDag.get(info.dagId)?.[0];
    return {
      id: info.dagId,
      basicContent: info.displayName,
      category: info.category,
      datetime: latest?.start_date ?? latest?.execution_date,
      fetchLog: latest ? () => fetchRunLogText(info.dagId, latest.dag_run_id) : undefined,
    };
  });

  const runningEntries: HistoryEntry[] = [];
  const successEntries: HistoryEntry[] = [];
  const failedEntries: HistoryEntry[] = [];

  allRuns.forEach((run) => {
    const dagId = run.dag_id;
    // 1분마다 도는 nifi_pipelines_metrics_collector는 알려진 executor 버그로 매번
    // failed로 찍히는 노이즈라 성공/실패 집계에서 제외한다(DAG 목록에는 그대로 둔다).
    if (!dagId || dagId === NIFI_METRICS_COLLECTOR_DAG_ID) {
      return;
    }
    const info = displayNameByDag.get(dagId);
    if (!info) {
      return;
    }
    const entry: HistoryEntry = {
      id: `${dagId}:${run.dag_run_id}`,
      basicContent: info.displayName,
      category: info.category,
      datetime: run.start_date ?? run.execution_date,
      fetchLog: () => fetchRunLogText(dagId, run.dag_run_id),
    };
    if (run.state === "success") {
      successEntries.push(entry);
    } else if (run.state === "failed") {
      failedEntries.push(entry);
    } else if (run.state === "running" || run.state === "queued") {
      runningEntries.push(entry);
    }
  });

  return {
    dagCount: dagInfos.length,
    dagCatalog,
    runningEntries,
    successEntries,
    failedEntries,
  };
}

type AirflowTileKind = "dag" | "running" | "success" | "failed";

const AIRFLOW_TILE_TITLE: Record<AirflowTileKind, string> = {
  dag: "DAG 목록",
  running: "실행중 내역",
  success: "성공 내역",
  failed: "실패 내역",
};

function formatLoadDate(date: string) {
  return dayjs(date).format("MM.DD");
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

const HOURS_PER_BUCKET = 6;
const BUCKET_COUNT = 24 / HOURS_PER_BUCKET;

/** 00~06시 … 18~24시. 정렬이 곧 시간 순서라 범례도 이 순서로 나온다. */
const HOUR_BUCKET_LABELS = Array.from({ length: BUCKET_COUNT }, (_, index) => {
  const start = index * HOURS_PER_BUCKET;
  return `${String(start).padStart(2, "0")}~${String(start + HOURS_PER_BUCKET).padStart(2, "0")}시`;
});

/**
 * 날짜 × 시간대 원본을 "날짜별 6시간 구간" 막대 데이터로 만든다.
 *
 * <p>백엔드는 건수가 0인 조합을 빼고 주므로 여기서 날짜×구간 격자를 채운다 - 안 그러면
 * 날짜마다 막대 개수가 달라져서 같은 구간이 서로 다른 x 위치에 그려진다.
 */
function toDailyHourBuckets(points: HourlyLoadPointResponse[]) {
  const byDate = new Map<string, Map<string, number>>();
  points.forEach((point) => {
    const label = HOUR_BUCKET_LABELS[Math.floor(point.hour / HOURS_PER_BUCKET)];
    if (!label) {
      return;
    }
    const buckets = byDate.get(point.date) ?? new Map<string, number>();
    buckets.set(label, (buckets.get(label) ?? 0) + point.count);
    byDate.set(point.date, buckets);
  });

  return Array.from(byDate.keys())
    .sort()
    .flatMap((date) =>
      HOUR_BUCKET_LABELS.map((bucket) => ({
        date: formatLoadDate(date),
        bucket,
        count: byDate.get(date)?.get(bucket) ?? 0,
      })),
    );
}

interface ChartPanelProps {
  title: string;
  loading: boolean;
  hasData: boolean;
  emptyText: string;
  children: ReactNode;
}

/**
 * 차트는 카드가 남긴 높이를 그대로 채운다(스크롤 없이 한 화면에 다 들어가야 하므로
 * 고정 높이를 쓰지 않는다). autoFit이 컨테이너 크기를 읽을 수 있도록 실제 크기가 있는
 * 상자를 하나 두고 그 안에 절대배치한다.
 */
function ChartPanel({ title, loading, hasData, emptyText, children }: ChartPanelProps) {
  return (
    <Card className="dashboard-panel dashboard-chart-card" title={title} loading={loading}>
      {hasData ? (
        <div className="chart-fit-shell">{children}</div>
      ) : (
        <div className="empty-chart-placeholder">
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={emptyText} />
        </div>
      )}
    </Card>
  );
}

/** 막대 공통 스펙: 24px 상한, 데이터 끝만 4px 라운드, 이웃 막대 사이 2px 여백. */
function barStyle(fill?: string) {
  return {
    ...(fill ? { fill } : {}),
    maxWidth: 24,
    radiusTopLeft: 4,
    radiusTopRight: 4,
    insetLeft: 1,
    insetRight: 1,
  };
}

const COUNT_TOOLTIP = { items: [{ channel: "y" as const, valueFormatter: (value: number) => `${formatCount(value)}건` }] };

// y축 눈금은 축약(12.3만)으로 두고 정확한 값은 hover 툴팁이 담당한다. 막대마다 숫자를
// 얹으면 24개 시간대 차트가 숫자로 뒤덮인다.
const COUNT_Y_AXIS = { labelFormatter: (value: number) => formatCompactCount(value) };
const NAME_X_AXIS = { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true };
// 시간대는 순서가 있는 값(0시 -> 24시)이라 서로 다른 색이 아니라 한 계열색의 명도
// 단계로 표현한다. 엔진별 계열색(NiFi 빨강 / CDC 파랑)은 그대로 유지된다.
//
// 4구간이라 단계마다 밝기를 크게 벌려서 서로 확실히 구분되게 잡았다(밝기가 단조
// 감소하므로 색이 안 보이는 환경에서도 이른 시간 -> 늦은 시간 순서로 읽힌다).
const NIFI_HOUR_RANGE = ["#ed979a", "#e05256", "#bf2227", "#791519"];
const CDC_HOUR_RANGE = ["#87b6e8", "#3e8ada", "#2164ab", "#143e6b"];

function hourBucketScale(range: string[]) {
  return { color: { domain: HOUR_BUCKET_LABELS, range } };
}

const HOUR_BUCKET_LEGEND = {
  color: { position: "top" as const, layout: { justifyContent: "flex-end" }, itemLabelFontSize: 11 },
};

export function DashboardPage() {
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);
  const navigate = useNavigate();
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

  const nifiHourlyQuery = useQuery({
    queryKey: ["dashboard-nifi-hourly", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () =>
      getHourlyLoadSummary(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD"), "NIFI"),
    refetchInterval: SUMMARY_REFETCH_INTERVAL,
    placeholderData: (previousData) => previousData,
  });

  const kafkaHourlyQuery = useQuery({
    queryKey: ["dashboard-kafka-hourly", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () =>
      getHourlyLoadSummary(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD"), "KAFKA"),
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

  // 서버 리소스/프로세스는 인프라 구역을 보고 있을 때만 폴링한다(통계만 보는 동안
  // 15초마다 NiFi/Kafka/Airflow를 헛되게 두드리지 않도록).
  const hostResourcesQuery = useQuery({
    queryKey: ["dashboard-host-resources"],
    queryFn: getHostResources,
    refetchInterval: 15000,
    placeholderData: (previousData) => previousData,
  });

  // 프로세스 확인 한 번에 Kafka/NiFi/Airflow를 다 두드리고 NiFi는 호출마다 토큰을 새로
  // 받으므로(bcrypt 검증) 리소스 조회보다 주기를 길게 잡는다 - 생존 확인엔 30초로 충분하다.
  const processHealthQuery = useQuery({
    queryKey: ["dashboard-process-health"],
    queryFn: getProcessHealth,
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

  const kafkaFailed = kafkaConnectors.filter((job) => job.status === "FAILED").length;
  const kafkaPaused = kafkaConnectors.filter((job) => job.status === "PAUSED").length;

  const nifiLoadDaily = useMemo(
    () => (nifiLoadQuery.data?.daily ?? []).map((point) => ({ date: formatLoadDate(point.date), count: point.count })),
    [nifiLoadQuery.data],
  );
  const kafkaLoadDaily = useMemo(
    () => (kafkaLoadQuery.data?.daily ?? []).map((point) => ({ date: formatLoadDate(point.date), count: point.count })),
    [kafkaLoadQuery.data],
  );
  const kafkaLoadTop = useMemo(
    () => (kafkaLoadQuery.data?.topPipelines ?? []).map((point) => ({ label: point.label, count: point.count })),
    [kafkaLoadQuery.data],
  );
  const nifiHourly = useMemo(() => toDailyHourBuckets(nifiHourlyQuery.data?.hourly ?? []), [nifiHourlyQuery.data]);
  const kafkaHourly = useMemo(() => toDailyHourBuckets(kafkaHourlyQuery.data?.hourly ?? []), [kafkaHourlyQuery.data]);

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

  const recentAirflowRuns = [
    ...airflowHistory.runningEntries.map((entry) => ({ ...entry, state: "RUNNING" as const })),
    ...airflowHistory.failedEntries.map((entry) => ({ ...entry, state: "FAILED" as const })),
    ...airflowHistory.successEntries.map((entry) => ({ ...entry, state: "SUCCESS" as const })),
  ]
    .sort((a, b) => new Date(b.datetime ?? 0).getTime() - new Date(a.datetime ?? 0).getTime())
    .slice(0, 10);

  // 원본 5-2 "전일 대비 증감". 일별 시계열의 마지막 날과 그 전날을 비교한다.
  // 하루치밖에 없으면 previous 를 null 로 둬서 "비교 불가"로 표시한다(0%로 속이지 않는다).
  function previousDayCount(points: Array<{ count: number }>): number | null {
    return points.length >= 2 ? points[points.length - 2].count : null;
  }
  function lastDayCount(points: Array<{ count: number }>): number {
    return points.length >= 1 ? points[points.length - 1].count : 0;
  }

  // 원본 5-2 "처리율 0에 맥락 부여" + "#4 해소 예상 시간".
  const cdcFlow = judgeCdcFlow(realtimeMetrics);
  const cdcRecovery = formatRecovery(realtimeMetrics);
  const nifiFlowText =
    nifiActiveThreads > 0
      ? `실행 중 ${nifiActiveThreads}개 작업`
      : nifiQueued > 0
        ? "대기 데이터가 남았는데 실행 중인 작업 없음 — 확인 필요"
        : "대기 데이터 없음 (정상)";
  const nifiFlowTone: "ok" | "warn" = nifiActiveThreads === 0 && nifiQueued > 0 ? "warn" : "ok";

  const connectorDriftCount = commandSummary?.connectorDrift.length ?? 0;

  const nifiLoading = nifiLoadQuery.isLoading && !nifiLoadQuery.data;
  const kafkaLoading = kafkaLoadQuery.isLoading && !kafkaLoadQuery.data;
  const nifiHourlyLoading = nifiHourlyQuery.isLoading && !nifiHourlyQuery.data;
  const kafkaHourlyLoading = kafkaHourlyQuery.isLoading && !kafkaHourlyQuery.data;
  const selectedDataLoading = nifiLoading || kafkaLoading || showAirflowInitialLoading;

  return (
    <div className="dashboard-page">
      <header className="dashboard-filter-bar">
        <div className="dashboard-filter-heading">
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

      <div className="dashboard-body">
      <main className="dashboard-main">
      {/* 원본 5-1: 조치 대기열은 KPI 카드보다 위, 대시보드 최상단이다.
          "화면을 보고 무엇을 해야 하는지가 나오게 한다"가 이 배치의 이유다. */}
      <ActionQueuePanel />
      <section className="dashboard-summary-grid">
        {/* 원본 5-2 KPI 카드 재구성: 누적형(기간 배지) 위 / 진행형("지금" 구분선) 아래 */}
        <Card className="dashboard-summary-card dashboard-summary-card--cdc" loading={kafkaLoading}>
          <div className="summary-card-heading">
            <span>CDC</span>
            <TimeBadge label={selectedPeriodLabel} />
          </div>
          <div className="summary-card-value" title={`${formatCount(kafkaPeriodTotal)}건`}>
            {formatCompactCount(kafkaPeriodTotal)}
            <small>건 적재</small>
          </div>
          <div className="summary-card-change">
            <ChangeIndicator current={lastDayCount(kafkaLoadDaily)} previous={previousDayCount(kafkaLoadDaily)} />
            <span className="summary-card-change-label">전일 대비</span>
          </div>

          <NowDivider />
          <MetricRow
            label="미처리"
            value={formatCompactCount(kafkaBacklog)}
            warn={kafkaBacklog > 0}
            hint="Kafka consumer lag 합계 — 아직 타깃에 반영되지 않은 건수"
          />
          <MetricRow label="해소 예상" value={cdcRecovery.text} warn={cdcRecovery.warn} />
          <MetricRow label="처리율" value={`${kafkaThroughput.toFixed(1)} rows/s`} />
          <MetricRow label="일시정지" value={String(kafkaPaused)} warn={kafkaPaused > 0} />
          {connectorDriftCount > 0 ? (
            <MetricRow label="커넥터 불일치" value={String(connectorDriftCount)} warn />
          ) : null}
          <FlowStateNote tone={cdcFlow.tone} text={cdcFlow.text} />

          <div className="summary-card-links">
            {kafkaFailed > 0 ? (
              <Button size="small" danger onClick={() => setKafkaFailedOpen(true)}>
                장애 {kafkaFailed}건
              </Button>
            ) : null}
            <Button size="small" onClick={() => navigate("/cdc/logs")}>
              처리 로그 →
            </Button>
          </div>
        </Card>

        <Card className="dashboard-summary-card dashboard-summary-card--nifi" loading={nifiLoading}>
          <div className="summary-card-heading">
            <span>ETL</span>
            <TimeBadge label={selectedPeriodLabel} />
          </div>
          <div className="summary-card-value" title={`${formatCount(nifiPeriodTotal)}건`}>
            {formatCompactCount(nifiPeriodTotal)}
            <small>건 적재</small>
          </div>
          <div className="summary-card-change">
            <ChangeIndicator current={lastDayCount(nifiLoadDaily)} previous={previousDayCount(nifiLoadDaily)} />
            <span className="summary-card-change-label">전일 대비</span>
          </div>

          <NowDivider />
          {/* 원본 #9 용어 정리: 활성 스레드 → 실행 중 작업, 대기 FlowFiles → 대기 데이터 */}
          <MetricRow
            label="실행 중 작업"
            value={String(nifiActiveThreads)}
            hint="NiFi 활성 스레드 수 — 지금 실제로 돌고 있는 작업"
          />
          <MetricRow
            label="대기 데이터"
            value={formatCompactCount(nifiQueued)}
            warn={nifiQueued > 0 && nifiActiveThreads === 0}
            hint="처리를 기다리며 큐에 쌓인 데이터 건수(FlowFile)"
          />
          <MetricRow label="실행" value={String(nifiRunning)} />
          <MetricRow label="중지" value={String(nifiStopped)} warn={nifiStopped > 0} />
          <FlowStateNote tone={nifiFlowTone} text={nifiFlowText} />

          <div className="summary-card-links">
            {nifiFailed > 0 ? (
              <Button size="small" danger onClick={() => setNifiFailedOpen(true)}>
                오류 그룹 {nifiFailed}건
              </Button>
            ) : null}
            <Button size="small" onClick={() => navigate("/etl/logs")}>
              ETL 로그 →
            </Button>
          </div>
        </Card>

        <Card className="dashboard-summary-card dashboard-summary-card--airflow" loading={showAirflowInitialLoading}>
          <div className="summary-card-heading">
            <span>Airflow</span>
            <TimeBadge label={selectedPeriodLabel} />
          </div>
          <div className="summary-card-value">
            {airflowSuccessRate}
            <small>% 성공 (완료 실행 기준)</small>
          </div>
          <div className="summary-card-change">
            <span className="summary-card-change-label">
              성공 {airflowHistory.successEntries.length} · 실패 {airflowHistory.failedEntries.length}
            </span>
          </div>

          <NowDivider />
          <MetricRow label="실행 중" value={String(airflowHistory.runningEntries.length)} />
          <MetricRow
            label="실패"
            value={String(airflowHistory.failedEntries.length)}
            warn={airflowHistory.failedEntries.length > 0}
          />
          <MetricRow label="DAG" value={String(airflowHistory.dagCount)} />

          {/* 원본 5-2 "Airflow 스트립 의미 불명 → 툴팁" */}
          <div className="airflow-run-strip" aria-label="최근 Airflow 실행 상태">
            {recentAirflowRuns.map((entry) => (
              <Tooltip
                key={entry.id}
                title={`${entry.basicContent} · ${entry.state}${entry.datetime ? ` · ${entry.datetime}` : ""}`}
              >
                <span className={`airflow-run-block airflow-run-block--${entry.state.toLowerCase()}`} />
              </Tooltip>
            ))}
          </div>

          <div className="summary-card-links">
            {airflowHistory.failedEntries.length > 0 ? (
              <Button size="small" danger onClick={() => setAirflowActiveTile("failed")}>
                실패 {airflowHistory.failedEntries.length}건 보기
              </Button>
            ) : null}
            <Button size="small" onClick={() => navigate("/airflow/dashboard")}>
              실행 이력 →
            </Button>
          </div>
        </Card>
      </section>

      <div className="dashboard-stats-grid">
        <ChartPanel
          title="ETL 일별·시간대별 적재 건수"
          loading={nifiHourlyLoading}
          hasData={nifiHourly.some((point) => point.count > 0)}
          emptyText="조회 기간에 적재 이력이 없습니다."
        >
          <Column
            autoFit
            data={nifiHourly}
            xField="date"
            yField="count"
            colorField="bucket"
            transform={[{ type: "dodgeX" }]}
            scale={hourBucketScale(NIFI_HOUR_RANGE)}
            style={barStyle()}
            axis={{ x: NAME_X_AXIS, y: COUNT_Y_AXIS }}
            legend={HOUR_BUCKET_LEGEND}
            tooltip={COUNT_TOOLTIP}
          />
        </ChartPanel>

        <JobTop5Card title="ETL Job Top 5" from={appliedRange[0]} to={appliedRange[1]} />

        <ChartPanel
          title="CDC 일별·시간대별 처리 건수"
          loading={kafkaHourlyLoading}
          hasData={kafkaHourly.some((point) => point.count > 0)}
          emptyText="조회 기간에 처리 이력이 없습니다."
        >
          <Column
            autoFit
            data={kafkaHourly}
            xField="date"
            yField="count"
            colorField="bucket"
            transform={[{ type: "dodgeX" }]}
            scale={hourBucketScale(CDC_HOUR_RANGE)}
            style={barStyle()}
            axis={{ x: NAME_X_AXIS, y: COUNT_Y_AXIS }}
            legend={HOUR_BUCKET_LEGEND}
            tooltip={COUNT_TOOLTIP}
          />
        </ChartPanel>

        <ChartPanel
          title="CDC Job 처리 건수 Top 5"
          loading={kafkaLoading}
          hasData={kafkaLoadTop.length > 0}
          emptyText="조회 기간에 처리 이력이 없습니다."
        >
          <Column
            autoFit
            data={kafkaLoadTop}
            xField="label"
            yField="count"
            style={barStyle(CDC_COLOR)}
            axis={{ x: NAME_X_AXIS, y: COUNT_Y_AXIS }}
            tooltip={COUNT_TOOLTIP}
          />
        </ChartPanel>
      </div>
      </main>

      <InfraRegion
        resources={hostResourcesQuery.data}
        resourcesLoading={hostResourcesQuery.isLoading && !hostResourcesQuery.data}
        processes={processHealthQuery.data?.groups}
        processesLoading={processHealthQuery.isLoading && !processHealthQuery.data}
        /* 원본 5-4: 프로세스 헬스와 데이터 흐름 헬스는 다른 축이다.
           판정은 KPI 카드가 이미 했으므로 그 결과를 넘기기만 한다(중복 구현 금지). */
        dataFlow={cdcFlow}
      />
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

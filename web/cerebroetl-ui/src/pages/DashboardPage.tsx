import { useMemo, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, ConfigProvider, DatePicker } from "antd";
import { LoadingOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import { ChartEmptyState } from "../components/ChartEmptyState";
import { cerebroBrandTheme } from "../theme/cerebro";
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
  getNifiBulletins,
  getNifiRootStatus,
  listAirflowDags,
  listAirflowTaskInstances,
  listAllAirflowDagRuns,
  listKafkaConnectorsWithStatus,
  listNifiExecutionLogs,
  listNifiProcessorRuns,
} from "../api/platform";
import type { AirflowDagRun } from "../api/platform";
import { listPipelines } from "../api/pipelines";
import type { PipelineResponse } from "../types/pipeline";
import { listCdcEventLogs, listCdcProcessingLogs } from "../api/cdcLogs";
import { listWorkflows } from "../api/workflows";
import { collectNifiJobs, extractErrorGroupMessages } from "../utils/nifiJobs";
import { summarizeKafkaConnectors } from "../utils/kafkaConnectors";
import { FailureDetailModal, type FailureDetailItem } from "../components/FailureDetailModal";
import { TaskFailureLogModal, type FailureLogTarget } from "../components/TaskFailureLogModal";
import { categorizeDag, NIFI_METRICS_COLLECTOR_DAG_ID, resolveDagDisplayName, type DagCategory } from "../utils/dagHistory";
import { HistoryModal, type HistoryEntry } from "../components/HistoryModal";
import { InfraRegion } from "../components/InfraRegion";
import { ActionQueuePanel } from "../components/ActionQueuePanel";
import { refitAfterLayout } from "../components/chartFit";
import { activeDayPreset, dayPresetRange } from "../components/dayPreset";
import { JobTop5Card } from "../components/JobTop5Card";
import { ChangeIndicator, MetricRow, NowDivider, TimeBadge } from "../components/kpi/kpiBits";
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
  const [dags, allRuns, nifiStatusResult, kafkaPipelines, workflows] = await Promise.all([
    listAirflowDags(),
    // 전체 DAG의 기간 내 실행을 한 번에 받는다. 예전에는 DAG마다 따로 물어봐서
    // 파이프라인 수에 비례해 요청이 늘었다(DAG 12개 기준 30초마다 48회 -> 2회).
    listAllAirflowDagRuns({
      startDateGte: range[0].startOf("day").toISOString(),
      startDateLte: range[1].endOf("day").toISOString(),
    }).catch(() => [] as AirflowDagRun[]),
    getNifiRootStatus().catch(() => undefined),
    listPipelines().catch(() => [] as PipelineResponse[]),
    // 워크플로우 DAG(etl_wf_*)는 dag_id 를 그대로 보여 주면 «etl_wf_etl_root_total» 이 된다 -
    // 워크플로우 로그 화면과 같은 이름(«total»)으로 적으려고 목록을 같이 받는다(2026-09-28).
    listWorkflows().catch(() => []),
  ]);
  const workflowNameByDag = new Map(
    workflows.filter((workflow) => workflow.dagId).map((workflow) => [workflow.dagId as string, workflow.name]),
  );

  const nifiJobs = collectNifiJobs(nifiStatusResult?.processGroupStatus?.aggregateSnapshot?.processGroupStatusSnapshots);

  const dagInfos: DagInfo[] = dags.map((dag) => {
    const category = categorizeDag(dag.dag_id);
    return {
      dagId: dag.dag_id,
      category,
      displayName: workflowNameByDag.get(dag.dag_id)
        ?? resolveDagDisplayName(dag.dag_id, category, nifiJobs, kafkaPipelines),
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
    // CDC 제어 DAG(kafka_pipeline_*_control) 실행은 이 카드에서 빼고 CDC 카드가 CDC 로그 기준으로 보여 준다
    // (2026-09-28). 예전에는 같은 실패가 두 카드에 겹쳐 잡혔고, «실행 중»에는 CDC 감시 Run 이 계속 쌓였다.
    if (info.category === "CDC") {
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
        <ChartEmptyState text={emptyText} />
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
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().startOf("day"), dayjs().endOf("day")]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);

  // 당일/전일 프리셋: 해당 날짜 하루(00:00~23:59)로 조회기간을 즉시 맞춘다.
  const applyDayPreset = (offsetDays: 0 | 1) => {
    const range = dayPresetRange(offsetDays);
    setDateRange(range);
    setAppliedRange(range);
  };
  // 지금 적용된 기간이 어느 프리셋인지(달력으로 직접 고르면 null) - 버튼 선택 표시에 쓴다.
  const selectedPreset = activeDayPreset(appliedRange[0], appliedRange[1]);
  const navigate = useNavigate();
  const [airflowActiveTile, setAirflowActiveTile] = useState<AirflowTileKind | null>(null);
  const [nifiFailedOpen, setNifiFailedOpen] = useState(false);
  const [kafkaFailedOpen, setKafkaFailedOpen] = useState(false);
  /** Airflow 실패 줄에서 연 실패 로그 창. 워크플로우 로그 화면과 <b>같은 창</b>을 쓴다(2026-09-21 요청). */
  const [workflowFailure, setWorkflowFailure] = useState<FailureLogTarget | undefined>();
  const [workflowFailureLoading, setWorkflowFailureLoading] = useState<string | null>(null);

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

  // CDC 로그 화면과 같은 재료를 같은 기간으로 받는다 - 두 화면이 같은 실패를 세게 하려고(2026-09-28).
  const cdcEventLogsQuery = useQuery({
    queryKey: ["dashboard-cdc-events", appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")],
    queryFn: () => listCdcEventLogs(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")),
    refetchInterval: 30000,
    placeholderData: (previousData) => previousData,
  });

  const cdcProcessingLogsQuery = useQuery({
    queryKey: ["dashboard-cdc-processing", appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")],
    queryFn: () => listCdcProcessingLogs(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")),
    refetchInterval: 30000,
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

  // ETL 카드 «실행 중 · 성공» 칸 - ETL 로그 화면 상단과 같은 원본(적재 프로세서 실행 구간)을 같은 기간으로 센다(2026-10-12).
  const nifiProcessorRunsQuery = useQuery({
    queryKey: ["dashboard-nifi-processor-runs", appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")],
    queryFn: () =>
      listNifiProcessorRuns(appliedRange[0].format("YYYY-MM-DD"), appliedRange[1].format("YYYY-MM-DD")),
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

  const nifiStopped = nifiJobs.filter((job) => job.status === "STOPPED").length;
  // ETL 로그 화면 상단 숫자와 같은 기준: 실행 중 = 아직 안 끝난 적재 실행 구간, 성공 = 끝난 구간, 실패 = 실패 이벤트(nifiFailedItems).
  const nifiRuns = nifiProcessorRunsQuery.data ?? [];
  const nifiRunsRunning = nifiRuns.filter((run) => run.status === "RUNNING").length;
  const nifiRunsSucceeded = nifiRuns.length - nifiRunsRunning;

  // 카드에 쓰는 «장애 N건»은 조회 기간의 실패 건수다(아래 cdcFailedItems 와 같은 것).
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

  /**
   * ETL 실패. ETL 로그 화면의 실패 줄과 같은 항목(그룹 · job · 프로세서 · 시각)을 먼저 적고 원문을 보여 준다.
   * GPT 해석은 job 이 특정되는 줄에만 붙인다 - 어느 job 인지 모르면 서버가 모을 정보가 없다.
   */
  const nifiFailedItems: FailureDetailItem[] = useMemo(
    () => [
      ...(nifiExecutionLogsQuery.data ?? [])
        .filter((entry) => entry.status === "FAILED")
        .map((entry) => ({
          id: `log-${entry.id}`,
          // 대상은 job 명으로 적는다 - ETL 로그 화면의 «job» 칸과 같은 값이어야 한다(2026-09-28).
          title: entry.jobName ?? entry.groupName ?? "ETL",
          datetime: entry.occurredAt,
          lines: [
            ["그룹", entry.rootGroupName ? `${entry.rootGroupName} > ${entry.groupName ?? "-"}` : entry.groupName ?? "-"],
            ["job", entry.jobName ?? "-"],
            ["프로세서", entry.processorName],
          ] as Array<[string, string]>,
          loadError: async () => entry.message ?? "오류 메시지를 찾을 수 없습니다.",
          gpt: entry.jobId
            ? { targetType: "ETL_JOB" as const, target: { jobId: String(entry.jobId) } }
            : undefined,
        })),
    ],
    // 여기에 «지금 Invalid 인 프로세스 그룹»을 같이 넣었었는데 뺐다(2026-09-28) - 그것은 실행 실패가 아니라
    // «설정이 덜 됐다»는 현재 상태라, 오늘 아무것도 돌리지 않아도 계속 잡혔다. 대시보드의 오류는 조회 기간 안의
    // 실행 실패만 센다(ETL 로그 화면과 같은 기준).
    [nifiExecutionLogsQuery.data],
  );

  /**
   * CDC 실패. **CDC 로그 화면과 같은 것을 센다**(2026-09-28 요청) - 조회 기간 안에서 실패로 남은 줄이다.
   *
   * <p>예전에는 «지금 커넥터가 FAILED 인가»를 셌다. 그러면 기간과 무관했고(오늘 아무것도 안 돌려도 계속 잡힘),
   * 반대로 기간 안에 났다가 복구된 실패는 보이지 않았다. 화면 위쪽에서 대충 보고 [CDC 로그]로 들어가 자세히 보는
   * 흐름이라 두 화면이 같은 것을 세어야 한다.
   */
  const cdcFailedItems: FailureDetailItem[] = useMemo(() => {
    // 지워진 파이프라인도 해석한다(2026-10-06) - 서버가 지울 때 남긴 정보 · 명령 이력과 오류 내용으로 해석한다.
    const gptTarget = (pipelineId: number) =>
      ({ targetType: "CDC_PIPELINE" as const, target: { pipelineId: String(pipelineId) } });

    const events: FailureDetailItem[] = (cdcEventLogsQuery.data ?? [])
      .filter((row) => row.result === "FAILED")
      .map((row) => ({
        id: `cdc-event-${row.id}`,
        title: row.pipelineName,
        datetime: row.occurredAt,
        lines: [
          ["파이프라인", row.pipelineName],
          ["구분", `${row.command} · 실패`],
        ] as Array<[string, string]>,
        loadError: async () => row.message ?? "오류 메시지를 찾을 수 없습니다.",
        gpt: gptTarget(row.pipelineId),
      }));

    const processing: FailureDetailItem[] = (cdcProcessingLogsQuery.data ?? [])
      .filter((row) => row.status === "FAILED")
      .map((row) => ({
        id: `cdc-proc-${row.pipelineId}-${row.occurredAt}`,
        title: row.pipelineName,
        datetime: row.occurredAt,
        lines: [
          ["파이프라인", row.pipelineName],
          ["구분", "처리 · 실패"],
          ["소스 → 타깃", `${row.source} → ${row.target}`],
          ["Topic", row.topicName || "-"],
        ] as Array<[string, string]>,
        loadError: async () => row.message ?? "오류 메시지를 찾을 수 없습니다.",
        gpt: gptTarget(row.pipelineId),
      }));

    return [...events, ...processing].sort((a, b) => (b.datetime ?? "").localeCompare(a.datetime ?? ""));
  }, [cdcEventLogsQuery.data, cdcProcessingLogsQuery.data]);


  /**
   * 워크플로우 실패. ETL · CDC 와 같은 창(FailureDetailModal)에 같은 차례로 담는다(2026-09-28 요청).
   * 줄을 펼치면 실행 로그가 보이고, [로그] 를 누르면 시도별 로그 · 하위 워크플로우까지 따라가는 전용 창이 열린다.
   */
  const workflowFailedItems: FailureDetailItem[] = useMemo(
    () => airflowHistory.failedEntries.map((entry) => {
      const splitAt = entry.id.indexOf(":");
      const dagId = splitAt > 0 ? entry.id.slice(0, splitAt) : entry.id;
      return {
        id: entry.id,
        title: entry.basicContent,
        datetime: entry.datetime,
        lines: [
          ["워크플로우", entry.basicContent],
          ["구분", entry.category],
          ["DAG", dagId],
        ] as Array<[string, string]>,
        loadError: async () =>
          (entry.fetchLog ? await entry.fetchLog() : undefined) ?? "로그를 불러올 수 없습니다.",
      };
    }),
    [airflowHistory.failedEntries],
  );

  /**
   * 실패한 실행의 «로그» - 워크플로우 로그 화면과 같은 창(TaskFailureLogModal)을 띄운다.
   * 그 창이 시도별 로그 · Airflow 기록 · 하위 워크플로우까지 따라가고 GPT 해석도 그 안에 있다.
   */
  const openWorkflowFailureLog = async (entry: HistoryEntry) => {
    // id 는 `${dagId}:${runId}` 인데 run_id 안에도 콜론이 있다(manual__2026-09-21T06:35:27…) - 첫 콜론만 구분자다.
    const splitAt = entry.id.indexOf(":");
    const dagId = splitAt > 0 ? entry.id.slice(0, splitAt) : "";
    const runId = splitAt > 0 ? entry.id.slice(splitAt + 1) : "";
    if (!dagId || !runId) {
      return;
    }
    setWorkflowFailureLoading(entry.id);
    try {
      const tasks = await listAirflowTaskInstances(dagId, runId);
      const failed = tasks.filter((task) => task.state === "failed" || task.state === "upstream_failed");
      setWorkflowFailure({
        title: entry.basicContent,
        dagId,
        runId,
        tasks: failed.length > 0 ? failed : tasks,
      });
    } catch {
      // 창을 못 열면 기존처럼 줄을 펼쳐 원문을 볼 수 있다 - 여기서 더 막지 않는다.
      setWorkflowFailure(undefined);
    } finally {
      setWorkflowFailureLoading(null);
    }
  };

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

  // 원본 5-2 "전일 대비 증감". 일별 시계열의 마지막 날과 그 전날을 비교한다.
  // 하루치밖에 없으면 previous 를 null 로 둬서 "비교 불가"로 표시한다(0%로 속이지 않는다).
  function previousDayCount(points: Array<{ count: number }>): number | null {
    return points.length >= 2 ? points[points.length - 2].count : null;
  }
  function lastDayCount(points: Array<{ count: number }>): number {
    return points.length >= 1 ? points[points.length - 1].count : 0;
  }

  // 원본 5-2 "처리율 0에 맥락 부여" + "#4 해소 예상 시간". cdcFlow 는 아래 프로세스 헬스 패널(데이터 흐름 줄)이 쓴다.
  // 카드 아래 상태 줄(CDC «N분째 정지 — 확인 필요» · ETL «대기 데이터가 남았는데 …» · Workflow 최근 실행 띠)은
  // 2026-10-11 요청으로 뺐다 - 시험 · 원천 변경이 없는 시간에도 경고처럼 떠서 오히려 헷갈렸다.
  const cdcFlow = judgeCdcFlow(realtimeMetrics);
  const cdcRecovery = formatRecovery(realtimeMetrics);

  const connectorDriftCount = commandSummary?.connectorDrift.length ?? 0;

  const nifiLoading = nifiLoadQuery.isLoading && !nifiLoadQuery.data;
  const kafkaLoading = kafkaLoadQuery.isLoading && !kafkaLoadQuery.data;
  const nifiHourlyLoading = nifiHourlyQuery.isLoading && !nifiHourlyQuery.data;
  const kafkaHourlyLoading = kafkaHourlyQuery.isLoading && !kafkaHourlyQuery.data;
  const selectedDataLoading = nifiLoading || kafkaLoading || showAirflowInitialLoading;

  return (
    <ConfigProvider theme={cerebroBrandTheme}>
    <div className="dashboard-page">
      {/* 디자인 초안(index.html): 제목·기준 설명을 한 줄에 두고, 오른쪽에 조치 대기열 알약과 조회 기간을 모은다. */}
      <header className="dashboard-filter-bar">
        <div className="dashboard-filter-heading">
          <h2>통합 운영 대시보드</h2>
          <span className="dashboard-filter-sep" aria-hidden="true">|</span>
          <p>
            처리·실행 이력은 <strong>{selectedPeriodLabel}</strong> 기준이며, 큐와 실행 상태는 현재 기준입니다.
          </p>
        </div>
        <div className="dashboard-filter-tools">
          {/* 원본 5-1: 조치 대기열은 KPI 카드보다 위, 대시보드 최상단이다.
              "화면을 보고 무엇을 해야 하는지가 나오게 한다"가 이 배치의 이유다. */}
          <ActionQueuePanel />
          <div className="dashboard-date-filter">
            <div className="btn-group-date" role="group" aria-label="조회 기간 빠른 선택">
              <button
                type="button"
                className={selectedPreset === 0 ? "btn-date-tab active" : "btn-date-tab"}
                aria-pressed={selectedPreset === 0}
                onClick={() => applyDayPreset(0)}
              >
                당일
              </button>
              <button
                type="button"
                className={selectedPreset === 1 ? "btn-date-tab active" : "btn-date-tab"}
                aria-pressed={selectedPreset === 1}
                onClick={() => applyDayPreset(1)}
              >
                전일
              </button>
            </div>
            <RangePicker
              value={dateRange}
              onChange={(value) => {
                if (value && value[0] && value[1]) {
                  setDateRange([value[0], value[1]]);
                }
              }}
              allowClear={false}
            />
            {/* antd 버튼의 loading 과 같게: 첫 조회 중에는 누름을 무시하고 도는 아이콘을 보인다. */}
            <button
              type="button"
              className="btn-search-exec"
              aria-busy={selectedDataLoading}
              onClick={() => {
                if (!selectedDataLoading) {
                  setAppliedRange(dateRange);
                }
              }}
            >
              {selectedDataLoading ? (
                <LoadingOutlined />
              ) : (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  aria-hidden="true"
                >
                  <circle cx="11" cy="11" r="8" />
                  <line x1="21" y1="21" x2="16.65" y2="16.65" />
                </svg>
              )}
              검색
            </button>
          </div>
        </div>
      </header>

      <div className="dashboard-body">
      <main className="dashboard-main">
      <section className="dashboard-summary-grid">
        {/* 원본 5-2 KPI 카드 재구성: 누적형(기간 배지) 위 / 진행형("지금" 구분선) 아래 */}
        <Card className="dashboard-summary-card dashboard-summary-card--cdc" loading={kafkaLoading}>
          <div className="summary-card-main">
            <div className="summary-card-heading">
              <span className="summary-card-title">
                <span className="summary-card-dot" />
                CDC
              </span>
              <TimeBadge label={selectedPeriodLabel} />
            </div>
            <div className="summary-card-hero">
              <div className="summary-card-value" title={`${formatCount(kafkaPeriodTotal)}건`}>
                {formatCompactCount(kafkaPeriodTotal)}
                <small>건 적재</small>
              </div>
              <div className="summary-card-change">
                <span className="summary-card-change-label">전일 대비</span>
                <ChangeIndicator current={lastDayCount(kafkaLoadDaily)} previous={previousDayCount(kafkaLoadDaily)} />
              </div>
            </div>

            <NowDivider />
            <div className="kpi-detail-list">
              <MetricRow
                label="미처리 건"
                value={formatCompactCount(kafkaBacklog)}
                warn={kafkaBacklog > 0}
                hint="CDC consumer lag 합계 — 아직 타깃에 반영되지 않은 건수"
              />
              <MetricRow label="해소 예상" value={cdcRecovery.text} warn={cdcRecovery.warn} />
              <MetricRow label="처리율" value={`${kafkaThroughput.toFixed(1)} rows/s`} />
              <MetricRow label="일시정지" value={String(kafkaPaused)} warn={kafkaPaused > 0} />
              {/* «커넥터 실패(현재)» 칸은 뺐다(2026-10-11 요청). 실패는 아래 «실패 N건» · CDC 로그 · 알림이 보여 준다. */}
              {connectorDriftCount > 0 ? (
                <MetricRow label="커넥터 불일치" value={String(connectorDriftCount)} warn />
              ) : null}
            </div>
          </div>

          <div className="summary-card-footer">
            <div className="summary-card-links">
              {cdcFailedItems.length > 0 ? (
                <button type="button" className="btn-kpi-danger-chip" onClick={() => setKafkaFailedOpen(true)}>
                  실패 {cdcFailedItems.length}건
                </button>
              ) : null}
              <button type="button" className="btn-kpi-action" onClick={() => navigate("/cdc/logs")}>
                로그 →
              </button>
            </div>
          </div>
        </Card>

        <Card className="dashboard-summary-card dashboard-summary-card--nifi" loading={nifiLoading}>
          <div className="summary-card-main">
            <div className="summary-card-heading">
              <span className="summary-card-title">
                <span className="summary-card-dot" />
                ETL
              </span>
              <TimeBadge label={selectedPeriodLabel} />
            </div>
            <div className="summary-card-hero">
              <div className="summary-card-value" title={`${formatCount(nifiPeriodTotal)}건`}>
                {formatCompactCount(nifiPeriodTotal)}
                <small>건 적재</small>
              </div>
              <div className="summary-card-change">
                <span className="summary-card-change-label">전일 대비</span>
                <ChangeIndicator current={lastDayCount(nifiLoadDaily)} previous={previousDayCount(nifiLoadDaily)} />
              </div>
            </div>

            <NowDivider />
            {/* 칸 순서 «실행 중 · 실패 · 성공 · 중지 · 대기 데이터»(2026-10-12 요청 - 예전 «실행 중 작업(활성 스레드) · 대기 데이터 · 실행 · 중지»).
                실행 중 · 실패 · 성공은 ETL 로그 화면 상단과 같은 숫자다. 중지 · 대기 데이터는 지금 상태다. */}
            <div className="kpi-detail-list">
              <MetricRow
                label="실행 중"
                value={String(nifiRunsRunning)}
                hint="지금 적재 중인 실행 - ETL 로그 «실행 중»과 같은 기준"
              />
              <MetricRow
                label="실패"
                value={String(nifiFailedItems.length)}
                warn={nifiFailedItems.length > 0}
                hint="조회 기간의 실패 - ETL 로그 «실패»와 같은 기준"
              />
              <MetricRow
                label="성공"
                value={String(nifiRunsSucceeded)}
                hint="조회 기간에 끝난 적재 실행 - ETL 로그 «성공»과 같은 기준"
              />
              <MetricRow label="중지" value={String(nifiStopped)} warn={nifiStopped > 0} />
              <MetricRow
                label="대기 데이터"
                value={formatCompactCount(nifiQueued)}
                warn={nifiQueued > 0 && nifiActiveThreads === 0}
                hint="처리를 기다리며 큐에 쌓인 데이터 건수(FlowFile)"
              />
              {/* «설정 확인 필요» 칸은 뺐다(2026-10-11 요청). */}
            </div>
          </div>

          <div className="summary-card-footer">
            <div className="summary-card-links">
              {nifiFailedItems.length > 0 ? (
                <button type="button" className="btn-kpi-danger-chip" onClick={() => setNifiFailedOpen(true)}>
                  실패 {nifiFailedItems.length}건
                </button>
              ) : null}
              {/* 세 카드 버튼 이름을 «로그»로 통일했다(2026-10-12 요청 - CDC «로그» · ETL «ETL 로그» · Workflow «실행 이력»이 제각각이었다). */}
              <button type="button" className="btn-kpi-action" onClick={() => navigate("/etl/logs")}>
                로그 →
              </button>
            </div>
          </div>
        </Card>

        <Card className="dashboard-summary-card dashboard-summary-card--airflow" loading={showAirflowInitialLoading}>
          <div className="summary-card-main">
            <div className="summary-card-heading">
              <span className="summary-card-title">
                <span className="summary-card-dot" />
                Workflow
              </span>
              <TimeBadge label={selectedPeriodLabel} />
            </div>
            <div className="summary-card-hero">
              <div className="summary-card-value">
                {airflowSuccessRate}
                <small>% 성공</small>
                <span className="summary-card-value-note">(완료 실행 기준)</span>
              </div>
              <div className="summary-card-change">
                <span className="summary-card-change-label">
                  성공 {airflowHistory.successEntries.length} · 실패 {airflowHistory.failedEntries.length}
                </span>
              </div>
            </div>

            <NowDivider />
            <div className="kpi-detail-list">
              <MetricRow label="실행 중" value={String(airflowHistory.runningEntries.length)} />
              <MetricRow
                label="실패"
                value={String(airflowHistory.failedEntries.length)}
                warn={airflowHistory.failedEntries.length > 0}
              />
              <MetricRow label="성공" value={String(airflowHistory.successEntries.length)} />
              <MetricRow label="DAG" value={String(airflowHistory.dagCount)} />
            </div>
          </div>

          <div className="summary-card-footer">
            <div className="summary-card-links">
              {airflowHistory.failedEntries.length > 0 ? (
                <button type="button" className="btn-kpi-danger-chip" onClick={() => setAirflowActiveTile("failed")}>
                  실패 {airflowHistory.failedEntries.length}건 보기
                </button>
              ) : null}
              <button type="button" className="btn-kpi-action" onClick={() => navigate("/airflow/dashboard")}>
                로그 →
              </button>
            </div>
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
            onReady={({ chart }) => refitAfterLayout(chart)}
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
            onReady={({ chart }) => refitAfterLayout(chart)}
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
            onReady={({ chart }) => refitAfterLayout(chart)}
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

      {/* 실패만 ETL · CDC 와 같은 창으로 보여 준다(2026-09-28). 나머지 타일(실행 중 · 성공 · DAG)은 목록이라 예전 창 그대로다. */}
      <HistoryModal
        open={airflowActiveTile !== null && airflowActiveTile !== "failed"}
        onClose={() => setAirflowActiveTile(null)}
        title={airflowActiveTile ? AIRFLOW_TILE_TITLE[airflowActiveTile] : ""}
        loading={showAirflowInitialLoading}
        rows={airflowModalRows}
      />
      <FailureDetailModal
        open={airflowActiveTile === "failed"}
        onClose={() => setAirflowActiveTile(null)}
        title={`워크플로우 실패 상세 · ${selectedPeriodLabel}`}
        loading={showAirflowInitialLoading}
        items={workflowFailedItems}
        rowAction={{
          label: "로그",
          busyId: workflowFailureLoading ?? undefined,
          onClick: (item) => {
            const entry = airflowHistory.failedEntries.find((row) => row.id === item.id);
            if (entry) {
              void openWorkflowFailureLog(entry);
            }
          },
        }}
      />
      <FailureDetailModal
        open={nifiFailedOpen}
        onClose={() => setNifiFailedOpen(false)}
        title={`ETL 실패 상세 · ${selectedPeriodLabel}`}
        loading={nifiJobsQuery.isLoading && !nifiJobsQuery.data}
        items={nifiFailedItems}
      />
      <FailureDetailModal
        open={kafkaFailedOpen}
        onClose={() => setKafkaFailedOpen(false)}
        title={`CDC 실패 상세 · ${selectedPeriodLabel}`}
        loading={kafkaConnectorsQuery.isLoading && !kafkaConnectorsQuery.data}
        items={cdcFailedItems}
      />
      {/* 워크플로우(Airflow) 실패는 로그 화면과 같은 창을 그대로 띄운다 - 시도별 로그 · 하위 워크플로우 · GPT 해석이 한 자리에 있다. */}
      <TaskFailureLogModal target={workflowFailure} onClose={() => setWorkflowFailure(undefined)} />
    </div>
    </ConfigProvider>
  );
}

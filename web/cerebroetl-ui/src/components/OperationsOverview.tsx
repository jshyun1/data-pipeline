import { Card, Empty } from "antd";
import { Column, Line, Pie } from "@ant-design/plots";
import dayjs from "dayjs";
import type { ReactNode } from "react";
import type { RealtimePipelineMetricResponse } from "../api/dashboard";
import type { NifiExecutionLogEntry } from "../api/platform";
import type { PipelineResponse } from "../types/pipeline";
import type { KafkaConnectorJob } from "../utils/kafkaConnectors";
import type { NifiJob } from "../utils/nifiJobs";

const RECOVERY_DELAY_SECONDS = 300;

type OperationState = "NORMAL" | "DELAYED" | "FAILED" | "PAUSED" | "READY" | "STOPPED" | "UNOBSERVED";

interface OperationRow {
  key: string;
  name: string;
  engine: "KAFKA" | "NIFI";
  type: string;
  path: string;
  state: OperationState;
  throughput: number | null;
  backlog: number | null;
  backlogUnit: "records" | "FlowFiles";
  recoverySeconds: number | null;
  lastProgressAt: string | null;
  errorMessage?: string;
}

interface LoadPoint {
  date: string;
  count: number;
}

interface TopPoint {
  label: string;
  count: number;
}

interface OperationsOverviewProps {
  pipelines: PipelineResponse[];
  realtimeMetrics: RealtimePipelineMetricResponse[];
  kafkaConnectors: KafkaConnectorJob[];
  nifiJobs: NifiJob[];
  nifiExecutionLogs: NifiExecutionLogEntry[];
  // 아래 4개는 상위(DashboardPage)에서 화면 상단 날짜 필터(appliedRange)로 이미
  // 조회해온 값을 그대로 받는다 - 이 컴포넌트 안에서 별도로 기간을 고정하지 않고,
  // 항상 그 필터를 따라가게 하기 위함.
  nifiDaily: LoadPoint[];
  kafkaDaily: LoadPoint[];
  nifiTop: TopPoint[];
  kafkaTop: TopPoint[];
  loading?: boolean;
  headerExtra?: ReactNode;
}

const STATE_META: Record<OperationState, { label: string; color: string }> = {
  NORMAL: { label: "정상", color: "green" },
  DELAYED: { label: "지연", color: "orange" },
  FAILED: { label: "실패", color: "red" },
  PAUSED: { label: "일시정지", color: "default" },
  READY: { label: "실행 대기", color: "blue" },
  STOPPED: { label: "Sink 중지", color: "default" },
  UNOBSERVED: { label: "관측 대기", color: "blue" },
};

function kafkaState(
  pipeline: PipelineResponse,
  metric: RealtimePipelineMetricResponse | undefined,
  connectors: KafkaConnectorJob[],
): OperationState {
  const connectorNames = new Set(pipeline.connectors.map((connector) => connector.connectorName));
  const jobs = connectors.filter((connector) => connectorNames.has(connector.name));
  if (pipeline.status === "FAILED" || jobs.some((job) => job.status === "FAILED")) {
    return "FAILED";
  }
  if (pipeline.status === "PAUSED" || jobs.some((job) => job.status === "PAUSED")) {
    return "PAUSED";
  }
  if (pipeline.status === "READY") {
    return "READY";
  }
  if (pipeline.status === "CREATED" || pipeline.status === "STOPPED") {
    return "STOPPED";
  }
  if (!metric || metric.collectionStatus === "NO_DATA") {
    return "UNOBSERVED";
  }
  if (
    (Number(metric.consumerLag ?? 0) > 0 && metric.throughputPerSecond <= 0) ||
    Number(metric.estimatedRecoverySeconds ?? 0) > RECOVERY_DELAY_SECONDS
  ) {
    return "DELAYED";
  }
  return "NORMAL";
}

function nifiState(job: NifiJob): OperationState {
  if (job.status === "FAILED") {
    return "FAILED";
  }
  if (job.status === "STOPPED") {
    return "STOPPED";
  }
  if (job.flowFilesQueued > 0 && job.activeThreadCount === 0) {
    return "DELAYED";
  }
  return "NORMAL";
}

function latestNifiLogsByGroup(logs: NifiExecutionLogEntry[]) {
  const result = new Map<string, NifiExecutionLogEntry[]>();
  const cutoff = dayjs().subtract(90, "second");
  logs.forEach((entry) => {
    if (!entry.groupId || dayjs(entry.occurredAt).isBefore(cutoff)) {
      return;
    }
    result.set(entry.groupId, [...(result.get(entry.groupId) ?? []), entry]);
  });
  return result;
}

function buildRows(
  pipelines: PipelineResponse[],
  metrics: RealtimePipelineMetricResponse[],
  kafkaConnectors: KafkaConnectorJob[],
  nifiJobs: NifiJob[],
  nifiLogs: NifiExecutionLogEntry[],
): OperationRow[] {
  const metricByPipelineId = new Map(metrics.map((metric) => [metric.pipelineId, metric]));
  const recentLogsByGroup = latestNifiLogsByGroup(nifiLogs);

  const kafkaRows: OperationRow[] = pipelines.map((pipeline) => {
    const metric = metricByPipelineId.get(pipeline.id);
    const connectorNames = new Set(pipeline.connectors.map((connector) => connector.connectorName));
    const failedConnector = kafkaConnectors.find(
      (connector) => connectorNames.has(connector.name) && connector.status === "FAILED",
    );
    const source = pipeline.pipelineType === "LOG_FILE"
      ? "로그 파일"
      : `${pipeline.sourceDbType ?? "DB"} ${pipeline.sourceSchema ?? ""}.${pipeline.sourceTable ?? ""}`;
    const target = `${pipeline.targetDbType} ${pipeline.targetSchema}.${pipeline.targetTable}`;

    return {
      key: `kafka-${pipeline.id}`,
      name: pipeline.name,
      engine: "KAFKA",
      type: pipeline.pipelineType === "LOG_FILE" ? "로그 실시간 적재" : "CDC 실시간 적재",
      path: `${source} → Kafka → ${target}`,
      state: kafkaState(pipeline, metric, kafkaConnectors),
      throughput: metric?.collectionStatus === "COLLECTED" ? metric.throughputPerSecond : null,
      backlog: metric?.consumerLag ?? null,
      backlogUnit: "records",
      recoverySeconds: metric?.estimatedRecoverySeconds ?? null,
      lastProgressAt: metric?.lastProgressAt ?? null,
      errorMessage: failedConnector
        ? `${failedConnector.name}: 실패 태스크 ${failedConnector.failedTaskCount}개`
        : undefined,
    };
  });

  const nifiRows: OperationRow[] = nifiJobs.map((job) => {
    const recentLogs = recentLogsByGroup.get(job.id) ?? [];
    // 실패 행(bulletin에서 만든 것)은 적재 건수가 null이라 합계에서 제외한다.
    const insertedCount = recentLogs.reduce((sum, entry) => sum + (entry.insertedCount ?? 0), 0);
    const throughput = recentLogs.length > 0 ? Math.round((insertedCount / 60) * 10) / 10 : 0;
    const latestProgressAt = recentLogs
      .map((entry) => entry.occurredAt)
      .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())[0] ?? null;
    return {
      key: `nifi-${job.id}`,
      name: job.name,
      engine: "NIFI",
      type: "NiFi ETL",
      path: "파일·HTTP·배치 → NiFi → Target DB",
      state: nifiState(job),
      throughput,
      backlog: job.flowFilesQueued,
      backlogUnit: "FlowFiles",
      recoverySeconds: throughput > 0 && job.flowFilesQueued > 0 ? Math.ceil(job.flowFilesQueued / throughput) : null,
      lastProgressAt: latestProgressAt,
      errorMessage: job.errorMessage,
    };
  });

  return [...kafkaRows, ...nifiRows].sort((a, b) => {
    const priority: Record<OperationState, number> = {
      FAILED: 0,
      DELAYED: 1,
      UNOBSERVED: 2,
      PAUSED: 3,
      READY: 4,
      STOPPED: 5,
      NORMAL: 6,
    };
    return priority[a.state] - priority[b.state] || a.name.localeCompare(b.name, "ko");
  });
}

export function OperationsOverview({
  pipelines,
  realtimeMetrics,
  kafkaConnectors,
  nifiJobs,
  nifiExecutionLogs,
  nifiDaily,
  kafkaDaily,
  nifiTop,
  kafkaTop,
  loading,
  headerExtra,
}: OperationsOverviewProps) {
  const rows = buildRows(pipelines, realtimeMetrics, kafkaConnectors, nifiJobs, nifiExecutionLogs);

  const stateChartData = (Object.keys(STATE_META) as OperationState[])
    .map((state) => ({ state: STATE_META[state].label, value: rows.filter((row) => row.state === state).length }))
    .filter((point) => point.value > 0);

  // 이미지 속 "최근 10분 CPU 사용률" 자리 - 다만 분 단위 실시간 대신, 화면 상단
  // 날짜 필터로 조회한 기간의 일별 처리 건수를 엔진별로 겹쳐 그린다.
  const dailyTrendChart = [
    ...nifiDaily.map((point) => ({ date: point.date, count: point.count, engine: "NIFI" })),
    ...kafkaDaily.map((point) => ({ date: point.date, count: point.count, engine: "KAFKA" })),
  ];

  const backlogTop5 = [...rows]
    .filter((row) => Number(row.backlog ?? 0) > 0)
    .sort((a, b) => (b.backlog ?? 0) - (a.backlog ?? 0))
    .slice(0, 5)
    .map((row) => ({ name: row.name, value: row.backlog ?? 0, engine: row.engine }));

  const throughputTop5 = [...rows]
    .filter((row) => Number(row.throughput ?? 0) > 0)
    .sort((a, b) => (b.throughput ?? 0) - (a.throughput ?? 0))
    .slice(0, 5)
    .map((row) => ({ name: row.name, value: row.throughput ?? 0, engine: row.engine }));

  const recoveryTop5 = [...rows]
    .filter((row) => Number(row.recoverySeconds ?? 0) > 0)
    .sort((a, b) => (b.recoverySeconds ?? 0) - (a.recoverySeconds ?? 0))
    .slice(0, 5)
    .map((row) => ({ name: row.name, value: Math.round(((row.recoverySeconds ?? 0) / 60) * 10) / 10, engine: row.engine }));

  // 이미지 속 "테이블 입력 건수" 자리 - 화면 상단 날짜 필터로 조회한 기간의
  // 파이프라인별 적재 건수 Top5 (NiFi/Kafka 통합).
  const loadTop5 = [
    ...nifiTop.map((point) => ({ name: point.label, value: point.count, engine: "NIFI" })),
    ...kafkaTop.map((point) => ({ name: point.label, value: point.count, engine: "KAFKA" })),
  ]
    .sort((a, b) => b.value - a.value)
    .slice(0, 5);

  return (
    <>
      <section className="dashboard-section operations-overview">
        <div className="section-heading">
          <div>
            <h3>적재 프로세스 상태</h3>
            <p className="section-description">
              파이프라인 상태 분포와, 화면 상단 날짜 필터에 맞춘 기간의 일별 처리 건수 추이입니다.
            </p>
          </div>
          {headerExtra}
        </div>
        <div className="dashboard-chart-grid">
          <Card title="상태별 파이프라인 수" size="small" loading={loading}>
            {stateChartData.length > 0 ? (
              <Pie data={stateChartData} angleField="value" colorField="state" height={230} legend={{ color: { position: "bottom" } }} />
            ) : (
              <div className="empty-chart-placeholder"><Empty description="등록된 파이프라인이 없습니다." /></div>
            )}
          </Card>
          <Card title="선택 기간 일별 처리 건수 추이" size="small" loading={loading}>
            {dailyTrendChart.length > 0 ? (
              <Line data={dailyTrendChart} xField="date" yField="count" colorField="engine" height={230} />
            ) : (
              <div className="empty-chart-placeholder"><Empty description="선택한 기간에 적재 이력이 없습니다." /></div>
            )}
          </Card>
        </div>
      </section>

      <section className="dashboard-section">
        <div className="section-heading">
          <div>
            <h3>파이프라인별 처리 현황</h3>
          </div>
        </div>
        <div className="dashboard-chart-grid dashboard-chart-grid--3">
          <Card title="미처리량 Top 5" size="small" loading={loading}>
            {backlogTop5.length > 0 ? (
              <Column
                data={backlogTop5}
                xField="name"
                yField="value"
                colorField="engine"
                height={220}
                axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
              />
            ) : (
              <div className="empty-chart-placeholder"><Empty description="현재 미처리 데이터가 없습니다." /></div>
            )}
          </Card>
          <Card title="처리율 Top 5 (건/s)" size="small" loading={loading}>
            {throughputTop5.length > 0 ? (
              <Column
                data={throughputTop5}
                xField="name"
                yField="value"
                colorField="engine"
                height={220}
                axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
              />
            ) : (
              <div className="empty-chart-placeholder"><Empty description="현재 처리 중인 데이터가 없습니다." /></div>
            )}
          </Card>
          <Card title="예상 복구시간 Top 5 (분)" size="small" loading={loading}>
            {recoveryTop5.length > 0 ? (
              <Column
                data={recoveryTop5}
                xField="name"
                yField="value"
                colorField="engine"
                height={220}
                axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
              />
            ) : (
              <div className="empty-chart-placeholder"><Empty description="복구 대기 중인 파이프라인이 없습니다." /></div>
            )}
          </Card>
        </div>
      </section>

      <section className="dashboard-section">
        <div className="section-heading">
          <div>
            <h3>선택 기간 적재 현황</h3>
          </div>
        </div>
        <div className="dashboard-chart-grid">
          <Card title="파이프라인별 적재 건수 Top 5" size="small" loading={loading}>
            {loadTop5.length > 0 ? (
              <Column
                data={loadTop5}
                xField="name"
                yField="value"
                colorField="engine"
                height={230}
                axis={{ x: { labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: true } }}
              />
            ) : (
              <div className="empty-chart-placeholder"><Empty description="선택한 기간에 적재 이력이 없습니다." /></div>
            )}
          </Card>
        </div>
      </section>
    </>
  );
}

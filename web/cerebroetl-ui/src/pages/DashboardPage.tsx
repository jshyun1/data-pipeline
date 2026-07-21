import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Badge, Card, Col, Row, Statistic, Table, Tag } from "antd";
import { getDashboardSummary } from "../api/dashboard";
import {
  getAirflowHealth,
  getNifiRootStatus,
  listAirflowDagRuns,
  listAirflowDags,
  listAirflowTaskInstances,
} from "../api/platform";
import type { AirflowDagRun, AirflowTaskInstance, NifiProcessGroupStatusSnapshot } from "../api/platform";
import type { PipelineCommandHistoryResponse } from "../types/pipeline";

const RESULT_COLOR: Record<string, string> = {
  SUCCESS: "success",
  FAILED: "error",
};

const historyColumns = [
  { title: "파이프라인 ID", dataIndex: "pipelineId", width: 120 },
  { title: "명령", dataIndex: "command", width: 120 },
  {
    title: "결과",
    dataIndex: "result",
    width: 100,
    render: (value: string | null) => (value ? <Tag color={RESULT_COLOR[value] ?? "default"}>{value}</Tag> : "-"),
  },
  { title: "메시지", dataIndex: "message", ellipsis: true },
  { title: "요청 시각", dataIndex: "requestedAt" },
];

type EtlJobFilter = "all" | "running" | "failed" | "stopped";

interface NifiJob {
  id: string;
  name: string;
  status: "RUNNING" | "FAILED" | "STOPPED";
  processorCount: number;
  runningProcessorCount: number;
  failedProcessorCount: number;
  queued: string;
  activeThreadCount: number;
}

interface AirflowDashboardStats {
  totalDags: number;
  activeDags: number;
  running: number;
  todaySuccess: number;
  todayFailed: number;
  delayed: number;
}

interface AirflowIssueRow {
  id: string;
  dagId: string;
  status: "failed" | "delayed";
  failedTask: string;
  startedAt: string;
  duration: string;
  retry: string;
}

interface AirflowDashboardData {
  stats: AirflowDashboardStats;
  issues: AirflowIssueRow[];
}

const EMPTY_AIRFLOW_DASHBOARD: AirflowDashboardData = {
  stats: {
    totalDags: 0,
    activeDags: 0,
    running: 0,
    todaySuccess: 0,
    todayFailed: 0,
    delayed: 0,
  },
  issues: [],
};

function collectNifiJobs(groups: NifiProcessGroupStatusSnapshot[] = []): NifiJob[] {
  return groups.flatMap((group) => {
    const snapshot = group.processGroupStatusSnapshot;
    if (!snapshot?.id) {
      return [];
    }

    const processors = snapshot.processorStatusSnapshots ?? [];
    const childJobs = collectNifiJobs(snapshot.processGroupStatusSnapshots);
    const readableProcessors = processors
      .map((processor) => processor.processorStatusSnapshot)
      .filter((processor) => Boolean(processor?.id));

    if (readableProcessors.length === 0) {
      return childJobs;
    }

    const runningProcessorCount = readableProcessors.filter((processor) => processor?.runStatus === "Running").length;
    const failedProcessorCount = readableProcessors.filter((processor) => processor?.runStatus === "Invalid").length;
    const status = failedProcessorCount > 0 ? "FAILED" : runningProcessorCount > 0 ? "RUNNING" : "STOPPED";

    return [
      {
        id: snapshot.id,
        name: snapshot.name ?? snapshot.id,
        status,
        processorCount: readableProcessors.length,
        runningProcessorCount,
        failedProcessorCount,
        queued: snapshot.queued ?? `${snapshot.flowFilesQueued ?? 0}`,
        activeThreadCount: snapshot.activeThreadCount ?? 0,
      },
      ...childJobs,
    ];
  });
}

function etlStatusTag(status: NifiJob["status"]) {
  if (status === "RUNNING") {
    return <Tag color="success">RUNNING</Tag>;
  }
  if (status === "FAILED") {
    return <Tag color="error">FAILED</Tag>;
  }
  return <Tag>STOPPED</Tag>;
}

function isToday(value?: string) {
  if (!value) {
    return false;
  }

  const date = new Date(value);
  const today = new Date();
  return (
    date.getFullYear() === today.getFullYear() &&
    date.getMonth() === today.getMonth() &&
    date.getDate() === today.getDate()
  );
}

function formatAirflowTime(value?: string) {
  if (!value) {
    return "-";
  }

  return new Intl.DateTimeFormat("ko-KR", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function formatAirflowDuration(start?: string, end?: string) {
  if (!start) {
    return "-";
  }

  const startTime = new Date(start).getTime();
  const endTime = end ? new Date(end).getTime() : Date.now();
  const minutes = Math.max(1, Math.round((endTime - startTime) / 60000));
  return `${minutes}분`;
}

function formatAirflowRetry(task?: AirflowTaskInstance) {
  if (!task) {
    return "-";
  }

  const tryNumber = task.try_number ?? 0;
  const maxTries = Math.max((task.max_tries ?? 0) + 1, tryNumber);
  return `${tryNumber}/${maxTries}`;
}

async function getAirflowDashboard(): Promise<AirflowDashboardData> {
  const dags = await listAirflowDags();
  const activeDags = dags.filter((dag) => dag.is_active !== false && dag.is_paused !== true);
  const runResults = await Promise.allSettled(
    activeDags.map(async (dag) => ({
      dagId: dag.dag_id,
      runs: await listAirflowDagRuns(dag.dag_id, 100),
    })),
  );
  const dagRunGroups = runResults.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []));
  const dagRuns = dagRunGroups.flatMap((group) =>
    group.runs.map((run) => ({
      ...run,
      dag_id: run.dag_id ?? group.dagId,
    })),
  );
  const todayRuns = dagRuns.filter((run) => isToday(run.end_date ?? run.start_date ?? run.execution_date));
  const running = dagRuns.filter((run) => run.state === "running" || run.state === "queued").length;
  const todaySuccess = todayRuns.filter((run) => run.state === "success").length;
  const todayFailed = todayRuns.filter((run) => run.state === "failed").length;
  const latestRunsByDag = new Map<string, AirflowDagRun>();

  dagRuns.forEach((run) => {
    const dagId = run.dag_id as string;
    const previous = latestRunsByDag.get(dagId);
    const runTime = new Date(run.start_date ?? run.execution_date ?? 0).getTime();
    const previousTime = new Date(previous?.start_date ?? previous?.execution_date ?? 0).getTime();
    if (!previous || runTime > previousTime) {
      latestRunsByDag.set(dagId, run);
    }
  });

  const failureResults = await Promise.allSettled(
    dagRunGroups.flatMap((group) =>
      group.runs
        .filter((run) => run.state === "failed")
        .slice(0, 3)
        .map(async (run) => {
          const tasks = await listAirflowTaskInstances(group.dagId, run.dag_run_id).catch(() => []);
          const failedTask = tasks.find((task) => task.state === "failed");
          return {
            id: `${group.dagId}:${run.dag_run_id}`,
            dagId: group.dagId,
            status: "failed" as const,
            failedTask: failedTask?.task_id ?? "-",
            startedAt: formatAirflowTime(run.start_date ?? run.execution_date),
            duration: formatAirflowDuration(run.start_date ?? run.execution_date, run.end_date),
            retry: formatAirflowRetry(failedTask),
          };
        }),
    ),
  );
  const failures = failureResults
    .flatMap((result) => (result.status === "fulfilled" ? [result.value] : []))
    .slice(0, 10);
  const delayedIssues = activeDags
    .filter((dag) => !latestRunsByDag.has(dag.dag_id))
    .map((dag) => ({
      id: `${dag.dag_id}:delayed`,
      dagId: dag.dag_id,
      status: "delayed" as const,
      failedTask: "-",
      startedAt: "실행 이력 없음",
      duration: "-",
      retry: "-",
    }));

  return {
    stats: {
      totalDags: dags.length,
      activeDags: activeDags.length,
      running,
      todaySuccess,
      todayFailed,
      delayed: activeDags.filter((dag) => !latestRunsByDag.has(dag.dag_id)).length,
    },
    issues: [...failures, ...delayedIssues].slice(0, 10),
  };
}

export function DashboardPage() {
  const [etlJobFilter, setEtlJobFilter] = useState<EtlJobFilter>("all");
  const dashboardSummary = useQuery({
    queryKey: ["dashboard-summary"],
    queryFn: getDashboardSummary,
    refetchInterval: 60000,
    placeholderData: (previousData) => previousData,
  });

  const airflowHealth = useQuery({
    queryKey: ["dashboard-airflow-health"],
    queryFn: getAirflowHealth,
    refetchInterval: 60000,
    retry: false,
    placeholderData: (previousData) => previousData,
  });

  const airflowStats = useQuery({
    queryKey: ["dashboard-airflow-stats"],
    queryFn: getAirflowDashboard,
    refetchInterval: 60000,
    retry: false,
    placeholderData: (previousData) => previousData,
  });

  const nifiStatus = useQuery({
    queryKey: ["dashboard-nifi-root-status"],
    queryFn: getNifiRootStatus,
    refetchInterval: 60000,
    retry: false,
    placeholderData: (previousData) => previousData,
  });

  const data = dashboardSummary.data;
  const nifiSnapshot = nifiStatus.data?.processGroupStatus?.aggregateSnapshot;
  const nifiJobs = useMemo(
    () => collectNifiJobs(nifiSnapshot?.processGroupStatusSnapshots),
    [nifiSnapshot?.processGroupStatusSnapshots],
  );
  const runningNifiJobs = nifiJobs.filter((job) => job.status === "RUNNING");
  const failedNifiJobs = nifiJobs.filter((job) => job.status === "FAILED");
  const stoppedNifiJobs = nifiJobs.filter((job) => job.status === "STOPPED");
  const filteredNifiJobs =
    etlJobFilter === "running"
      ? runningNifiJobs
      : etlJobFilter === "failed"
        ? failedNifiJobs
        : etlJobFilter === "stopped"
          ? stoppedNifiJobs
          : nifiJobs;
  const etlJobFilterLabel =
    etlJobFilter === "running"
      ? "실행중 JOB"
      : etlJobFilter === "failed"
        ? "실패 JOB"
        : etlJobFilter === "stopped"
          ? "중지 JOB"
          : "전체 JOB";
  const airflowServices = [
    { label: "Metadatabase", status: airflowHealth.data?.metadatabase?.status },
    { label: "Scheduler", status: airflowHealth.data?.scheduler?.status },
    { label: "Triggerer", status: airflowHealth.data?.triggerer?.status },
    { label: "DAG Processor", status: airflowHealth.data?.dag_processor?.status },
  ];
  const airflowHealthyCount = airflowServices.filter((service) => service.status?.toLowerCase() === "healthy").length;
  const airflowOnline = !airflowHealth.isError && airflowHealthyCount > 0;
  const airflowDashboard = airflowStats.data ?? EMPTY_AIRFLOW_DASHBOARD;
  const showDashboardInitialLoading = dashboardSummary.isLoading && !dashboardSummary.data;
  const showAirflowInitialLoading = airflowStats.isLoading && !airflowStats.data;
  const showNifiInitialLoading = nifiStatus.isLoading && !nifiStatus.data;

  return (
    <div>
      <div className="page-toolbar">
        <div>
          <div className="page-kicker">SYSTEM OVERVIEW</div>
          <h2 className="page-title">대시보드</h2>
        </div>
      </div>

      <div className="dashboard-stack">
        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <div className="section-kicker">AIRFLOW</div>
              <h3>워크플로우 오케스트레이션</h3>
            </div>
            <Badge status={airflowOnline ? "success" : "error"} text={airflowOnline ? "정상" : "응답 없음"} />
          </div>
          <Row gutter={[12, 12]} className="airflow-metrics">
            <Col xs={12} md={8} xl={4}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="전체 DAG" value={airflowDashboard.stats.totalDags} />
              </Card>
            </Col>
            <Col xs={12} md={8} xl={4}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="활성 DAG" value={airflowDashboard.stats.activeDags} />
              </Card>
            </Col>
            <Col xs={12} md={8} xl={4}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="실행 중" value={airflowDashboard.stats.running} valueStyle={{ color: "#1677ff" }} />
              </Card>
            </Col>
            <Col xs={12} md={8} xl={4}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="오늘 성공" value={airflowDashboard.stats.todaySuccess} valueStyle={{ color: "#2f7d32" }} />
              </Card>
            </Col>
            <Col xs={12} md={8} xl={4}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="오늘 실패" value={airflowDashboard.stats.todayFailed} valueStyle={{ color: "#c62828" }} />
              </Card>
            </Col>
            <Col xs={12} md={8} xl={4}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="지연" value={airflowDashboard.stats.delayed} />
              </Card>
            </Col>
            <Col xs={24}>
              <Card title="실패 / 지연 목록" size="small">
                <Table<AirflowIssueRow>
                  rowKey="id"
                  size="small"
                  loading={showAirflowInitialLoading}
                  dataSource={airflowDashboard.issues}
                  pagination={false}
                  columns={[
                    { title: "DAG", dataIndex: "dagId" },
                    {
                      title: "상태",
                      dataIndex: "status",
                      width: 100,
                      render: (status: AirflowIssueRow["status"]) => (
                        <Tag color={status === "failed" ? "error" : "warning"}>
                          {status === "failed" ? "실패" : "지연"}
                        </Tag>
                      ),
                    },
                    { title: "실패 Task", dataIndex: "failedTask", width: 180 },
                    { title: "실행 시각", dataIndex: "startedAt", width: 120 },
                    { title: "경과시간", dataIndex: "duration", width: 120 },
                    { title: "재시도", dataIndex: "retry", width: 100 },
                  ]}
                />
              </Card>
            </Col>
          </Row>
        </section>

        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <div className="section-kicker">ETL</div>
              <h3>NiFi 대시보드</h3>
            </div>
            <Badge status={nifiStatus.isError ? "error" : "success"} text={nifiStatus.isError ? "응답 없음" : "정상"} />
          </div>
          <Row gutter={[16, 16]}>
            <Col xs={12} xl={6}>
              <Card
                className={etlJobFilter === "all" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("all")}
              >
                <Statistic title="전체 JOB 수" value={nifiJobs.length} />
              </Card>
            </Col>
            <Col xs={12} xl={6}>
              <Card
                className={etlJobFilter === "running" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("running")}
              >
                <Statistic title="실행중 JOB" value={runningNifiJobs.length} valueStyle={{ color: "#2f7d32" }} />
              </Card>
            </Col>
            <Col xs={12} xl={6}>
              <Card
                className={etlJobFilter === "failed" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("failed")}
              >
                <Statistic title="실패 JOB" value={failedNifiJobs.length} valueStyle={{ color: "#c62828" }} />
              </Card>
            </Col>
            <Col xs={12} xl={6}>
              <Card
                className={etlJobFilter === "stopped" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("stopped")}
              >
                <Statistic title="중지 JOB" value={stoppedNifiJobs.length} />
              </Card>
            </Col>
            <Col xs={24}>
              <Card title={`${etlJobFilterLabel} 목록`} size="small">
                <Table<NifiJob>
                  rowKey="id"
                  size="small"
                  loading={showNifiInitialLoading}
                  dataSource={filteredNifiJobs}
                  pagination={false}
                  columns={[
                    {
                      title: "Process Group ID",
                      dataIndex: "id",
                      width: 280,
                      render: (id: string) => <Link to={`/etl/manage?processGroupId=${encodeURIComponent(id)}`}>{id}</Link>,
                    },
                    { title: "Name", dataIndex: "name" },
                    {
                      title: "상태",
                      dataIndex: "status",
                      width: 110,
                      render: (status: NifiJob["status"]) => etlStatusTag(status),
                    },
                    { title: "Processor", dataIndex: "processorCount", width: 110 },
                    { title: "Running", dataIndex: "runningProcessorCount", width: 110 },
                    { title: "Invalid", dataIndex: "failedProcessorCount", width: 110 },
                    { title: "Queued", dataIndex: "queued", width: 130 },
                  ]}
                />
              </Card>
            </Col>
          </Row>
        </section>

        <section className="dashboard-section cdc-section">
          <div className="section-heading">
            <div>
              <div className="section-kicker">CDC</div>
              <h3>Kafka CDC 파이프라인</h3>
            </div>
            <div className="section-statuses">
              <Badge
                status={data?.kafkaBrokerHealthy ? "success" : "error"}
                text={data?.kafkaBrokerHealthy ? "Kafka Broker 정상" : "Kafka Broker 응답 없음"}
              />
              <Badge
                status={data?.kafkaConnectHealthy ? "success" : "error"}
                text={data?.kafkaConnectHealthy ? "Kafka Connect 정상" : "Kafka Connect 응답 없음"}
              />
            </div>
          </div>
          <Row gutter={[16, 16]} className="dashboard-metrics">
            <Col xs={12} xl={6}>
              <Card loading={showDashboardInitialLoading}>
                <Statistic title="전체 파이프라인" value={data?.totalPipelines ?? 0} />
              </Card>
            </Col>
            <Col xs={12} xl={6}>
              <Card loading={showDashboardInitialLoading}>
                <Statistic title="RUNNING" value={data?.runningCount ?? 0} valueStyle={{ color: "#3f8600" }} />
              </Card>
            </Col>
            <Col xs={12} xl={6}>
              <Card loading={showDashboardInitialLoading}>
                <Statistic title="FAILED" value={data?.failedCount ?? 0} valueStyle={{ color: "#cf1322" }} />
              </Card>
            </Col>
            <Col xs={12} xl={6}>
              <Card loading={showDashboardInitialLoading}>
                <Statistic title="PAUSED" value={data?.pausedCount ?? 0} />
              </Card>
            </Col>
          </Row>
          <Row gutter={[16, 16]}>
            <Col xs={24} xl={12}>
              <Card title="최근 오류" size="small">
                <Table<PipelineCommandHistoryResponse>
                  rowKey="id"
                  size="small"
                  loading={showDashboardInitialLoading}
                  dataSource={data?.recentErrors}
                  pagination={false}
                  columns={historyColumns}
                />
              </Card>
            </Col>
            <Col xs={24} xl={12}>
              <Card title="최근 배포 이력" size="small">
                <Table<PipelineCommandHistoryResponse>
                  rowKey="id"
                  size="small"
                  loading={showDashboardInitialLoading}
                  dataSource={data?.recentDeployments}
                  pagination={false}
                  columns={historyColumns}
                />
              </Card>
            </Col>
          </Row>
        </section>
      </div>
    </div>
  );
}

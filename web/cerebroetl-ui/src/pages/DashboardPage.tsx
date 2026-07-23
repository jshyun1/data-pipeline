import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Badge, Button, Card, Col, DatePicker, Row, Space, Statistic, Table, Tag } from "antd";
import { Column, Line } from "@ant-design/plots";
import dayjs, { type Dayjs } from "dayjs";
import { getDashboardSummary } from "../api/dashboard";
import {
  getAirflowHealth,
  getNifiRootStatus,
  listAirflowAssetEvents,
  listAirflowDagRuns,
  listAirflowDagTasks,
  listAirflowDags,
  listAirflowTaskInstances,
} from "../api/platform";
import type { AirflowDagRun, AirflowTaskInstance, NifiProcessGroupStatusSnapshot } from "../api/platform";
import type { PipelineCommandHistoryResponse } from "../types/pipeline";

const { RangePicker } = DatePicker;

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

interface DataLoadDashboardData {
  dagCount: number;
  taskCount: number;
  daily: DailyLoadPoint[];
  topDags: DagLoadPoint[];
  topTasks: TaskLoadPoint[];
}

const EMPTY_DATA_LOAD_DASHBOARD: DataLoadDashboardData = {
  dagCount: 0,
  taskCount: 0,
  daily: [],
  topDags: [],
  topTasks: [],
};

// nifi_pipelines_dynamic.py/kafka_pipelines_dynamic.py의 verify_target_db_landing이
// 실제로 적재된 건수를 검증에 성공했을 때만 Airflow Asset 이벤트의 extra.count로
// 남긴다(실패/스킵 시엔 이벤트 자체가 없음) - 그 이벤트들을 날짜/DAG/태스크별로
// 집계한다. 가짜 수치가 아니라 실제 커밋된 데이터 건수만 반영된다.
async function getDataLoadDashboard(range: [Dayjs, Dayjs]): Promise<DataLoadDashboardData> {
  const dags = await listAirflowDags();
  const taskListResults = await Promise.allSettled(dags.map((dag) => listAirflowDagTasks(dag.dag_id)));
  const taskCount = taskListResults.reduce(
    (sum, result) => sum + (result.status === "fulfilled" ? result.value.length : 0),
    0,
  );

  const events = await listAirflowAssetEvents({
    timestampGte: range[0].startOf("day").toISOString(),
    timestampLte: range[1].endOf("day").toISOString(),
    limit: 1000,
  });

  const dailyMap = new Map<string, number>();
  const dagMap = new Map<string, number>();
  const taskMap = new Map<string, number>();

  events.forEach((event) => {
    const rawCount = event.extra?.count;
    const count = typeof rawCount === "number" ? rawCount : 0;
    if (count <= 0) {
      return;
    }
    const day = dayjs(event.timestamp).format("YYYY.MM.DD");
    dailyMap.set(day, (dailyMap.get(day) ?? 0) + count);
    if (event.source_dag_id) {
      dagMap.set(event.source_dag_id, (dagMap.get(event.source_dag_id) ?? 0) + count);
    }
    if (event.source_dag_id && event.source_task_id) {
      const taskKey = `${event.source_dag_id}.${event.source_task_id}`;
      taskMap.set(taskKey, (taskMap.get(taskKey) ?? 0) + count);
    }
  });

  const daily = Array.from(dailyMap.entries())
    .map(([date, count]) => ({ date, count }))
    .sort((a, b) => a.date.localeCompare(b.date));
  const topDags = Array.from(dagMap.entries())
    .map(([dagId, count]) => ({ dagId, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);
  const topTasks = Array.from(taskMap.entries())
    .map(([taskKey, count]) => ({ taskKey, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  return { dagCount: dags.length, taskCount, daily, topDags, topTasks };
}

export function DashboardPage() {
  const [etlJobFilter, setEtlJobFilter] = useState<EtlJobFilter>("all");
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([dayjs().subtract(6, "day"), dayjs()]);
  const [appliedRange, setAppliedRange] = useState<[Dayjs, Dayjs]>(dateRange);

  const dataLoadStats = useQuery({
    queryKey: ["dashboard-data-load", appliedRange[0].toISOString(), appliedRange[1].toISOString()],
    queryFn: () => getDataLoadDashboard(appliedRange),
    placeholderData: (previousData) => previousData,
  });

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

  const dataLoad = dataLoadStats.data ?? EMPTY_DATA_LOAD_DASHBOARD;
  const showDataLoadInitialLoading = dataLoadStats.isLoading && !dataLoadStats.data;

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
          <Row gutter={[16, 16]}>
            <Col xs={24} lg={6}>
              <Row gutter={[12, 12]}>
                <Col span={24}>
                  <Card loading={showDataLoadInitialLoading}>
                    <Statistic title="DAG" value={dataLoad.dagCount} valueStyle={{ color: "#08979c" }} />
                  </Card>
                </Col>
                <Col span={24}>
                  <Card loading={showDataLoadInitialLoading}>
                    <Statistic title="태스크" value={dataLoad.taskCount} valueStyle={{ color: "#5cdbd3" }} />
                  </Card>
                </Col>
              </Row>
            </Col>
            <Col xs={24} lg={18}>
              <Card title="일자별 데이터 로드 건수" size="small" loading={showDataLoadInitialLoading}>
                {dataLoad.daily.length > 0 ? (
                  <Line data={dataLoad.daily} xField="date" yField="count" height={220} />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </Col>
          </Row>
          <Row gutter={[16, 16]}>
            <Col xs={24} xl={12}>
              <Card title="Top 5 데이터 로드 DAG" size="small" loading={showDataLoadInitialLoading}>
                {dataLoad.topDags.length > 0 ? (
                  <Column data={dataLoad.topDags} xField="dagId" yField="count" height={240} />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </Col>
            <Col xs={24} xl={12}>
              <Card title="Top 10 데이터 로드 태스크" size="small" loading={showDataLoadInitialLoading}>
                {dataLoad.topTasks.length > 0 ? (
                  <Column data={dataLoad.topTasks} xField="taskKey" yField="count" height={240} />
                ) : (
                  <div className="empty-chart-placeholder">선택한 기간에 적재 이력이 없습니다.</div>
                )}
              </Card>
            </Col>
          </Row>
        </section>

        <section className="dashboard-section">
          <div className="section-heading">
            <div>
              <h3>Airflow 대시보드</h3>
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

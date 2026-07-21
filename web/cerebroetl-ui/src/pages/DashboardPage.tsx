import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Badge, Card, Col, Descriptions, Row, Statistic, Table, Tag } from "antd";
import { getDashboardSummary } from "../api/dashboard";
import { getAirflowHealth, getNifiRootStatus } from "../api/platform";
import type { NifiProcessGroupStatusSnapshot } from "../api/platform";
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

type EtlJobFilter = "all" | "running" | "failed";

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
  const filteredNifiJobs =
    etlJobFilter === "running" ? runningNifiJobs : etlJobFilter === "failed" ? failedNifiJobs : nifiJobs;
  const etlJobFilterLabel =
    etlJobFilter === "running" ? "실행중 JOB" : etlJobFilter === "failed" ? "실패 JOB" : "전체 JOB";
  const airflowServices = [
    { label: "Metadatabase", status: airflowHealth.data?.metadatabase?.status },
    { label: "Scheduler", status: airflowHealth.data?.scheduler?.status },
    { label: "Triggerer", status: airflowHealth.data?.triggerer?.status },
    { label: "DAG Processor", status: airflowHealth.data?.dag_processor?.status },
  ];
  const airflowHealthyCount = airflowServices.filter((service) => service.status?.toLowerCase() === "healthy").length;
  const airflowOnline = !airflowHealth.isError && airflowHealthyCount > 0;
  const showDashboardInitialLoading = dashboardSummary.isLoading && !dashboardSummary.data;
  const showAirflowInitialLoading = airflowHealth.isLoading && !airflowHealth.data;
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
          <Row gutter={[16, 16]}>
            <Col xs={24} md={8}>
              <Card loading={showAirflowInitialLoading}>
                <Statistic title="정상 컴포넌트" value={airflowHealthyCount} suffix={`/ ${airflowServices.length}`} />
              </Card>
            </Col>
            <Col xs={24} md={16}>
              <Card loading={showAirflowInitialLoading}>
                <Descriptions column={{ xs: 1, md: 2 }} size="small" bordered>
                  {airflowServices.map((service) => (
                    <Descriptions.Item key={service.label} label={service.label}>
                      <Tag color={service.status?.toLowerCase() === "healthy" ? "success" : "error"}>
                        {service.status ?? "UNKNOWN"}
                      </Tag>
                    </Descriptions.Item>
                  ))}
                </Descriptions>
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
            <Col xs={24} md={8}>
              <Card
                className={etlJobFilter === "all" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("all")}
              >
                <Statistic title="전체 JOB 수" value={nifiJobs.length} />
              </Card>
            </Col>
            <Col xs={24} md={8}>
              <Card
                className={etlJobFilter === "running" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("running")}
              >
                <Statistic title="실행중 JOB" value={runningNifiJobs.length} valueStyle={{ color: "#2f7d32" }} />
              </Card>
            </Col>
            <Col xs={24} md={8}>
              <Card
                className={etlJobFilter === "failed" ? "metric-card active" : "metric-card"}
                loading={showNifiInitialLoading}
                onClick={() => setEtlJobFilter("failed")}
              >
                <Statistic title="실패 JOB" value={failedNifiJobs.length} valueStyle={{ color: "#c62828" }} />
              </Card>
            </Col>
            <Col xs={24} lg={8}>
              <Card loading={showNifiInitialLoading}>
                <Statistic title="대기 FlowFile" value={nifiSnapshot?.flowFilesQueued ?? 0} />
              </Card>
            </Col>
            <Col xs={24} lg={8}>
              <Card loading={showNifiInitialLoading}>
                <Statistic title="활성 Thread" value={nifiSnapshot?.activeThreadCount ?? 0} />
              </Card>
            </Col>
            <Col xs={24} lg={8}>
              <Card loading={showNifiInitialLoading}>
                <Statistic title="중지 JOB" value={nifiJobs.filter((job) => job.status === "STOPPED").length} />
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

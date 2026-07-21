import { useQuery } from "@tanstack/react-query";
import { Badge, Card, Col, Row, Statistic, Table, Tag } from "antd";
import { getDashboardSummary } from "../api/dashboard";
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

export function DashboardPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["dashboard-summary"],
    queryFn: getDashboardSummary,
    refetchInterval: 15000,
  });

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>대시보드</h2>

      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={4}>
          <Card loading={isLoading}>
            <Statistic title="전체 파이프라인" value={data?.totalPipelines ?? 0} />
          </Card>
        </Col>
        <Col span={4}>
          <Card loading={isLoading}>
            <Statistic title="RUNNING" value={data?.runningCount ?? 0} valueStyle={{ color: "#3f8600" }} />
          </Card>
        </Col>
        <Col span={4}>
          <Card loading={isLoading}>
            <Statistic title="FAILED" value={data?.failedCount ?? 0} valueStyle={{ color: "#cf1322" }} />
          </Card>
        </Col>
        <Col span={4}>
          <Card loading={isLoading}>
            <Statistic title="PAUSED" value={data?.pausedCount ?? 0} />
          </Card>
        </Col>
        <Col span={4}>
          <Card loading={isLoading}>
            <div style={{ marginBottom: 8, color: "rgba(0,0,0,0.45)" }}>Kafka Broker</div>
            <Badge
              status={data?.kafkaBrokerHealthy ? "success" : "error"}
              text={data?.kafkaBrokerHealthy ? "정상" : "응답 없음"}
            />
          </Card>
        </Col>
        <Col span={4}>
          <Card loading={isLoading}>
            <div style={{ marginBottom: 8, color: "rgba(0,0,0,0.45)" }}>Kafka Connect</div>
            <Badge
              status={data?.kafkaConnectHealthy ? "success" : "error"}
              text={data?.kafkaConnectHealthy ? "정상" : "응답 없음"}
            />
          </Card>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={12}>
          <Card title="최근 오류" size="small">
            <Table<PipelineCommandHistoryResponse>
              rowKey="id"
              size="small"
              loading={isLoading}
              dataSource={data?.recentErrors}
              pagination={false}
              columns={historyColumns}
            />
          </Card>
        </Col>
        <Col span={12}>
          <Card title="최근 배포 이력" size="small">
            <Table<PipelineCommandHistoryResponse>
              rowKey="id"
              size="small"
              loading={isLoading}
              dataSource={data?.recentDeployments}
              pagination={false}
              columns={historyColumns}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
}

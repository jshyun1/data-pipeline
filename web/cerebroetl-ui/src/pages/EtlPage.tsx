import { useQuery } from "@tanstack/react-query";
import { Button, Card, Col, Descriptions, Row, Space, Statistic, Table, Tag } from "antd";
import { ExportOutlined, ReloadOutlined } from "@ant-design/icons";
import { getKafkaConnectInfo, listKafkaConnectors } from "../api/platform";

export function KafkaConnectPage() {
  const connectInfo = useQuery({
    queryKey: ["kafka-connect-info"],
    queryFn: getKafkaConnectInfo,
    refetchInterval: 60000,
    placeholderData: (previousData) => previousData,
  });
  const connectors = useQuery({
    queryKey: ["kafka-connectors"],
    queryFn: listKafkaConnectors,
    refetchInterval: 60000,
    placeholderData: (previousData) => previousData,
  });

  const isConnectHealthy = !connectInfo.isError && Boolean(connectInfo.data);
  const showConnectInitialLoading = connectInfo.isLoading && !connectInfo.data;
  const showConnectorsInitialLoading = connectors.isLoading && !connectors.data;

  return (
    <div>
      <div className="page-toolbar">
        <div>
          <div className="page-kicker">ETL RUNTIME</div>
          <h2 className="page-title">Kafka Connect</h2>
        </div>
        <Space>
          <Button
            icon={<ReloadOutlined />}
            onClick={() => {
              connectInfo.refetch();
              connectors.refetch();
            }}
          >
            새로고침
          </Button>
          <Button icon={<ExportOutlined />} href="https://localhost:8443/nifi" target="_blank" rel="noreferrer">
            NiFi 열기
          </Button>
          <Button icon={<ExportOutlined />} href="http://localhost:8083" target="_blank" rel="noreferrer">
            Kafka Connect 열기
          </Button>
        </Space>
      </div>

      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card loading={showConnectInitialLoading}>
            <Statistic
              title="Kafka Connect"
              value={isConnectHealthy ? "정상" : "응답 없음"}
              valueStyle={{ color: isConnectHealthy ? "#2f7d32" : "#c62828", fontSize: 24 }}
            />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card loading={showConnectorsInitialLoading}>
            <Statistic title="Connectors" value={connectors.data?.length ?? 0} />
          </Card>
        </Col>
        <Col xs={24} md={8}>
          <Card>
            <Statistic title="NiFi" value="콘솔 연결" valueStyle={{ fontSize: 24 }} />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title="Kafka Connect 정보" loading={showConnectInitialLoading}>
            <Descriptions column={1} size="small" bordered>
              <Descriptions.Item label="상태">
                <Tag color={isConnectHealthy ? "success" : "error"}>{isConnectHealthy ? "ONLINE" : "OFFLINE"}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Version">{connectInfo.data?.version ?? "-"}</Descriptions.Item>
              <Descriptions.Item label="Commit">{connectInfo.data?.commit ?? "-"}</Descriptions.Item>
              <Descriptions.Item label="Cluster ID">{connectInfo.data?.kafka_cluster_id ?? "-"}</Descriptions.Item>
            </Descriptions>
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title="Connector 목록">
            <Table
              rowKey="name"
              size="small"
              loading={showConnectorsInitialLoading}
              pagination={false}
              dataSource={(connectors.data ?? []).map((name) => ({ name }))}
              columns={[{ title: "Connector", dataIndex: "name" }]}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
}

import { useQuery } from "@tanstack/react-query";
import { Button, Card, Col, Descriptions, Row, Space, Statistic, Tag } from "antd";
import { ExportOutlined, ReloadOutlined } from "@ant-design/icons";
import { getAirflowHealth } from "../api/platform";

function healthColor(status?: string) {
  return status?.toLowerCase() === "healthy" ? "success" : "error";
}

export function AirflowPage() {
  const { data, isLoading, refetch } = useQuery({
    queryKey: ["airflow-health"],
    queryFn: getAirflowHealth,
    refetchInterval: 60000,
    placeholderData: (previousData) => previousData,
  });
  const showInitialLoading = isLoading && !data;

  const services = [
    { label: "Metadatabase", value: data?.metadatabase?.status },
    { label: "Scheduler", value: data?.scheduler?.status },
    { label: "Triggerer", value: data?.triggerer?.status },
    { label: "DAG Processor", value: data?.dag_processor?.status },
  ];

  const healthyCount = services.filter((service) => service.value?.toLowerCase() === "healthy").length;

  return (
    <div>
      <div className="page-toolbar">
        <div>
          <div className="page-kicker">WORKFLOW ORCHESTRATION</div>
          <h2 className="page-title">AirFlow</h2>
        </div>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={() => refetch()}>
            새로고침
          </Button>
          <Button icon={<ExportOutlined />} href="http://localhost:8090" target="_blank" rel="noreferrer">
            콘솔 열기
          </Button>
        </Space>
      </div>

      <Row gutter={[16, 16]}>
        <Col xs={24} md={8}>
          <Card loading={showInitialLoading}>
            <Statistic title="정상 컴포넌트" value={healthyCount} suffix={`/ ${services.length}`} />
          </Card>
        </Col>
        <Col xs={24} md={16}>
          <Card title="Airflow Health" loading={showInitialLoading}>
            <Descriptions column={1} size="small" bordered>
              {services.map((service) => (
                <Descriptions.Item key={service.label} label={service.label}>
                  <Tag color={healthColor(service.value)}>{service.value ?? "UNKNOWN"}</Tag>
                </Descriptions.Item>
              ))}
            </Descriptions>
          </Card>
        </Col>
      </Row>
    </div>
  );
}

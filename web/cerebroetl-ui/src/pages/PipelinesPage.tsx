import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Button,
  Checkbox,
  Collapse,
  Descriptions,
  Drawer,
  Form,
  Input,
  message,
  Modal,
  Popconfirm,
  Segmented,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
} from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { listConnections } from "../api/connections";
import {
  createLogFilePipeline,
  createPipeline,
  deletePipeline,
  getPipelineHistory,
  listPipelines,
} from "../api/pipelines";
import type { LogPipelineCreateRequest, PipelineCommandHistoryResponse, PipelineCreateRequest, PipelineResponse } from "../types/pipeline";

const STATUS_COLOR: Record<string, string> = {
  CREATED: "default",
  DEPLOYING: "processing",
  DEPLOYED: "success",
  PAUSED: "warning",
  STOPPED: "default",
  FAILED: "error",
};

export function PipelinesPage() {
  const queryClient = useQueryClient();
  const [modalOpen, setModalOpen] = useState(false);
  const [pipelineType, setPipelineType] = useState<"TABLE_CDC" | "LOG_FILE">("TABLE_CDC");
  const [form] = Form.useForm<PipelineCreateRequest>();
  const [logForm] = Form.useForm<LogPipelineCreateRequest>();
  const [detailPipelineId, setDetailPipelineId] = useState<number | null>(null);

  const { data: pipelines, isLoading } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });
  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });
  const { data: history, isLoading: historyLoading } = useQuery({
    queryKey: ["pipeline-history", detailPipelineId],
    queryFn: () => getPipelineHistory(detailPipelineId!),
    enabled: detailPipelineId !== null,
  });
  const detailPipeline = pipelines?.find((p) => p.id === detailPipelineId) ?? null;

  const invalidatePipelines = () => queryClient.invalidateQueries({ queryKey: ["pipelines"] });

  const closeModal = () => {
    setModalOpen(false);
    form.resetFields();
    logForm.resetFields();
  };

  const createMutation = useMutation({
    mutationFn: createPipeline,
    onSuccess: () => {
      message.success("파이프라인을 생성했습니다.");
      invalidatePipelines();
      closeModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const createLogMutation = useMutation({
    mutationFn: createLogFilePipeline,
    onSuccess: () => {
      message.success("로그 파이프라인을 생성했습니다.");
      invalidatePipelines();
      closeModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deletePipeline,
    onSuccess: () => {
      message.success("파이프라인을 삭제했습니다 (Kafka Connect 커넥터도 함께 정리됨).");
      invalidatePipelines();
    },
    onError: (error: Error) => message.error(error.message),
  });

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 16 }}>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setModalOpen(true)}
          disabled={!connections || connections.length < 1}
          title={!connections || connections.length < 1 ? "연결정보가 최소 1개는 있어야 합니다" : undefined}
        >
          신규 생성
        </Button>
      </div>

      <Table<PipelineResponse>
        rowKey="id"
        loading={isLoading}
        dataSource={pipelines}
        pagination={false}
        expandable={{
          expandedRowRender: (record) => (
            <Table
              rowKey="id"
              size="small"
              pagination={false}
              dataSource={record.connectors}
              columns={[
                { title: "역할", dataIndex: "connectorRole" },
                { title: "커넥터명", dataIndex: "connectorName" },
                { title: "클래스", dataIndex: "connectorClass" },
                {
                  title: "상태",
                  dataIndex: "status",
                  render: (value: string) => <Tag>{value}</Tag>,
                },
              ]}
            />
          ),
        }}
        columns={[
          { title: "이름", dataIndex: "name" },
          {
            title: "유형",
            dataIndex: "pipelineType",
            render: (value: string) => <Tag color={value === "LOG_FILE" ? "purple" : "blue"}>{value}</Tag>,
          },
          {
            title: "소스",
            render: (_, r) => (r.pipelineType === "LOG_FILE" ? "Filebeat" : `${r.sourceDbType} · ${r.sourceSchema}.${r.sourceTable}`),
          },
          {
            title: "타겟",
            render: (_, r) => `${r.targetDbType} · ${r.targetSchema}.${r.targetTable}`,
          },
          { title: "Topic", dataIndex: "topicName" },
          {
            title: "상태",
            dataIndex: "status",
            render: (value: string) => <Tag color={STATUS_COLOR[value] ?? "default"}>{value}</Tag>,
          },
          {
            title: "제어",
            render: (_, record) => (
              <Space wrap>
                <Button size="small" onClick={() => setDetailPipelineId(record.id)}>
                  상세
                </Button>
                <Popconfirm
                  title="이 파이프라인을 삭제할까요?"
                  description="배포된 Kafka Connect 커넥터도 함께 삭제됩니다."
                  onConfirm={() => deleteMutation.mutate(record.id)}
                >
                  <Button danger size="small">
                    삭제
                  </Button>
                </Popconfirm>
              </Space>
            ),
          },
        ]}
      />

      <Modal
        title="파이프라인 신규 생성"
        open={modalOpen}
        onCancel={closeModal}
        onOk={() => (pipelineType === "TABLE_CDC" ? form.submit() : logForm.submit())}
        confirmLoading={createMutation.isPending || createLogMutation.isPending}
        destroyOnHidden
        width={640}
      >
        <Segmented
          block
          value={pipelineType}
          onChange={(value) => setPipelineType(value as "TABLE_CDC" | "LOG_FILE")}
          options={[
            { label: "테이블 CDC", value: "TABLE_CDC" },
            { label: "로그파일 적재", value: "LOG_FILE" },
          ]}
          style={{ marginBottom: 16 }}
        />

        {pipelineType === "TABLE_CDC" ? (
          <Form<PipelineCreateRequest>
            form={form}
            layout="vertical"
            initialValues={{ deleteEnabled: true }}
            onFinish={(values) => createMutation.mutate(values)}
          >
            <Form.Item name="name" label="파이프라인명" rules={[{ required: true }]}>
              <Input placeholder="예: oracle-customers-to-postgres" />
            </Form.Item>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item
                name="sourceConnectionId"
                label="소스 연결"
                rules={[{ required: true }]}
                style={{ width: 260 }}
              >
                <Select
                  options={connections?.map((c) => ({ value: c.id, label: `${c.name} (${c.dbType})` }))}
                  placeholder="소스 DB 선택"
                />
              </Form.Item>
              <Form.Item
                name="targetConnectionId"
                label="타겟 연결"
                rules={[{ required: true }]}
                style={{ width: 260 }}
              >
                <Select
                  options={connections?.map((c) => ({ value: c.id, label: `${c.name} (${c.dbType})` }))}
                  placeholder="타겟 DB 선택"
                />
              </Form.Item>
            </Space>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="sourceSchema" label="소스 스키마" rules={[{ required: true }]} style={{ width: 260 }}>
                <Input placeholder="예: APPUSER" />
              </Form.Item>
              <Form.Item name="sourceTable" label="소스 테이블" rules={[{ required: true }]} style={{ width: 260 }}>
                <Input placeholder="예: CUSTOMERS" />
              </Form.Item>
            </Space>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="targetSchema" label="타겟 스키마" rules={[{ required: true }]} style={{ width: 260 }}>
                <Input placeholder="예: cdc_landing" />
              </Form.Item>
              <Form.Item name="targetTable" label="타겟 테이블" rules={[{ required: true }]} style={{ width: 260 }}>
                <Input placeholder="예: customers" />
              </Form.Item>
            </Space>
            <Form.Item
              name="topicPrefix"
              label="Topic Prefix"
              rules={[{ required: true }]}
              tooltip="실제 Kafka 토픽명은 {prefix}.{소스 스키마}.{소스 테이블} 형식으로 자동 생성됩니다"
            >
              <Input placeholder="예: oracle-cdc" />
            </Form.Item>
            <Form.Item name="deleteEnabled" valuePropName="checked">
              <Checkbox>DELETE 반영</Checkbox>
            </Form.Item>
            <Form.Item name="description" label="설명">
              <Input.TextArea rows={2} />
            </Form.Item>
          </Form>
        ) : (
          <Form<LogPipelineCreateRequest>
            form={logForm}
            layout="vertical"
            initialValues={{ readFrom: "END", encoding: "UTF-8" }}
            onFinish={(values) => createLogMutation.mutate(values)}
          >
            <Form.Item name="name" label="파이프라인명" rules={[{ required: true }]}>
              <Input placeholder="예: app-log-ingest" />
            </Form.Item>
            <Form.Item
              name="filePath"
              label="로그 파일 경로"
              rules={[{ required: true }]}
              tooltip="filebeat 컨테이너 기준 경로입니다 (호스트 경로 아님, docker-compose의 ./log-sources가 /var/log/app로 마운트됨)"
            >
              <Input placeholder="예: /var/log/app/app.log" />
            </Form.Item>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="readFrom" label="읽기 시작 위치" style={{ width: 260 }}>
                <Select
                  options={[
                    { value: "END", label: "END (신규 라인부터)" },
                    { value: "BEGINNING", label: "BEGINNING (파일 처음부터)" },
                  ]}
                />
              </Form.Item>
              <Form.Item name="encoding" label="인코딩" style={{ width: 260 }}>
                <Input placeholder="UTF-8" />
              </Form.Item>
            </Space>
            <Form.Item
              name="targetConnectionId"
              label="타겟 연결"
              rules={[{ required: true }]}
            >
              <Select
                options={connections?.map((c) => ({ value: c.id, label: `${c.name} (${c.dbType})` }))}
                placeholder="랜딩할 DB 선택"
              />
            </Form.Item>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="targetSchema" label="타겟 스키마" rules={[{ required: true }]} style={{ width: 260 }}>
                <Input placeholder="예: log_landing" />
              </Form.Item>
              <Form.Item name="targetTable" label="타겟 테이블" rules={[{ required: true }]} style={{ width: 260 }}>
                <Input placeholder="예: app_log" />
              </Form.Item>
            </Space>
            <Form.Item
              name="topicName"
              label="Kafka Topic"
              tooltip="비우면 log-{파이프라인 ID}로 자동 생성됩니다"
            >
              <Input placeholder="비워두면 자동 생성" />
            </Form.Item>
            <Form.Item name="agentHost" label="Agent Host" tooltip="메타데이터용 식별 필드 (선택)">
              <Input placeholder="예: filebeat" />
            </Form.Item>
            <Form.Item name="description" label="설명">
              <Input.TextArea rows={2} />
            </Form.Item>
          </Form>
        )}
      </Modal>

      <Drawer
        title={detailPipeline ? `파이프라인 상세 · ${detailPipeline.name}` : "파이프라인 상세"}
        open={detailPipelineId !== null}
        onClose={() => setDetailPipelineId(null)}
        width={640}
        destroyOnHidden
      >
        {detailPipeline && (
          <Tabs
            items={[
              {
                key: "info",
                label: "기본정보",
                children: (
                  <Descriptions column={1} bordered size="small">
                    <Descriptions.Item label="이름">{detailPipeline.name}</Descriptions.Item>
                    <Descriptions.Item label="유형">{detailPipeline.pipelineType}</Descriptions.Item>
                    <Descriptions.Item label="소스">
                      {detailPipeline.pipelineType === "LOG_FILE"
                        ? "Filebeat"
                        : `${detailPipeline.sourceDbType} · ${detailPipeline.sourceSchema}.${detailPipeline.sourceTable}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="타겟">
                      {`${detailPipeline.targetDbType} · ${detailPipeline.targetSchema}.${detailPipeline.targetTable}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="Topic">{detailPipeline.topicName}</Descriptions.Item>
                    <Descriptions.Item label="상태">
                      <Tag color={STATUS_COLOR[detailPipeline.status] ?? "default"}>{detailPipeline.status}</Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="설명">{detailPipeline.description ?? "-"}</Descriptions.Item>
                    <Descriptions.Item label="생성 시각">{detailPipeline.createdAt}</Descriptions.Item>
                    <Descriptions.Item label="수정 시각">{detailPipeline.updatedAt}</Descriptions.Item>
                  </Descriptions>
                ),
              },
              {
                key: "connectors",
                label: "Connector",
                children: (
                  <>
                    <Table
                      rowKey="id"
                      size="small"
                      pagination={false}
                      dataSource={detailPipeline.connectors}
                      style={{ marginBottom: 16 }}
                      columns={[
                        { title: "역할", dataIndex: "connectorRole" },
                        { title: "커넥터명", dataIndex: "connectorName" },
                        {
                          title: "상태",
                          dataIndex: "status",
                          render: (value: string) => <Tag>{value}</Tag>,
                        },
                      ]}
                    />
                    <Collapse
                      items={detailPipeline.connectors.map((c) => ({
                        key: c.id,
                        label: `${c.connectorName} · 설정/실행상태`,
                        children: (
                          <>
                            <div style={{ fontWeight: 600, marginBottom: 4 }}>Connector 설정</div>
                            <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>{c.connectorConfigJson}</pre>
                            <div style={{ fontWeight: 600, margin: "12px 0 4px" }}>실행상태</div>
                            <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>
                              {c.lastStatusJson ?? "아직 조회된 상태가 없습니다."}
                            </pre>
                          </>
                        ),
                      }))}
                    />
                  </>
                ),
              },
              {
                key: "history",
                label: "이력",
                children: (
                  <Table<PipelineCommandHistoryResponse>
                    rowKey="id"
                    size="small"
                    loading={historyLoading}
                    dataSource={history}
                    pagination={false}
                    columns={[
                      { title: "명령", dataIndex: "command" },
                      {
                        title: "결과",
                        dataIndex: "result",
                        render: (value: string | null) =>
                          value ? <Tag color={value === "SUCCESS" ? "success" : "error"}>{value}</Tag> : "-",
                      },
                      { title: "메시지", dataIndex: "message", ellipsis: true },
                      { title: "요청 시각", dataIndex: "requestedAt" },
                    ]}
                  />
                ),
              },
            ]}
          />
        )}
      </Drawer>
    </div>
  );
}

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  AutoComplete,
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
  Spin,
  Table,
  Tabs,
  Tag,
} from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { listConnections, listConnectionSchemas, listConnectionTables } from "../api/connections";
import { getPipelineDashboardSummary } from "../api/dashboard";
import {
  createLogFilePipeline,
  createPipeline,
  deletePipeline,
  deployPipeline,
  dismissConnectorDrift,
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

  // metadata-db는 RUNNING이라고 알고 있는데 실제 Kafka Connect엔 없는 커넥터가 있는지
  // 주기적으로 확인한다 - 예전에 이걸 놓쳐서 데이터가 조용히 안 들어온 적이 있었다.
  const { data: dashboardSummary } = useQuery({
    queryKey: ["pipeline-dashboard-summary"],
    queryFn: getPipelineDashboardSummary,
    refetchInterval: 30_000,
  });
  const connectorDrift = dashboardSummary?.connectorDrift ?? [];

  // 스키마/테이블을 자유 텍스트로 입력받으면 실제 DB 카탈로그와 대소문자가 어긋나서
  // CDC 토픽이 조용히 끊기는 문제가 실제로 있었다 - 커넥션을 고르면 그 DB에 실제로
  // 존재하는 스키마/테이블 목록을 조회해서 드롭다운으로만 고르게 한다.
  const sourceConnectionId = Form.useWatch("sourceConnectionId", form);
  const sourceSchema = Form.useWatch("sourceSchema", form);
  const targetConnectionId = Form.useWatch("targetConnectionId", form);
  const targetSchema = Form.useWatch("targetSchema", form);
  const logTargetConnectionId = Form.useWatch("targetConnectionId", logForm);
  const logTargetSchema = Form.useWatch("targetSchema", logForm);

  const sourceSchemasQuery = useQuery({
    queryKey: ["connection-schemas", sourceConnectionId],
    queryFn: () => listConnectionSchemas(sourceConnectionId!),
    enabled: sourceConnectionId != null,
  });
  const sourceTablesQuery = useQuery({
    queryKey: ["connection-tables", sourceConnectionId, sourceSchema],
    queryFn: () => listConnectionTables(sourceConnectionId!, sourceSchema!),
    enabled: sourceConnectionId != null && !!sourceSchema,
  });
  const targetSchemasQuery = useQuery({
    queryKey: ["connection-schemas", targetConnectionId],
    queryFn: () => listConnectionSchemas(targetConnectionId!),
    enabled: targetConnectionId != null,
  });
  const targetTablesQuery = useQuery({
    queryKey: ["connection-tables", targetConnectionId, targetSchema],
    queryFn: () => listConnectionTables(targetConnectionId!, targetSchema!),
    enabled: targetConnectionId != null && !!targetSchema,
  });
  const logTargetSchemasQuery = useQuery({
    queryKey: ["connection-schemas", logTargetConnectionId],
    queryFn: () => listConnectionSchemas(logTargetConnectionId!),
    enabled: logTargetConnectionId != null,
  });
  const logTargetTablesQuery = useQuery({
    queryKey: ["connection-tables", logTargetConnectionId, logTargetSchema],
    queryFn: () => listConnectionTables(logTargetConnectionId!, logTargetSchema!),
    enabled: logTargetConnectionId != null && !!logTargetSchema,
  });

  const schemaTableNotFoundContent = (query: { isFetching: boolean; isError: boolean }, emptyHint: string) =>
    query.isFetching ? <Spin size="small" /> : query.isError ? "조회 실패 - 연결정보를 확인하세요" : emptyHint;
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
      message.success("파이프라인을 생성했습니다. 이제 배포하세요.");
      invalidatePipelines();
      closeModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const createLogMutation = useMutation({
    mutationFn: createLogFilePipeline,
    onSuccess: () => {
      message.success("로그 파이프라인을 생성했습니다. 이제 배포하세요.");
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

  const deployMutation = useMutation({
    mutationFn: deployPipeline,
    onSuccess: () => {
      message.success("배포 완료");
      invalidatePipelines();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const dismissDriftMutation = useMutation({
    mutationFn: dismissConnectorDrift,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["pipeline-dashboard-summary"] }),
    onError: (error: Error) => message.error(error.message),
  });

  const driftByPipeline = connectorDrift.reduce<Record<number, { pipelineName: string; connectorNames: string[] }>>(
    (acc, entry) => {
      const existing = acc[entry.pipelineId] ?? { pipelineName: entry.pipelineName, connectorNames: [] };
      existing.connectorNames.push(entry.connectorName);
      acc[entry.pipelineId] = existing;
      return acc;
    },
    {},
  );

  return (
    <div>
      {Object.entries(driftByPipeline).map(([pipelineId, info]) => (
        <Alert
          key={pipelineId}
          type="warning"
          showIcon
          closable
          onClose={() => dismissDriftMutation.mutate(Number(pipelineId))}
          style={{ marginBottom: 12 }}
          message="파이프라인 커넥터가 Kafka Connect에서 사라졌습니다"
          description={
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <span>
                <b>{info.pipelineName}</b> — {info.connectorNames.join(", ")} 없음
              </span>
              <Button
                size="small"
                loading={deployMutation.isPending}
                onClick={() => deployMutation.mutate(Number(pipelineId))}
              >
                지금 재배포
              </Button>
            </div>
          }
        />
      ))}
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
                <Button
                  size="small"
                  loading={deployMutation.isPending}
                  onClick={() => deployMutation.mutate(record.id)}
                >
                  배포
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
                  onChange={(value) => {
                    form.setFieldsValue({ sourceSchema: undefined, sourceTable: undefined });
                    // 사용자가 이미 직접 입력한 값이 있으면 덮어쓰지 않고, 비어있을 때만
                    // 소스 DB 종류 기준 기본값을 채워준다(원하면 언제든 직접 수정 가능).
                    if (!form.getFieldValue("topicPrefix")) {
                      const dbType = connections?.find((c) => c.id === value)?.dbType;
                      if (dbType) {
                        form.setFieldsValue({ topicPrefix: `${dbType.toLowerCase()}-cdc` });
                      }
                    }
                  }}
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
                  onChange={() => form.setFieldsValue({ targetSchema: undefined, targetTable: undefined })}
                />
              </Form.Item>
            </Space>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="sourceSchema" label="소스 스키마" rules={[{ required: true }]} style={{ width: 260 }}>
                <Select
                  showSearch
                  disabled={!sourceConnectionId}
                  loading={sourceSchemasQuery.isFetching}
                  options={sourceSchemasQuery.data?.map((s) => ({ value: s, label: s }))}
                  notFoundContent={schemaTableNotFoundContent(sourceSchemasQuery, "스키마 없음")}
                  placeholder={sourceConnectionId ? "스키마 선택" : "먼저 소스 연결을 선택하세요"}
                  onChange={() => form.setFieldsValue({ sourceTable: undefined })}
                />
              </Form.Item>
              <Form.Item name="sourceTable" label="소스 테이블" rules={[{ required: true }]} style={{ width: 260 }}>
                <Select
                  showSearch
                  disabled={!sourceSchema}
                  loading={sourceTablesQuery.isFetching}
                  options={sourceTablesQuery.data?.map((t) => ({ value: t, label: t }))}
                  notFoundContent={schemaTableNotFoundContent(sourceTablesQuery, "테이블 없음")}
                  placeholder={sourceSchema ? "테이블 선택" : "먼저 스키마를 선택하세요"}
                />
              </Form.Item>
            </Space>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="targetSchema" label="타겟 스키마" rules={[{ required: true }]} style={{ width: 260 }}>
                <Select
                  showSearch
                  disabled={!targetConnectionId}
                  loading={targetSchemasQuery.isFetching}
                  options={targetSchemasQuery.data?.map((s) => ({ value: s, label: s }))}
                  notFoundContent={schemaTableNotFoundContent(targetSchemasQuery, "스키마 없음")}
                  placeholder={targetConnectionId ? "스키마 선택" : "먼저 타겟 연결을 선택하세요"}
                  onChange={() => form.setFieldsValue({ targetTable: undefined })}
                />
              </Form.Item>
              <Form.Item
                name="targetTable"
                label="타겟 테이블"
                rules={[{ required: true }]}
                style={{ width: 260 }}
                tooltip="기존 테이블을 고르거나, 첫 배포 때 자동 생성될 새 테이블명을 직접 입력할 수 있습니다"
              >
                <AutoComplete
                  disabled={!targetSchema}
                  options={targetTablesQuery.data?.map((t) => ({ value: t, label: t }))}
                  notFoundContent={schemaTableNotFoundContent(targetTablesQuery, "기존 테이블 없음 (새 이름으로 입력 가능)")}
                  placeholder={targetSchema ? "기존 테이블 선택 또는 새 이름 입력" : "먼저 스키마를 선택하세요"}
                  filterOption={(inputValue, option) =>
                    (option?.value ?? "").toLowerCase().includes(inputValue.toLowerCase())
                  }
                />
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
                onChange={() => logForm.setFieldsValue({ targetSchema: undefined, targetTable: undefined })}
              />
            </Form.Item>
            <Space style={{ width: "100%" }} size="large">
              <Form.Item name="targetSchema" label="타겟 스키마" rules={[{ required: true }]} style={{ width: 260 }}>
                <Select
                  showSearch
                  disabled={!logTargetConnectionId}
                  loading={logTargetSchemasQuery.isFetching}
                  options={logTargetSchemasQuery.data?.map((s) => ({ value: s, label: s }))}
                  notFoundContent={schemaTableNotFoundContent(logTargetSchemasQuery, "스키마 없음")}
                  placeholder={logTargetConnectionId ? "스키마 선택" : "먼저 타겟 연결을 선택하세요"}
                  onChange={() => logForm.setFieldsValue({ targetTable: undefined })}
                />
              </Form.Item>
              <Form.Item
                name="targetTable"
                label="타겟 테이블"
                rules={[{ required: true }]}
                style={{ width: 260 }}
                tooltip="기존 테이블을 고르거나, 첫 배포 때 자동 생성될 새 테이블명을 직접 입력할 수 있습니다"
              >
                <AutoComplete
                  disabled={!logTargetSchema}
                  options={logTargetTablesQuery.data?.map((t) => ({ value: t, label: t }))}
                  notFoundContent={schemaTableNotFoundContent(logTargetTablesQuery, "기존 테이블 없음 (새 이름으로 입력 가능)")}
                  placeholder={logTargetSchema ? "기존 테이블 선택 또는 새 이름 입력" : "먼저 스키마를 선택하세요"}
                  filterOption={(inputValue, option) =>
                    (option?.value ?? "").toLowerCase().includes(inputValue.toLowerCase())
                  }
                />
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

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  AutoComplete,
  Button,
  Card,
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
  Typography,
} from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { listConnections, listConnectionSchemas, listConnectionTables } from "../api/connections";
import {
  getPipelineDashboardSummary,
  getRealtimePipelineMetrics,
  type RealtimePipelineMetricResponse,
} from "../api/dashboard";
import {
  createLogFilePipeline,
  createPipeline,
  deletePipeline,
  dismissConnectorDrift,
  getPipelineHistory,
  listPipelineRuntimeStatuses,
  listPipelines,
} from "../api/pipelines";
import type {
  LogPipelineCreateRequest,
  PipelineCommandHistoryResponse,
  PipelineCreateRequest,
  PipelineResponse,
  PipelineRuntimeStatusResponse,
} from "../types/pipeline";

const STATUS_COLOR: Record<string, string> = {
  CREATED: "default",
  DEPLOYING: "processing",
  READY: "blue",
  DEPLOYED: "success",
  PAUSED: "warning",
  STOPPED: "default",
  FAILED: "error",
};

const STATUS_LABEL: Record<string, string> = {
  CREATED: "생성됨",
  DEPLOYING: "준비 중",
  READY: "실행 대기",
  DEPLOYED: "실행 중",
  PAUSED: "일시정지",
  STOPPED: "Sink 중지",
  FAILED: "실패",
};

const STATUS_OPTIONS = ["CREATED", "DEPLOYING", "READY", "DEPLOYED", "PAUSED", "STOPPED", "FAILED"].map((value) => ({
  value,
  label: STATUS_LABEL[value],
}));

const TYPE_OPTIONS = [
  { value: "TABLE_CDC", label: "TABLE_CDC" },
  { value: "LOG_FILE", label: "LOG_FILE" },
];

const RUNTIME_STATUS_LABEL: Record<string, string> = {
  NOT_DEPLOYED: "미배포",
  READY: "실행 대기",
  RUNNING: "실행 중",
  PAUSED: "일시정지",
  STOPPED: "중지",
  FAILED: "실패",
  MISSING: "구성 누락",
  UNKNOWN: "확인 불가",
  DEGRADED: "부분 이상",
};

const RUNTIME_STATUS_COLOR: Record<string, string> = {
  NOT_DEPLOYED: "default",
  READY: "blue",
  RUNNING: "success",
  PAUSED: "warning",
  STOPPED: "default",
  FAILED: "error",
  MISSING: "error",
  UNKNOWN: "default",
  DEGRADED: "warning",
};

const STATUS_SEVERITY: Record<string, number> = {
  FAILED: 0,
  DEPLOYING: 1,
  STOPPED: 2,
  PAUSED: 3,
  CREATED: 4,
  READY: 5,
  DEPLOYED: 6,
};

function formatDuration(seconds: number) {
  if (seconds < 60) return `${seconds}초`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)}분`;
  if (seconds < 86_400) return `${Math.ceil(seconds / 3600)}시간`;
  return `${Math.ceil(seconds / 86_400)}일`;
}

function formatRelativeTime(value: string | null) {
  if (!value) return "—";
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return "—";
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return "방금 전";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}분 전`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3600)}시간 전`;
  return `${Math.floor(seconds / 86_400)}일 전`;
}

export function PipelinesPage() {
  const queryClient = useQueryClient();
  const [modalOpen, setModalOpen] = useState(false);
  const [pipelineType, setPipelineType] = useState<"TABLE_CDC" | "LOG_FILE">("TABLE_CDC");
  const [form] = Form.useForm<PipelineCreateRequest>();
  const [logForm] = Form.useForm<LogPipelineCreateRequest>();
  const [detailPipelineId, setDetailPipelineId] = useState<number | null>(null);
  const [nameFilter, setNameFilter] = useState("");
  const [topicFilter, setTopicFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState<string[]>([]);
  const [typeFilter, setTypeFilter] = useState<string[]>([]);

  const { data: pipelines, isLoading } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });
  const { data: runtimeStatuses } = useQuery({
    queryKey: ["pipeline-runtime-statuses"],
    queryFn: listPipelineRuntimeStatuses,
    refetchInterval: 10_000,
  });
  const runtimeByPipeline = useMemo(
    () => new Map((runtimeStatuses ?? []).map((runtime) => [runtime.pipelineId, runtime])),
    [runtimeStatuses],
  );
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
  const { data: realtimeMetrics } = useQuery({
    queryKey: ["realtime-pipeline-metrics"],
    queryFn: getRealtimePipelineMetrics,
    refetchInterval: 20_000,
  });
  const connectorDrift = dashboardSummary?.connectorDrift ?? [];
  const metricByPipeline = useMemo(
    () => new Map((realtimeMetrics ?? []).map((metric) => [metric.pipelineId, metric])),
    [realtimeMetrics],
  );

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

  const filteredPipelines = useMemo(() => {
    const name = nameFilter.trim().toLowerCase();
    const topic = topicFilter.trim().toLowerCase();
    return (pipelines ?? []).filter((p) => {
      if (name && !p.name.toLowerCase().includes(name)) {
        return false;
      }
      if (topic && !(p.topicName ?? "").toLowerCase().includes(topic)) {
        return false;
      }
      if (statusFilter.length > 0 && !statusFilter.includes(p.status)) {
        return false;
      }
      if (typeFilter.length > 0 && !typeFilter.includes(p.pipelineType)) {
        return false;
      }
      return true;
    }).sort((a, b) => {
      const severity = (STATUS_SEVERITY[a.status] ?? 99) - (STATUS_SEVERITY[b.status] ?? 99);
      if (severity !== 0) return severity;
      const aLag = metricByPipeline.get(a.id)?.consumerLag ?? -1;
      const bLag = metricByPipeline.get(b.id)?.consumerLag ?? -1;
      return bLag - aLag;
    });
  }, [pipelines, nameFilter, topicFilter, statusFilter, typeFilter, metricByPipeline]);

  const renderLag = (metric: RealtimePipelineMetricResponse | undefined) => {
    if (!metric || metric.collectionStatus === "NO_DATA" || metric.consumerLag == null) return "—";
    const hasLag = metric.consumerLag > 0;
    return (
      <div>
        <Typography.Text type={hasLag ? "warning" : undefined} strong={hasLag}>
          {metric.consumerLag.toLocaleString()}
        </Typography.Text>
        {metric.estimatedRecoverySeconds != null && (
          <div>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              약 {formatDuration(metric.estimatedRecoverySeconds)}
            </Typography.Text>
          </div>
        )}
      </div>
    );
  };

  const renderRuntimeStatus = (pipeline: PipelineResponse, runtime: PipelineRuntimeStatusResponse | undefined) => {
    if (!runtime) {
      return <Tag color={STATUS_COLOR[pipeline.status] ?? "default"}>{STATUS_LABEL[pipeline.status] ?? pipeline.status}</Tag>;
    }
    return (
      <div>
        <Tag color={RUNTIME_STATUS_COLOR[runtime.runtimeStatus] ?? "default"}>
          {RUNTIME_STATUS_LABEL[runtime.runtimeStatus] ?? runtime.runtimeStatus}
        </Tag>
        {runtime.statusMismatch && <Tag color="warning">저장 상태 불일치</Tag>}
        <div>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            Source {runtime.sourceConnectorState ?? "—"} · Sink {runtime.sinkConnectorState ?? "—"}
          </Typography.Text>
        </div>
      </div>
    );
  };

  const invalidatePipelines = () => queryClient.invalidateQueries({ queryKey: ["pipelines"] });

  const closeModal = () => {
    setModalOpen(false);
    form.resetFields();
    logForm.resetFields();
  };

  const createMutation = useMutation({
    mutationFn: createPipeline,
    onSuccess: () => {
      message.success("CDC 파이프라인을 실행 대기 상태로 준비했습니다. Airflow DAG에서 시작하세요.");
      invalidatePipelines();
      closeModal();
    },
    onError: (error: Error) => {
      invalidatePipelines();
      message.error(error.message);
    },
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
      <Card title="CDC 파이프라인">
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
                  {/* 여기서 바로 재배포하지 않는다 - 배포/시작/중지는 Airflow 제어 DAG가
                      단독으로 지시한다(화면은 생성/삭제만 담당). 화면과 DAG 양쪽에서
                      배포가 가능하면 "누가 언제 실행시켰는지"가 이력에서 흐려진다. */}
                  <br />
                  <Typography.Text type="secondary">
                    복구하려면 Airflow에서 이 파이프라인의 제어 DAG를 action=deploy로 실행하세요.
                  </Typography.Text>
                </span>
              </div>
            }
          />
        ))}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16, gap: 12 }}>
          <Space wrap>
            <Input
              placeholder="이름 검색"
              allowClear
              style={{ width: 200 }}
              value={nameFilter}
              onChange={(e) => setNameFilter(e.target.value)}
            />
            <Input
              placeholder="Topic 검색"
              allowClear
              style={{ width: 200 }}
              value={topicFilter}
              onChange={(e) => setTopicFilter(e.target.value)}
            />
            <Select
              mode="multiple"
              allowClear
              placeholder="상태"
              style={{ minWidth: 160 }}
              options={STATUS_OPTIONS}
              value={statusFilter}
              onChange={setStatusFilter}
            />
            <Select
              mode="multiple"
              allowClear
              placeholder="유형"
              style={{ minWidth: 140 }}
              options={TYPE_OPTIONS}
              value={typeFilter}
              onChange={setTypeFilter}
            />
          </Space>
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
        dataSource={filteredPipelines}
        pagination={{ pageSize: 20, hideOnSinglePage: true, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
        columns={[
          { title: "이름", dataIndex: "name" },
          {
            title: "유형",
            dataIndex: "pipelineType",
            render: (value: string) => <Tag color={value === "LOG_FILE" ? "purple" : "blue"}>{value}</Tag>,
          },
          {
            title: "소스 → 타깃",
            render: (_, r) => (
              <div>
                <div>{r.pipelineType === "LOG_FILE" ? "Filebeat" : `${r.sourceDbType} · ${r.sourceSchema}.${r.sourceTable}`}</div>
                <Typography.Text type="secondary">
                  → {r.targetDbType} · {r.targetSchema}.{r.targetTable}
                </Typography.Text>
              </div>
            ),
          },
          { title: "Topic", dataIndex: "topicName" },
          {
            title: "상태",
            render: (_, record) => renderRuntimeStatus(record, runtimeByPipeline.get(record.id)),
          },
          {
            title: "지연",
            render: (_, record) => renderLag(metricByPipeline.get(record.id)),
          },
          {
            title: "마지막 처리",
            render: (_, record) => {
              const lastProgressAt = metricByPipeline.get(record.id)?.lastProgressAt ?? null;
              return <span title={lastProgressAt ?? undefined}>{formatRelativeTime(lastProgressAt)}</span>;
            },
          },
          {
            title: "관리",
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
      </Card>

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
                      {renderRuntimeStatus(detailPipeline, runtimeByPipeline.get(detailPipeline.id))}
                    </Descriptions.Item>
                    <Descriptions.Item label="설명">{detailPipeline.description ?? "-"}</Descriptions.Item>
                    <Descriptions.Item label="생성 시각">{detailPipeline.createdAt}</Descriptions.Item>
                    <Descriptions.Item label="수정 시각">{detailPipeline.updatedAt}</Descriptions.Item>
                  </Descriptions>
                ),
              },
              {
                key: "runtime",
                label: "운영 상태",
                children: (() => {
                  const runtime = runtimeByPipeline.get(detailPipeline.id);
                  if (!runtime) return <Spin size="small" />;
                  return (
                    <>
                      {runtime.statusMismatch && (
                        <Alert
                          type="warning"
                          showIcon
                          message="저장 상태와 Kafka Connect 실측 상태가 다릅니다"
                          description={runtime.runtimeStatusReason}
                          style={{ marginBottom: 16 }}
                        />
                      )}
                      <Descriptions column={1} bordered size="small">
                        <Descriptions.Item label="화면 실측 상태">
                          <Tag color={RUNTIME_STATUS_COLOR[runtime.runtimeStatus] ?? "default"}>
                            {RUNTIME_STATUS_LABEL[runtime.runtimeStatus] ?? runtime.runtimeStatus}
                          </Tag>
                        </Descriptions.Item>
                        <Descriptions.Item label="메타데이터 저장 상태">
                          {STATUS_LABEL[runtime.storedStatus] ?? runtime.storedStatus}
                        </Descriptions.Item>
                        <Descriptions.Item label="Source Connector">
                          {runtime.sourceConnectorState ?? "—"} · Tasks {runtime.sourceTaskStates.join(", ") || "—"}
                        </Descriptions.Item>
                        <Descriptions.Item label="Sink Connector">
                          {runtime.sinkConnectorState ?? "—"} · Tasks {runtime.sinkTaskStates.join(", ") || "—"}
                        </Descriptions.Item>
                        <Descriptions.Item label="마지막 실측 시각">
                          {runtime.runtimeCheckedAt ?? "확인 기록 없음"}
                        </Descriptions.Item>
                        <Descriptions.Item label="최근 제어 요청">
                          {runtime.lastCommand
                            ? `${runtime.lastCommand} · ${runtime.lastCommandResult ?? "처리 중"} · ${runtime.lastCommandAt ?? "—"}`
                            : "이력 없음"}
                        </Descriptions.Item>
                        <Descriptions.Item label="최근 제어 메시지">
                          {runtime.lastCommandMessage ?? "—"}
                        </Descriptions.Item>
                      </Descriptions>
                    </>
                  );
                })(),
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

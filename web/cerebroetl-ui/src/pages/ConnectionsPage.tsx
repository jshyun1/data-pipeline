import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Card, Descriptions, Form, Input, InputNumber, List, message, Modal, Popconfirm, Select, Space, Table, Tag, Tooltip } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import {
  checkCdcPrerequisites,
  createConnection,
  deleteConnection,
  listConnections,
  listConnectionUsages,
  testConnection,
  updateConnection,
  validateConnectionUpdate,
  validateNewConnection,
} from "../api/connections";
import { listPipelines } from "../api/pipelines";
import type {
  CdcPrerequisiteResponse,
  ConnectionCreateRequest,
  ConnectionResponse,
  ConnectionTestResponse,
  ConnectionUpdateRequest,
  DbType,
} from "../types/connection";

const STATUS_COLOR: Record<string, string> = {
  UNKNOWN: "default",
  TESTING: "processing",
  SUCCESS: "success",
  FAILED: "error",
};

export function ConnectionsPage() {
  const queryClient = useQueryClient();
  const [modalOpen, setModalOpen] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [readOnly, setReadOnly] = useState(false);
  const [formTestResult, setFormTestResult] = useState<ConnectionTestResponse | null>(null);
  const [prerequisites, setPrerequisites] = useState<CdcPrerequisiteResponse | null>(null);
  const [form] = Form.useForm<ConnectionCreateRequest>();

  const { data: connections, isLoading } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });

  const { data: usages } = useQuery({
    queryKey: ["connection-usages"],
    queryFn: listConnectionUsages,
  });
  const usageById = new Map((usages ?? []).map((usage) => [usage.connectionId, usage]));

  // 수정 화면에서 "이미 배포된 파이프라인은 재배포해야 반영된다"는 안내에 쓴다 -
  // 연결정보를 바꿔도 Kafka Connect에 이미 등록된 커넥터 설정은 자동으로 안 바뀐다.
  const { data: pipelines } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });

  const invalidateConnections = () => {
    queryClient.invalidateQueries({ queryKey: ["connections"] });
    queryClient.invalidateQueries({ queryKey: ["connection-usages"] });
  };

  const closeModal = () => {
    setModalOpen(false);
    setEditingId(null);
    setReadOnly(false);
    setFormTestResult(null);
    setPrerequisites(null);
    form.resetFields();
  };

  const createMutation = useMutation({
    mutationFn: createConnection,
    onSuccess: () => {
      message.success("연결정보를 등록했습니다.");
      invalidateConnections();
      closeModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, request }: { id: number; request: ConnectionUpdateRequest }) => updateConnection(id, request),
    onSuccess: () => {
      message.success("연결정보를 수정했습니다.");
      invalidateConnections();
      closeModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deleteConnection,
    onSuccess: () => {
      message.success("연결정보를 삭제했습니다.");
      invalidateConnections();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const testMutation = useMutation({
    mutationFn: testConnection,
    onSuccess: (result) => {
      if (result.status === "SUCCESS") {
        message.success(`${result.name}: 연결 성공`);
      } else {
        message.error(`${result.name}: 연결 실패`);
      }
      invalidateConnections();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const validateMutation = useMutation({
    mutationFn: async (values: ConnectionCreateRequest) => {
      if (editingId === null) {
        return validateNewConnection({
          dbType: values.dbType,
          host: values.host,
          port: values.port,
          databaseName: values.databaseName,
          serviceName: values.serviceName,
          username: values.username,
          password: values.password,
        });
      }
      const { password, ...rest } = values;
      return validateConnectionUpdate(editingId, { ...rest, password: password || undefined });
    },
    onSuccess: (result) => {
      setFormTestResult(result);
      result.success ? message.success(`연결 성공 (${result.latencyMs}ms)`) : message.error(result.message);
    },
    onError: (error: Error) => {
      setFormTestResult(null);
      message.error(error.message);
    },
  });

  const prerequisiteMutation = useMutation({
    mutationFn: checkCdcPrerequisites,
    onSuccess: setPrerequisites,
    onError: (error: Error) => message.error(error.message),
  });

  const dbType = Form.useWatch("dbType", form);

  const openCreateModal = () => {
    setEditingId(null);
    setReadOnly(false);
    form.resetFields();
    setFormTestResult(null);
    setModalOpen(true);
  };

  const fillFormFrom = (record: ConnectionResponse) => {
    form.setFieldsValue({
      name: record.name,
      dbType: record.dbType,
      host: record.host,
      port: record.port,
      databaseName: record.databaseName ?? undefined,
      serviceName: record.serviceName ?? undefined,
      schemaName: record.schemaName ?? undefined,
      username: record.username,
      password: undefined,
    });
  };

  const openEditModal = (record: ConnectionResponse) => {
    setEditingId(record.id);
    setReadOnly(false);
    setFormTestResult(null);
    fillFormFrom(record);
    setModalOpen(true);
  };

  const openDetailModal = (record: ConnectionResponse) => {
    setEditingId(record.id);
    setReadOnly(true);
    setPrerequisites(null);
    fillFormFrom(record);
    setModalOpen(true);
  };

  const referencingPipelines = (pipelines ?? []).filter(
    (p) => p.sourceConnectionId === editingId || p.targetConnectionId === editingId,
  );

  return (
    <div>
      <Card title="CDC 연결정보">
        <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 16 }}>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreateModal}>
            신규 등록
          </Button>
        </div>

        <Table<ConnectionResponse>
          rowKey="id"
          loading={isLoading}
          dataSource={connections}
          pagination={{ pageSize: 10, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
          columns={[
            { title: "이름", dataIndex: "name" },
            {
              title: "DB 유형",
              dataIndex: "dbType",
              render: (value: DbType) => <Tag>{value}</Tag>,
            },
            {
              title: "접속 정보",
              render: (_, record) => `${record.host}:${record.port}`,
            },
            { title: "Service/DB명", render: (_, r) => r.serviceName ?? r.databaseName ?? "-" },
            { title: "사용자", dataIndex: "username" },
            {
              title: "상태",
              dataIndex: "status",
              render: (value: string, record) => (
                <Tooltip title={record.lastTestedAt ? `마지막 테스트: ${record.lastTestedAt}` : "아직 테스트 안 함"}>
                  <Tag color={STATUS_COLOR[value] ?? "default"}>{value}</Tag>
                </Tooltip>
              ),
            },
            {
              title: "사용처",
              render: (_, record) => {
                const usage = usageById.get(record.id);
                if (!usage || usage.references.length === 0) return <Tag>미사용</Tag>;
                const detail = usage.references
                  .map((reference) => `${reference.referenceType} · ${reference.name} (${reference.role})`)
                  .join("\n");
                return (
                  <Tooltip title={<span style={{ whiteSpace: "pre-line" }}>{detail}</span>}>
                    <Space size={4} wrap>
                      {usage.cdcSourceCount + usage.cdcTargetCount > 0 && (
                        <Tag color="blue">CDC {usage.cdcSourceCount + usage.cdcTargetCount}</Tag>
                      )}
                      {usage.etlJobCount > 0 && <Tag color="purple">ETL {usage.etlJobCount}</Tag>}
                    </Space>
                  </Tooltip>
                );
              },
            },
            {
              title: "관리",
              render: (_, record) => (
                <Space wrap>
                  <Button size="small" onClick={() => openDetailModal(record)}>
                    상세
                  </Button>
                  <Button
                    size="small"
                    loading={testMutation.isPending && testMutation.variables === record.id}
                    onClick={() => testMutation.mutate(record.id)}
                  >
                    테스트
                  </Button>
                  <Button size="small" onClick={() => openEditModal(record)}>
                    수정
                  </Button>
                  <Popconfirm
                    title="이 연결정보를 삭제할까요?"
                    description={
                      usageById.get(record.id)?.deletable === false
                        ? "CDC 파이프라인 또는 ETL 작업에서 사용 중이므로 삭제할 수 없습니다."
                        : "삭제 후에는 복구할 수 없습니다."
                    }
                    onConfirm={() => deleteMutation.mutate(record.id)}
                    disabled={usageById.get(record.id)?.deletable === false}
                  >
                    <Button
                      danger
                      size="small"
                      disabled={usageById.get(record.id)?.deletable === false}
                      loading={deleteMutation.isPending}
                    >
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
        title={editingId === null ? "연결정보 신규 등록" : readOnly ? "연결정보 상세" : "연결정보 수정"}
        open={modalOpen}
        onCancel={closeModal}
        footer={
          readOnly ? (
            <Button onClick={closeModal}>닫기</Button>
          ) : (
            <Space>
              <Button onClick={closeModal}>취소</Button>
              <Button
                loading={validateMutation.isPending}
                onClick={async () => validateMutation.mutate(await form.validateFields())}
              >
                연결 테스트
              </Button>
              <Tooltip title={!formTestResult?.success ? "연결 테스트에 성공해야 저장할 수 있습니다." : undefined}>
                <Button
                  type="primary"
                  disabled={!formTestResult?.success}
                  loading={createMutation.isPending || updateMutation.isPending}
                  onClick={() => form.submit()}
                >
                  저장
                </Button>
              </Tooltip>
            </Space>
          )
        }
        destroyOnHidden
      >
        {editingId !== null && !readOnly && (
          <Alert
            type="warning"
            showIcon
            style={{ marginBottom: 16 }}
            message="수정해도 이미 배포된 Kafka Connect 커넥터에는 즉시 반영되지 않습니다"
            description={
              referencingPipelines.length > 0 ? (
                <>
                  이 연결정보를 쓰는 파이프라인: {referencingPipelines.map((p) => `${p.name}(${p.status})`).join(", ")}
                  <br />
                  변경 사항(호스트/계정/비밀번호 등)을 실제로 반영하려면 위 파이프라인들을 다시 "배포"해야 합니다.
                </>
              ) : (
                "이 연결정보를 참조하는 파이프라인은 없지만, 나중에 연결해서 배포할 때는 새 값이 그대로 쓰입니다."
              )
            }
          />
        )}
        <Form<ConnectionCreateRequest>
          form={form}
          layout="vertical"
          disabled={readOnly}
          initialValues={{ dbType: "ORACLE", port: 1521 }}
          onValuesChange={() => setFormTestResult(null)}
          onFinish={(values) => {
            if (editingId === null) {
              createMutation.mutate(values);
            } else {
              const { password, ...rest } = values;
              updateMutation.mutate({
                id: editingId,
                request: { ...rest, password: password || undefined },
              });
            }
          }}
        >
          {!readOnly && formTestResult && (
            <Alert
              type={formTestResult.success ? "success" : "error"}
              showIcon
              style={{ marginBottom: 16 }}
              message={formTestResult.success ? `연결 성공 (${formTestResult.latencyMs}ms)` : "연결 실패"}
              description={formTestResult.message}
            />
          )}
          <Form.Item name="name" label="연결명" rules={[{ required: true }]}>
            <Input placeholder="예: oracle-source-poc" />
          </Form.Item>
          <Form.Item name="dbType" label="DB 유형" rules={[{ required: true }]}>
            <Select
              options={[
                { value: "ORACLE", label: "ORACLE" },
                { value: "POSTGRESQL", label: "POSTGRESQL" },
              ]}
            />
          </Form.Item>
          <Form.Item name="host" label="Host" rules={[{ required: true }]}>
            <Input placeholder="예: oracle-db" />
          </Form.Item>
          <Form.Item name="port" label="Port" rules={[{ required: true }]}>
            <InputNumber style={{ width: "100%" }} min={1} max={65535} />
          </Form.Item>
          {dbType === "ORACLE" ? (
            <Form.Item name="serviceName" label="Service Name (PDB)" rules={[{ required: true }]}>
              <Input placeholder="예: XEPDB1" />
            </Form.Item>
          ) : (
            <Form.Item name="databaseName" label="Database 명" rules={[{ required: true }]}>
              <Input placeholder="예: tarantula" />
            </Form.Item>
          )}
          <Form.Item
            name="username"
            label="사용자명"
            rules={[{ required: true }]}
            extra={
              dbType === "ORACLE"
                ? "CDC 소스로 사용할 계정이라면 반드시 C##으로 시작하는 공통 사용자를 입력하세요 " +
                  "(예: C##DBZUSER). LogMiner는 CDB 레벨에서 동작해서 appuser 같은 PDB 로컬 사용자는 " +
                  "비밀번호가 맞아도 인증되지 않습니다. 타겟(싱크)으로만 쓸 계정이라면 일반 사용자로 충분합니다."
                : undefined
            }
          >
            <Input placeholder={dbType === "ORACLE" ? "예: C##DBZUSER" : undefined} />
          </Form.Item>
          <Form.Item
            name="password"
            label="비밀번호"
            rules={[{ required: editingId === null && !readOnly }]}
            extra={
              readOnly
                ? "보안상 저장된 비밀번호는 화면에 표시되지 않습니다."
                : editingId === null
                  ? undefined
                  : "비워두면 기존 비밀번호를 그대로 유지합니다."
            }
          >
            <Input.Password
              placeholder={readOnly ? "(저장된 값 사용 중)" : editingId === null ? undefined : "변경하지 않으려면 비워두세요"}
            />
          </Form.Item>
        </Form>
        {readOnly && editingId !== null && (
          <Card
            size="small"
            title="CDC 사전요건"
            style={{ marginTop: 16 }}
            extra={
              <Button
                size="small"
                loading={prerequisiteMutation.isPending}
                onClick={() => prerequisiteMutation.mutate(editingId)}
              >
                사전요건 점검
              </Button>
            }
          >
            {!prerequisites ? (
              <Alert type="info" showIcon message="CDC 소스로 사용하기 전에 데이터베이스 설정과 권한을 점검하세요." />
            ) : (
              <>
                <Descriptions size="small" column={2} style={{ marginBottom: 8 }}>
                  <Descriptions.Item label="종합 결과">
                    <Tag color={prerequisites.overallStatus === "PASS" ? "success" : prerequisites.overallStatus === "FAIL" ? "error" : "warning"}>
                      {prerequisites.overallStatus}
                    </Tag>
                  </Descriptions.Item>
                  <Descriptions.Item label="점검 시각">{prerequisites.checkedAt}</Descriptions.Item>
                </Descriptions>
                <List
                  size="small"
                  dataSource={prerequisites.checks}
                  renderItem={(check) => (
                    <List.Item>
                      <List.Item.Meta
                        title={
                          <Space>
                            <Tag color={check.status === "PASS" ? "success" : check.status === "FAIL" ? "error" : "warning"}>
                              {check.status}
                            </Tag>
                            {check.label}
                            {check.actualValue && <span style={{ color: "#777" }}>({check.actualValue})</span>}
                          </Space>
                        }
                        description={check.status === "PASS" ? undefined : check.guidance}
                      />
                    </List.Item>
                  )}
                />
              </>
            )}
          </Card>
        )}
      </Modal>
    </div>
  );
}

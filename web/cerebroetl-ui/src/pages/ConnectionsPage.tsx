import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Card, Form, Input, InputNumber, message, Modal, Popconfirm, Select, Space, Table, Tag, Tooltip } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { createConnection, deleteConnection, listConnections, testConnection, updateConnection } from "../api/connections";
import { listPipelines } from "../api/pipelines";
import type { ConnectionCreateRequest, ConnectionResponse, ConnectionUpdateRequest, DbType } from "../types/connection";

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
  const [form] = Form.useForm<ConnectionCreateRequest>();

  const { data: connections, isLoading } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });

  // 수정 화면에서 "이미 배포된 파이프라인은 재배포해야 반영된다"는 안내에 쓴다 -
  // 연결정보를 바꿔도 Kafka Connect에 이미 등록된 커넥터 설정은 자동으로 안 바뀐다.
  const { data: pipelines } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });

  const invalidateConnections = () => queryClient.invalidateQueries({ queryKey: ["connections"] });

  const closeModal = () => {
    setModalOpen(false);
    setEditingId(null);
    setReadOnly(false);
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

  const dbType = Form.useWatch("dbType", form);

  const openCreateModal = () => {
    setEditingId(null);
    setReadOnly(false);
    form.resetFields();
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
    fillFormFrom(record);
    setModalOpen(true);
  };

  const openDetailModal = (record: ConnectionResponse) => {
    setEditingId(record.id);
    setReadOnly(true);
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
              title: "관리",
              render: (_, record) => (
                <Space>
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
                    description="이 연결정보를 쓰는 파이프라인이 있으면 먼저 정리해야 합니다."
                    onConfirm={() => deleteMutation.mutate(record.id)}
                  >
                    <Button danger size="small" loading={deleteMutation.isPending}>
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
        onOk={() => form.submit()}
        confirmLoading={createMutation.isPending || updateMutation.isPending}
        footer={readOnly ? <Button onClick={closeModal}>닫기</Button> : undefined}
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
            <Form.Item name="serviceName" label="Service Name (PDB)">
              <Input placeholder="예: XEPDB1" />
            </Form.Item>
          ) : (
            <Form.Item name="databaseName" label="Database 명">
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
      </Modal>
    </div>
  );
}

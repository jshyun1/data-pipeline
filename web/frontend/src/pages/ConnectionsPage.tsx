import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Form, Input, InputNumber, message, Modal, Popconfirm, Select, Table, Tag } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { createConnection, deleteConnection, listConnections } from "../api/connections";
import type { ConnectionCreateRequest, ConnectionResponse, DbType } from "../types/connection";

const STATUS_COLOR: Record<string, string> = {
  UNKNOWN: "default",
  TESTING: "processing",
  SUCCESS: "success",
  FAILED: "error",
};

export function ConnectionsPage() {
  const queryClient = useQueryClient();
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm<ConnectionCreateRequest>();

  const { data: connections, isLoading } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });

  const createMutation = useMutation({
    mutationFn: createConnection,
    onSuccess: () => {
      message.success("연결정보를 등록했습니다.");
      queryClient.invalidateQueries({ queryKey: ["connections"] });
      setModalOpen(false);
      form.resetFields();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deleteConnection,
    onSuccess: () => {
      message.success("연결정보를 삭제했습니다.");
      queryClient.invalidateQueries({ queryKey: ["connections"] });
    },
    onError: (error: Error) => message.error(error.message),
  });

  const dbType = Form.useWatch("dbType", form);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
        <h2 style={{ margin: 0 }}>연결정보 관리</h2>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setModalOpen(true)}>
          신규 등록
        </Button>
      </div>

      <Table<ConnectionResponse>
        rowKey="id"
        loading={isLoading}
        dataSource={connections}
        pagination={false}
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
            render: (value: string) => <Tag color={STATUS_COLOR[value] ?? "default"}>{value}</Tag>,
          },
          {
            title: "관리",
            render: (_, record) => (
              <Popconfirm
                title="이 연결정보를 삭제할까요?"
                description="이 연결정보를 쓰는 파이프라인이 있으면 먼저 정리해야 합니다."
                onConfirm={() => deleteMutation.mutate(record.id)}
              >
                <Button danger size="small" loading={deleteMutation.isPending}>
                  삭제
                </Button>
              </Popconfirm>
            ),
          },
        ]}
      />

      <Modal
        title="연결정보 신규 등록"
        open={modalOpen}
        onCancel={() => setModalOpen(false)}
        onOk={() => form.submit()}
        confirmLoading={createMutation.isPending}
        destroyOnHidden
      >
        <Form<ConnectionCreateRequest>
          form={form}
          layout="vertical"
          initialValues={{ dbType: "ORACLE", port: 1521 }}
          onFinish={(values) => createMutation.mutate(values)}
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
          <Form.Item name="password" label="비밀번호" rules={[{ required: true }]}>
            <Input.Password />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}

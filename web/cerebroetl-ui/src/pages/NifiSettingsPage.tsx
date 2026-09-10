import { DeleteOutlined, EditOutlined, PlusOutlined, SettingOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Card, Checkbox, Form, Input, Modal, Popconfirm, Radio, Space, Table, Tag, message } from "antd";
import { useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import {
  createNifiParameterContext,
  deleteNifiParameterContext,
  listNifiParameterContexts,
  updateNifiParameterContext,
  type NifiParameterContextResponse,
  type NifiParameterRequest,
} from "../api/nifi";

type NifiSettingsTarget = "parameters";

interface ParameterEntry {
  id: string;
  name: string;
  value: string | null;
  sensitive: boolean;
  description?: string | null;
}

interface ContextFormValues {
  name: string;
  description?: string;
}

interface ParameterFormValues {
  name: string;
  value?: string;
  setEmptyString?: boolean;
  sensitive: boolean;
  description?: string;
}

const NIFI_SETTINGS_ITEMS: Array<{
  key: NifiSettingsTarget;
  label: string;
  icon: ReactNode;
}> = [
  {
    key: "parameters",
    label: "전역변수 설정",
    icon: <SettingOutlined />,
  },
];

function contextParameters(record: NifiParameterContextResponse): ParameterEntry[] {
  return (record.component.parameters ?? []).map((entity, index) => {
    const parameter = entity.parameter;
    return {
      id: `${parameter.name}-${index}`,
      name: parameter.name,
      value: parameter.value ?? null,
      sensitive: Boolean(parameter.sensitive),
      description: parameter.description ?? null,
    };
  });
}

function toRequestParameters(parameters: ParameterEntry[]): NifiParameterRequest[] {
  return parameters.map((parameter) => ({
    name: parameter.name,
    value: parameter.value,
    sensitive: parameter.sensitive,
    description: parameter.description ?? null,
  }));
}

export function NifiSettingsPage() {
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const targetParam = searchParams.get("target");
  const selectedKey: NifiSettingsTarget = targetParam === "parameters" ? targetParam : "parameters";
  const selectedItem = useMemo(
    () => NIFI_SETTINGS_ITEMS.find((item) => item.key === selectedKey) ?? NIFI_SETTINGS_ITEMS[0],
    [selectedKey],
  );
  const [contextModalOpen, setContextModalOpen] = useState(false);
  const [parameterModalOpen, setParameterModalOpen] = useState(false);
  const [editingContextId, setEditingContextId] = useState<string | null>(null);
  const [editingParameterId, setEditingParameterId] = useState<string | null>(null);
  const [draftParameters, setDraftParameters] = useState<ParameterEntry[]>([]);
  const [contextForm] = Form.useForm<ContextFormValues>();
  const [parameterForm] = Form.useForm<ParameterFormValues>();
  const setEmptyString = Form.useWatch("setEmptyString", parameterForm);

  const { data: contexts, isLoading } = useQuery({
    queryKey: ["nifi-parameter-contexts"],
    queryFn: listNifiParameterContexts,
  });

  const invalidateParameterContexts = () => {
    queryClient.invalidateQueries({ queryKey: ["nifi-parameter-contexts"] });
  };

  const createMutation = useMutation({
    mutationFn: createNifiParameterContext,
    onSuccess: () => {
      message.success("NiFi 파라미터 컨텍스트를 등록했습니다.");
      invalidateParameterContexts();
      closeContextModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, parameters }: { id: string; parameters: ContextFormValues }) =>
      updateNifiParameterContext(id, {
        name: parameters.name,
        description: parameters.description ?? null,
        parameters: toRequestParameters(draftParameters),
      }),
    onSuccess: () => {
      message.success("NiFi 파라미터 컨텍스트를 수정했습니다.");
      invalidateParameterContexts();
      closeContextModal();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deleteNifiParameterContext,
    onSuccess: () => {
      message.success("NiFi 파라미터 컨텍스트를 삭제했습니다.");
      invalidateParameterContexts();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const selectItem = (key: NifiSettingsTarget) => {
    setSearchParams({ target: key });
  };

  const openCreateContextModal = () => {
    setEditingContextId(null);
    setDraftParameters([]);
    contextForm.resetFields();
    setContextModalOpen(true);
  };

  const openEditContextModal = (record: NifiParameterContextResponse) => {
    setEditingContextId(record.id);
    setDraftParameters(contextParameters(record));
    contextForm.setFieldsValue({
      name: record.component.name,
      description: record.component.description ?? undefined,
    });
    setContextModalOpen(true);
  };

  const closeContextModal = () => {
    setContextModalOpen(false);
    setEditingContextId(null);
    setDraftParameters([]);
    contextForm.resetFields();
  };

  const openCreateParameterModal = () => {
    setEditingParameterId(null);
    parameterForm.resetFields();
    parameterForm.setFieldsValue({ sensitive: false, setEmptyString: false });
    setParameterModalOpen(true);
  };

  const openEditParameterModal = (record: ParameterEntry) => {
    setEditingParameterId(record.id);
    parameterForm.setFieldsValue({
      name: record.name,
      value: record.value ?? undefined,
      setEmptyString: record.value === "",
      sensitive: record.sensitive,
      description: record.description ?? undefined,
    });
    setParameterModalOpen(true);
  };

  const closeParameterModal = () => {
    setParameterModalOpen(false);
    setEditingParameterId(null);
    parameterForm.resetFields();
  };

  const saveParameter = async () => {
    const values = await parameterForm.validateFields();
    const nextParameter: ParameterEntry = {
      id: editingParameterId ?? `${values.name}-${Date.now()}`,
      name: values.name,
      value: values.setEmptyString ? "" : values.value ?? null,
      sensitive: values.sensitive,
      description: values.description ?? null,
    };

    setDraftParameters((current) =>
      editingParameterId === null
        ? [...current, nextParameter]
        : current.map((parameter) => (parameter.id === editingParameterId ? nextParameter : parameter)),
    );
    closeParameterModal();
  };

  const saveContext = async () => {
    const values = await contextForm.validateFields();
    if (editingContextId === null) {
      createMutation.mutate({
        name: values.name,
        description: values.description ?? null,
        parameters: toRequestParameters(draftParameters),
      });
      return;
    }
    updateMutation.mutate({ id: editingContextId, parameters: values });
  };

  const deleteDraftParameter = (id: string) => {
    setDraftParameters((current) => current.filter((parameter) => parameter.id !== id));
  };

  return (
    <div className="nifi-settings-page">
      <aside className="nifi-settings-list" aria-label="ETL 관리 설정 목록">
        {NIFI_SETTINGS_ITEMS.map((item) => (
          <button
            key={item.key}
            type="button"
            className={item.key === selectedKey ? "nifi-settings-list-item active" : "nifi-settings-list-item"}
            onClick={() => selectItem(item.key)}
          >
            <span className="nifi-settings-list-icon" aria-hidden="true">
              {item.icon}
            </span>
            <span>{item.label}</span>
          </button>
        ))}
      </aside>

      <section className="nifi-settings-frame" aria-label={selectedItem.label}>
        <Card title={selectedItem.label}>
          <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 16 }}>
            <Button type="primary" icon={<PlusOutlined />} onClick={openCreateContextModal}>
              신규 등록
            </Button>
          </div>

          <Table<NifiParameterContextResponse>
            rowKey="id"
            loading={isLoading}
            dataSource={contexts}
            pagination={{ pageSize: 10, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
            columns={[
              { title: "이름", render: (_, record) => record.component.name },
              {
                title: "설명",
                render: (_, record) => record.component.description || "-",
              },
              {
                title: "파라미터",
                render: (_, record) => <Tag color="blue">{record.component.parameters?.length ?? 0}개</Tag>,
              },
              {
                title: "관리",
                render: (_, record) => (
                  <Space wrap>
                    <Button size="small" icon={<EditOutlined />} onClick={() => openEditContextModal(record)}>
                      수정
                    </Button>
                    <Popconfirm
                      title="이 파라미터 컨텍스트를 삭제할까요?"
                      description="NiFi에서도 함께 삭제됩니다."
                      onConfirm={() => deleteMutation.mutate(record.id)}
                    >
                      <Button
                        danger
                        size="small"
                        icon={<DeleteOutlined />}
                        loading={deleteMutation.isPending && deleteMutation.variables === record.id}
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
      </section>

      <Modal
        title={editingContextId === null ? "전역변수 신규등록" : "전역변수 수정"}
        open={contextModalOpen}
        onCancel={closeContextModal}
        footer={
          <Space>
            <Button onClick={closeContextModal}>Cancel</Button>
            <Button type="primary" loading={createMutation.isPending || updateMutation.isPending} onClick={saveContext}>
              OK
            </Button>
          </Space>
        }
        destroyOnHidden
        width={860}
      >
        <div className="nifi-context-modal-tabs">
          <span className="active">Settings</span>
          <span>Parameters</span>
        </div>
        <Form<ContextFormValues> form={contextForm} layout="vertical" className="nifi-context-form">
          <Form.Item name="name" label="Name" rules={[{ required: true, message: "Name is required." }]}>
            <Input />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <Input.TextArea rows={5} />
          </Form.Item>
        </Form>

        <div className="nifi-parameter-toolbar">
          <Button icon={<PlusOutlined />} onClick={openCreateParameterModal}>
            파라미터 추가 +
          </Button>
        </div>

        <Table<ParameterEntry>
          rowKey="id"
          size="small"
          dataSource={draftParameters}
          pagination={false}
          columns={[
            { title: "파라미터 이름", dataIndex: "name" },
            {
              title: "값",
              render: (_, record) => (record.sensitive ? <Tag color="red">Sensitive</Tag> : record.value ?? "-"),
            },
            {
              title: "설명",
              dataIndex: "description",
              render: (value?: string | null) => value || "-",
            },
            {
              title: "관리",
              width: 150,
              render: (_, record) => (
                <Space>
                  <Button size="small" onClick={() => openEditParameterModal(record)}>
                    수정
                  </Button>
                  <Button danger size="small" onClick={() => deleteDraftParameter(record.id)}>
                    삭제
                  </Button>
                </Space>
              ),
            },
          ]}
        />
      </Modal>

      <Modal
        title={editingParameterId === null ? "Add Parameter" : "Edit Parameter"}
        open={parameterModalOpen}
        onCancel={closeParameterModal}
        footer={
          <Space>
            <Button onClick={closeParameterModal}>Cancel</Button>
            <Button type="primary" onClick={saveParameter}>
              OK
            </Button>
          </Space>
        }
        destroyOnHidden
        width={480}
      >
        <Form<ParameterFormValues>
          form={parameterForm}
          layout="vertical"
          className="nifi-parameter-form"
          initialValues={{ sensitive: false, setEmptyString: false }}
        >
          <Form.Item name="name" label="Name" rules={[{ required: true, message: "Property name is required." }]}>
            <Input />
          </Form.Item>
          <Form.Item name="value" label="Value" rules={[{ required: !setEmptyString, message: "Value is required." }]}>
            <Input.TextArea rows={4} disabled={setEmptyString} />
          </Form.Item>
          <Form.Item name="setEmptyString" valuePropName="checked" className="nifi-checkbox-item">
            <Checkbox>Set empty string</Checkbox>
          </Form.Item>
          <Form.Item name="sensitive" label="Sensitive Value">
            <Radio.Group
              options={[
                { label: "Yes", value: true },
                { label: "No", value: false },
              ]}
            />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <Input.TextArea rows={5} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}

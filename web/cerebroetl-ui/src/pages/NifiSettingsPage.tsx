import { DeleteOutlined, EditOutlined, PlusOutlined, SettingOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Checkbox, Form, Input, Modal, Popconfirm, Radio, Space, Table, Tag, message } from "antd";
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

  const contextRows = contexts ?? [];

  return (
    <div className="etl-setting-wrap">
      <div className="etl-setting-grid">
      <aside className="setting-side-card" aria-label="ETL 관리 설정 목록">
        {NIFI_SETTINGS_ITEMS.map((item) => (
          <button
            key={item.key}
            type="button"
            className={item.key === selectedKey ? "setting-nav-item active" : "setting-nav-item"}
            onClick={() => selectItem(item.key)}
          >
            <span className="setting-nav-icon" aria-hidden="true">
              {item.icon}
            </span>
            <span>{item.label}</span>
          </button>
        ))}
      </aside>

      <section className="setting-main-card" aria-label={selectedItem.label}>
        <div className="setting-card-header">
          <h2 className="setting-card-title">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" aria-hidden="true">
              <polyline points="16 18 22 12 16 6" />
              <polyline points="8 6 2 12 8 18" />
            </svg>
            {selectedItem.label}
          </h2>
          <div className="setting-header-actions">
            <button type="button" className="btn-setting-create" onClick={openCreateContextModal}>
              <PlusOutlined />
              신규 등록
            </button>
          </div>
        </div>

        <div className="setting-table-wrap">
          <table className="setting-data-table">
            <thead>
              <tr>
                <th style={{ width: "25%" }}>이름</th>
                <th style={{ width: "35%" }}>설명</th>
                <th style={{ width: "18%" }}>파라미터</th>
                <th style={{ width: "22%" }}>관리</th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                <tr>
                  <td colSpan={4} className="setting-empty-cell">불러오는 중입니다.</td>
                </tr>
              ) : contextRows.length === 0 ? (
                <tr>
                  <td colSpan={4} className="setting-empty-cell">등록된 전역변수가 없습니다.</td>
                </tr>
              ) : (
                contextRows.map((record) => (
                  <tr key={record.id}>
                    <td>
                      <span className="var-name-text">{record.component.name}</span>
                    </td>
                    <td>
                      <span className="var-desc-text">{record.component.description || "-"}</span>
                    </td>
                    <td>
                      <button type="button" className="param-count-badge" onClick={() => openEditContextModal(record)}>
                        {record.component.parameters?.length ?? 0}개
                      </button>
                    </td>
                    <td>
                      <div className="table-action-group">
                        <button type="button" className="btn-action-edit" onClick={() => openEditContextModal(record)}>
                          <EditOutlined /> 수정
                        </button>
                        <Popconfirm
                          title="이 파라미터 컨텍스트를 삭제할까요?"
                          description="NiFi에서도 함께 삭제됩니다."
                          onConfirm={() => deleteMutation.mutate(record.id)}
                        >
                          <button
                            type="button"
                            className="btn-action-delete"
                            disabled={deleteMutation.isPending && deleteMutation.variables === record.id}
                          >
                            <DeleteOutlined /> 삭제
                          </button>
                        </Popconfirm>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="setting-pagination-bar">
          <div className="pagination-total-info">전체 {contextRows.length}건</div>
          <div className="pagination-controls">
            <button type="button" className="btn-page-arrow" disabled title="이전 페이지">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                <polyline points="15 18 9 12 15 6" />
              </svg>
            </button>
            <span className="page-number-box">1</span>
            <button type="button" className="btn-page-arrow" disabled title="다음 페이지">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                <polyline points="9 18 15 12 9 6" />
              </svg>
            </button>
          </div>
          <select className="page-size-select" defaultValue="10" aria-label="페이지 크기">
            <option value="10">10 / page</option>
            <option value="20">20 / page</option>
            <option value="50">50 / page</option>
          </select>
        </div>
      </section>
      </div>

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

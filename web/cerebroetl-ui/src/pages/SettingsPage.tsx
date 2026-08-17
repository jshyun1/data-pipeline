import { useQueryClient, useQuery } from "@tanstack/react-query";
import { Button, Card, Form, Input, message, Space, Switch, Table, Tabs, Tag } from "antd";
import {
  createRecipient,
  deleteRecipient,
  getAlertRules,
  getChannels,
  getRecipients,
  testChannel,
  updateAlertRule,
  updateChannel,
  type AlertRule,
  type ChannelConfig,
  type Recipient,
} from "../api/config";

function RulesTab() {
  const qc = useQueryClient();
  const { data: rules = [], isLoading } = useQuery({ queryKey: ["alert-rules"], queryFn: getAlertRules });

  async function toggle(rule: AlertRule, enabled: boolean) {
    await updateAlertRule(rule.id, { enabled });
    message.success(`${rule.name} ${enabled ? "활성화" : "비활성화"}`);
    qc.invalidateQueries({ queryKey: ["alert-rules"] });
  }

  return (
    <Table<AlertRule>
      rowKey="id"
      loading={isLoading}
      dataSource={rules}
      pagination={false}
      columns={[
        { title: "규칙", dataIndex: "name" },
        { title: "유형", dataIndex: "type_label", width: 140 },
        {
          title: "심각도",
          dataIndex: "severity",
          width: 100,
          render: (s: string) => <Tag color={s === "CRITICAL" ? "error" : "warning"}>{s}</Tag>,
        },
        { title: "발화(초)", dataIndex: "for_seconds", width: 90 },
        { title: "해제(초)", dataIndex: "clear_seconds", width: 90 },
        {
          title: "사용",
          dataIndex: "enabled",
          width: 90,
          render: (enabled: boolean, r) => (
            <Switch checked={enabled} disabled={r.mandatory && enabled} onChange={(v) => toggle(r, v)} />
          ),
        },
      ]}
    />
  );
}

function ChannelsTab() {
  const qc = useQueryClient();
  const { data: channels = [], isLoading } = useQuery({ queryKey: ["channels"], queryFn: getChannels });

  async function toggle(ch: ChannelConfig, enabled: boolean) {
    await updateChannel(ch.channel_type, { enabled });
    message.success(`${ch.channel_type} ${enabled ? "켜짐" : "꺼짐"}`);
    qc.invalidateQueries({ queryKey: ["channels"] });
  }
  async function test(ch: ChannelConfig) {
    await testChannel(ch.channel_type);
    message.success(`${ch.channel_type} 테스트 발송 요청됨`);
  }

  return (
    <Table<ChannelConfig>
      rowKey="channel_type"
      loading={isLoading}
      dataSource={channels}
      pagination={false}
      columns={[
        { title: "채널", dataIndex: "channel_type", width: 120 },
        { title: "서킷", dataIndex: "circuit_state", width: 120 },
        {
          title: "최근 실패",
          dataIndex: "last_failure_reason",
          render: (v: string | null) => v ?? "-",
        },
        {
          title: "사용",
          dataIndex: "enabled",
          width: 90,
          render: (enabled: boolean, ch) => <Switch checked={enabled} onChange={(v) => toggle(ch, v)} />,
        },
        {
          title: "테스트",
          width: 90,
          render: (_: unknown, ch) => (
            <Button size="small" onClick={() => test(ch)}>
              발송
            </Button>
          ),
        },
      ]}
    />
  );
}

function RecipientsTab() {
  const qc = useQueryClient();
  const [form] = Form.useForm();
  const { data: recipients = [], isLoading } = useQuery({ queryKey: ["recipients"], queryFn: getRecipients });

  async function add(values: { displayName: string; email?: string; phone?: string }) {
    await createRecipient(values);
    message.success("수신자 추가됨");
    form.resetFields();
    qc.invalidateQueries({ queryKey: ["recipients"] });
  }
  async function remove(id: number) {
    await deleteRecipient(id);
    qc.invalidateQueries({ queryKey: ["recipients"] });
  }

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <Form form={form} layout="inline" onFinish={add}>
        <Form.Item name="displayName" rules={[{ required: true, message: "이름" }]}>
          <Input placeholder="이름" />
        </Form.Item>
        <Form.Item name="email">
          <Input placeholder="이메일" />
        </Form.Item>
        <Form.Item name="phone">
          <Input placeholder="전화번호" />
        </Form.Item>
        <Button type="primary" htmlType="submit">
          추가
        </Button>
      </Form>
      <Table<Recipient>
        rowKey="id"
        loading={isLoading}
        dataSource={recipients}
        pagination={false}
        columns={[
          { title: "이름", dataIndex: "display_name" },
          { title: "이메일", dataIndex: "email", render: (v: string | null) => v ?? "-" },
          { title: "전화", dataIndex: "phone_masked", render: (v: string | null) => v ?? "-" },
          {
            title: "",
            width: 80,
            render: (_: unknown, r) => (
              <Button size="small" danger onClick={() => remove(r.id)}>
                삭제
              </Button>
            ),
          },
        ]}
      />
    </Space>
  );
}

export function SettingsPage() {
  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>알림/발송 설정</h2>
      <Card>
        <Tabs
          items={[
            { key: "rules", label: "알림 규칙", children: <RulesTab /> },
            { key: "channels", label: "발송 채널", children: <ChannelsTab /> },
            { key: "recipients", label: "수신자", children: <RecipientsTab /> },
          ]}
        />
      </Card>
    </div>
  );
}

import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQueryClient, useQuery } from "@tanstack/react-query";
import { AlertHistoryTab } from "../components/AlertHistoryTab";
import {
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Switch,
  Table,
  Tabs,
  Tag,
  Tooltip,
  message,
} from "antd";
import {
  createAlertRule,
  createRecipient,
  deleteAlertRule,
  deleteRecipient,
  getAlertRuleTypes,
  getAlertRules,
  getChannels,
  getRecipients,
  testChannel,
  updateAlertRule,
  updateChannel,
  updateRecipient,
  type AlertRule,
  type AlertRuleType,
  type ChannelConfig,
  type Recipient,
  type RuleParamSpec,
} from "../api/config";

const SEVERITIES = [
  { label: "위험", value: "CRITICAL" },
  { label: "경고", value: "WARNING" },
  { label: "정보", value: "INFO" },
];

function severityTag(s: string) {
  const color = s === "CRITICAL" ? "error" : s === "WARNING" ? "warning" : "default";
  const label = SEVERITIES.find((x) => x.value === s)?.label ?? s;
  return <Tag color={color}>{label}</Tag>;
}

/** params_json 은 백엔드가 jsonb 로 내려서 object 로 온다. 문자열로 올 때도 방어한다. */
function readParams(raw: unknown): Record<string, number> {
  if (raw == null) return {};
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }
  return raw as Record<string, number>;
}

/**
 * 조건을 사람이 읽는 한 줄로 바꾼다 (PDF 9쪽 "조건" 열).
 * 지금까지 화면에 조건이 아예 안 보여서, 규칙이 무엇을 보고 있는지 알 수 없었다.
 */
function describeCondition(rule: AlertRule, spec: RuleParamSpec[]): string {
  const params = readParams(rule.params_json);
  if (spec.length === 0) {
    return "발생 시 즉시";
  }
  return spec
    .map((p) => {
      const v = params[p.key];
      return `${p.label} ${v ?? p.defaultValue}${p.unit}`;
    })
    .join(" · ");
}

function RulesTab() {
  const qc = useQueryClient();
  const { data: rules = [], isLoading } = useQuery({ queryKey: ["alert-rules"], queryFn: getAlertRules });
  const { data: types = [] } = useQuery({ queryKey: ["alert-rule-types"], queryFn: getAlertRuleTypes });
  const [editing, setEditing] = useState<AlertRule | null>(null);
  const [creating, setCreating] = useState(false);

  const specByCode = useMemo(() => {
    const m = new Map<string, RuleParamSpec[]>();
    types.forEach((t) => m.set(t.code, t.paramSpec ?? []));
    return m;
  }, [types]);

  async function toggle(rule: AlertRule, enabled: boolean) {
    await updateAlertRule(rule.id, { enabled });
    message.success(`${rule.name} ${enabled ? "활성화" : "비활성화"}`);
    qc.invalidateQueries({ queryKey: ["alert-rules"] });
  }

  async function remove(rule: AlertRule) {
    try {
      await deleteAlertRule(rule.id);
      message.success("규칙 삭제됨");
      qc.invalidateQueries({ queryKey: ["alert-rules"] });
    } catch {
      message.error("규칙 삭제에 실패했습니다.");
    }
  }

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <Button type="primary" onClick={() => setCreating(true)}>
          + 규칙 추가
        </Button>
      </div>
      <Table<AlertRule>
        rowKey="id"
        loading={isLoading}
        dataSource={rules}
        pagination={false}
        size="middle"
        columns={[
          { title: "규칙명", dataIndex: "name", width: 190 },
          { title: "유형", dataIndex: "type_label", width: 140 },
          {
            title: "조건",
            width: 240,
            render: (_: unknown, r) => (
              <span>{describeCondition(r, specByCode.get(r.rule_type_code) ?? [])}</span>
            ),
          },
          { title: "심각도", dataIndex: "severity", width: 90, render: severityTag },
          {
            title: "지속",
            width: 130,
            render: (_: unknown, r) => (
              <Tooltip title="발화: 조건이 이만큼 이어져야 알림 / 해제: 조건이 풀린 뒤 이만큼 지나야 종료">
                <span>
                  발화 {r.for_seconds}s · 해제 {r.clear_seconds}s
                </span>
              </Tooltip>
            ),
          },
          {
            title: "사용",
            dataIndex: "enabled",
            width: 80,
            // 필수 규칙도 운영자가 끌 수 있게 한다(환경마다 감시 대상이 다르다).
            render: (enabled: boolean, r) => <Switch checked={enabled} onChange={(v) => toggle(r, v)} />,
          },
          {
            title: "",
            width: 120,
            render: (_: unknown, r) => (
              <Space size="small">
                <Button size="small" onClick={() => setEditing(r)}>
                  수정
                </Button>
                <Button size="small" danger onClick={() => remove(r)}>
                  삭제
                </Button>
              </Space>
            ),
          },
        ]}
      />
      {rules.some((r) => r.last_eval_error) ? (
        <div style={{ color: "#b45309", fontSize: 12 }}>
          ⚠ 일부 규칙에서 평가 오류가 있습니다 — 자가진단 화면에서 확인하세요.
        </div>
      ) : null}

      <RuleFormModal
        open={creating}
        types={types}
        onClose={() => setCreating(false)}
        onDone={() => {
          setCreating(false);
          qc.invalidateQueries({ queryKey: ["alert-rules"] });
        }}
      />
      <RuleFormModal
        open={editing != null}
        rule={editing ?? undefined}
        types={types}
        onClose={() => setEditing(null)}
        onDone={() => {
          setEditing(null);
          qc.invalidateQueries({ queryKey: ["alert-rules"] });
        }}
      />
    </Space>
  );
}

/**
 * 규칙 추가/수정 폼. 조건 입력란은 서버가 내려준 paramSpec 으로 그린다 —
 * 화면에 조건 스키마를 하드코딩하면 규칙 유형이 늘 때마다 두 곳이 어긋난다.
 */
function RuleFormModal({
  open,
  rule,
  types,
  onClose,
  onDone,
}: {
  open: boolean;
  rule?: AlertRule;
  types: AlertRuleType[];
  onClose: () => void;
  onDone: () => void;
}) {
  const [form] = Form.useForm();
  const isEdit = rule != null;
  const [typeCode, setTypeCode] = useState<string | undefined>(rule?.rule_type_code);

  const activeCode = isEdit ? rule!.rule_type_code : typeCode;
  const spec = types.find((t) => t.code === activeCode)?.paramSpec ?? [];

  // 모달이 열릴 때마다 대상 규칙 값으로 초기화한다.
  const initial = useMemo(() => {
    if (!rule) return { severity: undefined, forSeconds: 120, clearSeconds: 300 };
    const params = readParams(rule.params_json);
    return {
      name: rule.name,
      severity: rule.severity,
      forSeconds: rule.for_seconds,
      clearSeconds: rule.clear_seconds,
      ...Object.fromEntries(spec.map((p) => [`param_${p.key}`, params[p.key] ?? p.defaultValue])),
    };
  }, [rule, spec]);

  async function submit() {
    const v = await form.validateFields();
    const params: Record<string, number> = {};
    spec.forEach((p) => {
      const val = v[`param_${p.key}`];
      if (val != null) params[p.key] = Number(val);
    });
    const payload = {
      name: v.name as string,
      severity: v.severity as string,
      paramsJson: JSON.stringify(params),
      forSeconds: v.forSeconds as number,
      clearSeconds: v.clearSeconds as number,
    };
    try {
      if (isEdit) {
        await updateAlertRule(rule!.id, payload);
        message.success("규칙 수정됨");
      } else {
        await createAlertRule({ ruleTypeCode: activeCode!, ...payload });
        message.success("규칙 추가됨");
      }
      form.resetFields();
      onDone();
    } catch {
      message.error("저장에 실패했습니다. 입력값을 확인하세요.");
    }
  }

  return (
    <Modal
      open={open}
      title={isEdit ? "알림 규칙 수정" : "알림 규칙 추가"}
      onCancel={() => {
        form.resetFields();
        onClose();
      }}
      onOk={submit}
      okText="저장"
      cancelText="취소"
      destroyOnClose
    >
      <Form form={form} layout="vertical" initialValues={initial} preserve={false}>
        {isEdit ? null : (
          <Form.Item
            label="규칙 유형"
            name="ruleTypeCode"
            rules={[{ required: true, message: "유형을 선택하세요" }]}
          >
            <Select
              placeholder="무엇을 감시할지 고릅니다"
              options={types.map((t) => ({ label: `${t.label} (${t.category})`, value: t.code }))}
              onChange={(v) => setTypeCode(v)}
            />
          </Form.Item>
        )}
        <Form.Item label="규칙명" name="name" rules={[{ required: true, message: "규칙명을 입력하세요" }]}>
          <Input placeholder="예: 메모리 위험(90%)" />
        </Form.Item>
        <Form.Item label="심각도" name="severity" rules={[{ required: true, message: "심각도를 고르세요" }]}>
          <Select options={SEVERITIES} />
        </Form.Item>

        {spec.length > 0 ? (
          <div style={{ padding: "8px 12px", background: "#f8fafc", borderRadius: 6, marginBottom: 12 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#64748b", marginBottom: 6 }}>조건</div>
            {spec.map((p) => (
              <Form.Item
                key={p.key}
                label={`${p.label} (${p.unit})`}
                name={`param_${p.key}`}
                rules={[{ required: true, message: `${p.label}을(를) 입력하세요` }]}
                style={{ marginBottom: 8 }}
              >
                <InputNumber min={p.min} max={p.max} style={{ width: "100%" }} />
              </Form.Item>
            ))}
          </div>
        ) : activeCode ? (
          <div style={{ padding: "8px 12px", background: "#f8fafc", borderRadius: 6, marginBottom: 12,
                        fontSize: 12, color: "#64748b" }}>
            이 유형은 입력할 조건 값이 없습니다 — 상태가 감지되면 바로 발화합니다.
          </div>
        ) : null}

        <Space size="middle">
          <Form.Item label="발화 지속(초)" name="forSeconds" tooltip="조건이 이만큼 이어져야 알림을 냅니다">
            <InputNumber min={0} max={86400} />
          </Form.Item>
          <Form.Item label="해제 지속(초)" name="clearSeconds" tooltip="조건이 풀린 뒤 이만큼 지나야 종료합니다">
            <InputNumber min={0} max={86400} />
          </Form.Item>
        </Space>
      </Form>
    </Modal>
  );
}

// IN_APP 은 세레브로 조치대기열/화면 상단에 뜨는 알림이라 사용자 표기는 «화면알림».
const CHANNEL_LABEL: Record<string, string> = {
  IN_APP: "화면알림",
  EMAIL: "이메일",
  SMS: "SMS(문자)",
};
function channelLabel(type: string): string {
  return CHANNEL_LABEL[type] ?? type;
}

function ChannelsTab() {
  const qc = useQueryClient();
  const { data: channels = [], isLoading } = useQuery({ queryKey: ["channels"], queryFn: getChannels });

  async function toggle(ch: ChannelConfig, enabled: boolean) {
    await updateChannel(ch.channel_type, { enabled });
    message.success(`${channelLabel(ch.channel_type)} ${enabled ? "켜짐" : "꺼짐"}`);
    qc.invalidateQueries({ queryKey: ["channels"] });
  }
  async function test(ch: ChannelConfig) {
    await testChannel(ch.channel_type);
    message.success(`${channelLabel(ch.channel_type)} 테스트 발송 요청됨`);
  }

  return (
    <Table<ChannelConfig>
      rowKey="channel_type"
      loading={isLoading}
      dataSource={channels}
      pagination={false}
      columns={[
        { title: "채널", dataIndex: "channel_type", width: 120, render: (v: string) => channelLabel(v) },
        { title: "서킷", dataIndex: "circuit_state", width: 120 },
        { title: "최근 실패", dataIndex: "last_failure_reason", render: (v: string | null) => v ?? "-" },
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
  const [editing, setEditing] = useState<Recipient | null>(null);
  const { data: recipients = [], isLoading } = useQuery({ queryKey: ["recipients"], queryFn: getRecipients });

  async function add(values: { displayName: string; email?: string; phone?: string }) {
    try {
      await createRecipient(values);
      message.success("수신자 추가됨");
      form.resetFields();
      qc.invalidateQueries({ queryKey: ["recipients"] });
    } catch {
      message.error("이메일 또는 전화번호 중 하나는 필요합니다.");
    }
  }
  async function remove(id: number) {
    await deleteRecipient(id);
    qc.invalidateQueries({ queryKey: ["recipients"] });
  }
  async function toggleEnabled(r: Recipient, enabled: boolean) {
    await updateRecipient(r.id, { enabled });
    message.success(`${r.display_name} ${enabled ? "사용" : "미사용"}`);
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
          { title: "이름", dataIndex: "display_name", width: 140 },
          { title: "이메일", dataIndex: "email", render: (v: string | null) => v ?? "-" },
          // 마스킹하지 않는다 - 가려진 번호로는 맞는지 확인할 수도, 고칠 수도 없다.
          { title: "전화", dataIndex: "phone", width: 160, render: (v: string | null) => v ?? "-" },
          {
            title: "사용여부",
            dataIndex: "enabled",
            width: 90,
            render: (enabled: boolean, r) => (
              <Switch checked={enabled} onChange={(v) => toggleEnabled(r, v)} />
            ),
          },
          {
            title: "",
            width: 130,
            render: (_: unknown, r) => (
              <Space size="small">
                <Button size="small" onClick={() => setEditing(r)}>
                  수정
                </Button>
                <Button size="small" danger onClick={() => remove(r.id)}>
                  삭제
                </Button>
              </Space>
            ),
          },
        ]}
      />
      <RecipientEditModal
        recipient={editing}
        onClose={() => setEditing(null)}
        onDone={() => {
          setEditing(null);
          qc.invalidateQueries({ queryKey: ["recipients"] });
        }}
      />
    </Space>
  );
}

/**
 * 수신자 수정. 지금까지 등록/삭제만 있어서 오타 하나를 고치려면 지웠다 다시 넣어야 했고,
 * 그때마다 id 가 바뀌어 구독 설정이 끊겼다.
 */
function RecipientEditModal({
  recipient,
  onClose,
  onDone,
}: {
  recipient: Recipient | null;
  onClose: () => void;
  onDone: () => void;
}) {
  const [form] = Form.useForm();

  async function submit() {
    const v = await form.validateFields();
    try {
      await updateRecipient(recipient!.id, {
        displayName: v.displayName,
        email: v.email ?? "",
        phone: v.phone ?? "",
      });
      message.success("수신자 수정됨");
      onDone();
    } catch {
      message.error("이메일 또는 전화번호 중 하나는 남아 있어야 합니다.");
    }
  }

  return (
    <Modal
      open={recipient != null}
      title="수신자 수정"
      onCancel={onClose}
      onOk={submit}
      okText="저장"
      cancelText="취소"
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        preserve={false}
        initialValues={{
          displayName: recipient?.display_name,
          email: recipient?.email ?? "",
          phone: recipient?.phone ?? "",
        }}
      >
        <Form.Item label="이름" name="displayName" rules={[{ required: true, message: "이름은 필수입니다" }]}>
          <Input />
        </Form.Item>
        <Form.Item label="이메일" name="email" tooltip="비워두면 이메일 수신을 지웁니다">
          <Input placeholder="hong@example.com" />
        </Form.Item>
        <Form.Item label="전화번호" name="phone" tooltip="비워두면 문자 수신을 지웁니다">
          <Input placeholder="01012345678" />
        </Form.Item>
      </Form>
    </Modal>
  );
}

export function SettingsPage() {
  const [params, setParams] = useSearchParams();
  const active = params.get("tab") ?? "history";
  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>알림/발송 설정</h2>
      <Card>
        <Tabs
          activeKey={active}
          onChange={(k) => setParams({ tab: k })}
          items={[
            { key: "history", label: "알림 이력", children: <AlertHistoryTab /> },
            { key: "rules", label: "알림 규칙", children: <RulesTab /> },
            { key: "channels", label: "발송 채널", children: <ChannelsTab /> },
            { key: "recipients", label: "수신자", children: <RecipientsTab /> },
          ]}
        />
      </Card>
    </div>
  );
}

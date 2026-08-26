import { useEffect, useMemo, useRef, useState } from "react";
import dayjs from "dayjs";
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
  TimePicker,
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
  getRuleRecipients,
  getScopeTargets,
  replaceRuleRecipients,
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

/** 반복 점검 표기("5분×3회"). 1회뿐이면 빈 문자열. */
function scheduleRepeat(r: AlertRule): string {
  const gap = Math.round((r.renotify_seconds ?? 0) / 60);
  // params_json 은 서버가 text 로 내리므로 반드시 readParams 를 거친다.
  const times = Number(readParams(r.params_json).notify_max ?? 1);
  return gap > 0 && times > 1 ? `${gap}분×${times}회` : "";
}

function scheduleHint(r: AlertRule): string {
  const rep = scheduleRepeat(r);
  return rep
    ? `매일 ${hhmm(r.schedule_time)}부터 ${rep} 점검해서, 그때 실패건이 있으면 발송합니다`
    : `매일 ${hhmm(r.schedule_time)}에 한 번 점검해서 알립니다`;
}

/** 서버가 내려주는 "HH:mm:ss" 를 화면 표기용 "HH:mm" 으로. */
function hhmm(t: string | null): string {
  return t ? t.slice(0, 5) : "-";
}

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
  const [recipientsOf, setRecipientsOf] = useState<AlertRule | null>(null);
  const [creating, setCreating] = useState(false);
  // 규칙은 대부분 꺼 둔 채로 두고 쓰는 것만 켜서 운영한다. 기본을 «사용»으로 두면
  // 화면을 열자마자 지금 실제로 도는 규칙만 보인다(전체/미사용은 골라서 본다).
  const [enabledFilter, setEnabledFilter] = useState<"all" | "on" | "off">("on");

  const specByCode = useMemo(() => {
    const m = new Map<string, RuleParamSpec[]>();
    types.forEach((t) => m.set(t.code, t.paramSpec ?? []));
    return m;
  }, [types]);

  const filtered = useMemo(
    () => rules.filter((r) => enabledFilter === "all" || (enabledFilter === "on" ? r.enabled : !r.enabled)),
    [rules, enabledFilter],
  );

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
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span>
          사용여부{" "}
          <Select
            size="small"
            value={enabledFilter}
            style={{ width: 110 }}
            onChange={setEnabledFilter}
            options={[
              { label: "전체", value: "all" },
              { label: "사용", value: "on" },
              { label: "미사용", value: "off" },
            ]}
          />
        </span>
        <Button type="primary" onClick={() => setCreating(true)}>
          + 규칙 추가
        </Button>
      </div>
      <Table<AlertRule>
        rowKey="id"
        loading={isLoading}
        dataSource={filtered}
        pagination={{ pageSize: 10, showSizeChanger: true, pageSizeOptions: [10, 20, 50], showTotal: (t) => `총 ${t}건` }}
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
            title: "점검",
            width: 120,
            render: (_: unknown, r) =>
              r.schedule_enabled ? (
                <Tooltip title={scheduleHint(r)}>
                  <Tag color="blue">
                    매일 {hhmm(r.schedule_time)}
                    {scheduleRepeat(r) ? ` +${scheduleRepeat(r)}` : ""}
                  </Tag>
                </Tooltip>
              ) : (
                <Tooltip title="20초 주기 평가 루프에서 상시 판정합니다">
                  <span style={{ color: "#94a3b8" }}>상시</span>
                </Tooltip>
              ),
          },
          {
            title: "지속",
            width: 130,
            render: (_: unknown, r) => (
              r.schedule_enabled ? (
                // 하루 한 번 보는 규칙은 지속시간이 찰 수가 없어 그 시각 상태로 즉시 판정한다.
                <Tooltip title="일별 점검 규칙은 점검 시각의 상태로 즉시 판정합니다">
                  <span style={{ color: "#94a3b8" }}>즉시 판정</span>
                </Tooltip>
              ) : (
                <Tooltip title="발화: 조건이 이만큼 이어져야 알림 / 해제: 조건이 풀린 뒤 이만큼 지나야 종료">
                  <span>
                    발화 {r.for_seconds}s · 해제 {r.clear_seconds}s
                  </span>
                </Tooltip>
              )
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
            width: 180,
            render: (_: unknown, r) => (
              <Space size="small">
                <Button size="small" onClick={() => setRecipientsOf(r)}>
                  수신자
                </Button>
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

      <RuleRecipientsModal rule={recipientsOf} onClose={() => setRecipientsOf(null)} />

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

// 감시 범위(scope)를 고를 수 있는 규칙 유형과, 대상 목록의 종류.
// ETL_CHAIN = 적재 테이블 하나(그 앞의 trigger/extract/truncate 까지 한 묶음),
// ETL = NiFi 프로세스 그룹, CDC = 파이프라인.
// "ETL Job 실패"만 체인 단위다 — 그룹 단위로는 "COM001M 적재만 감시"가 불가능했다.
type ScopeCategory = "ETL_CHAIN" | "ETL" | "CDC";
const SCOPE_TYPES: Record<string, ScopeCategory> = {
  JOB_FAILURE: "ETL_CHAIN",
  JOB_CONSECUTIVE_FAILURE: "ETL",
  JOB_NOT_RUN: "ETL",
  CDC_LAG: "CDC",
  CONNECTOR_FAILED: "CDC",
};

type ScopeKind = "ALL" | "INCLUDE" | "EXCLUDE";
// ids 가 무엇의 id 인지. CHAIN=etl_job_step(적재), JOB=etl_job/pipeline_definition.
type ScopeIdKind = "CHAIN" | "JOB";
function parseScopeJson(s: string | null | undefined): { mode: ScopeKind; ids: number[]; idKind: ScopeIdKind } {
  if (!s) return { mode: "ALL", ids: [], idKind: "JOB" };
  try {
    const o = JSON.parse(s) as { kind?: ScopeKind; ids?: number[]; idKind?: ScopeIdKind };
    return {
      mode: o.kind ?? "ALL",
      ids: Array.isArray(o.ids) ? o.ids : [],
      idKind: o.idKind === "CHAIN" ? "CHAIN" : "JOB",
    };
  } catch {
    return { mode: "ALL", ids: [], idKind: "JOB" };
  }
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
  const scopeCategory = activeCode ? SCOPE_TYPES[activeCode] : undefined;
  const scopeMode = Form.useWatch("scopeMode", form) as ScopeKind | undefined;
  const scheduleEnabled = Form.useWatch("scheduleEnabled", form) as boolean | undefined;
  const scheduleTime = Form.useWatch("scheduleTime", form) as ReturnType<typeof dayjs> | null | undefined;
  const renotifyMinutes = Form.useWatch("renotifyMinutes", form) as number | undefined;
  const notifyMax = Form.useWatch("notifyMax", form) as number | undefined;
  // 입력한 값이 실제로 몇 시에 몇 번 도는지 그대로 보여준다 - 간격·횟수는 상시 규칙과
  // 뜻이 달라서(발송 반복이 아니라 «점검» 반복) 말로만 두면 오해하기 쉽다.
  const scheduleSummary = useMemo(() => {
    if (!scheduleEnabled || !scheduleTime) return "";
    const gap = Number(renotifyMinutes ?? 0);
    const times = Math.max(1, Number(notifyMax ?? 1));
    if (gap <= 0 || times <= 1) return `점검: 매일 ${scheduleTime.format("HH:mm")} 1회`;
    const at = Array.from({ length: Math.min(times, 6) }, (_, i) =>
      scheduleTime.add(gap * i, "minute").format("HH:mm"),
    );
    return `점검: 매일 ${at.join(" · ")}${times > 6 ? ` … 총 ${times}회` : ""}`;
  }, [scheduleEnabled, scheduleTime, renotifyMinutes, notifyMax]);
  const { data: scopeTargets = [] } = useQuery({
    queryKey: ["scope-targets", scopeCategory],
    queryFn: () => getScopeTargets(scopeCategory!),
    enabled: !!scopeCategory,
  });

  // 모달이 열릴 때마다 대상 규칙 값으로 초기화한다.
  const initial = useMemo(() => {
    if (!rule) {
      return {
        severity: undefined, forSeconds: 120, clearSeconds: 300,
        renotifyMinutes: 30, notifyMax: 1, scopeMode: "ALL" as ScopeKind, scopeIds: [] as number[],
        scheduleEnabled: false, scheduleTime: null as ReturnType<typeof dayjs> | null,
      };
    }
    const params = readParams(rule.params_json);
    const scope = parseScopeJson(rule.scope_json);
    // 그룹 단위로 저장돼 있던 규칙을 체인 단위 화면에서 열면 id 의 의미가 달라진다.
    // 숫자만 그대로 두면 전혀 다른 테이블을 감시하게 되므로 전체 감시로 되돌리고 다시 고르게 한다.
    const staleIds = scope.mode !== "ALL"
        && scope.idKind !== (scopeCategory === "ETL_CHAIN" ? "CHAIN" : "JOB");
    return {
      name: rule.name,
      severity: rule.severity,
      forSeconds: rule.for_seconds,
      clearSeconds: rule.clear_seconds,
      renotifyMinutes: Math.round((rule.renotify_seconds ?? 1800) / 60),
      notifyMax: params.notify_max ?? 1,
      scheduleEnabled: rule.schedule_enabled,
      // 서버는 "HH:mm:ss" 로 내려주고 TimePicker 는 dayjs 를 받는다. 포맷 문자열 파싱은
      // customParseFormat 플러그인이 있어야 하므로, 플러그인 없이도 되는 ISO 로 붙여 넣는다.
      scheduleTime: rule.schedule_time ? dayjs(`1970-01-01T${rule.schedule_time}`) : null,
      scopeMode: staleIds ? ("ALL" as ScopeKind) : scope.mode,
      scopeIds: staleIds ? [] : scope.ids,
      ...Object.fromEntries(spec.map((p) => [`param_${p.key}`, params[p.key] ?? p.defaultValue])),
    };
  }, [rule, spec, scopeCategory]);

  // 열 때마다 대상 규칙 값으로 폼을 다시 채운다.
  //
  // Form 의 initialValues 는 «마운트 시점»에만 적용되는데, 이 컴포넌트는 부모에 상주하고
  // open 만 토글되며 form 인스턴스도 계속 살아 있다. destroyOnHidden 으로 폼을 다시
  // 마운트해도 규칙명(Input)처럼 값이 남는 필드가 있어, 다른 규칙을 열었는데 직전 규칙명이
  // 그대로 보였다. 초깃값에 기대지 않고 명시적으로 세팅한다.
  //
  // 같은 규칙을 여는 동안 spec/scopeCategory 가 바뀌며 initial 의 identity 가 자주 바뀌므로,
  // «열림 1회 + 대상 1개»당 한 번만 적용한다. 그렇지 않으면 사용자가 입력하던 값을 덮는다.
  const appliedFor = useRef<number | string | null>(null);
  useEffect(() => {
    if (!open) {
      appliedFor.current = null;
      return;
    }
    const target = rule?.id ?? "new";
    if (appliedFor.current === target) {
      return;
    }
    appliedFor.current = target;
    setTypeCode(rule?.rule_type_code);
    form.resetFields();
    form.setFieldsValue(initial);
  }, [open, rule, initial, form]);

  async function submit() {
    const v = await form.validateFields();
    const params: Record<string, number> = {};
    spec.forEach((p) => {
      const val = v[`param_${p.key}`];
      if (val != null) params[p.key] = Number(val);
    });
    params.notify_max = Number(v.notifyMax ?? 1);
    const payload: {
      name: string;
      severity: string;
      paramsJson: string;
      forSeconds: number;
      clearSeconds: number;
      renotifySeconds: number;
      scopeJson?: string;
      scheduleEnabled?: boolean;
      scheduleTime?: string | null;
    } = {
      name: v.name as string,
      severity: v.severity as string,
      paramsJson: JSON.stringify(params),
      forSeconds: v.forSeconds as number,
      clearSeconds: v.clearSeconds as number,
      renotifySeconds: Number(v.renotifyMinutes ?? 30) * 60,
    };
    // 스케줄 on/off. 켜면 시각을 "HH:mm" 로 보낸다(서버가 time 으로 캐스팅).
    const on = Boolean(v.scheduleEnabled);
    payload.scheduleEnabled = on;
    payload.scheduleTime = on && v.scheduleTime ? (v.scheduleTime as ReturnType<typeof dayjs>).format("HH:mm") : null;
    if (scopeCategory) {
      const mode = (v.scopeMode ?? "ALL") as ScopeKind;
      payload.scopeJson = JSON.stringify({
        kind: mode,
        ids: mode === "ALL" ? [] : ((v.scopeIds as number[]) ?? []),
        idKind: scopeCategory === "ETL_CHAIN" ? "CHAIN" : "JOB",
      });
    }
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
      destroyOnHidden
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

        <div style={{ padding: "8px 12px", background: "#f8fafc", borderRadius: 6, marginBottom: 12 }}>
          <Space align="center" size="middle">
            <Form.Item
              label="점검 스케줄"
              name="scheduleEnabled"
              valuePropName="checked"
              style={{ marginBottom: 0 }}
              tooltip="끄면 지금처럼 상시(20초 주기) 판정합니다. 켜면 지정한 시각에 하루 한 번만 점검합니다"
            >
              <Switch checkedChildren="일별" unCheckedChildren="상시" />
            </Form.Item>
            {scheduleEnabled ? (
              <Form.Item
                label="점검 시각"
                name="scheduleTime"
                style={{ marginBottom: 0 }}
                rules={[{ required: true, message: "점검 시각을 고르세요" }]}
              >
                <TimePicker format="HH:mm" minuteStep={5} needConfirm={false} />
              </Form.Item>
            ) : null}
          </Space>
          {scheduleEnabled ? (
            <div style={{ fontSize: 12, color: "#64748b", marginTop: 8 }}>
              그 시각의 «현재 상태»를 점검합니다 — ETL Job 실패라면 각 Job의 마지막 실행 결과가
              실패인 것을 찾아 대상별로 알립니다. 아래 간격·횟수를 주면 그 간격마다 다시 점검해
              그때도 실패면 또 보냅니다(그 사이 복구된 대상은 빠집니다). 지속시간(발화/해제)은
              적용되지 않습니다.
            </div>
          ) : null}
        </div>

        {!scheduleEnabled ? (
          <Space size="middle">
            <Form.Item label="발화 지속(초)" name="forSeconds" tooltip="조건이 이만큼 이어져야 알림을 냅니다">
              <InputNumber min={0} max={86400} />
            </Form.Item>
            <Form.Item label="해제 지속(초)" name="clearSeconds" tooltip="조건이 풀린 뒤 이만큼 지나야 종료합니다">
              <InputNumber min={0} max={86400} />
            </Form.Item>
          </Space>
        ) : null}

        <Space size="middle">
          <Form.Item
            label="재발송 간격(분)"
            name="renotifyMinutes"
            tooltip={
              scheduleEnabled
                ? "점검 시각부터 이 간격마다 다시 점검해서 발송합니다(0=1회만 점검)"
                : "진행 중·미확인이면 이 간격마다 다시 발송합니다(0=재발송 안 함)"
            }
          >
            <InputNumber min={0} max={1440} />
          </Form.Item>
          <Form.Item
            label="최대 발송 횟수"
            name="notifyMax"
            tooltip={
              scheduleEnabled
                ? "첫 점검을 포함해 하루에 몇 번 점검할지(예: 03:00·03:05·03:10 이면 3회)"
                : "처음 1회를 포함해 총 몇 번까지 보낼지(예: 5회)"
            }
          >
            <InputNumber min={1} max={50} />
          </Form.Item>
        </Space>
        {scheduleEnabled ? (
          <div style={{ fontSize: 12, color: "#64748b", marginTop: -8, marginBottom: 12 }}>
            {scheduleSummary}
          </div>
        ) : null}

        {scopeCategory ? (
          <div style={{ padding: "8px 12px", background: "#f8fafc", borderRadius: 6, marginTop: 4 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#64748b", marginBottom: 6 }}>
              감시 범위 (
              {scopeCategory === "ETL_CHAIN" ? "적재 Job" : scopeCategory === "ETL" ? "ETL 그룹" : "CDC 파이프라인"})
            </div>
            <Form.Item name="scopeMode" style={{ marginBottom: 8 }}>
              <Select
                options={[
                  { label: "전체 감시", value: "ALL" },
                  { label: "선택한 것만 감시(포함)", value: "INCLUDE" },
                  { label: "선택한 것 제외", value: "EXCLUDE" },
                ]}
              />
            </Form.Item>
            {scopeMode && scopeMode !== "ALL" ? (
              <Form.Item name="scopeIds" style={{ marginBottom: 0 }}>
                <Select
                  mode="multiple"
                  allowClear
                  showSearch
                  optionFilterProp="label"
                  placeholder={
                    scopeCategory === "CDC" ? "감시/제외할 파이프라인 선택" : "감시/제외할 Job 선택"
                  }
                  options={scopeTargets.map((t) => ({ label: t.name, value: t.id }))}
                />
              </Form.Item>
            ) : null}
          </div>
        ) : null}
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

  // 화면알림(IN_APP)은 채널로 관리하지 않는다 — 알림 규칙을 «사용»으로 켜면 자동으로 화면에 뜬다.
  const external = channels.filter((c) => c.channel_type !== "IN_APP");
  return (
    <Space direction="vertical" style={{ width: "100%" }} size="small">
      <span style={{ color: "#888", fontSize: 13 }}>
        화면알림은 별도 설정이 없습니다 — 알림 규칙을 «사용»으로 켜면 조치 대기열·헤더 알림에 자동으로 표시됩니다.
        외부 발송(이메일·SMS)만 여기서 켜고 끕니다.
      </span>
      <Table<ChannelConfig>
        rowKey="channel_type"
        loading={isLoading}
        dataSource={external}
        pagination={false}
        columns={[
          { title: "채널", dataIndex: "channel_type", width: 160, render: (v: string) => channelLabel(v) },
        {
          title: "상태",
          key: "status",
          dataIndex: "enabled",
          width: 120,
          render: (enabled: boolean) =>
            enabled ? <Tag color="success">정상</Tag> : <Tag>중지</Tag>,
        },
        {
          title: "사용",
          dataIndex: "enabled",
          key: "enabled-toggle",
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
    </Space>
  );
}

function RecipientsTab() {
  const qc = useQueryClient();
  const [form] = Form.useForm();
  const [editing, setEditing] = useState<Recipient | null>(null);
  const [enabledFilter, setEnabledFilter] = useState<"all" | "on" | "off">("all");
  const { data: recipients = [], isLoading } = useQuery({ queryKey: ["recipients"], queryFn: getRecipients });
  const filtered = useMemo(
    () => recipients.filter((r) => enabledFilter === "all" || (enabledFilter === "on" ? r.enabled : !r.enabled)),
    [recipients, enabledFilter],
  );

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
      <div>
        사용여부{" "}
        <Select
          size="small"
          value={enabledFilter}
          style={{ width: 110 }}
          onChange={setEnabledFilter}
          options={[
            { label: "전체", value: "all" },
            { label: "사용", value: "on" },
            { label: "미사용", value: "off" },
          ]}
        />
      </div>
      <Table<Recipient>
        rowKey="id"
        loading={isLoading}
        dataSource={filtered}
        pagination={{ pageSize: 10, showSizeChanger: true, pageSizeOptions: [10, 20, 50], showTotal: (t) => `총 ${t}건` }}
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
 * 규칙별 «수신자» 설정. 알림 규칙 화면의 [수신자] 버튼이 연다.
 *
 * <p>규칙 하나(예: "ETL Job 실패")를 누가 받을지 켬/끔으로만 고른다. 어떤 Job 을 감시할지는
 * 규칙의 «감시 범위»가 정하므로, 여기서 Job 을 또 고르게 하지 않는다.
 *
 * <p>한 번도 손대지 않은 규칙은 전원이 받는다(열면 전부 켜져 있다). 끈 사람도 «껐다»는 사실을
 * 저장하므로, 전원을 꺼 두면 아무에게도 가지 않는다.
 */
function RuleRecipientsModal({ rule, onClose }: { rule: AlertRule | null; onClose: () => void }) {
  const qc = useQueryClient();
  const open = rule != null;
  const [picked, setPicked] = useState<Record<number, boolean>>({});

  const { data: rows } = useQuery({
    queryKey: ["rule-recipients", rule?.id],
    queryFn: () => getRuleRecipients(rule!.id),
    enabled: open,
  });

  useEffect(() => {
    if (!open || rows == null) {
      return;
    }
    setPicked(Object.fromEntries(rows.map((r) => [r.recipient_id, r.enabled])));
  }, [open, rows]);

  async function save() {
    await replaceRuleRecipients(
      rule!.id,
      (rows ?? []).map((r) => ({
        recipientId: r.recipient_id,
        enabled: picked[r.recipient_id] ?? true,
      })),
    );
    message.success("수신자 설정 저장됨");
    qc.invalidateQueries({ queryKey: ["rule-recipients", rule!.id] });
    onClose();
  }

  const onCount = Object.values(picked).filter(Boolean).length;

  return (
    <Modal
      open={open}
      title={`수신자 — ${rule?.name ?? ""}`}
      onCancel={onClose}
      onOk={save}
      okText="저장"
      cancelText="취소"
      width={560}
      destroyOnHidden
    >
      <div style={{ fontSize: 12, color: onCount === 0 ? "#b45309" : "#64748b", marginBottom: 10 }}>
        {onCount === 0
          ? "⚠ 켜진 수신자가 없습니다 — 저장하면 이 규칙의 알림은 아무에게도 발송되지 않습니다."
          : `이 규칙의 알림을 받을 사람을 고릅니다 (현재 ${onCount}명).`}
      </div>
      <div style={{ maxHeight: 440, overflowY: "auto" }}>
        {(rows ?? []).map((r) => (
          <div
            key={r.recipient_id}
            style={{ padding: "8px 10px", borderBottom: "1px solid #f1f5f9", display: "flex", gap: 10, alignItems: "center" }}
          >
            <Switch
              size="small"
              checked={picked[r.recipient_id] ?? true}
              onChange={(v) => setPicked((prev) => ({ ...prev, [r.recipient_id]: v }))}
            />
            <div style={{ fontWeight: 600 }}>
              {r.display_name}{" "}
              <span style={{ color: "#94a3b8", fontWeight: 400, fontSize: 12 }}>
                {r.email ?? ""} {r.phone ?? ""}
              </span>
              {/* 수신자 자체를 꺼 두면 규칙에서 켜도 발송되지 않는다 - 헷갈리지 않게 표시한다. */}
              {!r.recipient_enabled ? (
                <Tag color="default" style={{ marginLeft: 6 }}>
                  수신자 미사용
                </Tag>
              ) : null}
            </div>
          </div>
        ))}
      </div>
    </Modal>
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
      destroyOnHidden
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

import { useEffect, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Card, Checkbox, Form, Input, InputNumber, Radio, Select, Space, Spin, message } from "antd";
import {
  BILLING_LABEL,
  DEFAULT_MAX_INPUT_CHARS,
  DEPLOYMENT_LABEL,
  PROVIDER_LABEL,
  PROVIDERS_BY_DEPLOYMENT,
  createLlmProvider,
  getLlmProvider,
  testLlmDraft,
  updateLlmProvider,
  type LlmBillingType,
  type LlmDeployment,
  type LlmProvider,
  type LlmProviderRequest,
  type LlmTestResult,
} from "../../api/llm";

interface FormValues {
  deployment: LlmDeployment;
  provider: LlmProvider;
  billingType: LlmBillingType;
  name: string;
  modelName: string;
  baseUrl?: string;
  apiKey?: string;
  maxInputChars?: number;
  maskSensitive: boolean;
  description?: string;
}

/** 외부 제공자는 기본 주소를 서버가 채운다. 화면에는 참고로만 보여 준다(서버 LlmProvider 와 같은 값). */
const DEFAULT_BASE_URL: Partial<Record<LlmProvider, string>> = {
  ANTHROPIC: "https://api.anthropic.com",
  OPENAI: "https://api.openai.com/v1",
  GEMINI: "https://generativelanguage.googleapis.com",
};

const INITIAL: FormValues = {
  deployment: "EXTERNAL",
  provider: "ANTHROPIC",
  billingType: "FREE",
  name: "",
  modelName: "",
  maskSensitive: true,
};

/**
 * GPT 등록 · 수정(?id=). 설계: docs/2026-09-13-gpt-settings-design.md 3-2절.
 *
 * <p>배치 위치를 바꾸면 제공자·마스킹·최대 입력 길이 기본값이 따라 바뀐다. 다만 관리자가 마스킹이나
 * 입력 길이를 <b>직접 바꾼 뒤에는</b> 따라가지 않는다 - 끈 마스킹이 배치 위치를 건드리는 순간 다시 켜지면 안 된다.
 */
export function GptRegisterPage() {
  const [params] = useSearchParams();
  const editId = params.get("id") ? Number(params.get("id")) : undefined;
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form] = Form.useForm<FormValues>();
  const deployment = Form.useWatch("deployment", form) ?? INITIAL.deployment;
  const provider = Form.useWatch("provider", form) ?? INITIAL.provider;
  const [testResult, setTestResult] = useState<LlmTestResult>();
  const touched = useRef({ mask: false, maxInput: false });

  const detailQuery = useQuery({
    queryKey: ["llm-provider", editId],
    queryFn: () => getLlmProvider(editId!),
    enabled: editId !== undefined,
  });
  const existing = detailQuery.data;

  useEffect(() => {
    if (!existing) return;
    // 수정 화면은 저장된 값이 곧 «직접 정한 값»이라 기본값 따라가기를 끈다.
    touched.current = { mask: true, maxInput: true };
    form.setFieldsValue({
      deployment: existing.deployment,
      provider: existing.provider,
      billingType: existing.billingType,
      name: existing.name,
      modelName: existing.modelName,
      baseUrl: existing.baseUrl ?? undefined,
      apiKey: undefined,
      maxInputChars: existing.maxInputChars,
      maskSensitive: existing.maskSensitive,
      description: existing.description ?? undefined,
    });
  }, [existing, form]);

  const onDeploymentChange = (next: LlmDeployment) => {
    const patch: Partial<FormValues> = { provider: PROVIDERS_BY_DEPLOYMENT[next][0] };
    if (!touched.current.mask) patch.maskSensitive = next === "EXTERNAL";
    if (!touched.current.maxInput) patch.maxInputChars = DEFAULT_MAX_INPUT_CHARS[next];
    if (next === "EXTERNAL") patch.baseUrl = undefined;
    form.setFieldsValue(patch);
    setTestResult(undefined);
  };

  const toRequest = (values: FormValues): LlmProviderRequest => ({
    name: values.name.trim(),
    provider: values.provider,
    billingType: values.billingType,
    baseUrl: values.baseUrl?.trim() || null,
    modelName: values.modelName.trim(),
    apiKey: values.apiKey?.trim() || null,
    maskSensitive: values.maskSensitive,
    maxInputChars: values.maxInputChars ?? null,
    description: values.description?.trim() || null,
  });

  const testMutation = useMutation({
    mutationFn: async () => testLlmDraft(toRequest(await form.validateFields()), editId),
    onMutate: () => setTestResult(undefined),
    onSuccess: setTestResult,
    onError: (error: Error) => {
      // validateFields 실패는 폼이 이미 칸마다 표시한다.
      if (error.message) message.error(error.message);
    },
  });

  const saveMutation = useMutation({
    mutationFn: async () => {
      const request = toRequest(await form.validateFields());
      return editId ? updateLlmProvider(editId, request) : createLlmProvider(request);
    },
    onSuccess: () => {
      message.success(editId ? "GPT 설정을 수정했습니다." : "GPT 설정을 등록했습니다.");
      void queryClient.invalidateQueries({ queryKey: ["llm-providers"] });
      void queryClient.invalidateQueries({ queryKey: ["llm-status"] });
      navigate("/gpt/manage");
    },
    onError: (error: Error) => {
      if (error.message) message.error(error.message);
    },
  });

  if (editId !== undefined && detailQuery.isLoading) {
    return <Card><Spin /> GPT 설정을 불러오는 중입니다.</Card>;
  }
  if (editId !== undefined && detailQuery.isError) {
    return <Card><Alert type="error" showIcon message="GPT 설정을 찾을 수 없습니다." /></Card>;
  }

  const external = deployment === "EXTERNAL";
  const keyPlaceholder = existing?.apiKeySet
    ? `변경할 때만 입력 (현재 ${existing.apiKeyHint ?? "설정됨"})`
    : external ? "API 키" : "필요한 경우만 입력";

  return (
    <Card title={editId ? "GPT 수정" : "GPT 등록"} style={{ maxWidth: 820 }}>
      <Form<FormValues>
        form={form}
        layout="horizontal"
        labelCol={{ flex: "140px" }}
        // flex:auto 면 긴 안내문(extra)이 칸 최소 폭을 밀어 올려 라벨과 입력칸이 두 줄로 갈라진다
        // (개인정보 마스킹 줄에서 실측). 남은 폭만 쓰고 줄바꿈하게 min-width 를 0 으로 둔다.
        wrapperCol={{ flex: "1 1 0", style: { minWidth: 0 } }}
        initialValues={{ ...INITIAL, maxInputChars: DEFAULT_MAX_INPUT_CHARS.EXTERNAL }}
        onValuesChange={(changed) => {
          if ("maskSensitive" in changed) touched.current.mask = true;
          if ("maxInputChars" in changed) touched.current.maxInput = true;
          if (!("description" in changed) && !("name" in changed)) setTestResult(undefined);
        }}
      >
        <Form.Item label="배치 위치" name="deployment">
          <Radio.Group onChange={(e) => onDeploymentChange(e.target.value as LlmDeployment)}>
            <Radio value="EXTERNAL">{DEPLOYMENT_LABEL.EXTERNAL} (Claude · ChatGPT · Gemini)</Radio>
            <Radio value="INTERNAL">{DEPLOYMENT_LABEL.INTERNAL}</Radio>
          </Radio.Group>
        </Form.Item>

        <Form.Item label="제공자" name="provider" rules={[{ required: true }]}>
          <Select
            style={{ maxWidth: 320 }}
            options={PROVIDERS_BY_DEPLOYMENT[deployment].map((value) => ({ value, label: PROVIDER_LABEL[value] }))}
          />
        </Form.Item>
        {!external && (
          <Form.Item wrapperCol={{ offset: 0 }} style={{ marginTop: -12 }} label=" " colon={false}>
            <span style={{ color: "#6b7280", fontSize: 12 }}>
              Ollama · vLLM · LM Studio 등 OpenAI 와 같은 형식의 API 를 제공하는 서빙이면 주소만 넣어 연결됩니다.
            </span>
          </Form.Item>
        )}

        <Form.Item label="과금 구분" name="billingType">
          <Radio.Group>
            <Radio value="FREE">{BILLING_LABEL.FREE}</Radio>
            <Radio value="PAID">{BILLING_LABEL.PAID}</Radio>
          </Radio.Group>
        </Form.Item>

        <Form.Item label="이름" name="name" rules={[{ required: true, whitespace: true, message: "이름을 입력하십시오." }, { max: 100 }]}>
          <Input placeholder="예) 운영용 Claude" style={{ maxWidth: 420 }} />
        </Form.Item>

        <Form.Item
          label="모델명"
          name="modelName"
          rules={[{ required: true, whitespace: true, message: "모델명을 입력하십시오." }, { max: 200 }]}
          extra="제공자가 안내하는 모델 ID 를 그대로 입력합니다. 저장 전에 연결 테스트로 확인하십시오."
        >
          <Input placeholder={external ? "제공자 문서의 모델 ID" : "예) llama-3-70b"} style={{ maxWidth: 420 }} />
        </Form.Item>

        <Form.Item
          label="API 주소"
          name="baseUrl"
          rules={[
            { required: !external, whitespace: true, message: "내부 LLM 은 API 주소가 필요합니다." },
            { pattern: /^https?:\/\//i, message: "http:// 또는 https:// 로 시작해야 합니다." },
          ]}
          extra={external ? `비워 두면 기본 주소를 씁니다: ${DEFAULT_BASE_URL[provider] ?? ""}` : undefined}
        >
          <Input placeholder={external ? DEFAULT_BASE_URL[provider] : "예) http://10.0.0.5:8000/v1"} style={{ maxWidth: 520 }} />
        </Form.Item>

        <Form.Item
          label="API 키"
          name="apiKey"
          rules={[{
            required: external && !existing?.apiKeySet,
            whitespace: true,
            message: "외부 제공자는 무료 등급이어도 API 키가 필요합니다.",
          }]}
          extra={existing?.apiKeySet
            ? "제공자나 API 주소를 바꾸면 저장된 키는 쓰이지 않습니다. 이때는 키를 다시 입력하십시오."
            : undefined}
        >
          <Input.Password placeholder={keyPlaceholder} autoComplete="new-password" style={{ maxWidth: 520 }} />
        </Form.Item>

        <Form.Item
          label="최대 입력 길이"
          name="maxInputChars"
          extra="GPT 에게 한 번에 보내는 정보의 최대 글자 수입니다. 넘치면 중요도가 낮은 정보부터 잘라 보냅니다."
        >
          <InputNumber min={1000} max={2_000_000} step={1000} style={{ width: 200 }} addonAfter="자" />
        </Form.Item>

        <Form.Item
          label="개인정보 마스킹"
          name="maskSensitive"
          valuePropName="checked"
          extra="전화번호·이메일·주민번호·카드번호 형태를 <종류> 로 가려서 보냅니다. 이름·주소처럼 형태가 없는 값은 가리지 못합니다."
        >
          <Checkbox>전송 시 개인정보 마스킹</Checkbox>
        </Form.Item>

        <Form.Item label="설명" name="description">
          <Input.TextArea rows={2} style={{ maxWidth: 520 }} />
        </Form.Item>

        {testResult && (
          <Form.Item label=" " colon={false}>
            <Alert
              type={testResult.success ? "success" : "error"}
              showIcon
              message={testResult.message}
              description={testResult.detail
                ? <span style={{ fontFamily: "monospace", fontSize: 12, wordBreak: "break-all" }}>{testResult.detail}</span>
                : undefined}
            />
          </Form.Item>
        )}

        <Form.Item label=" " colon={false}>
          <Space>
            <Button onClick={() => testMutation.mutate()} loading={testMutation.isPending}>연결 테스트</Button>
            <Button type="primary" onClick={() => saveMutation.mutate()} loading={saveMutation.isPending}>저장</Button>
            <Button onClick={() => navigate("/gpt/manage")}>목록</Button>
          </Space>
        </Form.Item>
      </Form>
    </Card>
  );
}

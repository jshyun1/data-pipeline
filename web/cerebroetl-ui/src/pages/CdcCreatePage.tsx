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
  Form,
  Input,
  message,
  Result,
  Select,
  Space,
  Spin,
  Tabs,
  Tag,
  Typography,
} from "antd";
import { CheckCircleOutlined, LockOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import { listConnections, listConnectionSchemas, listConnectionTables, testConnection } from "../api/connections";
import { createLogFilePipeline, createPipeline } from "../api/pipelines";
import type { ConnectionResponse } from "../types/connection";
import type { LogPipelineCreateRequest, PipelineCreateRequest, PipelineResponse } from "../types/pipeline";

type StepKey = "basic" | "connections" | "targets" | "options" | "review";

const STEP_ORDER: StepKey[] = ["basic", "connections", "targets", "options", "review"];
const STEP_NUMBER: Record<StepKey, number> = {
  basic: 1,
  connections: 2,
  targets: 3,
  options: 4,
  review: 5,
};

function StepLabel({ step, title, completed, unlocked }: {
  step: StepKey;
  title: string;
  completed: boolean;
  unlocked: boolean;
}) {
  return (
    <Space>
      <Tag color={completed ? "success" : unlocked ? "blue" : "default"}>{STEP_NUMBER[step]}</Tag>
      <span>{title}</span>
      {completed && <CheckCircleOutlined style={{ color: "#52c41a" }} />}
      {!unlocked && <LockOutlined style={{ color: "#999" }} />}
    </Space>
  );
}

function connectionLabel(connection: ConnectionResponse) {
  return `${connection.name} (${connection.dbType} · ${connection.host}:${connection.port})`;
}

function CdcCreateWizard() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form] = Form.useForm<PipelineCreateRequest>();
  const [activeStep, setActiveStep] = useState<StepKey>("basic");
  const [completedSteps, setCompletedSteps] = useState<StepKey[]>([]);
  const [sourceTestedId, setSourceTestedId] = useState<number | null>(null);
  const [targetTestedId, setTargetTestedId] = useState<number | null>(null);
  const [createdPipeline, setCreatedPipeline] = useState<PipelineResponse | null>(null);

  const { data: connections, isLoading: connectionsLoading } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });

  const sourceConnectionId = Form.useWatch("sourceConnectionId", form);
  const targetConnectionId = Form.useWatch("targetConnectionId", form);
  const sourceSchema = Form.useWatch("sourceSchema", form);
  const targetSchema = Form.useWatch("targetSchema", form);
  const formValues = Form.useWatch([], form);

  const sourceSchemasQuery = useQuery({
    queryKey: ["connection-schemas", sourceConnectionId],
    queryFn: () => listConnectionSchemas(sourceConnectionId!),
    enabled: sourceTestedId === sourceConnectionId && sourceConnectionId != null,
  });
  const sourceTablesQuery = useQuery({
    queryKey: ["connection-tables", sourceConnectionId, sourceSchema],
    queryFn: () => listConnectionTables(sourceConnectionId!, sourceSchema!),
    enabled: sourceTestedId === sourceConnectionId && sourceConnectionId != null && !!sourceSchema,
  });
  const targetSchemasQuery = useQuery({
    queryKey: ["connection-schemas", targetConnectionId],
    queryFn: () => listConnectionSchemas(targetConnectionId!),
    enabled: targetTestedId === targetConnectionId && targetConnectionId != null,
  });
  const targetTablesQuery = useQuery({
    queryKey: ["connection-tables", targetConnectionId, targetSchema],
    queryFn: () => listConnectionTables(targetConnectionId!, targetSchema!),
    enabled: targetTestedId === targetConnectionId && targetConnectionId != null && !!targetSchema,
  });

  const sourceConnection = connections?.find((connection) => connection.id === sourceConnectionId);
  const targetConnection = connections?.find((connection) => connection.id === targetConnectionId);
  const connectionsVerified = sourceTestedId === sourceConnectionId && targetTestedId === targetConnectionId;

  const isUnlocked = (step: StepKey) => {
    const index = STEP_ORDER.indexOf(step);
    return index === 0 || completedSteps.includes(STEP_ORDER[index - 1]);
  };

  const completeAndOpen = (current: StepKey, next: StepKey) => {
    setCompletedSteps((previous) => previous.includes(current) ? previous : [...previous, current]);
    setActiveStep(next);
  };

  const invalidateFrom = (step: StepKey) => {
    const index = STEP_ORDER.indexOf(step);
    setCompletedSteps((previous) => previous.filter((item) => STEP_ORDER.indexOf(item) < index));
  };

  const sourceTestMutation = useMutation({
    mutationFn: testConnection,
    onSuccess: (result) => {
      if (result.status === "SUCCESS") {
        setSourceTestedId(result.id);
        message.success(`${result.name}: 소스 연결 성공`);
      } else {
        setSourceTestedId(null);
        message.error(`${result.name}: 소스 연결 실패`);
      }
    },
    onError: (error: Error) => {
      setSourceTestedId(null);
      message.error(error.message);
    },
  });

  const targetTestMutation = useMutation({
    mutationFn: testConnection,
    onSuccess: (result) => {
      if (result.status === "SUCCESS") {
        setTargetTestedId(result.id);
        message.success(`${result.name}: 타깃 연결 성공`);
      } else {
        setTargetTestedId(null);
        message.error(`${result.name}: 타깃 연결 실패`);
      }
    },
    onError: (error: Error) => {
      setTargetTestedId(null);
      message.error(error.message);
    },
  });

  const createMutation = useMutation({
    mutationFn: createPipeline,
    onSuccess: (result) => {
      setCreatedPipeline(result);
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      message.success("CDC 파이프라인을 실행 대기 상태로 준비했습니다.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  const schemaTableNotFoundContent = (
    query: { isFetching: boolean; isError: boolean },
    emptyHint: string,
  ) => query.isFetching ? <Spin size="small" /> : query.isError ? "조회 실패 - 연결정보를 확인하세요" : emptyHint;

  const topicPreview = useMemo(() => {
    if (!formValues?.topicPrefix || !formValues?.sourceSchema || !formValues?.sourceTable) return "—";
    return `${formValues.topicPrefix}.${formValues.sourceSchema}.${formValues.sourceTable}`;
  }, [formValues]);

  if (createdPipeline) {
    return (
      <Card>
        <Result
          status="success"
          title="CDC 파이프라인 생성 완료"
          subTitle={`${createdPipeline.name}이(가) 실행 대기 상태로 준비되었습니다. 실행 제어는 AirFlow에서 진행하세요.`}
          extra={[
            <Button type="primary" key="list" onClick={() => navigate("/cdc/pipelines")}>파이프라인 목록</Button>,
            <Button key="airflow" onClick={() => navigate("/airflow/dashboard")}>AirFlow에서 열기</Button>,
            <Button key="again" onClick={() => {
              form.resetFields();
              setCompletedSteps([]);
              setSourceTestedId(null);
              setTargetTestedId(null);
              setCreatedPipeline(null);
              setActiveStep("basic");
            }}>하나 더 생성</Button>,
          ]}
        />
      </Card>
    );
  }

  return (
    <div style={{ maxWidth: 980, margin: "0 auto" }}>
      <Card title="CDC 파이프라인 생성" style={{ marginBottom: 16 }}>
        <Alert
          type="info"
          showIcon
          message="각 단계를 완료해야 다음 단계가 열립니다"
          description="연결 단계에서는 소스와 타깃 연결 테스트가 모두 성공해야 대상 테이블을 선택할 수 있습니다."
        />
      </Card>

      <Form<PipelineCreateRequest>
        form={form}
        layout="vertical"
        initialValues={{ deleteEnabled: false }}
      >
        <Collapse
          accordion
          activeKey={activeStep}
          onChange={(key) => {
            const requested = Array.isArray(key) ? key[0] : key;
            if (requested && isUnlocked(requested as StepKey)) setActiveStep(requested as StepKey);
          }}
          items={[
            {
              key: "basic",
              label: <StepLabel step="basic" title="기본 정보" completed={completedSteps.includes("basic")} unlocked />,
              children: (
                <>
                  <Form.Item name="name" label="파이프라인명" rules={[
                    { required: true, message: "파이프라인명을 입력하세요." },
                    { pattern: /^[a-zA-Z0-9][a-zA-Z0-9_-]{2,149}$/, message: "영문·숫자로 시작하고 영문·숫자·_·- 조합 3자 이상으로 입력하세요." },
                  ]}>
                    <Input placeholder="예: postgres-orders-to-warehouse" />
                  </Form.Item>
                  <Form.Item label="유형">
                    <Input value="테이블 CDC" disabled />
                  </Form.Item>
                  <Form.Item name="description" label="설명">
                    <Input.TextArea rows={3} placeholder="파이프라인의 목적과 담당 업무를 입력하세요." />
                  </Form.Item>
                  <div style={{ textAlign: "right" }}>
                    <Button type="primary" onClick={async () => {
                      await form.validateFields(["name"]);
                      completeAndOpen("basic", "connections");
                    }}>다음: 연결</Button>
                  </div>
                </>
              ),
            },
            {
              key: "connections",
              collapsible: isUnlocked("connections") ? undefined : "disabled",
              label: <StepLabel step="connections" title="연결" completed={completedSteps.includes("connections")} unlocked={isUnlocked("connections")} />,
              children: (
                <>
                  {connections?.length === 0 && (
                    <Alert type="warning" showIcon message="등록된 연결정보가 없습니다" action={<Button onClick={() => navigate("/cdc/connections")}>연결정보 등록</Button>} />
                  )}
                  <Space align="start" size="large" wrap style={{ width: "100%" }}>
                    <div style={{ flex: 1, minWidth: 360 }}>
                      <Form.Item name="sourceConnectionId" label="소스 연결" rules={[{ required: true, message: "소스 연결을 선택하세요." }]}>
                        <Select
                          loading={connectionsLoading}
                          options={connections?.map((connection) => ({ value: connection.id, label: connectionLabel(connection) }))}
                          onChange={(value) => {
                            setSourceTestedId(null);
                            invalidateFrom("connections");
                            form.setFieldsValue({ sourceSchema: undefined, sourceTable: undefined });
                            if (!form.getFieldValue("topicPrefix")) {
                              const selected = connections?.find((connection) => connection.id === value);
                              if (selected) form.setFieldValue("topicPrefix", `${selected.dbType.toLowerCase()}-cdc`);
                            }
                          }}
                          placeholder="소스 DB 선택"
                        />
                      </Form.Item>
                      <Button
                        block
                        disabled={!sourceConnectionId}
                        loading={sourceTestMutation.isPending}
                        onClick={() => sourceConnectionId && sourceTestMutation.mutate(sourceConnectionId)}
                      >소스 연결 테스트</Button>
                      {sourceTestedId === sourceConnectionId && <Alert style={{ marginTop: 8 }} type="success" showIcon message="소스 연결 성공" />}
                    </div>
                    <div style={{ flex: 1, minWidth: 360 }}>
                      <Form.Item name="targetConnectionId" label="타깃 연결" rules={[{ required: true, message: "타깃 연결을 선택하세요." }]}>
                        <Select
                          loading={connectionsLoading}
                          options={connections?.map((connection) => ({ value: connection.id, label: connectionLabel(connection) }))}
                          onChange={() => {
                            setTargetTestedId(null);
                            invalidateFrom("connections");
                            form.setFieldsValue({ targetSchema: undefined, targetTable: undefined });
                          }}
                          placeholder="타깃 DB 선택"
                        />
                      </Form.Item>
                      <Button
                        block
                        disabled={!targetConnectionId}
                        loading={targetTestMutation.isPending}
                        onClick={() => targetConnectionId && targetTestMutation.mutate(targetConnectionId)}
                      >타깃 연결 테스트</Button>
                      {targetTestedId === targetConnectionId && <Alert style={{ marginTop: 8 }} type="success" showIcon message="타깃 연결 성공" />}
                    </div>
                  </Space>
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("basic")}>이전</Button>
                    <Button type="primary" disabled={!connectionsVerified} onClick={async () => {
                      await form.validateFields(["sourceConnectionId", "targetConnectionId"]);
                      completeAndOpen("connections", "targets");
                    }}>다음: 대상 선택</Button>
                  </div>
                </>
              ),
            },
            {
              key: "targets",
              collapsible: isUnlocked("targets") ? undefined : "disabled",
              label: <StepLabel step="targets" title="대상 선택" completed={completedSteps.includes("targets")} unlocked={isUnlocked("targets")} />,
              children: (
                <>
                  <Typography.Title level={5}>소스</Typography.Title>
                  <Space align="start" size="large" wrap style={{ width: "100%" }}>
                    <Form.Item name="sourceSchema" label="소스 스키마" rules={[{ required: true }]} style={{ minWidth: 300, flex: 1 }}>
                      <Select showSearch loading={sourceSchemasQuery.isFetching} options={sourceSchemasQuery.data?.map((value) => ({ value }))}
                        notFoundContent={schemaTableNotFoundContent(sourceSchemasQuery, "스키마 없음")}
                        onChange={() => { form.setFieldValue("sourceTable", undefined); invalidateFrom("targets"); }} />
                    </Form.Item>
                    <Form.Item name="sourceTable" label="소스 테이블" rules={[{ required: true }]} style={{ minWidth: 300, flex: 1 }}>
                      <Select showSearch disabled={!sourceSchema} loading={sourceTablesQuery.isFetching}
                        options={sourceTablesQuery.data?.map((value) => ({ value }))}
                        notFoundContent={schemaTableNotFoundContent(sourceTablesQuery, "테이블 없음")}
                        onChange={() => invalidateFrom("targets")} />
                    </Form.Item>
                  </Space>
                  <Typography.Title level={5}>타깃</Typography.Title>
                  <Space align="start" size="large" wrap style={{ width: "100%" }}>
                    <Form.Item name="targetSchema" label="타깃 스키마" rules={[{ required: true }]} style={{ minWidth: 300, flex: 1 }}>
                      <Select showSearch loading={targetSchemasQuery.isFetching} options={targetSchemasQuery.data?.map((value) => ({ value }))}
                        notFoundContent={schemaTableNotFoundContent(targetSchemasQuery, "스키마 없음")}
                        onChange={() => { form.setFieldValue("targetTable", undefined); invalidateFrom("targets"); }} />
                    </Form.Item>
                    <Form.Item name="targetTable" label="타깃 테이블" rules={[{ required: true }]} style={{ minWidth: 300, flex: 1 }}>
                      <AutoComplete disabled={!targetSchema} options={targetTablesQuery.data?.map((value) => ({ value }))}
                        notFoundContent={schemaTableNotFoundContent(targetTablesQuery, "기존 테이블 없음 - 새 이름 입력 가능")}
                        onChange={() => invalidateFrom("targets")} placeholder="기존 테이블 선택 또는 새 이름 입력" />
                    </Form.Item>
                  </Space>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <Button onClick={() => setActiveStep("connections")}>이전</Button>
                    <Button type="primary" onClick={async () => {
                      await form.validateFields(["sourceSchema", "sourceTable", "targetSchema", "targetTable"]);
                      completeAndOpen("targets", "options");
                    }}>다음: 실행 옵션</Button>
                  </div>
                </>
              ),
            },
            {
              key: "options",
              collapsible: isUnlocked("options") ? undefined : "disabled",
              label: <StepLabel step="options" title="실행 옵션" completed={completedSteps.includes("options")} unlocked={isUnlocked("options")} />,
              children: (
                <>
                  <Form.Item label="스냅샷 모드">
                    <Select value="initial" disabled options={[{ value: "initial", label: "초기 적재 후 CDC (현재 지원 모드)" }]} />
                  </Form.Item>
                  <Form.Item name="topicPrefix" label="Topic Prefix" rules={[{ required: true }]} tooltip="실제 토픽은 prefix.schema.table 형식입니다.">
                    <Input placeholder="예: postgres-cdc" />
                  </Form.Item>
                  <Form.Item name="deleteEnabled" valuePropName="checked">
                    <Checkbox>소스 DELETE 이벤트를 타깃에 반영</Checkbox>
                  </Form.Item>
                  <Alert type="info" showIcon message={`생성 예정 Topic: ${topicPreview}`} style={{ marginBottom: 16 }} />
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <Button onClick={() => setActiveStep("targets")}>이전</Button>
                    <Button type="primary" onClick={async () => {
                      await form.validateFields(["topicPrefix"]);
                      completeAndOpen("options", "review");
                    }}>다음: 검토</Button>
                  </div>
                </>
              ),
            },
            {
              key: "review",
              collapsible: isUnlocked("review") ? undefined : "disabled",
              label: <StepLabel step="review" title="검토 및 생성" completed={false} unlocked={isUnlocked("review")} />,
              children: (
                <>
                  <Descriptions bordered column={1} size="small">
                    <Descriptions.Item label="파이프라인명">{formValues?.name ?? "—"}</Descriptions.Item>
                    <Descriptions.Item label="소스 연결">{sourceConnection ? connectionLabel(sourceConnection) : "—"}</Descriptions.Item>
                    <Descriptions.Item label="소스 대상">{formValues?.sourceSchema}.{formValues?.sourceTable}</Descriptions.Item>
                    <Descriptions.Item label="타깃 연결">{targetConnection ? connectionLabel(targetConnection) : "—"}</Descriptions.Item>
                    <Descriptions.Item label="타깃 대상">{formValues?.targetSchema}.{formValues?.targetTable}</Descriptions.Item>
                    <Descriptions.Item label="Kafka Topic">{topicPreview}</Descriptions.Item>
                    <Descriptions.Item label="DELETE 반영">{formValues?.deleteEnabled ? "사용" : "사용 안 함"}</Descriptions.Item>
                  </Descriptions>
                  <Alert type="warning" showIcon style={{ marginTop: 16 }} message="생성 시 Kafka Connect 커넥터까지 준비됩니다" description="완료 후 실행은 AirFlow에서 진행합니다." />
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("options")}>이전</Button>
                    <Button type="primary" loading={createMutation.isPending} onClick={async () => {
                      const values = await form.validateFields();
                      if (!connectionsVerified) {
                        message.error("연결정보가 변경되었습니다. 소스와 타깃 연결을 다시 테스트하세요.");
                        setActiveStep("connections");
                        return;
                      }
                      createMutation.mutate(values);
                    }}>파이프라인 생성</Button>
                  </div>
                </>
              ),
            },
          ]}
        />
      </Form>
    </div>
  );
}

type LogStepKey = "basic" | "connection" | "target" | "options" | "review";
const LOG_STEP_ORDER: LogStepKey[] = ["basic", "connection", "target", "options", "review"];

function LogCreateWizard() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [form] = Form.useForm<LogPipelineCreateRequest>();
  const [activeStep, setActiveStep] = useState<LogStepKey>("basic");
  const [completedSteps, setCompletedSteps] = useState<LogStepKey[]>([]);
  const [testedConnectionId, setTestedConnectionId] = useState<number | null>(null);
  const [createdPipeline, setCreatedPipeline] = useState<PipelineResponse | null>(null);
  const targetConnectionId = Form.useWatch("targetConnectionId", form);
  const targetSchema = Form.useWatch("targetSchema", form);
  const values = Form.useWatch([], form);

  const { data: connections, isLoading: connectionsLoading } = useQuery({ queryKey: ["connections"], queryFn: listConnections });
  const schemasQuery = useQuery({
    queryKey: ["connection-schemas", targetConnectionId],
    queryFn: () => listConnectionSchemas(targetConnectionId!),
    enabled: testedConnectionId === targetConnectionId && targetConnectionId != null,
  });
  const tablesQuery = useQuery({
    queryKey: ["connection-tables", targetConnectionId, targetSchema],
    queryFn: () => listConnectionTables(targetConnectionId!, targetSchema!),
    enabled: testedConnectionId === targetConnectionId && targetConnectionId != null && !!targetSchema,
  });
  const targetConnection = connections?.find((connection) => connection.id === targetConnectionId);
  const unlocked = (step: LogStepKey) => {
    const index = LOG_STEP_ORDER.indexOf(step);
    return index === 0 || completedSteps.includes(LOG_STEP_ORDER[index - 1]);
  };
  const advance = (current: LogStepKey, next: LogStepKey) => {
    setCompletedSteps((previous) => previous.includes(current) ? previous : [...previous, current]);
    setActiveStep(next);
  };
  const invalidateFrom = (step: LogStepKey) => {
    const index = LOG_STEP_ORDER.indexOf(step);
    setCompletedSteps((previous) => previous.filter((item) => LOG_STEP_ORDER.indexOf(item) < index));
  };
  const label = (step: LogStepKey, title: string) => {
    const isComplete = completedSteps.includes(step);
    const isUnlocked = unlocked(step);
    return <Space><Tag color={isComplete ? "success" : isUnlocked ? "blue" : "default"}>{LOG_STEP_ORDER.indexOf(step) + 1}</Tag><span>{title}</span>{isComplete && <CheckCircleOutlined style={{ color: "#52c41a" }} />}{!isUnlocked && <LockOutlined style={{ color: "#999" }} />}</Space>;
  };
  const testMutation = useMutation({
    mutationFn: testConnection,
    onSuccess: (result) => {
      if (result.status === "SUCCESS") {
        setTestedConnectionId(result.id);
        message.success(`${result.name}: 타깃 연결 성공`);
      } else {
        setTestedConnectionId(null);
        message.error(`${result.name}: 타깃 연결 실패`);
      }
    },
    onError: (error: Error) => { setTestedConnectionId(null); message.error(error.message); },
  });
  const createMutation = useMutation({
    mutationFn: createLogFilePipeline,
    onSuccess: (result) => {
      setCreatedPipeline(result);
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      message.success("로그파일 파이프라인을 생성했습니다.");
    },
    onError: (error: Error) => message.error(error.message),
  });
  const notFound = (query: { isFetching: boolean; isError: boolean }, hint: string) => query.isFetching ? <Spin size="small" /> : query.isError ? "조회 실패 - 연결정보를 확인하세요" : hint;

  if (createdPipeline) {
    return <Card><Result status="success" title="로그파일 파이프라인 생성 완료" subTitle={`${createdPipeline.name}이(가) 생성되었습니다.`} extra={[
      <Button type="primary" key="list" onClick={() => navigate("/cdc/pipelines")}>파이프라인 목록</Button>,
      <Button key="again" onClick={() => { form.resetFields(); setCompletedSteps([]); setTestedConnectionId(null); setCreatedPipeline(null); setActiveStep("basic"); }}>하나 더 생성</Button>,
    ]} /></Card>;
  }

  return <>
    <Card title="로그파일 파이프라인 생성" style={{ marginBottom: 16 }}>
      <Alert type="info" showIcon message="각 단계를 완료해야 다음 단계가 열립니다" description="타깃 연결 테스트가 성공해야 스키마와 테이블을 선택할 수 있습니다." />
    </Card>
    <Form<LogPipelineCreateRequest> form={form} layout="vertical" initialValues={{ readFrom: "END", encoding: "UTF-8" }}>
      <Collapse accordion activeKey={activeStep} onChange={(key) => { const requested = (Array.isArray(key) ? key[0] : key) as LogStepKey; if (requested && unlocked(requested)) setActiveStep(requested); }} items={[
        { key: "basic", label: label("basic", "기본 및 파일 정보"), children: <>
          <Form.Item name="name" label="파이프라인명" rules={[{ required: true }, { pattern: /^[a-zA-Z0-9][a-zA-Z0-9_-]{2,149}$/, message: "영문·숫자로 시작하고 영문·숫자·_·- 조합 3자 이상으로 입력하세요." }]}><Input placeholder="예: app-log-ingest" /></Form.Item>
          <Form.Item name="filePath" label="로그 파일 경로" rules={[{ required: true }]} tooltip="Filebeat 컨테이너 기준 경로입니다. docker-compose의 ./log-sources는 /var/log/app에 마운트됩니다."><Input placeholder="예: /var/log/app/app.log" /></Form.Item>
          <Form.Item name="description" label="설명"><Input.TextArea rows={3} /></Form.Item>
          <div style={{ textAlign: "right" }}><Button type="primary" onClick={async () => { await form.validateFields(["name", "filePath"]); advance("basic", "connection"); }}>다음: 연결</Button></div>
        </> },
        { key: "connection", collapsible: unlocked("connection") ? undefined : "disabled", label: label("connection", "타깃 연결"), children: <>
          <Form.Item name="targetConnectionId" label="타깃 연결" rules={[{ required: true }]}><Select loading={connectionsLoading} options={connections?.map((connection) => ({ value: connection.id, label: connectionLabel(connection) }))} placeholder="랜딩할 DB 선택" onChange={() => { setTestedConnectionId(null); invalidateFrom("connection"); form.setFieldsValue({ targetSchema: undefined, targetTable: undefined }); }} /></Form.Item>
          <Button block disabled={!targetConnectionId} loading={testMutation.isPending} onClick={() => targetConnectionId && testMutation.mutate(targetConnectionId)}>타깃 연결 테스트</Button>
          {testedConnectionId === targetConnectionId && <Alert style={{ marginTop: 8 }} type="success" showIcon message="타깃 연결 성공" />}
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}><Button onClick={() => setActiveStep("basic")}>이전</Button><Button type="primary" disabled={testedConnectionId !== targetConnectionId} onClick={() => advance("connection", "target")}>다음: 적재 대상</Button></div>
        </> },
        { key: "target", collapsible: unlocked("target") ? undefined : "disabled", label: label("target", "적재 대상"), children: <>
          <Space align="start" size="large" wrap style={{ width: "100%" }}>
            <Form.Item name="targetSchema" label="타깃 스키마" rules={[{ required: true }]} style={{ minWidth: 300, flex: 1 }}><Select showSearch loading={schemasQuery.isFetching} options={schemasQuery.data?.map((value) => ({ value }))} notFoundContent={notFound(schemasQuery, "스키마 없음")} onChange={() => { form.setFieldValue("targetTable", undefined); invalidateFrom("target"); }} /></Form.Item>
            <Form.Item name="targetTable" label="타깃 테이블" rules={[{ required: true }]} style={{ minWidth: 300, flex: 1 }}><AutoComplete disabled={!targetSchema} options={tablesQuery.data?.map((value) => ({ value }))} notFoundContent={notFound(tablesQuery, "기존 테이블 없음 - 새 이름 입력 가능")} placeholder="기존 테이블 선택 또는 새 이름 입력" onChange={() => invalidateFrom("target")} /></Form.Item>
          </Space>
          <div style={{ display: "flex", justifyContent: "space-between" }}><Button onClick={() => setActiveStep("connection")}>이전</Button><Button type="primary" onClick={async () => { await form.validateFields(["targetSchema", "targetTable"]); advance("target", "options"); }}>다음: 수집 옵션</Button></div>
        </> },
        { key: "options", collapsible: unlocked("options") ? undefined : "disabled", label: label("options", "수집 옵션"), children: <>
          <Space align="start" size="large" wrap style={{ width: "100%" }}><Form.Item name="readFrom" label="읽기 시작 위치" style={{ minWidth: 300, flex: 1 }}><Select options={[{ value: "END", label: "END (신규 라인부터)" }, { value: "BEGINNING", label: "BEGINNING (파일 처음부터)" }]} /></Form.Item><Form.Item name="encoding" label="인코딩" style={{ minWidth: 300, flex: 1 }}><Input /></Form.Item></Space>
          <Form.Item name="topicName" label="Kafka Topic" tooltip="비우면 log-{파이프라인 ID}로 자동 생성됩니다"><Input placeholder="비워두면 자동 생성" /></Form.Item>
          <Form.Item name="agentHost" label="Agent Host" tooltip="메타데이터용 식별 필드 (선택)"><Input placeholder="예: filebeat" /></Form.Item>
          <div style={{ display: "flex", justifyContent: "space-between" }}><Button onClick={() => setActiveStep("target")}>이전</Button><Button type="primary" onClick={() => advance("options", "review")}>다음: 검토</Button></div>
        </> },
        { key: "review", collapsible: unlocked("review") ? undefined : "disabled", label: label("review", "검토 및 생성"), children: <>
          <Descriptions bordered column={1} size="small"><Descriptions.Item label="파이프라인명">{values?.name ?? "—"}</Descriptions.Item><Descriptions.Item label="로그 파일">{values?.filePath ?? "—"}</Descriptions.Item><Descriptions.Item label="타깃 연결">{targetConnection ? connectionLabel(targetConnection) : "—"}</Descriptions.Item><Descriptions.Item label="적재 대상">{values?.targetSchema}.{values?.targetTable}</Descriptions.Item><Descriptions.Item label="읽기 시작">{values?.readFrom}</Descriptions.Item><Descriptions.Item label="Kafka Topic">{values?.topicName || "자동 생성"}</Descriptions.Item></Descriptions>
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}><Button onClick={() => setActiveStep("options")}>이전</Button><Button type="primary" loading={createMutation.isPending} onClick={async () => { const request = await form.validateFields(); if (testedConnectionId !== targetConnectionId) { message.error("타깃 연결을 다시 테스트하세요."); setActiveStep("connection"); return; } createMutation.mutate(request); }}>로그파일 파이프라인 생성</Button></div>
        </> },
      ]} />
    </Form>
  </>;
}

export function CdcCreatePage() {
  const [activeTab, setActiveTab] = useState("cdc");
  return (
    <div style={{ maxWidth: 980, margin: "0 auto", minWidth: 0 }}>
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        destroyInactiveTabPane
        items={[
          { key: "cdc", label: "CDC 생성", children: <CdcCreateWizard /> },
          { key: "log", label: "로그파일 생성", children: <LogCreateWizard /> },
        ]}
      />
    </div>
  );
}

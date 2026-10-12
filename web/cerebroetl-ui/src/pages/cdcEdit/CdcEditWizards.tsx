import { useMemo, useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Alert, Button, Checkbox, Collapse, Descriptions, Form, Input, message, Result, Select, Space, Table, Tag, Tooltip, TreeSelect } from "antd";
import { useNavigate } from "react-router-dom";
import { WizardStepStatus, WizardStepTitle, WizardTop, type WizardStepState } from "../../components/StdWizard";
import { WfIcon } from "../../components/WorkflowPageHeader";
import { listConnectionColumns, listConnections } from "../../api/connections";
import { updatePipeline } from "../../api/pipelines";
import { listPipelineGroups, type PipelineGroupResponse } from "../../api/pipelineGroups";
import type { ConnectionResponse } from "../../types/connection";
import type { PipelineResponse, PipelineUpdateRequest } from "../../types/pipeline";
import { loadModeLabel } from "../../types/pipeline";

/*
 * CDC 파이프라인 수정(2026-10-12 요청 - 관리 화면 «수정» 버튼이 생성 위자드를 수정 모드로 연다).
 *
 * 생성 위자드(CdcCreatePage)와 같은 단계 · 모양을 쓰되 따로 둔다 - 생성은 여러 테이블 · 연결 테스트 · 테이블 고르기가 얽혀 있어,
 * 거기에 수정 분기를 섞으면 생성 흐름을 건드릴 위험이 크다.
 *
 * 운영 중인 파이프라인에 영향이 없게:
 *  - 저장은 정의 값만 바꾼다(PUT /api/pipelines/{id}). 등록된 커넥터는 그대로고, 바뀐 커넥터 설정은 다음 «배포» 때 들어간다.
 *  - 고칠 수 있는 값: 이름 · 그룹 · 설명(실행 영향 없음), 제외 · 마스킹 컬럼 · DELETE 반영(커넥터 설정만 바뀜).
 *  - 나머지(연결 · 스키마 · 테이블 · Topic Prefix · 적재 방식 · 구분컬럼 · 스냅샷 모드)는 잠근다 - 커넥터 이름 · 토픽 · 읽던 위치에 묶인 값이라
 *    바꾸면 새 파이프라인이 된다. 화면은 지금 값을 그대로 보내고 서버가 같은지 다시 확인한다.
 */

type EditStep = "basic" | "connections" | "targets" | "options" | "review";
const EDIT_STEPS: EditStep[] = ["basic", "connections", "targets", "options", "review"];
const EDIT_TITLE: Record<EditStep, string> = {
  basic: "기본 정보",
  connections: "연결",
  targets: "대상 선택",
  options: "실행 옵션",
  review: "검토 및 저장",
};
const NAME_RULE = /^[a-zA-Z0-9][a-zA-Z0-9_-]{2,149}$/;
const LOCK_HINT = "바꾸면 커넥터 이름 · 토픽 · 읽던 위치(오프셋)가 달라져 처음부터 다시 적재하는 새 파이프라인이 됩니다. 바꾸려면 새로 생성하세요.";

function Locked({ children }: { children: ReactNode }) {
  return (
    <Space size={6} wrap>
      <span>{children}</span>
      <Tooltip title={LOCK_HINT}><Tag>수정 불가</Tag></Tooltip>
    </Space>
  );
}

function connectionLabel(connection?: ConnectionResponse) {
  return connection ? `${connection.name} (${connection.dbType} · ${connection.host}:${connection.port})` : "—";
}

function splitColumns(value: string | null | undefined) {
  return (value ?? "").split(",").map((item) => item.trim()).filter(Boolean);
}

function sameColumns(a: string[], b: string[]) {
  const left = [...new Set(a)].sort().join(",");
  const right = [...new Set(b)].sort().join(",");
  return left === right;
}

/** 저장된 토픽 이름(prefix.schema.table)에서 prefix 를 되찾는다(서버 PipelineEditService.topicPrefixOf 와 같은 규칙). */
function topicPrefixOf(pipeline: PipelineResponse) {
  const suffix = `.${pipeline.sourceSchema ?? ""}.${pipeline.sourceTable ?? ""}`;
  return pipeline.topicName.toLowerCase().endsWith(suffix.toLowerCase())
    ? pipeline.topicName.slice(0, pipeline.topicName.length - suffix.length)
    : pipeline.topicName;
}

/** 이름 · 그룹 · 설명 칸과 그룹 트리. 두 위자드가 같이 쓴다. */
function useGroupTree() {
  const { data: groups } = useQuery({ queryKey: ["pipeline-groups"], queryFn: listPipelineGroups });
  return useMemo(() => {
    interface GroupOption { value: number; title: string; children?: GroupOption[] }
    const build = (parentId: number | null): GroupOption[] => (groups ?? [])
      .filter((group: PipelineGroupResponse) => group.parentId === parentId)
      .map((group: PipelineGroupResponse) => {
        const children = build(group.id);
        return { value: group.id, title: group.name, children: children.length ? children : undefined };
      });
    return { tree: build(null), nameOf: (id: number | null | undefined) => (id == null ? "그룹 미지정" : groups?.find((group) => group.id === id)?.name ?? `그룹 ${id}`) };
  }, [groups]);
}

interface BasicValues {
  name: string;
  groupId?: number | null;
  description?: string;
  deleteEnabled?: boolean;
}

function BasicFields({ pipeline, groupTree, typeLabel }: {
  pipeline: PipelineResponse;
  groupTree: Array<{ value: number; title: string }>;
  typeLabel: string;
}) {
  return (
    <>
      <Form.Item name="name" label="파이프라인명" rules={[
        { required: true, message: "파이프라인명을 입력하세요." },
        // 예전 이름은 규칙과 달라도 그대로 둘 수 있다 - 바꿀 때만 생성과 같은 규칙을 본다.
        { validator: async (_, value: string) => {
          const trimmed = (value ?? "").trim();
          if (trimmed !== pipeline.name && !NAME_RULE.test(trimmed)) {
            throw new Error("영문·숫자로 시작하고 영문·숫자·_·- 조합 3자 이상으로 입력하세요.");
          }
        } },
      ]}>
        <Input />
      </Form.Item>
      <Form.Item name="groupId" label="그룹" tooltip="CDC 관리 화면 트리에서 이 파이프라인이 놓일 그룹입니다. 비워 두면 그룹 미지정입니다.">
        <TreeSelect allowClear treeDefaultExpandAll placeholder="그룹 선택 (선택 안 함 = 그룹 미지정)" treeData={groupTree} />
      </Form.Item>
      <Form.Item label="유형">
        <Locked><Input value={typeLabel} disabled style={{ width: 220 }} /></Locked>
      </Form.Item>
      <Form.Item name="description" label="설명">
        <Input.TextArea rows={3} placeholder="파이프라인의 목적과 담당 업무를 입력하세요." />
      </Form.Item>
    </>
  );
}

/** 저장 결과 화면. 커넥터 설정이 바뀐 수정이면 «다음 배포 때 반영»을 분명히 적는다. */
function SavedResult({ pipeline, runtimeChanged, onAgain }: { pipeline: PipelineResponse; runtimeChanged: boolean; onAgain: () => void }) {
  const navigate = useNavigate();
  return (
    <div className="std-card std-card-body">
      <Result
        status="success"
        title="수정 내용을 저장했습니다"
        subTitle={runtimeChanged
          ? `${pipeline.name}의 정의를 저장했습니다. 제외 · 마스킹 컬럼 · DELETE 반영 변경은 워크플로우에서 «배포»를 지시하면 커넥터에 들어가고, 그때까지는 지금 설정으로 계속 동작합니다.`
          : `${pipeline.name}의 정의를 저장했습니다. 실행 중인 커넥터에는 영향이 없습니다.`}
        extra={[
          <Button type="primary" key="list" onClick={() => navigate("/cdc/pipelines")}>파이프라인 목록</Button>,
          ...(runtimeChanged ? [<Button key="airflow" onClick={() => navigate("/airflow/dashboard")}>워크플로우에서 열기</Button>] : []),
          <Button key="again" onClick={onAgain}>계속 수정</Button>,
        ]}
      />
    </div>
  );
}

function EditHeader({ pipeline }: { pipeline: PipelineResponse }) {
  return (
    <span className="std-wizard-notice" style={{ marginRight: 12 }}>
      <WfIcon name="doc" size={14} />
      <b>{pipeline.name}</b>
    </span>
  );
}

// ------------------------------------------------------------------ 테이블 CDC

export function CdcEditWizard({ pipeline: initial }: { pipeline: PipelineResponse }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [pipeline, setPipeline] = useState(initial);
  const [form] = Form.useForm<BasicValues>();
  const [activeStep, setActiveStep] = useState<EditStep>("basic");
  const [excluded, setExcluded] = useState<string[]>(splitColumns(initial.excludedColumns));
  const [masked, setMasked] = useState<string[]>(splitColumns(initial.maskedColumns));
  const [saved, setSaved] = useState<{ pipeline: PipelineResponse; runtimeChanged: boolean } | null>(null);
  const values = Form.useWatch([], form);
  const { tree: groupTree, nameOf: groupName } = useGroupTree();
  const { data: connections } = useQuery({ queryKey: ["connections"], queryFn: listConnections });
  const columnsQuery = useQuery({
    queryKey: ["connection-columns", pipeline.sourceConnectionId, pipeline.sourceSchema, pipeline.sourceTable],
    queryFn: () => listConnectionColumns(pipeline.sourceConnectionId!, pipeline.sourceSchema!, pipeline.sourceTable!),
    enabled: pipeline.sourceConnectionId != null && !!pipeline.sourceSchema && !!pipeline.sourceTable,
    staleTime: 30_000,
  });
  const columns = columnsQuery.data ?? [];
  const isDelta = pipeline.loadMode === "DELTA_APPEND" || pipeline.loadMode === "DELTA_UPSERT";
  const sourceConnection = connections?.find((connection) => connection.id === pipeline.sourceConnectionId);
  const targetConnection = connections?.find((connection) => connection.id === pipeline.targetConnectionId);

  const initialValues: BasicValues = {
    name: pipeline.name,
    groupId: pipeline.groupId,
    description: pipeline.description ?? "",
    deleteEnabled: pipeline.deleteEnabled,
  };
  const name = (values?.name ?? pipeline.name).trim();
  const description = (values?.description ?? "").trim();
  const groupId = values?.groupId ?? null;
  const deleteEnabled = values?.deleteEnabled ?? pipeline.deleteEnabled;

  // 검토 단계의 «바뀐 항목». 커넥터 설정이 바뀌는 항목(runtime)은 따로 표시한다.
  const changes = useMemo(() => {
    const rows: Array<{ key: string; label: string; before: string; after: string; runtime: boolean }> = [];
    if (name !== pipeline.name) rows.push({ key: "name", label: "이름", before: pipeline.name, after: name, runtime: false });
    if (description !== (pipeline.description ?? "").trim()) rows.push({ key: "description", label: "설명", before: pipeline.description || "-", after: description || "-", runtime: false });
    if ((groupId ?? null) !== (pipeline.groupId ?? null)) rows.push({ key: "group", label: "그룹", before: groupName(pipeline.groupId), after: groupName(groupId), runtime: false });
    const beforeExcluded = splitColumns(pipeline.excludedColumns);
    if (!sameColumns(excluded, beforeExcluded)) rows.push({ key: "excluded", label: "제외 컬럼", before: beforeExcluded.join(", ") || "없음", after: excluded.join(", ") || "없음", runtime: true });
    const beforeMasked = splitColumns(pipeline.maskedColumns);
    if (!sameColumns(masked, beforeMasked)) rows.push({ key: "masked", label: "마스킹 컬럼", before: beforeMasked.join(", ") || "없음", after: masked.join(", ") || "없음", runtime: true });
    if (!isDelta && deleteEnabled !== pipeline.deleteEnabled) rows.push({ key: "delete", label: "DELETE 반영", before: pipeline.deleteEnabled ? "사용" : "사용 안 함", after: deleteEnabled ? "사용" : "사용 안 함", runtime: true });
    return rows;
  }, [name, description, groupId, excluded, masked, deleteEnabled, pipeline, isDelta, groupName]);
  const runtimeChanged = changes.some((row) => row.runtime);

  const saveMutation = useMutation({
    mutationFn: (request: PipelineUpdateRequest) => updatePipeline(pipeline.id, request),
    onSuccess: (result) => {
      setSaved({ pipeline: result, runtimeChanged });
      setPipeline(result);
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      queryClient.invalidateQueries({ queryKey: ["pipeline-pending-config"] });
      message.success("수정 내용을 저장했습니다.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  const save = async () => {
    const ok = await form.validateFields().then(() => true, () => false);
    if (!ok) { setActiveStep("basic"); return; }
    saveMutation.mutate({
      name,
      description: description || null,
      groupId,
      excludedColumns: excluded,
      maskedColumns: masked,
      deleteEnabled: isDelta ? false : deleteEnabled,
      // 잠긴 값 - 지금 값을 그대로 보내 서버가 같은지 확인하게 한다.
      sourceConnectionId: pipeline.sourceConnectionId,
      targetConnectionId: pipeline.targetConnectionId,
      sourceSchema: pipeline.sourceSchema,
      sourceTable: pipeline.sourceTable,
      targetSchema: pipeline.targetSchema,
      targetTable: pipeline.targetTable,
      topicPrefix: topicPrefixOf(pipeline),
      loadMode: pipeline.loadMode,
      deltaOpColumn: isDelta ? pipeline.deltaOpColumn : null,
      snapshotMode: pipeline.snapshotMode,
    });
  };

  if (saved) {
    return <SavedResult pipeline={saved.pipeline} runtimeChanged={saved.runtimeChanged} onAgain={() => setSaved(null)} />;
  }

  const stepState = (step: EditStep): WizardStepState => (activeStep === step ? "active" : "done");
  const label = (step: EditStep) => <WizardStepTitle number={EDIT_STEPS.indexOf(step) + 1} title={EDIT_TITLE[step]} state={stepState(step)} />;
  const nav = (back: EditStep | null, next: EditStep | null) => (
    <div style={{ display: "flex", justifyContent: "space-between", marginTop: 16 }}>
      {back ? <Button onClick={() => setActiveStep(back)}>이전</Button> : <span />}
      {next ? <Button type="primary" onClick={() => setActiveStep(next)}>다음</Button> : null}
    </div>
  );

  return (
    <>
      <WizardTop
        modeSwitch={<EditHeader pipeline={pipeline} />}
        noticeTitle="저장하면 정의 값만 바뀌고, 실행 중인 커넥터는 그대로입니다"
        noticeDesc="제외·마스킹 컬럼·DELETE 반영은 다음 «배포» 때 커넥터에 들어갑니다. 회색 «수정 불가» 항목은 새로 생성해야 바꿀 수 있습니다."
        steps={EDIT_STEPS.map((key) => ({ key, title: EDIT_TITLE[key] }))}
        stateOf={(key) => stepState(key as EditStep)}
        onStep={(key) => setActiveStep(key as EditStep)}
      />
      <Form<BasicValues> form={form} layout="vertical" initialValues={initialValues}>
        <Collapse
          accordion
          bordered={false}
          expandIconPlacement="end"
          className="std-wizard"
          activeKey={activeStep}
          onChange={(key) => { const requested = Array.isArray(key) ? key[0] : key; if (requested) setActiveStep(requested as EditStep); }}
          items={[
            {
              key: "basic",
              label: label("basic"),
              extra: <WizardStepStatus state={stepState("basic")} />,
              forceRender: true,
              children: (
                <>
                  <BasicFields pipeline={pipeline} groupTree={groupTree} typeLabel="테이블 CDC" />
                  {nav(null, "connections")}
                </>
              ),
            },
            {
              key: "connections",
              label: label("connections"),
              extra: <WizardStepStatus state={stepState("connections")} />,
              children: (
                <>
                  <Descriptions bordered column={1} size="small">
                    <Descriptions.Item label="소스 연결"><Locked>{connectionLabel(sourceConnection)}</Locked></Descriptions.Item>
                    <Descriptions.Item label="타깃 연결"><Locked>{connectionLabel(targetConnection)}</Locked></Descriptions.Item>
                  </Descriptions>
                  <Alert type="info" showIcon style={{ marginTop: 12 }}
                         message="연결 대상은 바꿀 수 없습니다"
                         description="다른 DB 를 가리키면 읽던 위치(오프셋)가 맞지 않습니다. 같은 DB 의 접속 정보(호스트 · 계정 · 비밀번호)는 설정 › 연결정보에서 고치면 다음 «배포» 때 반영됩니다." />
                  {nav("basic", "targets")}
                </>
              ),
            },
            {
              key: "targets",
              label: label("targets"),
              extra: <WizardStepStatus state={stepState("targets")} />,
              children: (
                <>
                  <Descriptions bordered column={1} size="small" style={{ marginBottom: 16 }}>
                    <Descriptions.Item label="소스 테이블"><Locked>{`${pipeline.sourceSchema}.${pipeline.sourceTable}`}</Locked></Descriptions.Item>
                    <Descriptions.Item label="타깃 테이블"><Locked>{`${pipeline.targetSchema}.${pipeline.targetTable}`}</Locked></Descriptions.Item>
                  </Descriptions>
                  {columnsQuery.isError && (
                    <Alert type="warning" showIcon style={{ marginBottom: 12 }}
                           message="소스 DB 에서 컬럼 정보를 읽지 못했습니다"
                           description="지금 지정된 값은 그대로 보입니다. 바꾼 값은 저장할 때 서버가 소스 DB 로 다시 확인합니다." />
                  )}
                  {pipeline.sourceDbType === "MYSQL" && (
                    <Alert type="info" showIcon style={{ marginBottom: 12 }}
                           message="MySQL 소스는 제외 · 마스킹을 아직 커넥터에 적용하지 않습니다"
                           description="값은 저장되지만 지금 MySQL 커넥터 설정에는 들어가지 않습니다." />
                  )}
                  <Form.Item label="제외 컬럼 (선택)" tooltip="타깃으로 보내지 않을 컬럼. PK 는 고를 수 없습니다.">
                    <Select
                      mode="multiple" allowClear maxTagCount="responsive"
                      loading={columnsQuery.isFetching}
                      value={excluded}
                      options={(columns.length ? columns : excluded.map((name) => ({ name, dataType: "", primaryKey: false, maskable: true })))
                        .map((column) => ({ value: column.name, disabled: column.primaryKey || masked.includes(column.name), label: column.primaryKey ? `${column.name} (PK)` : column.name }))}
                      placeholder="전송하지 않을 컬럼"
                      onChange={setExcluded}
                    />
                  </Form.Item>
                  <Form.Item label="마스킹 컬럼 (선택)" tooltip="8자리로 가려 보낼 컬럼. PK · 마스킹 불가 형식은 고를 수 없습니다.">
                    <Select
                      mode="multiple" allowClear maxTagCount="responsive"
                      loading={columnsQuery.isFetching}
                      value={masked}
                      options={(columns.length ? columns : masked.map((name) => ({ name, dataType: "", primaryKey: false, maskable: true })))
                        .map((column) => ({
                          value: column.name,
                          disabled: column.primaryKey || !column.maskable || excluded.includes(column.name),
                          label: column.dataType ? `${column.name} (${column.dataType}${column.primaryKey ? ", PK" : !column.maskable ? ", 마스킹 불가" : ""})` : column.name,
                        }))}
                      placeholder="8자리로 마스킹할 컬럼"
                      onChange={setMasked}
                    />
                  </Form.Item>
                  <Alert type="info" showIcon
                         message="이미 적재된 데이터는 바뀌지 않습니다"
                         description="제외 · 마스킹 변경은 다음 «배포» 이후 새로 들어오는 변경분부터 적용됩니다. 마스킹을 풀어도 이미 가려 적재된 값은 복구되지 않습니다." />
                  {nav("connections", "options")}
                </>
              ),
            },
            {
              key: "options",
              label: label("options"),
              extra: <WizardStepStatus state={stepState("options")} />,
              forceRender: true,
              children: (
                <>
                  <Descriptions bordered column={1} size="small" style={{ marginBottom: 16 }}>
                    <Descriptions.Item label="스냅샷 모드">
                      <Locked>{pipeline.snapshotMode === "NO_DATA" ? "기존 데이터 미적재 · 이후 CDC" : "초기 적재 후 CDC"}</Locked>
                    </Descriptions.Item>
                    <Descriptions.Item label="Topic Prefix"><Locked>{`${topicPrefixOf(pipeline)} (토픽 ${pipeline.topicName})`}</Locked></Descriptions.Item>
                    <Descriptions.Item label="적재 방식"><Locked>{loadModeLabel(pipeline.loadMode, pipeline.deltaOpColumn ?? "cdc_op")}</Locked></Descriptions.Item>
                  </Descriptions>
                  {!isDelta && (
                    <Form.Item name="deleteEnabled" valuePropName="checked" style={{ marginBottom: 8 }}>
                      <Checkbox>소스 DELETE 이벤트를 타깃에 반영</Checkbox>
                    </Form.Item>
                  )}
                  {isDelta && (
                    <Alert type="info" showIcon message="델타 적재는 DELETE 를 행으로 쌓습니다 - «DELETE 반영» 옵션이 없습니다." />
                  )}
                  {nav("targets", "review")}
                </>
              ),
            },
            {
              key: "review",
              label: label("review"),
              extra: <WizardStepStatus state={stepState("review")} />,
              children: (
                <>
                  {changes.length === 0 ? (
                    <Alert type="info" showIcon message="바뀐 항목이 없습니다" />
                  ) : (
                    <Table
                      rowKey="key"
                      size="small"
                      pagination={false}
                      dataSource={changes}
                      columns={[
                        { title: "항목", dataIndex: "label", width: 130 },
                        { title: "지금", dataIndex: "before" },
                        { title: "바꿀 값", dataIndex: "after" },
                        { title: "반영", width: 150, render: (_, row) => row.runtime ? <Tag color="warning">다음 «배포» 때</Tag> : <Tag color="success">저장 즉시</Tag> },
                      ]}
                    />
                  )}
                  {runtimeChanged && (
                    <Alert type="warning" showIcon style={{ marginTop: 12 }}
                           message="커넥터 설정이 바뀌는 수정입니다"
                           description="저장해도 실행 중인 커넥터는 지금 설정으로 계속 동작합니다. 워크플로우에서 «배포»를 지시하면 새 설정이 들어가고(커넥터 이름 · 읽던 위치는 그대로), 그때까지 목록에 «설정 미반영»이 표시됩니다." />
                  )}
                  {masked.length > 0 && !sameColumns(masked, splitColumns(pipeline.maskedColumns)) && (
                    <Alert type="warning" showIcon style={{ marginTop: 12 }}
                           message="마스킹된 값은 타깃에서 원본으로 복원할 수 없습니다"
                           description="원본 조회가 필요한 컬럼인지 저장 전에 다시 확인하세요." />
                  )}
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("options")}>이전</Button>
                    <Space>
                      <Button onClick={() => navigate("/cdc/pipelines")}>취소</Button>
                      <Button type="primary" disabled={changes.length === 0} loading={saveMutation.isPending} onClick={() => void save()}>저장</Button>
                    </Space>
                  </div>
                </>
              ),
            },
          ]}
        />
      </Form>
    </>
  );
}

// ------------------------------------------------------------------ 로그 파일

type LogEditStep = "basic" | "target" | "review";
const LOG_EDIT_STEPS: LogEditStep[] = ["basic", "target", "review"];
const LOG_EDIT_TITLE: Record<LogEditStep, string> = { basic: "기본 정보", target: "적재 대상", review: "검토 및 저장" };

/** 로그 파일 파이프라인 - 이름 · 그룹 · 설명만 고친다. 파일 경로 · 타깃 · 토픽은 같은 이유로 잠근다. */
export function LogEditWizard({ pipeline: initial }: { pipeline: PipelineResponse }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [pipeline, setPipeline] = useState(initial);
  const [form] = Form.useForm<BasicValues>();
  const [activeStep, setActiveStep] = useState<LogEditStep>("basic");
  const [saved, setSaved] = useState<PipelineResponse | null>(null);
  const values = Form.useWatch([], form);
  const { tree: groupTree, nameOf: groupName } = useGroupTree();
  const { data: connections } = useQuery({ queryKey: ["connections"], queryFn: listConnections });
  const targetConnection = connections?.find((connection) => connection.id === pipeline.targetConnectionId);

  const name = (values?.name ?? pipeline.name).trim();
  const description = (values?.description ?? "").trim();
  const groupId = values?.groupId ?? null;
  const changes = [
    ...(name !== pipeline.name ? [{ key: "name", label: "이름", before: pipeline.name, after: name }] : []),
    ...(description !== (pipeline.description ?? "").trim() ? [{ key: "description", label: "설명", before: pipeline.description || "-", after: description || "-" }] : []),
    ...((groupId ?? null) !== (pipeline.groupId ?? null) ? [{ key: "group", label: "그룹", before: groupName(pipeline.groupId), after: groupName(groupId) }] : []),
  ];

  const saveMutation = useMutation({
    mutationFn: (request: PipelineUpdateRequest) => updatePipeline(pipeline.id, request),
    onSuccess: (result) => {
      setSaved(result);
      setPipeline(result);
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      message.success("수정 내용을 저장했습니다.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  if (saved) {
    return <SavedResult pipeline={saved} runtimeChanged={false} onAgain={() => setSaved(null)} />;
  }

  const stepState = (step: LogEditStep): WizardStepState => (activeStep === step ? "active" : "done");
  const label = (step: LogEditStep) => <WizardStepTitle number={LOG_EDIT_STEPS.indexOf(step) + 1} title={LOG_EDIT_TITLE[step]} state={stepState(step)} />;

  return (
    <>
      <WizardTop
        modeSwitch={<EditHeader pipeline={pipeline} />}
        noticeTitle="저장하면 정의 값만 바뀌고, 실행 중인 수집은 그대로입니다"
        noticeDesc="로그 파이프라인은 이름·그룹·설명만 바꿀 수 있습니다."
        steps={LOG_EDIT_STEPS.map((key) => ({ key, title: LOG_EDIT_TITLE[key] }))}
        stateOf={(key) => stepState(key as LogEditStep)}
        onStep={(key) => setActiveStep(key as LogEditStep)}
      />
      <Form<BasicValues> form={form} layout="vertical"
                         initialValues={{ name: pipeline.name, groupId: pipeline.groupId, description: pipeline.description ?? "" }}>
        <Collapse
          accordion bordered={false} expandIconPlacement="end" className="std-wizard" activeKey={activeStep}
          onChange={(key) => { const requested = Array.isArray(key) ? key[0] : key; if (requested) setActiveStep(requested as LogEditStep); }}
          items={[
            {
              key: "basic", label: label("basic"), extra: <WizardStepStatus state={stepState("basic")} />, forceRender: true,
              children: (
                <>
                  <BasicFields pipeline={pipeline} groupTree={groupTree} typeLabel="로그파일 적재" />
                  <div style={{ textAlign: "right" }}><Button type="primary" onClick={() => setActiveStep("target")}>다음</Button></div>
                </>
              ),
            },
            {
              key: "target", label: label("target"), extra: <WizardStepStatus state={stepState("target")} />,
              children: (
                <>
                  <Descriptions bordered column={1} size="small">
                    <Descriptions.Item label="타깃 연결"><Locked>{connectionLabel(targetConnection)}</Locked></Descriptions.Item>
                    <Descriptions.Item label="적재 대상"><Locked>{`${pipeline.targetSchema}.${pipeline.targetTable}`}</Locked></Descriptions.Item>
                    <Descriptions.Item label="CDC Topic"><Locked>{pipeline.topicName}</Locked></Descriptions.Item>
                  </Descriptions>
                  <Alert type="info" showIcon style={{ marginTop: 12 }} message="파일 경로 · 읽기 시작 위치 · 인코딩 등 수집 설정도 바꿀 수 없습니다"
                         description="바꾸면 이미 읽은 위치 · 토픽과 어긋납니다. 새 로그 파이프라인으로 생성하세요." />
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 16 }}>
                    <Button onClick={() => setActiveStep("basic")}>이전</Button>
                    <Button type="primary" onClick={() => setActiveStep("review")}>다음</Button>
                  </div>
                </>
              ),
            },
            {
              key: "review", label: label("review"), extra: <WizardStepStatus state={stepState("review")} />,
              children: (
                <>
                  {changes.length === 0 ? <Alert type="info" showIcon message="바뀐 항목이 없습니다" /> : (
                    <Table rowKey="key" size="small" pagination={false} dataSource={changes} columns={[
                      { title: "항목", dataIndex: "label", width: 130 },
                      { title: "지금", dataIndex: "before" },
                      { title: "바꿀 값", dataIndex: "after" },
                    ]} />
                  )}
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
                    <Button onClick={() => setActiveStep("target")}>이전</Button>
                    <Space>
                      <Button onClick={() => navigate("/cdc/pipelines")}>취소</Button>
                      <Button type="primary" disabled={changes.length === 0} loading={saveMutation.isPending} onClick={async () => {
                        const ok = await form.validateFields().then(() => true, () => false);
                        if (!ok) { setActiveStep("basic"); return; }
                        saveMutation.mutate({
                          name, description: description || null, groupId,
                          targetConnectionId: pipeline.targetConnectionId,
                          targetSchema: pipeline.targetSchema,
                          targetTable: pipeline.targetTable,
                        });
                      }}>저장</Button>
                    </Space>
                  </div>
                </>
              ),
            },
          ]}
        />
      </Form>
    </>
  );
}

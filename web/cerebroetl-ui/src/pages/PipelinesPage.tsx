import dayjs from "dayjs";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useAuth } from "../auth/AuthContext";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  Button,
  Collapse,
  Descriptions,
  Drawer,
  Input,
  message,
  Popconfirm,
  Select,
  Space,
  Spin,
  Table,
  Checkbox,
  Tabs,
  Tag,
  Typography,
} from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { Dropdown, Form, Modal } from "antd";
import type { StuckSinkPosition } from "../api/pipelines";
import type { MenuProps } from "antd";
import { WfIcon } from "../components/WorkflowPageHeader";
import { TreeHead } from "../components/TreeHead";
import { StdPage } from "../components/StdPage";
import { GptChatPanel } from "../components/gpt/GptChatPanel";
import { useGptEnabled } from "../components/gpt/useGptEnabled";
import { useNavigate, useSearchParams } from "react-router-dom";
import { listConnections } from "../api/connections";
import { listWorkflows, type WorkflowSummary } from "../api/workflows";
import {
  createPipelineGroup,
  deletePipelineGroup,
  listPipelineGroups,
  updatePipelineGroup,
  type PipelineGroupResponse,
} from "../api/pipelineGroups";
import {
  getPipelineDashboardSummary,
  getRealtimePipelineMetrics,
} from "../api/dashboard";
import {
  deletePipeline,
  checkPipelineConsistency,
  dismissConnectorDrift,
  refreshSinkConfig,
  getStuckSink,
  skipStuckRecord,
  getPipelineHistory,
  listPipelineRuntimeStatuses,
  listPipelineConsistencyChecks,
  listPipelines,
} from "../api/pipelines";
import type {
  PipelineCommandHistoryResponse,
  PipelineResponse,
} from "../types/pipeline";
import { loadModeLabel } from "../types/pipeline";
import {
  CDC_STATUS_LABEL,
  RUNTIME_STATUS_COLOR,
  RUNTIME_STATUS_LABEL,
  cdcPipelineColumns,
  renderRuntimeStatus,
} from "../utils/cdcPresentation";

const STATUS_OPTIONS = ["CREATED", "DEPLOYING", "READY", "DEPLOYED", "PAUSED", "STOPPED", "FAILED"].map((value) => ({
  value,
  label: CDC_STATUS_LABEL[value],
}));

const TYPE_OPTIONS = [
  { value: "TABLE_CDC", label: "TABLE_CDC" },
  { value: "LOG_FILE", label: "LOG_FILE" },
];

const STATUS_SEVERITY: Record<string, number> = {
  FAILED: 0,
  DEPLOYING: 1,
  STOPPED: 2,
  PAUSED: 3,
  CREATED: 4,
  READY: 5,
  DEPLOYED: 6,
};

/**
 * 제어 이력의 명령 이름. 코드 그대로 보여 주면 «PREPARE 가 뭔가»를 매번 물어야 한다.
 * 모르는 코드는 그대로 둔다(새 명령이 생겨도 화면이 비지 않게).
 */
const COMMAND_LABEL: Record<string, string> = {
  START: "시작",
  STOP: "중지",
  RESTART: "재시작",
  PREPARE: "사전 점검",
  DEPLOY: "배포",
  SINK_CONFIG_REFRESH: "싱크 설정 갱신",
  RUNTIME_MONITOR: "자동 감시",
  SKIP_RECORD: "건너뛰기(손실)",
};

export function PipelinesPage() {
  const navigate = useNavigate();
  // CDC 쓰기 권한이 없으면 생성·삭제를 막는다. 권한이 아직 안 실렸을 때는 사이드바와 같은
  // 규칙으로 fail-open 한다(잠깐 비활성으로 깜빡이지 않게). 실제 차단은 백엔드가 한다.
  const { can, permissionsLoaded } = useAuth();
  const canWrite = !permissionsLoaded || can("KAFKA", "WRITE");
  const queryClient = useQueryClient();
  const [detailPipelineId, setDetailPipelineId] = useState<number | null>(null);
  // 실행 현황에서 «이 파이프라인이 실패했다»를 누르면 바로 그 상세 창이 열려야 한다.
  // 목록에서 다시 찾게 하면 CDC 가 여럿일 때 어느 것이었는지 놓친다(2026-09-17).
  const [searchParams, setSearchParams] = useSearchParams();
  useEffect(() => {
    const requested = Number(searchParams.get("pipelineId"));
    if (!requested) {
      return;
    }
    setDetailPipelineId(requested);
    // 한 번 열고 나면 주소에서 지운다 - 남겨 두면 창을 닫아도 새로고침 때 또 열린다.
    searchParams.delete("pipelineId");
    setSearchParams(searchParams, { replace: true });
  }, [searchParams, setSearchParams]);
  // 선택된 LLM 이 없으면 상세 창에 GPT 탭을 두지 않는다(설계 5-1).
  const gptEnabled = useGptEnabled();
  const [nameFilter, setNameFilter] = useState("");
  const [topicFilter, setTopicFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState<string[]>([]);
  const [typeFilter, setTypeFilter] = useState<string[]>([]);
  const [treeSearch, setTreeSearch] = useState("");
  // 그룹 만들기·이름 고치기 모달. mode 로 무엇을 하는 중인지 구분한다.
  const [groupEdit, setGroupEdit] = useState<
    { mode: "create"; parentId: number | null } | { mode: "rename"; group: PipelineGroupResponse } | undefined
  >();
  const [groupForm] = Form.useForm<{ name: string }>();
  const [selectedTreeKey, setSelectedTreeKey] = useState<string>("all");
  // 확인 창을 hook 으로 연다 - 정적 Modal.confirm 은 화면 테마(StdPage)를 못 받아 파란 버튼 · 옛 모양으로 떴다.
  const [modal, modalHolder] = Modal.useModal();

  // 트리 머리 «전체 접기»(ETL 관리 트리와 같은 단추, 2026-10-01).
  const [treeCollapseSignal, setTreeCollapseSignal] = useState(0);
  const [treeExpandSignal, setTreeExpandSignal] = useState(0);
  const { data: pipelines, isLoading, isFetching: pipelinesFetching, refetch: refetchPipelines } = useQuery({
    queryKey: ["pipelines"],
    queryFn: listPipelines,
  });
  const { data: groups } = useQuery({
    queryKey: ["pipeline-groups"],
    queryFn: listPipelineGroups,
  });
  const groupById = useMemo(
    () => new Map((groups ?? []).map((group) => [group.id, group])),
    [groups],
  );
  const { data: runtimeStatuses } = useQuery({
    queryKey: ["pipeline-runtime-statuses"],
    queryFn: listPipelineRuntimeStatuses,
    refetchInterval: 10_000,
  });
  const runtimeByPipeline = useMemo(
    () => new Map((runtimeStatuses ?? []).map((runtime) => [runtime.pipelineId, runtime])),
    [runtimeStatuses],
  );
  const { data: connections } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });
  // «소속 워크플로우» 열(2026-10-10): CDC 는 워크플로우로만 제어한다(2026-10-01) - 어느 게시된 워크플로우에도
  // 없으면 실행할 수 없다. 예전에는 워크플로우 로그 화면의 CDC 가지만 그걸 알려 줬는데, 그 가지를 빼면서 여기로 옮겼다.
  const { data: workflows } = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  const workflowsByPipeline = useMemo(() => {
    const byPipeline = new Map<number, WorkflowSummary[]>();
    (workflows ?? []).forEach((workflow) => {
      new Set((workflow.cdcNodes ?? []).map((node) => node.pipelineId)).forEach((pipelineId) => {
        if (pipelineId != null) {
          byPipeline.set(pipelineId, [...(byPipeline.get(pipelineId) ?? []), workflow]);
        }
      });
    });
    return byPipeline;
  }, [workflows]);

  // metadata-db는 RUNNING이라고 알고 있는데 실제 Kafka Connect엔 없는 커넥터가 있는지
  // 주기적으로 확인한다 - 예전에 이걸 놓쳐서 데이터가 조용히 안 들어온 적이 있었다.
  const { data: dashboardSummary } = useQuery({
    queryKey: ["pipeline-dashboard-summary"],
    queryFn: getPipelineDashboardSummary,
    refetchInterval: 30_000,
  });
  const { data: realtimeMetrics } = useQuery({
    queryKey: ["realtime-pipeline-metrics"],
    queryFn: getRealtimePipelineMetrics,
    refetchInterval: 20_000,
  });
  const connectorDrift = dashboardSummary?.connectorDrift ?? [];
  const metricByPipeline = useMemo(
    () => new Map((realtimeMetrics ?? []).map((metric) => [metric.pipelineId, metric])),
    [realtimeMetrics],
  );

  // 자동 감시 기록(RUNTIME_MONITOR)은 기본으로 감춘다 - 30초 주기라 수천 건이다.
  const [historyIncludeAuto, setHistoryIncludeAuto] = useState(false);
  const { data: history, isLoading: historyLoading } = useQuery({
    queryKey: ["pipeline-history", detailPipelineId, historyIncludeAuto],
    queryFn: () => getPipelineHistory(detailPipelineId!, { includeAuto: historyIncludeAuto }),
    enabled: detailPipelineId !== null,
  });
  const detailPipeline = pipelines?.find((p) => p.id === detailPipelineId) ?? null;
  const { data: consistencyChecks, isLoading: consistencyLoading } = useQuery({
    queryKey: ["pipeline-consistency-checks", detailPipelineId],
    queryFn: () => listPipelineConsistencyChecks(detailPipelineId!),
    enabled: detailPipelineId !== null,
  });


  /** 고른 그룹과 그 하위 전부의 id. 트리 숫자와 목록이 같은 범위를 보게 한다. */
  const groupScope = useMemo(() => {
    const scope = new Set<number>();
    if (!selectedTreeKey.startsWith("group:")) return scope;
    let frontier = [Number(selectedTreeKey.split(":")[1])];
    while (frontier.length) {
      frontier.forEach((id) => scope.add(id));
      const parents = frontier;
      frontier = (groups ?? [])
        .filter((g) => g.parentId !== null && parents.includes(g.parentId) && !scope.has(g.id))
        .map((g) => g.id);
    }
    return scope;
  }, [selectedTreeKey, groups]);

  const filteredPipelines = useMemo(() => {
    const name = nameFilter.trim().toLowerCase();
    const topic = topicFilter.trim().toLowerCase();
    return (pipelines ?? []).filter((p) => {
      if (selectedTreeKey.startsWith("group:")) {
        // 그룹을 고르면 하위 그룹의 파이프라인까지 함께 본다(트리의 숫자와 같은 기준).
        if (!groupScope.has(p.groupId ?? -1)) return false;
      } else if (selectedTreeKey.startsWith("pipeline:")) {
        if (p.id !== Number(selectedTreeKey.split(":")[1])) return false;
      }
      if (name && !p.name.toLowerCase().includes(name)) {
        return false;
      }
      if (topic && !(p.topicName ?? "").toLowerCase().includes(topic)) {
        return false;
      }
      if (statusFilter.length > 0 && !statusFilter.includes(p.status)) {
        return false;
      }
      if (typeFilter.length > 0 && !typeFilter.includes(p.pipelineType)) {
        return false;
      }
      return true;
    }).sort((a, b) => {
      const severity = (STATUS_SEVERITY[a.status] ?? 99) - (STATUS_SEVERITY[b.status] ?? 99);
      if (severity !== 0) return severity;
      const aLag = metricByPipeline.get(a.id)?.consumerLag ?? -1;
      const bLag = metricByPipeline.get(b.id)?.consumerLag ?? -1;
      return bLag - aLag;
    });
  }, [pipelines, nameFilter, topicFilter, statusFilter, typeFilter, metricByPipeline, runtimeByPipeline, selectedTreeKey, groupScope]);

  /**
   * 관리 트리. 사용자가 만든 «그룹» 계층으로 세운다.
   *
   * <p>예전에는 소스 DB > 스키마로 자동으로 묶었다. 그건 파이프라인이 어디서 오는지일 뿐,
   * 운영자가 묶어서 보고 싶은 단위(업무·과제·담당)와 다르다. 그래서 ETL 이 NiFi 그룹으로
   * 폴더를 갖듯 CDC 도 자기 폴더를 갖는다. 소스·스키마는 목록 표와 상세에 그대로 있다.
   *
   * <p>최상단 «전체 파이프라인»은 서버에 행이 없는 가상 뿌리다. 그래서 여기서만 «하위 그룹
   * 생성»이 되고 수정·삭제는 없다. 그룹을 아직 하나도 안 만들었으면 파이프라인이 전부
   * 뿌리 바로 밑에 놓여 예전과 비슷하게 보인다.
   */
  const pipelineTree = useMemo((): PipelineTreeNode => {
    const search = treeSearch.trim().toLowerCase();
    const connectionNameById = new Map((connections ?? []).map((connection) => [connection.id, connection.name]));

    const matches = (pipeline: PipelineResponse) => !search
      || pipeline.name.toLowerCase().includes(search)
      || (pipeline.sourceTable ?? "").toLowerCase().includes(search)
      || (pipeline.sourceSchema ?? "").toLowerCase().includes(search)
      || (pipeline.targetTable ?? "").toLowerCase().includes(search)
      || (pipeline.sourceConnectionId != null
          && (connectionNameById.get(pipeline.sourceConnectionId) ?? "").toLowerCase().includes(search));

    const shown = (pipelines ?? []).filter(matches);
    const byGroup = new Map<number | null, PipelineResponse[]>();
    shown.forEach((pipeline) => {
      // 없는 그룹을 가리키는 파이프라인(그룹이 지워진 뒤 등)은 미지정으로 본다 - 트리에서
      // 사라지면 손댈 방법이 없어진다.
      const groupId = pipeline.groupId != null && groupById.has(pipeline.groupId) ? pipeline.groupId : null;
      byGroup.set(groupId, [...(byGroup.get(groupId) ?? []), pipeline]);
    });

    // 파이프라인 줄은 이름만 보인다(2026-10-10 요청) - 오른쪽 소스 테이블 태그(색 = 실행 상태)는 뺐다.
    // 검색은 그대로 소스 · 타깃 테이블 · 연결 이름으로도 걸린다.
    const pipelineNode = (pipeline: PipelineResponse): PipelineTreeNode => ({
      key: `pipeline:${pipeline.id}`,
      label: pipeline.name,
      kind: "pipeline",
      count: 0,
      children: [],
    });

    /** 그룹 한 칸과 그 아래(하위 그룹 + 그 그룹의 파이프라인). 숫자는 하위까지 합친 수다. */
    const groupNode = (group: PipelineGroupResponse): PipelineTreeNode => {
      const subs = (groups ?? []).filter((g) => g.parentId === group.id).map(groupNode);
      const own = byGroup.get(group.id) ?? [];
      return {
        key: `group:${group.id}`,
        label: group.name,
        kind: "group",
        count: own.length + subs.reduce((sum, sub) => sum + sub.count, 0),
        children: [...subs, ...own.map(pipelineNode)],
      };
    };

    const rootGroups = (groups ?? []).filter((g) => g.parentId === null).map(groupNode);
    const ungrouped = (byGroup.get(null) ?? []).map(pipelineNode);
    return {
      key: "all",
      // 최상단 이름은 ETL 트리(«ETL Root»)와 같은 꼴로 맞췄다(2026-10-08 요청).
      label: "CDC Root",
      kind: "root",
      count: (pipelines ?? []).length,
      children: [...rootGroups, ...ungrouped],
    };
  }, [pipelines, connections, groups, groupById, treeSearch]);

  /**
   * 트리 우클릭 메뉴.
   *
   * <p>최상단 «전체 파이프라인»은 서버에 행이 없는 가상 뿌리라 «하위 그룹 생성»만 둔다.
   * 그 아래 그룹부터 이름 수정·삭제가 붙는다. 파이프라인 줄에는 메뉴가 없다(파이프라인
   * 자체의 삭제·상세는 오른쪽 목록에서 한다).
   */
  /**
   * 파이프라인이 트리에서 놓인 자리를 «전체 파이프라인 > 상위 > 하위» 로 적는다.
   *
   * <p>그룹이 없으면 뿌리 바로 밑이므로 «전체 파이프라인»만 나온다. 그룹이 지워졌는데
   * 파이프라인이 그 id 를 아직 들고 있는 경우도 트리와 같게 미지정으로 본다.
   */
  const groupPathOf = (groupId: number | null) => {
    const names: string[] = [];
    let current = groupId != null ? groupById.get(groupId) : undefined;
    const guard = new Set<number>();          // 데이터가 꼬여 순환이 생겨도 멈춘다
    while (current && !guard.has(current.id)) {
      guard.add(current.id);
      names.unshift(current.name);
      current = current.parentId != null ? groupById.get(current.parentId) : undefined;
    }
    return ["전체 파이프라인", ...names].join(" > ");
  };

  const treeMenuItems = (key: string) => {
    if (!canWrite) return [];
    if (key === "all") {
      return [{ key: "create", label: "하위 그룹 생성" }];
    }
    if (key.startsWith("group:")) {
      return [
        { key: "create", label: "하위 그룹 생성" },
        { key: "rename", label: "그룹 수정" },
        { key: "delete", label: "그룹 삭제", danger: true },
      ];
    }
    return [];
  };

  const onTreeMenu = (key: string, action: string) => {
    const groupId = key.startsWith("group:") ? Number(key.split(":")[1]) : null;
    if (action === "create") {
      setGroupEdit({ mode: "create", parentId: groupId });
      groupForm.setFieldsValue({ name: "" });
      return;
    }
    const group = groupId != null ? groupById.get(groupId) : undefined;
    if (!group) return;
    if (action === "rename") {
      setGroupEdit({ mode: "rename", group });
      groupForm.setFieldsValue({ name: group.name });
      return;
    }
    if (action === "delete") {
      modal.confirm({
        title: `그룹 «${group.name}»을(를) 삭제할까요?`,
        content: "하위 그룹이나 파이프라인이 남아 있으면 삭제되지 않습니다. 먼저 옮겨주세요.",
        okText: "삭제",
        okButtonProps: { danger: true },
        cancelText: "취소",
        onOk: () => groupDeleteMutation.mutateAsync(group.id),
      });
    }
  };

  const invalidatePipelines = () => queryClient.invalidateQueries({ queryKey: ["pipelines"] });
  const invalidateGroups = () => queryClient.invalidateQueries({ queryKey: ["pipeline-groups"] });

  const groupMutation = useMutation({
    mutationFn: (input: { id?: number; parentId: number | null; name: string }) =>
      input.id === undefined
        ? createPipelineGroup({ parentId: input.parentId, name: input.name }).then(() => undefined)
        : updatePipelineGroup(input.id, { parentId: input.parentId, name: input.name }).then(() => undefined),
    onSuccess: () => {
      message.success("그룹을 저장했습니다.");
      setGroupEdit(undefined);
      groupForm.resetFields();
      void invalidateGroups();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const groupDeleteMutation = useMutation({
    mutationFn: deletePipelineGroup,
    onSuccess: () => {
      message.success("그룹을 삭제했습니다.");
      // 지운 그룹을 고른 채였다면 트리 선택이 허공을 가리키므로 최상단으로 돌린다.
      setSelectedTreeKey("all");
      void invalidateGroups();
      void invalidatePipelines();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const deleteMutation = useMutation({
    mutationFn: deletePipeline,
    onSuccess: () => {
      message.success("파이프라인을 삭제했습니다 (CDC 커넥터도 함께 정리됨).");
      invalidatePipelines();
    },
    onError: (error: Error) => message.error(error.message),
  });

  const consistencyMutation = useMutation({
    mutationFn: checkPipelineConsistency,
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ["pipeline-consistency-checks", result.pipelineId] });
      if (result.result === "MATCH") message.success("소스와 타깃의 통계 추정 행 수가 일치합니다.");
      else if (result.result === "MISMATCH") message.warning("소스와 타깃의 통계 추정 행 수가 다릅니다.");
      else message.info(result.message ?? "검증 결과를 확인하세요.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  const refreshSinkMutation = useMutation({
    mutationFn: refreshSinkConfig,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      message.success("싱크 설정을 다시 적용했습니다.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  const dismissDriftMutation = useMutation({
    mutationFn: dismissConnectorDrift,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["pipeline-dashboard-summary"] }),
    onError: (error: Error) => message.error(error.message),
  });

  const driftByPipeline = connectorDrift.reduce<Record<number, { pipelineName: string; connectorNames: string[] }>>(
    (acc, entry) => {
      const existing = acc[entry.pipelineId] ?? { pipelineName: entry.pipelineName, connectorNames: [] };
      existing.connectorNames.push(entry.connectorName);
      acc[entry.pipelineId] = existing;
      return acc;
    },
    {},
  );

  return (
    <StdPage menu="cdc" title="CDC 파이프라인" icon="database" fill>
      {modalHolder}
        {Object.entries(driftByPipeline).map(([pipelineId, info]) => (
          <Alert
            key={pipelineId}
            type="warning"
            showIcon
            closable
            onClose={() => dismissDriftMutation.mutate(Number(pipelineId))}
            style={{ marginBottom: 12 }}
            message="파이프라인 커넥터가 CDC에서 사라졌습니다"
            description={
              <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                <span>
                  <b>{info.pipelineName}</b> — {info.connectorNames.join(", ")} 없음
                  {/* 여기서 바로 재배포하지 않는다 - 배포/시작/중지는 워크플로우가 단독으로 지시한다
                      (2026-10-01 제어 DAG 폐지, 화면은 생성/삭제만 담당). 양쪽에서 가능하면
                      "누가 언제 실행시켰는지"가 이력에서 흐려진다. */}
                  <br />
                  <Typography.Text type="secondary">
                    복구하려면 이 파이프라인이 든 워크플로우를 «실행 설정»에서 이 파이프라인만 «배포»로 실행하세요.
                  </Typography.Text>
                </span>
              </div>
            }
          />
        ))}
        {/* 디자인 작업물 cdc-manage.html: 왼쪽 «파이프라인 탐색» 트리 카드 · 오른쪽 필터 + 표 카드. 두 카드는 화면 바닥까지 늘인다. */}
        <div className="cdc-manage-layout">
          <section className="std-card cdc-tree-card">
            {/* 카드 제목 줄(«파이프라인 탐색»)은 뺐다(2026-10-01 요청) - 다른 트리처럼 «트리 검색» 머리줄부터 시작한다. */}
            <TreeHead onRefresh={() => void refetchPipelines()} refreshing={pipelinesFetching}
                      refreshTitle="파이프라인 목록을 다시 불러옵니다"
                      onCollapseAll={() => setTreeCollapseSignal((n) => n + 1)}
                      onExpandAll={() => setTreeExpandSignal((n) => n + 1)} />
            <input
              className="nifi-tree-search-input"
              aria-label="트리 검색"
              value={treeSearch}
              onChange={(event) => setTreeSearch(event.target.value)}
              placeholder="이름·스키마·테이블"
            />
            <div className="wf-tree-scroll cdc-tree-scroll">
              <PipelineTreeView
                root={pipelineTree}
                selectedKey={selectedTreeKey}
                searching={Boolean(treeSearch.trim())}
                // 트리에서 고르면 목록(내역)만 그 범위로 좁힌다. 상세창(Drawer)은 '상세' 버튼으로만 연다.
                onSelect={setSelectedTreeKey}
                menuItemsOf={treeMenuItems}
                onMenu={onTreeMenu}
                collapseSignal={treeCollapseSignal}
                expandSignal={treeExpandSignal}
              />
            </div>
          </section>
          <section className="std-card std-table-card cdc-manage-main">
        <div className="std-filter-card cdc-manage-filter">
          <Space wrap>
            <Input
              placeholder="이름 검색"
              allowClear
              style={{ width: 200 }}
              value={nameFilter}
              onChange={(e) => setNameFilter(e.target.value)}
            />
            <Input
              placeholder="Topic 검색"
              allowClear
              style={{ width: 200 }}
              value={topicFilter}
              onChange={(e) => setTopicFilter(e.target.value)}
            />
            <Select
              mode="multiple"
              allowClear
              placeholder="상태"
              style={{ minWidth: 160 }}
              options={STATUS_OPTIONS}
              value={statusFilter}
              onChange={setStatusFilter}
            />
            <Select
              mode="multiple"
              allowClear
              placeholder="유형"
              style={{ minWidth: 140 }}
              options={TYPE_OPTIONS}
              value={typeFilter}
              onChange={setTypeFilter}
            />
          </Space>
          <Space wrap className="std-push-end">
            {/* 쓰기 권한이 없으면 버튼을 비활성이 아니라 아예 감춘다 - 사이드바에서 «생성»
                메뉴가 사라지는 것과 같은 규칙이라, 할 수 없는 동작을 화면에 남기지 않는다.
                (연결정보 부족처럼 «권한은 있는데 조건이 아직» 인 경우는 그대로 비활성+안내) */}
            {canWrite && (
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => navigate("/cdc/create")}
                disabled={!connections || connections.length < 1}
                title={!connections || connections.length < 1 ? "연결정보가 최소 1개는 있어야 합니다" : undefined}
              >
                파이프라인 신규 생성
              </Button>
            )}
          </Space>
        </div>

        <Table<PipelineResponse>
        rowKey="id"
        className="std-full-table"
        loading={isLoading}
        dataSource={filteredPipelines}
        scroll={{ x: 1120 }}
        pagination={{ pageSize: 20, hideOnSinglePage: true, showSizeChanger: true, showTotal: (total) => `총 ${total}건` }}
        columns={[
          // 이름은 굵게 한 줄로(디자인). 공용 열 정의(cdcPipelineColumns)는 로그 화면도 쓰므로 여기서만 바꾼다.
          ...cdcPipelineColumns(runtimeByPipeline, metricByPipeline).map((column) =>
            column.title === "이름"
              ? { ...column, width: 190, render: (value: string) => <strong className="cdc-manage-name">{value}</strong> }
              : column),
          {
            // 게시된 워크플로우 이름(누르면 워크플로우 로그에서 그 워크플로우를 연다 - 실행 설정이 거기 있다).
            // 게시 전 워크플로우에만 있으면 «게시 전», 어디에도 없으면 «없음 · 실행 불가».
            title: "소속 워크플로우",
            width: 150,
            sorter: (a, b) => (workflowsByPipeline.get(a.id)?.find((w) => w.published)?.name ?? "")
              .localeCompare(workflowsByPipeline.get(b.id)?.find((w) => w.published)?.name ?? ""),
            render: (_, record) => {
              const owners = workflowsByPipeline.get(record.id) ?? [];
              const published = owners.filter((workflow) => workflow.published);
              if (published.length > 0) {
                return (
                  <Space direction="vertical" size={0}>
                    {published.map((workflow) => (
                      <Typography.Link key={workflow.id} title="워크플로우 로그에서 열기"
                                       onClick={() => navigate(`/airflow/dashboard?dagId=${encodeURIComponent(workflow.dagId)}`)}>
                        {workflow.name}
                      </Typography.Link>
                    ))}
                  </Space>
                );
              }
              if (owners.length > 0) {
                return <Tag title={owners.map((workflow) => workflow.name).join(", ")}>게시 전</Tag>;
              }
              return <Tag color="warning" title="워크플로우에 CDC 노드로 넣고 게시해야 실행할 수 있습니다">없음 · 실행 불가</Tag>;
            },
          },
          {
            title: "관리",
            width: 130,
            render: (_, record) => (
              <Space wrap>
                <Button size="small" onClick={() => setDetailPipelineId(record.id)}>
                  상세
                </Button>
                <Popconfirm
                  title="이 파이프라인을 삭제할까요?"
                  description="배포된 CDC 커넥터도 함께 삭제됩니다."
                  onConfirm={() => deleteMutation.mutate(record.id)}
                  disabled={!canWrite}
                >
                  <Button
                    danger
                    size="small"
                    disabled={!canWrite}
                    title={!canWrite ? "CDC 쓰기 권한이 없습니다" : undefined}
                  >
                    삭제
                  </Button>
                </Popconfirm>
              </Space>
            ),
          },
        ]}
        />
          </section>
        </div>


      <Drawer
        title={detailPipeline ? `파이프라인 상세 · ${detailPipeline.name}` : "파이프라인 상세"}
        open={detailPipelineId !== null}
        onClose={() => setDetailPipelineId(null)}
        width={640}
        destroyOnHidden
        styles={{ body: { padding: 0 } }}
      >
        {detailPipeline && (
          // ETL 관리 화면 속성창과 같은 2단 구조(2026-09-17 요청): 위 «상세정보 | AI 어시스턴트» 탭,
          // 상세정보 안에 예전 탭(기본정보 · 운영 상태 · Connector · 정합성 · 이력)을 그대로 둔다.
          <CdcDetailShell
            key={detailPipeline.id}
            ai={gptEnabled ? (
              <GptChatPanel
                targetType="CDC_PIPELINE"
                target={{ pipelineId: String(detailPipeline.id) }}
                targetNoun="이 파이프라인"
              />
            ) : null}
          >
          <Tabs
            items={[
              {
                key: "info",
                label: "기본정보",
                children: (
                  <Descriptions column={1} bordered size="small">
                    <Descriptions.Item label="이름">{detailPipeline.name}</Descriptions.Item>
                    {/* 트리 어디에 놓여 있는지. 최상단(전체 파이프라인)부터 순서대로 보여준다. */}
                    <Descriptions.Item label="경로">{groupPathOf(detailPipeline.groupId)}</Descriptions.Item>
                    <Descriptions.Item label="유형">{detailPipeline.pipelineType}</Descriptions.Item>
                    <Descriptions.Item label="소스">
                      {detailPipeline.pipelineType === "LOG_FILE"
                        ? "Filebeat"
                        : `${detailPipeline.sourceDbType} · ${detailPipeline.sourceSchema}.${detailPipeline.sourceTable}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="타겟">
                      {`${detailPipeline.targetDbType} · ${detailPipeline.targetSchema}.${detailPipeline.targetTable}`}
                    </Descriptions.Item>
                    <Descriptions.Item label="Topic">{detailPipeline.topicName}</Descriptions.Item>
                    {detailPipeline.pipelineType === "TABLE_CDC" && (
                      <Descriptions.Item label="스냅샷 모드">
                        {detailPipeline.snapshotMode === "NO_DATA" ? "기존 데이터 미적재 · 이후 CDC" : "초기 적재 후 CDC"}
                      </Descriptions.Item>
                    )}
                    {detailPipeline.pipelineType === "TABLE_CDC" && (
                      <Descriptions.Item label="적재 방식">
                        {loadModeLabel(detailPipeline.loadMode, detailPipeline.deltaOpColumn)}
                      </Descriptions.Item>
                    )}
                    <Descriptions.Item label="상태">
                      {renderRuntimeStatus(detailPipeline, runtimeByPipeline.get(detailPipeline.id))}
                    </Descriptions.Item>
                    <Descriptions.Item label="설명">{detailPipeline.description ?? "-"}</Descriptions.Item>
                    <Descriptions.Item label="생성 시각">{detailPipeline.createdAt}</Descriptions.Item>
                    <Descriptions.Item label="수정 시각">{detailPipeline.updatedAt}</Descriptions.Item>
                  </Descriptions>
                ),
              },
              {
                key: "runtime",
                label: "운영 상태",
                children: (() => {
                  const runtime = runtimeByPipeline.get(detailPipeline.id);
                  if (!runtime) return <Spin size="small" />;
                  return (
                    <>
                      {runtime.statusMismatch && (
                        <Alert
                          type="warning"
                          showIcon
                          message="저장 상태와 CDC 실측 상태가 다릅니다"
                          description={runtime.runtimeStatusReason}
                          style={{ marginBottom: 16 }}
                        />
                      )}
                      <Descriptions column={1} bordered size="small">
                        <Descriptions.Item label="화면 실측 상태">
                          <Tag color={RUNTIME_STATUS_COLOR[runtime.runtimeStatus] ?? "default"}>
                            {RUNTIME_STATUS_LABEL[runtime.runtimeStatus] ?? runtime.runtimeStatus}
                          </Tag>
                        </Descriptions.Item>
                        <Descriptions.Item label="메타데이터 저장 상태">
                          {CDC_STATUS_LABEL[runtime.storedStatus] ?? runtime.storedStatus}
                        </Descriptions.Item>
                        <Descriptions.Item label="Source Connector">
                          {runtime.sourceConnectorState ?? "—"} · Tasks {runtime.sourceTaskStates.join(", ") || "—"}
                        </Descriptions.Item>
                        <Descriptions.Item label="Sink Connector">
                          {runtime.sinkConnectorState ?? "—"} · Tasks {runtime.sinkTaskStates.join(", ") || "—"}
                        </Descriptions.Item>
                        <Descriptions.Item label="마지막 실측 시각">
                          {runtime.runtimeCheckedAt ?? "확인 기록 없음"}
                        </Descriptions.Item>
                        <Descriptions.Item label="최근 제어 요청">
                          {runtime.lastCommand
                            ? `${runtime.lastCommand} · ${runtime.lastCommandResult ?? "처리 중"} · ${runtime.lastCommandAt ?? "—"}`
                            : "이력 없음"}
                        </Descriptions.Item>
                        <Descriptions.Item label="최근 제어 메시지">
                          {runtime.lastCommandMessage ?? "—"}
                        </Descriptions.Item>
                      </Descriptions>
                    </>
                  );
                })(),
              },
              {
                key: "connectors",
                label: "Connector",
                children: (
                  <>
                    <SinkErrorPolicy
                      pipeline={detailPipeline}
                      canWrite={canWrite}
                      loading={refreshSinkMutation.isPending}
                      onRefresh={() => refreshSinkMutation.mutate(detailPipeline.id)}
                    />
                    <SinkStuckRecord pipeline={detailPipeline} canWrite={canWrite} />
                    <Table
                      rowKey="id"
                      size="small"
                      pagination={false}
                      dataSource={detailPipeline.connectors}
                      style={{ marginBottom: 16 }}
                      columns={[
                        { title: "역할", dataIndex: "connectorRole" },
                        { title: "커넥터명", dataIndex: "connectorName" },
                        {
                          title: "상태",
                          dataIndex: "status",
                          render: (value: string) => <Tag>{value}</Tag>,
                        },
                      ]}
                    />
                    <Collapse
                      items={detailPipeline.connectors.map((c) => ({
                        key: c.id,
                        label: `${c.connectorName} · 설정/실행상태`,
                        children: (
                          <>
                            {/* Connector 설정/상태는 공백 없는 JSON 한 줄이라 pre-wrap 만으로는
                                안 접힌다(줄바꿈할 공백이 없다). break-word 로 강제 개행하고
                                길면 세로 스크롤을 준다. */}
                            <div style={{ fontWeight: 600, marginBottom: 4 }}>Connector 설정</div>
                            <pre className="connector-json-block">{c.connectorConfigJson}</pre>
                            <div style={{ fontWeight: 600, margin: "12px 0 4px" }}>실행상태</div>
                            <pre className="connector-json-block">
                              {c.lastStatusJson ?? "아직 조회된 상태가 없습니다."}
                            </pre>
                          </>
                        ),
                      }))}
                    />
                  </>
                ),
              },
              {
                key: "consistency",
                label: "정합성",
                children: (
                  <>
                    <Alert
                      type="info"
                      showIcon
                      message="부하 제한 통계 검증"
                      description="운영 테이블을 COUNT(*)로 전체 스캔하지 않고 DB 옵티마이저 통계의 추정 행 수를 비교합니다. 불일치 시 실제 누락 확정이 아니라 추가 점검이 필요하다는 뜻입니다."
                      style={{ marginBottom: 16 }}
                    />
                    <Button
                      type="primary"
                      loading={consistencyMutation.isPending}
                      // 정합성 검증은 POST 라 백엔드에서 KAFKA WRITE 로 판정된다.
                      // 막아두지 않으면 조회자가 눌렀을 때 403 오류만 보게 된다.
                      disabled={!canWrite || detailPipeline.pipelineType === "LOG_FILE"}
                      title={!canWrite ? "CDC 쓰기 권한이 없습니다" : undefined}
                      onClick={() => consistencyMutation.mutate(detailPipeline.id)}
                      style={{ marginBottom: 16 }}
                    >지금 검증</Button>
                    <Table
                      rowKey="id"
                      size="small"
                      loading={consistencyLoading}
                      dataSource={consistencyChecks}
                      pagination={false}
                      scroll={{ x: 650 }}
                      columns={[
                        { title: "검증 시각", dataIndex: "checkedAt", width: 170 },
                        { title: "소스 추정", dataIndex: "sourceCount", width: 110, render: (value: number | null) => value == null ? "—" : value.toLocaleString() },
                        { title: "타깃 추정", dataIndex: "targetCount", width: 110, render: (value: number | null) => value == null ? "—" : value.toLocaleString() },
                        { title: "차이", dataIndex: "difference", width: 90, render: (value: number | null) => value == null ? "—" : value.toLocaleString() },
                        { title: "결과", dataIndex: "result", width: 100, render: (value: string) => <Tag color={value === "MATCH" ? "success" : value === "MISMATCH" ? "warning" : "default"}>{value === "MATCH" ? "일치" : value === "MISMATCH" ? "불일치" : "확인 불가"}</Tag> },
                      ]}
                    />
                  </>
                ),
              },
              {
                key: "history",
                label: "제어 이력",
                children: (
                  <>
                    {/* 이 탭은 «누가 이 파이프라인에 무엇을 했는지»를 보는 자리다. 30초 주기 자동 감시는
                        기본으로 감춘다 - 수천 건이라 켜 두면 실제 조치(시작 · 중지 · 배포 · 건너뛰기)가 묻힌다. */}
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                      <span style={{ color: "#888", fontSize: 12 }}>
                        사람이 한 제어를 최근 것부터 보여 줍니다. «건너뛰기»는 그 건이 타깃에 <b>들어가지 않은</b> 기록입니다.
                      </span>
                      <Checkbox checked={historyIncludeAuto} onChange={(e) => setHistoryIncludeAuto(e.target.checked)}>
                        자동 감시 기록 포함
                      </Checkbox>
                    </div>
                    <Table<PipelineCommandHistoryResponse>
                      rowKey="id"
                      size="small"
                      loading={historyLoading}
                      dataSource={history}
                      pagination={{ pageSize: 10, hideOnSinglePage: true, showTotal: (total) => `총 ${total}건` }}
                      rowClassName={(row) => (row.command === "SKIP_RECORD" ? "cdc-history-skip" : "")}
                      expandable={{
                        rowExpandable: (row) => Boolean(row.message),
                        expandedRowRender: (row) => (
                          <pre style={{ whiteSpace: "pre-wrap", margin: 0, fontSize: 12 }}>{row.message}</pre>
                        ),
                      }}
                      columns={[
                        {
                          title: "명령",
                          dataIndex: "command",
                          width: 120,
                          render: (value: string) =>
                            value === "SKIP_RECORD"
                              ? <Tag color="error">건너뛰기(손실)</Tag>
                              : <span>{COMMAND_LABEL[value] ?? value}</span>,
                        },
                        {
                          title: "결과",
                          dataIndex: "result",
                          width: 80,
                          render: (value: string | null) =>
                            value ? <Tag color={value === "SUCCESS" ? "success" : "error"}>{value}</Tag> : "-",
                        },
                        { title: "요청자", dataIndex: "requestedBy", width: 90, render: (v: string | null) => v ?? "시스템" },
                        // 메시지는 남은 폭을 다 쓴다(서랍이 좁아 열을 나눠 주면 «메.» 처럼 제목까지 잘린다).
                        // 전체 내용은 줄을 펼치면 보인다.
                        { title: "메시지", dataIndex: "message", ellipsis: true, render: (v: string | null) => v ?? "-" },
                        {
                          title: "요청 시각",
                          dataIndex: "requestedAt",
                          width: 140,
                          // 저장된 값은 앱과 같은 시간대(KST)의 LocalDateTime 이다. 초까지만 보여 준다.
                          render: (v: string) => (v ? dayjs(v).format("YYYY-MM-DD HH:mm:ss") : "-"),
                        },
                      ]}
                    />
                  </>
                ),
              },
              // 속성창 GPT 탭(docs/2026-09-13-gpt-settings-design.md 5-2)은 위 «AI 어시스턴트» 탭으로 옮겼다.
              // 창을 닫거나 다른 파이프라인을 열면 대화가 사라지는 것은 같다(CdcDetailShell key).
            ]}
          />
          </CdcDetailShell>
        )}
      </Drawer>

      {/* 그룹 만들기·이름 고치기. 위치(부모)는 우클릭한 자리로 정해지므로 이름만 받는다. */}
      <Modal
        open={Boolean(groupEdit)}
        title={groupEdit?.mode === "rename" ? "그룹 수정" : "하위 그룹 생성"}
        okText="저장"
        cancelText="취소"
        confirmLoading={groupMutation.isPending}
        onCancel={() => { setGroupEdit(undefined); groupForm.resetFields(); }}
        onOk={() => {
          void groupForm.validateFields().then((values) => {
            if (!groupEdit) return;
            groupMutation.mutate(groupEdit.mode === "create"
              ? { parentId: groupEdit.parentId, name: values.name }
              // 이름만 고칠 때도 현재 부모를 그대로 실어 보낸다. 안 보내면 서버가
              // «최상단으로 옮긴다»로 읽어 그룹이 트리 꼭대기로 튀어 오른다.
              : { id: groupEdit.group.id, parentId: groupEdit.group.parentId, name: values.name });
          });
        }}
      >
        <Form form={groupForm} layout="vertical">
          <Form.Item
            name="name"
            label="그룹 이름"
            rules={[
              { required: true, message: "그룹 이름을 입력해주세요." },
              { max: 100, message: "100자를 넘을 수 없습니다." },
            ]}
          >
            <Input placeholder="예: 주문업무" autoFocus />
          </Form.Item>
        </Form>
      </Modal>
    </StdPage>
  );
}

/**
 * CDC 파이프라인 상세 창의 윗단 탭 — ETL 관리 화면 속성창(nifi-detail-tabs)과 같은 모양 · 이름.
 * AI 어시스턴트는 선택된 GPT 가 있을 때만 보인다(예전 GPT 탭과 같은 조건).
 */
function CdcDetailShell({ ai, children }: { ai: ReactNode; children: ReactNode }) {
  const [tab, setTab] = useState<"detail" | "ai">("detail");
  const current = ai ? tab : "detail";
  return (
    <div className={`cdc-detail-shell${current === "ai" ? " is-ai" : ""}`}>
      <div className="nifi-detail-tabs" role="tablist" aria-label="상세 패널 탭">
        <button type="button" role="tab" aria-selected={current === "detail"}
                className={`nifi-detail-tab${current === "detail" ? " active" : ""}`} onClick={() => setTab("detail")}>
          상세정보
        </button>
        {ai ? (
          <button type="button" role="tab" aria-selected={current === "ai"}
                  className={`nifi-detail-tab${current === "ai" ? " active" : ""}`} onClick={() => setTab("ai")}>
            AI 어시스턴트
          </button>
        ) : null}
      </div>
      {current === "ai" ? ai : <div className="cdc-detail-body">{children}</div>}
    </div>
  );
}

/**
 * 싱크 커넥터의 실패 정책(errors.tolerance)과 재적용 버튼(2026-09-17 실패 정책 전환).
 *
 * <p>CDC 테이블 싱크는 한 건이라도 거부되면 멈추는 것(none)이 기준이다. 전환 전에 준비한 커넥터는 설정이 등록 시점에 굳어
 * «버리고 계속»(all)으로 남아 있으므로, 여기서 경고하고 싱크 설정만 다시 넣게 한다. 로그 파이프라인은 거부된 줄을 건너뛰는
 * 것(all)이 기준이다. 결정 경위: docs/2026-09-09-cdc-error-policy-dlq.md
 */
function SinkErrorPolicy({ pipeline, canWrite, loading, onRefresh }: {
  pipeline: PipelineResponse;
  canWrite: boolean;
  loading: boolean;
  onRefresh: () => void;
}) {
  const sink = pipeline.connectors.find((connector) => connector.connectorRole === "SINK");
  if (!sink) return null;
  let tolerance: string | undefined;
  let hasDlq = false;
  try {
    const config = JSON.parse(sink.connectorConfigJson ?? "{}") as Record<string, unknown>;
    tolerance = typeof config["errors.tolerance"] === "string" ? config["errors.tolerance"] : undefined;
    hasDlq = Object.keys(config).some((key) => key.startsWith("errors.deadletterqueue."));
  } catch {
    tolerance = undefined;
  }
  const isLog = pipeline.pipelineType === "LOG_FILE";
  const expected = isLog ? "all" : "none";
  const outdated = tolerance !== expected || hasDlq;
  const label = (value?: string) => (value === "none" ? "멈춤" : value === "all" ? "건너뛰고 계속" : "기본값(멈춤)");
  return (
    <Alert
      type={outdated ? "warning" : "info"}
      showIcon
      style={{ marginBottom: 16 }}
      message={`싱크 실패 정책: ${label(tolerance)}${hasDlq ? " + DLQ" : ""}`}
      description={
        <>
          {isLog
            ? "로그 파이프라인은 거부된 줄을 건너뛰고 계속 적재합니다(건너뛴 줄은 Kafka Connect 로그에 남습니다)."
            : "CDC 테이블 싱크는 한 건이라도 거부되면 멈춥니다. 원인을 고치고 재시작하면 그 건부터 다시 들어옵니다."}
          {outdated && (
            <div style={{ marginTop: 6 }}>
              이 커넥터는 이전 정책으로 등록돼 있습니다. «싱크 설정 재적용»으로 싱크 설정만 새로 넣을 수 있습니다
              (소스는 그대로, 실행 중이면 싱크 태스크만 재시작되어 커밋된 위치부터 이어 읽습니다).
            </div>
          )}
        </>
      }
      action={
        <Popconfirm
          title="싱크 설정을 다시 적용할까요?"
          description="실행 중인 싱크는 태스크가 잠깐 재시작됩니다. 소스 커넥터는 건드리지 않습니다."
          okText="적용"
          cancelText="취소"
          onConfirm={onRefresh}
          disabled={!canWrite}
        >
          <Button size="small" loading={loading} disabled={!canWrite}
                  title={!canWrite ? "CDC 쓰기 권한이 없습니다" : undefined}>
            싱크 설정 재적용
          </Button>
        </Popconfirm>
      }
    />
  );
}

/**
 * 멈춘 싱크의 «멈춘 한 건 건너뛰기»(2026-09-17). 싱크는 한 건이라도 거부되면 멈추는데, 원본을 고칠 수 없는 건(깨진 메시지 등)은
 * 재시작해도 같은 자리에서 또 멈춘다. 그 한 건만 넘기고 다시 띄운다.
 *
 * <p><b>넘긴 건은 타깃에 들어가지 않는다</b> - 그래서 넘기기 전에 무엇을 넘기는지(내용·원인) 보여 주고 사유를 받는다.
 * 저장된 실행 상태에 FAILED 태스크가 있을 때만 Kafka 를 조회한다(정상 파이프라인에서 괜히 브로커를 부르지 않게).
 */
function SinkStuckRecord({ pipeline, canWrite }: { pipeline: PipelineResponse; canWrite: boolean }) {
  const queryClient = useQueryClient();
  const [reason, setReason] = useState("");
  const [target, setTarget] = useState<StuckSinkPosition>();

  const sink = pipeline.connectors.find((connector) => connector.connectorRole === "SINK");
  const sinkLooksFailed = (() => {
    try {
      const status = JSON.parse(sink?.lastStatusJson ?? "{}") as { tasks?: Array<{ state?: string }> };
      return (status.tasks ?? []).some((task) => task.state === "FAILED");
    } catch {
      return false;
    }
  })();

  const stuckQuery = useQuery({
    queryKey: ["pipeline-stuck-sink", pipeline.id],
    queryFn: () => getStuckSink(pipeline.id),
    enabled: sinkLooksFailed,
    retry: false,
  });

  const skipMutation = useMutation({
    mutationFn: (input: { topic: string; partition: number; offset: number; reason: string }) =>
      skipStuckRecord(pipeline.id, input),
    onSuccess: () => {
      setTarget(undefined);
      setReason("");
      queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      queryClient.invalidateQueries({ queryKey: ["pipeline-stuck-sink", pipeline.id] });
      queryClient.invalidateQueries({ queryKey: ["pipeline-history", pipeline.id] });
      message.success("멈춰 있던 한 건을 건너뛰고 싱크를 다시 띄웠습니다.");
    },
    onError: (error: Error) => message.error(error.message),
  });

  if (!sinkLooksFailed) return null;
  if (stuckQuery.isLoading) return <Alert type="info" showIcon message="멈춘 위치를 확인하는 중입니다." style={{ marginBottom: 16 }} />;
  if (stuckQuery.error) {
    return <Alert type="warning" showIcon style={{ marginBottom: 16 }}
                  message="멈춘 위치를 확인하지 못했습니다" description={(stuckQuery.error as Error).message} />;
  }
  const stuck = stuckQuery.data;
  if (!stuck?.stopped) return null;

  return (
    <>
      <Alert
        type="error"
        showIcon
        style={{ marginBottom: 16 }}
        message="싱크가 멈춰 있습니다"
        description={
          <>
            <div>
              원인을 고칠 수 있으면 고친 뒤 <strong>워크플로우 › 로그 → 이 파이프라인 우클릭 → «실행 설정» → 이 파이프라인 «재시작»</strong> 을 하십시오.
              멈춘 건부터 다시 들어옵니다. 원본을 고칠 수 없는 건(깨진 메시지 등)만 아래에서 건너뜁니다.
            </div>
            {stuck.failureTraces.length > 0 && (
              <Collapse
                size="small"
                style={{ marginTop: 8 }}
                items={[{
                  key: "trace",
                  label: "멈춘 원인",
                  children: <pre className="connector-json-block">{stuck.failureTraces.join("\n\n")}</pre>,
                }]}
              />
            )}
            {stuck.positions.length === 0 && (
              <div style={{ marginTop: 8 }}>아직 읽지 않은 메시지가 없습니다 - 멈춘 원인만 고치고 재시작하면 됩니다.</div>
            )}
            {stuck.positions.map((position) => (
              <div key={`${position.topic}-${position.partition}`} style={{ marginTop: 10 }}>
                <div>
                  <strong>{position.topic} · 파티션 {position.partition}</strong> · 멈춘 위치 {position.committedOffset.toLocaleString()}
                  {" · "}밀린 건수 {position.lag.toLocaleString()}
                </div>
                {position.readError
                  ? <div style={{ color: "#b45309" }}>{position.readError}</div>
                  : (
                    <pre className="connector-json-block" style={{ marginTop: 4 }}>
                      {position.keyPreview ? `키: ${position.keyPreview}\n` : ""}{position.preview}
                    </pre>
                  )}
                <Button danger size="small" disabled={!canWrite}
                        title={!canWrite ? "CDC 쓰기 권한이 없습니다" : undefined}
                        onClick={() => { setTarget(position); setReason(""); }}>
                  이 한 건 건너뛰기
                </Button>
              </div>
            ))}
          </>
        }
      />
      <Modal
        open={Boolean(target)}
        title="멈춰 있는 한 건을 건너뜁니다"
        okText="건너뛰기"
        okButtonProps={{ danger: true, disabled: !reason.trim() }}
        cancelText="취소"
        confirmLoading={skipMutation.isPending}
        onCancel={() => setTarget(undefined)}
        onOk={() => target && skipMutation.mutate({
          topic: target.topic, partition: target.partition, offset: target.committedOffset, reason: reason.trim(),
        })}
      >
        <Alert type="warning" showIcon style={{ marginBottom: 12 }}
               message="이 건은 타깃에 들어가지 않습니다"
               description="Kafka 의 읽은 위치를 한 칸 넘겨 다음 건부터 이어 갑니다. 되돌릴 수 없고, 필요하면 원본을 확인해 수동으로 넣어야 합니다." />
        <div style={{ marginBottom: 8 }}>
          {target?.topic} · 파티션 {target?.partition} · 오프셋 {target?.committedOffset.toLocaleString()}
        </div>
        <Input.TextArea
          value={reason}
          onChange={(event) => setReason(event.target.value)}
          rows={3}
          maxLength={500}
          placeholder="건너뛰는 사유 (이력에 남습니다) - 예: 역직렬화 불가한 깨진 메시지, 원본 확인함"
        />
      </Modal>
    </>
  );
}

/** CDC 관리 트리 한 칸. 뿌리(전체 파이프라인)·그룹·파이프라인 셋 중 하나다. */
interface PipelineTreeNode {
  key: string;
  label: string;
  kind: "root" | "group" | "pipeline";
  /** 뿌리·그룹 줄 배지. 그룹은 하위 그룹까지 합친 수(검색에 걸린 것만), 뿌리는 전체 수다. */
  count: number;
  children: PipelineTreeNode[];
}

/**
 * CDC 관리 트리. 워크플로우 관리 화면 트리와 같은 마크업(tree-node · tree-item-row · ▶ · 폴더/파일 아이콘 ·
 * 개수 배지 · tree-sub-group 점선)을 쓰고, 감싸는 칸도 같은 클래스(.wf-tree-scroll)라 줄 모양 CSS 가 그대로
 * 적용된다(2026-09-11 요청). antd Tree 로는 그 화살표·점선·줄 간격이 나오지 않았다.
 *
 * <p>동작은 antd Tree 때와 같다.
 * <ul>
 *   <li>줄을 누르면 오른쪽 목록이 그 칸 범위로 좁혀지고, 고른 줄을 다시 누르면 전체로 돌아간다.</li>
 *   <li>▶ 는 접기/펼치기만 한다 - 누른다고 목록이 바뀌지 않는다.</li>
 *   <li>처음엔 전부 펼쳐 둔다. 검색 중에는 걸린 파이프라인이 보여야 하므로 접어 둔 칸도 펼쳐 보인다.</li>
 *   <li>뿌리·그룹 줄에는 우클릭 메뉴(하위 그룹 생성·그룹 수정·그룹 삭제)가 그대로 붙는다.</li>
 * </ul>
 */
function PipelineTreeView({
  root,
  selectedKey,
  searching,
  onSelect,
  menuItemsOf,
  onMenu,
  collapseSignal = 0,
  expandSignal = 0,
}: {
  root: PipelineTreeNode;
  selectedKey: string;
  searching: boolean;
  onSelect: (key: string) => void;
  menuItemsOf: (key: string) => MenuProps["items"];
  onMenu: (key: string, action: string) => void;
  /** 값이 바뀌면 전부 접는다(트리 머리의 «전체 접기», 2026-10-01). */
  collapseSignal?: number;
  /** 값이 바뀌면 전부 연다(«전체 열기»). */
  expandSignal?: number;
}) {
  // 접어 둔 칸만 기억한다 - 새로 생긴 그룹은 따로 챙기지 않아도 펼친 채로 나온다.
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());

  useEffect(() => {
    if (collapseSignal === 0) {
      return;
    }
    // 하위가 있는 칸을 모두 접는다 - 접힌 칸 속은 그려지지 않으므로 자료에서 바로 모은다.
    const keys = new Set<string>();
    const walk = (node: PipelineTreeNode) => {
      if (node.children.length > 0) {
        keys.add(node.key);
        node.children.forEach(walk);
      }
    };
    walk(root);
    setCollapsed(keys);
    // root 는 자료가 바뀔 때마다 새로 만들어진다 - 단추를 누른 그 순간의 것만 쓴다.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collapseSignal]);

  useEffect(() => {
    if (expandSignal > 0) {
      setCollapsed(new Set());
    }
  }, [expandSignal]);

  const toggle = (key: string) => {
    setCollapsed((current) => {
      const next = new Set(current);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const renderNode = (node: PipelineTreeNode): ReactNode => {
    const hasChildren = node.children.length > 0;
    const open = hasChildren && (searching || !collapsed.has(node.key));
    const active = selectedKey === node.key;
    // antd Tree 와 같게 고른 줄을 다시 누르면 해제된다 - 해제는 곧 «전체 파이프라인»이다.
    const select = () => onSelect(active ? "all" : node.key);
    const items = menuItemsOf(node.key) ?? [];
    const row = (
      <div
        className={active ? "tree-item-row active" : "tree-item-row"}
        tabIndex={0}
        title={node.label}
        onClick={select}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            select();
          }
        }}
      >
        <button
          type="button"
          className={`tree-toggle-arrow${hasChildren ? (open ? " open" : "") : " empty"}`}
          tabIndex={hasChildren ? 0 : -1}
          aria-label={hasChildren ? (open ? "하위 접기" : "하위 펼치기") : undefined}
          onClick={(event) => {
            // 접기만 한다 - 줄 선택까지 같이 되면 접으려다 목록이 바뀐다.
            event.stopPropagation();
            if (hasChildren) toggle(node.key);
          }}
          onKeyDown={(event) => event.stopPropagation()}
        >
          ▶
        </button>
        <WfIcon name={node.kind === "pipeline" ? "file" : "folder"} size={15} className="tree-node-icon" />
        <span className="tree-node-label">{node.label}</span>
        {node.kind === "pipeline" ? null : <span className="tree-node-badge">{node.count}</span>}
      </div>
    );
    return (
      <div key={node.key} className="tree-node" role="treeitem" aria-selected={active}
           aria-expanded={hasChildren ? open : undefined}>
        {items.length > 0 ? (
          <Dropdown trigger={["contextMenu"]} menu={{ items, onClick: ({ key: action }) => onMenu(node.key, action) }}>
            {row}
          </Dropdown>
        ) : row}
        {open ? (
          <div className="tree-sub-group" role="group">
            {node.children.map(renderNode)}
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <div role="tree" aria-label="파이프라인 그룹" className="tree-root">
      {renderNode(root)}
    </div>
  );
}

import { useMemo, useState } from "react";
import { useAuth } from "../auth/AuthContext";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  Button,
  Card,
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
  Tabs,
  Tag,
  Tree,
  Typography,
} from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { Dropdown, Form, Modal } from "antd";
import type { TreeProps } from "antd";
import { useNavigate } from "react-router-dom";
import { listConnections } from "../api/connections";
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

export function PipelinesPage() {
  const navigate = useNavigate();
  // CDC 쓰기 권한이 없으면 생성·삭제를 막는다. 권한이 아직 안 실렸을 때는 사이드바와 같은
  // 규칙으로 fail-open 한다(잠깐 비활성으로 깜빡이지 않게). 실제 차단은 백엔드가 한다.
  const { can, permissionsLoaded } = useAuth();
  const canWrite = !permissionsLoaded || can("KAFKA", "WRITE");
  const queryClient = useQueryClient();
  const [detailPipelineId, setDetailPipelineId] = useState<number | null>(null);
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

  const { data: pipelines, isLoading } = useQuery({
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

  const { data: history, isLoading: historyLoading } = useQuery({
    queryKey: ["pipeline-history", detailPipelineId],
    queryFn: () => getPipelineHistory(detailPipelineId!),
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
  const pipelineTreeData = useMemo(() => {
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

    const pipelineNode = (pipeline: PipelineResponse) => ({
      key: `pipeline:${pipeline.id}`,
      title: (
        <Space size={4}>
          <span>{pipeline.name}</span>
          <Tag color={RUNTIME_STATUS_COLOR[runtimeByPipeline.get(pipeline.id)?.runtimeStatus ?? ""] ?? "default"}>
            {pipeline.sourceTable ?? pipeline.targetTable}
          </Tag>
        </Space>
      ),
      isLeaf: true,
    });

    /** 그룹 한 칸과 그 아래(하위 그룹 + 그 그룹의 파이프라인). 숫자는 하위까지 합친 수다. */
    const groupNode = (group: PipelineGroupResponse): { node: NonNullable<TreeProps["treeData"]>[number]; count: number } => {
      const subs = (groups ?? []).filter((g) => g.parentId === group.id).map(groupNode);
      const own = byGroup.get(group.id) ?? [];
      const count = own.length + subs.reduce((sum, sub) => sum + sub.count, 0);
      return {
        count,
        node: {
          key: `group:${group.id}`,
          title: <Space size={4}><span>{group.name}</span><Tag>{count}</Tag></Space>,
          children: [...subs.map((sub) => sub.node), ...own.map(pipelineNode)],
        },
      };
    };

    const rootGroups = (groups ?? []).filter((g) => g.parentId === null).map(groupNode);
    const ungrouped = (byGroup.get(null) ?? []).map(pipelineNode);
    const children: NonNullable<TreeProps["treeData"]> = [
      ...rootGroups.map((root) => root.node),
      ...ungrouped,
    ];
    const visibleCount = (pipelines ?? []).length;
    return [{ key: "all", title: <Space size={4}><span>전체 파이프라인</span><Tag>{visibleCount}</Tag></Space>, children }];
  }, [pipelines, connections, groups, groupById, treeSearch, runtimeByPipeline]);

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
      Modal.confirm({
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
    <div>
      <Card title="CDC 파이프라인">
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
                  {/* 여기서 바로 재배포하지 않는다 - 배포/시작/중지는 Airflow 제어 DAG가
                      단독으로 지시한다(화면은 생성/삭제만 담당). 화면과 DAG 양쪽에서
                      배포가 가능하면 "누가 언제 실행시켰는지"가 이력에서 흐려진다. */}
                  <br />
                  <Typography.Text type="secondary">
                    복구하려면 Airflow에서 이 파이프라인의 제어 DAG를 action=deploy로 실행하세요.
                  </Typography.Text>
                </span>
              </div>
            }
          />
        ))}
        <div style={{ display: "grid", gridTemplateColumns: "290px minmax(0, 1fr)", gap: 16, alignItems: "start" }}>
          <Card size="small" title="파이프라인 탐색" styles={{ body: { padding: 12, overflowX: "auto" } }}>
            <Input.Search
              allowClear
              placeholder="이름·스키마·테이블 검색"
              value={treeSearch}
              onChange={(event) => setTreeSearch(event.target.value)}
              style={{ marginBottom: 12 }}
            />
            <Tree
              blockNode
              // treeData 가 조회 후에 채워지므로 defaultExpandAll 만으로는 첫 렌더(빈 트리)에
              // 걸려 아무것도 안 펼쳐진다. 데이터가 들어오면 key 가 바뀌어 다시 마운트되게 한다.
              key={`pipeline-tree-${(pipelines ?? []).length}-${(groups ?? []).length}`}
              defaultExpandAll
              autoExpandParent={Boolean(treeSearch)}
              treeData={pipelineTreeData}
              selectedKeys={[selectedTreeKey]}
              onSelect={(keys) => {
                // 트리에서 파이프라인을 선택하면 목록(내역)만 그 파이프라인으로 좁힌다.
                // 상세창(Drawer)은 '상세' 버튼으로만 열도록 하여 선택만으로 열리지 않게 한다.
                const key = String(keys[0] ?? "all");
                setSelectedTreeKey(key);
              }}
              titleRender={(node) => {
                const key = String((node as { key: React.Key }).key);
                const title = (node as { title: React.ReactNode }).title;
                const items = treeMenuItems(key);
                // 파이프라인 줄에는 메뉴가 없다 - 그룹만 만들고 고치고 지운다.
                if (!items.length) {
                  return <span>{title}</span>;
                }
                return (
                  <Dropdown trigger={["contextMenu"]} menu={{ items, onClick: ({ key: action }) => onTreeMenu(key, action) }}>
                    <span>{title}</span>
                  </Dropdown>
                );
              }}
            />
          </Card>
          <div style={{ minWidth: 0 }}>
        <div style={{ display: "flex", flexWrap: "wrap", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16, gap: 12 }}>
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
          <Space wrap>
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
        loading={isLoading}
        dataSource={filteredPipelines}
        scroll={{ x: 1120 }}
        pagination={{ pageSize: 20, hideOnSinglePage: true, showSizeChanger: true, showTotal: (total) => `전체 ${total}건` }}
        columns={[
          ...cdcPipelineColumns(runtimeByPipeline, metricByPipeline),
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
          </div>
        </div>
      </Card>


      <Drawer
        title={detailPipeline ? `파이프라인 상세 · ${detailPipeline.name}` : "파이프라인 상세"}
        open={detailPipelineId !== null}
        onClose={() => setDetailPipelineId(null)}
        width={640}
        destroyOnHidden
      >
        {detailPipeline && (
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
                label: "이력",
                children: (
                  <Table<PipelineCommandHistoryResponse>
                    rowKey="id"
                    size="small"
                    loading={historyLoading}
                    dataSource={history}
                    pagination={false}
                    columns={[
                      { title: "명령", dataIndex: "command" },
                      {
                        title: "결과",
                        dataIndex: "result",
                        render: (value: string | null) =>
                          value ? <Tag color={value === "SUCCESS" ? "success" : "error"}>{value}</Tag> : "-",
                      },
                      { title: "메시지", dataIndex: "message", ellipsis: true },
                      { title: "요청 시각", dataIndex: "requestedAt" },
                    ]}
                  />
                ),
              },
            ]}
          />
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
    </div>
  );
}

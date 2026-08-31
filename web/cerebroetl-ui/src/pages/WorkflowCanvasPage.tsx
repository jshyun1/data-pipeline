import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addEdge,
  Background,
  Handle,
  Position,
  Controls,
  MiniMap,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  Alert,
  Button,
  Card,
  Empty,
  Input,
  InputNumber,
  message,
  Radio,
  Select,
  Space,
  Spin,
  Tree,
  Tag,
  Typography,
} from "antd";
import { ArrowLeftOutlined, SyncOutlined } from "@ant-design/icons";
import { useAuth } from "../auth/AuthContext";
import { listEtlJobs, type EtlJobResponse } from "../api/etlJobs";
import { getNifiProcessGroupTree, type NifiProcessGroupTreeNode } from "../api/platform";
import {
  nextPresetRuns,
  parseCronToPreset,
  presetCron,
  presetDescription,
  validCron,
  WEEKDAY_LABELS,
  type SchedulePreset,
} from "../utils/schedulePreset";
import {
  getWorkflow,
  listWorkflows,
  publishWorkflow,
  saveWorkflowGraph,
  unpublishWorkflow,
  updateWorkflow,
  validateWorkflow,
  type ValidationResult,
  type WorkflowDetail,
  type WorkflowSummary,
} from "../api/workflows";

// 워크플로우 캔버스. ETL(NiFi)에서 만들어 둔 job을 끌어와 실행 순서를 그린다.
//
// NiFi 캔버스에는 job 하나만 그리고 job끼리 잇지 않는다 - 순서는 여기가 유일한 원장이고,
// 게시하면 그대로 Airflow DAG로 컴파일된다(워크플로우 1개 = DAG 1개).
//
// 저장과 게시를 나눈 이유: 그리다 만 그래프가 곧바로 운영 DAG가 되면 위험하다.

const CONDITION_LABEL: Record<string, string> = {
  SUCCESS: "성공 시",
  FAILURE: "실패 시",
  ALWAYS: "완료 시",
};

/**
 * 선 위에 띄울 글자. 성공 시는 모든 선의 기본값이라 전부 붙으면 캔버스만 어지럽다.
 * 예외 경로(실패 시·완료 시)만 글자로 알린다 - 색만으로는 구분이 약하다.
 */
function edgeLabel(condition: string): string | undefined {
  return condition === "SUCCESS" ? undefined : (CONDITION_LABEL[condition] ?? condition);
}

const CONDITION_COLOR: Record<string, string> = {
  SUCCESS: "#94a3b8",
  FAILURE: "#ef4444",
  ALWAYS: "#0ea5e9",
};

/** 노드 키는 TaskGroup id가 되므로 Airflow가 받아들이는 문자만 남긴다. */
function toNodeKey(jobName: string, taken: Set<string>): string {
  const base = jobName.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "") || "job";
  let key = base;
  let index = 2;
  while (taken.has(key)) {
    key = `${base}_${index++}`;
  }
  return key;
}

export function WorkflowCanvasPage() {
  const { id } = useParams();
  const workflowId = Number(id);
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { can, permissionsLoaded } = useAuth();
  const canWrite = !permissionsLoaded || can("NIFI", "WRITE");

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string>();
  const [selectedEdgeId, setSelectedEdgeId] = useState<string>();
  const [flow, setFlow] = useState<{ fitView: (o?: object) => void }>();
  const [validation, setValidation] = useState<ValidationResult>();
  const [jobSearch, setJobSearch] = useState("");
  // 소속 그룹 밖 job이 필요할 때만 켠다(교차 그룹 워크플로우).
  const [showAllJobs, setShowAllJobs] = useState(false);
  const [dirty, setDirty] = useState(false);

  const workflowQuery = useQuery({
    queryKey: ["workflow", workflowId],
    queryFn: () => getWorkflow(workflowId),
    enabled: Number.isFinite(workflowId),
  });
  const jobsQuery = useQuery({ queryKey: ["etl-jobs"], queryFn: listEtlJobs });
  // 선행 워크플로우 선택지. 자기 자신은 제외한다(순환 방지는 서버 검증 V10이 한 번 더 본다).
  const workflowsQuery = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  // 팔레트를 ETL 화면과 같은 계층으로 보여주기 위해 NiFi 그룹 트리를 그대로 쓴다.
  const treeQuery = useQuery({ queryKey: ["nifi-pg-tree"], queryFn: getNifiProcessGroupTree });

  // 서버에서 받은 그래프를 캔버스로 옮긴다. 저장하지 않은 편집을 덮어쓰지 않도록
  // 아직 손대지 않았을 때만(dirty=false) 반영한다.
  useEffect(() => {
    const detail = workflowQuery.data;
    if (!detail || dirty) {
      return;
    }
    setNodes(detail.nodes.map((node, index) => ({
      id: node.nodeKey,
      position: { x: node.displayX ?? 80 + index * 220, y: node.displayY ?? 120 },
      data: {
        label: node.parentGroupName && node.jobName
          ? `${node.parentGroupName} / ${node.jobName}`
          : (node.jobName ?? node.nodeKey),
        jobId: node.jobId,
        nodeType: node.nodeType,
        triggerRule: node.triggerRule,
        retries: node.retries,
        retryDelaySec: node.retryDelaySec,
        jobMissing: node.jobMissing,
      },
      type: "job",
      className: nodeClass(node.jobMissing),
    })));
    // 엣지에는 어느 변에서 나가는지가 저장돼 있지 않다. 두 노드의 상대 위치로 정해
    // 가로로 늘어놓으면 오른쪽→왼쪽, 세로로 쌓으면 아래→위로 그린다.
    const placeOf = new Map(detail.nodes.map((node, index) => [
      node.nodeKey,
      { x: node.displayX ?? 80 + index * 220, y: node.displayY ?? 120 },
    ]));
    setEdges(detail.edges.map((edge) => {
      const from = placeOf.get(edge.fromNodeKey);
      const to = placeOf.get(edge.toNodeKey);
      const horizontal = from && to
        && Math.abs(to.x - from.x) > Math.abs(to.y - from.y) && to.x > from.x;
      return {
        id: `${edge.fromNodeKey}->${edge.toNodeKey}`,
        source: edge.fromNodeKey,
        target: edge.toNodeKey,
        sourceHandle: horizontal ? "s-right" : "s-bottom",
        targetHandle: horizontal ? "t-left" : "t-top",
        label: edgeLabel(edge.conditionType),
        data: { conditionType: edge.conditionType },
        animated: edge.conditionType === "FAILURE",
        style: { stroke: CONDITION_COLOR[edge.conditionType] ?? "#94a3b8" },
      };
    }));
  }, [workflowQuery.data, dirty, setNodes, setEdges]);

  const markDirty = useCallback(() => setDirty(true), []);

  /** 연결의 조건을 바꾼다. 성공 시(기본) / 실패 시(보상·알림 경로) / 완료 시(성패 무관). */
  const setEdgeCondition = useCallback((edgeId: string, condition: string) => {
    setEdges((current) => current.map((edge) => edge.id === edgeId ? {
      ...edge,
      label: edgeLabel(condition),
      data: { ...(edge.data ?? {}), conditionType: condition },
      animated: condition === "FAILURE",
      style: { stroke: CONDITION_COLOR[condition] ?? "#94a3b8" },
    } : edge));
    markDirty();
  }, [setEdges, markDirty]);

  // React Flow는 프로그램이 setNodes 할 때도 dimensions/select 변경을 흘린다.
  // 그걸 사용자 편집으로 세면 저장 직후 데이터를 다시 불러오는 순간 again dirty가 되어
  // 게시 버튼이 영영 잠긴다. 실제 편집(이동 끝/추가/삭제)만 dirty로 본다.
  const isUserEdit = (changes: Array<{ type: string; dragging?: boolean }>) =>
    changes.some((change) =>
      change.type === "remove"
      || change.type === "add"
      || (change.type === "position" && change.dragging === false));

  const onConnect = useCallback((connection: Connection) => {
    setEdges((current) => addEdge({
      ...connection,
      id: `${connection.source}->${connection.target}`,
      data: { conditionType: "SUCCESS" },
      style: { stroke: CONDITION_COLOR.SUCCESS },
    }, current));
    markDirty();
  }, [setEdges, markDirty]);

  /** 팔레트에서 job을 클릭하면 캔버스에 노드로 얹는다(참조지 복사가 아니다). */
  const addJobNode = useCallback((job: EtlJobResponse, parentLabel?: string) => {
    setNodes((current) => {
      const taken = new Set(current.map((node) => node.id));
      // 체인 이름은 그룹마다 겹치므로(DW/DZ 아래 COM001M) 그룹을 접두어로 붙여
      // TaskGroup id가 충돌하지 않게 한다.
      const key = toNodeKey(parentLabel ? `${parentLabel}_${job.jobName}` : job.jobName, taken);
      return [...current, {
        id: key,
        position: { x: 60 + (current.length % 3) * 200, y: 40 + Math.floor(current.length / 3) * 120 },
        data: {
          label: parentLabel ? `${parentLabel} / ${job.jobName}` : job.jobName,
          jobId: job.id,
          nodeType: "JOB",
          triggerRule: "ALL_SUCCESS",
          retries: 0,
          retryDelaySec: 60,
          jobMissing: false,
        },
        type: "job",
        className: nodeClass(false),
      }];
    });
    markDirty();
    // 새 노드가 화면 밖에 놓이면 사용자가 못 찾는다. 추가 직후 전체가 보이게 맞춘다.
    window.setTimeout(() => flow?.fitView({ padding: 0.2, duration: 200 }), 60);
  }, [setNodes, markDirty, flow]);

  const saveMutation = useMutation({
    mutationFn: () => saveWorkflowGraph(workflowId, {
      nodes: nodes.map((node) => ({
        nodeKey: node.id,
        nodeType: String(node.data.nodeType ?? "JOB"),
        jobId: (node.data.jobId as number | undefined) ?? null,
        triggerRule: String(node.data.triggerRule ?? "ALL_SUCCESS"),
        retries: Number(node.data.retries ?? 0),
        retryDelaySec: Number(node.data.retryDelaySec ?? 60),
        displayX: node.position.x,
        displayY: node.position.y,
      })),
      edges: edges.map((edge) => ({
        fromNodeKey: edge.source,
        toNodeKey: edge.target,
        conditionType: String((edge.data as { conditionType?: string } | undefined)?.conditionType ?? "SUCCESS"),
      })),
    }),
    onSuccess: () => {
      message.success("저장했습니다. (게시해야 Airflow에 반영됩니다)");
      setDirty(false);
      void queryClient.invalidateQueries({ queryKey: ["workflow", workflowId] });
    },
    onError: (error: Error) => message.error(error.message),
  });

  const validateMutation = useMutation({
    mutationFn: () => validateWorkflow(workflowId),
    onSuccess: (result) => {
      setValidation(result);
      if (result.valid && result.warnings.length === 0) {
        message.success("검증 통과. 게시할 수 있습니다.");
      }
    },
    onError: (error: Error) => message.error(error.message),
  });

  const publishMutation = useMutation({
    mutationFn: () => publishWorkflow(workflowId),
    onSuccess: (result) => {
      setValidation(result);
      if (result.valid) {
        message.success("게시했습니다. 최대 5분 내 Airflow에 DAG가 나타납니다.");
        void queryClient.invalidateQueries({ queryKey: ["workflow", workflowId] });
      } else {
        message.error("검증 오류가 있어 게시하지 못했습니다. 아래 목록을 확인해주세요.");
      }
    },
    onError: (error: Error) => message.error(error.message),
  });

  const unpublishMutation = useMutation({
    mutationFn: () => unpublishWorkflow(workflowId),
    onSuccess: () => {
      message.success("게시를 내렸습니다.");
      void queryClient.invalidateQueries({ queryKey: ["workflow", workflowId] });
    },
    onError: (error: Error) => message.error(error.message),
  });

  const settingsMutation = useMutation({
    mutationFn: (body: Parameters<typeof updateWorkflow>[1]) => updateWorkflow(workflowId, body),
    onSuccess: () => {
      message.success("속성을 저장했습니다.");
      void queryClient.invalidateQueries({ queryKey: ["workflow", workflowId] });
    },
    onError: (error: Error) => message.error(error.message),
  });

  // 검증에서 지목된 노드를 캔버스에서 빨갛게 칠한다.
  const problemNodeKeys = useMemo(
    () => new Set((validation?.errors ?? []).map((issue) => issue.nodeKey).filter(Boolean) as string[]),
    [validation],
  );
  useEffect(() => {
    setNodes((current) => current.map((node) => ({
      ...node,
      className: nodeClass(Boolean(node.data.jobMissing) || problemNodeKeys.has(node.id)),
    })));
  }, [problemNodeKeys, setNodes]);

  const detail = workflowQuery.data;
  const selectedNode = nodes.find((node) => node.id === selectedNodeId);
  const selectedEdge = edges.find((edge) => edge.id === selectedEdgeId);
  const usedJobIdSet = new Set(nodes.map((node) => node.data.jobId).filter(Boolean));
  // 체인을 자식 PG로 나누면 이름이 겹친다(DW/DZ 아래 COM001M 등). 부모 그룹 자체는
  // 프로세서가 없어 잡 목록에 안 나오므로 백엔드가 채워준 이름을 그대로 쓴다.
  const parentNameOf = (job: EtlJobResponse) => job.parentGroupName ?? undefined;
  const jobByPgId = new Map((jobsQuery.data ?? []).map((job) => [job.nifiPgId, job]));

  /**
   * NiFi 그룹 트리를 팔레트 트리로 옮긴다. ETL 관리 화면과 같은 계층으로 보여야
   * "어느 그룹의 어느 체인인지"를 이름 중복 없이 알아볼 수 있다.
   *
   * job(= etl_job에 잡힌 그룹)만 클릭 가능하게 하고, 상위 그룹은 접기/펼치기만 한다.
   */
  const buildPaletteTree = (node: NifiProcessGroupTreeNode): {
    key: string; title: React.ReactNode; children?: unknown[]; selectable: boolean;
  } | null => {
    const job = jobByPgId.get(node.id);
    const children = (node.children ?? [])
      .map(buildPaletteTree)
      .filter(Boolean) as Array<{ key: string; title: React.ReactNode; selectable: boolean }>;
    const needle = jobSearch.trim().toLowerCase();
    const selfMatches = !needle || node.name.toLowerCase().includes(needle);
    // 검색어가 있으면 자신 또는 자손이 걸리는 가지만 남긴다.
    if (needle && !selfMatches && children.length === 0) {
      return null;
    }
    const placed = job ? usedJobIdSet.has(job.id) : false;
    return {
      key: node.id,
      selectable: Boolean(job),
      title: (
        <span style={{ opacity: job || children.length ? 1 : 0.45 }}>
          {job ? (placed ? "● " : "○ ") : ""}
          {node.name}
          {/* 하위 그룹 수만 표시한다(프로세서 수는 여기서 알 필요가 없다). */}
          {children.length ? <span style={{ color: "#888" }}> ({children.length})</span> : null}
        </span>
      ),
      children: children.length ? children : undefined,
    };
  };
  /**
   * 팔레트의 뿌리. 워크플로우가 그룹에 속하면 그 그룹 아래만 보여준다.
   *
   * 그룹별로 워크플로우를 그리는 게 기본이라(Informatica의 폴더), 전체를 늘 펼쳐두면
   * DW 워크플로우를 그리다 DZ의 같은 이름 job을 집는 사고가 난다. 다른 그룹 job이
   * 정말 필요할 때만 "전체 보기"로 넓힌다.
   */
  const findGroup = (node: NifiProcessGroupTreeNode, pgId: string): NifiProcessGroupTreeNode | undefined => {
    if (node.id === pgId) {
      return node;
    }
    for (const child of node.children ?? []) {
      const hit = findGroup(child, pgId);
      if (hit) {
        return hit;
      }
    }
    return undefined;
  };
  const ownGroup = treeQuery.data && detail?.nifiGroupPgId
    ? findGroup(treeQuery.data, detail.nifiGroupPgId)
    : undefined;
  const paletteRoot = showAllJobs ? treeQuery.data : (ownGroup ?? treeQuery.data);
  const paletteTree = paletteRoot ? [buildPaletteTree(paletteRoot)].filter(Boolean) : [];

  if (workflowQuery.isLoading) {
    return <Card><Spin /> 워크플로우를 불러오는 중입니다.</Card>;
  }
  if (!detail) {
    return <Card><Empty description="워크플로우를 찾을 수 없습니다." /></Card>;
  }

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Card
        size="small"
        title={
          <Space>
            <Button size="small" icon={<ArrowLeftOutlined />} onClick={() => navigate("/workflows/design")} />
            <strong>{detail.name}</strong>
            <Tag>{detail.dagId}</Tag>
            {detail.published ? <Tag color="success">게시됨</Tag> : <Tag>미게시 (DAG 없음)</Tag>}
            {(dirty || detail.dirty) && detail.published && <Tag color="warning">게시본과 다름</Tag>}
            {dirty && <Tag color="processing">저장 안 됨</Tag>}
          </Space>
        }
        extra={
          <Space>
            <Button onClick={() => saveMutation.mutate()} loading={saveMutation.isPending} disabled={!canWrite}>
              저장
            </Button>
            <Button onClick={() => validateMutation.mutate()} loading={validateMutation.isPending}>
              검증
            </Button>
            <Button type="primary" onClick={() => publishMutation.mutate()}
                    loading={publishMutation.isPending} disabled={!canWrite || dirty}>
              게시
            </Button>
            {detail.published && (
              <Button danger onClick={() => unpublishMutation.mutate()}
                      loading={unpublishMutation.isPending} disabled={!canWrite}>
                게시 취소
              </Button>
            )}
          </Space>
        }
      >
        {dirty && (
          <Alert type="info" showIcon style={{ marginBottom: 8 }}
                 message="저장하지 않은 변경이 있습니다. 게시하려면 먼저 저장해주세요." />
        )}
        {validation && !validation.valid && (
          <Alert type="error" showIcon style={{ marginBottom: 8 }}
                 message={`검증 오류 ${validation.errors.length}건`}
                 description={
                   <ul style={{ margin: 0, paddingLeft: 18 }}>
                     {validation.errors.map((issue, index) => (
                       <li key={index}>
                         {issue.nodeKey ? <Tag>{issue.nodeKey}</Tag> : null}{issue.message}
                       </li>
                     ))}
                   </ul>
                 } />
        )}
        {validation && validation.warnings.length > 0 && (
          <Alert type="warning" showIcon style={{ marginBottom: 8 }}
                 message={`경고 ${validation.warnings.length}건 (게시는 가능합니다)`}
                 description={
                   <ul style={{ margin: 0, paddingLeft: 18 }}>
                     {validation.warnings.map((issue, index) => <li key={index}>{issue.message}</li>)}
                   </ul>
                 } />
        )}

        <div style={{ display: "grid", gridTemplateColumns: "260px 1fr 300px", gap: 12, minHeight: 560 }}>
          {/* 좌: job 팔레트 - ETL(NiFi)에서 만들어 둔 job 목록 */}
          <Card size="small" title="job 팔레트"
                extra={<Button size="small" icon={<SyncOutlined />} onClick={() => { void jobsQuery.refetch(); void treeQuery.refetch(); }} />}
                styles={{ body: { maxHeight: 520, overflowY: "auto" } }}>
            {ownGroup && (
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between",
                            marginBottom: 8, gap: 6 }}>
                <Tag color="blue" style={{ margin: 0 }}>{ownGroup.name}</Tag>
                <Button size="small" type="link" style={{ padding: 0, height: "auto" }}
                        onClick={() => setShowAllJobs((current) => !current)}>
                  {showAllJobs ? "이 그룹만" : "전체 보기"}
                </Button>
              </div>
            )}
            <Input.Search placeholder="그룹·job 검색" allowClear size="small" style={{ marginBottom: 8 }}
                          onChange={(event) => setJobSearch(event.target.value)} />
            {treeQuery.isLoading ? (
              <Spin size="small" />
            ) : paletteTree.length === 0 ? (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="job이 없습니다" />
            ) : (
              <Tree
                blockNode
                defaultExpandAll
                autoExpandParent={Boolean(jobSearch.trim())}
                selectedKeys={[]}
                treeData={paletteTree as never}
                onSelect={(_, info) => {
                  // job 노드만 캔버스에 추가한다(상위 그룹은 접기/펼치기 전용).
                  const job = jobByPgId.get(String(info.node.key));
                  if (job && canWrite) {
                    addJobNode(job, parentNameOf(job));
                  }
                }}
              />
            )}
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              클릭하면 캔버스에 추가됩니다. ● 는 이미 배치된 job입니다.
            </Typography.Text>
          </Card>

          {/* 중: 캔버스 - 노드를 잇는 선이 곧 실행 순서다 */}
          <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, height: 560 }}>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={(changes) => { onNodesChange(changes); if (isUserEdit(changes)) markDirty(); }}
              onEdgesChange={(changes) => { onEdgesChange(changes); if (isUserEdit(changes)) markDirty(); }}
              onConnect={onConnect}
              onNodeClick={(_, node) => { setSelectedNodeId(node.id); setSelectedEdgeId(undefined); }}
              onEdgeClick={(_, edge) => { setSelectedEdgeId(edge.id); setSelectedNodeId(undefined); }}
              onPaneClick={() => { setSelectedNodeId(undefined); setSelectedEdgeId(undefined); }}
              fitView
              nodeTypes={NODE_TYPES}
              onInit={(instance) => setFlow(instance)}
              minZoom={0.2}
              deleteKeyCode={["Backspace", "Delete"]}
            >
              <Background />
              <Controls />
              <MiniMap pannable zoomable />
            </ReactFlow>
          </div>

          {/* 우: 속성 - 워크플로우 전체 / 선택 노드 */}
          <Card size="small" title={
            selectedNode ? `노드: ${selectedNode.id}`
              : selectedEdge ? "연결 조건"
              : "워크플로우 속성"}
                styles={{ body: { maxHeight: 520, overflowY: "auto" } }}>
            {selectedNode ? (
              <Space direction="vertical" style={{ width: "100%" }} size={10}>
                <div>
                  <div style={{ fontSize: 12, color: "#888" }}>job</div>
                  <div>{String(selectedNode.data.label)}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, color: "#888" }}>진입 조건 (trigger rule)</div>
                  <Select size="small" style={{ width: "100%" }}
                          value={String(selectedNode.data.triggerRule ?? "ALL_SUCCESS")}
                          onChange={(value) => {
                            setNodes((current) => current.map((node) => node.id === selectedNode.id
                              ? { ...node, data: { ...node.data, triggerRule: value } } : node));
                            markDirty();
                          }}
                          options={[
                            { value: "ALL_SUCCESS", label: "선행이 모두 성공해야" },
                            { value: "ALL_DONE", label: "선행이 끝나면(성패 무관)" },
                            { value: "ONE_FAILED", label: "선행 중 하나가 실패하면" },
                          ]} />
                </div>
                <div>
                  <div style={{ fontSize: 12, color: "#888" }}>재시도 횟수</div>
                  <InputNumber size="small" min={0} max={10} style={{ width: "100%" }}
                               value={Number(selectedNode.data.retries ?? 0)}
                               onChange={(value) => {
                                 setNodes((current) => current.map((node) => node.id === selectedNode.id
                                   ? { ...node, data: { ...node.data, retries: value ?? 0 } } : node));
                                 markDirty();
                               }} />
                </div>
                <Button size="small" onClick={() => setSelectedNodeId(undefined)}>워크플로우 속성 보기</Button>
              </Space>
            ) : selectedEdge ? (
              <Space direction="vertical" style={{ width: "100%" }} size={10}>
                <div>
                  <div style={{ fontSize: 12, color: "#888" }}>연결</div>
                  <div>{selectedEdge.source} → {selectedEdge.target}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, color: "#888" }}>실행 조건</div>
                  <Select size="small" style={{ width: "100%" }} disabled={!canWrite}
                          value={String((selectedEdge.data as { conditionType?: string } | undefined)?.conditionType ?? "SUCCESS")}
                          onChange={(value) => setEdgeCondition(selectedEdge.id, value)}
                          options={[
                            { value: "SUCCESS", label: "성공 시 (앞 job이 성공해야 진행)" },
                            { value: "FAILURE", label: "실패 시 (보상·알림 경로)" },
                            { value: "ALWAYS", label: "완료 시 (성패 무관)" },
                          ]} />
                </div>
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  연결을 지우려면 선택 후 Delete 키를 누르세요.
                </Typography.Text>
                <Button size="small" onClick={() => setSelectedEdgeId(undefined)}>워크플로우 속성 보기</Button>
              </Space>
            ) : (
              <WorkflowSettings detail={detail} disabled={!canWrite}
                                candidates={(workflowsQuery.data ?? []).filter((w) => w.id !== workflowId)}
                                onSave={(body) => settingsMutation.mutate(body)}
                                saving={settingsMutation.isPending} />
            )}
          </Card>
        </div>
      </Card>
    </Space>
  );
}

/**
 * job 노드. 상·하·좌·우 네 곳에 연결 점을 둔다.
 *
 * 기본 노드는 위/아래에만 점이 있어 좌우로 늘어놓으면 선이 크게 돌아가고 조준도 어려웠다.
 * 들어오는 쪽(위·왼쪽)과 나가는 쪽(아래·오른쪽)을 나눠 배치한다. 같은 자리에 source와
 * target을 겹쳐 두면 React Flow가 어느 점을 잡았는지 가려내지 못해 연결이 성립하지 않으므로
 * 한 변에는 한 종류만 둔다. 위→아래, 왼쪽→오른쪽 두 방향으로 그릴 수 있다.
 */
function JobNode({ data, isConnectable }: { data: { label?: string }; isConnectable?: boolean }) {
  return (
    <>
      <Handle type="target" position={Position.Top} id="t-top" isConnectable={isConnectable} />
      <Handle type="target" position={Position.Left} id="t-left" isConnectable={isConnectable} />
      <span>{String(data?.label ?? "")}</span>
      <Handle type="source" position={Position.Bottom} id="s-bottom" isConnectable={isConnectable} />
      <Handle type="source" position={Position.Right} id="s-right" isConnectable={isConnectable} />
    </>
  );
}

const NODE_TYPES = { job: JobNode };

function nodeClass(problem: boolean): string {
  return problem ? "wf-node wf-node--problem" : "wf-node";
}

function WorkflowSettings({ detail, disabled, onSave, saving, candidates }: {
  detail: WorkflowDetail;
  disabled: boolean;
  onSave: (body: Parameters<typeof updateWorkflow>[1]) => void;
  saving: boolean;
  candidates: WorkflowSummary[];
}) {
  const [name, setName] = useState(detail.name);
  // 저장된 크론을 프리셋으로 되돌려 채운다. 프리셋으로 표현 안 되는 식이면 직접 입력에만 남는다.
  const parsed = parseCronToPreset(detail.scheduleCron);
  const [scheduleMode, setScheduleMode] = useState<"manual" | "recurring">(
    detail.scheduleCron ? "recurring" : "manual");
  const [preset, setPreset] = useState<SchedulePreset>(parsed.preset);
  const [hour, setHour] = useState(parsed.hour);
  const [minute, setMinute] = useState(parsed.minute);
  const [weekday, setWeekday] = useState(parsed.weekday);
  const [customCron, setCustomCron] = useState(parsed.matched ? "" : (detail.scheduleCron ?? ""));
  const [maxRuns, setMaxRuns] = useState(detail.maxActiveRuns);
  const [upstream, setUpstream] = useState<number[]>(detail.upstreamWorkflowIds ?? []);
  const [upstreamMode, setUpstreamMode] = useState(detail.upstreamMode ?? "ALL");

  return (
    <Space direction="vertical" style={{ width: "100%" }} size={10}>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>이름</div>
        <Input size="small" value={name} onChange={(event) => setName(event.target.value)} />
      </div>
      <div>
        <div style={{ fontSize: 12, color: "#888", marginBottom: 4 }}>스케줄</div>
        <Radio.Group size="small" value={scheduleMode} style={{ marginBottom: 8 }}
                     onChange={(event) => setScheduleMode(event.target.value)}>
          <Radio value="manual">수동 실행</Radio>
          <Radio value="recurring">정기 실행</Radio>
        </Radio.Group>
        {scheduleMode === "recurring" && (
          <Space direction="vertical" style={{ width: "100%" }} size={6}>
            <Space size={4} wrap>
              <Select size="small" style={{ width: 90 }} value={preset} onChange={setPreset}
                      options={[{ label: "매시간", value: "hourly" },
                                { label: "매일", value: "daily" },
                                { label: "매주", value: "weekly" }]} />
              {preset === "weekly" && (
                <Select size="small" style={{ width: 90 }} value={weekday} onChange={setWeekday}
                        options={WEEKDAY_LABELS.map((label, value) => ({ label: `${label}요일`, value }))} />
              )}
              {preset !== "hourly" && (
                <InputNumber size="small" style={{ width: 60 }} min={0} max={23} value={hour}
                             onChange={(value) => setHour(value ?? 0)} />
              )}
              <InputNumber size="small" style={{ width: 74 }} min={0} max={59} value={minute}
                           addonAfter="분" onChange={(value) => setMinute(value ?? 0)} />
            </Space>
            <Input size="small" placeholder={presetCron(preset, hour, minute, weekday)}
                   value={customCron} onChange={(event) => setCustomCron(event.target.value)} />
            {customCron.trim() && !validCron(customCron.trim()) && (
              <div style={{ fontSize: 12, color: "#ef4444" }}>크론 형식이 올바르지 않습니다 (분 시 일 월 요일)</div>
            )}
            <div style={{ fontSize: 12, color: "#555" }}>
              ▶ {customCron.trim()
                ? `${customCron.trim()} (${detail.timezone})`
                : `${presetDescription(preset, hour, minute, weekday)} (${detail.timezone})`}
            </div>
            {/* 저장 전에 "정말 이 시각이 맞나"를 눈으로 확인하게 한다. */}
            {!customCron.trim() && (
              <div style={{ fontSize: 12, color: "#888", lineHeight: 1.7 }}>
                <strong style={{ color: "#555" }}>다음 5회 실행</strong>
                {nextPresetRuns(preset, hour, minute, weekday).map((d) => (
                  <div key={d.valueOf()}>{d.format("YYYY-MM-DD HH:mm")}</div>
                ))}
              </div>
            )}
          </Space>
        )}
      </div>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>선행 워크플로우</div>
        <Select size="small" mode="multiple" allowClear style={{ width: "100%" }}
                placeholder="없음 (스케줄/수동으로 실행)"
                value={upstream}
                onChange={(value) => setUpstream(value)}
                options={candidates.map((w) => ({ value: w.id, label: w.name }))} />
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          지정하면 그 워크플로우가 끝날 때 이 워크플로우가 자동 실행됩니다.
        </Typography.Text>
      </div>
      {upstream.length > 1 && (
        <div>
          <div style={{ fontSize: 12, color: "#888" }}>선행 조건</div>
          <Select size="small" style={{ width: "100%" }} value={upstreamMode}
                  onChange={(value) => setUpstreamMode(value)}
                  options={[
                    { value: "ALL", label: "모두 완료되면 실행" },
                    { value: "ANY", label: "하나라도 완료되면 실행" },
                  ]} />
        </div>
      )}
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>동시 실행 수</div>
        <InputNumber size="small" min={1} max={5} style={{ width: "100%" }}
                     value={maxRuns} onChange={(value) => setMaxRuns(value ?? 1)} />
      </div>
      <div style={{ fontSize: 12, color: "#888" }}>
        시간대 {detail.timezone} · DAG {detail.dagId}
      </div>
      <Button size="small" type="primary" block disabled={disabled} loading={saving}
              onClick={() => onSave({
                name,
                description: detail.description,
                nifiGroupPgId: detail.nifiGroupPgId,
                scheduleCron: scheduleMode === "manual" ? null
                  : (customCron.trim() || presetCron(preset, hour, minute, weekday)),
                timezone: detail.timezone,
                catchup: detail.catchup,
                maxActiveRuns: maxRuns,
                suspendOnError: detail.suspendOnError,
                upstreamWorkflowIds: upstream,
                upstreamMode,
              })}>
        속성 저장
      </Button>
    </Space>
  );
}

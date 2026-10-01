import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addEdge,
  Handle,
  Position,
  ControlButton,
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
  Modal,
  Radio,
  Select,
  Space,
  Spin,
  Tooltip,
  Tag,
  Typography,
} from "antd";
import {
  ArrowLeftOutlined,
  ColumnWidthOutlined,
  ExpandOutlined,
  InfoCircleFilled,
  LockOutlined,
  MinusOutlined,
  PlusOutlined,
  SyncOutlined,
  UnlockOutlined,
} from "@ant-design/icons";
import { useAuth } from "../auth/AuthContext";
import { setLeaveGuard } from "../utils/navigationGuard";
import { listEtlJobs, syncEtlJobs, type EtlJobResponse } from "../api/etlJobs";
import { getNifiProcessGroupTree, type NifiProcessGroupTreeNode } from "../api/platform";
import { listPipelineRuntimeStatuses, listPipelines } from "../api/pipelines";
import { renderRuntimeStatus, sourceLabel, targetLabel } from "../utils/cdcPresentation";
import { listPipelineGroups, type PipelineGroupResponse } from "../api/pipelineGroups";
import { loadModeLabel, type PipelineResponse } from "../types/pipeline";
import { buildWorkflowHierarchy, parentWorkflowPathOf, publishedParentsOf, type WorkflowTreeItem } from "../utils/workflowTree";
import { PaletteTree, type PaletteTreeNode } from "../components/PaletteTree";
import { EtlJobSummary } from "./ConsoleFramePage";
import { GptChatPanel } from "../components/gpt/GptChatPanel";
import { SideDock } from "../components/SideDock";
import { useSideDock } from "../components/useSideDock";
import {
  ParentScheduleNotice,
  WorkflowCompositionSection,
  WorkflowPublishSection,
  WorkflowRecentRuns,
} from "../components/WorkflowInfoSections";
import { useGptEnabled } from "../components/gpt/useGptEnabled";
import {
  cronToKoreanText,
  nextPresetRuns,
  parseCronToPreset,
  presetCron,
  presetDescription,
  WEEKDAY_LABELS,
  type SchedulePreset,
} from "../utils/schedulePreset";
import {
  createWorkflow,
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
const normalizeKeyPart = (raw: string) =>
  raw.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");

/**
 * 잡 이름이 이미 그룹명으로 시작하는지 본다.
 *
 * NiFi 프로세스 그룹 이름 규칙이 도중에 바뀌어 두 세대가 섞여 있다. 옛 잡은 `COM001M`처럼
 * 그룹명이 없어서 접두어를 붙여야 `dz_com_com001m`이 되지만, 새 잡은 `DZ_COM001M`처럼 이미
 * 그룹명을 품고 있어서 또 붙이면 `dz_com_dz_com001m`이 된다.
 *
 * 앞글자만 우연히 겹치는 경우(`COM` 그룹의 `COMMON_LOAD`)를 «이미 붙어 있다»로 오인하면
 * 서로 다른 그룹의 동명 잡이 한 키로 뭉치므로, 그룹명 뒤가 끝이거나 `_` 또는 숫자로
 * 이어질 때만 접두어로 인정한다.
 */
function hasGroupPrefix(parentLabel: string, jobName: string): boolean {
  const parent = normalizeKeyPart(parentLabel);
  const job = normalizeKeyPart(jobName);
  if (!parent || !job.startsWith(parent)) {
    return false;
  }
  const rest = job.slice(parent.length);
  return rest === "" || rest.startsWith("_") || /^[0-9]/.test(rest);
}

function toNodeKey(jobName: string, taken: Set<string>): string {
  const base = normalizeKeyPart(jobName) || "job";
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
  // «생성» 메뉴(/workflows/create)로 들어오면 id 가 없다. 서버에 아무것도 없는 «초안» 상태로
  // 빈 캔버스를 열고, 이름을 속성창에서 받아 «저장»할 때 비로소 워크플로우를 만든다.
  // (예전엔 목록 화면의 모달에서 이름을 먼저 받았는데, 이름을 정하기 전에 그림부터 그리고
  // 싶다는 요청으로 바꿨다. 2026-09-10)
  const isDraft = !Number.isFinite(workflowId);
  const paletteDock = useSideDock("left", 260, 200, 560);
  const detailDock = useSideDock("right", 300, 260, 720);
  const [draftName, setDraftName] = useState("");
  const navigate = useNavigate();

  const queryClient = useQueryClient();
  const { can, permissionsLoaded } = useAuth();
  // 그래프 편집(저장·노드 추가)은 ETL 설계라 NIFI 쓰기,
  // 게시/게시취소는 DAG 를 만들고 스케줄을 켜는 운영 동작이라 AIRFLOW 쓰기를 본다.
  const canWrite = !permissionsLoaded || can("NIFI", "WRITE");
  const canPublish = !permissionsLoaded || can("AIRFLOW", "WRITE");

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string>();
  // 캔버스 «편집 잠금». 기본 컨트롤의 Toggle interactivity 를 우리 버튼으로 대신한다.
  const [interactive, setInteractive] = useState(true);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string>();
  // 직접 만든 컨트롤(확대·축소·전체 보기)이 쓰는 인스턴스.
  const [flow, setFlow] = useState<{
    fitView: (o?: object) => void;
    zoomIn: () => void;
    zoomOut: () => void;
    screenToFlowPosition: (p: { x: number; y: number }) => { x: number; y: number };
  }>();
  const [validation, setValidation] = useState<ValidationResult>();
  // 소속 그룹 밖 job이 필요할 때만 켠다(교차 그룹 워크플로우).
  const [dirty, setDirty] = useState(false);
  // 노드 속성창의 [속성] [GPT] 탭. 선택된 LLM 이 없으면 탭 없이 속성만 보인다(설계 5-1).
  const gptEnabled = useGptEnabled();
  const [nodePanelTab, setNodePanelTab] = useState<"props" | "gpt">("props");

  /** 캔버스를 떠나기 전 확인. 저장 안 한 변경을 말없이 버리지 않는다. */
  const askLeave = () => new Promise<boolean>((resolve) => {
    Modal.confirm({
      title: "저장하지 않은 변경이 있습니다",
      content: "이동하면 이 캔버스의 변경 내용이 사라집니다. 이동할까요?",
      okText: "이동",
      cancelText: "취소",
      onOk: () => resolve(true),
      onCancel: () => resolve(false),
    });
  });

  const leaveTo = (path: string) => {
    if (!dirty) {
      navigate(path);
      return;
    }
    void askLeave().then((ok) => { if (ok) navigate(path); });
  };

  // 사이드 메뉴·상단 로고 등 이 화면 밖에서 일어나는 이동에도 같은 확인을 걸어 둔다.
  // 저장하면(dirty=false) 즉시 해제되고, 화면을 떠날 때도 반드시 해제한다.
  useEffect(() => {
    setLeaveGuard(dirty ? askLeave : null);
    return () => setLeaveGuard(null);
    // askLeave 는 매 렌더 새로 만들어지지만 하는 일이 같아 의존성에서 뺀다.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dirty]);

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
  // CDC 팔레트. 관리 화면과 같은 계층(전체 파이프라인 > 그룹 > 파이프라인)으로 보여준다.
  const pipelinesQuery = useQuery({ queryKey: ["pipelines"], queryFn: listPipelines });
  const pipelineGroupsQuery = useQuery({ queryKey: ["pipeline-groups"], queryFn: listPipelineGroups });

  // 서버에서 받은 그래프를 캔버스로 옮긴다. 저장하지 않은 편집을 덮어쓰지 않도록
  /**
   * 다른 워크플로우로 옮겨가면 화면을 비운다.
   *
   * 캔버스 로드는 «손대지 않았을 때만» 반영하는데(dirty 가드), 이동해도 dirty가 남아
   * 있으면 그 가드에 걸려 <b>이전 워크플로우의 그래프가 그대로 남는다</b>. 제목만 바뀌고
   * 그림은 그대로여서 «반응이 없다»로 보였다.
   */
  useEffect(() => {
    setDirty(false);
    setNodes([]);
    setEdges([]);
    setSelectedNodeId(undefined);
    setSelectedEdgeId(undefined);
    setValidation(undefined);
    setDraftName("");
  }, [workflowId, setNodes, setEdges]);

  // 아직 손대지 않았을 때만(dirty=false) 반영한다.
  useEffect(() => {
    const detail = workflowQuery.data;
    if (!detail || dirty) {
      return;
    }
    const loadedNodes: Node[] = detail.nodes.map((node, index) => ({
      id: node.nodeKey,
      position: { x: node.displayX ?? 80 + index * 220, y: node.displayY ?? 120 },
      data: {
        // SUBWF는 job 이름이 없다. 가리키는 워크플로우 이름을 보여준다(키가 아니라).
        label: node.nodeType === "SUBWF"
          // 이름 앞 «▷» 는 뺐다(2026-09-11 요청) - 노드 왼쪽 «wf» 띠가 이미 종류를 알려 준다.
          ? (node.subWorkflowName ?? node.nodeKey)
          : node.nodeType === "CDC"
          ? (node.pipelineName ?? node.nodeKey)
          // 노드에는 job 이름만 쓴다. 상위 경로를 함께 적으면 이름이 길어져 오히려
          // 구분이 어렵고, 그 정보는 우측 속성창에 이미 나온다(2026-09-03 제보).
          : (node.jobName ?? node.nodeKey),
        jobId: node.jobId,
        nodeType: node.nodeType,
        triggerRule: node.triggerRule,
        retries: node.retries,
        retryDelaySec: node.retryDelaySec,
        jobMissing: node.jobMissing,
        nifiPgId: node.nifiPgId,
        subWorkflowId: node.subWorkflowId,
        pipelineId: node.pipelineId,
      },
      type: "job",
      className: nodeClass(node.jobMissing, node.nodeType),
    }));
    // 흐름은 항상 왼쪽에서 오른쪽이다. 연결점이 좌우 하나씩뿐이라 어느 변인지 추정할 것이 없다.
    const loadedEdges: Edge[] = detail.edges.map((edge) => {
      return {
        id: `${edge.fromNodeKey}->${edge.toNodeKey}`,
        source: edge.fromNodeKey,
        target: edge.toNodeKey,
        sourceHandle: "s-right",
        targetHandle: "t-left",
        label: edgeLabel(edge.conditionType),
        data: { conditionType: edge.conditionType },
        animated: edge.conditionType === "FAILURE",
        style: { stroke: CONDITION_COLOR[edge.conditionType] ?? "#94a3b8" },
      };
    });
    // 열 때는 늘 정렬된 모습으로 시작한다(2026-09-11 요청). 저장된 좌표는 무시하고 «정렬» 버튼과
    // 같은 규칙으로 펴되, 자동 정렬은 편집이 아니므로 dirty 는 건드리지 않는다.
    setNodes(layoutHorizontally(loadedNodes, loadedEdges));
    setEdges(loadedEdges);
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
    // CDC 노드에는 선을 잇지 못한다. 순서가 성립하지 않기 때문이다 - CDC 시작은 «끝»이 없어
    // 다음 노드를 기다리게 할 수 없고, 워크플로우는 지시만 쏘고 끝난다(2026-09-10 설계 방안 B).
    // 서버도 같은 규칙으로 막지만, 화면에서 먼저 막아야 «그렸는데 게시에서 거절»이 안 된다.
    const touchesCdc = [connection.source, connection.target]
      .some((id) => nodes.find((node) => node.id === id)?.data.nodeType === "CDC");
    if (touchesCdc) {
      message.warning("CDC 노드는 연결할 수 없습니다 - 고른 지시를 모든 CDC 노드에 한 번에 보냅니다.");
      return;
    }
    setEdges((current) => addEdge({
      ...connection,
      id: `${connection.source}->${connection.target}`,
      data: { conditionType: "SUCCESS" },
      style: { stroke: CONDITION_COLOR.SUCCESS },
    }, current));
    markDirty();
  }, [setEdges, markDirty, nodes]);

  /** 팔레트에서 job을 클릭하면 캔버스에 노드로 얹는다(참조지 복사가 아니다). */
  /** at 을 주면 그 자리에, 없으면 빈 칸을 찾아 놓는다(끌어다 놓기는 놓은 자리를 준다). */
  const addJobNode = useCallback((job: EtlJobResponse, parentLabel?: string,
                                  at?: { x: number; y: number }) => {
    setNodes((current) => {
      const taken = new Set(current.map((node) => node.id));
      // 체인 이름은 그룹마다 겹치므로(DW/DZ 아래 COM001M) 그룹을 접두어로 붙여
      // TaskGroup id가 충돌하지 않게 한다. 다만 잡 이름이 이미 그룹명으로 시작하면
      // (DZ_COM 그룹의 DZ_COM001M) 또 붙이지 않는다 — dz_com_dz_com001m 방지.
      const key = toNodeKey(
        parentLabel && !hasGroupPrefix(parentLabel, job.jobName)
          ? `${parentLabel}_${job.jobName}`
          : job.jobName,
        taken);
      return [...current, {
        id: key,
        position: at
          ?? { x: 60 + (current.length % 3) * 200, y: 40 + Math.floor(current.length / 3) * 120 },
        data: {
          label: job.jobName,
          jobId: job.id,
          nodeType: "JOB",
          triggerRule: "ALL_SUCCESS",
          retries: 0,
          retryDelaySec: 60,
          jobMissing: false,
          nifiPgId: job.nifiPgId,
        },
        type: "job",
        className: nodeClass(false, "JOB"),
      }];
    });
    markDirty();
  }, [setNodes, markDirty]);

  /** 팔레트의 워크플로우를 클릭하면 «그 워크플로우를 통째로 실행하는 노드»를 얹는다. */
  /** at 을 주면 그 자리에, 없으면 빈 칸을 찾아 놓는다(끌어다 놓기는 놓은 자리를 준다). */
  const addSubWorkflowNode = useCallback((target: WorkflowSummary, at?: { x: number; y: number }) => {
    setNodes((current) => {
      const taken = new Set(current.map((node) => node.id));
      const key = toNodeKey(`wf_${target.workflowKey}`, taken);
      return [...current, {
        id: key,
        position: at
          ?? { x: 60 + (current.length % 3) * 200, y: 40 + Math.floor(current.length / 3) * 120 },
        data: {
          label: target.name,
          nodeType: "SUBWF",
          subWorkflowId: target.id,
          triggerRule: "ALL_SUCCESS",
          retries: 0,
          retryDelaySec: 60,
          jobMissing: false,
        },
        type: "job",
        className: nodeClass(false, "SUBWF"),
      }];
    });
    markDirty();
  }, [setNodes, markDirty]);

  /**
   * 팔레트의 CDC 파이프라인을 얹는다. 노드 하나가 파이프라인 하나의 제어 DAG 를 부른다.
   *
   * <p>CDC 노드는 서로 잇지 않는다(순서가 없다). 워크플로우 실행 때 고른 지시(배포·시작·중지·
   * 재시작·감시)를 소속 파이프라인 전부에 한 번에 쏘고 끝난다 - 기다리지 않는다.
   */
  const addCdcNode = useCallback((pipeline: PipelineResponse, at?: { x: number; y: number }) => {
    setNodes((current) => {
      const taken = new Set(current.map((node) => node.id));
      const key = toNodeKey(`cdc_${pipeline.name}`, taken);
      return [...current, {
        id: key,
        position: at
          ?? { x: 60 + (current.length % 3) * 200, y: 40 + Math.floor(current.length / 3) * 120 },
        data: {
          label: pipeline.name,
          nodeType: "CDC",
          pipelineId: pipeline.id,
          // 잇지 않으므로 선행 조건이랄 것이 없다. 저장 형식을 맞추려고 기본값만 채운다.
          triggerRule: "ALL_SUCCESS",
          retries: 0,
          retryDelaySec: 60,
          jobMissing: false,
        },
        type: "job",
        className: nodeClass(false, "CDC"),
      }];
    });
    markDirty();
  }, [setNodes, markDirty]);

  /**
   * 연결된 노드들을 첫 노드 기준으로 가로 한 줄로 편다.
   *
   * 손으로 끌어다 놓으면 금세 삐뚤어져 읽기 어려워진다. 흐름이 왼쪽→오른쪽 한 방향이라
   * 위상 순서대로 늘어놓기만 하면 된다.
   *
   * <p>줄은 «이어진 덩어리»마다 하나씩 준다. 칸마다 줄을 따로 세면 A→B→C의 C가 그 칸의
   * 첫 노드라는 이유로 남의 줄(맨 위)로 올라가 버려서, 눈으로는 연결이 끊겨 보인다.
   * 연결이 없는 노드는 그 아래 줄에 모은다.
   */
  const alignHorizontally = useCallback(() => {
    setNodes((current) => layoutHorizontally(current, edges));
    markDirty();
    window.setTimeout(() => flow?.fitView({ padding: 0.35, maxZoom: 0.85, duration: 250 }), 80);
  }, [setNodes, edges, markDirty, flow]);

  /**
   * 속성 패널의 «현재 입력값». 상단 «저장» 이 그래프와 속성을 함께 저장하기 위해 들고 있는다.
   *
   * <p>패널은 노드/연결을 선택하면 언마운트되므로(그때는 노드 속성이 대신 뜬다) 패널 안의
   * state 만으로는 저장 시점에 값을 못 읽는다. 그래서 입력이 바뀔 때마다 여기로 올려둔다.
   * 워크플로우를 바꾸면 남은 초안이 새 워크플로우에 섞이지 않게 비운다.
   */
  const settingsDraftRef = useRef<Parameters<typeof updateWorkflow>[1] | null>(null);
  useEffect(() => {
    settingsDraftRef.current = null;
  }, [workflowId]);

  const graphBody = () => ({
      nodes: nodes.map((node) => ({
        nodeKey: node.id,
        nodeType: String(node.data.nodeType ?? "JOB"),
        jobId: (node.data.jobId as number | undefined) ?? null,
        subWorkflowId: (node.data.subWorkflowId as number | undefined) ?? null,
        pipelineId: (node.data.pipelineId as number | undefined) ?? null,
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
  });

  const saveMutation = useMutation({
    // 예전에는 그래프만 저장해서, 스케줄을 바꾸고 «저장»을 눌러도 조용히 버려졌다
    // (스케줄은 «속성 저장» 버튼에만 있었다). 화면의 변경은 «저장» 하나로 다 저장한다.
    mutationFn: async () => {
      const draft = settingsDraftRef.current;
      if (isDraft) {
        // 초안: 여기서 처음 만든다. 이름은 필수(workflowKey·dag_id 의 재료).
        const name = (draft?.name ?? draftName).trim();
        if (!name) {
          throw new Error("이름을 입력해주세요. 우측 «워크플로우 속성»의 이름 칸에 적으면 됩니다.");
        }
        const created = await createWorkflow({
          name,
          description: draft?.description ?? undefined,
          scheduleCron: draft?.scheduleCron ?? undefined,
          timezone: draft?.timezone,
        });
        // 생성 API 가 안 받는 나머지 속성(메모·catchup 등)은 만든 뒤 한 번 더 저장한다.
        if (draft) {
          await updateWorkflow(created.id, draft);
        }
        if (nodes.length) {
          await saveWorkflowGraph(created.id, graphBody());
        }
        return { createdId: created.id };
      }
      if (draft) {
        await updateWorkflow(workflowId, draft);
      }
      await saveWorkflowGraph(workflowId, graphBody());
      return { createdId: undefined as number | undefined };
    },
    onSuccess: ({ createdId }) => {
      if (createdId !== undefined) {
        // 초안이 진짜 워크플로우가 됐다. 이탈 확인이 뜨지 않게 dirty 를 먼저 내리고 그 id 로 옮긴다.
        setDirty(false);
        message.success("워크플로우를 만들었습니다.");
        void queryClient.invalidateQueries({ queryKey: ["workflows"] });
        navigate(`/workflows/design/${createdId}`, { replace: true });
        return;
      }
      message.success("저장했습니다. (게시해야 Airflow에 반영됩니다)");
      setDirty(false);
      void queryClient.invalidateQueries({ queryKey: ["workflow", workflowId] });
    },
    onError: (error: Error) => message.error(error.message),
  });

  /**
   * 팔레트 새로고침. 조회만 다시 하면 «백엔드가 아직 NiFi를 안 본» 상태라 방금 만든 job이
   * 나오지 않는다(미러링은 5분 주기). 그래서 미러링을 먼저 돌리고 그 다음에 다시 읽는다.
   *
   * 동기화가 실패해도 조회는 해준다 - NiFi가 잠깐 응답하지 않는다고 화면까지 멈출 이유는 없다.
   */
  const syncMutation = useMutation({
    mutationFn: syncEtlJobs,
    onSuccess: (result) => {
      const changed = result.created + result.deleted;
      message.success(changed > 0
          ? `동기화 완료 - job ${result.jobsSeen}개 (신규 ${result.created}, 삭제 ${result.deleted})`
          : `동기화 완료 - job ${result.jobsSeen}개 (변경 없음)`);
    },
    onError: (error: Error) => message.warning(`동기화 실패(목록만 새로고침합니다): ${error.message}`),
    onSettled: () => {
      void jobsQuery.refetch();
      void treeQuery.refetch();
    },
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
      className: nodeClass(Boolean(node.data.jobMissing) || problemNodeKeys.has(node.id),
                           String(node.data.nodeType ?? "JOB")),
    })));
  }, [problemNodeKeys, setNodes]);

  /** 초안(생성 중)일 때 속성창·머리줄이 쓸 빈 워크플로우. 서버에는 아직 없다. */
  const draftDetail: WorkflowDetail = {
    id: 0, workflowKey: "", dagId: "", name: draftName, description: null,
    scheduleCron: null, timezone: "Asia/Seoul", upstreamCount: 0,
    published: false, publishedAt: null, publishedBy: null,
    jobCount: 0, childWorkflowIds: [], updatedAt: "",
    catchup: false, maxActiveRuns: 1, suspendOnError: false,
    upstreamWorkflowIds: [], upstreamMode: null, memo: null, parents: [],
    dirty: false, nodes: [], edges: [],
  };
  const detail = isDraft ? draftDetail : workflowQuery.data;
  const selectedNode = nodes.find((node) => node.id === selectedNodeId);
  const selectedEdge = edges.find((edge) => edge.id === selectedEdgeId);
  const usedJobIdSet = new Set(nodes.map((node) => node.data.jobId).filter(Boolean));
  const usedPipelineIdSet = new Set(nodes.map((node) => node.data.pipelineId).filter(Boolean));
  // 노드가 <b>전부</b> CDC 면 «CDC 워크플로우»다 - 종류를 따로 고르게 하지 않고 구성에서 읽는다.
  // CDC 워크플로우는 파이프라인에 지시만 쏘므로 스케줄도 연결선도 없다(2026-09-10 설계 방안 B).
  // «하나라도 있으면»으로 보던 때는, 스케줄 있는 ETL 워크플로우에 CDC 노드 하나를 놓고 저장하면 스케줄이
  // 경고 없이 지워졌다(2026-09-17 실측: 0 3 * * * → NULL). 섞인 상태는 게시 검증(V12)이 막고, 스케줄은 건드리지 않는다.
  const isCdcWorkflow = nodes.length > 0 && nodes.every((node) => node.data.nodeType === "CDC");
  const hasCdcNode = nodes.some((node) => node.data.nodeType === "CDC");
  const hasNonCdcNode = nodes.some((node) => node.data.nodeType !== "CDC");
  // 체인을 자식 PG로 나누면 이름이 겹친다(DW/DZ 아래 COM001M 등). 부모 그룹 자체는
  // 프로세서가 없어 잡 목록에 안 나오므로 백엔드가 채워준 이름을 그대로 쓴다.
  const parentNameOf = (job: EtlJobResponse) => job.parentGroupName ?? undefined;
  const jobByPgId = new Map((jobsQuery.data ?? []).map((job) => [job.nifiPgId, job]));

  /**
   * NiFi 그룹 트리를 팔레트 트리로 옮긴다. ETL 관리 화면과 같은 계층으로 보여야
   * "어느 그룹의 어느 체인인지"를 이름 중복 없이 알아볼 수 있다.
   *
   * <p>job(= etl_job에 잡힌 그룹)만 끌어다 캔버스에 놓을 수 있고, 상위 그룹은 누르면 접기/펼치기만 한다.
   * 모양은 워크플로우 관리 화면 트리와 같다(2026-09-11) - 그룹은 폴더, job 은 파일 아이콘, 배지는 하위 그룹 수.
   * 캔버스에 이미 놓인 job 은 이름 뒤에 «(배치됨)»을 흐리게 붙인다(예전의 ●/○ 자리).
   */
  const buildPaletteTree = (node: NifiProcessGroupTreeNode): PaletteTreeNode => {
    const job = jobByPgId.get(node.id);
    const children = (node.children ?? []).map(buildPaletteTree);
    const placed = job ? usedJobIdSet.has(job.id) : false;
    return {
      key: node.id,
      icon: job && !children.length ? "file" : "folder",
      label: placed ? <>{node.name}<span className="tree-node-note"> (배치됨)</span></> : node.name,
      title: job ? `${node.name}${placed ? " (배치됨)" : ""}\n끌어다 캔버스에 놓으면 추가됩니다` : node.name,
      // 하위 그룹 수만 표시한다(프로세서 수는 여기서 알 필요가 없다).
      badge: children.length || undefined,
      // job 도 하위 그룹도 없는 빈 그룹은 놓을 것이 없어 흐리게 둔다.
      muted: !job && !children.length,
      // 워크플로우 트리와 같은 조작으로 맞춘다 - 얹는 것은 «끌어다 놓기»로만 한다.
      onDragStart: job && canWrite
        ? (event) => {
            event.dataTransfer.setData("application/cerebro-job", String(job.id));
            event.dataTransfer.effectAllowed = "copy";
          }
        : undefined,
      children: children.length ? children : undefined,
    };
  };
  /**
   * 팔레트의 뿌리. ETL(NiFi) 그룹 계층 전체를 그대로 보여준다.
   *
   * <p>예전에는 워크플로우가 «소속 업무 그룹»을 갖고 그 그룹 아래 job 만 보여줬다(«전체 보기»로
   * 넓힐 수 있었다). 워크플로우에서 업무 그룹 개념을 걷어내면서 좁힐 기준이 없어졌다 -
   * 이제는 늘 전체를 보여주고, 필요한 job 은 폴더를 펼쳐 찾는다(팔레트 검색창은 2026-09-03 에 걷어냈다).
   */
  const paletteRoot = treeQuery.data;
  const paletteTree: PaletteTreeNode[] = paletteRoot ? [buildPaletteTree(paletteRoot)] : [];

  /**
   * 얹을 수 있는 워크플로우. 자기 자신은 뺀다(자기 안에 자기를 넣을 수 없다).
   *
   * <p>예전에는 NiFi 업무 그룹 계층으로 묶어 보여줬는데, 여기서 고르는 기준은 «무엇이
   * 무엇을 품고 있는가»다. 그래서 워크플로우 상하 계층으로 세운다 - 이름 오른쪽 숫자는
   * 그 워크플로우 캔버스에 놓인 job 수다. 목록 화면(관리)과 같은 모양의 트리를 쓴다(components/PaletteTree).
   *
   * <p>자기 자신을 빼면 그 하위였던 워크플로우는 부모를 잃는데, 빌더가 그런 것들을
   * 최상단으로 올려 주므로 목록에서 사라지지 않는다.
   */
  const allWorkflows = workflowsQuery.data ?? [];
  const canvasPath = parentWorkflowPathOf(allWorkflows, workflowId);
  // 선택한 워크플로우 노드의 상위 경로. 노드 속성창 제목에 이름과 함께 적는다.
  const selectedNodeParentPath = selectedNode?.data.subWorkflowId
    ? parentWorkflowPathOf(allWorkflows, Number(selectedNode.data.subWorkflowId))
    : [];
  // 얹을 수 있는 것은 «자기 자신을 뺀» 목록이다(자기 안에 자기를 넣을 수 없다).
  // 다만 트리에는 자기 자신도 그린다 - 지금 어디를 열고 있는지 보여야 길을 잃지 않는다.
  const selectableWorkflows = allWorkflows.filter((w) => w.id !== workflowId);

  /**
   * 워크플로우 구역의 줄. 모양은 워크플로우 관리 화면 트리와 같다(2026-09-11) - 하위가 있으면 폴더,
   * 없으면 파일 아이콘, 오른쪽 배지는 그 워크플로우 캔버스에 놓인 job 수.
   *
   * <p>줄을 한 번 누르면 그 워크플로우로 옮겨 간다. ▶ 는 접기만 한다. 캔버스에 얹는 것은
   * «끌어다 놓기»로만 한다 - 한 번 클릭으로 얹히면 트리를 훑어보다가 노드가 생겨 버려서, 지우고
   * 다시 저장하는 일이 잦았다. 끌면 click 이 안 나고 dragstart 가 나서 두 동작은 부딪히지 않는다.
   *
   * <p>지금 열려 있는 워크플로우는 관리 화면의 «고른 줄»과 같은 색으로 칠한다(예전의 «· 열림» 글자).
   * 자기 안에 자기를 넣을 수 없으니 끌 수 없고, 눌러도 옮겨 가지 않는다.
   */
  const toSubWorkflowNodes = (items: WorkflowTreeItem[]): PaletteTreeNode[] => items.map((item) => {
    const wf = item.workflow;
    const isCurrent = wf.id === workflowId;
    return {
      key: `wf:${wf.id}`,
      icon: item.children.length ? "folder" : "file",
      label: (
        <>
          {wf.name}
          {(wf.cdcNodeCount ?? 0) > 0 ? <span className="tree-node-note"> · CDC</span> : null}
          {wf.published ? null : <span className="tree-node-note"> (미게시)</span>}
        </>
      ),
      title: `${wf.name}${(wf.cdcNodeCount ?? 0) > 0 ? " · CDC 워크플로우(하위로 넣을 수 없음)" : ""}${wf.published ? "" : " (미게시)"}\n`
        + (isCurrent ? "지금 열려 있는 워크플로우" : "누르면 이 워크플로우를 엽니다"),
      badge: wf.jobCount,
      badgeTitle: "캔버스에 놓인 job 수",
      active: isCurrent,
      onClick: () => {
        if (!isCurrent) leaveTo(`/workflows/design/${wf.id}`);
      },
      onDragStart: canWrite && !isCurrent
        ? (event) => {
            event.dataTransfer.setData("application/cerebro-workflow", String(wf.id));
            event.dataTransfer.effectAllowed = "copy";
          }
        : undefined,
      children: item.children.length ? toSubWorkflowNodes(item.children) : undefined,
    };
  });
  const subWorkflowHierarchy = buildWorkflowHierarchy(allWorkflows);
  const subWorkflowTree = toSubWorkflowNodes(subWorkflowHierarchy);
  // 최상단은 펼쳐 둔다(= 하위 워크플로우까지 바로 보인다).
  const subWorkflowOpenKeys = subWorkflowTree.map((node) => node.key);

  /**
   * CDC 팔레트. CDC 관리 화면과 같은 계층으로 그린다.
   *
   * <p>지금은 «무엇이 있는지» 보여주기만 한다. 캔버스에 얹으려면 노드 종류(현재 JOB·SUBWF)에
   * CDC 를 더하고, 컴파일러가 그 노드를 Kafka Connect 시작/정지 태스크로 풀어내야 한다.
   * 그 전에 얹을 수 있게 해두면 저장은 되는데 게시가 깨진다.
   */
  const cdcTree: PaletteTreeNode[] = ((): PaletteTreeNode[] => {
    const pipelines = pipelinesQuery.data ?? [];
    const groups = pipelineGroupsQuery.data ?? [];
    const groupIds = new Set(groups.map((g) => g.id));
    const byGroup = new Map<number | null, PipelineResponse[]>();
    pipelines.forEach((pipeline) => {
      const key = pipeline.groupId != null && groupIds.has(pipeline.groupId) ? pipeline.groupId : null;
      byGroup.set(key, [...(byGroup.get(key) ?? []), pipeline]);
    });
    // 파이프라인 줄. job·워크플로우와 같은 조작이다 - 끌어다 놓으면 CDC 노드가 된다.
    const leaf = (pipeline: PipelineResponse): PaletteTreeNode => {
      const table = pipeline.sourceTable ?? pipeline.targetTable;
      const placed = usedPipelineIdSet.has(pipeline.id);
      return {
        key: `cdc:${pipeline.id}`,
        icon: "file",
        label: (
          <>
            {pipeline.name}
            {table ? <span className="tree-node-note"> · {table}</span> : null}
            {placed ? <span className="tree-node-note"> (배치됨)</span> : null}
          </>
        ),
        title: `${pipeline.name}${table ? ` · ${table}` : ""}${placed ? " (배치됨)" : ""}`
          + "\n끌어다 캔버스에 놓으면 추가됩니다",
        onDragStart: canWrite
          ? (event) => {
              event.dataTransfer.setData("application/cerebro-pipeline", String(pipeline.id));
              event.dataTransfer.effectAllowed = "copy";
            }
          : undefined,
      };
    };
    const branch = (group: PipelineGroupResponse): { node: PaletteTreeNode; count: number } => {
      const subs = groups.filter((g) => g.parentId === group.id).map(branch);
      const own = byGroup.get(group.id) ?? [];
      const count = own.length + subs.reduce((sum, sub) => sum + sub.count, 0);
      const children = [...subs.map((sub) => sub.node), ...own.map(leaf)];
      return {
        count,
        node: {
          key: `cdcgroup:${group.id}`,
          icon: "folder",
          label: group.name,
          title: group.name,
          badge: count,
          children: children.length ? children : undefined,
        },
      };
    };
    const roots = groups.filter((g) => g.parentId === null).map(branch);
    const ungrouped = (byGroup.get(null) ?? []).map(leaf);
    if (!pipelines.length) {
      return [];
    }
    return [{
      key: "cdc:all",
      icon: "folder",
      label: "전체 파이프라인",
      title: "전체 파이프라인",
      badge: pipelines.length,
      children: [...roots.map((root) => root.node), ...ungrouped],
    }];
  })();
  const cdcOpenKeys = ["cdc:all"];

  /**
   * 처음 펼쳐둘 가지. 최상단 하나만 연다(그 바로 아래 계층까지 «보이는» 상태).
   *
   * 전부 펼치면 job이 수십 개 쏟아져 어디를 봐야 할지 알 수 없다.
   */
  const defaultOpenKeys = paletteRoot ? [paletteRoot.id] : [];

  /**
   * 초안 AI 어시스턴트에 보낼 «작성 중 설계»(JSON). 질문을 보낼 때마다 새로 만든다 - 방금 놓은 노드 · 고친 스케줄까지 들어가게.
   * 저장 본문과 같은 재료(속성 입력 · 노드 · 연결)만 보내고, 이름 · 단계 같은 나머지는 서버가 id 로 찾아 붙인다.
   */
  const aiDraft = () => {
    const settings = settingsDraftRef.current;
    const graph = graphBody();
    return JSON.stringify({
      name: settings?.name ?? draftName,
      description: settings?.description ?? null,
      scheduleCron: settings?.scheduleCron ?? null,
      timezone: settings?.timezone ?? null,
      catchup: settings?.catchup ?? null,
      maxActiveRuns: settings?.maxActiveRuns ?? null,
      suspendOnError: settings?.suspendOnError ?? null,
      memo: settings?.memo ?? null,
      nodes: graph.nodes.map((node) => {
        const { displayX, displayY, ...rest } = node;
        void displayX; void displayY;
        return { ...rest, label: String(nodes.find((item) => item.id === node.nodeKey)?.data.label ?? node.nodeKey) };
      }),
      edges: graph.edges,
    });
  };
  // 속성 패널 «AI 어시스턴트» 탭: 노드를 골랐을 때(그 노드) 또는 아무것도 안 골랐을 때(이 워크플로우 전체).
  // 새로 만드는 중(초안)이면 화면의 작성 중 설계를 보낸다(WORKFLOW_DRAFT, 2026-09-18 요청).
  // 연결선을 볼 때는 없다. 선택된 GPT 가 없으면 어디서도 안 보인다.
  const aiTabAvailable = gptEnabled && (selectedNode ? true : !selectedEdge);
  const aiTabActive = aiTabAvailable && nodePanelTab === "gpt";

  if (!isDraft && workflowQuery.isLoading) {
    return <Card><Spin /> 워크플로우를 불러오는 중입니다.</Card>;
  }
  if (!detail) {
    return <Card><Empty description="워크플로우를 찾을 수 없습니다." /></Card>;
  }

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Card
        size="small"
        className="wf-canvas-shell"
        title={
          <Space>
            <Button size="small" icon={<ArrowLeftOutlined />} onClick={() => leaveTo("/workflows/design")} />
            <strong>{isDraft ? (draftName.trim() || "새 워크플로우") : detail.name}</strong>
            {/* 상위 워크플로우 경로. 하위 워크플로우를 열었을 때 «어디에 속한 것인지»가 보인다.
                자기 이름은 바로 옆 제목에 있어서 상위까지만 적는다(2026-09-11 요청). */}
            {canvasPath.length > 0 && (
              <span style={{ color: "#888", fontSize: 12 }}>{canvasPath.join(" > ")}</span>
            )}
            {/* 초안(생성 중)에는 안내 태그를 달지 않는다(2026-09-11 요청). 이름 없이 «저장»하면
                이름을 적으라는 메시지가 뜬다. */}
            {!isDraft && (
              <>
                {/* DAG ID(etl_wf_…) 태그는 제목과 겹쳐 보여 뺐다(2026-09-11 요청). 로그 화면 속성창에서 볼 수 있다. */}
                {detail.published ? <Tag color="success">게시됨</Tag> : <Tag>미게시 (DAG 없음)</Tag>}
                {(dirty || detail.dirty) && detail.published && <Tag color="warning">게시본과 다름</Tag>}
                {dirty && <Tag color="processing">저장 안 됨</Tag>}
              </>
            )}
          </Space>
        }
        extra={
          <Space>
            <Button onClick={() => saveMutation.mutate()} loading={saveMutation.isPending} disabled={!canWrite}>
              저장
            </Button>
            {/* 초안은 서버에 없어 검증·게시할 대상이 아직 없다. 먼저 «저장»으로 만든다. */}
            {!isDraft && (
              <Button onClick={() => validateMutation.mutate()} loading={validateMutation.isPending}>
                검증
              </Button>
            )}
            {!isDraft && (
              <Button type="primary" onClick={() => publishMutation.mutate()}
                      loading={publishMutation.isPending} disabled={!canPublish || dirty}
                      title={canPublish ? undefined : "Airflow 쓰기 권한이 없습니다"}>
                게시
              </Button>
            )}
            {!isDraft && detail.published && (
              <Button danger onClick={() => unpublishMutation.mutate()}
                      loading={unpublishMutation.isPending} disabled={!canPublish}
                      title={canPublish ? undefined : "Airflow 쓰기 권한이 없습니다"}>
                게시 취소
              </Button>
            )}
          </Space>
        }
      >
        {/* 저장 여부는 제목 옆 «저장 안 됨»·«게시본과 다름» 태그가 이미 말해 준다.
            같은 말을 배너로 또 하면 캔버스만 밀려 내려가서 없앴다(2026-09-03). */}
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

        {/* 양옆 패널은 ETL 관리 화면처럼 접고 너비를 끌어 바꾼다(2026-09-18 요청). 캔버스가 남은 폭을 가져간다. */}
        <div className="wf-canvas-grid"
             style={{ display: "grid", gridTemplateColumns: `${paletteDock.column} minmax(0, 1fr) ${detailDock.column}`, gap: 12 }}>
          <SideDock side="left" dock={paletteDock} label="트리 패널">
          {/* 좌: job 팔레트 - ETL(NiFi)에서 만들어 둔 job 목록.
              모양은 ETL 관리 화면 왼쪽 트리 패널(nifi-tree-panel)과 같다(2026-09-17 요청). 트리 줄 · 점선은 그대로. */}
          <aside className="nifi-tree-panel wf-palette-panel" aria-label="job 팔레트">
            <div className="nifi-tree-search-heading">
              <span className="nifi-tree-section-title">job 팔레트</span>
              <Button type="text" size="small" className="nifi-tree-refresh-button" icon={<SyncOutlined />}
                      title="ETL에서 다시 읽어옵니다" aria-label="ETL에서 다시 읽어옵니다"
                      loading={syncMutation.isPending}
                      onClick={() => syncMutation.mutate()} />
            </div>
            <div className="nifi-tree-scroll wf-palette-scroll">
            {/* 워크플로우도 하나의 노드로 얹을 수 있다. 그래야 «daily = monthly 끝나면
                years» 같은 조립이 가능하다. 자기 자신은 넣을 수 없다(무한 중첩).
                조립이 이 화면의 관심사라 job 목록보다 위에 둔다. */}
            <section className="wf-palette-section">
              <div className="wf-palette-section-title">워크플로우</div>
              {subWorkflowTree.length === 0 ? (
                <Typography.Text type="secondary" style={{ fontSize: 12, paddingLeft: 8 }}>
                  얹을 수 있는 다른 워크플로우가 없습니다.
                </Typography.Text>
              ) : (
                // 세 구역 모두 워크플로우 관리 화면 트리와 같은 모양이다(components/PaletteTree, 2026-09-11).
                <PaletteTree ariaLabel="워크플로우" nodes={subWorkflowTree} defaultOpenKeys={subWorkflowOpenKeys} />
              )}
            </section>
            <section className="wf-palette-section">
              <div className="wf-palette-section-title">ETL 태스크</div>
              {treeQuery.isLoading ? (
                <Spin size="small" />
              ) : paletteTree.length === 0 ? (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="job이 없습니다" />
              ) : (
                <PaletteTree ariaLabel="ETL 태스크" nodes={paletteTree} defaultOpenKeys={defaultOpenKeys} />
              )}
            </section>
            <section className="wf-palette-section">
              <div className="wf-palette-section-title">CDC</div>
              {cdcTree.length === 0 ? (
                <Typography.Text type="secondary" style={{ fontSize: 12, paddingLeft: 8 }}>
                  등록된 CDC 파이프라인이 없습니다.
                </Typography.Text>
              ) : (
                // 아직 캔버스에 얹을 수 없다(노드 종류에 CDC 가 없다). 그래서 줄은 끌 수 없게 두고
                // 흐리게만 그린다. 화면에 따로 적지는 않는다.
                <PaletteTree ariaLabel="CDC" nodes={cdcTree} defaultOpenKeys={cdcOpenKeys} />
              )}
            </section>
            </div>
          </aside>
          </SideDock>

          {/* 중: 캔버스 - 노드를 잇는 선이 곧 실행 순서다 */}
          <div className="console-frame-shell wf-canvas-frame">
            <ReactFlow
              // 팔레트에서 끌어온 워크플로우를 놓은 자리에 얹는다.
              onDragOver={(event) => {
                const kinds = event.dataTransfer.types;
                if (kinds.includes("application/cerebro-workflow")
                    || kinds.includes("application/cerebro-job")
                    || kinds.includes("application/cerebro-pipeline")) {
                  event.preventDefault();
                  event.dataTransfer.dropEffect = "copy";
                }
              }}
              onDrop={(event) => {
                if (!canWrite) return;
                const at = () => (flow?.screenToFlowPosition
                  ? flow.screenToFlowPosition({ x: event.clientX, y: event.clientY })
                  : undefined);
                // CDC 노드와 ETL job·하위 워크플로우는 한 워크플로우에 섞지 않는다. 실행 방식이 다르다 - ETL 은 순서대로
                // 돌고 끝나기를 기다리지만, CDC 는 지시만 쏘고 끝난다. 섞이면 워크플로우 전체가 CDC 로 취급돼 스케줄·완료
                // 신호가 꺼진다(2026-09-17 확인). 게시 검증(V12)도 막지만, 놓는 순간 막아야 «그렸는데 게시에서 거절»이 안 된다.
                const workflowRaw = event.dataTransfer.getData("application/cerebro-workflow");
                if (workflowRaw) {
                  event.preventDefault();
                  const picked = selectableWorkflows.find((w) => w.id === Number(workflowRaw));
                  if (picked && (picked.cdcNodeCount ?? 0) > 0) {
                    message.warning("CDC 워크플로우는 하위 워크플로우로 넣을 수 없습니다. 상위가 돌 때마다 CDC 시작 지시가 나갑니다 - 실시간 모니터링의 «실행 설정»에서 따로 실행하세요.");
                    return;
                  }
                  if (hasCdcNode) {
                    message.warning("CDC 워크플로우에는 하위 워크플로우를 놓을 수 없습니다. CDC 노드만 둡니다.");
                    return;
                  }
                  if (picked) addSubWorkflowNode(picked, at());
                  return;
                }
                const pipelineRaw = event.dataTransfer.getData("application/cerebro-pipeline");
                if (pipelineRaw) {
                  event.preventDefault();
                  if (hasNonCdcNode) {
                    message.warning("ETL job·하위 워크플로우가 있는 워크플로우에는 CDC 노드를 놓을 수 없습니다. CDC 는 워크플로우를 따로 만드세요.");
                    return;
                  }
                  const pipeline = (pipelinesQuery.data ?? []).find((p) => p.id === Number(pipelineRaw));
                  if (pipeline) addCdcNode(pipeline, at());
                  return;
                }
                const jobRaw = event.dataTransfer.getData("application/cerebro-job");
                if (jobRaw) {
                  event.preventDefault();
                  if (hasCdcNode) {
                    message.warning("CDC 워크플로우에는 ETL job 을 놓을 수 없습니다. CDC 노드만 둡니다.");
                    return;
                  }
                  const job = (jobsQuery.data ?? []).find((j) => j.id === Number(jobRaw));
                  if (job) addJobNode(job, parentNameOf(job), at());
                }
              }}
              nodes={nodes}
              edges={edges}
              onNodesChange={(changes) => { onNodesChange(changes); if (isUserEdit(changes)) markDirty(); }}
              onEdgesChange={(changes) => { onEdgesChange(changes); if (isUserEdit(changes)) markDirty(); }}
              onConnect={onConnect}
              onNodeClick={(_, node) => { setSelectedNodeId(node.id); setSelectedEdgeId(undefined); }}
              // 더블클릭 = 그 노드가 가리키는 곳으로 이동. 속성창의 «바로가기» 버튼 두 개와
              // 같은 자리로 간다(워크플로우 노드 → 그 설계화면, job 노드 → ETL 관리의 그 job).
              onNodeDoubleClick={(_, node) => {
                const data = node.data as { subWorkflowId?: unknown; nifiPgId?: unknown };
                if (data.subWorkflowId) {
                  leaveTo(`/workflows/design/${Number(data.subWorkflowId)}`);
                  return;
                }
                if (data.nifiPgId) {
                  leaveTo(`/etl/manage?processGroupId=${encodeURIComponent(String(data.nifiPgId))}`);
                }
              }}
              onEdgeClick={(_, edge) => { setSelectedEdgeId(edge.id); setSelectedNodeId(undefined); }}
              onPaneClick={() => { setSelectedNodeId(undefined); setSelectedEdgeId(undefined); }}
              className="wf-flow"
              fitView
              // 처음 열면 노드가 코앞에 있는 것처럼 크게 잡혀 부담스러웠다.
              // 한 화면에 전체가 여유 있게 들어오도록 확대 상한과 여백을 준다.
              fitViewOptions={{ padding: 0.35, maxZoom: 0.85 }}
              minZoom={0.15}
              nodeTypes={NODE_TYPES}
              onInit={(instance) => setFlow(instance)}
              deleteKeyCode={["Backspace", "Delete"]}
              nodesDraggable={interactive}
              nodesConnectable={interactive}
              elementsSelectable={interactive}
            >
              {/* 배경 점(dot grid)은 뺐다 - 흰 바탕이 노드와 선을 더 또렷하게 한다. */}
              {/*
                기본 컨트롤 넷은 툴팁이 영문(Zoom in/Zoom out/Fit view/Toggle interactivity)으로
                고정돼 있어 우리가 바꿀 수 없다. 그래서 전부 끄고 같은 자리에 직접 만든다.
                «정렬»을 맨 위에 둔 것은 캔버스 아래 따로 떠 있던 버튼을 여기로 합친 것이다.
              */}
              <Controls showZoom={false} showFitView={false} showInteractive={false}>
                <ControlButton title="정렬" aria-label="정렬"
                               disabled={!canWrite} onClick={alignHorizontally}>
                  <ColumnWidthOutlined />
                </ControlButton>
                <ControlButton title="확대" aria-label="확대" onClick={() => flow?.zoomIn()}>
                  <PlusOutlined />
                </ControlButton>
                <ControlButton title="축소" aria-label="축소" onClick={() => flow?.zoomOut()}>
                  <MinusOutlined />
                </ControlButton>
                <ControlButton title="전체 보기" aria-label="전체 보기"
                               onClick={() => flow?.fitView({ padding: 0.35, maxZoom: 0.85 })}>
                  <ExpandOutlined />
                </ControlButton>
                <ControlButton title={interactive ? "편집 잠금" : "편집 잠금 해제"}
                               aria-label={interactive ? "편집 잠금" : "편집 잠금 해제"}
                               onClick={() => setInteractive((current) => !current)}>
                  {interactive ? <UnlockOutlined /> : <LockOutlined />}
                </ControlButton>
              </Controls>
              <MiniMap pannable zoomable />
              {/* 캔버스 위 «메모» 상자는 뺐다(2026-09-03) - 속성창에 같은 메모 칸이 있어
                  두 곳에 같은 것이 떠 있었고, 캔버스 좌상단을 늘 가리고 있었다. */}
            </ReactFlow>
          </div>

          {/* 우: 속성 - 워크플로우 전체 / 선택 노드.
              모양은 ETL 관리 화면 오른쪽 상세 패널(nifi-detail-panel)과 같다(2026-09-17 요청): 제목 줄 → «상세정보 | AI 어시스턴트»
              탭 → 내용. AI 어시스턴트는 노드를 골랐고 선택된 GPT 가 있을 때만(예전 [GPT] 탭과 같은 조건). */}
          <SideDock side="right" dock={detailDock} label="속성 패널">
          <aside className={`nifi-detail-panel wf-detail-panel${aiTabActive ? " nifi-detail-panel--ai" : ""}`}
                 aria-label="속성">
          <div className="nifi-detail-header">
          <div className="nifi-detail-title">{
            selectedNode ? (
              // 노드 키(wf_dz_com_daily 같은 id) 대신 워크플로우·job 이름을 적는다(2026-09-11 요청).
              // 워크플로우 노드는 상위 경로를 설계 화면 머리줄처럼 이름 옆에 흐리게 붙인다.
              <Space size={6}>
                <span>노드: {String(selectedNode.data.label)}</span>
                {selectedNodeParentPath.length > 0 && (
                  <span style={{ color: "#888", fontSize: 12, fontWeight: 400 }}>상위 워크플로우 · {selectedNodeParentPath.join(" > ")}</span>
                )}
              </Space>
            )
              : selectedEdge ? "연결 조건"
              : (
                <Space size={6}>
                  <span>워크플로우 속성</span>
                  {/* 상위가 있으면 자체 스케줄이 안 돈다. 예전엔 이 설명을 스케줄 위에
                      큰 알림으로 깔아 패널을 절반이나 잡아먹었다. 아이콘 하나로 줄이고
                      필요할 때만 펴 본다. */}
                  {/* DAG 생성기는 «게시된» 상위가 있을 때만 자체 스케줄을 끈다. */}
                  {detail && publishedParentsOf(detail).length > 0 && (
                    <Tooltip
                      title={
                        <span>
                          상위 워크플로우가 있어 이 스케줄은 동작하지 않습니다.<br />
                          상위가 지시할 때만 실행됩니다. 같은 적재가 두 번 돌지 않도록
                          자체 스케줄을 끕니다.
                        </span>
                      }
                    >
                      <InfoCircleFilled style={{ color: "#1677ff" }} />
                    </Tooltip>
                  )}
                </Space>
              )}
          </div>
          </div>
          <div className="nifi-detail-tabs" role="tablist" aria-label="속성 패널 탭">
            <button type="button" role="tab"
                    aria-selected={!(aiTabActive)}
                    className={`nifi-detail-tab${aiTabActive ? "" : " active"}`}
                    onClick={() => setNodePanelTab("props")}>
              상세정보
            </button>
            {aiTabAvailable ? (
              <button type="button" role="tab" aria-selected={nodePanelTab === "gpt"}
                      className={`nifi-detail-tab${nodePanelTab === "gpt" ? " active" : ""}`}
                      onClick={() => setNodePanelTab("gpt")}>
                AI 어시스턴트
              </button>
            ) : null}
          </div>
          <div className={aiTabActive ? "wf-detail-ai" : "wf-detail-body"}>
            {selectedNode ? (() => {
              const nodeProps = (
              <div className="wf-node-detail">
                      {/* 종류·이름·상위 경로 줄은 뺐다(2026-09-11 요청) - 이름과 상위 경로는 카드 제목에 있다. */}
                      {/* 정보 구역은 ETL 관리 화면 job 속성창과 같은 «제목 + 아래 구분선» 모양이다(2026-09-11 요청).
                          job 은 그쪽 컴포넌트를 그대로 쓰고(연결 DAG 만 뺌), 워크플로우 노드는 같은 모양으로 그린다. */}
                      {selectedNode.data.jobId && !selectedNode.data.subWorkflowId ? (
                        <EtlJobSummary
                          jobId={Number(selectedNode.data.jobId)}
                          nifiPgId={selectedNode.data.nifiPgId ? String(selectedNode.data.nifiPgId) : null}
                          onNavigate={leaveTo}
                        />
                      ) : null}
                      {selectedNode.data.subWorkflowId ? (
                        <SubWorkflowSummary
                          workflowId={Number(selectedNode.data.subWorkflowId)}
                          // «최근 실행»은 이 캔버스의 워크플로우가 이 노드로 부른 실행만 보여 준다.
                          calledBy={{ dagId: detail.dagId, published: !isDraft && detail.published, nodeKey: selectedNode.id }}
                        />
                      ) : null}
                      {selectedNode.data.nodeType === "CDC" ? (
                        <CdcNodeSummary
                          pipeline={(pipelinesQuery.data ?? []).find(
                            (pipeline) => pipeline.id === Number(selectedNode.data.pipelineId))}
                        />
                      ) : null}
                      {/* 구분선 아래는 이 캔버스에서 고치는 값(실행 조건)과 바로가기다. */}
                      <Space direction="vertical" style={{ width: "100%" }} size={10} className="wf-node-detail-controls">
                        {/* CDC 노드는 잇지 않으므로 «선행 조건»이 없다 - 고를 것이 없는 칸은 그리지 않는다. */}
                        {selectedNode.data.nodeType !== "CDC" && (
                        <div>
                          <div style={{ fontSize: 12, color: "#888" }}>실행 조건</div>
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
                        )}
                        {/* CDC 노드면 그 파이프라인 상세(재시작·싱크 설정 재적용·멈춘 한 건 건너뛰기)로 바로 간다. */}
                        {selectedNode.data.nodeType === "CDC" && selectedNode.data.pipelineId ? (
                          <Button size="small" block
                                  onClick={() => leaveTo(
                                    `/cdc/pipelines?pipelineId=${Number(selectedNode.data.pipelineId)}`)}>
                            CDC 파이프라인 바로가기
                          </Button>
                        ) : null}
                        {/* «워크플로우 바로가기» 버튼은 뺐다(2026-09-18 요청). 하위 워크플로우 캔버스는 노드를 더블클릭하면 열린다. */}
                        {/* 이 job이 NiFi에서 실제로 어떻게 생겼는지 바로 열어본다. */}
                        {selectedNode.data.nifiPgId ? (
                          <Button size="small" block
                                  onClick={() => leaveTo(
                                    `/etl/manage?processGroupId=${encodeURIComponent(String(selectedNode.data.nifiPgId))}`)}>
                            ETL job 바로가기
                          </Button>
                        ) : null}
                      </Space>
                    </div>
              );
              if (!gptEnabled || nodePanelTab !== "gpt") return nodeProps;
              // 노드를 바꾸면 대화를 새로 시작한다(key). 서버는 저장된 설계 기준으로 정보를 모은다.
              // CDC 노드는 «그 파이프라인»에 대해 묻는 것과 같다 - CDC 관리 화면의 AI 어시스턴트와 같은 정보를 모은다.
              return selectedNode.data.nodeType === "CDC" && selectedNode.data.pipelineId ? (
                        <GptChatPanel
                          key={`${workflowId}:${selectedNode.id}`}
                          targetType="CDC_PIPELINE"
                          target={{ pipelineId: String(selectedNode.data.pipelineId) }}
                          targetNoun="이 CDC 파이프라인"
                        />
                      ) : (
                        <GptChatPanel
                          key={`${workflowId}:${selectedNode.id}`}
                          targetType="WORKFLOW_NODE"
                          target={{
                            ...(isDraft ? {} : { workflowId: String(workflowId) }),
                            nodeKey: selectedNode.id,
                            ...(selectedNode.data.jobId ? { jobId: String(selectedNode.data.jobId) } : {}),
                            ...(selectedNode.data.subWorkflowId ? { subWorkflowId: String(selectedNode.data.subWorkflowId) } : {}),
                          }}
                          targetNoun="이 노드"
                          notice={dirty ? "저장하지 않은 변경(연결·실행 조건 등)은 반영되지 않습니다. 저장된 설계 기준으로 답합니다." : undefined}
                        />
                      );
            })() : selectedEdge ? (
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
              <>
              {aiTabActive && (isDraft ? (
                // 새로 만드는 중: 서버에 아직 없어 화면의 작성 중 설계(속성 입력 · 노드 · 연결)를 보낸다(WORKFLOW_DRAFT).
                <GptChatPanel
                  key="workflow:draft"
                  targetType="WORKFLOW_DRAFT"
                  target={{ mode: "draft" }}
                  draft={aiDraft}
                />
              ) : (
                // 워크플로우 전체에 대해 묻는다(서버 WORKFLOW 수집기: 구성 · 상위 · 스케줄 · 게시). 워크플로우마다 대화 새로 시작.
                <GptChatPanel
                  key={`workflow:${workflowId}`}
                  targetType="WORKFLOW"
                  target={{ workflowId: String(workflowId) }}
                  targetNoun="이 워크플로우"
                  notice={dirty ? "저장하지 않은 변경은 반영되지 않습니다. 저장된 설계 기준으로 답합니다." : undefined}
                />
              ))}
              {/* AI 탭을 보는 동안에도 속성 입력은 지우지 않고 숨긴다 - 지웠다 다시 그리면 초안에 적던 설명 · 스케줄이 빈 값으로 돌아간다. */}
              <div style={aiTabActive ? { display: "none" } : undefined}>
              <WorkflowSettings
                                // 같은 화면 컴포넌트가 다른 워크플로우로 재사용될 때(더블클릭 이동, 생성 → 저장 후
                                // 그 id 로, 다시 «생성») 속성창 입력 상태가 남지 않도록 워크플로우마다 새로 만든다.
                                key={isDraft ? "draft" : `wf-${workflowId}`}
                                detail={detail} disabled={!canWrite} cdc={isCdcWorkflow}
                                onSave={(body) => {
                                  // 초안은 «속성 저장»도 생성이다(따로 갱신할 대상이 없다).
                                  settingsDraftRef.current = body;
                                  if (isDraft) saveMutation.mutate(); else settingsMutation.mutate(body);
                                }}
                                saving={isDraft ? saveMutation.isPending : settingsMutation.isPending}
                                onDraftChange={(body) => {
                                  settingsDraftRef.current = body;
                                  if (isDraft) {
                                    // 머리줄에 이름을 비추고, 뭔가 적었으면 이탈 확인이 걸리게 한다.
                                    setDraftName(body.name ?? "");
                                    if ((body.name ?? "").trim()) markDirty();
                                  }
                                }} />
              </div>
              </>
            )}
          </div>
          </aside>
          </SideDock>
        </div>
      </Card>
    </Space>
  );
}

/**
 * 캔버스 노드. 연결점은 <b>오른쪽(나가는 곳)과 왼쪽(들어오는 곳)</b> 둘뿐이다.
 *
 * 네 방향을 다 열어 두니 어느 점을 잡았는지 헷갈리고 선이 사방으로 뻗어 그림이 어지러웠다.
 * 흐름을 «왼쪽에서 오른쪽»으로 고정하면 읽기도 쉽고 조준도 한 곳이면 된다.
 * 점은 평소 숨어 있다가 노드에 마우스를 올리면 나타난다(CSS).
 */
function JobNode({ data, isConnectable }: {
  data: { label?: string; nodeType?: string };
  isConnectable?: boolean;
}) {
  // 왼쪽 띠에 종류를 적는다. 색만으로는 job 과 워크플로우가 헷갈렸다(파랑·보라 차이뿐이었다).
  const nodeType = data?.nodeType;
  const kind = nodeType === "SUBWF" ? "wf" : nodeType === "CDC" ? "cdc" : "job";
  // CDC 노드는 잇지 못하므로 연결점 자체를 그리지 않는다 - 잡히지도 않는 점이 뜨면
  // «이을 수 있는데 안 되는» 것처럼 보인다(막는 것은 onConnect 와 서버 검증이 이미 한다).
  if (nodeType === "CDC") {
    return (
      <>
        <b className="wf-node__kind">{kind}</b>
        <span>{String(data?.label ?? "")}</span>
      </>
    );
  }
  return (
    <>
      <Handle type="target" position={Position.Left} id="t-left" isConnectable={isConnectable} />
      <b className="wf-node__kind">{kind}</b>
      <span>{String(data?.label ?? "")}</span>
      <Handle type="source" position={Position.Right} id="s-right" isConnectable={isConnectable} />
    </>
  );
}

const NODE_TYPES = { job: JobNode };

/**
 * 연결된 노드들을 첫 노드 기준으로 가로 한 줄로 편다(순수 함수 - 상태를 건드리지 않는다).
 *
 * <p>«정렬» 버튼과 «화면을 열 때의 자동 정렬»이 같은 규칙을 쓰도록 여기로 뽑았다. 버튼은
 * 결과를 넣고 dirty 를 켜지만, 자동 정렬은 보기 좋게 펴 줄 뿐 «저장 안 됨»을 만들면 안 된다.
 */
function layoutHorizontally(current: Node[], edges: Edge[]): Node[] {
  if (current.length === 0) {
    return current;
  }
  const outgoing = new Map<string, string[]>();
  const indegree = new Map<string, number>();
  current.forEach((node) => indegree.set(node.id, 0));
  edges.forEach((edge) => {
    outgoing.set(edge.source, [...(outgoing.get(edge.source) ?? []), edge.target]);
    indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
  });
  const connected = new Set<string>();
  edges.forEach((edge) => { connected.add(edge.source); connected.add(edge.target); });

  // 위상 정렬로 «몇 번째 칸»인지 정한다. 갈래가 갈리면 같은 칸에 위아래로 쌓는다.
  const column = new Map<string, number>();
  const queue = current.filter((node) => connected.has(node.id) && !indegree.get(node.id))
    .map((node) => node.id);
  queue.forEach((id) => column.set(id, 0));
  const pending = new Map(indegree);
  while (queue.length) {
    const id = queue.shift()!;
    for (const next of outgoing.get(id) ?? []) {
      column.set(next, Math.max(column.get(next) ?? 0, (column.get(id) ?? 0) + 1));
      pending.set(next, (pending.get(next) ?? 1) - 1);
      if ((pending.get(next) ?? 0) === 0) {
        queue.push(next);
      }
    }
  }

  // 이어진 노드끼리 한 덩어리로 묶는다(방향 무시 - 갈래가 합류해도 한 줄이다).
  const neighbors = new Map<string, string[]>();
  const link = (from: string, to: string) =>
    neighbors.set(from, [...(neighbors.get(from) ?? []), to]);
  edges.forEach((edge) => { link(edge.source, edge.target); link(edge.target, edge.source); });

  const nodeById = new Map(current.map((node) => [node.id, node]));
  const seen = new Set<string>();
  const chains: string[][] = [];
  for (const node of current) {
    if (!column.has(node.id) || seen.has(node.id)) {
      continue;
    }
    const members: string[] = [];
    const stack = [node.id];
    seen.add(node.id);
    while (stack.length) {
      const id = stack.pop()!;
      members.push(id);
      for (const next of neighbors.get(id) ?? []) {
        if (column.has(next) && !seen.has(next)) {
          seen.add(next);
          stack.push(next);
        }
      }
    }
    chains.push(members);
  }
  // 지금 위에 있던 덩어리가 정렬 후에도 위에 오게 한다(순서가 뒤집히면 어디로 갔나 찾게 된다).
  // 기준은 «시작 노드»의 y다. 덩어리 최소 y를 쓰면 위로 끌어올려 둔 끝 노드 하나 때문에
  // 그 줄 전체가 남의 위로 올라가 버린다.
  const startY = (members: string[]) => {
    const head = [...members].sort((a, b) =>
      (column.get(a)! - column.get(b)!)
      || ((nodeById.get(a)?.position.y ?? 0) - (nodeById.get(b)?.position.y ?? 0)))[0];
    return nodeById.get(head)?.position.y ?? 0;
  };
  chains.sort((a, b) => startY(a) - startY(b));

  // 덩어리마다 줄을 배정한다. 한 덩어리 안에서 갈래가 갈리면 같은 칸에 위아래로 쌓되,
  // 그만큼만 줄을 차지하고 다음 덩어리는 그 아래에서 시작한다.
  const rowOfNode = new Map<string, number>();
  let rowCursor = 0;
  for (const members of chains) {
    const usedInColumn = new Map<number, number>();
    let height = 1;
    const ordered = [...members].sort((a, b) =>
      (column.get(a)! - column.get(b)!)
      || ((nodeById.get(a)?.position.y ?? 0) - (nodeById.get(b)?.position.y ?? 0)));
    for (const id of ordered) {
      const col = column.get(id)!;
      const offset = usedInColumn.get(col) ?? 0;
      usedInColumn.set(col, offset + 1);
      rowOfNode.set(id, rowCursor + offset);
      height = Math.max(height, offset + 1);
    }
    rowCursor += height;
  }

  const base = current.find((node) => connected.has(node.id)) ?? current[0];
  const loose: string[] = [];
  const placed = current.map((node) => {
    if (!column.has(node.id)) {
      loose.push(node.id);
      return node;
    }
    return {
      ...node,
      position: {
        x: base.position.x + column.get(node.id)! * 260,
        y: base.position.y + rowOfNode.get(node.id)! * 110,
      },
    };
  });
  // 연결 없는 노드는 아래 줄에 나란히.
  const looseY = base.position.y + Math.max(rowCursor, 1) * 110 + 90;
  return placed.map((node) => {
    const index = loose.indexOf(node.id);
    return index < 0 ? node
      : { ...node, position: { x: base.position.x + index * 260, y: looseY } };
  });
}

/**
 * 캔버스에서 고른 CDC 노드의 요약(읽기 전용). 워크플로우 노드 요약과 같은 «제목 + 구분선» 모양이다.
 *
 * <p>여기서 보여 주는 상태는 저장된 상태다. 실제로 돌고 있는지·실패했는지는 실시간 모니터링과
 * CDC 관리 화면이 커넥터에서 읽어 보여 준다 - 설계 화면은 «무엇을 가리키는지»까지만 맡는다.
 */
function CdcNodeSummary({ pipeline }: { pipeline?: PipelineResponse }) {
  // 커넥터 실제 상태. 로그 화면 CDC 속성창과 같은 조회 · 표시(renderRuntimeStatus)를 쓴다(2026-09-18 요청).
  const runtimeQuery = useQuery({ queryKey: ["pipeline-runtime-statuses"], queryFn: listPipelineRuntimeStatuses });
  if (!pipeline) {
    return (
      <div className="wf-subworkflow-summary">
        <section className="nifi-detail-section">
          <h3>CDC 파이프라인</h3>
          <div className="nifi-detail-message error">
            가리키던 CDC 파이프라인을 찾을 수 없습니다. 노드를 지우고 다시 얹어 주세요.
          </div>
        </section>
      </div>
    );
  }
  const runtime = runtimeQuery.data?.find((row) => row.pipelineId === pipeline.id);
  // 로그 화면(AirflowDashboardPage PropertyPanel) CDC 속성창 «기본정보»와 같은 항목 · 같은 표시.
  return (
    <div className="wf-subworkflow-summary">
      <section className="nifi-detail-section">
        <h3>기본정보</h3>
        <dl className="wf-cdc-basic">
          <div><dt>이름</dt><dd>{pipeline.name}</dd></div>
          <div><dt>유형</dt><dd>{pipeline.pipelineType}</dd></div>
          <div><dt>소스</dt><dd>{sourceLabel(pipeline)}</dd></div>
          <div><dt>타겟</dt><dd>{targetLabel(pipeline)}</dd></div>
          <div><dt>Topic</dt><dd>{pipeline.topicName}</dd></div>
          {pipeline.pipelineType === "TABLE_CDC" && (
            <>
              <div><dt>스냅샷 모드</dt><dd>{pipeline.snapshotMode === "NO_DATA" ? "기존 데이터 미적재 · 이후 CDC" : "초기 적재 후 CDC"}</dd></div>
              <div><dt>적재 방식</dt><dd>{loadModeLabel(pipeline.loadMode, pipeline.deltaOpColumn)}</dd></div>
            </>
          )}
          <div><dt>커넥터 상태</dt><dd>{renderRuntimeStatus(pipeline, runtime)}</dd></div>
          <div><dt>설명</dt><dd>{pipeline.description ?? "-"}</dd></div>
          <div><dt>생성 시각</dt><dd>{pipeline.createdAt}</dd></div>
          <div><dt>수정 시각</dt><dd>{pipeline.updatedAt}</dd></div>
        </dl>
      </section>
      <section className="nifi-detail-section">
        <h3>실행 방식</h3>
        <div className="wf-node-detail-sub">
          연결 없이, 워크플로우를 실행할 때 고른 지시(시작·중지·재시작·배포)를 받습니다.
          이미 그 상태면 건너뜁니다.
        </div>
      </section>
    </div>
  );
}

/** 노드 겉모습. 워크플로우 노드는 job과 한눈에 구분돼야 한다(보라 계열 + 왼쪽 굵은 띠). */
function nodeClass(problem: boolean, nodeType?: string): string {
  const kind = nodeType === "SUBWF" ? " wf-node--subwf"
    : nodeType === "CDC" ? " wf-node--cdc"
    : " wf-node--job";
  return problem ? `wf-node${kind} wf-node--problem` : `wf-node${kind}`;
}

/**
 * 캔버스에 얹은 «워크플로우 노드»를 눌렀을 때 보여주는 그 워크플로우의 요약(읽기 전용).
 *
 * <p>예전에는 이름과 진입 조건만 나와서, 이 하위 워크플로우가 언제 도는지·누가 위에
 * 있는지를 알려면 캔버스를 옮겨 다녀야 했다. 값은 그 워크플로우의 것이므로 여기서는
 * 고치지 않는다 - 고치려면 «워크플로우 바로가기»로 넘어가 그 캔버스에서 저장한다.
 */
function SubWorkflowSummary({ workflowId, calledBy }: {
  workflowId: number;
  calledBy: { dagId: string; published: boolean; nodeKey: string };
}) {
  const query = useQuery({
    queryKey: ["workflow", workflowId],
    queryFn: () => getWorkflow(workflowId),
  });
  const detail = query.data;
  if (query.isLoading) {
    return <div className="nifi-detail-message">불러오는 중</div>;
  }
  if (!detail) {
    return <div className="nifi-detail-message error">정보를 불러오지 못했습니다</div>;
  }
  const parsed = parseCronToPreset(detail.scheduleCron);
  const parents = detail.parents ?? [];
  const manual = !detail.scheduleCron;
  // 게시된 상위가 있으면 자체 스케줄은 꺼져 있다 - 꺼진 스케줄의 «다음 2회 실행»을 보여 주면 그 시각에 도는 것처럼 읽힌다.
  const suppressed = publishedParentsOf(detail).length > 0;
  // 내용(스케줄·다음 실행·상위 워크플로우·메모)은 그대로 두고, 모양만 job 노드와 같은
  // «제목 + 아래 구분선» 구역으로 맞췄다(2026-09-11 요청).
  return (
    <div className="wf-subworkflow-summary">
      <section className="nifi-detail-section">
        <h3>스케줄</h3>
        {suppressed ? (
          <ParentScheduleNotice detail={detail} />
        ) : manual ? (
          <div className="wf-node-detail-value">수동 실행</div>
        ) : (
          <>
            <div className="wf-node-detail-value">정기 실행 · {cronToKoreanText(detail.scheduleCron)}</div>
            {/* 크론을 직접 적은 워크플로우는 다음 시각을 계산하지 않는다(프리셋 계산기라 어긋난다). */}
            {parsed.matched && (
              <div className="wf-node-detail-sub">
                <strong>다음 2회 실행</strong>
                {nextPresetRuns(parsed.preset, parsed.hour, parsed.minute, parsed.weekday).map((d) => (
                  <div key={d.valueOf()}>{d.format("YYYY-MM-DD HH:mm")}</div>
                ))}
              </div>
            )}
          </>
        )}
      </section>
      <section className="nifi-detail-section">
        <h3>상위 워크플로우</h3>
        {parents.length === 0 ? (
          <div className="wf-node-detail-sub">없음 (위 스케줄대로 단독 실행)</div>
        ) : !suppressed ? (
          <>
            {parents.map((parent) => (
              <div key={parent.id} className="wf-node-detail-value">
                <Link to={`/workflows/design/${parent.id}`}>{parent.name}</Link>
                <span className="wf-node-detail-sub-inline"> · 미게시</span>
              </div>
            ))}
            <div className="wf-node-detail-sub">상위가 모두 미게시라 위 스케줄대로 단독 실행됩니다.</div>
          </>
        ) : (
          parents.map((parent) => (
            <div key={parent.id} className="wf-node-detail-value">
              <Link to={`/workflows/design/${parent.id}`}>{parent.name}</Link>
              <span className="wf-node-detail-sub-inline"> · {cronToKoreanText(parent.scheduleCron)}</span>
            </div>
          ))
        )}
      </section>
      <WorkflowPublishSection detail={detail} asNode />
      <WorkflowCompositionSection detail={detail} />
      <WorkflowRecentRuns detail={detail} calledBy={calledBy} />
      <section className="nifi-detail-section">
        <h3>메모</h3>
        {detail.memo
          ? <div className="wf-node-detail-value wf-node-detail-memo">{detail.memo}</div>
          : <div className="wf-node-detail-sub">없음</div>}
      </section>
    </div>
  );
}

function WorkflowSettings({ detail, disabled, onSave, saving, onDraftChange, cdc = false }: {
  detail: WorkflowDetail;
  disabled: boolean;
  /** CDC 워크플로우(캔버스에 CDC 노드가 있다). 스케줄을 두지 않는다 - 실행할 때 지시를 골라 보낸다. */
  cdc?: boolean;
  onSave: (body: Parameters<typeof updateWorkflow>[1]) => void;
  saving: boolean;
  /** 입력이 바뀔 때마다 상단 «저장»이 쓸 수 있도록 현재 값을 올려보낸다. */
  onDraftChange?: (body: Parameters<typeof updateWorkflow>[1]) => void;
}) {
  const [name, setName] = useState(detail.name);
  // 저장된 크론을 프리셋으로 되돌려 채운다. 프리셋으로 표현 안 되는 식이면 그 크론을 그대로 들고 있다가
  // (customCron) 프리셋 칸을 건드리는 순간 프리셋 값으로 바뀐다 - 크론 직접 입력 칸을 뺐기 때문이다(2026-09-11).
  const parsed = parseCronToPreset(detail.scheduleCron);
  const [scheduleMode, setScheduleMode] = useState<"manual" | "recurring">(
    detail.scheduleCron ? "recurring" : "manual");
  const [preset, setPreset] = useState<SchedulePreset>(parsed.preset);
  const [hour, setHour] = useState(parsed.hour);
  const [minute, setMinute] = useState(parsed.minute);
  const [weekday, setWeekday] = useState(parsed.weekday);
  const [customCron, setCustomCron] = useState(parsed.matched ? "" : (detail.scheduleCron ?? ""));
  const [memo, setMemo] = useState(detail.memo ?? "");
  const parents = detail.parents ?? [];
  // 게시된 상위가 있으면 이 스케줄은 동작하지 않는다(DAG 생성기가 끈다). 값은 그대로 저장해 둔다 - 상위에서 빠지면 다시 쓰인다.
  const suppressed = publishedParentsOf(detail).length > 0;

  // 저장 본문은 한 곳에서 만든다 - «속성 저장» 버튼과 상단 «저장» 이 같은 값을 쓰게 한다.
  const settingsBody = {
    name,
    description: detail.description,
    // CDC 워크플로우는 스케줄을 저장하지 않는다(게시 검증 V12 가 막는다). 예전에 정기 실행이던
    // 워크플로우에 CDC 노드를 얹으면 여기서 비워져, «저장했는데 게시에서 거절»이 생기지 않는다.
    scheduleCron: cdc || scheduleMode === "manual" ? null
      : (customCron.trim() || presetCron(preset, hour, minute, weekday)),
    timezone: detail.timezone,
    catchup: detail.catchup,
    suspendOnError: detail.suspendOnError,
    memo,
  };
  // ref 에 쓰기만 하므로 렌더를 유발하지 않는다. 매 렌더 갱신해야 «마지막 입력»이 남는다.
  useEffect(() => {
    onDraftChange?.(settingsBody);
  });

  return (
    <Space direction="vertical" style={{ width: "100%" }} size={10}>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>이름</div>
        <Input size="small" value={name} onChange={(event) => setName(event.target.value)} />
        {/* 이름 밑 «상위 워크플로우 · …» 줄은 뺐다(2026-09-18 요청) - 아래 «상위 워크플로우» 칸에 같은 정보가 있다. */}
      </div>
      {cdc ? (
        <div>
          <div style={{ fontSize: 12, color: "#888", marginBottom: 4 }}>스케줄</div>
          {/* CDC 는 한 번 켜면 계속 돈다 - «매일 다시 켜기»가 뜻을 갖지 않는다(2026-09-10 설계 방안 B). */}
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            CDC 워크플로우는 수동 실행만 됩니다. 워크플로우 › 로그의 «실행 설정»에서 파이프라인마다 시작·중지·재시작·배포를
            골라 보냅니다(«유지»는 건너뜀). 각 노드는 실제로 그 상태가 됐는지 확인까지 해야 성공으로 끝납니다.
          </Typography.Text>
        </div>
      ) : (
      <div>
        <div style={{ fontSize: 12, color: "#888", marginBottom: 4 }}>스케줄</div>
        {suppressed && <ParentScheduleNotice detail={detail} />}
        <Radio.Group size="small" value={scheduleMode} style={{ marginBottom: 8 }}
                     onChange={(event) => setScheduleMode(event.target.value)}>
          <Radio value="manual">수동 실행</Radio>
          <Radio value="recurring">정기 실행</Radio>
        </Radio.Group>
        {scheduleMode === "recurring" && (
          <Space direction="vertical" style={{ width: "100%" }} size={6}>
            <Space size={4} wrap>
              <Select size="small" style={{ width: 90 }} value={preset} onChange={(value) => { setPreset(value); setCustomCron(""); }}
                      options={[{ label: "매시간", value: "hourly" },
                                { label: "매일", value: "daily" },
                                { label: "매주", value: "weekly" }]} />
              {preset === "weekly" && (
                <Select size="small" style={{ width: 90 }} value={weekday} onChange={(value) => { setWeekday(value); setCustomCron(""); }}
                        options={WEEKDAY_LABELS.map((label, value) => ({ label: `${label}요일`, value }))} />
              )}
              {preset !== "hourly" && (
                <InputNumber size="small" style={{ width: 60 }} min={0} max={23} value={hour}
                             onChange={(value) => { setHour(value ?? 0); setCustomCron(""); }} />
              )}
              <InputNumber size="small" style={{ width: 74 }} min={0} max={59} value={minute}
                           addonAfter="분" onChange={(value) => { setMinute(value ?? 0); setCustomCron(""); }} />
            </Space>
            {/* 크론 직접 입력 칸은 뺐다(2026-09-11 요청). 스케줄은 위 매시간·매일·매주 칸으로만 정한다. */}
            <div style={{ fontSize: 12, color: "#555" }}>
              ▶ {customCron.trim()
                ? customCron.trim()
                : presetDescription(preset, hour, minute, weekday)}
            </div>
            {/* 저장 전에 "정말 이 시각이 맞나"를 눈으로 확인하게 한다. */}
            {suppressed ? (
              <div style={{ fontSize: 12, color: "#888" }}>
                위 스케줄은 상위 워크플로우에서 빠지면 다시 쓰입니다.
              </div>
            ) : !customCron.trim() && (
              <div style={{ fontSize: 12, color: "#888", lineHeight: 1.7 }}>
                <strong style={{ color: "#555" }}>다음 2회 실행</strong>
                {nextPresetRuns(preset, hour, minute, weekday).map((d) => (
                  <div key={d.valueOf()}>{d.format("YYYY-MM-DD HH:mm")}</div>
                ))}
              </div>
            )}
          </Space>
        )}
      </div>
      )}
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>상위 워크플로우</div>
        {parents.length === 0 ? (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            없음 (위 스케줄대로 단독 실행)
          </Typography.Text>
        ) : !suppressed ? (
          <Space direction="vertical" size={2} style={{ width: "100%" }}>
            {parents.map((parent) => (
              <div key={parent.id}>
                <Link to={`/workflows/design/${parent.id}`}>{parent.name}</Link>
                <span style={{ color: "#888", fontSize: 12 }}> · 미게시</span>
              </div>
            ))}
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              상위가 모두 미게시라 위 스케줄대로 단독 실행됩니다.
            </Typography.Text>
          </Space>
        ) : (
          <Space direction="vertical" size={2} style={{ width: "100%" }}>
            {parents.map((parent) => (
              <div key={parent.id}>
                <Link to={`/workflows/design/${parent.id}`}>{parent.name}</Link>
                <span style={{ color: "#888", fontSize: 12 }}>
                  {" "}· {cronToKoreanText(parent.scheduleCron)}
                </span>
              </div>
            ))}
          </Space>
        )}
      </div>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>메모</div>
        <Input.TextArea size="small" rows={3} value={memo} placeholder="담당자·주의사항·재실행 절차 등"
                        onChange={(event) => setMemo(event.target.value)} />
      </div>
      {/* «동시 실행 수» 입력은 뺐다(2026-09-11 요청). 저장할 때 값을 보내지 않으면 서버가 기존 값을 그대로 두고,
          새 워크플로우는 기본값 1 로 만들어진다. */}
      {/* 맨 아래 «시간대 · DAG ID» 줄과 스케줄 설명 끝의 «(Asia/Seoul)» 은 뺐다(2026-09-11 요청).
          시간대 값은 저장·게시에 그대로 쓰인다 - 화면에 적지만 않는다. */}
      <Button size="small" type="primary" block disabled={disabled} loading={saving}
              onClick={() => onSave(settingsBody)}>
        속성 저장
      </Button>
      {/* 아래는 읽기 전용 정보(게시 상태 · 구성 요약 · 최근 실행). 워크플로우 노드 속성창과 같은 구역을 쓴다. 초안에는 없다. */}
      {detail.id > 0 && (
        <div className="wf-node-detail wf-settings-info">
          <WorkflowPublishSection detail={detail} />
          <WorkflowCompositionSection detail={detail} />
          <WorkflowRecentRuns detail={detail} />
        </div>
      )}
    </Space>
  );
}

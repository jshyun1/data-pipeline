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
  Tree,
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
import { listPipelines } from "../api/pipelines";
import { listPipelineGroups, type PipelineGroupResponse } from "../api/pipelineGroups";
import type { PipelineResponse } from "../types/pipeline";
import { buildWorkflowHierarchy, workflowPathOf, type WorkflowTreeItem } from "../utils/workflowTree";
import { EtlJobSummary } from "./ConsoleFramePage";
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
  }, [workflowId, setNodes, setEdges]);

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
        // SUBWF는 job 이름이 없다. 가리키는 워크플로우 이름을 보여준다(키가 아니라).
        label: node.nodeType === "SUBWF"
          ? `▷ ${node.subWorkflowName ?? node.nodeKey}`
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
      },
      type: "job",
      className: nodeClass(node.jobMissing, node.nodeType),
    })));
    // 흐름은 항상 왼쪽에서 오른쪽이다. 연결점이 좌우 하나씩뿐이라 어느 변인지 추정할 것이 없다.
    setEdges(detail.edges.map((edge) => {
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
          label: `▷ ${target.name}`,
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
    setNodes((current) => {
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
    });
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

  const saveMutation = useMutation({
    // 예전에는 그래프만 저장해서, 스케줄을 바꾸고 «저장»을 눌러도 조용히 버려졌다
    // (스케줄은 «속성 저장» 버튼에만 있었다). 화면의 변경은 «저장» 하나로 다 저장한다.
    mutationFn: async () => {
      const draft = settingsDraftRef.current;
      if (draft) {
        await updateWorkflow(workflowId, draft);
      }
      return saveWorkflowGraph(workflowId, {
      nodes: nodes.map((node) => ({
        nodeKey: node.id,
        nodeType: String(node.data.nodeType ?? "JOB"),
        jobId: (node.data.jobId as number | undefined) ?? null,
        subWorkflowId: (node.data.subWorkflowId as number | undefined) ?? null,
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
    },
    onSuccess: () => {
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
    const placed = job ? usedJobIdSet.has(job.id) : false;
    return {
      key: node.id,
      selectable: Boolean(job),
      title: (
        // 워크플로우 트리와 같은 조작으로 맞춘다 - 얹는 것은 «끌어다 놓기»로만 한다.
        <span style={{ opacity: job || children.length ? 1 : 0.45 }}
              title={job ? "끌어다 캔버스에 놓으면 추가됩니다" : undefined}
              draggable={Boolean(job) && canWrite}
              onDragStart={(event) => {
                if (!job) return;
                event.dataTransfer.setData("application/cerebro-job", String(job.id));
                event.dataTransfer.effectAllowed = "copy";
              }}>
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
   * 팔레트의 뿌리. ETL(NiFi) 그룹 계층 전체를 그대로 보여준다.
   *
   * <p>예전에는 워크플로우가 «소속 업무 그룹»을 갖고 그 그룹 아래 job 만 보여줬다(«전체 보기»로
   * 넓힐 수 있었다). 워크플로우에서 업무 그룹 개념을 걷어내면서 좁힐 기준이 없어졌다 -
   * 이제는 늘 전체를 보여주고, 필요한 job 은 위 검색으로 찾는다.
   */
  const paletteRoot = treeQuery.data;
  const paletteTree = paletteRoot ? [buildPaletteTree(paletteRoot)].filter(Boolean) : [];

  /**
   * 얹을 수 있는 워크플로우. 자기 자신은 뺀다(자기 안에 자기를 넣을 수 없다).
   *
   * <p>예전에는 NiFi 업무 그룹 계층으로 묶어 보여줬는데, 여기서 고르는 기준은 «무엇이
   * 무엇을 품고 있는가»다. 그래서 워크플로우 상하 계층으로 세운다 - 이름 오른쪽 숫자는
   * 그 워크플로우 캔버스에 놓인 job 수다. 목록 화면(스케줄링)과 같은 트리를 쓴다.
   *
   * <p>자기 자신을 빼면 그 하위였던 워크플로우는 부모를 잃는데, 빌더가 그런 것들을
   * 최상단으로 올려 주므로 목록에서 사라지지 않는다.
   */
  const allWorkflows = workflowsQuery.data ?? [];
  const canvasPath = workflowPathOf(allWorkflows, workflowId);
  // 얹을 수 있는 것은 «자기 자신을 뺀» 목록이다(자기 안에 자기를 넣을 수 없다).
  // 다만 트리에는 자기 자신도 그린다 - 지금 어디를 열고 있는지 보여야 길을 잃지 않는다.
  const selectableWorkflows = allWorkflows.filter((w) => w.id !== workflowId);

  const toSubWorkflowNodes = (items: WorkflowTreeItem[]): Array<{
    key: string; title: React.ReactNode; children?: unknown[]; selectable: boolean;
  }> => items.map((item) => {
    const isCurrent = item.workflow.id === workflowId;
    return {
      key: `wf:${item.workflow.id}`,
      // 지금 열려 있는 워크플로우는 «얹는» 대상이 아니다(자기 안에 자기를 넣을 수 없다).
      selectable: !isCurrent,
      title: (
        // 더블클릭은 여기(제목)에 건다. antd Tree 에는 onDoubleClick prop 이 없어서
        // Tree 에 넘기면 조용히 무시된다(실측 2026-09-09).
        <span style={isCurrent ? { fontWeight: 700, color: "#1677ff" } : undefined}
              title={isCurrent ? "지금 열려 있는 워크플로우" : "더블클릭하면 이 워크플로우를 엽니다"}
              onDoubleClick={(event) => {
                event.stopPropagation();
                if (!isCurrent) leaveTo(`/workflows/design/${item.workflow.id}`);
              }}
              // 캔버스에 얹는 것은 «끌어다 놓기»로만 한다. 한 번 클릭으로 얹히면 트리를
              // 훑어보다가 노드가 생겨 버려서, 지우고 다시 저장하는 일이 잦았다.
              draggable={canWrite && !isCurrent}
              onDragStart={(event) => {
                event.dataTransfer.setData("application/cerebro-workflow", String(item.workflow.id));
                event.dataTransfer.effectAllowed = "copy";
              }}>
          ▷ {item.workflow.name}{item.workflow.published ? "" : " (미게시)"}
          <span style={{ color: "#888" }}> ({item.workflow.jobCount})</span>
          {isCurrent && <span style={{ color: "#1677ff" }}> · 열림</span>}
        </span>
      ),
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
  const cdcTree = (() => {
    const pipelines = pipelinesQuery.data ?? [];
    const groups = pipelineGroupsQuery.data ?? [];
    const groupIds = new Set(groups.map((g) => g.id));
    const byGroup = new Map<number | null, PipelineResponse[]>();
    pipelines.forEach((pipeline) => {
      const key = pipeline.groupId != null && groupIds.has(pipeline.groupId) ? pipeline.groupId : null;
      byGroup.set(key, [...(byGroup.get(key) ?? []), pipeline]);
    });
    const leaf = (pipeline: PipelineResponse) => ({
      key: `cdc:${pipeline.id}`,
      selectable: false,
      title: (
        <span style={{ color: "#8c8c8c" }}>
          {pipeline.name}
          <span style={{ color: "#bfbfbf" }}> · {pipeline.sourceTable ?? pipeline.targetTable}</span>
        </span>
      ),
    });
    const branch = (group: PipelineGroupResponse): { node: Record<string, unknown>; count: number } => {
      const subs = groups.filter((g) => g.parentId === group.id).map(branch);
      const own = byGroup.get(group.id) ?? [];
      const count = own.length + subs.reduce((sum, sub) => sum + sub.count, 0);
      return {
        count,
        node: {
          key: `cdcgroup:${group.id}`,
          selectable: false,
          title: <span>{group.name}<span style={{ color: "#888" }}> ({count})</span></span>,
          children: [...subs.map((sub) => sub.node), ...own.map(leaf)],
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
      selectable: false,
      title: <span>전체 파이프라인<span style={{ color: "#888" }}> ({pipelines.length})</span></span>,
      children: [...roots.map((root) => root.node), ...ungrouped],
    }];
  })();
  const cdcOpenKeys = ["cdc:all"];

  /**
   * 처음 펼쳐둘 가지. 최상단 하나만 연다(그 바로 아래 계층까지 «보이는» 상태).
   *
   * 전부 펼치면 job이 수십 개 쏟아져 어디를 봐야 할지 알 수 없다. 검색 중일 때는
   * 걸린 가지가 보여야 하므로 antd의 자동 펼침에 맡긴다.
   */
  const defaultOpenKeys = paletteRoot ? [paletteRoot.id] : [];

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
            <Button size="small" icon={<ArrowLeftOutlined />} onClick={() => leaveTo("/workflows/design")} />
            <strong>{detail.name}</strong>
            {/* 최상단부터의 경로. 하위 워크플로우를 열었을 때 «어디에 속한 것인지»가 보인다. */}
            {canvasPath.length > 1 && (
              <span style={{ color: "#888", fontSize: 12 }}>{canvasPath.join(" > ")}</span>
            )}
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
                    loading={publishMutation.isPending} disabled={!canPublish || dirty}
                    title={canPublish ? undefined : "Airflow 쓰기 권한이 없습니다"}>
              게시
            </Button>
            {detail.published && (
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

        <div className="wf-canvas-grid"
             style={{ display: "grid", gridTemplateColumns: "260px 1fr 300px", gap: 12 }}>
          {/* 좌: job 팔레트 - ETL(NiFi)에서 만들어 둔 job 목록 */}
          <Card size="small" title="job 팔레트"
                extra={<Button size="small" icon={<SyncOutlined />} title="NiFi에서 다시 읽어옵니다"
                               loading={syncMutation.isPending}
                               onClick={() => syncMutation.mutate()} />}
                className="wf-side-card">
            {/* 워크플로우도 하나의 노드로 얹을 수 있다. 그래야 «daily = monthly 끝나면
                years» 같은 조립이 가능하다. 자기 자신은 넣을 수 없다(무한 중첩).
                조립이 이 화면의 관심사라 job 목록보다 위에 둔다. */}
            <div style={{ fontWeight: 600, marginBottom: 6 }}>워크플로우</div>
            {subWorkflowTree.length === 0 ? (
              <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                얹을 수 있는 다른 워크플로우가 없습니다.
              </Typography.Text>
            ) : (
              <Tree
                blockNode
                defaultExpandedKeys={subWorkflowOpenKeys}
                selectedKeys={[]}
                treeData={subWorkflowTree as never}
              />
            )}
            <div style={{ borderTop: "1px solid #e5e7eb", marginTop: 12, paddingTop: 10 }}>
              <div style={{ fontWeight: 600, marginBottom: 6 }}>ETL 태스크</div>
              {treeQuery.isLoading ? (
                <Spin size="small" />
              ) : paletteTree.length === 0 ? (
                <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="job이 없습니다" />
              ) : (
                <Tree
                  blockNode
                  defaultExpandedKeys={defaultOpenKeys}
                  selectedKeys={[]}
                  treeData={paletteTree as never}
                />
              )}
            </div>
            <div style={{ borderTop: "1px solid #e5e7eb", marginTop: 12, paddingTop: 10 }}>
              <div style={{ fontWeight: 600, marginBottom: 6 }}>CDC</div>
              {cdcTree.length === 0 ? (
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  등록된 CDC 파이프라인이 없습니다.
                </Typography.Text>
              ) : (
                // 아직 캔버스에 얹을 수 없다(노드 종류에 CDC 가 없다). 그래서 트리 항목은
                // selectable:false·draggable 아님으로 두고, 화면에는 따로 적지 않는다.
                <Tree
                  blockNode
                  defaultExpandedKeys={cdcOpenKeys}
                  selectedKeys={[]}
                  treeData={cdcTree as never}
                />
              )}
            </div>
          </Card>

          {/* 중: 캔버스 - 노드를 잇는 선이 곧 실행 순서다 */}
          <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, height: "100%" }}>
            <ReactFlow
              // 팔레트에서 끌어온 워크플로우를 놓은 자리에 얹는다.
              onDragOver={(event) => {
                const kinds = event.dataTransfer.types;
                if (kinds.includes("application/cerebro-workflow")
                    || kinds.includes("application/cerebro-job")) {
                  event.preventDefault();
                  event.dataTransfer.dropEffect = "copy";
                }
              }}
              onDrop={(event) => {
                if (!canWrite) return;
                const at = () => (flow?.screenToFlowPosition
                  ? flow.screenToFlowPosition({ x: event.clientX, y: event.clientY })
                  : undefined);
                const workflowRaw = event.dataTransfer.getData("application/cerebro-workflow");
                if (workflowRaw) {
                  event.preventDefault();
                  const picked = selectableWorkflows.find((w) => w.id === Number(workflowRaw));
                  if (picked) addSubWorkflowNode(picked, at());
                  return;
                }
                const jobRaw = event.dataTransfer.getData("application/cerebro-job");
                if (jobRaw) {
                  event.preventDefault();
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

          {/* 우: 속성 - 워크플로우 전체 / 선택 노드 */}
          <Card size="small" title={
            selectedNode ? `노드: ${selectedNode.id}`
              : selectedEdge ? "연결 조건"
              : (
                <Space size={6}>
                  <span>워크플로우 속성</span>
                  {/* 상위가 있으면 자체 스케줄이 안 돈다. 예전엔 이 설명을 스케줄 위에
                      큰 알림으로 깔아 패널을 절반이나 잡아먹었다. 아이콘 하나로 줄이고
                      필요할 때만 펴 본다. */}
                  {(detail?.parents?.length ?? 0) > 0 && (
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
                className="wf-side-card">
            {selectedNode ? (
              <Space direction="vertical" style={{ width: "100%" }} size={10}>
                <div>
                  <div style={{ fontSize: 12, color: "#888" }}>
                    {selectedNode.data.subWorkflowId ? "워크플로우" : "실행 job"}
                  </div>
                  <div>{String(selectedNode.data.label)}</div>
                  {/* 워크플로우 노드는 이름만으로 «어디에 속한 것인지»를 알 수 없다.
                      워크플로우 속성창과 같은 자리·같은 모양으로 최상단부터의 경로를 적는다. */}
                  {selectedNode.data.subWorkflowId ? (() => {
                    const path = workflowPathOf(allWorkflows, Number(selectedNode.data.subWorkflowId));
                    return path.length > 1
                      ? <div style={{ fontSize: 12, color: "#888" }}>{path.join(" > ")}</div>
                      : null;
                  })() : null}
                </div>
                {/* ETL 관리 화면과 같은 job 속성(마지막 실행·상위 경로·작성자·연결 DAG·대상·로그).
                    계산이 두 벌이 되지 않도록 그쪽 컴포넌트를 그대로 가져다 쓴다. */}
                {selectedNode.data.jobId && !selectedNode.data.subWorkflowId ? (
                  <EtlJobSummary
                    jobId={Number(selectedNode.data.jobId)}
                    nifiPgId={selectedNode.data.nifiPgId ? String(selectedNode.data.nifiPgId) : null}
                  />
                ) : null}
                {selectedNode.data.subWorkflowId ? (
                  <SubWorkflowSummary workflowId={Number(selectedNode.data.subWorkflowId)} />
                ) : null}
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
                {/* 워크플로우 노드면 그 워크플로우 캔버스로 바로 넘어간다. */}
                {selectedNode.data.subWorkflowId ? (
                  <Button size="small" block
                          onClick={() => leaveTo(
                            `/workflows/design/${Number(selectedNode.data.subWorkflowId)}`)}>
                    워크플로우 바로가기
                  </Button>
                ) : null}
                {/* 이 job이 NiFi에서 실제로 어떻게 생겼는지 바로 열어본다. */}
                {selectedNode.data.nifiPgId ? (
                  <Button size="small" block
                          onClick={() => leaveTo(
                            `/etl/manage?processGroupId=${encodeURIComponent(String(selectedNode.data.nifiPgId))}`)}>
                    ETL job 바로가기
                  </Button>
                ) : null}
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
                                onSave={(body) => settingsMutation.mutate(body)}
                                saving={settingsMutation.isPending}
                                onDraftChange={(body) => { settingsDraftRef.current = body; }} />
            )}
          </Card>
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
  const kind = data?.nodeType === "SUBWF" ? "wf" : "job";
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

/** 노드 겉모습. 워크플로우 노드는 job과 한눈에 구분돼야 한다(보라 계열 + 왼쪽 굵은 띠). */
function nodeClass(problem: boolean, nodeType?: string): string {
  const kind = nodeType === "SUBWF" ? " wf-node--subwf" : " wf-node--job";
  return problem ? `wf-node${kind} wf-node--problem` : `wf-node${kind}`;
}

/**
 * 캔버스에 얹은 «워크플로우 노드»를 눌렀을 때 보여주는 그 워크플로우의 요약(읽기 전용).
 *
 * <p>예전에는 이름과 진입 조건만 나와서, 이 하위 워크플로우가 언제 도는지·누가 위에
 * 있는지를 알려면 캔버스를 옮겨 다녀야 했다. 값은 그 워크플로우의 것이므로 여기서는
 * 고치지 않는다 - 고치려면 «워크플로우 바로가기»로 넘어가 그 캔버스에서 저장한다.
 */
function SubWorkflowSummary({ workflowId }: { workflowId: number }) {
  const query = useQuery({
    queryKey: ["workflow", workflowId],
    queryFn: () => getWorkflow(workflowId),
  });
  const detail = query.data;
  if (query.isLoading) {
    return <Typography.Text type="secondary" style={{ fontSize: 12 }}>불러오는 중</Typography.Text>;
  }
  if (!detail) {
    return <Typography.Text type="secondary" style={{ fontSize: 12 }}>정보를 불러오지 못했습니다</Typography.Text>;
  }
  const parsed = parseCronToPreset(detail.scheduleCron);
  const parents = detail.parents ?? [];
  const manual = !detail.scheduleCron;
  return (
    <>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>스케줄</div>
        {manual ? (
          <Typography.Text style={{ fontSize: 13 }}>수동 실행</Typography.Text>
        ) : (
          <Space direction="vertical" size={2} style={{ width: "100%" }}>
            <Typography.Text style={{ fontSize: 13 }}>
              정기 실행 · {parsed
                ? presetDescription(parsed.preset, parsed.hour, parsed.minute, parsed.weekday)
                : detail.scheduleCron}
            </Typography.Text>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {detail.scheduleCron} ({detail.timezone})
            </Typography.Text>
            {/* 크론을 직접 적은 워크플로우는 다음 시각을 계산하지 않는다(프리셋 계산기라 어긋난다). */}
            {parsed && (
              <div style={{ fontSize: 12, color: "#888", lineHeight: 1.7 }}>
                <strong style={{ color: "#555" }}>다음 2회 실행</strong>
                {nextPresetRuns(parsed.preset, parsed.hour, parsed.minute, parsed.weekday).map((d) => (
                  <div key={d.valueOf()}>{d.format("YYYY-MM-DD HH:mm")}</div>
                ))}
              </div>
            )}
          </Space>
        )}
      </div>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>상위 워크플로우</div>
        {parents.length === 0 ? (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            없음 (위 스케줄대로 단독 실행)
          </Typography.Text>
        ) : (
          <Space direction="vertical" size={2} style={{ width: "100%" }}>
            {parents.map((parent) => (
              <div key={parent.id}>
                <Link to={`/workflows/design/${parent.id}`}>{parent.name}</Link>
                <span style={{ color: "#888", fontSize: 12 }}>
                  {" "}{parent.scheduleCron ? `· ${parent.scheduleCron}` : "· 수동"}
                </span>
              </div>
            ))}
          </Space>
        )}
      </div>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>메모</div>
        {detail.memo ? (
          <Typography.Paragraph style={{ fontSize: 13, marginBottom: 0, whiteSpace: "pre-wrap" }}>
            {detail.memo}
          </Typography.Paragraph>
        ) : (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>없음</Typography.Text>
        )}
      </div>
    </>
  );
}

function WorkflowSettings({ detail, disabled, onSave, saving, onDraftChange }: {
  detail: WorkflowDetail;
  disabled: boolean;
  onSave: (body: Parameters<typeof updateWorkflow>[1]) => void;
  saving: boolean;
  /** 입력이 바뀔 때마다 상단 «저장»이 쓸 수 있도록 현재 값을 올려보낸다. */
  onDraftChange?: (body: Parameters<typeof updateWorkflow>[1]) => void;
}) {
  const [name, setName] = useState(detail.name);
  // 최상단부터의 경로. 목록 조회는 이미 캐시에 있어 추가 호출이 사실상 없다.
  const pathQuery = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  const settingsPath = workflowPathOf(pathQuery.data ?? [], detail.id);
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
  const [memo, setMemo] = useState(detail.memo ?? "");
  const parents = detail.parents ?? [];

  // 저장 본문은 한 곳에서 만든다 - «속성 저장» 버튼과 상단 «저장» 이 같은 값을 쓰게 한다.
  const settingsBody = {
    name,
    description: detail.description,
    scheduleCron: scheduleMode === "manual" ? null
      : (customCron.trim() || presetCron(preset, hour, minute, weekday)),
    timezone: detail.timezone,
    catchup: detail.catchup,
    maxActiveRuns: maxRuns,
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
        {/* 최상단부터 이 워크플로우까지. 상위가 없으면 자기 이름뿐이라 굳이 적지 않는다. */}
        {settingsPath.length > 1 && (
          <div style={{ fontSize: 12, color: "#888", marginTop: 4 }}>{settingsPath.join(" > ")}</div>
        )}
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
                <strong style={{ color: "#555" }}>다음 2회 실행</strong>
                {nextPresetRuns(preset, hour, minute, weekday).map((d) => (
                  <div key={d.valueOf()}>{d.format("YYYY-MM-DD HH:mm")}</div>
                ))}
              </div>
            )}
          </Space>
        )}
      </div>
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>상위 워크플로우</div>
        {parents.length === 0 ? (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            없음 (위 스케줄대로 단독 실행)
          </Typography.Text>
        ) : (
          <Space direction="vertical" size={2} style={{ width: "100%" }}>
            {parents.map((parent) => (
              <div key={parent.id}>
                <Link to={`/workflows/design/${parent.id}`}>{parent.name}</Link>
                <span style={{ color: "#888", fontSize: 12 }}>
                  {" "}{parent.scheduleCron ? `· ${parent.scheduleCron}` : "· 수동"}
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
      <div>
        <div style={{ fontSize: 12, color: "#888" }}>동시 실행 수</div>
        <InputNumber size="small" min={1} max={5} style={{ width: "100%" }}
                     value={maxRuns} onChange={(value) => setMaxRuns(value ?? 1)} />
      </div>
      <div style={{ fontSize: 12, color: "#888" }}>
        시간대 {detail.timezone} · DAG {detail.dagId}
      </div>
      <Button size="small" type="primary" block disabled={disabled} loading={saving}
              onClick={() => onSave(settingsBody)}>
        속성 저장
      </Button>
    </Space>
  );
}

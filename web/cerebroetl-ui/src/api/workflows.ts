import { apiClient, unwrap, type ApiResponse } from "./client";

// 워크플로우 캔버스 API. 저장(draft)과 게시(publish)가 분리되어 있어서,
// 게시하기 전에는 Airflow에 DAG가 생기지 않는다.

export interface WorkflowSummary {
  id: number;
  workflowKey: string;
  dagId: string;
  name: string;
  description?: string | null;
  nifiGroupPgId?: string | null;
  scheduleCron?: string | null;
  timezone: string;
  upstreamCount?: number;
  published: boolean;
  publishedAt?: string | null;
  publishedBy?: string | null;
  nodeCount: number;
  updatedAt: string;
}

export interface WorkflowNodeView {
  nodeKey: string;
  nodeType: string;
  jobId?: number | null;
  jobName?: string | null;
  parentGroupName?: string | null;
  nifiPgId?: string | null;
  subWorkflowId?: number | null;
  subWorkflowName?: string | null;
  triggerRule: string;
  branchExpr?: string | null;
  retries: number;
  retryDelaySec: number;
  displayX?: number | null;
  displayY?: number | null;
  /** 참조하던 job이 사라진 노드. 캔버스에서 빨갛게 표시하고 게시도 막힌다. */
  jobMissing: boolean;
}

export interface WorkflowEdgeView {
  fromNodeKey: string;
  toNodeKey: string;
  conditionType: string;
  conditionExpr?: string | null;
}

export interface WorkflowDetail extends Omit<WorkflowSummary, "nodeCount"> {
  catchup: boolean;
  maxActiveRuns: number;
  suspendOnError: boolean;
  upstreamWorkflowIds?: number[] | null;
  upstreamMode?: string | null;
  /** 캔버스·속성창 공용 메모. */
  memo?: string | null;
  /** 이 워크플로우를 노드로 품고 있는 상위들. 있으면 자체 스케줄은 돌지 않는다. */
  parents?: Array<{
    id: number; workflowKey: string; name: string; dagId: string;
    scheduleCron?: string | null; published: boolean;
  }>;
  /** 게시 후 캔버스를 고쳤는지. "게시본과 다름" 배지의 근거다. */
  dirty: boolean;
  nodes: WorkflowNodeView[];
  edges: WorkflowEdgeView[];
}

export interface ValidationIssue {
  code: string;
  message: string;
  nodeKey?: string | null;
}

export interface ValidationResult {
  valid: boolean;
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
}

export interface SaveGraphRequest {
  nodes: Array<{
    nodeKey: string;
    nodeType?: string;
    jobId?: number | null;
    subWorkflowId?: number | null;
    triggerRule?: string;
    branchExpr?: string | null;
    retries?: number;
    retryDelaySec?: number;
    displayX?: number;
    displayY?: number;
  }>;
  edges: Array<{
    fromNodeKey: string;
    toNodeKey: string;
    conditionType?: string;
    conditionExpr?: string | null;
  }>;
}

export async function listWorkflows(): Promise<WorkflowSummary[]> {
  return unwrap((await apiClient.get<ApiResponse<WorkflowSummary[]>>("/etl/workflows")).data);
}

export async function getWorkflow(id: number): Promise<WorkflowDetail> {
  return unwrap((await apiClient.get<ApiResponse<WorkflowDetail>>(`/etl/workflows/${id}`)).data);
}

export async function createWorkflow(body: {
  /** 비워 보내면 서버가 그룹·이름에서 만들어 채운다. 화면은 키를 입력받지 않는다. */
  workflowKey?: string;
  name: string;
  description?: string;
  nifiGroupPgId?: string;
  scheduleCron?: string;
  timezone?: string;
  maxActiveRuns?: number;
}): Promise<WorkflowSummary> {
  return unwrap((await apiClient.post<ApiResponse<WorkflowSummary>>("/etl/workflows", body)).data);
}

export async function updateWorkflow(id: number, body: {
  name: string;
  description?: string | null;
  nifiGroupPgId?: string | null;
  scheduleCron?: string | null;
  timezone?: string;
  catchup?: boolean;
  maxActiveRuns?: number;
  suspendOnError?: boolean;
  upstreamWorkflowIds?: number[];
  upstreamMode?: string;
  memo?: string | null;
}): Promise<WorkflowSummary> {
  return unwrap((await apiClient.put<ApiResponse<WorkflowSummary>>(`/etl/workflows/${id}`, body)).data);
}

/** 캔버스 저장(draft). 노드·엣지를 통째로 교체한다. */
export async function saveWorkflowGraph(id: number, body: SaveGraphRequest): Promise<WorkflowDetail> {
  return unwrap((await apiClient.put<ApiResponse<WorkflowDetail>>(`/etl/workflows/${id}/graph`, body)).data);
}

export async function validateWorkflow(id: number): Promise<ValidationResult> {
  return unwrap((await apiClient.post<ApiResponse<ValidationResult>>(`/etl/workflows/${id}/validate`)).data);
}

/** 검증 + 컴파일 + Airflow 게시. 오류가 있으면 게시하지 않고 결과만 돌려준다. */
export async function publishWorkflow(id: number): Promise<ValidationResult> {
  return unwrap((await apiClient.post<ApiResponse<ValidationResult>>(`/etl/workflows/${id}/publish`)).data);
}

export async function unpublishWorkflow(id: number): Promise<void> {
  unwrap((await apiClient.post<ApiResponse<void>>(`/etl/workflows/${id}/unpublish`)).data);
}

export async function deleteWorkflow(id: number): Promise<void> {
  unwrap((await apiClient.delete<ApiResponse<void>>(`/etl/workflows/${id}`)).data);
}

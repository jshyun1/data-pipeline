import axios from "axios";
import { apiClient, unwrap } from "./client";

export interface AirflowHealthResponse {
  metadatabase?: { status?: string };
  scheduler?: { status?: string };
  triggerer?: { status?: string };
  dag_processor?: { status?: string };
}

export interface AirflowDag {
  dag_id: string;
  is_active?: boolean;
  is_paused?: boolean;
}

export interface AirflowDagRun {
  dag_id?: string;
  dag_run_id: string;
  state?: string;
  start_date?: string;
  end_date?: string;
  execution_date?: string;
  conf?: Record<string, unknown>;
}

export interface AirflowTaskInstance {
  task_id: string;
  state?: string;
  try_number?: number;
  max_tries?: number;
  start_date?: string;
  end_date?: string;
}

export interface AirflowTask {
  task_id: string;
}

export interface KafkaConnectInfoResponse {
  version?: string;
  commit?: string;
  kafka_cluster_id?: string;
}

export interface NifiRootStatusResponse {
  processGroupStatus?: {
    id?: string;
    name?: string;
    statsLastRefreshed?: string;
    aggregateSnapshot?: {
      activeThreadCount?: number;
      flowFilesQueued?: number;
      bytesQueued?: number;
      flowFilesSent?: number;
      bytesSent?: number;
      flowFilesReceived?: number;
      bytesReceived?: number;
      processorStatusSnapshots?: NifiProcessorStatusSnapshot[];
      processGroupStatusSnapshots?: NifiProcessGroupStatusSnapshot[];
    };
  };
}

export interface NifiProcessorStatusSnapshot {
  id?: string;
  canRead?: boolean;
  processorStatusSnapshot?: {
    id?: string;
    groupId?: string;
    name?: string;
    type?: string;
    runStatus?: string;
    activeThreadCount?: number;
  };
}

export interface NifiProcessGroupStatusSnapshot {
  id?: string;
  canRead?: boolean;
  processGroupStatusSnapshot?: {
    id?: string;
    name?: string;
    flowFilesQueued?: number;
    queued?: string;
    activeThreadCount?: number;
    processorStatusSnapshots?: NifiProcessorStatusSnapshot[];
    processGroupStatusSnapshots?: NifiProcessGroupStatusSnapshot[];
  };
}

export interface NifiProcessGroupEntity {
  id?: string;
  name?: string;
  parentGroupId?: string;
}

// Airflow의 세션 쿠키(_token/session)는 Path=/airflow/로 발급된다(AIRFLOW__API__BASE_URL이
// /airflow 프리픽스라 FAB가 그렇게 스코프함) - /airflow-api/* 처럼 다른 최상위 경로로
// 호출하면 브라우저가 쿠키를 아예 안 붙여서 항상 401이 난다(로그인/로그아웃과 무관하게
// 재현됨 - 실제 로그인 플로우를 curl로 재현해서 확인). 그래서 반드시 /airflow/ 아래의
// 기존 프록시 경로(nginx의 /airflow/ 블록, 곧 airflow-apiserver:8080)를 그대로 쓴다.
const AIRFLOW_API_BASE = "/airflow/api/v2";

export async function getAirflowHealth(): Promise<AirflowHealthResponse> {
  const res = await axios.get<AirflowHealthResponse>(`${AIRFLOW_API_BASE}/monitor/health`);
  return res.data;
}

export async function listAirflowDags(): Promise<AirflowDag[]> {
  const res = await axios.get<{ dags?: AirflowDag[] }>(`${AIRFLOW_API_BASE}/dags`, {
    params: { limit: 100 },
  });
  return res.data.dags ?? [];
}

export async function listAirflowDagRuns(
  dagId: string,
  options: { limit?: number; startDateGte?: string; startDateLte?: string; state?: string } = {},
): Promise<AirflowDagRun[]> {
  const res = await axios.get<{ dag_runs?: AirflowDagRun[] }>(
    `${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/dagRuns`,
    {
      params: {
        limit: options.limit ?? 100,
        order_by: "-start_date",
        start_date_gte: options.startDateGte,
        start_date_lte: options.startDateLte,
        state: options.state,
      },
    },
  );
  return res.data.dag_runs ?? [];
}

export async function listAirflowTaskInstances(dagId: string, dagRunId: string): Promise<AirflowTaskInstance[]> {
  const res = await axios.get<{ task_instances?: AirflowTaskInstance[] }>(
    `${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}/taskInstances`,
  );
  return res.data.task_instances ?? [];
}

export async function listAirflowDagTasks(dagId: string): Promise<AirflowTask[]> {
  const res = await axios.get<{ tasks?: AirflowTask[] }>(
    `${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/tasks`,
  );
  return res.data.tasks ?? [];
}

interface AirflowLogMessage {
  timestamp?: string;
  event?: string;
}

export async function getAirflowTaskLog(
  dagId: string,
  dagRunId: string,
  taskId: string,
  tryNumber = 1,
): Promise<string> {
  const res = await axios.get<{ content?: Array<AirflowLogMessage | string> }>(
    `${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}/taskInstances/${encodeURIComponent(taskId)}/logs/${tryNumber}`,
    { params: { full_content: true } },
  );
  const content = res.data.content ?? [];
  if (content.length === 0) {
    return "로그가 없습니다.";
  }
  return content
    .map((entry) =>
      typeof entry === "string" ? entry : `${entry.timestamp ? `[${entry.timestamp}] ` : ""}${entry.event ?? ""}`,
    )
    .join("\n");
}


export async function getKafkaConnectInfo(): Promise<KafkaConnectInfoResponse> {
  const res = await axios.get<KafkaConnectInfoResponse>("/kafka-connect-api/");
  return res.data;
}

export async function listKafkaConnectors(): Promise<string[]> {
  const res = await axios.get<string[]>("/kafka-connect-api/connectors");
  return res.data;
}

export async function getNifiRootStatus(): Promise<NifiRootStatusResponse> {
  const res = await axios.get<NifiRootStatusResponse>("/nifi-api/flow/process-groups/root/status", {
    params: { recursive: true },
  });
  return res.data;
}

export async function createNifiProcessGroup(name: string): Promise<NifiProcessGroupEntity> {
  const res = await apiClient.post("/nifi/process-groups", { name });
  return unwrap<NifiProcessGroupEntity>(res.data);
}

import axios from "axios";
import { apiClient, unwrap } from "./client";

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

export interface NifiBulletin {
  groupId?: string;
  bulletin?: {
    level?: string;
    message?: string;
    sourceName?: string;
    category?: string;
    timestamp?: string;
  };
}

export interface NifiBulletinBoardResponse {
  bulletinBoard?: {
    bulletins?: NifiBulletin[];
  };
}

export interface KafkaConnectorTaskStatus {
  id?: number;
  state?: string;
}

export interface KafkaConnectorStatusEntry {
  status?: {
    name?: string;
    connector?: { state?: string };
    tasks?: KafkaConnectorTaskStatus[];
    type?: string;
  };
}

export type KafkaConnectorStatusMap = Record<string, KafkaConnectorStatusEntry>;

export interface KafkaConnectorTraceResponse {
  name?: string;
  connector?: { state?: string; trace?: string };
  tasks?: Array<{ id?: number; state?: string; trace?: string }>;
}

// Airflow의 세션 쿠키(_token/session)는 Path=/airflow/로 발급된다(AIRFLOW__API__BASE_URL이
// /airflow 프리픽스라 FAB가 그렇게 스코프함) - /airflow-api/* 처럼 다른 최상위 경로로
// 호출하면 브라우저가 쿠키를 아예 안 붙여서 항상 401이 난다(로그인/로그아웃과 무관하게
// 재현됨 - 실제 로그인 플로우를 curl로 재현해서 확인). 그래서 반드시 /airflow/ 아래의
// 기존 프록시 경로(nginx의 /airflow/ 블록, 곧 airflow-apiserver:8080)를 그대로 쓴다.
const AIRFLOW_API_BASE = "/airflow/api/v2";

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


export async function getNifiRootStatus(): Promise<NifiRootStatusResponse> {
  const res = await axios.get<NifiRootStatusResponse>("/nifi-api/flow/process-groups/root/status", {
    params: { recursive: true },
  });
  return res.data;
}

// NiFi는 Airflow의 DAG-run 같은 "실행 성공/실패" 개념이 없고 대신 실시간 오류를
// bulletin board에 올린다 - 그룹의 runStatus가 Invalid가 아니어도(정상 기동 중이어도)
// 처리 중 오류(예: SQL 예외)는 여기로만 올라오므로, 실패 판정은 이 값도 같이 봐야 한다.
export async function getNifiBulletins(): Promise<NifiBulletinBoardResponse> {
  const res = await axios.get<NifiBulletinBoardResponse>("/nifi-api/flow/bulletin-board");
  return res.data;
}

// expand=info를 같이 요청하면 커넥터 config(DB 비밀번호 평문 포함)까지 브라우저로
// 그대로 내려오므로 절대 같이 쓰지 않는다 - status만 요청.
export async function listKafkaConnectorsWithStatus(): Promise<KafkaConnectorStatusMap> {
  const res = await axios.get<KafkaConnectorStatusMap>("/kafka-connect-api/connectors", {
    params: { expand: "status" },
  });
  return res.data;
}

export async function getKafkaConnectorTrace(connectorName: string): Promise<KafkaConnectorTraceResponse> {
  const res = await axios.get<KafkaConnectorTraceResponse>(
    `/kafka-connect-api/connectors/${encodeURIComponent(connectorName)}/status`,
  );
  return res.data;
}

export async function createNifiProcessGroup(name: string): Promise<NifiProcessGroupEntity> {
  const res = await apiClient.post("/nifi/process-groups", { name });
  return unwrap<NifiProcessGroupEntity>(res.data);
}

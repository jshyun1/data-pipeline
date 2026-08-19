import axios from "axios";
import { apiClient, unwrap, type ApiResponse } from "./client";

export interface NifiExecutionLogEntry {
  id: number;
  processorId: string;
  processorName: string;
  groupId?: string;
  groupName?: string;
  jobId?: number | null;
  jobName?: string | null;
  rootGroupName?: string | null;
  occurredAt: string;
  /** 실패 행은 셀 대상이 없어 null. */
  insertedCount: number | null;
  status: string;
  /** 실패 행의 원인(NiFi bulletin 원문). */
  message?: string | null;
  /** 실패 행의 심각도(ERROR/WARNING). */
  level?: string | null;
}

export interface AirflowDag {
  dag_id: string;
  is_active?: boolean;
  is_paused?: boolean;
}

export interface AirflowDagCatalogEntry {
  dagId: string;
  businessGroup: "CDC" | "ETL";
  businessFolder: string;
  displayName: string;
  description?: string;
  monitoringEnabled: boolean;
  consecutiveFailureThreshold: number;
  staleDaysThreshold: number;
  durationMultiplier: number;
  slaMinutes: number | null;
}

export async function listAirflowDagCatalog(): Promise<AirflowDagCatalogEntry[]> {
  const res = await apiClient.get<ApiResponse<AirflowDagCatalogEntry[]>>("/airflow/dag-catalog");
  return unwrap(res.data);
}

export interface AirflowDagCatalogSyncResult {
  discoveredCount: number;
  eligibleCount: number;
  createdCount: number;
  createdDagIds: string[];
  disabledCount: number;
  disabledDagIds: string[];
}

export async function syncAirflowDagCatalog(): Promise<AirflowDagCatalogSyncResult> {
  const res = await apiClient.post<ApiResponse<AirflowDagCatalogSyncResult>>("/airflow/dag-catalog/sync");
  return unwrap(res.data);
}

export async function deleteAirflowDagCatalog(dagId: string): Promise<void> {
  const res = await apiClient.delete<ApiResponse<void>>(
    `/airflow/dag-catalog/${encodeURIComponent(dagId)}`,
  );
  unwrap(res.data);
}

export interface AirflowMonitoringSettings {
  dagId: string;
  monitoringEnabled: boolean;
  consecutiveFailureThreshold: number;
  staleDaysThreshold: number;
  durationMultiplier: number;
  slaMinutes: number | null;
}

export interface AirflowDagAlert {
  id: number;
  dagId: string;
  ruleType: "CONSECUTIVE_FAILURE" | "STALE" | "DURATION_ANOMALY" | "SLA_EXCEEDED";
  severity: "DANGER" | "WARNING" | "INFO";
  status: "OPEN" | "ACKNOWLEDGED" | "RESOLVED";
  message: string;
  detectedAt: string;
  lastDetectedAt: string;
  acknowledgedAt?: string | null;
  resolvedAt?: string | null;
}

export async function listAirflowDagAlerts(): Promise<AirflowDagAlert[]> {
  const res = await apiClient.get<ApiResponse<AirflowDagAlert[]>>("/airflow/alerts");
  return unwrap(res.data);
}

export async function listAirflowDagAlertHistory(): Promise<AirflowDagAlert[]> {
  const res = await apiClient.get<ApiResponse<AirflowDagAlert[]>>("/airflow/alerts/history");
  return unwrap(res.data);
}

export async function saveAirflowMonitoringSettings(
  dagId: string,
  settings: Omit<AirflowMonitoringSettings, "dagId">,
): Promise<AirflowMonitoringSettings> {
  const res = await apiClient.patch<ApiResponse<AirflowMonitoringSettings>>(
    `/airflow/dag-catalog/${encodeURIComponent(dagId)}/monitoring`,
    settings,
  );
  return unwrap(res.data);
}

export async function acknowledgeAirflowDagAlert(id: number): Promise<AirflowDagAlert> {
  const res = await apiClient.patch<ApiResponse<AirflowDagAlert>>(`/airflow/alerts/${id}/acknowledge`);
  return unwrap(res.data);
}

export interface AirflowDagRun {
  dag_id?: string;
  dag_run_id: string;
  state?: string;
  run_type?: string;
  duration?: number;
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
  processorCount?: number | null;
}

export interface NifiProcessGroupTreeNode {
  id: string;
  name: string;
  processorCount: number;
  runningCount: number;
  stoppedCount: number;
  invalidCount: number;
  disabledCount: number;
  children: NifiProcessGroupTreeNode[];
}

export interface NifiProcessorDetailResponse {
  id?: string;
  revision?: { version?: number | null };
  permissions?: { canRead?: boolean; canWrite?: boolean };
  component?: {
    id?: string;
    parentGroupId?: string;
    name?: string;
    type?: string;
    bundleGroup?: string;
    bundleArtifact?: string;
    bundleVersion?: string;
    bundle?: { group?: string; artifact?: string; version?: string };
    state?: string;
    validationStatus?: string;
    position?: { x?: number; y?: number };
    comments?: string | null;
    config?: {
      properties?: Record<string, string | null | undefined>;
      descriptors?: Record<
        string,
        {
          name?: string;
          displayName?: string;
          description?: string;
          sensitive?: boolean;
          dynamic?: boolean;
          required?: boolean;
          identifiesControllerService?: boolean;
        }
      >;
      schedulingStrategy?: string;
      schedulingPeriod?: string;
      executionNode?: string;
      penaltyDuration?: string;
      yieldDuration?: string;
      concurrentlySchedulableTaskCount?: number;
      comments?: string | null;
      runDurationMillis?: string;
      bulletinLevel?: string;
      retryCount?: string;
      retriedRelationships?: string;
      autoTerminatedRelationships?: string[];
    };
    descriptors?: Record<string, NifiProcessorPropertyDescriptor>;
    propertyDescriptors?: Record<string, NifiProcessorPropertyDescriptor>;
    relationships?: Array<{ name?: string; description?: string; autoTerminate?: boolean }>;
    autoTerminatedRelationships?: string[];
    supportsParallelProcessing?: string;
    supportsEventDriven?: string;
    supportsBatching?: string;
  };
  status?: {
    runStatus?: string;
    validationStatus?: string;
    activeThreadCount?: number;
    aggregateSnapshot?: {
      runStatus?: string;
      validationStatus?: string;
      activeThreadCount?: number;
    };
  };
  bulletins?: Array<{
    bulletin?: {
      level?: string;
      category?: string;
      message?: string;
      timestamp?: string;
    };
  }>;
}

interface NifiProcessorPropertyDescriptor {
  name?: string;
  displayName?: string;
  description?: string;
  sensitive?: boolean;
  dynamic?: boolean;
  required?: boolean;
  identifiesControllerService?: boolean;
}

export interface NifiControllerServiceEntity {
  id?: string;
  component?: {
    id?: string;
    name?: string;
    type?: string;
    state?: string;
    properties?: Record<string, string | null | undefined>;
  };
}

export interface NifiControllerServicesResponse {
  controllerServices?: NifiControllerServiceEntity[];
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

/**
 * 전체 DAG의 실행 이력을 한 번에 조회한다(dag_id 자리의 `~`가 와일드카드).
 *
 * <p>예전에는 DAG마다 따로 호출해서 파이프라인이 늘수록 요청이 비례해 늘었다
 * (DAG 12개 기준 30초마다 48회). 이 엔드포인트는 기간/정렬 필터를 그대로 받으므로
 * 같은 결과를 1회로 얻는다.
 *
 * <p>한 번에 받는 만큼 limit에 걸려 조용히 잘리면 집계가 틀어지므로, total_entries를
 * 보고 남은 페이지를 이어서 받는다.
 */
export async function listAllAirflowDagRuns(
  options: { startDateGte?: string; startDateLte?: string; pageSize?: number; maxRuns?: number } = {},
): Promise<AirflowDagRun[]> {
  const pageSize = options.pageSize ?? 500;
  const maxRuns = options.maxRuns ?? 5000;
  const runs: AirflowDagRun[] = [];

  for (let offset = 0; offset < maxRuns; offset += pageSize) {
    const res = await axios.get<{ dag_runs?: AirflowDagRun[]; total_entries?: number }>(
      `${AIRFLOW_API_BASE}/dags/~/dagRuns`,
      {
        params: {
          limit: pageSize,
          offset,
          order_by: "-start_date",
          start_date_gte: options.startDateGte,
          start_date_lte: options.startDateLte,
        },
      },
    );
    const page = res.data.dag_runs ?? [];
    runs.push(...page);
    if (page.length < pageSize || runs.length >= (res.data.total_entries ?? runs.length)) {
      break;
    }
  }
  return runs;
}

export async function triggerAirflowDag(
  dagId: string,
  conf: Record<string, unknown> = {},
): Promise<AirflowDagRun> {
  const res = await axios.post<AirflowDagRun>(
    `${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/dagRuns`,
    { logical_date: null, conf },
  );
  return res.data;
}

export async function listAirflowTaskInstances(dagId: string, dagRunId: string): Promise<AirflowTaskInstance[]> {
  const res = await axios.get<{ task_instances?: AirflowTaskInstance[] }>(
    `${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}/taskInstances`,
  );
  return res.data.task_instances ?? [];
}

export async function retryAirflowTaskFrom(dagId: string, dagRunId: string, taskId: string): Promise<void> {
  await axios.post(`${AIRFLOW_API_BASE}/dags/${encodeURIComponent(dagId)}/clearTaskInstances`, {
    dry_run: false,
    dag_run_id: dagRunId,
    task_ids: [taskId],
    include_downstream: true,
    only_failed: false,
    reset_dag_runs: true,
  });
}

interface AirflowLogMessage {
  timestamp?: string;
  event?: string;
  error_detail?: Array<{ exc_type?: string; exc_value?: string }>;
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
    .map((entry) => {
      if (typeof entry === "string") return entry;
      const error = entry.error_detail?.map((detail) => [detail.exc_type, detail.exc_value].filter(Boolean).join(": ")).filter(Boolean).join("; ");
      return `${entry.timestamp ? `[${entry.timestamp}] ` : ""}${entry.event ?? ""}${error ? `: ${error}` : ""}`;
    })
    .join("\n");
}


export async function getNifiRootStatus(): Promise<NifiRootStatusResponse> {
  const res = await axios.get<NifiRootStatusResponse>("/nifi-api/flow/process-groups/root/status", {
    params: { recursive: true },
  });
  return res.data;
}

export async function listRootNifiControllerServices(): Promise<NifiControllerServiceEntity[]> {
  const res = await axios.get<NifiControllerServicesResponse>("/nifi-api/flow/process-groups/root/controller-services", {
    params: { includeAncestorGroups: false, includeDescendantGroups: false },
  });
  return res.data.controllerServices ?? [];
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

export interface InitialDbToDbFlowCreateRequest {
  jobName: string;
  parentGroupId: string;
  comments: string;
  sourceServiceId: string;
  sourceDatabaseType: string;
  sourceSchema: string;
  sourceTable: string;
  targetServiceId: string;
  targetDatabaseType: string;
  targetSchema: string;
  targetTable: string;
  loadMode: "INSERT" | "TRUNCATE" | "UPSERT";
  truncateSql?: string;
  changeKeyColumn?: string;
  primaryKeys?: string;
}

export async function createInitialDbToDbFlow(
  request: InitialDbToDbFlowCreateRequest,
): Promise<NifiProcessGroupEntity> {
  const res = await apiClient.post<ApiResponse<NifiProcessGroupEntity>>("/nifi/etl/initial-db-to-db", request);
  return unwrap<NifiProcessGroupEntity>(res.data);
}

export async function getNifiProcessGroupTree(): Promise<NifiProcessGroupTreeNode> {
  const res = await apiClient.get<ApiResponse<NifiProcessGroupTreeNode>>("/nifi/process-group-tree");
  return unwrap<NifiProcessGroupTreeNode>(res.data);
}

export async function getNifiProcessor(processorId: string): Promise<NifiProcessorDetailResponse> {
  const res = await apiClient.get<ApiResponse<NifiProcessorDetailResponse>>(
    `/nifi/processors/${encodeURIComponent(processorId)}`,
  );
  return unwrap<NifiProcessorDetailResponse>(res.data);
}

async function saveAirflowVariable(key: string, value: string): Promise<void> {
  try {
    await axios.post(`${AIRFLOW_API_BASE}/variables`, { key, value });
  } catch (error) {
    // 신규 그룹의 Variable은 POST로 만들되, 네트워크 오류 뒤 재시도처럼 이미 만들어진
    // 경우에는 같은 값을 PATCH해서 저장 작업을 멱등하게 만든다.
    if (axios.isAxiosError(error) && error.response?.status === 409) {
      await axios.patch(`${AIRFLOW_API_BASE}/variables/${encodeURIComponent(key)}`, { key, value });
      return;
    }
    throw error;
  }
}

async function deleteAirflowVariable(key: string): Promise<void> {
  try {
    await axios.delete(`${AIRFLOW_API_BASE}/variables/${encodeURIComponent(key)}`);
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 404) {
      return;
    }
    throw error;
  }
}

/** NiFi 동적 DAG의 실행 주기를 저장한다. 빈 값은 수동 실행 전용으로 되돌린다. */
export async function saveAirflowDagSchedule(dagId: string, cron?: string): Promise<void> {
  if (!/^nifi_pipeline_[a-z0-9]{8}_control$/i.test(dagId)) {
    throw new Error("NiFi에서 생성된 DAG만 화면에서 스케줄을 변경할 수 있습니다.");
  }
  const scheduleKey = `${dagId}__schedule`;
  if (cron?.trim()) {
    await saveAirflowVariable(scheduleKey, cron.trim());
  } else {
    await deleteAirflowVariable(scheduleKey);
  }
  await saveAirflowVariable(`${dagId}__auto_stop_after_run`, "true");
}

/** NiFi 동적 DAG가 읽는 스케줄과 배치 실행 후 자동 정지 설정을 함께 저장한다. */
export async function saveNifiDagSchedule(processGroupId: string, cron: string): Promise<string> {
  if (processGroupId.length < 8) {
    throw new Error("올바르지 않은 Processor Group ID입니다.");
  }

  const dagId = `nifi_pipeline_${processGroupId.slice(0, 8)}_control`;
  await saveAirflowDagSchedule(dagId, cron);
  return dagId;
}

// NiFi에는 Airflow의 dag_run 같은 "실행 이력" 개념이 없고, Provenance 조회도 이 환경에서
// 구조적으로 안 되는 것으로 확인돼서(인덱스/이벤트파일 불일치), 적재 프로세서(PutDatabaseRecord)의
// 누적 카운터 증가분을 60초 주기로 감지해 한 행씩 남긴 것을 백엔드가 대신 제공한다
// (NifiPipelineMetricScheduler). "실행 1회 = 행 1개"가 아니라 "60초 구간 안에 증가가
// 있었다 = 행 1개"에 가깝다.
export async function listNifiExecutionLogs(from: string, to: string): Promise<NifiExecutionLogEntry[]> {
  const res = await apiClient.get<ApiResponse<NifiExecutionLogEntry[]>>("/nifi/execution-logs", {
    params: { from, to },
  });
  return unwrap(res.data);
}

/** 적재 프로세서의 실행 구간 1회. 시작~종료가 있어 소요시간과 처리량을 알 수 있다. */
export interface NifiProcessorRun {
  id: number;
  processorId: string;
  processorName: string;
  /** PutDatabaseRecord / ExecuteGroovyScript 등. 소급 병합된 과거 구간은 null. */
  processorType?: string | null;
  groupId?: string | null;
  groupName?: string | null;
  /** 적재 대상 schema.table. 스크립트 기반 적재는 알 수 없어 null. */
  targetTable?: string | null;
  startedAt: string;
  /** 진행 중이면 null. */
  endedAt?: string | null;
  durationSeconds: number;
  insertedCount: number;
  /** 소요가 0초면(한 폴링 주기 안에 끝남) 계산 불가라 null. */
  rowsPerSecond?: number | null;
  status: string;
}

// 백엔드가 15초마다 activeThreadCount와 적재 카운터를 관측해 만든 구간이다. NiFi에는
// 실행 이력 개념이 없어서(Provenance 0건, 프로세서 Status History 빈 응답) 직접
// 관측하는 것 외에 방법이 없고, 그래서 시작/종료는 최대 15초 오차가 있는 추정값이다.
export async function listNifiProcessorRuns(from: string, to: string): Promise<NifiProcessorRun[]> {
  const res = await apiClient.get<ApiResponse<NifiProcessorRun[]>>("/nifi/processor-runs", {
    params: { from, to },
  });
  return unwrap(res.data);
}

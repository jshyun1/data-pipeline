import { apiClient, unwrap, type ApiResponse } from "./client";

export interface EtlJobResponse {
  id: number;
  nifiPgId: string;
  parentPgId?: string | null;
  parentGroupName?: string | null;
  jobName: string;
  engine: string;
  comments?: string | null;
  parameterContextName?: string | null;
  airflowDagId?: string | null;
  stepCount: number;
  runningCount: number;
  stoppedCount: number;
  invalidCount: number;
  firstSeenAt: string;
  lastSyncedAt: string;
}

export interface EtlJobStepView {
  id: number;
  nifiProcessorId: string;
  stepName: string;
  stepType?: string | null;
  schedulingStrategy?: string | null;
  schedulingPeriod?: string | null;
  sqlText?: string | null;
  targetTable?: string | null;
  statementType?: string | null;
  updateKeys?: string | null;
  dbcpServiceId?: string | null;
  propsJson?: string | null;
  validationStatus?: string | null;
  runStatus?: string | null;
  xPos?: number | null;
  yPos?: number | null;
}

export interface EtlJobLinkView {
  fromComponentId?: string | null;
  fromName?: string | null;
  toComponentId?: string | null;
  toName?: string | null;
  relationships?: string | null;
}

export interface EtlJobParamView {
  paramName: string;
  paramValue?: string | null;
  sensitive: boolean;
  description?: string | null;
  syncDirection?: string | null;
}

export interface EtlJobDetailResponse {
  job: EtlJobResponse;
  steps: EtlJobStepView[];
  links: EtlJobLinkView[];
  params: EtlJobParamView[];
}

export interface EtlJobRunResponse {
  id: number;
  jobId: number;
  status: string;
  triggerSource?: string | null;
  airflowDagRunId?: string | null;
  startedAt: string;
  endedAt?: string | null;
  durationSeconds?: number | null;
  stepRunCount: number;
  totalInserted: number;
  failedStepCount: number;
}

export async function listEtlJobs(): Promise<EtlJobResponse[]> {
  const res = await apiClient.get<ApiResponse<EtlJobResponse[]>>("/etl/jobs");
  return unwrap(res.data);
}

export async function getEtlJob(id: number): Promise<EtlJobDetailResponse> {
  const res = await apiClient.get<ApiResponse<EtlJobDetailResponse>>(`/etl/jobs/${id}`);
  return unwrap(res.data);
}

export async function getEtlJobRuns(id: number): Promise<EtlJobRunResponse[]> {
  const res = await apiClient.get<ApiResponse<EtlJobRunResponse[]>>(`/etl/jobs/${id}/runs`);
  return unwrap(res.data);
}

/** NiFi 잡 미러링 1회 실행 결과. 화면에서 "무엇이 바뀌었는지" 알려주는 데 쓴다. */
export interface EtlJobSyncResult {
  jobsSeen: number;
  created: number;
  updated: number;
  deleted: number;
  snapshots: number;
}

/**
 * 5분 주기를 기다리지 않고 NiFi에서 즉시 다시 읽어온다.
 *
 * 캔버스에서 job을 만들거나 지운 직후에는 화면을 새로고침해도 소용이 없다 - 백엔드가
 * 아직 NiFi를 안 봤기 때문이다. 그래서 조회를 다시 하기 전에 이걸 먼저 부른다.
 */
export async function syncEtlJobs(): Promise<EtlJobSyncResult> {
  const res = await apiClient.post<ApiResponse<EtlJobSyncResult>>("/etl/jobs/sync");
  return unwrap(res.data);
}

/**
 * 실행 원장의 job 실행 한 건. 열린 실행(endedAt 없음)의 rowsProcessed 는 «지금까지 적재 건수»다 -
 * NiFi 완료 감시가 실행 중에 2초 판정마다 바뀐 값을 보낸다(2026-10-06).
 */
export interface JobRunLedgerView {
  id: number;
  jobId: number;
  status: string;
  completionSource?: string | null;
  rowsProcessed?: number | null;
  startedAt: string;
  endedAt?: string | null;
  nodeKey?: string | null;
}

/** 워크플로우 실행 한 번(Airflow dag run)이 연 job 실행들. 로그 화면이 실행 중인 job 의 진행 건수를 읽는다. */
export async function listJobRunsOfDagRun(dagRunId: string): Promise<JobRunLedgerView[]> {
  const res = await apiClient.get<ApiResponse<JobRunLedgerView[]>>("/etl/job-runs/by-dag-run", { params: { dagRunId } });
  return unwrap(res.data);
}

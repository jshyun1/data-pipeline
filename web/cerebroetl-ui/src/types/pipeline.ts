import type { DbType } from "./connection";

// web/backend의 com.company.pipeline.pipeline.dto.* / PipelineStatus 와 1:1로 맞춘 타입.
// UPSERT: 타깃을 소스와 동일하게 유지. DELTA_APPEND: 이벤트마다 한 행 append(순번 cdc_seq).
// DELTA_UPSERT: PK당 한 행, 마지막 상태 + 마지막 작업 종류(시각 cdc_ts). 백엔드 PipelineLoadMode 와 1:1.
export type PipelineLoadMode = "UPSERT" | "DELTA_APPEND" | "DELTA_UPSERT";

export function loadModeLabel(mode: PipelineLoadMode | null | undefined, opColumn?: string | null): string {
  const column = opColumn ?? "cdc_op";
  if (mode === "DELTA_APPEND") return `델타 append · 이벤트마다 한 행 (구분컬럼 ${column})`;
  if (mode === "DELTA_UPSERT") return `델타 최신상태 · PK당 한 행 (구분컬럼 ${column})`;
  return "동기화 upsert";
}

export type PipelineStatus = "CREATED" | "DEPLOYING" | "READY" | "DEPLOYED" | "PAUSED" | "STOPPED" | "FAILED";

export interface PipelineConnectorSummary {
  id: number;
  connectorRole: "SOURCE" | "SINK";
  connectorName: string;
  connectorClass: string;
  status: string;
  connectorConfigJson: string;
  lastStatusJson: string | null;
}

// web/backend의 PipelineCommandHistoryResponse와 1:1.
export interface PipelineCommandHistoryResponse {
  id: number;
  pipelineId: number;
  command: string;
  result: string | null;
  message: string | null;
  requestedAt: string;
  completedAt: string | null;
}

// LOG_FILE 파이프라인은 소스가 DB 연결이 아니라 파일이라 source* 필드가 전부 null이다.
export interface PipelineResponse {
  id: number;
  name: string;
  pipelineType: string;
  sourceConnectionId: number | null;
  targetConnectionId: number;
  sourceDbType: DbType | null;
  targetDbType: DbType;
  sourceSchema: string | null;
  sourceTable: string | null;
  targetSchema: string;
  targetTable: string;
  topicName: string;
  status: PipelineStatus;
  snapshotMode: "INITIAL" | "NO_DATA";
  excludedColumns: string | null;
  maskedColumns: string | null;
  deleteEnabled: boolean;
  // UPSERT: 타깃을 소스와 같은 모습으로 유지(기존). DELTA_APPEND: 변경 이벤트를 구분컬럼과 함께
  // append-only 델타 테이블에 한 행씩 쌓는다(CDC 없는 외부 솔루션이 주기적으로 읽어 가는 용도).
  loadMode: PipelineLoadMode;
  deltaOpColumn: string | null;
  description: string | null;
  connectors: PipelineConnectorSummary[];
  createdAt: string;
  updatedAt: string;
}

export interface PipelineRuntimeStatusResponse {
  pipelineId: number;
  storedStatus: PipelineStatus;
  runtimeStatus: "NOT_DEPLOYED" | "READY" | "RUNNING" | "PAUSED" | "STOPPED" | "FAILED" | "MISSING" | "UNKNOWN" | "DEGRADED";
  sourceConnectorState: string | null;
  sourceTaskStates: string[];
  sinkConnectorState: string | null;
  sinkTaskStates: string[];
  runtimeCheckedAt: string | null;
  statusMismatch: boolean;
  runtimeStatusReason: string | null;
  lastCommand: string | null;
  lastCommandResult: string | null;
  lastCommandMessage: string | null;
  lastCommandAt: string | null;
}

export interface PipelineConsistencyCheckResponse {
  id: number;
  pipelineId: number;
  checkMode: "STATISTICS_ESTIMATE";
  sourceCount: number | null;
  targetCount: number | null;
  difference: number | null;
  result: "MATCH" | "MISMATCH" | "UNKNOWN" | "UNSUPPORTED";
  message: string | null;
  checkedAt: string;
}

export interface PipelineCreateRequest {
  name: string;
  sourceConnectionId: number;
  targetConnectionId: number;
  sourceSchema: string;
  sourceTable: string;
  targetSchema: string;
  targetTable: string;
  topicPrefix: string;
  snapshotMode?: "INITIAL" | "NO_DATA";
  excludedColumns?: string[];
  maskedColumns?: string[];
  deleteEnabled?: boolean;
  loadMode?: PipelineLoadMode;
  deltaOpColumn?: string;
  description?: string;
}

// web/backend의 LogPipelineCreateRequest와 1:1. parseType/multilineEnabled는 이번 버전
// PLAIN/false만 지원해서 화면에 노출하지 않는다(백엔드가 자동으로 그 값으로 처리).
export interface LogPipelineCreateRequest {
  name: string;
  agentHost?: string;
  filePath: string;
  filePattern?: string;
  readFrom?: "BEGINNING" | "END";
  encoding?: string;
  targetConnectionId: number;
  targetSchema: string;
  targetTable: string;
  topicName?: string;
  description?: string;
}

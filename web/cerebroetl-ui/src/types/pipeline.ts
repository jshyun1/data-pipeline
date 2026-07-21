import type { DbType } from "./connection";

// web/backend의 com.company.pipeline.pipeline.dto.* / PipelineStatus 와 1:1로 맞춘 타입.
export type PipelineStatus = "CREATED" | "DEPLOYING" | "DEPLOYED" | "PAUSED" | "STOPPED" | "FAILED";

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
  deleteEnabled: boolean;
  description: string | null;
  connectors: PipelineConnectorSummary[];
  createdAt: string;
  updatedAt: string;
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
  deleteEnabled?: boolean;
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

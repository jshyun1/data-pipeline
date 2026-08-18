import { apiClient, unwrap, type ApiResponse } from "./client";

export type CdcProcessingStatus = "SUCCESS" | "DELAYED" | "STOPPED" | "FAILED";

export interface CdcProcessingLogEntry {
  pipelineId: number;
  pipelineName: string;
  source: string;
  target: string;
  topicName: string;
  occurredAt: string;
  processedCount: number;
  committedOffset: number;
  dailyProcessedCount: number;
  consumerLag: number;
  /** 로그 파이프라인은 소스가 filebeat라 Kafka Connect 소스 커넥터가 없다 -> null. */
  sourceState: string | null;
  sinkState: string;
  status: CdcProcessingStatus;
  message: string | null;
}

export interface CdcEventLogEntry {
  id: number;
  pipelineId: number;
  pipelineName: string;
  command: string;
  result: string | null;
  message: string | null;
  requestedBy: string | null;
  occurredAt: string;
  completedAt: string | null;
}

export interface DlqRecordEntry {
  pipelineId: number;
  pipelineName: string;
  topic: string;
  partition: number;
  offset: number;
  occurredAt: string;
  connectorName: string | null;
  errorClass: string | null;
  errorMessage: string | null;
}

export interface DlqRecordDetail extends DlqRecordEntry {
  key: string | null;
  payload: string | null;
}

export async function listCdcProcessingLogs(from: string, to: string): Promise<CdcProcessingLogEntry[]> {
  const res = await apiClient.get<ApiResponse<CdcProcessingLogEntry[]>>("/cdc/logs/processing", {
    params: { from, to },
  });
  return unwrap(res.data);
}

export async function listCdcEventLogs(from: string, to: string): Promise<CdcEventLogEntry[]> {
  const res = await apiClient.get<ApiResponse<CdcEventLogEntry[]>>("/cdc/logs/events", {
    params: { from, to },
  });
  return unwrap(res.data);
}

export async function listDlqRecords(pipelineId: number, from: string, to: string): Promise<DlqRecordEntry[]> {
  const res = await apiClient.get<ApiResponse<DlqRecordEntry[]>>("/cdc/logs/dlq", { params: { pipelineId, from, to } });
  return unwrap(res.data);
}

export async function getDlqRecordDetail(pipelineId: number, partition: number, offset: number): Promise<DlqRecordDetail> {
  const res = await apiClient.get<ApiResponse<DlqRecordDetail>>("/cdc/logs/dlq/detail", { params: { pipelineId, partition, offset } });
  return unwrap(res.data);
}

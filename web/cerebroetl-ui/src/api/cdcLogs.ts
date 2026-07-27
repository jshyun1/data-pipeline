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
  consumerLag: number;
  sourceState: string;
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

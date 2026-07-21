import axios from "axios";

export interface AirflowHealthResponse {
  metadatabase?: { status?: string };
  scheduler?: { status?: string };
  triggerer?: { status?: string };
  dag_processor?: { status?: string };
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

export async function getAirflowHealth(): Promise<AirflowHealthResponse> {
  const res = await axios.get<AirflowHealthResponse>("/airflow-api/monitor/health");
  return res.data;
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

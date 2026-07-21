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


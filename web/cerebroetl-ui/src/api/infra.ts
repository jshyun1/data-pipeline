import { apiClient, unwrap, type ApiResponse } from "./client";

export interface HostCpuUsage {
  usedPercent: number;
  cores: number;
}

export interface HostMemoryUsage {
  totalBytes: number;
  usedBytes: number;
  availableBytes: number;
  usedPercent: number;
}

export interface HostDiskUsage {
  mount: string;
  totalBytes: number;
  usedBytes: number;
  availableBytes: number;
  usedPercent: number;
}

export interface HostSystemInfo {
  uptimeSeconds: number;
  load1: number;
  load5: number;
  load15: number;
  runningProcesses: number;
  totalProcesses: number;
}

/** 읽을 수 없었던 항목은 null로 온다(procfs가 없는 환경 등) - 화면에서 "측정 불가"로 표시한다. */
export interface HostResourceResponse {
  collectedAt: string;
  cpu: HostCpuUsage | null;
  memory: HostMemoryUsage | null;
  disks: HostDiskUsage[];
  system: HostSystemInfo | null;
}

// pipeline-api는 컨테이너로 뜨지만 /proc/stat·/proc/meminfo는 네임스페이스로 가려지지
// 않아서 여기 값은 컨테이너가 아니라 서버(호스트) 기준이다.
export async function getHostResources(): Promise<HostResourceResponse> {
  const res = await apiClient.get<ApiResponse<HostResourceResponse>>("/infra/resources");
  return unwrap(res.data);
}

/**
 * STOPPED는 사람이 일부러 멈춘 상태(장애 아님), UNKNOWN은 "죽었다"가 아니라
 * "물어볼 수 없었다/구성 안 함"이다.
 */
export type ProcessStatus = "UP" | "STOPPED" | "DEGRADED" | "DOWN" | "UNKNOWN";

export interface ProcessItem {
  name: string;
  status: ProcessStatus;
  detail?: string | null;
}

export interface ProcessGroup {
  key: string;
  label: string;
  status: ProcessStatus;
  processes: ProcessItem[];
}

export interface ProcessHealthResponse {
  collectedAt: string;
  groups: ProcessGroup[];
}

export async function getProcessHealth(): Promise<ProcessHealthResponse> {
  const res = await apiClient.get<ApiResponse<ProcessHealthResponse>>("/infra/processes");
  return unwrap(res.data);
}

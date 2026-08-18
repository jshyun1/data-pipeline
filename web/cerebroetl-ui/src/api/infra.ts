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
  /**
   * false면 "이 설치에 구성되지 않음" = 미사용. 장애가 아니라 감시 대상이 아니라는 뜻이라
   * 화면은 회색 ○로 그리고 이상 카운트에서 뺀다(원본 문서 5-4).
   * UNKNOWN 하나로 "못 물어봤다"와 "안 쓴다"를 섞으면 회색을 아무도 안 보게 된다.
   */
  configured: boolean;
  /** 마지막 성공 시각. 툴팁의 "마지막 성공" 줄에 쓴다. */
  lastHeartbeatAt?: string | null;
  /** 판정 기준(초). 화면에 하드코딩하면 설정이 바뀌는 순간 화면이 거짓말을 한다. */
  staleAfterSeconds?: number | null;
  downAfterSeconds?: number | null;
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

/* -------------------------------------------------------------------------
 * 리소스 추이(스파크라인) — 원본 문서 5-3 #11
 * ---------------------------------------------------------------------- */

export interface ResourcePoint {
  at: string;
  usedPercent: number | null;
  usedBytes: number | null;
  totalBytes: number | null;
}

/** 키는 "CPU" | "MEMORY" | "LOAD1" | "DISK:{mount}". 결손 구간은 행이 아예 없다. */
export type ResourceTimeseries = Record<string, ResourcePoint[]>;

export async function getResourceTimeseries(minutes = 60): Promise<ResourceTimeseries> {
  const res = await apiClient.get<ApiResponse<ResourceTimeseries>>("/infra/resources/timeseries", {
    params: { minutes },
  });
  return unwrap(res.data);
}

/* -------------------------------------------------------------------------
 * 디스크 용도별 — 원본 문서 5-3 #12
 * ---------------------------------------------------------------------- */

export interface DiskBreakdownEntry {
  label: string;
  path: string;
  usedBytes: number;
  isLowerBound: boolean;
  /** BLOCKS=du 실측(정확) / APPARENT=파일 길이 합(희소 파일에서 과대) / NONE */
  measuredBy: "BLOCKS" | "APPARENT" | "NONE";
  error: string | null;
}

export interface DiskBreakdown {
  collectedAt: string | null;
  entries: DiskBreakdownEntry[];
}

// 마운트를 안 걸어둔 환경에서는 entries 가 비어 온다 - 화면은 구역을 통째로 감춘다.
export async function getDiskBreakdown(): Promise<DiskBreakdown> {
  const res = await apiClient.get<ApiResponse<DiskBreakdown>>("/infra/resources/breakdown");
  return unwrap(res.data);
}

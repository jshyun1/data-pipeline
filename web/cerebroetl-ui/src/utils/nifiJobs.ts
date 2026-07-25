import type { NifiBulletin, NifiProcessGroupStatusSnapshot } from "../api/platform";

export interface NifiJob {
  id: string;
  name: string;
  status: "RUNNING" | "FAILED" | "STOPPED";
  processorCount: number;
  runningProcessorCount: number;
  failedProcessorCount: number;
  queued: string;
  flowFilesQueued: number;
  activeThreadCount: number;
  errorMessage?: string;
}

// Invalid 설정(구성 오류)만으로는 실제 처리 중 오류(예: SQL 예외로 인한 반복 실패)를
// 못 잡는다 - 그런 경우 프로세서는 runStatus=Running을 유지한 채 bulletin만 계속 쌓인다.
// groupId -> 가장 최근 ERROR 메시지로 맵을 만들어 failed 판정에 같이 반영한다.
export function extractErrorGroupMessages(bulletins: NifiBulletin[] = []): Map<string, string> {
  const errors = new Map<string, string>();
  bulletins.forEach((entry) => {
    const level = entry.bulletin?.level;
    const groupId = entry.groupId;
    if (level !== "ERROR" || !groupId) {
      return;
    }
    if (!errors.has(groupId)) {
      errors.set(groupId, entry.bulletin?.message ?? "오류 메시지 없음");
    }
  });
  return errors;
}

export function collectNifiJobs(
  groups: NifiProcessGroupStatusSnapshot[] = [],
  errorGroupMessages: Map<string, string> = new Map(),
): NifiJob[] {
  return groups.flatMap((group) => {
    const snapshot = group.processGroupStatusSnapshot;
    if (!snapshot?.id) {
      return [];
    }

    const processors = snapshot.processorStatusSnapshots ?? [];
    const childJobs = collectNifiJobs(snapshot.processGroupStatusSnapshots, errorGroupMessages);
    const readableProcessors = processors
      .map((processor) => processor.processorStatusSnapshot)
      .filter((processor) => Boolean(processor?.id));

    if (readableProcessors.length === 0) {
      return childJobs;
    }

    const runningProcessorCount = readableProcessors.filter((processor) => processor?.runStatus === "Running").length;
    const invalidProcessorCount = readableProcessors.filter((processor) => processor?.runStatus === "Invalid").length;
    const bulletinError = errorGroupMessages.get(snapshot.id);
    const failedProcessorCount = invalidProcessorCount + (bulletinError && invalidProcessorCount === 0 ? 1 : 0);
    const status = failedProcessorCount > 0 ? "FAILED" : runningProcessorCount > 0 ? "RUNNING" : "STOPPED";

    return [
      {
        id: snapshot.id,
        name: snapshot.name ?? snapshot.id,
        status,
        processorCount: readableProcessors.length,
        runningProcessorCount,
        failedProcessorCount,
        queued: snapshot.queued ?? `${snapshot.flowFilesQueued ?? 0}`,
        flowFilesQueued: snapshot.flowFilesQueued ?? 0,
        activeThreadCount: snapshot.activeThreadCount ?? 0,
        errorMessage: bulletinError,
      },
      ...childJobs,
    ];
  });
}

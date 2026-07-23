import type { NifiProcessGroupStatusSnapshot } from "../api/platform";

export interface NifiJob {
  id: string;
  name: string;
  status: "RUNNING" | "FAILED" | "STOPPED";
  processorCount: number;
  runningProcessorCount: number;
  failedProcessorCount: number;
  queued: string;
  activeThreadCount: number;
}

export function collectNifiJobs(groups: NifiProcessGroupStatusSnapshot[] = []): NifiJob[] {
  return groups.flatMap((group) => {
    const snapshot = group.processGroupStatusSnapshot;
    if (!snapshot?.id) {
      return [];
    }

    const processors = snapshot.processorStatusSnapshots ?? [];
    const childJobs = collectNifiJobs(snapshot.processGroupStatusSnapshots);
    const readableProcessors = processors
      .map((processor) => processor.processorStatusSnapshot)
      .filter((processor) => Boolean(processor?.id));

    if (readableProcessors.length === 0) {
      return childJobs;
    }

    const runningProcessorCount = readableProcessors.filter((processor) => processor?.runStatus === "Running").length;
    const failedProcessorCount = readableProcessors.filter((processor) => processor?.runStatus === "Invalid").length;
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
        activeThreadCount: snapshot.activeThreadCount ?? 0,
      },
      ...childJobs,
    ];
  });
}

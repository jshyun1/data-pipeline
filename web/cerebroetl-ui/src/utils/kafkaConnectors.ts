import type { KafkaConnectorStatusMap } from "../api/platform";

export interface KafkaConnectorJob {
  name: string;
  role: "source" | "sink" | string;
  status: "RUNNING" | "FAILED" | "PAUSED" | "UNASSIGNED";
  totalTaskCount: number;
  failedTaskCount: number;
}

// Kafka Connect는 태스크 하나가 죽어도(connector.state=RUNNING인 채로) 커넥터 자체
// 상태는 안 바뀌는 경우가 많아서, connector.state뿐 아니라 tasks[].state도 같이
// 봐야 실제 실패를 놓치지 않는다.
export function summarizeKafkaConnectors(statusMap: KafkaConnectorStatusMap = {}): KafkaConnectorJob[] {
  return Object.values(statusMap).flatMap((entry) => {
    const status = entry.status;
    if (!status?.name) {
      return [];
    }
    const tasks = status.tasks ?? [];
    const failedTaskCount = tasks.filter((task) => task.state === "FAILED").length;
    const connectorState = status.connector?.state;
    const jobStatus: KafkaConnectorJob["status"] =
      connectorState === "FAILED" || failedTaskCount > 0
        ? "FAILED"
        : connectorState === "PAUSED"
          ? "PAUSED"
          : connectorState === "RUNNING"
            ? "RUNNING"
            : "UNASSIGNED";

    return [
      {
        name: status.name,
        role: status.type ?? "unknown",
        status: jobStatus,
        totalTaskCount: tasks.length,
        failedTaskCount,
      },
    ];
  });
}

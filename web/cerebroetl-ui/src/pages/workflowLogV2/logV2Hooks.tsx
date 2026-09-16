import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { message, Modal } from "antd";
import {
  getAirflowTaskLog,
  listAirflowTaskInstances,
  markAirflowTaskSuccess,
  retryAirflowTasks,
  type AirflowTaskInstance,
} from "../../api/platform";
import { groupJobs, jobKeyOf } from "./logV2Data";

/**
 * 한 실행의 task 목록과(성공 task 의) 로그. 목록 행·상세 창·타임라인이 같은 캐시 키를 써서 한 번만 받는다.
 * 적재 건수는 성공 task 로그에서 읽는다 - 기존 화면과 같은 방식이다.
 */
export function useRunTasks(
  dagId: string | undefined,
  runId: string | undefined,
  { enabled, refreshSeconds, withLogs = true }: { enabled: boolean; refreshSeconds: number; withLogs?: boolean },
) {
  const tasksQuery = useQuery({
    queryKey: ["logv2-tasks", dagId, runId],
    queryFn: () => listAirflowTaskInstances(dagId!, runId!),
    enabled: enabled && Boolean(dagId && runId),
    refetchInterval: refreshSeconds * 1000,
  });
  const tasks = tasksQuery.data;
  const completed = (tasks ?? []).filter((task) => task.state === "success");
  const signature = completed.map((task) => `${task.task_id}:${task.try_number}`).join(",");
  const logsQuery = useQuery({
    queryKey: ["logv2-task-logs", dagId, runId, signature],
    queryFn: async () => Object.fromEntries(await Promise.all(completed.map(async (task): Promise<[string, string]> => [
      task.task_id,
      await getAirflowTaskLog(dagId!, runId!, task.task_id, task.try_number || 1).catch(() => ""),
    ]))) as Record<string, string>,
    enabled: enabled && withLogs && Boolean(dagId && runId && completed.length),
    staleTime: Infinity,
  });
  const jobs = tasks ? groupJobs(tasks) : undefined;
  // 성공 task 가 하나도 없으면 읽을 로그가 없다 - «불러오는 중»으로 남지 않게 빈 객체로 둔다.
  const logs = completed.length ? logsQuery.data : tasks ? {} : undefined;
  return { tasksQuery, tasks, jobs, logs };
}

/**
 * job 재실행(«이 작업만 실행» / «이 작업부터 실행»). 기존 화면 TaskRows 의 rerunJob·doRerun 을 그대로 옮겼다.
 *
 * <p>선행이 실패로 남은 job 을 그냥 clear 하면 Airflow 가 곧바로 upstream_failed 로 되돌리므로, 선행을 성공으로
 * 표시할지 먼저 묻는다. 자기 단계에 실패가 있으면 «선행 때문에 막힌» 것이 아니다(뒷단계 upstream_failed 착각 방지).
 */
export function useRerun(
  dagId: string | undefined,
  runId: string | undefined,
  tasks: AirflowTaskInstance[] | undefined,
  refetch: () => Promise<unknown>,
) {
  const [busyKey, setBusyKey] = useState<string>();

  const doRerun = async (jobKey: string, taskIds: string[], withDownstream: boolean, markBlockers: string[] = []) => {
    if (!dagId || !runId) return;
    setBusyKey(jobKey);
    try {
      for (const taskId of markBlockers) {
        await markAirflowTaskSuccess(dagId, runId, taskId);
      }
      await retryAirflowTasks(dagId, runId, taskIds, withDownstream);
      message.success(withDownstream ? `${jobKey}부터 후행까지 재실행했습니다.` : `${jobKey}만 재실행했습니다.`);
      await refetch();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "작업 재실행에 실패했습니다.");
    } finally {
      setBusyKey(undefined);
    }
  };

  const rerun = async (jobKey: string, taskIds: string[], withDownstream: boolean) => {
    if (!dagId || !runId) return;
    const all = tasks ?? [];
    const own = all.filter((task) => taskIds.includes(task.task_id));
    const blocked = own.some((task) => task.state === "upstream_failed")
      && !own.some((task) => task.state === "failed");
    if (!blocked) {
      await doRerun(jobKey, taskIds, withDownstream);
      return;
    }
    const blockers = all.filter((task) => task.state === "failed" && !taskIds.includes(task.task_id));
    const blockerJobs = [...new Set(blockers.map((task) => jobKeyOf(task.task_id)))];
    if (!blockerJobs.length) {
      message.warning("선행 작업이 끝나지 않아 아직 실행할 수 없습니다.");
      return;
    }
    Modal.confirm({
      title: "선행 작업이 실패로 남아 있습니다",
      okText: "선행을 성공 처리하고 실행",
      okButtonProps: { danger: true },
      cancelText: "취소",
      width: 560,
      content: (
        <div>
          <p>
            <b>{blockerJobs.join(", ")}</b> 이(가) 실패 상태여서, <b>{jobKey}</b> 만 다시 눌러도
            실행되지 않고 «선행 실패»로 되돌아갑니다.
          </p>
          <p>
            계속하면 위 선행 작업을 <b>성공으로 표시한 뒤</b> {jobKey} 을(를) 실행합니다.
            상태만 바꾸는 것이라 <b>선행의 적재는 일어나지 않습니다</b> - 빠진 데이터는 그대로입니다.
          </p>
          <p style={{ marginBottom: 0 }}>
            선행을 <b>다시 돌리려면</b> 취소하고 해당 행의 «이 작업부터 실행»을 쓰십시오.
          </p>
        </div>
      ),
      onOk: () => doRerun(jobKey, taskIds, withDownstream, blockers.map((task) => task.task_id)),
    });
  };

  return { busyKey, rerun };
}

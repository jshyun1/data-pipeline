import { useMemo, useState } from "react";
import { useQueries, useQuery } from "@tanstack/react-query";
import { message, Modal } from "antd";
import { listJobRunsOfDagRun } from "../../api/etlJobs";
import {
  getAirflowTaskLog,
  listAirflowTaskInstances,
  markAirflowTaskSuccess,
  retryAirflowTasks,
  type AirflowTaskInstance,
} from "../../api/platform";
import { groupJobs, jobKeyOf } from "./logV2Data";
import { hasLiveTasks, isLiveRun, LIVE_REFRESH_MS } from "./logV2Live";

/**
 * 한 실행의 task 목록과(성공 task 의) 로그. 목록 행·상세 창·타임라인이 같은 캐시 키를 써서 한 번만 받는다.
 * 적재 건수는 성공 task 로그에서 읽는다 - 기존 화면과 같은 방식이다.
 *
 * <p>실행 중이면(2026-10-06) task 를 3초마다 받고, 아직 끝나지 않은 job 의 «지금까지 적재 건수»를 실행 원장에서 읽어
 * {@code progress}(노드 키 → 건수)로 준다 - NiFi 완료 감시가 실행 중에 2초 판정마다 원장에 적는다. 다 끝나면 화면 주기로 돌아간다.
 */
export function useRunTasks(
  dagId: string | undefined,
  runId: string | undefined,
  { enabled, refreshSeconds, withLogs = true, runState }:
    { enabled: boolean; refreshSeconds: number; withLogs?: boolean; runState?: string },
) {
  const tasksQuery = useQuery({
    queryKey: ["logv2-tasks", dagId, runId],
    queryFn: () => listAirflowTaskInstances(dagId!, runId!),
    enabled: enabled && Boolean(dagId && runId),
    refetchInterval: (query) =>
      isLiveRun(runState) && hasLiveTasks(query.state.data) ? LIVE_REFRESH_MS : refreshSeconds * 1000,
  });
  const tasks = tasksQuery.data;
  const live = Boolean(tasks) && isLiveRun(runState) && hasLiveTasks(tasks);
  // 로그는 성공한 task 마다 한 번만 받는다 - 같은 시도의 로그는 바뀌지 않는다. 예전에는 task 하나가 끝날 때마다 끝난 task
  // 전부의 로그를 다시 받았다(job 5개짜리 실행이면 수십 번). 실행 중 갱신을 빠르게 하면서 그 부담까지 늘지 않게 했다.
  const completed = (tasks ?? []).filter((task) => task.state === "success");
  const logQueries = useQueries({
    queries: completed.map((task) => ({
      queryKey: ["logv2-task-log", dagId, runId, task.task_id, task.try_number || 1],
      queryFn: () => getAirflowTaskLog(dagId!, runId!, task.task_id, task.try_number || 1).catch(() => ""),
      enabled: enabled && withLogs && Boolean(dagId && runId),
      staleTime: Infinity,
    })),
  });
  const ledgerQuery = useQuery({
    queryKey: ["logv2-ledger", runId],
    queryFn: () => listJobRunsOfDagRun(runId!),
    enabled: enabled && withLogs && live && Boolean(runId),
    refetchInterval: LIVE_REFRESH_MS,
  });
  const jobs = tasks ? groupJobs(tasks) : undefined;
  // 받은 로그만 담는다. 아직 못 받은 task 는 빠져 있고, 화면은 그 칸을 «…»로 둔다.
  const logs: Record<string, string> | undefined = tasks
    ? Object.fromEntries(completed.flatMap((task, index) => {
      const log = logQueries[index]?.data;
      return log == null ? [] : [[task.task_id, log] as [string, string]];
    }))
    : undefined;
  const ledger = ledgerQuery.data;
  const progress = useMemo(() => {
    const out: Record<string, number> = {};
    if (!live) return out;
    // 한 노드가 재시도로 여러 번 열렸으면 마지막 것(시작 순 정렬)이 남는다.
    (ledger ?? []).forEach((run) => {
      if (run.nodeKey && run.rowsProcessed != null) out[run.nodeKey] = run.rowsProcessed;
    });
    return out;
  }, [ledger, live]);
  return { tasksQuery, tasks, jobs, logs, progress };
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

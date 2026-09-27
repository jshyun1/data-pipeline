import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import axios from "axios";
import { Alert, Button, Modal, Spin, Tag } from "antd";
import { GptErrorAnalysis } from "./gpt/GptErrorAnalysis";
import { useGptEnabled } from "./gpt/useGptEnabled";
import {
  getAirflowTaskLogEntries,
  listAirflowTaskEventLogs,
  listAirflowTaskInstances,
  type AirflowTaskInstance,
} from "../api/platform";
import { listWorkflows } from "../api/workflows";
import {
  failureEvents,
  KEPT_PROGRESS_LINES,
  parseFailureLog,
  type FailureLog,
  type FailureLogLine,
} from "../utils/airflowFailureLog";

/** 실패 로그 창을 열 대상: 한 DAG 실행에서 실패한 task 들(job 줄이면 그 job 의 실패 단계, 단계 줄이면 그 단계). */
export interface FailureLogTarget {
  title: string;
  dagId: string;
  runId: string;
  /** 어느 실행(차수)의 로그인지. 창을 열고 나면 목록이 안 보이므로 머리에 적어 둔다(예: «total · 09-18 10:54:59 · 2차»). */
  runLabel?: string;
  tasks: AirflowTaskInstance[];
}

interface LoadedAttempt {
  tryNumber: number;
  log?: FailureLog;
  error?: string;
}

interface LoadedTaskLog {
  taskId: string;
  /** 시도별 로그(1회차부터). */
  attempts: LoadedAttempt[];
  /** Airflow 기록(event log) 중 실패 사유를 말하는 것. 작업 로그에 원인이 없을 때 여기에만 남는 경우가 있다. */
  events: FailureLogLine[];
  eventsError?: string;
  /** 하위 워크플로우를 부른 task 였으면, 그 하위 실행에서 실패한 task 들의 로그. */
  child?: { dagId: string; runId: string; tasks: LoadedTaskLog[]; error?: string };
}

/** 하위의 하위까지 따라가는 깊이. 워크플로우 조립이 이보다 깊을 일은 없고, 순환 설정이어도 멈춘다. */
const MAX_DEPTH = 3;

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * 재시도가 있었으면 모든 시도의 로그를 받는다. 첫 시도에 원래 오류(NiFi bulletin 원문·연쇄 원인)가 온전히 남고,
 * 뒤 시도는 같은 실패를 잘린 문구로 다시 던지는 경우가 있다(실측 2026-09-11: DZ_COM001M await 4회 시도 -
 * 마지막 시도 로그만 보면 원인 문구가 중간에 끊겨 있었다).
 */
async function loadAttempts(dagId: string, runId: string, taskId: string, lastTry: number): Promise<LoadedAttempt[]> {
  return Promise.all(Array.from({ length: lastTry }, (_, index) => index + 1).map(async (tryNumber) => {
    try {
      return { tryNumber, log: parseFailureLog(await getAirflowTaskLogEntries(dagId, runId, taskId, tryNumber)) };
    } catch (error) {
      return { tryNumber, error: errorText(error) };
    }
  }));
}

/**
 * 실패 task 의 로그·Airflow 기록을 받고, 하위 워크플로우를 부른 task 면 하위 실행까지 따라간다.
 *
 * <p>상위 워크플로우의 «하위 호출» task 로그에는 «DagRun finished with failed state» 한 줄뿐이라 원인이 없다
 * (실측 2026-09-11: total 09-05 실행). 진짜 원인은 하위 실행 안의 실패 job 에 있으므로 거기까지 간다.
 * 하위 실행 id 는 DAG 생성기(etl_workflows_dynamic.py)의 규칙 그대로다: manual__{상위 run_id}__{노드 키}.
 */
async function loadTaskLogs(dagId: string, runId: string, tasks: AirflowTaskInstance[], depth: number): Promise<LoadedTaskLog[]> {
  return Promise.all(tasks.map(async (task): Promise<LoadedTaskLog> => {
    const attempts = await loadAttempts(dagId, runId, task.task_id, Math.max(1, task.try_number ?? 1));
    let events: FailureLogLine[] = [];
    let eventsError: string | undefined;
    try {
      events = failureEvents(await listAirflowTaskEventLogs(dagId, runId, task.task_id));
    } catch (error) {
      eventsError = errorText(error);
    }
    const loaded: LoadedTaskLog = { taskId: task.task_id, attempts, events, eventsError };
    // 하위 실행을 만들기 전에 실패했으면(하위 DAG 없음 등) 따라갈 곳이 없다 - 원인은 이 task 의 예외에 있다.
    const trigger = [...attempts].reverse().find((attempt) => attempt.log?.triggerDagId && attempt.log.childTriggered)?.log;
    if (trigger?.triggerDagId && depth < MAX_DEPTH) {
      const childDagId = trigger.triggerDagId;
      const groupKey = task.task_id.replace(/\.run$/, "");
      const childRunId = `manual__${runId}__${groupKey}`;
      try {
        const childTasks = await listAirflowTaskInstances(childDagId, childRunId);
        const failed = childTasks.filter((child) => child.state === "failed");
        loaded.child = {
          dagId: childDagId,
          runId: childRunId,
          tasks: await loadTaskLogs(childDagId, childRunId, failed, depth + 1),
        };
      } catch (error) {
        const notFound = axios.isAxiosError(error) && error.response?.status === 404;
        loaded.child = {
          dagId: childDagId,
          runId: childRunId,
          tasks: [],
          error: notFound ? "하위 워크플로우 실행 기록이 없습니다." : `하위 실행 기록을 불러오지 못했습니다: ${errorText(error)}`,
        };
      }
    }
    return loaded;
  }));
}

/** «dz_com_dz_com001m.await» 를 «dz_com_dz_com001m · await» 로. */
const taskLabel = (taskId: string) => taskId.replace(/\.([^.]+)$/, " · $1");

function LogLines({ lines }: { lines: FailureLogLine[] }) {
  return (
    <pre className="failure-log-lines">
      {lines.map((line, index) => (
        <div key={index} className={`failure-log-line is-${line.level}`}>
          <span className="failure-log-time">{line.time}</span>{line.text}
        </div>
      ))}
    </pre>
  );
}

function AttemptView({ attempt, single }: { attempt: LoadedAttempt; single: boolean }) {
  const log = attempt.log;
  return (
    <div className="failure-log-attempt">
      {!single && <div className="failure-log-attempt-head">{attempt.tryNumber}회차 시도</div>}
      {attempt.error || !log ? (
        <Alert type="warning" showIcon message={`로그를 불러오지 못했습니다: ${attempt.error ?? "알 수 없는 오류"}`} />
      ) : (
        <>
          {log.exceptions.length > 0 && (
            <div className="failure-log-block">
              <div className="failure-log-label">실패 원인</div>
              {log.exceptions.map((text, index) => (
                <pre key={index} className="failure-log-exception">{text}</pre>
              ))}
            </div>
          )}
          {(log.lines.length > 0 || single) && (
            <div className="failure-log-block">
              <div className="failure-log-label">
                실행 로그
                {log.omittedProgress > 0 && (
                  <span> · 반복되는 대기 줄 {log.omittedProgress}줄은 빼고 마지막 {KEPT_PROGRESS_LINES}줄만 보여 줍니다</span>
                )}
              </div>
              {log.lines.length > 0
                ? <LogLines lines={log.lines} />
                : <div className="failure-log-empty">남은 실행 로그가 없습니다.</div>}
            </div>
          )}
        </>
      )}
    </div>
  );
}

/** GPT 에 넘길 «화면에 보이는 실패 로그»: 시도마다 실패 원인과 오류·경고 줄, 끝으로 Airflow 기록. 하위 실행은 그쪽 창에서 따로 분석한다. */
function screenErrorText(item: LoadedTaskLog): string {
  const blocks: string[] = [];
  // 재시도는 대개 같은 오류를 되풀이한다. 앞 시도에서 보낸 문구는 다시 보내지 않는다(입력 길이 절약).
  const seen = new Set<string>();
  for (const attempt of item.attempts) {
    const log = attempt.log;
    if (!log) continue;
    const head = item.attempts.length > 1 ? `[${attempt.tryNumber}회차 시도]\n` : "";
    const exceptions = log.exceptions.filter((text) => !seen.has(text));
    const lines = log.lines
      .filter((line) => line.level === "error" || line.level === "critical" || line.level === "warning")
      .map((line) => line.text)
      .filter((text) => !seen.has(text));
    [...log.exceptions, ...log.lines.map((line) => line.text)].forEach((text) => seen.add(text));
    const body = [...exceptions, ...(lines.length ? [`실행 로그의 오류·경고 줄:\n${lines.join("\n")}`] : [])];
    if (body.length) blocks.push(head + body.join("\n\n"));
    else if (log.exceptions.length) blocks.push(`${head}앞 시도와 같은 오류로 실패`);
  }
  if (item.events.length) {
    blocks.push(`Airflow 기록:\n${item.events.map((line) => `${line.time} ${line.text}`).join("\n")}`);
  }
  return blocks.join("\n\n");
}

function TaskLogView({ item, dagId, runId, nameOf }: {
  item: LoadedTaskLog;
  dagId: string;
  runId: string;
  nameOf: (dagId: string) => string;
}) {
  const single = item.attempts.length <= 1;
  const hasReason = item.attempts.some((attempt) => (attempt.log?.exceptions.length ?? 0) > 0) || item.events.length > 0;
  const gptEnabled = useGptEnabled();
  const [analyzing, setAnalyzing] = useState(false);
  return (
    <section className="failure-log-task">
      <div className="failure-log-task-head">
        <strong>{taskLabel(item.taskId)}</strong>
        {!single && <Tag>{item.attempts.length}회 시도</Tag>}
        {gptEnabled && hasReason && !analyzing && (
          <Button size="small" className="gpt-analysis-open" onClick={() => setAnalyzing(true)}>GPT 해석</Button>
        )}
      </div>
      {analyzing && (
        <GptErrorAnalysis
          targetType="WORKFLOW_TASK"
          target={{ dagId, runId, taskId: item.taskId }}
          errorText={screenErrorText(item)}
          onClose={() => setAnalyzing(false)}
        />
      )}
      {item.attempts.map((attempt) => <AttemptView key={attempt.tryNumber} attempt={attempt} single={single} />)}
      {item.events.length > 0 && (
        <div className="failure-log-block">
          <div className="failure-log-label">Airflow 기록<span> · 스케줄러·실행기가 남긴 실패 사유</span></div>
          <LogLines lines={item.events} />
        </div>
      )}
      {!hasReason && !item.child && (
        <div className="failure-log-note">작업 로그와 Airflow 기록에 실패 원인(오류 메시지)이 남아 있지 않습니다.</div>
      )}
      {item.eventsError && <div className="failure-log-empty">Airflow 기록을 불러오지 못했습니다: {item.eventsError}</div>}
      {item.child && (
        <div className="failure-log-child">
          <div className="failure-log-label">하위 워크플로우 «{nameOf(item.child.dagId)}» 에서 실패한 작업</div>
          {item.child.error ? (
            <div className="failure-log-empty">{item.child.error}</div>
          ) : item.child.tasks.length > 0 ? (
            item.child.tasks.map((child) => (
              <TaskLogView key={child.taskId} item={child} dagId={item.child!.dagId} runId={item.child!.runId} nameOf={nameOf} />
            ))
          ) : (
            <div className="failure-log-empty">하위 실행에 실패로 끝난 작업이 없습니다.</div>
          )}
        </div>
      )}
    </section>
  );
}

/**
 * 워크플로우 task 실패 로그 창(2026-09-11 요청). 표의 «적재/실패» 칸에서 실패한 줄의 «로그» 버튼으로 연다.
 * 시도마다 실패 원인 예외를 먼저, 그 아래 걸러 낸 실행 로그를 원문 그대로 보여 준다(utils/airflowFailureLog).
 * 작업 로그에 원인이 없고 Airflow 기록에만 남은 실패(state mismatch 등)는 «Airflow 기록»으로 함께 보여 준다.
 */
export function TaskFailureLogModal({ target, onClose }: { target?: FailureLogTarget; onClose: () => void }) {
  // 하위 워크플로우 이름을 DAG id 대신 보여 주기 위한 목록. 로그 화면이 이미 불러 둔 캐시를 쓴다.
  const workflowsQuery = useQuery({ queryKey: ["workflows"], queryFn: listWorkflows });
  const nameOf = (dagId: string) => (workflowsQuery.data ?? []).find((workflow) => workflow.dagId === dagId)?.name ?? dagId;
  const query = useQuery({
    queryKey: ["airflow-failure-log", target?.dagId, target?.runId,
      (target?.tasks ?? []).map((task) => `${task.task_id}:${task.try_number}`).join(",")],
    queryFn: () => loadTaskLogs(target!.dagId, target!.runId, target!.tasks, 0),
    enabled: Boolean(target),
    staleTime: 60_000,
  });
  return (
    <Modal open={Boolean(target)} title={target ? `${target.title} 실패 로그` : undefined} onCancel={onClose}
           footer={null} width={960} destroyOnHidden rootClassName="cerebro-modal">
      {target?.runLabel && <div className="failure-log-run">{target.runLabel} 실행</div>}
      {query.isLoading ? (
        <div className="failure-log-empty"><Spin size="small" /> 로그를 불러오는 중입니다.</div>
      ) : query.error ? (
        <Alert type="error" showIcon message={`로그를 불러오지 못했습니다: ${errorText(query.error)}`} />
      ) : (
        (query.data ?? []).map((item) => (
          <TaskLogView key={item.taskId} item={item} dagId={target!.dagId} runId={target!.runId} nameOf={nameOf} />
        ))
      )}
    </Modal>
  );
}

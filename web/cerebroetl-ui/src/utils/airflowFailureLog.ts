import dayjs from "dayjs";
import type { AirflowEventLog, AirflowLogEntry } from "../api/platform";

/**
 * Airflow task 로그에서 «왜 실패했는지»만 남긴다 - 워크플로우 로그 화면의 실패 로그 창(2026-09-11 요청).
 *
 * <p>Airflow 3 task 로그는 한 줄마다 구조화된 항목(level · logger · event · error_detail …)이다. 실패 원인은
 * 거의 늘 «Task failed with exception» · «Top level error» 항목의 error_detail(예외 종류·메시지)에 있고,
 * 그 앞에 우리 DAG 코드가 찍은 진행 상황과 NiFi 오류(bulletin) 줄이 있다. 나머지는 원인과 무관한 잡음이다 -
 * DAG 파일을 읽을 때의 안내(Pre Execute 구간), 파이썬 경고(py.warnings), 그룹 표시(::group::) 같은 것.
 *
 * <p>줄을 지어내거나 번역하지 않는다. 원문에서 고르기만 한다 - 실패 원인은 원문 그대로여야 믿을 수 있다.
 *
 * <p>예전 표의 «실패 사유» 칸은 로그 한 줄을 180자로 잘라 보여 줘서 원인이 잘리거나 엉뚱한 줄을 집었다.
 */

export interface FailureLogLine {
  time: string;
  level: string;
  text: string;
}

export interface FailureLog {
  /** 실패 원인 예외(«종류: 메시지»). 연쇄된 예외면 가장 안쪽 원인이 먼저 온다. */
  exceptions: string[];
  /** 걸러 낸 실행 로그 줄(원문 순서). */
  lines: FailureLogLine[];
  /** 대기 단계가 주기적으로 찍는 진행 상황 줄 중 앞쪽이라 빼 둔 줄 수. */
  omittedProgress: number;
  /** 하위 워크플로우를 부른 task 면 그 DAG id(TriggerDagRunOperator 로그의 trigger_dag_id). */
  triggerDagId?: string;
  /**
   * 하위 워크플로우 실행이 실제로 만들어졌는지(«Dag Run triggered successfully» 이후).
   * 만들기 전에 실패했으면(예: 하위 DAG 없음 404) 따라갈 하위 실행이 없다.
   */
  childTriggered: boolean;
}

/** 반복 줄은 끝에서 이만큼만 남긴다 - 실패 직전 상태를 보는 데는 이 정도면 충분하다. */
export const KEPT_PROGRESS_LINES = 6;

const NOISE: RegExp[] = [
  /^::(?:end)?group::/,
  // 로그 맨 앞의 원본 파일 경로(/opt/airflow/logs/dag_id=…/attempt=1.log) - 내용이 아니다.
  /^\/opt\/airflow\/logs\/.+\.log$/,
  /^DAG bundles loaded/,
  /^Filling up the DagBag/,
  // DAG 파일을 읽을 때마다 찍힌다 - 이 task 의 실행과 무관하다.
  /상위 워크플로우가 있어 자체 스케줄/,
  // 하위 워크플로우를 기다리는 30초 간격 줄. 시작·끝 줄만 있으면 된다.
  /^Waiting for dag run to complete/,
];

/**
 * 주기적으로 반복되는 줄. 끝의 몇 줄만 남긴다.
 * - NiFi 그룹 대기(await) 단계: «[JOB] queued=3 activeThreads=1 …», «[JOB]   > 05_INSERT (…) 스레드=1»
 * - 센서(reschedule) 대기: «Poking callable: …», «Rescheduling task, marking task as UP_FOR_RESCHEDULE»
 *   (실측 2026-09-11: CDC 제어 DAG 의 monitor_cdc_runtime 이 30초마다 수백 줄을 남겨 원인이 묻혔다)
 */
const PROGRESS: RegExp[] = [
  /queued=\d+ activeThreads=\d+/,
  /^\[[^\]]+\]\s+>\s.*스레드=\d+/,
  /^Poking callable:/,
  /^Rescheduling task, marking task as UP_FOR_RESCHEDULE/,
];

/** 줄 뒤에 붙여 보여 줄 부가 필드. 하위 워크플로우 호출이 어느 DAG 를 불렀고 어떻게 끝났는지가 여기 있다. */
const SHOWN_FIELDS = ["trigger_dag_id", "state", "exit_code"];

type ErrorDetail = NonNullable<AirflowLogEntry["error_detail"]>[number];

function exceptionText(detail: ErrorDetail): string {
  const head = [detail.exc_type, detail.exc_value].filter(Boolean).join(": ");
  // 스택 전체는 잡음이지만, 우리 DAG 코드(/opt/airflow/dags) 안에서 났으면 그 위치가 원인을 짚는다.
  const own = (detail.frames ?? []).filter((frame) => frame.filename?.includes("/opt/airflow/dags/")).at(-1);
  return own
    ? `${head}\n(위치: ${own.filename?.split("/").at(-1)}:${own.lineno ?? "?"} ${own.name ?? ""})`
    : head;
}

export function parseFailureLog(entries: AirflowLogEntry[]): FailureLog {
  const exceptions: Array<{ text: string; hasMessage: boolean }> = [];
  const candidates: Array<FailureLogLine & { progress: boolean }> = [];
  let triggerDagId: string | undefined;
  let childTriggered = false;
  let inPreExecute = false;

  for (const entry of entries) {
    const event = String(entry.event ?? "").trim();
    const level = String(entry.level ?? "info").toLowerCase();
    if (typeof entry.trigger_dag_id === "string" && entry.trigger_dag_id) {
      triggerDagId = entry.trigger_dag_id;
    }
    if (/^Dag Run triggered successfully|^DagRun finished with/.test(event)) {
      childTriggered = true;
    }
    if (event === "::group::Pre Execute") {
      inPreExecute = true;
      continue;
    }
    if (inPreExecute && event === "::endgroup::") {
      inPreExecute = false;
      continue;
    }
    if (entry.error_detail?.length) {
      // 머리 줄(«Task failed with exception» 등)은 예외가 대신 말한다.
      [...entry.error_detail].reverse().forEach((detail) => {
        const text = exceptionText(detail);
        if (text && !exceptions.some((known) => known.text === text)) {
          exceptions.push({ text, hasMessage: Boolean(String(detail.exc_value ?? "").trim()) });
        }
      });
      continue;
    }
    const serious = level === "error" || level === "critical";
    if (!event || (inPreExecute && !serious) || entry.logger === "py.warnings") {
      continue;
    }
    if (NOISE.some((pattern) => pattern.test(event))) {
      continue;
    }
    const fields = SHOWN_FIELDS
      .filter((key) => entry[key] !== undefined && entry[key] !== null && entry[key] !== "")
      .map((key) => `${key}=${String(entry[key])}`);
    candidates.push({
      time: entry.timestamp ? dayjs(entry.timestamp).format("HH:mm:ss") : "",
      level,
      text: fields.length ? `${event} (${fields.join(", ")})` : event,
      progress: PROGRESS.some((pattern) => pattern.test(event)),
    });
  }

  const progressTotal = candidates.filter((line) => line.progress).length;
  const omittedProgress = Math.max(0, progressTotal - KEPT_PROGRESS_LINES);
  let skip = omittedProgress;
  const lines: FailureLogLine[] = [];
  for (const line of candidates) {
    if (line.progress && skip > 0) {
      skip -= 1;
      continue;
    }
    lines.push({ time: line.time, level: line.level, text: line.text });
  }
  // 메시지 없는 예외(DagRunTriggerException 같은 Airflow 내부 흐름 제어용)는 이름만으로 원인을 말하지 않는다.
  // 메시지가 있는 예외가 하나라도 있으면 뺀다(실측 2026-09-11: total 09-09 실행에서 원인 목록 맨 위에 섞였다).
  const meaningful = exceptions.filter((exception) => exception.hasMessage);
  return {
    exceptions: (meaningful.length ? meaningful : exceptions).map((exception) => exception.text),
    lines,
    omittedProgress,
    triggerDagId,
    childTriggered,
  };
}

/** 실행·재예약 같은 일상 기록. 사유 문구가 없으면 뺀다. */
const ROUTINE_EVENTS = /^(running|queued|scheduled|up_for_reschedule|up_for_retry|success|failed|skipped|cli_task_run|trigger|clear|restarting|deferred)$/i;
/** 실패 사유를 말하는 기록. */
const REASON_EVENTS = /mismatch|heartbeat|zombie|kill|timeout|timed out|error|exception|external|sigterm|oom/i;

/**
 * Airflow 기록(event log) 중 실패 사유를 말하는 것만 시간 순으로 돌려준다.
 * 이름이 사유를 말하거나(state mismatch, heartbeat …), 호스트 이름 말고 다른 설명(extra)이 붙은 기록만 남긴다.
 */
export function failureEvents(events: AirflowEventLog[]): FailureLogLine[] {
  return events
    .filter((event) => {
      const name = String(event.event ?? "");
      const extra = String(event.extra ?? "").trim();
      if (REASON_EVENTS.test(name) || REASON_EVENTS.test(extra)) {
        return true;
      }
      const hostOnly = /^\{\s*"host_name"\s*:\s*"[^"]*"\s*\}$/.test(extra);
      return !ROUTINE_EVENTS.test(name) && Boolean(extra) && !hostOnly;
    })
    .reverse()
    .map((event) => ({
      time: event.when ? dayjs(event.when).format("HH:mm:ss") : "",
      level: "error",
      text: `${event.event ?? ""}${event.extra ? `: ${event.extra}` : ""}`,
    }));
}

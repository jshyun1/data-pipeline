import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, message, Modal, Radio, Spin } from "antd";
import { Link } from "react-router-dom";
import dayjs from "dayjs";
import {
  getAirflowTaskLogEntries,
  listAirflowTaskEventLogs,
  triggerAirflowDag,
  type AirflowDagRun,
  type AirflowTaskInstance,
} from "../../api/platform";
import { getAlertRulesWatching } from "../../api/config";
import { failureEvents, parseFailureLog } from "../../utils/airflowFailureLog";
import type { FailureLogTarget } from "../../components/TaskFailureLogModal";
import {
  categoryOf,
  displayName,
  jobKeyOf,
  stateLabel,
  stateTone,
  stepName,
  type DashboardDag,
} from "./logV2Data";

export function StateBadge({ state }: { state?: string }) {
  return <span className={`lv2-badge lv2-badge--${stateTone(state)}`}>{stateLabel(state)}</span>;
}

export function Tile({ label, value, bad = false }: { label: string; value: React.ReactNode; bad?: boolean }) {
  return (
    <div className={`lv2-tile${bad ? " is-bad" : ""}`}>
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

/** 이 대상을 감시하는 알림 규칙. 기존 화면 WatchingRules 와 같은 조회·항목이다. */
export function WatchingRules({ target, ids }: { target: "CDC" | "ETL"; ids: Array<number | string> }) {
  const query = useQuery({
    queryKey: ["alert-rules-watching", target, ids.join(",")],
    queryFn: () => getAlertRulesWatching(target, ids),
    enabled: ids.length > 0,
  });
  const rules = query.data ?? [];
  return (
    <div>
      {query.isLoading ? <Spin size="small" /> : rules.length === 0 ? (
        <p className="lv2-muted">이 작업을 감시 중인 규칙이 없습니다.</p>
      ) : (
        <div className="lv2-rules">
          {rules.map((rule) => (
            <article key={rule.id} className="lv2-rule">
              <div className="lv2-rule-title"><span className="lv2-type">{rule.type_label}</span><b>{rule.name}</b></div>
              <small>
                심각도 {rule.severity}
                {rule.schedule_enabled && rule.schedule_time ? ` · 매일 ${rule.schedule_time.slice(0, 5)}` : ""}
              </small>
              {rule.last_eval_error && <small className="lv2-error-text">{rule.last_eval_error}</small>}
            </article>
          ))}
        </div>
      )}
      <div className="lv2-links"><Link className="lv2-btn sm" to="/settings?tab=rules">알림 규칙 관리 바로가기 ↗</Link></div>
    </div>
  );
}

type ExecutionAction = "deploy" | "start" | "stop" | "monitor";

/** 실행 설정 창. 기존 화면 ExecutionSettingsModal 과 같은 동작·잠금 규칙이다. */
export function ExecutionSettingsModal({
  open,
  dag,
  runs,
  onClose,
  onExecuted,
}: {
  open: boolean;
  dag?: DashboardDag;
  runs: AirflowDagRun[];
  onClose: () => void;
  onExecuted: () => void;
}) {
  const [action, setAction] = useState<ExecutionAction | undefined>("start");
  const [triggering, setTriggering] = useState(false);
  // 감시 센서까지 가는 동작(start/monitor)의 실행이 살아 있으면 이미 감시 중이다.
  const watching = runs.some((run) => run.state === "running"
    && (run.conf?.action === "monitor" || run.conf?.action === "start"));

  useEffect(() => {
    if (open) setAction(watching ? undefined : "start");
  }, [dag?.dag_id, open, watching]);

  const options = dag && categoryOf(dag) === "CDC"
    ? [
        { label: "배포 (Deploy)", value: "deploy" },
        { label: "시작 (Start)", value: "start", disabled: watching },
        { label: "중지 (Stop)", value: "stop" },
        { label: "감시 재개", value: "monitor", disabled: watching },
      ]
    : [
        { label: "시작 (Start)", value: "start" },
        { label: "중지 (Stop)", value: "stop" },
      ];

  const execute = async () => {
    if (!dag || !action) return;
    if ((action === "monitor" || action === "start") && watching) {
      message.warning("이미 감시 중인 실행이 있습니다. 중지 후 다시 시작해주세요.");
      return;
    }
    setTriggering(true);
    try {
      await triggerAirflowDag(dag.dag_id, { action });
      message.success(`${displayName(dag)} ${options.find((option) => option.value === action)?.label} 요청을 전송했습니다.`);
      onClose();
      onExecuted();
    } catch (error) {
      message.error(error instanceof Error ? error.message : "DAG 실행 요청에 실패했습니다.");
    } finally {
      setTriggering(false);
    }
  };

  return (
    <Modal
      title={`${dag ? displayName(dag) : "DAG"} 실행 설정`}
      open={open}
      onCancel={onClose}
      footer={<>
        <Button type="primary" loading={triggering} disabled={!action} onClick={() => void execute()}>실행</Button>
        <Button disabled={triggering} onClick={onClose}>취소</Button>
      </>}
      destroyOnHidden
      rootClassName="cerebro-modal"
    >
      <p>Airflow DAG에 전달할 실행 동작을 선택하세요.</p>
      {watching && (
        <p style={{ color: "#888", fontSize: 12 }}>
          이미 감시 중인 실행이 있어 «시작»과 «감시 재개»는 선택할 수 없습니다.
          다시 켜려면 «중지» 후 «시작»을 해주세요.
        </p>
      )}
      <Radio.Group
        block
        optionType="button"
        buttonStyle="solid"
        value={action}
        options={options}
        onChange={(event) => setAction(event.target.value as ExecutionAction)}
      />
    </Modal>
  );
}

/**
 * 상세 창 «로그» 탭. 실패한 단계를 왼쪽에 늘어놓고, 고른 단계의 실패 원인(예외)과 걸러 낸 실행 로그를 오른쪽에 바로 보여 준다.
 *
 * <p>줄을 고르는 규칙은 실패 로그 창(TaskFailureLogModal)과 같은 parseFailureLog 다. 이전 시도·Airflow 기록·하위 워크플로우
 * 안쪽 로그까지 따라가 보려면 «전체 로그 창»으로 연다 - 창은 기존 로그 화면과 같은 컴포넌트다.
 */
export function InlineFailureLog({
  title,
  dagId,
  runId,
  tasks,
  onOpenFull,
}: {
  title: string;
  dagId: string;
  runId: string;
  tasks: AirflowTaskInstance[];
  onOpenFull: (target: FailureLogTarget) => void;
}) {
  const [activeId, setActiveId] = useState<string>();
  const active = tasks.find((task) => task.task_id === activeId) ?? tasks[0];
  const query = useQuery({
    queryKey: ["logv2-failure-entries", dagId, runId, active?.task_id, active?.try_number],
    queryFn: async () => parseFailureLog(await getAirflowTaskLogEntries(dagId, runId, active!.task_id, active!.try_number || 1)),
    enabled: Boolean(active),
    staleTime: 60_000,
  });
  // 작업 로그에 원인이 없고 Airflow 기록에만 남는 실패(state mismatch 등)가 있다 - 실패 로그 창과 같이 함께 보여 준다.
  const eventsQuery = useQuery({
    queryKey: ["logv2-failure-events", dagId, runId, active?.task_id],
    queryFn: async () => failureEvents(await listAirflowTaskEventLogs(dagId, runId, active!.task_id)),
    enabled: Boolean(active),
    staleTime: 60_000,
  });
  if (!tasks.length) {
    return <div className="lv2-empty">실패한 단계가 없습니다.</div>;
  }
  const parsed = query.data;
  const events = eventsQuery.data ?? [];
  const text = [
    ...(parsed ? [...parsed.exceptions, ...parsed.lines.map((line) => `[${line.time}] ${line.level.toUpperCase()} - ${line.text}`)] : []),
    ...(events.length ? ["", "[Airflow 기록]", ...events.map((line) => `[${line.time}] ${line.text}`)] : []),
  ].join("\n");
  return (
    <div className="lv2-log">
      <div className="lv2-log-list">
        {tasks.map((task) => (
          <button key={task.task_id} type="button"
                  className={task.task_id === active?.task_id ? "on" : ""}
                  onClick={() => setActiveId(task.task_id)}>
            {jobKeyOf(task.task_id) !== task.task_id && <span className="lv2-muted">{jobKeyOf(task.task_id)} · </span>}
            {stepName(task.task_id)}
            <small>시도 {task.try_number ?? 1}{task.end_date ? ` · ${dayjs(task.end_date).format("HH:mm:ss")}` : ""}</small>
          </button>
        ))}
      </div>
      <div className="lv2-console">
        <div className="lv2-console-tools">
          <button type="button" disabled={!text}
                  onClick={() => void navigator.clipboard?.writeText(text).then(() => message.success("로그를 복사했습니다."))}>
            복사
          </button>
          <button type="button" onClick={() => onOpenFull({ title, dagId, runId, tasks })}>전체 로그 창</button>
        </div>
        {query.isLoading ? (
          <span className="lv2-console-note">로그를 불러오는 중입니다.</span>
        ) : query.error ? (
          <span className="lv2-console-hl">로그를 불러오지 못했습니다: {query.error instanceof Error ? query.error.message : String(query.error)}</span>
        ) : parsed && (
          <>
            {parsed.exceptions.map((exception, index) => (
              <div key={`e${index}`} className="lv2-console-hl">{exception}</div>
            ))}
            {parsed.exceptions.length > 0 && parsed.lines.length > 0 && <div className="lv2-console-gap" />}
            {parsed.omittedProgress > 0 && (
              <div className="lv2-console-note">… 반복 진행 줄 {parsed.omittedProgress}줄 생략</div>
            )}
            {parsed.lines.map((line, index) => (
              <div key={`l${index}`}
                   className={/error|critical/i.test(line.level) ? "lv2-console-hl" : /warn/i.test(line.level) ? "lv2-console-warn" : undefined}>
                <span className="lv2-console-time">{line.time}</span> {line.level ? `${line.level.toUpperCase()} - ` : ""}{line.text}
              </div>
            ))}
            {!parsed.exceptions.length && !parsed.lines.length && !events.length && !eventsQuery.isLoading && (
              <span className="lv2-console-note">작업 로그에 원인이 남지 않았습니다. «전체 로그 창»에서 Airflow 기록을 확인하세요.</span>
            )}
            {events.length > 0 && (
              <>
                <div className="lv2-console-gap" />
                <div className="lv2-console-note">Airflow 기록 · 스케줄러·실행기가 남긴 실패 사유</div>
                {events.map((line, index) => (
                  <div key={`ev${index}`} className="lv2-console-hl"><span className="lv2-console-time">{line.time}</span> {line.text}</div>
                ))}
              </>
            )}
            {parsed.triggerDagId && (
              <div className="lv2-console-note">
                하위 워크플로우({parsed.triggerDagId})를 부른 단계입니다. 하위 실행 안의 실패 원인은 «전체 로그 창»에서 이어서 볼 수 있습니다.
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Button, Spin, Tag, message } from "antd";
import { Link } from "react-router-dom";
import dayjs from "dayjs";
import {
  listAirflowDagRuns,
  listAirflowTaskInstances,
  type AirflowDagRun,
} from "../api/platform";
import type { WorkflowDetail } from "../api/workflows";
import { publishedParentsOf } from "../utils/workflowTree";
import { TaskFailureLogModal, type FailureLogTarget } from "./TaskFailureLogModal";

/**
 * 워크플로우 속성창 공통 구역 — 설계 대상 워크플로우 속성창과 캔버스의 «워크플로우 노드» 속성창이 같이 쓴다(2026-09-17 요청).
 * 스케줄 실제 동작 · 게시 상태 · 구성 요약 · 최근 실행. 모두 읽기 전용이다.
 */

/**
 * 게시된 상위가 있을 때의 스케줄 안내(자체 스케줄은 꺼지고 상위가 부를 때만 돈다).
 * 예전에는 꺼진 자체 스케줄과 «다음 2회 실행»을 그대로 보여 줘서 그 시각에 도는 것처럼 읽혔다.
 */
export function ParentScheduleNotice({ detail }: { detail: WorkflowDetail }) {
  if (publishedParentsOf(detail).length === 0) return null;
  return (
    // 자체 스케줄 · 최상위 기준 다음 실행 줄은 뺐다(2026-09-18 요청) - 한 줄만 남긴다.
    <div className="wf-info-notice">
      <div className="wf-info-notice-title">상위 워크플로우가 부를 때만 실행됩니다.</div>
    </div>
  );
}

/** 게시 상태. 미게시 하위 워크플로우는 상위가 실행되면 그 노드에서 실패하므로 노드 속성창에서는 경고한다. */
export function WorkflowPublishSection({ detail, asNode = false }: { detail: WorkflowDetail; asNode?: boolean }) {
  return (
    <section className="nifi-detail-section">
      <h3>게시 상태</h3>
      {detail.published ? (
        <>
          <div className="wf-node-detail-value">
            <Tag color="success">게시됨</Tag>
            {detail.dirty && <Tag color="warning">게시본과 다름</Tag>}
          </div>
          <div className="wf-node-detail-sub">
            {detail.publishedAt ? dayjs(detail.publishedAt).format("YYYY-MM-DD HH:mm") : "-"}
            {detail.publishedBy ? ` · ${detail.publishedBy}` : ""}
          </div>
          {detail.dirty && (
            <div className="wf-node-detail-sub">게시 뒤 캔버스를 고쳤습니다. 다시 게시해야 실행에 반영됩니다.</div>
          )}
        </>
      ) : (
        <>
          <div className="wf-node-detail-value"><Tag>미게시 (DAG 없음)</Tag></div>
          <div className={`wf-node-detail-sub${asNode ? " wf-info-warn" : ""}`}>
            {asNode
              ? "게시 전이라 DAG 가 없습니다. 이 상태로 상위 워크플로우가 실행되면 이 노드에서 실패합니다."
              : "게시해야 스케줄 · 수동 실행이 됩니다."}
          </div>
        </>
      )}
    </section>
  );
}

const MAX_LISTED = 6;

/** 구성 요약: 노드 종류별 개수와 이름. 캔버스를 열지 않아도 무엇이 도는지 보인다. */
export function WorkflowCompositionSection({ detail }: { detail: WorkflowDetail }) {
  const nodes = detail.nodes ?? [];
  const jobs = nodes.filter((node) => node.nodeType === "JOB");
  const subs = nodes.filter((node) => node.nodeType === "SUBWF");
  const cdcs = nodes.filter((node) => node.nodeType === "CDC");
  const missing = jobs.filter((node) => node.jobMissing).length;
  const listed = (names: string[]) => (
    <div className="wf-node-detail-sub">
      {names.slice(0, MAX_LISTED).join(", ")}
      {names.length > MAX_LISTED ? ` 외 ${names.length - MAX_LISTED}개` : ""}
    </div>
  );
  const counts = [
    jobs.length ? `job ${jobs.length}` : null,
    subs.length ? `하위 워크플로우 ${subs.length}` : null,
    cdcs.length ? `CDC ${cdcs.length}` : null,
  ].filter(Boolean);
  return (
    <section className="nifi-detail-section">
      <h3>구성 요약</h3>
      <div className="wf-node-detail-value">{counts.length ? counts.join(" · ") : "노드 없음"}</div>
      {jobs.length > 0 && listed(jobs.map((node) => node.jobName ?? node.nodeKey))}
      {subs.length > 0 && listed(subs.map((node) => node.subWorkflowName ?? node.nodeKey))}
      {cdcs.length > 0 && listed(cdcs.map((node) => node.pipelineName ?? node.nodeKey))}
      {missing > 0 && <div className="wf-node-detail-sub wf-info-warn">참조하던 job 이 사라진 노드 {missing}개</div>}
    </section>
  );
}

function runStateTag(state?: string) {
  if (state === "success") return <Tag color="success">성공</Tag>;
  if (state === "failed") return <Tag color="error">실패</Tag>;
  if (state === "running") return <Tag color="processing">실행 중</Tag>;
  if (state === "queued" || state === "scheduled") return <Tag color="warning">대기</Tag>;
  return <Tag>{state ?? "-"}</Tag>;
}

function durationText(start?: string, end?: string) {
  if (!start || !end) return "-";
  const seconds = Math.max(0, dayjs(end).diff(dayjs(start), "second"));
  const minutes = Math.floor(seconds / 60);
  return minutes >= 60
    ? `${Math.floor(minutes / 60)}시간 ${minutes % 60}분`
    : minutes > 0 ? `${minutes}분 ${seconds % 60}초` : `${seconds}초`;
}

function runTypeText(run: AirflowDagRun) {
  if (run.run_type === "scheduled") return "스케줄";
  // 상위가 부른 실행은 manual__{상위 run id}__{노드 키} 모양이다(DAG 생성기 규칙).
  if (run.dag_run_id.startsWith("manual__") && run.dag_run_id.lastIndexOf("__") > "manual__".length) return "상위 호출";
  return "수동";
}

const RECENT_LIMIT = 2;

/**
 * 최근 실행(최대 2회, 2026-09-18 요청). 실패한 실행은 «로그»로 실패 로그 창(로그 화면과 같은 창)을 연다.
 *
 * <p>{@code calledBy} 가 있으면(캔버스의 워크플로우 노드) 그 상위 워크플로우가 <b>이 노드로</b> 부른 실행만 보여 준다.
 * 하위 실행 id 는 DAG 생성기 규칙 {@code manual__{상위 run id}__{노드 키}} 라 상위 실행 목록과 맞춰 정확히 골라낸다.
 */
export function WorkflowRecentRuns({ detail, calledBy }: {
  detail: WorkflowDetail;
  calledBy?: { dagId: string; published: boolean; nodeKey: string };
}) {
  const [failureLog, setFailureLog] = useState<FailureLogTarget>();
  const [opening, setOpening] = useState<string>();
  const enabled = detail.published && Boolean(detail.dagId) && (!calledBy || (calledBy.published && Boolean(calledBy.dagId)));
  const runsQuery = useQuery({
    queryKey: ["wf-panel-recent-runs", detail.dagId, calledBy?.dagId, calledBy?.nodeKey],
    enabled,
    staleTime: 15_000,
    queryFn: async () => {
      if (!calledBy) {
        return listAirflowDagRuns(detail.dagId, { limit: RECENT_LIMIT });
      }
      const [parentRuns, childRuns] = await Promise.all([
        listAirflowDagRuns(calledBy.dagId, { limit: 50 }),
        listAirflowDagRuns(detail.dagId, { limit: 100 }),
      ]);
      const calledIds = new Set(parentRuns.map((run) => `manual__${run.dag_run_id}__${calledBy.nodeKey}`));
      return childRuns.filter((run) => calledIds.has(run.dag_run_id)).slice(0, RECENT_LIMIT);
    },
  });

  const openLog = async (run: AirflowDagRun) => {
    setOpening(run.dag_run_id);
    try {
      const failed = (await listAirflowTaskInstances(detail.dagId, run.dag_run_id)).filter((task) => task.state === "failed");
      if (failed.length === 0) {
        message.info("실패로 끝난 단계가 없습니다(선행 실패로 건너뛴 단계만 있을 수 있습니다).");
        return;
      }
      setFailureLog({ title: `${detail.name} · ${dayjs(run.start_date ?? run.execution_date).format("MM-DD HH:mm")}`, dagId: detail.dagId, runId: run.dag_run_id, tasks: failed });
    } catch (error) {
      message.error(error instanceof Error ? error.message : "실패 단계를 불러오지 못했습니다.");
    } finally {
      setOpening(undefined);
    }
  };

  let body: React.ReactNode;
  if (!detail.published) {
    body = <div className="wf-node-detail-sub">게시 전이라 실행 이력이 없습니다.</div>;
  } else if (calledBy && !calledBy.published) {
    body = <div className="wf-node-detail-sub">이 캔버스의 워크플로우가 게시 전이라 이 노드로 부른 이력이 없습니다.</div>;
  } else if (runsQuery.isLoading) {
    body = <Spin size="small" />;
  } else if (runsQuery.isError) {
    body = <div className="wf-node-detail-sub">실행 이력을 불러오지 못했습니다(워크플로우 로그 조회 권한이 필요합니다).</div>;
  } else if (!runsQuery.data?.length) {
    body = <div className="wf-node-detail-sub">{calledBy ? "이 노드로 부른 실행이 아직 없습니다." : "실행 이력이 없습니다."}</div>;
  } else {
    body = (
      <ul className="wf-recent-runs">
        {runsQuery.data.map((run) => {
          const started = run.start_date ?? run.execution_date;
          return (
            <li key={run.dag_run_id}>
              {runStateTag(run.state)}
              <span className="wf-recent-runs-time">{started ? dayjs(started).format("MM-DD HH:mm") : "-"}</span>
              <span className="wf-recent-runs-sub">{durationText(run.start_date, run.end_date)} · {runTypeText(run)}</span>
              {run.state === "failed" && (
                <Button size="small" danger loading={opening === run.dag_run_id} onClick={() => void openLog(run)}>로그</Button>
              )}
            </li>
          );
        })}
      </ul>
    );
  }

  return (
    <section className="nifi-detail-section">
      <h3>최근 실행</h3>
      {body}
      {detail.published && (
        <div className="wf-node-detail-sub">
          <Link to={`/airflow/dashboard?dagId=${encodeURIComponent(detail.dagId)}`}>로그 화면에서 보기</Link>
        </div>
      )}
      <TaskFailureLogModal target={failureLog} onClose={() => setFailureLog(undefined)} />
    </section>
  );
}

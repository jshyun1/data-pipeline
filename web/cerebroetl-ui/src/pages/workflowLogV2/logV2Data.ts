/**
 * 워크플로우 로그2 화면의 데이터 규칙.
 *
 * <p>로그2는 기존 «로그» 화면(AirflowDashboardPage)과 나란히 두고 관리자가 둘 중 하나를 고르기 위한 시안이다
 * (2026-09-16 요청). 채택 전까지 기존 화면을 한 줄도 건드리지 않으려고, 판정 규칙(오늘 기준 지표·상태 문구·
 * 적재 건수 읽기)을 여기로 <b>그대로 옮겨 적었다</b>. 한쪽을 버리면 이 파일(또는 원본)을 지우면 된다 -
 * 채택 전 규칙을 고칠 일이 생기면 두 곳을 같이 고칠 것.
 */
import dayjs, { type Dayjs } from "dayjs";
import {
  listAirflowDagCatalog,
  listAirflowDags,
  listAllAirflowDagRuns,
  type AirflowDag,
  type AirflowDagRun,
  type AirflowTaskInstance,
} from "../../api/platform";
import { listEtlJobs } from "../../api/etlJobs";
import { categorizeDag, type DagCategory } from "../../utils/dagHistory";

export type BusinessCategory = Exclude<DagCategory, "기타">;
export const CATEGORY_ORDER: BusinessCategory[] = ["CDC", "ETL"];

export interface DashboardDag extends AirflowDag {
  dag_display_name?: string;
  description?: string;
  tags?: Array<string | { name?: string }>;
  next_dagrun?: string;
  next_dagrun_run_after?: string;
  timetable_summary?: string;
  business_category?: BusinessCategory;
  business_folder?: string;
  etl_job_id?: number;
  nifi_process_group_id?: string;
  created_at?: string;
}

export interface DashboardData {
  dags: DashboardDag[];
  runs: AirflowDagRun[];
}

export const REFRESH_OPTIONS = [
  { label: "10초", value: 10 },
  { label: "30초", value: 30 },
  { label: "1분", value: 60 },
];

export type StatusMetric = "all" | "running" | "success" | "failed" | "waiting" | "new";

export function runAt(run?: AirflowDagRun) {
  return run?.start_date ?? run?.execution_date;
}

export function latestRunToday(runs: AirflowDagRun[], today: Dayjs = dayjs().startOf("day")) {
  return runs.find((run) => {
    const startedAt = runAt(run);
    return Boolean(startedAt && dayjs(startedAt).isAfter(today));
  });
}

export function durationSeconds(start?: string, end?: string) {
  if (!start || !end) return 0;
  return Math.max(0, dayjs(end).diff(dayjs(start), "second"));
}

export function duration(start?: string, end?: string) {
  if (!start || !end) {
    return "-";
  }
  const seconds = durationSeconds(start, end);
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const rest = seconds % 60;
  return hours > 0
    ? `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(rest).padStart(2, "0")}`
    : `${String(minutes).padStart(2, "0")}:${String(rest).padStart(2, "0")}`;
}

export function stateLabel(state?: string) {
  if (state === "success") return "성공";
  if (state === "failed") return "실패";
  if (state === "running") return "실행 중";
  if (state === "queued" || state === "scheduled") return "대기";
  if (state === "upstream_failed") return "선행 실패";
  if (state === "skipped") return "건너뜀";
  if (state === "up_for_retry") return "재시도 대기";
  return state ?? "실행 없음";
}

/** 배지 색 이름(CSS lv2-badge--*). 기존 화면의 antd Tag 색 분류와 같은 묶음이다. */
export function stateTone(state?: string) {
  if (state === "success") return "success";
  if (state === "failed") return "failed";
  if (state === "upstream_failed") return "upstream";
  if (state === "running") return "running";
  if (state === "queued" || state === "scheduled" || state === "up_for_retry") return "waiting";
  return "none";
}

export function displayName(dag: DashboardDag) {
  return dag.dag_display_name || dag.dag_id;
}

export function categoryOf(dag: DashboardDag): DagCategory {
  return dag.business_category ?? categorizeDag(dag.dag_id);
}

export function folderName(dag: DashboardDag) {
  if (dag.business_folder) return dag.business_folder;
  const tags = (dag.tags ?? []).map((tag) => (typeof tag === "string" ? tag : tag.name ?? ""));
  const folder = tags
    .map((tag) => tag.match(/^(?:folder|business|업무)[:=](.+)$/i)?.[1]?.trim())
    .find(Boolean);
  return folder || "미분류";
}

export function groupRuns(runs: AirflowDagRun[]) {
  const grouped = new Map<string, AirflowDagRun[]>();
  runs.forEach((run) => {
    if (!run.dag_id) return;
    const bucket = grouped.get(run.dag_id) ?? [];
    bucket.push(run);
    grouped.set(run.dag_id, bucket);
  });
  grouped.forEach((bucket) =>
    bucket.sort((a, b) => dayjs(runAt(b) ?? 0).valueOf() - dayjs(runAt(a) ?? 0).valueOf()),
  );
  return grouped;
}

/** 기존 로그 화면의 loadDashboard 와 같은 병합 규칙(카탈로그 + Airflow DAG + ETL job 연결). */
export async function loadDashboard(): Promise<DashboardData> {
  const [airflowDags, catalog, runs, etlJobs] = await Promise.all([
    listAirflowDags(),
    listAirflowDagCatalog(),
    listAllAirflowDagRuns({ maxRuns: 2000 }),
    listEtlJobs().catch(() => []),
  ]);
  const etlJobsByDagId = new Map(etlJobs.flatMap((job) => job.airflowDagId ? [[job.airflowDagId, job] as const] : []));
  const withEtlJob = (dag: DashboardDag): DashboardDag => {
    const job = etlJobsByDagId.get(dag.dag_id);
    return job ? { ...dag, etl_job_id: job.id, nifi_process_group_id: job.nifiPgId } : dag;
  };
  const catalogDagIds = new Set(catalog.map((entry) => entry.dagId));
  const catalogDags = catalog.map((entry): DashboardDag => {
    const airflowDag = airflowDags.find((dag) => dag.dag_id === entry.dagId);
    return {
      ...airflowDag,
      dag_id: entry.dagId,
      dag_display_name: entry.displayName,
      description: airflowDag && "description" in airflowDag
        ? (airflowDag as DashboardDag).description
        : entry.description,
      business_category: entry.businessGroup,
      business_folder: entry.businessFolder,
      created_at: entry.createdAt,
    };
  }).map(withEtlJob);
  const discoveredDags = airflowDags
    .filter((dag) => !catalogDagIds.has(dag.dag_id))
    .map((dag) => ({ ...dag, business_category: categorizeDag(dag.dag_id), business_folder: "ETL" } as DashboardDag))
    .map(withEtlJob);
  return { dags: [...catalogDags, ...discoveredDags], runs };
}

/** 상단 지표 판정 - 기존 화면 matchesMetric 과 같다(«실행 중»만 날짜 무관, 나머지는 오늘 마지막 실행). */
export function matchesMetric(metric: StatusMetric, dag: DashboardDag, runs: AirflowDagRun[], today: Dayjs): boolean {
  if (metric === "all") return true;
  if (metric === "new") return Boolean(dag.created_at && dayjs(dag.created_at).isAfter(today));
  if (metric === "running") return runs.some((run) => run.state === "running");
  const latestToday = latestRunToday(runs, today);
  if (metric === "success") return latestToday?.state === "success";
  if (metric === "failed") return latestToday?.state === "failed";
  return latestToday?.state === "queued" || latestToday?.state === "scheduled";
}

/* ------------------------------------------------------------------ 기간 */

export type PeriodKey = "today" | "yesterday" | "7d" | "30d" | "custom";

export function periodRange(period: PeriodKey, custom?: [Dayjs, Dayjs]): [Dayjs, Dayjs] {
  const today = dayjs().startOf("day");
  if (period === "yesterday") return [today.subtract(1, "day"), today.subtract(1, "day").endOf("day")];
  if (period === "7d") return [today.subtract(6, "day"), dayjs().endOf("day")];
  if (period === "30d") return [today.subtract(29, "day"), dayjs().endOf("day")];
  if (period === "custom" && custom) return [custom[0].startOf("day"), custom[1].endOf("day")];
  return [today, dayjs().endOf("day")];
}

/* ------------------------------------------------------------------ 상위 ↔ 하위 실행 */

/**
 * 하위 워크플로우 실행이면 {상위 run id, 상위 노드 키}.
 *
 * <p>DAG 생성기(etl_workflows_dynamic.py)가 하위 워크플로우 노드를
 * TriggerDagRunOperator(trigger_run_id="manual__{{ dag_run.run_id }}__" + 노드키) 로 만든다. 그래서 하위 실행의
 * run id 만 보면 어느 상위 실행의 어느 노드가 불렀는지 <b>정확히</b> 알 수 있다(시각으로 짐작하지 않는다).
 */
export function parentOfRun(runId: string): { parentRunId: string; nodeKey: string } | undefined {
  if (!runId.startsWith("manual__")) return undefined;
  const rest = runId.slice("manual__".length);
  const cut = rest.lastIndexOf("__");
  if (cut <= 0) return undefined;
  return { parentRunId: rest.slice(0, cut), nodeKey: rest.slice(cut + 2) };
}

export function childRunIdOf(parentRunId: string, nodeKey: string) {
  return `manual__${parentRunId}__${nodeKey}`;
}

export function runTypeLabel(run: AirflowDagRun) {
  if (run.run_type === "scheduled") return "스케줄";
  if (run.run_type === "asset_triggered" || run.run_type === "dataset_triggered") return "선행 완료";
  if (parentOfRun(run.dag_run_id)) return "상위 워크플로우 호출";
  return "수동 실행";
}

/* ------------------------------------------------------------------ task → job */

export type JobKind = "job" | "call" | "marker" | "end";

export interface JobGroup {
  key: string;
  kind: JobKind;
  steps: AirflowTaskInstance[];
  state?: string;
  startedAt?: string;
  endedAt?: string;
  retryCount: number;
}

export function jobKeyOf(taskId: string) {
  const dot = taskId.lastIndexOf(".");
  return dot > 0 ? taskId.slice(0, dot) : taskId;
}

export function stepName(taskId: string) {
  return taskId.split(".").at(-1) ?? taskId;
}

/** 단계 상태를 job 하나의 상태로 접는다. 하나라도 실패면 실패다(기존 화면과 같음). */
export function rollUpState(steps: AirflowTaskInstance[]) {
  const states = steps.map((step) => step.state);
  if (states.includes("failed")) return "failed";
  if (states.includes("running")) return "running";
  if (states.includes("upstream_failed")) return "upstream_failed";
  if (states.length && states.every((state) => state === "success")) return "success";
  return states.find(Boolean) ?? undefined;
}

/**
 * task 목록을 job(TaskGroup) 단위로 묶고 «처음 시작한 시각» 순으로 세운다(기존 화면과 같은 정렬 규칙).
 * 그룹이 `{키}.run` 하나뿐이면 하위 워크플로우 호출, `{키}.marker` 면 표시용 노드, 점이 없으면 워크플로우 종단이다.
 */
export function groupJobs(tasks: AirflowTaskInstance[]): JobGroup[] {
  const order: string[] = [];
  const byJob = new Map<string, AirflowTaskInstance[]>();
  for (const task of tasks) {
    const key = jobKeyOf(task.task_id);
    if (!byJob.has(key)) {
      byJob.set(key, []);
      order.push(key);
    }
    byJob.get(key)!.push(task);
  }
  const firstStartOf = (key: string) => {
    const starts = byJob.get(key)!
      .map((step) => (step.start_date ? new Date(step.start_date).getTime() : Number.NaN))
      .filter((at) => Number.isFinite(at));
    return starts.length ? Math.min(...starts) : undefined;
  };
  const apiOrder = new Map(order.map((key, index) => [key, index] as const));
  const startedAtOf = new Map(order.map((key) => [key, firstStartOf(key)] as const));
  order.sort((a, b) => {
    const left = startedAtOf.get(a);
    const right = startedAtOf.get(b);
    if (left !== undefined && right !== undefined && left !== right) return left - right;
    if (left !== undefined && right === undefined) return -1;
    if (left === undefined && right !== undefined) return 1;
    return apiOrder.get(a)! - apiOrder.get(b)!;
  });
  return order.map((key) => {
    const steps = byJob.get(key)!;
    const only = steps.length === 1 ? steps[0].task_id : undefined;
    const kind: JobKind = !steps.some((step) => step.task_id.includes(".")) ? "end"
      : only?.endsWith(".run") ? "call"
      : only?.endsWith(".marker") ? "marker"
      : "job";
    const starts = steps.map((step) => step.start_date).filter(Boolean) as string[];
    const ends = steps.map((step) => step.end_date).filter(Boolean) as string[];
    return {
      key,
      kind,
      steps,
      state: rollUpState(steps),
      startedAt: starts.sort()[0],
      endedAt: ends.sort().at(-1),
      retryCount: steps.reduce((sum, step) => sum + Math.max(0, (step.try_number ?? 1) - 1), 0),
    };
  });
}

/**
 * 성공한 task 로그에서 적재 건수를 읽는다(기존 화면 outcomeDetail 과 같은 규칙).
 * await 로그의 «이번 실행 적재 건수 (총 99,594행)»의 합계를 먼저 본다 - 뒤따르는 프로세서별 줄도 같은 모양이다.
 */
export function loadCountOf(log: string | undefined): string {
  if (log == null) return "-";
  const total = log.match(/총\s*([\d,]+)\s*행/);
  if (total) return `${total[1]}행`;
  if (/적재 건수: 없음/.test(log)) return "0행";
  const counts = [...log.matchAll(/([\d,]+)\s*(?:행|건)/g)];
  return counts.at(-1) ? `${counts.at(-1)![1]}행` : "-";
}

export function countValue(text: string) {
  const match = text.match(/^([\d,]+)행$/);
  return match ? Number(match[1].replace(/,/g, "")) : undefined;
}

/** job 의 «적재 / 결과» 문구. 실패는 로그 버튼을 그리므로 빈 문자열. */
export function jobOutcome(job: JobGroup, logs?: Record<string, string>) {
  if (job.state === "failed") return "";
  if (job.state === "upstream_failed") return "선행 작업 실패로 미실행";
  if (job.state !== "success" || job.kind !== "job") return "-";
  const counted = job.steps.find((step) => step.task_id.endsWith(".await")) ?? job.steps.at(-1)!;
  return logs ? loadCountOf(logs[counted.task_id]) : "…";
}

export function formatAt(value?: string, pattern = "HH:mm:ss") {
  return value ? dayjs(value).format(pattern) : "-";
}

/* ------------------------------------------------------------------ 화면 선택 */

/** 가운데 목록·상세 창이 가리키는 대상. 실행 → job → 단계 순으로 좁아진다. */
export type Selection =
  | { kind: "run"; dagId: string; runId: string }
  | { kind: "job"; dagId: string; runId: string; jobKey: string }
  | { kind: "step"; dagId: string; runId: string; taskId: string };

export function selectionKey(sel: Selection) {
  if (sel.kind === "run") return `run|${sel.dagId}|${sel.runId}`;
  if (sel.kind === "job") return `job|${sel.dagId}|${sel.runId}|${sel.jobKey}`;
  return `step|${sel.dagId}|${sel.runId}|${sel.taskId}`;
}

export type DetailTab = "summary" | "children" | "log" | "history" | "rules" | "props";

/** 트리·지표에서 고른 목록 범위. */
export type ListScope =
  | { kind: "cdc"; scope: "all" }
  | { kind: "cdc"; scope: "connection"; connectionId: number }
  | { kind: "cdc"; scope: "schema"; connectionId: number; schema: string }
  | { kind: "cdc"; scope: "logfile" }
  | { kind: "cdc"; scope: "pipeline"; pipelineId: number }
  | { kind: "etl-all" }
  | { kind: "dag"; dagId: string }
  | { kind: "metric"; category: BusinessCategory; metric: StatusMetric };

export const STATUS_METRIC_LABEL: Record<StatusMetric, string> = {
  all: "전체 작업",
  running: "실행 중",
  success: "성공",
  failed: "실패",
  waiting: "대기",
  new: "신규",
};

/** 트리 줄 앞 상태 점: 오늘 마지막 실행 기준(돌고 있으면 실행 중이 먼저). */
export function dotOf(runs: AirflowDagRun[] | undefined) {
  if (!runs?.length) return "none";
  if (runs.some((run) => run.state === "running")) return "running";
  const latest = latestRunToday(runs);
  if (!latest) return "none";
  if (latest.state === "success") return "success";
  if (latest.state === "failed") return "failed";
  return "warn";
}

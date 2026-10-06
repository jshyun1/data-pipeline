import type { QueryClient } from "@tanstack/react-query";
import type { AirflowTaskInstance } from "../../api/platform";

/**
 * 실행 중일 때만 빠르게 갱신하기(2026-10-06 요청 - 수동 실행이 15~20초 뒤에야 보였다).
 *
 * <p>Airflow 는 실행을 받으면 1초 안에 시작한다(MSA 실측 0.7초). 늦은 것은 화면의 30초 주기였다. 그래서 대기 · 실행 중인
 * 것이 있을 때만 {@link LIVE_REFRESH_MS} 로 받고 - Airflow 자체 화면의 자동 갱신과 같은 간격 - 다 끝나면 화면에서 고른 주기로
 * 돌아간다. 빠르게 받는 것은 «바뀌는 것»(대기 · 실행 중 실행, 고른 실행의 task, 진행 건수)뿐이다.
 */
export const LIVE_REFRESH_MS = 3_000;

/**
 * 대기 · 실행 중인 실행이 하나도 없을 때 그 목록을 다시 볼 간격. 이 조회는 결과가 비면 100바이트 남짓이라, 쉬는 동안에도
 * 화면 주기(30초) 대신 이 간격으로 본다 - 스케줄 · Airflow · 다른 사람이 건 실행도 5초 안에 나타난다(시험에서 CLI 로 건
 * 실행이 30초 주기를 기다려 18초 뒤에야 보였다).
 */
export const ACTIVE_RUNS_IDLE_MS = 5_000;

/** 대기 · 실행 중인 실행 목록의 캐시 키. 실행을 건 화면이 이 키를 무효화하면 로그 화면이 바로 다시 받는다. */
export const ACTIVE_RUNS_KEY = ["airflow-active-runs"] as const;

const FINISHED_RUN_STATES = new Set(["success", "failed"]);
const FINISHED_TASK_STATES = new Set(["success", "failed", "upstream_failed", "skipped", "removed"]);

/** 실행이 아직 안 끝났는가. 상태를 모르면(목록을 아직 못 받음) 끝나지 않은 것으로 본다. */
export function isLiveRun(state?: string) {
  return !state || !FINISHED_RUN_STATES.has(state);
}

/** 아직 끝나지 않은 task 가 있는가. 상태가 없는 task 는 아직 차례가 안 온 것이다. */
export function hasLiveTasks(tasks?: AirflowTaskInstance[]) {
  return !tasks || tasks.some((task) => !task.state || !FINISHED_TASK_STATES.has(task.state));
}

/** Cerebro 화면에서 워크플로우 실행을 건 직후 부른다 - 로그 화면이 다음 주기를 기다리지 않고 새 실행을 바로 받는다. */
export function refreshRunsAfterTrigger(queryClient: QueryClient) {
  void queryClient.invalidateQueries({ queryKey: ACTIVE_RUNS_KEY });
}

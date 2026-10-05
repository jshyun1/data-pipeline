package com.company.cerebro.nifi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * job 그룹의 «한 번 실행»이 끝났는지 판정한다(NiFi 와 무관한 순수 로직 - 단위 테스트용으로 떼어 냈다).
 *
 * <p>NiFi 에는 «job 실행»이라는 개념이 없다. 그래서 그룹의 상태 변화로 실행 한 번을 잡는다:
 * <ul>
 *   <li><b>활동</b>: 스레드가 돌거나(active) 대기열에 FlowFile 이 있거나(queued), 직전 판정 뒤로 적재 카운터나 처리 횟수가 늘었다.
 *       카운터 · 처리 횟수를 같이 보는 이유: 판정 간격(2초)보다 짧게 끝난 job 은 active · queued 로는 한 번도 안 보인다.</li>
 *   <li><b>완료</b>: 활동을 본 뒤 {@code settleMillis} 동안 계속 조용하다. 그 사이 ERROR 알림(bulletin)이 있었으면 실패.</li>
 * </ul>
 * 처음 보는 그룹은 기준값만 잡는다 - 감시를 켠 순간의 카운터 값을 «방금 늘었다»로 오해하지 않게.
 */
final class CompletionDetector {

    /** 한 그룹의 이번 판정 시점 상태. {@code loadCount} 는 적재 카운터 합(없으면 null), {@code invocations} 는 최근 처리 횟수 합. */
    record Snapshot(int activeThreads, int queued, Long loadCount, long invocations) {
    }

    /**
     * 판정 결과 - 실행 한 번이 끝났다. {@code rows} 는 이번 실행의 적재 건수(카운터가 없으면 null), {@code errors} 는 실행 중 모은
     * ERROR 알림. 실패 여부는 호출하는 쪽이 끝난 순간의 알림까지 다시 훑어 정한다(짧은 job 은 실행 중 판정에 한 번도 안 잡힌다).
     */
    record Completion(String groupId, Long rows, List<String> errors, long startedAtMillis, long endedAtMillis,
                      Long baselineLoad, Long finalLoad) {
    }

    /** 이번 판정에서 «실행 시작»으로 본 그룹과 그 이유(로그용). */
    final List<String> startedThisTick = new ArrayList<>();

    private static final class State {
        boolean inRun;
        long runStartedAt;
        long lastActivityAt;
        Long lastLoad;
        long lastInvocations;
        Long baselineLoad;
        /** 직전 판정 시각. 활동을 이번에 처음 봤으면 실제 시작은 그 사이 어딘가다 - 시작 시각을 이것으로 잡는다. */
        long lastTickAt = -1L;
        final Set<String> errors = new LinkedHashSet<>();
    }

    static final int MAX_ERRORS = 5;

    private final long settleMillis;
    private final Map<String, State> states = new HashMap<>();

    CompletionDetector(long settleMillis) {
        this.settleMillis = settleMillis;
    }

    /** 지금 실행 중으로 보고 있는 그룹인가(이 그룹의 ERROR 알림을 모을지 정할 때 쓴다). */
    boolean inRun(String groupId) {
        State state = states.get(groupId);
        return state != null && state.inRun;
    }

    long runStartedAt(String groupId) {
        State state = states.get(groupId);
        return state == null ? 0L : state.runStartedAt;
    }

    /** 실행 중인 그룹에 ERROR 알림을 붙인다. 같은 문구는 한 번만. */
    void addError(String groupId, String message) {
        State state = states.get(groupId);
        if (state != null && state.inRun && state.errors.size() < MAX_ERRORS) {
            state.errors.add(message);
        }
    }

    /**
     * 한 번의 판정.
     *
     * @param snapshots 감시 대상 그룹의 지금 상태. 여기 없는 그룹(지워졌거나 감시에서 빠짐)은 잊는다
     * @return 이번 판정에서 끝난 실행들
     */
    List<Completion> tick(Map<String, Snapshot> snapshots, long nowMillis) {
        states.keySet().retainAll(snapshots.keySet());
        startedThisTick.clear();
        List<Completion> done = new ArrayList<>();
        snapshots.forEach((groupId, snap) -> {
            State state = states.get(groupId);
            if (state == null) {
                state = new State();
                state.lastLoad = snap.loadCount();
                state.lastInvocations = snap.invocations();
                state.lastTickAt = nowMillis;
                states.put(groupId, state);
                // 감시를 켰을 때 이미 돌고 있었으면 그 실행부터 잡는다(카운터 증가는 기준이 없어 보지 않는다).
                if (snap.activeThreads() > 0 || snap.queued() > 0) {
                    startRun(state, nowMillis, snap.loadCount());
                    startedThisTick.add(groupId + " 처음 봄(실행 중) 기준=" + snap.loadCount());
                }
                return;
            }
            boolean loadGrew = snap.loadCount() != null && state.lastLoad != null && snap.loadCount() > state.lastLoad;
            boolean invoked = snap.invocations() > state.lastInvocations;
            boolean busy = snap.activeThreads() > 0 || snap.queued() > 0;
            if (busy || loadGrew || invoked) {
                if (!state.inRun) {
                    // 카운터가 이번에 늘었다면 시작 기준은 직전 값이다(이번 증가분까지 이번 실행의 적재로 센다).
                    // 시작 시각은 직전 판정 시각 - 실제로는 그 뒤 어딘가에서 시작했다. 늦게 잡으면 그 사이 난 오류를 놓친다(시험 중 발견).
                    startRun(state, state.lastTickAt >= 0 ? state.lastTickAt : nowMillis, state.lastLoad);
                    state.lastActivityAt = nowMillis;
                    startedThisTick.add(groupId + " active=" + snap.activeThreads() + " queued=" + snap.queued()
                            + " 적재=" + state.lastLoad + "→" + snap.loadCount() + " 처리=" + state.lastInvocations + "→" + snap.invocations());
                }
                state.lastActivityAt = nowMillis;
            } else if (state.inRun && nowMillis - state.lastActivityAt >= settleMillis) {
                done.add(finish(groupId, state, snap.loadCount(), nowMillis));
            }
            state.lastLoad = snap.loadCount();
            // 처리 횟수는 «최근 5분» 누적이라 줄어들 수도 있다 - 줄어든 것은 활동이 아니라 기준만 옮긴다.
            state.lastInvocations = snap.invocations();
            state.lastTickAt = nowMillis;
        });
        return done;
    }

    private static void startRun(State state, long startedAtMillis, Long baselineLoad) {
        state.inRun = true;
        state.runStartedAt = startedAtMillis;
        state.lastActivityAt = startedAtMillis;
        state.baselineLoad = baselineLoad;
        state.errors.clear();
    }

    private static Completion finish(String groupId, State state, Long loadNow, long nowMillis) {
        Long rows = null;
        if (loadNow != null) {
            long base = state.baselineLoad == null ? 0L : state.baselineLoad;
            // NiFi 가 재시작되면 카운터가 0 부터 다시 오른다 - 그땐 현재값이 곧 이번분이다.
            rows = loadNow >= base ? loadNow - base : loadNow;
        }
        Completion completion = new Completion(groupId, rows, List.copyOf(state.errors), state.runStartedAt, state.lastActivityAt,
                state.baselineLoad, loadNow);
        state.inRun = false;
        state.errors.clear();
        return completion;
    }
}

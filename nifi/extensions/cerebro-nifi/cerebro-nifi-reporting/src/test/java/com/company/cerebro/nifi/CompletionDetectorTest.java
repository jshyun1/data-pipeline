package com.company.cerebro.nifi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CompletionDetectorTest {

    private static final String G = "11111111-2222-3333-4444-555555555555";
    private final CompletionDetector detector = new CompletionDetector(3_000);

    private List<CompletionDetector.Completion> tick(long at, int active, int queued, Long load, long invocations) {
        return detector.tick(Map.of(G, new CompletionDetector.Snapshot(active, queued, load, invocations)), at);
    }

    @Test
    void runIsReportedAfterQuietPeriod_withRowsFromCounterDelta() {
        assertTrue(tick(0, 0, 0, 100L, 0).isEmpty());          // 처음 보는 그룹: 기준만
        assertTrue(tick(2_000, 1, 5, 100L, 3).isEmpty());      // 시작
        assertTrue(tick(4_000, 1, 2, 150L, 6).isEmpty());
        assertTrue(tick(6_000, 0, 0, 143L + 100, 6).isEmpty()); // 카운터 증가 = 아직 활동
        assertTrue(tick(8_000, 0, 0, 243L, 6).isEmpty());       // 조용 2초
        List<CompletionDetector.Completion> done = tick(9_500, 0, 0, 243L, 6);
        assertEquals(1, done.size());
        assertTrue(done.get(0).errors().isEmpty());
        assertEquals(143L, done.get(0).rows());
        assertEquals(0L, done.get(0).startedAtMillis());        // 활동을 처음 본 판정(2초)의 직전 판정 시각
        assertEquals(6_000L, done.get(0).endedAtMillis());
        assertTrue(tick(20_000, 0, 0, 243L, 6).isEmpty());      // 한 번만 보고
    }

    @Test
    void shortRunBetweenTicks_isCaughtByCounterAndInvocations() {
        tick(0, 0, 0, 10L, 0);
        assertTrue(tick(2_000, 0, 0, 53L, 4).isEmpty());        // 판정 사이에 시작해 끝났다
        List<CompletionDetector.Completion> done = tick(5_000, 0, 0, 53L, 4);
        assertEquals(43L, done.get(0).rows());
    }

    @Test
    void errorBulletinDuringRun_marksFailure() {
        tick(0, 0, 0, null, 0);
        tick(2_000, 1, 1, null, 1);
        detector.addError(G, "PutSQL: Connection refused");
        detector.addError(G, "PutSQL: Connection refused");
        tick(4_000, 0, 0, null, 1);
        CompletionDetector.Completion done = tick(7_000, 0, 0, null, 1).get(0);
        assertEquals(List.of("PutSQL: Connection refused"), done.errors());
        assertNull(done.rows());
    }

    @Test
    void shortRunSeenOnlyByDelta_startsAtPreviousTick() {
        // 판정 사이에 시작해 끝난 job - 실행 시작을 «활동을 본 시각»으로 잡으면 그 전에 난 오류를 놓친다(시험 3 에서 실제로 놓쳤다).
        tick(10_000, 0, 0, 0L, 0);
        tick(12_000, 0, 0, 0L, 2);
        assertEquals(10_000L, detector.runStartedAt(G));
    }

    @Test
    void invocationsWindowShrinking_isNotActivity() {
        tick(0, 0, 0, null, 50);
        assertTrue(tick(2_000, 0, 0, null, 20).isEmpty());
        assertTrue(tick(10_000, 0, 0, null, 20).isEmpty());
        assertFalse(detector.inRun(G));
    }

    @Test
    void groupAlreadyBusyWhenFirstSeen_startsRun() {
        tick(0, 2, 3, 0L, 1);
        assertTrue(detector.inRun(G));
    }

    @Test
    void jsonEscaping() {
        assertEquals("\"a\\\"b\\nc\"", JobCompletionReportingTask.json("a\"b\nc"));
    }
}

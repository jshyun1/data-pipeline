package com.company.pipeline.airflowdashboard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.airflowdashboard.AirflowDagAlert.RuleType;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.DagRun;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class AirflowDagAlertEvaluatorTest {

    private final AirflowDagAlertEvaluator evaluator = new AirflowDagAlertEvaluator();
    private final OffsetDateTime now = OffsetDateTime.parse("2026-08-18T17:00:00+09:00");

    @Test
    void detectsConfiguredRulesAndClearsThemWhenRunIsNormal() {
        AirflowDagCatalog catalog = mock(AirflowDagCatalog.class);
        when(catalog.getConsecutiveFailureThreshold()).thenReturn(3);
        when(catalog.getStaleDaysThreshold()).thenReturn(7);
        when(catalog.getDurationMultiplier()).thenReturn(new BigDecimal("3.00"));
        when(catalog.getSlaMinutes()).thenReturn(30);

        List<DagRun> failedRuns = List.of(
                run("a", "failed", now.minusDays(8), now.minusDays(8).plusHours(2)),
                run("b", "failed", now.minusDays(9), now.minusDays(9).plusMinutes(10)),
                run("c", "failed", now.minusDays(10), now.minusDays(10).plusMinutes(10)),
                run("baseline", "success", now.minusDays(11), now.minusDays(11).plusMinutes(10)));

        var detected = evaluator.evaluate(catalog, failedRuns, now);

        assertThat(detected).containsKeys(
                RuleType.CONSECUTIVE_FAILURE,
                RuleType.STALE,
                RuleType.DURATION_ANOMALY,
                RuleType.SLA_EXCEEDED);

        List<DagRun> normalRuns = List.of(
                run("new", "success", now.minusMinutes(10), now),
                run("old", "success", now.minusHours(1), now.minusMinutes(50)));
        assertThat(evaluator.evaluate(catalog, normalRuns, now)).isEmpty();
    }

    private DagRun run(String id, String state, OffsetDateTime start, OffsetDateTime end) {
        // conf 는 «감시 Run 인지» 판정용이라 경보 평가와 무관하다. null 을 넘겨도
        // isWatchAction 이 null 을 걸러내므로 안전하다.
        return new DagRun(id, state, start, end, null);
    }
}

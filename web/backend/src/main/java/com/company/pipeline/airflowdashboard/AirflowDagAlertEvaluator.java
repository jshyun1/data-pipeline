package com.company.pipeline.airflowdashboard;

import com.company.pipeline.airflowdashboard.AirflowDagAlert.RuleType;
import com.company.pipeline.airflowdashboard.AirflowDagAlert.Severity;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.DagRun;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class AirflowDagAlertEvaluator {

    public Map<RuleType, Detection> evaluate(
            AirflowDagCatalog catalog,
            List<DagRun> runs,
            OffsetDateTime now) {
        Map<RuleType, Detection> detected = new EnumMap<>(RuleType.class);
        int failures = 0;
        for (DagRun run : runs) {
            if (!"failed".equals(run.state())) break;
            failures++;
        }
        if (failures >= catalog.getConsecutiveFailureThreshold()) {
            detected.put(RuleType.CONSECUTIVE_FAILURE, new Detection(
                    Severity.DANGER,
                    failures + "회 연속 실패 (기준 " + catalog.getConsecutiveFailureThreshold() + "회)"));
        }

        DagRun latest = runs.isEmpty() ? null : runs.getFirst();
        if (latest == null || latest.startDate() == null) return detected;

        long staleDays = Duration.between(latest.startDate(), now).toDays();
        if (staleDays >= catalog.getStaleDaysThreshold()) {
            detected.put(RuleType.STALE, new Detection(
                    Severity.WARNING,
                    staleDays + "일 동안 미실행 (기준 " + catalog.getStaleDaysThreshold() + "일)"));
        }

        long latestSeconds = runSeconds(latest, now);
        double averageSeconds = runs.stream().skip(1)
                .filter(run -> "success".equals(run.state()) && run.startDate() != null && run.endDate() != null)
                .limit(10)
                .mapToLong(run -> runSeconds(run, now))
                .average().orElse(0);
        if (averageSeconds > 0
                && latestSeconds >= averageSeconds * catalog.getDurationMultiplier().doubleValue()) {
            detected.put(RuleType.DURATION_ANOMALY, new Detection(
                    Severity.INFO,
                    "평균 실행시간 대비 " + String.format("%.1f", latestSeconds / averageSeconds) + "배 소요"));
        }

        Integer slaMinutes = catalog.getSlaMinutes();
        if (slaMinutes != null && latestSeconds >= slaMinutes * 60L) {
            detected.put(RuleType.SLA_EXCEEDED, new Detection(
                    Severity.WARNING,
                    "SLA " + slaMinutes + "분 초과 (현재 " + latestSeconds / 60 + "분)"));
        }
        return detected;
    }

    private long runSeconds(DagRun run, OffsetDateTime now) {
        OffsetDateTime end = run.endDate() == null ? now : run.endDate();
        return Math.max(0, Duration.between(run.startDate(), end).toSeconds());
    }

    public record Detection(Severity severity, String message) {
    }
}

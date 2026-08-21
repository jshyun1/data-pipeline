package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 차트 영역 보강 — 원본 문서 5-5.
 *
 * <p>기존 차트는 "적재 건수"만 있었다. 원본 지적 #4 "실패·지연 추이가 없다 — 이상 감지에 더 유용하다"와
 * #6 "건수 기준 Top 5만 존재한다"를 채운다.
 *
 * <p>프리셋(1h/24h/7d/30d)은 화면 상단 칩과 같은 값을 쓴다. 버킷 입도는 프리셋에서 유도한다 —
 * 1시간을 일 단위로 잘라봐야 막대가 하나뿐이고, 30일을 5분으로 자르면 8,640개가 된다.
 */
@RestController
@RequestMapping("/api/dashboard")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.COMMON)
public class DashboardTrendController {

    private final JdbcTemplate jdbc;

    public DashboardTrendController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    /** 프리셋 → (조회 구간 분, 버킷 입도). 화면 칩 4종과 1:1 대응한다. */
    private record Window(int minutes, String bucket) {}

    private static Window window(String preset) {
        return switch (preset == null ? "24h" : preset) {
            case "1h" -> new Window(60, "5 minutes");
            case "7d" -> new Window(7 * 24 * 60, "6 hours");
            case "30d" -> new Window(30 * 24 * 60, "1 day");
            default -> new Window(24 * 60, "1 hour");
        };
    }

    /**
     * 실패 추이 + 지연 추이. 두 계열을 한 번에 내려서 화면이 요청 2번을 하지 않게 한다.
     *
     * <p>실패는 nifi_execution_log(status='FAILED')와 etl_job_run(status='FAILED')의 합이다.
     * 지연은 pipeline_metric_snapshot.consumer_lag 의 버킷별 최대값 — 평균을 쓰면 순간 급등이 묻힌다.
     */
    @GetMapping("/trends")
    public ApiResponse<Map<String, Object>> trends(@RequestParam(defaultValue = "24h") String preset) {
        Window w = window(preset);

        List<Map<String, Object>> failures = jdbc.queryForList("""
                WITH buckets AS (
                    SELECT generate_series(
                        date_bin(?::interval, now() - make_interval(mins => ?), TIMESTAMP '2000-01-01'),
                        date_bin(?::interval, now(), TIMESTAMP '2000-01-01'),
                        ?::interval) AS bucket
                ),
                nifi AS (
                    SELECT date_bin(?::interval, occurred_at, TIMESTAMP '2000-01-01') AS bucket, count(*) AS c
                    FROM nifi_execution_log
                    WHERE status = 'FAILED' AND occurred_at >= now() - make_interval(mins => ?)
                    GROUP BY 1
                ),
                etl AS (
                    SELECT date_bin(?::interval, started_at, TIMESTAMP '2000-01-01') AS bucket, count(*) AS c
                    FROM etl_job_run
                    WHERE status = 'FAILED' AND started_at >= now() - make_interval(mins => ?)
                    GROUP BY 1
                )
                SELECT to_char(b.bucket, 'YYYY-MM-DD"T"HH24:MI:SS') AS at,
                       COALESCE(n.c, 0) + COALESCE(e.c, 0) AS count
                FROM buckets b
                LEFT JOIN nifi n ON n.bucket = b.bucket
                LEFT JOIN etl  e ON e.bucket = b.bucket
                ORDER BY b.bucket
                """, w.bucket(), w.minutes(), w.bucket(), w.bucket(),
                w.bucket(), w.minutes(), w.bucket(), w.minutes());

        List<Map<String, Object>> lag = jdbc.queryForList("""
                SELECT to_char(date_bin(?::interval, collected_at, TIMESTAMP '2000-01-01'),
                               'YYYY-MM-DD"T"HH24:MI:SS') AS at,
                       COALESCE(max(consumer_lag), 0) AS count
                FROM pipeline_metric_snapshot
                WHERE collected_at >= now() - make_interval(mins => ?) AND consumer_lag IS NOT NULL
                GROUP BY 1
                ORDER BY 1
                """, w.bucket(), w.minutes());

        return ApiResponse.success(Map.of(
                "preset", preset,
                "bucket", w.bucket(),
                "failures", failures,
                "lag", lag));
    }

    /**
     * Top 5 3종 — 건수 / 소요시간 / 실패 (원본 #16, #17).
     *
     * <p>각 행에 딥링크용 식별자를 함께 내린다. 막대를 클릭하면 그 작업의 실행 이력으로 이동해야 하는데
     * (원본 #16), 라벨 문자열만으로는 어느 화면으로 보낼지 알 수 없기 때문이다.
     *
     * <p>소요시간은 etl_job_run 의 (ended_at - started_at) 이다. 수집 주기가 15초라 ±15초 오차가 있다 —
     * 초 단위 비교가 아니라 "어느 작업이 오래 걸리는가"를 보는 용도다.
     */
    @GetMapping("/top5")
    public ApiResponse<Map<String, Object>> top5(
            @RequestParam(defaultValue = "count") String metric,
            @RequestParam(defaultValue = "24h") String preset,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to) {
        // from/to(YYYY-MM-DD)가 오면 상단 RangePicker 날짜범위로, 아니면 preset 롤링창으로 집계한다.
        boolean useRange = from != null && !from.isBlank() && to != null && !to.isBlank();
        Window w = window(preset);
        String timeCol = "duration".equals(metric) ? "r.started_at" : "occurred_at";
        String timePred;
        Object[] timeArgs;
        if (useRange) {
            timePred = timeCol + " >= ?::date AND " + timeCol + " < (?::date + 1)";
            timeArgs = new Object[]{from, to};
        } else {
            timePred = timeCol + " >= now() - make_interval(mins => ?)";
            timeArgs = new Object[]{w.minutes()};
        }
        List<Map<String, Object>> rows = switch (metric) {
            case "duration" -> jdbc.queryForList(
                    "SELECT COALESCE(j.job_name, 'job#' || r.job_id) AS label, "
                            + "round(avg(EXTRACT(EPOCH FROM (r.ended_at - r.started_at))))::bigint AS value, "
                            + "count(*) AS runs, r.job_id AS job_id "
                            + "FROM etl_job_run r LEFT JOIN etl_job j ON j.id = r.job_id "
                            + "WHERE r.ended_at IS NOT NULL AND " + timePred + " "
                            + "GROUP BY r.job_id, j.job_name ORDER BY 2 DESC LIMIT 5", timeArgs);
            case "failure" -> jdbc.queryForList(
                    "SELECT COALESCE(group_name, processor_name, '(미상)') AS label, count(*) AS value, "
                            + "count(*) AS runs, max(job_id) AS job_id FROM nifi_execution_log "
                            + "WHERE status = 'FAILED' AND " + timePred + " GROUP BY 1 ORDER BY 2 DESC LIMIT 5", timeArgs);
            default -> jdbc.queryForList(
                    "SELECT COALESCE(group_name, processor_name, '(미상)') AS label, "
                            + "COALESCE(sum(inserted_count), 0) AS value, count(*) AS runs, max(job_id) AS job_id "
                            + "FROM nifi_execution_log WHERE " + timePred + " GROUP BY 1 ORDER BY 2 DESC LIMIT 5", timeArgs);
        };
        return ApiResponse.success(Map.of("metric", metric, "preset", preset, "items", rows));
    }
}

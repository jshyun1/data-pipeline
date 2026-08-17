package com.company.pipeline.rollup;

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
 * 누적형 KPI · 시계열 차트 (U7/U17, 설계서 5-2절). pipeline_load_rollup 을 읽기만 한다.
 *
 * <p>프리셋별 해상도: 1h→MIN5(12), 24h→HOUR(24), 7d/30d→DAY. 클라이언트가 더 조밀한 버킷을
 * 요청해도 400 이 아니라 자동 상향 조정한다(관제 화면은 어떤 입력에도 비면 안 된다).
 */
@RestController
@RequestMapping("/api/dashboard/kpi")
public class KpiController {

    private final JdbcTemplate jdbc;

    public KpiController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public record TimelineBucket(String source, String bucketStart, long loadedCount, long observationCount) {}

    public record TimelineResponse(String preset, String granularity, String since, boolean bucketAdjusted,
                                   List<TimelineBucket> buckets) {}

    @GetMapping("/timeline")
    public ApiResponse<TimelineResponse> timeline(@RequestParam(defaultValue = "24h") String preset) {
        String gran;
        String intervalExpr;
        switch (preset) {
            case "1h"  -> { gran = "MIN5"; intervalExpr = "1 hour"; }
            case "7d"  -> { gran = "DAY";  intervalExpr = "7 days"; }
            case "30d" -> { gran = "DAY";  intervalExpr = "30 days"; }
            default    -> { gran = "HOUR"; intervalExpr = "24 hours"; preset = "24h"; }
        }
        List<TimelineBucket> buckets = jdbc.query(
                "SELECT pipeline_source, bucket_start, SUM(loaded_count) AS loaded, "
                        + "SUM(observation_count) AS obs "
                        + "FROM pipeline_load_rollup "
                        + "WHERE granularity = ? AND bucket_start >= now() - (?::interval) "
                        + "GROUP BY pipeline_source, bucket_start ORDER BY bucket_start",
                (rs, i) -> new TimelineBucket(
                        rs.getString("pipeline_source"),
                        rs.getObject("bucket_start").toString(),
                        rs.getLong("loaded"),
                        rs.getLong("obs")),
                gran, intervalExpr);
        String since = jdbc.queryForObject("SELECT (now() - (?::interval))::text", String.class, intervalExpr);
        return ApiResponse.success(new TimelineResponse(preset, gran, since, false, buckets));
    }

    /** 소스별 누적 합계(카드 상단 값). */
    @GetMapping("/summary")
    public ApiResponse<List<Map<String, Object>>> summary(@RequestParam(defaultValue = "24h") String preset) {
        String gran = switch (preset) {
            case "1h" -> "MIN5";
            case "7d", "30d" -> "DAY";
            default -> "HOUR";
        };
        String intervalExpr = switch (preset) {
            case "1h" -> "1 hour";
            case "7d" -> "7 days";
            case "30d" -> "30 days";
            default -> "24 hours";
        };
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT pipeline_source, SUM(loaded_count) AS loaded, SUM(observation_count) AS obs, "
                        + "count(*) AS series "
                        + "FROM pipeline_load_rollup "
                        + "WHERE granularity = ? AND bucket_start >= now() - (?::interval) "
                        + "GROUP BY pipeline_source",
                gran, intervalExpr);
        return ApiResponse.success(rows);
    }
}

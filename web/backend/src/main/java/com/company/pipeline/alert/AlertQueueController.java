package com.company.pipeline.alert;

import com.company.pipeline.common.ApiResponse;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 조치 대기열 (U16, 설계서 5-2 ①). 화면이 20초마다 이것 하나만 호출한다. 열린 alert_instance 를
 * 심각도순으로 내린다. (연쇄억제 접기·스누즈 필터·딥링크 해석 등은 후속 단위에서 확장.)
 */
@RestController
@RequestMapping("/api/dashboard/queue")
public class AlertQueueController {

    private final JdbcTemplate jdbc;

    public AlertQueueController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> queue(@RequestParam(defaultValue = "20") int limit) {
        Map<String, Object> counts = jdbc.queryForMap("""
                SELECT
                    count(*) FILTER (WHERE severity='CRITICAL' AND state<>'RESOLVED') AS critical,
                    count(*) FILTER (WHERE severity='WARNING'  AND state<>'RESOLVED') AS warning,
                    count(*) FILTER (WHERE severity='INFO'     AND state<>'RESOLVED') AS info,
                    count(*) FILTER (WHERE state='UNKNOWN') AS unknown,
                    count(*) FILTER (WHERE ack_at IS NOT NULL) AS acked,
                    count(*) FILTER (WHERE snooze_until IS NOT NULL AND snooze_until > now()) AS snoozed,
                    count(*) FILTER (WHERE suppressed_by IS NOT NULL) AS suppressed
                FROM alert_instance WHERE closed_at IS NULL
                """);
        Long totalOpen = jdbc.queryForObject(
                "SELECT count(*) FROM alert_instance WHERE closed_at IS NULL", Long.class);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT id, rule_type_code, severity, state, kpi_axis, target_key, target_label,
                       component_code, summary, observed_value, threshold_value,
                       EXTRACT(EPOCH FROM (now() - condition_since))::bigint AS duration_seconds,
                       deep_link, ack_at IS NOT NULL AS acked, notify_count
                FROM alert_instance
                WHERE closed_at IS NULL AND suppressed_by IS NULL
                ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
                         last_transition_at DESC
                LIMIT ?
                """, limit);
        return ApiResponse.success(Map.of(
                "counts", counts,
                "totalOpen", totalOpen == null ? 0 : totalOpen,
                "truncated", totalOpen != null && totalOpen > limit,
                "items", items));
    }

    /**
     * 알림 이력 (원본 5-7 / PDF 8쪽). 대기열은 "지금 열려 있는 것"만 보여주므로 종료된 알림과
     * 누가 언제 확인했는지가 화면 어디에도 남지 않았다.
     *
     * <p>filter: all | unacked | acked. 해소된 알림도 포함한다 — "그때 그거 누가 봤더라"에
     * 답하는 것이 이 화면의 목적이다.
     */
    @GetMapping("/history")
    public ApiResponse<Map<String, Object>> history(
            @RequestParam(defaultValue = "all") String filter,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "7") int days) {
        int window = Math.max(1, Math.min(days, 90));
        int cap = Math.max(1, Math.min(limit, 500));
        String where = switch (filter) {
            case "unacked" -> "AND ack_at IS NULL";
            case "acked" -> "AND ack_at IS NOT NULL";
            default -> "";
        };
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT id, rule_type_code, severity, state, kpi_axis, target_key, target_label,
                       summary, observed_value, threshold_value, deep_link, notify_count,
                       ack_by, ack_at, ack_comment,
                       ack_at IS NOT NULL AS acked,
                       closed_at IS NOT NULL AS closed,
                       resolve_reason,
                       started_at, condition_since, last_transition_at, resolved_at
                FROM alert_instance
                WHERE COALESCE(started_at, condition_since, created_at) >= now() - make_interval(days => ?)
                """ + where + "\n" + """
                ORDER BY COALESCE(last_transition_at, condition_since) DESC
                LIMIT ?
                """, window, cap);

        Map<String, Object> counts = jdbc.queryForMap("""
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE ack_at IS NULL) AS unacked,
                       count(*) FILTER (WHERE ack_at IS NOT NULL) AS acked
                FROM alert_instance
                WHERE COALESCE(started_at, condition_since, created_at) >= now() - make_interval(days => ?)
                """, window);

        return ApiResponse.success(Map.of(
                "filter", filter, "days", window, "counts", counts, "items", items));
    }

    /** 한 알림의 상태 전이 타임라인. 누가 언제 무엇을 했는지 그대로 보여준다. */
    @GetMapping("/history/{id}/events")
    public ApiResponse<List<Map<String, Object>>> events(@PathVariable long id) {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT event_type, from_state, to_state, actor, occurred_at
                FROM alert_instance_event WHERE instance_id = ? ORDER BY occurred_at, id
                """, id));
    }
}

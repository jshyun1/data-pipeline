package com.company.pipeline.alert;

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
}

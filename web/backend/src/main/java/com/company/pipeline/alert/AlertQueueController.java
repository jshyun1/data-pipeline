package com.company.pipeline.alert;

import com.company.pipeline.common.ApiResponse;
import java.util.ArrayList;
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
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.COMMON)
public class AlertQueueController {

    private final JdbcTemplate jdbc;

    public AlertQueueController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> queue(@RequestParam(defaultValue = "20") int limit) {
        // 심각도 배지(critical/warning/info)는 «목록에 실제로 보이는 조치 대상»과 같은 필터를 써야
        // 배지 숫자와 펼친 목록 건수가 일치한다(확인·스누즈·억제 제외). acked/snoozed/suppressed 는 별도 카운트.
        String actionable = "ack_at IS NULL AND (snooze_until IS NULL OR snooze_until <= now()) AND suppressed_by IS NULL";
        Map<String, Object> counts = jdbc.queryForMap("""
                SELECT
                    count(*) FILTER (WHERE severity='CRITICAL' AND state<>'RESOLVED' AND %1$s) AS critical,
                    count(*) FILTER (WHERE severity='WARNING'  AND state<>'RESOLVED' AND %1$s) AS warning,
                    count(*) FILTER (WHERE severity='INFO'     AND state<>'RESOLVED' AND %1$s) AS info,
                    count(*) FILTER (WHERE state='UNKNOWN') AS unknown,
                    count(*) FILTER (WHERE ack_at IS NOT NULL) AS acked,
                    count(*) FILTER (WHERE snooze_until IS NOT NULL AND snooze_until > now()) AS snoozed,
                    count(*) FILTER (WHERE suppressed_by IS NOT NULL) AS suppressed
                FROM alert_instance WHERE closed_at IS NULL
                """.formatted(actionable));
        Long totalOpen = jdbc.queryForObject(
                "SELECT count(*) FROM alert_instance WHERE closed_at IS NULL AND " + actionable, Long.class);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT id, rule_type_code, severity, state, kpi_axis, target_key, target_label,
                       component_code, summary, observed_value, threshold_value,
                       EXTRACT(EPOCH FROM (now() - condition_since))::bigint AS duration_seconds,
                       deep_link, ack_at IS NOT NULL AS acked, notify_count
                FROM alert_instance
                WHERE closed_at IS NULL AND suppressed_by IS NULL
                  -- 원본 5-1(f): 확인(ack)하면 목록에서 제거. 스누즈 중인 항목도 창이 지날 때까지 숨긴다.
                  AND ack_at IS NULL
                  AND (snooze_until IS NULL OR snooze_until <= now())
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
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "7") int days,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int pageSize) {
        int window = Math.max(1, Math.min(days, 90));
        int size = Math.max(1, Math.min(pageSize, 100));
        int offset = Math.max(0, page) * size;

        // 조회조건을 WHERE 로 조립(ETL/CDC 로그 화면처럼 심각도·검색어·확인여부·기간).
        StringBuilder where = new StringBuilder(
                "WHERE COALESCE(started_at, condition_since, created_at) >= now() - make_interval(days => ?)");
        List<Object> args = new ArrayList<>();
        args.add(window);
        if ("unacked".equals(filter)) {
            where.append(" AND ack_at IS NULL");
        } else if ("acked".equals(filter)) {
            where.append(" AND ack_at IS NOT NULL");
        }
        if (severity != null && !severity.isBlank() && !"ALL".equalsIgnoreCase(severity)) {
            where.append(" AND severity = ?");
            args.add(severity);
        }
        if (q != null && !q.isBlank()) {
            where.append(" AND (summary ILIKE ? OR target_label ILIKE ?)");
            String like = "%" + q.trim() + "%";
            args.add(like);
            args.add(like);
        }

        Long total = jdbc.queryForObject(
                "SELECT count(*) FROM alert_instance " + where, Long.class, args.toArray());

        List<Object> itemArgs = new ArrayList<>(args);
        itemArgs.add(size);
        itemArgs.add(offset);
        List<Map<String, Object>> items = jdbc.queryForList("""
                SELECT id, rule_type_code, severity, state, kpi_axis, target_key, target_label,
                       summary, observed_value, threshold_value, deep_link, notify_count,
                       ack_by, ack_at, ack_comment,
                       ack_at IS NOT NULL AS acked,
                       closed_at IS NOT NULL AS closed,
                       resolve_reason,
                       started_at, condition_since, last_transition_at, resolved_at
                FROM alert_instance
                """ + where + "\n" + """
                ORDER BY COALESCE(last_transition_at, condition_since) DESC
                LIMIT ? OFFSET ?
                """, itemArgs.toArray());

        // 필터 배지용 집계는 기간 전체 기준(심각도·검색어와 무관하게 개수를 보여준다).
        Map<String, Object> counts = jdbc.queryForMap("""
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE ack_at IS NULL) AS unacked,
                       count(*) FILTER (WHERE ack_at IS NOT NULL) AS acked
                FROM alert_instance
                WHERE COALESCE(started_at, condition_since, created_at) >= now() - make_interval(days => ?)
                """, window);

        Map<String, Object> body = new java.util.HashMap<>();
        body.put("filter", filter);
        body.put("severity", severity == null ? "ALL" : severity);
        body.put("q", q == null ? "" : q);
        body.put("days", window);
        body.put("page", Math.max(0, page));
        body.put("pageSize", size);
        body.put("total", total == null ? 0 : total);
        body.put("counts", counts);
        body.put("items", items);
        return ApiResponse.success(body);
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

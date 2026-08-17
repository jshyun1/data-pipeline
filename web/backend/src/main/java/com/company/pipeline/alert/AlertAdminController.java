package com.company.pipeline.alert;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 알림 규칙 관리 + 조치(확인/스누즈) API (U9/U18, 설계서 5-2 ⑤⑥).
 * 규칙 관리 화면(U21)·확인/스누즈 UI(U18)가 이 API 를 쓴다.
 */
@RestController
public class AlertAdminController {

    private final JdbcTemplate jdbc;

    public AlertAdminController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------- 규칙 관리

    @GetMapping("/api/admin/alert-rules")
    public ApiResponse<List<Map<String, Object>>> listRules() {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT r.id, r.rule_type_code, rt.label AS type_label, rt.category, r.builtin_key, r.name,
                       r.enabled, r.severity, r.params_json, r.for_seconds, r.clear_seconds, r.mandatory,
                       r.last_evaluated_at, r.last_eval_error
                FROM alert_rule r JOIN alert_rule_type rt ON r.rule_type_code = rt.code
                WHERE r.deleted_at IS NULL ORDER BY rt.eval_priority, r.name"""));
    }

    public record UpdateRuleRequest(Boolean enabled, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds) {}

    @PutMapping("/api/admin/alert-rules/{id}")
    public ApiResponse<Void> updateRule(@PathVariable long id, @RequestBody UpdateRuleRequest req,
                                        @AuthenticationPrincipal AppUser user) {
        String by = user != null ? user.getUserId() : "admin";
        try {
            int n = jdbc.update("""
                    UPDATE alert_rule SET
                        enabled       = COALESCE(?, enabled),
                        severity      = COALESCE(?, severity),
                        params_json   = COALESCE(?::jsonb, params_json),
                        for_seconds   = COALESCE(?, for_seconds),
                        clear_seconds = COALESCE(?, clear_seconds),
                        updated_by = ?, updated_at = now()
                    WHERE id = ? AND deleted_at IS NULL
                    """, req.enabled(), req.severity(), req.paramsJson(), req.forSeconds(), req.clearSeconds(), by, id);
            if (n == 0) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙을 찾을 수 없습니다: " + id);
            }
        } catch (DataAccessException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 값이 올바르지 않습니다(params_json 등 확인).");
        }
        return ApiResponse.success(null);
    }

    // ------------------------------------------------------------- 확인/스누즈

    public record AckRequest(String comment) {}

    public record SnoozeRequest(Integer minutes) {}

    @PostMapping("/api/alerts/{id}/ack")
    public ApiResponse<Void> ack(@PathVariable long id, @RequestBody(required = false) AckRequest req,
                                 @AuthenticationPrincipal AppUser user) {
        String actor = user != null ? user.getUserId() : "admin";
        String comment = req != null ? req.comment() : null;
        int n = jdbc.update("UPDATE alert_instance SET ack_by=?, ack_at=now(), ack_comment=?, updated_at=now(), "
                + "version=version+1 WHERE id=? AND closed_at IS NULL", actor, comment, id);
        requireFound(n, id);
        event(id, "ACKED", actor, comment);
        return ApiResponse.success(null);
    }

    @PostMapping("/api/alerts/{id}/unack")
    public ApiResponse<Void> unack(@PathVariable long id, @AuthenticationPrincipal AppUser user) {
        String actor = user != null ? user.getUserId() : "admin";
        int n = jdbc.update("UPDATE alert_instance SET ack_by=NULL, ack_at=NULL, ack_comment=NULL, "
                + "updated_at=now(), version=version+1 WHERE id=? AND closed_at IS NULL", id);
        requireFound(n, id);
        event(id, "UNACKED", actor, null);
        return ApiResponse.success(null);
    }

    @PostMapping("/api/alerts/{id}/snooze")
    public ApiResponse<Void> snooze(@PathVariable long id, @RequestBody SnoozeRequest req,
                                    @AuthenticationPrincipal AppUser user) {
        String actor = user != null ? user.getUserId() : "admin";
        int minutes = req != null && req.minutes() != null ? req.minutes() : 60;
        int n = jdbc.update("UPDATE alert_instance SET snooze_until=now() + (? * interval '1 minute'), "
                + "snooze_by=?, updated_at=now(), version=version+1 WHERE id=? AND closed_at IS NULL",
                minutes, actor, id);
        requireFound(n, id);
        event(id, "SNOOZED", actor, minutes + "분");
        return ApiResponse.success(null);
    }

    private void requireFound(int n, long id) {
        if (n == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "열린 알림을 찾을 수 없습니다: " + id);
        }
    }

    private void event(long instanceId, String type, String actor, String comment) {
        jdbc.update("INSERT INTO alert_instance_event (instance_id, event_type, actor, comment, occurred_at) "
                + "VALUES (?, ?, ?, ?, now())", instanceId, type, actor, comment);
    }
}

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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
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
                       r.enabled, r.severity,
                       -- jsonb 를 그대로 내리면 드라이버가 PGobject({type,value,null})로 감싸서
                       -- 화면이 조건 값을 못 읽는다. text 로 캐스팅해 순수 JSON 문자열로 보낸다.
                       r.params_json::text AS params_json,
                       r.scope_json::text AS scope_json, r.renotify_seconds,
                       r.for_seconds, r.clear_seconds, r.mandatory,
                       r.last_evaluated_at, r.last_eval_error
                FROM alert_rule r JOIN alert_rule_type rt ON r.rule_type_code = rt.code
                WHERE r.deleted_at IS NULL ORDER BY rt.eval_priority, r.name"""));
    }

    /**
     * 규칙 감시 범위 선택 후보. category=ETL → etl_job 목록(id,name), CDC → pipeline_definition 목록.
     * 규칙 추가/수정 폼의 «감시 대상 선택»이 이 목록에서 고른다.
     */
    @GetMapping("/api/admin/alert-scope-targets")
    public ApiResponse<List<Map<String, Object>>> scopeTargets(@RequestParam(defaultValue = "ETL") String category) {
        if ("CDC".equalsIgnoreCase(category)) {
            return ApiResponse.success(jdbc.queryForList(
                    "SELECT id, name FROM pipeline_definition ORDER BY name"));
        }
        return ApiResponse.success(jdbc.queryForList(
                "SELECT id, job_name AS name FROM etl_job WHERE deleted_at IS NULL ORDER BY job_name"));
    }

    /**
     * 규칙 유형 카탈로그 + 유형별 조건 파라미터 스키마.
     *
     * <p>화면이 "이 유형은 어떤 조건을 입력받아야 하는가"를 알아야 조건 입력 폼을 그릴 수 있다.
     * 스키마를 프론트에 하드코딩하면 유형이 하나 늘 때마다 두 곳을 고쳐야 하고, 곧 서로 어긋난다.
     */
    @GetMapping("/api/admin/alert-rule-types")
    public ApiResponse<List<Map<String, Object>>> listRuleTypes() {
        List<Map<String, Object>> types = jdbc.queryForList("""
                SELECT code, label, category, kpi_axis, mandatory, default_severity, min_severity,
                       description, eval_priority
                FROM alert_rule_type ORDER BY eval_priority, label""");
        for (Map<String, Object> t : types) {
            t.put("paramSpec", PARAM_SPEC.getOrDefault((String) t.get("code"), List.of()));
        }
        return ApiResponse.success(types);
    }

    /**
     * 유형별 조건 파라미터 정의. {key, label, unit, type, min, max, defaultValue}.
     *
     * <p>params_json 의 키와 1:1로 맞춘다 — 평가 엔진이 읽는 키와 화면이 쓰는 키가 다르면
     * 화면에서 저장한 값이 조용히 무시된다.
     */
    private static final Map<String, List<Map<String, Object>>> PARAM_SPEC = Map.of(
            "SERVER_MEMORY", List.of(
                    param("threshold", "발화 임계", "%", 1, 99, 80),
                    param("clear", "해제 임계", "%", 1, 99, 72)),
            "SERVER_DISK", List.of(
                    param("threshold", "발화 임계", "%", 1, 99, 80),
                    param("clear", "해제 임계", "%", 1, 99, 72)),
            "DATA_FRESHNESS", List.of(
                    param("staleness_minutes", "적재 정체 시간", "분", 1, 1440, 30)),
            "CDC_LAG", List.of(
                    param("threshold", "미처리 임계", "건", 1, 100000000, 50000),
                    param("clear", "해제 임계", "건", 0, 100000000, 10000)),
            "JOB_CONSECUTIVE_FAILURE", List.of(
                    param("count", "연속 실패 횟수", "회", 2, 100, 3)),
            "JOB_NOT_RUN", List.of(
                    param("days", "미실행 허용 기간", "일", 1, 365, 7)),
            "CONNECTOR_FAILED", List.of(),
            "SERVICE_UNREACHABLE", List.of(),
            "JOB_FAILURE", List.of(),
            "COLLECTOR_DOWN", List.of());

    private static Map<String, Object> param(String key, String label, String unit,
                                             long min, long max, long defaultValue) {
        return Map.of("key", key, "label", label, "unit", unit, "type", "INT",
                "min", min, "max", max, "defaultValue", defaultValue);
    }

    public record UpdateRuleRequest(Boolean enabled, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds, String name,
                                    String scopeJson, Integer renotifySeconds) {}

    public record CreateRuleRequest(String ruleTypeCode, String name, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds,
                                    String scopeJson, Integer renotifySeconds) {}

    /**
     * 규칙 추가 (PDF 9쪽 "[+ 규칙 추가]"). 같은 유형으로 임계값만 다른 규칙을 여러 개 두는 것을 허용한다 —
     * "메모리 80% 경고 / 90% 위험"처럼 한 지표에 두 단계를 거는 게 실제 운영 방식이다.
     */
    @PostMapping("/api/admin/alert-rules")
    public ApiResponse<Map<String, Object>> createRule(@RequestBody CreateRuleRequest req,
                                                       @AuthenticationPrincipal AppUser user) {
        if (req.ruleTypeCode() == null || req.ruleTypeCode().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 유형은 필수입니다.");
        }
        if (req.name() == null || req.name().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙명은 필수입니다.");
        }
        Integer known = jdbc.queryForObject(
                "SELECT count(*) FROM alert_rule_type WHERE code = ?", Integer.class, req.ruleTypeCode());
        if (known == null || known == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "알 수 없는 규칙 유형입니다: " + req.ruleTypeCode());
        }
        String by = user != null ? user.getUserId() : "admin";
        try {
            Long id = jdbc.queryForObject("""
                    INSERT INTO alert_rule
                        (rule_type_code, name, severity, params_json, scope_json, for_seconds, clear_seconds,
                         renotify_seconds, enabled, mandatory, created_by, updated_by)
                    VALUES (?, ?,
                            COALESCE(?, (SELECT default_severity FROM alert_rule_type WHERE code = ?)),
                            COALESCE(?::jsonb, '{}'::jsonb),
                            COALESCE(?::jsonb, '{"kind": "ALL"}'::jsonb),
                            COALESCE(?, 120), COALESCE(?, 300), COALESCE(?, 1800), TRUE, FALSE, ?, ?)
                    RETURNING id
                    """, Long.class,
                    req.ruleTypeCode(), req.name(), req.severity(), req.ruleTypeCode(),
                    req.paramsJson(), req.scopeJson(), req.forSeconds(), req.clearSeconds(),
                    req.renotifySeconds(), by, by);
            return ApiResponse.success(Map.of("id", id == null ? 0L : id));
        } catch (DataAccessException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 값이 올바르지 않습니다(조건·심각도 확인).");
        }
    }

    /**
     * 규칙 삭제. 운영자가 자기 환경에 맞게 규칙 구성을 정할 수 있어야 해서 필수 규칙도 삭제를 허용한다
     * (mandatory 는 기본값 표시용으로만 남는다).
     */
    @DeleteMapping("/api/admin/alert-rules/{id}")
    public ApiResponse<Void> deleteRule(@PathVariable long id) {
        int n = jdbc.update("UPDATE alert_rule SET deleted_at=now() WHERE id=? AND deleted_at IS NULL", id);
        if (n == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙을 찾을 수 없습니다: " + id);
        }
        return ApiResponse.success(null);
    }

    @PutMapping("/api/admin/alert-rules/{id}")
    public ApiResponse<Void> updateRule(@PathVariable long id, @RequestBody UpdateRuleRequest req,
                                        @AuthenticationPrincipal AppUser user) {
        String by = user != null ? user.getUserId() : "admin";
        try {
            int n = jdbc.update("""
                    UPDATE alert_rule SET
                        name             = COALESCE(?, name),
                        enabled          = COALESCE(?, enabled),
                        severity         = COALESCE(?, severity),
                        params_json      = COALESCE(?::jsonb, params_json),
                        scope_json       = COALESCE(?::jsonb, scope_json),
                        for_seconds      = COALESCE(?, for_seconds),
                        clear_seconds    = COALESCE(?, clear_seconds),
                        renotify_seconds = COALESCE(?, renotify_seconds),
                        updated_by = ?, updated_at = now()
                    WHERE id = ? AND deleted_at IS NULL
                    """, req.name(), req.enabled(), req.severity(), req.paramsJson(),
                    req.scopeJson(), req.forSeconds(), req.clearSeconds(), req.renotifySeconds(), by, id);
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

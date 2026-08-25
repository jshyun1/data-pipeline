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
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.ADMIN)
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
                       r.schedule_enabled, r.schedule_time, r.schedule_last_fired_on,
                       r.schedule_run_count, r.schedule_last_run_at,
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
        // ETL_CHAIN: 프로세스 그룹이 아니라 «적재 테이블 하나»를 감시 단위로 고른다.
        // 그룹(DZ) 하나가 테이블 5개를 적재하므로 그룹 단위로는 "COM001M만 감시"가 불가능했다.
        // 체인의 대표는 최종 적재 스텝(target_table 보유)이고, 그 앞의 trigger/extract/truncate 는
        // etl_job_link 를 거슬러 올라가 같은 체인으로 묶인다(AlertEngine.chainOfProcessor).
        if ("ETL_CHAIN".equalsIgnoreCase(category)) {
            return ApiResponse.success(jdbc.queryForList("""
                    SELECT s.id,
                           j.job_name || ' / ' || s.target_table
                           -- 같은 그룹에 같은 테이블을 적재하는 스텝이 둘 이상이면(Template 등)
                           -- 이름만으로는 드롭다운에서 구분이 안 되니 스텝명을 덧붙인다.
                           || CASE WHEN count(*) OVER (PARTITION BY j.job_name, s.target_table) > 1
                                   THEN ' (' || s.step_name || ')' ELSE '' END AS name
                    FROM etl_job_step s JOIN etl_job j ON j.id = s.job_id
                    WHERE s.deleted_at IS NULL AND j.deleted_at IS NULL AND s.target_table IS NOT NULL
                    ORDER BY j.job_name, s.target_table"""));
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

    // scheduleEnabled/scheduleTime("HH:mm") = 일별 점검. null 이면 종전 값 유지(부분 수정 허용).
    public record UpdateRuleRequest(Boolean enabled, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds, String name,
                                    String scopeJson, Integer renotifySeconds,
                                    Boolean scheduleEnabled, String scheduleTime) {}

    public record CreateRuleRequest(String ruleTypeCode, String name, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds,
                                    String scopeJson, Integer renotifySeconds,
                                    Boolean scheduleEnabled, String scheduleTime) {}

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
                         renotify_seconds, enabled, mandatory, created_by, updated_by,
                         schedule_enabled, schedule_time, schedule_last_fired_on,
                         schedule_run_count, schedule_last_run_at)
                    VALUES (?, ?,
                            COALESCE(?, (SELECT default_severity FROM alert_rule_type WHERE code = ?)),
                            COALESCE(?::jsonb, '{}'::jsonb),
                            COALESCE(?::jsonb, '{"kind": "ALL"}'::jsonb),
                            COALESCE(?, 120), COALESCE(?, 300), COALESCE(?, 1800), TRUE, FALSE, ?, ?,
                            COALESCE(?, FALSE), ?::time,
                            -- 이미 지난 시각으로 스케줄을 만들면 저장하자마자 발화한다.
                            -- 오늘 몫은 끝난 것으로 찍어 두고 내일부터 돈다. schedule_last_run_at
                            -- 이 NULL 이라 2회차 조건(간격 경과)도 오늘은 성립하지 않는다.
                            CASE WHEN ?::time IS NOT NULL AND ?::time <= localtime
                                 THEN current_date END,
                            0, NULL)
                    RETURNING id
                    """, Long.class,
                    req.ruleTypeCode(), req.name(), req.severity(), req.ruleTypeCode(),
                    req.paramsJson(), req.scopeJson(), req.forSeconds(), req.clearSeconds(),
                    req.renotifySeconds(), by, by,
                    req.scheduleEnabled(), req.scheduleTime(), req.scheduleTime(), req.scheduleTime());
            return ApiResponse.success(Map.of("id", id == null ? 0L : id));
        } catch (DataAccessException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 값이 올바르지 않습니다(조건·심각도 확인).");
        }
    }

    /**
     * 규칙 삭제. 운영자가 자기 환경에 맞게 규칙 구성을 정할 수 있어야 해서 필수 규칙도 삭제를 허용한다
     * (mandatory 는 기본값 표시용으로만 남는다).
     */
    // ------------------------------------------------------------ 규칙별 수신자

    /**
     * 이 규칙의 알림을 «누가» 받는지. 수신자 전원을 내려주고 각자 켬/끔 상태를 함께 준다.
     *
     * <p>행이 하나도 없는 규칙은 종전대로 «전원» 받는다(=응답의 enabled 가 모두 true).
     * 여기서는 누가 받는지만 정한다 — 어떤 Job 을 감시할지는 규칙의 «감시 범위»가 정한다.
     */
    @GetMapping("/api/admin/alert-rules/{id}/recipients")
    public ApiResponse<List<Map<String, Object>>> ruleRecipients(@PathVariable long id) {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT r.id AS recipient_id, r.display_name, r.email, r.phone, r.enabled AS recipient_enabled,
                       -- 규칙에 명시 목록이 없으면 전원 수신이 기본이라 true 로 채워 내린다.
                       COALESCE(rr.enabled, NOT EXISTS (
                           SELECT 1 FROM notification_recipient_rule x WHERE x.rule_id = ?
                       )) AS enabled
                FROM notification_recipient r
                LEFT JOIN notification_recipient_rule rr ON rr.recipient_id = r.id AND rr.rule_id = ?
                WHERE r.deleted_at IS NULL
                ORDER BY r.display_name
                """, id, id));
    }

    public record RuleRecipientRequest(Long recipientId, Boolean enabled) {}

    /**
     * 규칙별 수신자 «전체 교체». 화면이 켬/끔 목록을 통째로 저장하므로 교체가 맞다.
     *
     * <p>끈 수신자도 enabled=false 로 «남긴다». 행을 지워 버리면 "명시 목록이 없다 = 전원 수신"과
     * 구분되지 않아, 전원을 끄면 오히려 전원에게 가는 뒤집힌 결과가 된다.
     */
    @PutMapping("/api/admin/alert-rules/{id}/recipients")
    public ApiResponse<Void> replaceRuleRecipients(@PathVariable long id,
                                                   @RequestBody List<RuleRecipientRequest> recipients) {
        jdbc.update("DELETE FROM notification_recipient_rule WHERE rule_id = ?", id);
        if (recipients == null) {
            return ApiResponse.success(null);
        }
        for (RuleRecipientRequest r : recipients) {
            if (r == null || r.recipientId() == null) {
                continue;
            }
            jdbc.update("""
                    INSERT INTO notification_recipient_rule (recipient_id, rule_id, enabled)
                    VALUES (?, ?, COALESCE(?, TRUE))
                    ON CONFLICT (recipient_id, rule_id) DO UPDATE SET
                        enabled = EXCLUDED.enabled, updated_at = now()
                    """, r.recipientId(), id, r.enabled());
        }
        return ApiResponse.success(null);
    }

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
                        schedule_enabled = COALESCE(?, schedule_enabled),
                        schedule_time    = CASE WHEN ? IS NULL THEN schedule_time ELSE ?::time END,
                        -- 시각을 바꿨는데 그게 오늘 이미 지났으면 오늘 몫은 끝난 것으로 본다
                        -- (저장하자마자 발화하는 것을 막는다). 시각이 아직 안 지났으면 빗장을 푼다.
                        schedule_last_fired_on = CASE
                            WHEN ? IS NULL THEN schedule_last_fired_on
                            WHEN ?::time <= localtime THEN current_date
                            ELSE NULL END,
                        -- 시각을 바꾸면 오늘 돈 회차는 무효다. 되돌려 두지 않으면
                        -- "오늘 이미 3회 다 돌았다"로 남아 새 시각이 오늘 안 돈다.
                        schedule_run_count  = CASE WHEN ? IS NULL THEN schedule_run_count ELSE 0 END,
                        schedule_last_run_at = CASE WHEN ? IS NULL THEN schedule_last_run_at ELSE NULL END,
                        updated_by = ?, updated_at = now()
                    WHERE id = ? AND deleted_at IS NULL
                    """, req.name(), req.enabled(), req.severity(), req.paramsJson(),
                    req.scopeJson(), req.forSeconds(), req.clearSeconds(), req.renotifySeconds(),
                    req.scheduleEnabled(), req.scheduleTime(), req.scheduleTime(),
                    req.scheduleTime(), req.scheduleTime(),
                    req.scheduleTime(), req.scheduleTime(), by, id);
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

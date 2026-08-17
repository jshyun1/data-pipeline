package com.company.pipeline.notification;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 알림 발송 설정 API (U13, 설계서 5-2 ⑨). 채널 on/off·설정, 수신자·구독 관리, 연결 테스트.
 * 발송 설정 화면이 이 API 를 쓴다. 시크릿(비밀번호)은 응답에 절대 안 내리고 secretSet 불리언만.
 */
@RestController
public class NotificationAdminController {

    private final JdbcTemplate jdbc;

    public NotificationAdminController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------- 채널

    @GetMapping("/api/admin/notification/channels")
    public ApiResponse<List<Map<String, Object>>> channels() {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT channel_type, enabled, config_json,
                       (encrypted_secret IS NOT NULL) AS secret_set,
                       rate_critical_per_min, rate_other_per_min, hard_limit_per_hour,
                       circuit_state, consecutive_failures, last_success_at, last_failure_reason
                FROM notification_channel_config ORDER BY id"""));
    }

    public record UpdateChannelRequest(Boolean enabled, String configJson, Integer rateCriticalPerMin,
                                       Integer rateOtherPerMin, Integer hardLimitPerHour) {}

    @PutMapping("/api/admin/notification/channels/{type}")
    public ApiResponse<Void> updateChannel(@PathVariable String type, @RequestBody UpdateChannelRequest req) {
        int n = jdbc.update("""
                UPDATE notification_channel_config SET
                    enabled = COALESCE(?, enabled),
                    config_json = COALESCE(?::jsonb, config_json),
                    rate_critical_per_min = COALESCE(?, rate_critical_per_min),
                    rate_other_per_min = COALESCE(?, rate_other_per_min),
                    hard_limit_per_hour = COALESCE(?, hard_limit_per_hour),
                    updated_at = now()
                WHERE channel_type = ?
                """, req.enabled(), req.configJson(), req.rateCriticalPerMin(), req.rateOtherPerMin(),
                req.hardLimitPerHour(), type);
        if (n == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "채널을 찾을 수 없습니다: " + type);
        }
        return ApiResponse.success(null);
    }

    /** 연결 테스트 — CHANNEL_TEST 발송 건을 아웃박스에 넣어 디스패처가 실제 발송을 시도한다. */
    @PostMapping("/api/notifications/channels/{type}/test")
    public ApiResponse<Void> test(@PathVariable String type) {
        String eventKey = "TEST-" + type + "-" + jdbc.queryForObject("SELECT floor(extract(epoch from now()))::bigint", Long.class);
        jdbc.update("""
                INSERT INTO notification_delivery
                    (event_key, dedup_key, channel_type, severity, severity_rank, category, template_key,
                     subject, body, status, next_attempt_at, expires_at, occurred_at)
                VALUES (?, md5(?), ?, 'INFO', 2, 'SYSTEM', 'CHANNEL_TEST',
                        '[관제] 연결 테스트', '이 메시지가 보이면 채널이 정상입니다.', 'PENDING',
                        now(), now() + interval '1 hour', now())
                ON CONFLICT (dedup_key) DO NOTHING
                """, eventKey, eventKey, type);
        return ApiResponse.success(null);
    }

    // ---------------------------------------------------------------- 수신자

    @GetMapping("/api/admin/notification/recipients")
    public ApiResponse<List<Map<String, Object>>> recipients() {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT r.id, r.user_id, r.display_name, r.email,
                       CASE WHEN r.phone IS NULL THEN NULL
                            ELSE regexp_replace(r.phone, '(\\d{3})\\d+(\\d{4})', '\\1-****-\\2') END AS phone_masked,
                       r.enabled,
                       (SELECT json_agg(json_build_object('channel', s.channel_type, 'minSeverity', s.min_severity))
                        FROM notification_subscription s WHERE s.recipient_id = r.id AND s.enabled) AS subscriptions
                FROM notification_recipient r WHERE r.deleted_at IS NULL ORDER BY r.id"""));
    }

    public record RecipientRequest(String userId, String displayName, String email, String phone) {}

    @PostMapping("/api/admin/notification/recipients")
    public ApiResponse<Map<String, Object>> createRecipient(@RequestBody RecipientRequest req) {
        if (req.displayName() == null || req.displayName().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이름은 필수입니다.");
        }
        if ((req.email() == null || req.email().isBlank()) && (req.phone() == null || req.phone().isBlank())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이메일 또는 전화번호 중 하나는 필수입니다.");
        }
        Long id = jdbc.queryForObject("""
                INSERT INTO notification_recipient (user_id, display_name, email, phone)
                VALUES (?, ?, ?, ?) RETURNING id
                """, Long.class, req.userId(), req.displayName(), req.email(), req.phone());
        return ApiResponse.success(Map.of("id", id));
    }

    @DeleteMapping("/api/admin/notification/recipients/{id}")
    public ApiResponse<Void> deleteRecipient(@PathVariable long id) {
        jdbc.update("UPDATE notification_recipient SET deleted_at=now() WHERE id=?", id);
        return ApiResponse.success(null);
    }

    public record SubscriptionRequest(String channelType, String minSeverity) {}

    @PutMapping("/api/admin/notification/recipients/{id}/subscriptions")
    public ApiResponse<Void> upsertSubscription(@PathVariable long id, @RequestBody SubscriptionRequest req) {
        jdbc.update("""
                INSERT INTO notification_subscription (recipient_id, channel_type, min_severity)
                VALUES (?, ?, ?)
                ON CONFLICT (recipient_id, channel_type) DO UPDATE SET
                    min_severity = EXCLUDED.min_severity, enabled = true, updated_at = now()
                """, id, req.channelType(), req.minSeverity() == null ? "WARNING" : req.minSeverity());
        return ApiResponse.success(null);
    }
}

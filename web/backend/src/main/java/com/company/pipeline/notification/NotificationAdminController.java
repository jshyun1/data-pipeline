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
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.ADMIN)
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

    /**
     * 연결 테스트 — CHANNEL_TEST 발송 건을 아웃박스에 넣어 디스패처가 실제 발송을 시도한다.
     * EMAIL/SMS 는 «등록된 수신자의 실제 이메일/전화»로 보낸다(예전엔 주소가 비어 아무데도 안 갔다).
     * IN_APP(화면알림)은 주소가 필요 없어 브로드캐스트 1건.
     */
    @PostMapping("/api/notifications/channels/{type}/test")
    public ApiResponse<Map<String, Object>> test(@PathVariable String type) {
        long ts = jdbc.queryForObject("SELECT floor(extract(epoch from now()))::bigint", Long.class);
        String eventKey = "TEST-" + type + "-" + ts;
        int enqueued;
        if ("EMAIL".equals(type)) {
            enqueued = enqueueTestToRecipients("EMAIL", eventKey, "email");
        } else if ("SMS".equals(type)) {
            enqueued = enqueueTestToRecipients("SMS", eventKey, "phone");
        } else {
            jdbc.update("""
                    INSERT INTO notification_delivery
                        (event_key, dedup_key, channel_type, severity, severity_rank, category, template_key,
                         subject, body, status, next_attempt_at, expires_at, occurred_at)
                    VALUES (?, md5(?), ?, 'INFO', 2, 'SYSTEM', 'CHANNEL_TEST',
                            '[관제] 연결 테스트', '이 메시지가 보이면 채널이 정상입니다.', 'PENDING',
                            now(), now() + interval '1 hour', now())
                    ON CONFLICT (dedup_key) DO NOTHING
                    """, eventKey, eventKey, type);
            enqueued = 1;
        }
        return ApiResponse.success(Map.of("enqueued", enqueued));
    }

    /** 해당 연락 수단(email/phone)이 있는 활성 수신자 각각에게 테스트 발송을 아웃박스에 넣는다. */
    private int enqueueTestToRecipients(String channel, String eventKey, String addrCol) {
        // addrCol 은 코드 상수("email"/"phone")만 들어온다(사용자 입력 아님).
        List<Map<String, Object>> recips = jdbc.queryForList(
                "SELECT id, " + addrCol + " AS addr FROM notification_recipient "
                        + "WHERE deleted_at IS NULL AND enabled AND " + addrCol + " IS NOT NULL AND " + addrCol + " <> ''");
        int n = 0;
        for (Map<String, Object> r : recips) {
            long rid = ((Number) r.get("id")).longValue();
            String addr = (String) r.get("addr");
            String key = eventKey + "-" + rid;
            jdbc.update("""
                    INSERT INTO notification_delivery
                        (event_key, dedup_key, channel_type, severity, severity_rank, category, recipient_id,
                         target_address, template_key, subject, body, status, next_attempt_at, expires_at, occurred_at)
                    VALUES (?, md5(?), ?, 'INFO', 2, 'SYSTEM', ?, ?, 'CHANNEL_TEST',
                            '[관제] 연결 테스트', '이 메시지가 보이면 채널이 정상입니다.', 'PENDING',
                            now(), now() + interval '1 hour', now())
                    ON CONFLICT (dedup_key) DO NOTHING
                    """, key, key, channel, rid, addr);
            n++;
        }
        return n;
    }

    // ---------------------------------------------------------------- 수신자

    @GetMapping("/api/admin/notification/recipients")
    public ApiResponse<List<Map<String, Object>>> recipients() {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT r.id, r.user_id, r.display_name, r.email,
                       -- 마스킹하지 않는다. 관리자만 여는 설정 화면이고, 가려진 번호로는
                       -- "이 번호가 맞나"를 확인할 수도 수정할 수도 없다.
                       r.phone,
                       r.enabled,
                       (SELECT json_agg(json_build_object('channel', s.channel_type, 'minSeverity', s.min_severity))
                        FROM notification_subscription s WHERE s.recipient_id = r.id AND s.enabled) AS subscriptions
                FROM notification_recipient r WHERE r.deleted_at IS NULL ORDER BY r.id"""));
    }

    public record RecipientRequest(String userId, String displayName, String email, String phone, Boolean enabled) {}

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

    /**
     * 수신자 수정. 지금까지 등록/삭제만 있어서 오타 하나를 고치려면 지웠다 다시 넣어야 했고,
     * 그때마다 id 가 바뀌어 구독(notification_subscription)이 끊겼다.
     *
     * <p>null 인 필드는 건드리지 않는다(부분 수정). 빈 문자열은 "지운다"는 뜻으로 해석해 NULL 로 넣는다 —
     * 이메일만 남기고 전화번호를 빼는 조작이 가능해야 한다.
     */
    @PutMapping("/api/admin/notification/recipients/{id}")
    public ApiResponse<Void> updateRecipient(@PathVariable long id, @RequestBody RecipientRequest req) {
        if (req.displayName() != null && req.displayName().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이름은 비울 수 없습니다.");
        }
        String email = normalize(req.email());
        String phone = normalize(req.phone());
        // 수정 후에도 연락 수단이 하나는 남아야 한다. 현재 값과 합쳐서 판단한다.
        Map<String, Object> current;
        try {
            current = jdbc.queryForMap(
                    "SELECT email, phone FROM notification_recipient WHERE id=? AND deleted_at IS NULL", id);
        } catch (org.springframework.dao.EmptyResultDataAccessException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "수신자를 찾을 수 없습니다: " + id);
        }
        String finalEmail = req.email() == null ? (String) current.get("email") : email;
        String finalPhone = req.phone() == null ? (String) current.get("phone") : phone;
        if (finalEmail == null && finalPhone == null) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이메일 또는 전화번호 중 하나는 남아 있어야 합니다.");
        }
        jdbc.update("""
                UPDATE notification_recipient SET
                    display_name = COALESCE(?, display_name),
                    email = ?,
                    phone = ?,
                    enabled = COALESCE(?, enabled),
                    user_id = COALESCE(?, user_id),
                    updated_at = now()
                WHERE id = ? AND deleted_at IS NULL
                """, req.displayName(), finalEmail, finalPhone, req.enabled(), req.userId(), id);
        return ApiResponse.success(null);
    }

    /** 빈 문자열은 "지운다"는 의사표시로 보고 NULL 로 바꾼다. */
    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
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
